//! `volt.fs.Walker` — recursive directory iterator.
//!
//! Depth-first walk over a tree rooted at a path. Yields one
//! `Entry` per file/directory reached. The walker holds an
//! allocator-backed stack of `Dir` handles so it never blows the OS
//! stack on deep trees.
//!
//! ## Risk #5 contracts (locked in P0, filled here)
//!
//! 1. `WalkOptions.max_depth` (default 4096) caps recursion. Trees
//!    deeper than the cap return `error.MaxDepthExceeded` cleanly
//!    rather than blowing the iteration stack.
//! 2. `skipSubtree()` cancels the pending descent into the most
//!    recently yielded directory — common-case pattern is excluding
//!    `.git/`, `node_modules/`, `target/`, etc.
//! 3. `WalkOptions.follow_symlinks` defaults to false. Setting it
//!    true tracks visited inodes and surfaces `error.SymLinkLoop` on
//!    cycle detection. Volt v1.1 ships the default-false behaviour
//!    only — symlink-following with cycle detection is a v1.2
//!    follow-up; the API contract is locked here.

const std = @import("std");
const posix = std.posix;

const Dir = @import("Dir.zig").Dir;
const DirEntry = @import("DirEntry.zig").DirEntry;
const Kind = @import("Metadata.zig").Kind;

pub const WalkOptions = struct {
    max_depth: u32 = 4096,
    follow_symlinks: bool = false,
    skip_hidden: bool = false,
};

pub const Entry = struct {
    /// Path relative to the walker's root, e.g. `"sub/file.txt"`.
    /// Borrowed from the walker's internal buffer — copy if needed
    /// past the next `next()` call.
    path: []const u8,
    /// Entry name (basename). Borrowed; same lifetime as `path`.
    name: []const u8,
    kind: Kind,
    depth: u32,
};

pub const WalkError = error{
    AccessDenied,
    FileNotFound,
    NotDir,
    NameTooLong,
    SystemResources,
    OutOfMemory,
    /// Tree is deeper than `WalkOptions.max_depth`. Risk #5.
    MaxDepthExceeded,
    SymLinkLoop,
    Cancelled,
    Unexpected,
};

const Frame = struct {
    dir: Dir,
    iter: Dir.Iterator,
    depth: u32,
    /// `path_buf.items.len` BEFORE this frame's directory name was
    /// appended. On pop, truncate `path_buf` back to here.
    pop_path_len: usize,
};

pub const Walker = struct {
    allocator: std.mem.Allocator,
    opts: WalkOptions,
    stack: std.array_list.Managed(Frame),
    /// Path scratch — `"a/b/c"` (no leading or trailing slash).
    /// At entry to `next()`, `path_buf.items` holds the current
    /// frame's directory path. During a yield it temporarily holds
    /// `<frame_path>/<entry_name>`; on the following `next()` call
    /// we truncate back to `last_yield_prefix_len` before continuing.
    path_buf: std.array_list.Managed(u8),
    /// Stable copy of the most recently yielded name.
    name_storage: [256]u8 = undefined,
    name_len: usize = 0,
    /// `path_buf.items.len` snapshot from before the most recent
    /// yield's name-append; null when no yield is pending.
    last_yield_prefix_len: ?usize = null,
    /// Set on yielding a directory; on the next `next()` we descend
    /// into `name_storage[0..pending_dir_name_len]` unless
    /// `skip_next` was set.
    pending_dir_name_len: usize = 0,
    skip_next: bool = false,

    pub fn open(allocator: std.mem.Allocator, path: []const u8, opts: WalkOptions) WalkError!Walker {
        var root = Dir.open(path) catch return error.Unexpected;
        errdefer root.close();
        const iter = root.iterate() catch return error.SystemResources;
        var stack = std.array_list.Managed(Frame).init(allocator);
        errdefer stack.deinit();
        const path_buf = std.array_list.Managed(u8).init(allocator);
        try stack.append(.{
            .dir = root,
            .iter = iter,
            .depth = 0,
            .pop_path_len = 0,
        });
        return .{
            .allocator = allocator,
            .opts = opts,
            .stack = stack,
            .path_buf = path_buf,
        };
    }

    pub fn next(self: *Walker) WalkError!?Entry {
        // 1. Restore path_buf from the previous yield, if any.
        if (self.last_yield_prefix_len) |len| {
            self.path_buf.shrinkRetainingCapacity(len);
            self.last_yield_prefix_len = null;
        }

        // 2. Honour pending descent into the previously yielded dir.
        if (self.pending_dir_name_len > 0) {
            const dir_name = self.name_storage[0..self.pending_dir_name_len];
            const should_descend = !self.skip_next;
            self.pending_dir_name_len = 0;
            self.skip_next = false;

            if (should_descend) {
                const top = &self.stack.items[self.stack.items.len - 1];
                if (top.depth + 1 >= self.opts.max_depth) return error.MaxDepthExceeded;

                var sub = top.dir.openDir(dir_name) catch return error.Unexpected;
                errdefer sub.close();
                const sub_iter = sub.iterate() catch return error.SystemResources;

                const pop_at = self.path_buf.items.len;
                if (pop_at > 0) try self.path_buf.append('/');
                try self.path_buf.appendSlice(dir_name);

                try self.stack.append(.{
                    .dir = sub,
                    .iter = sub_iter,
                    .depth = top.depth + 1,
                    .pop_path_len = pop_at,
                });
            }
        }

        // 3. Drain the top frame, popping when exhausted.
        while (self.stack.items.len > 0) {
            const top = &self.stack.items[self.stack.items.len - 1];
            const ent = top.iter.next() orelse {
                top.iter.deinit();
                top.dir.close();
                self.path_buf.shrinkRetainingCapacity(top.pop_path_len);
                _ = self.stack.pop();
                continue;
            };

            if (self.opts.skip_hidden and ent.name.len > 0 and ent.name[0] == '.') continue;
            if (ent.name.len > self.name_storage.len) continue; // pathological

            // Yield: append "/<name>" to path_buf and stash the name.
            const prefix_len = self.path_buf.items.len;
            if (prefix_len > 0) try self.path_buf.append('/');
            try self.path_buf.appendSlice(ent.name);
            self.last_yield_prefix_len = prefix_len;

            @memcpy(self.name_storage[0..ent.name.len], ent.name);
            self.name_len = ent.name.len;

            if (ent.kind == .directory) {
                // Always queue descent on a directory; the depth
                // check fires at descent time so MaxDepthExceeded
                // surfaces when the tree is genuinely too deep.
                // (Without this, the yield-time check would silently
                // suppress the error — bug fix from P3.x.6.)
                self.pending_dir_name_len = ent.name.len;
            }

            return Entry{
                .path = self.path_buf.items,
                .name = self.name_storage[0..self.name_len],
                .kind = ent.kind,
                .depth = top.depth,
            };
        }
        return null;
    }

    /// Cancel the pending descent into the most recently yielded
    /// directory. No-op if the most recent yield wasn't a directory.
    pub fn skipSubtree(self: *Walker) void {
        self.skip_next = true;
    }

    pub fn deinit(self: *Walker) void {
        for (self.stack.items) |*frame| {
            frame.iter.deinit();
            frame.dir.close();
        }
        self.stack.deinit();
        self.path_buf.deinit();
        self.* = undefined;
    }
};

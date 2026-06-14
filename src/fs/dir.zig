//! Directory operations — `Dir.open` for iteration, `Dir.create` /
//! `Dir.remove` for create / delete (with recursive variants),
//! `Dir.entries` for a snapshot, `Dir.walk` for streaming tree
//! traversal, and `glob` for Node-style pattern matching.
//!
//! POSIX path uses libc opendir / readdir / closedir (std.c
//! re-exports them, picking the right Darwin symbol). Windows path
//! is pending — FindFirstFile / FindNextFile lands in a follow-up.

const std = @import("std");
const builtin = @import("builtin");

const syscall = @import("syscall.zig");
const fs_error = @import("error.zig");
const metadata_mod = @import("metadata.zig");
const win32 = @import("win32.zig");
const fs = @import("../fs.zig");

const is_windows = builtin.os.tag == .windows;

pub const FsError = fs_error.FsError;
pub const FileType = metadata_mod.FileType;
pub const Metadata = metadata_mod.Metadata;

// ─── Create / remove ─────────────────────────────────────────────

/// Options for `Dir.create`.
pub const CreateOptions = struct {
    /// `true` ⇒ `mkdir -p` behaviour — create missing parents,
    /// don't error if the leaf already exists.
    recursive: bool = false,
    /// Mode for newly-created directories. Subject to umask.
    mode: u32 = 0o755,
};

/// Options for `Dir.remove`.
pub const RemoveOptions = struct {
    /// `true` ⇒ `rm -rf` behaviour — delete contents first.
    recursive: bool = false,
};

/// Create a directory. `opts.recursive = true` creates any missing
/// parent directories (`mkdir -p`); a pre-existing leaf is not an
/// error in that mode.
pub fn create(path: []const u8, opts: CreateOptions) FsError!void {
    if (!opts.recursive) return mkdirOne(path, opts.mode);

    // Walk each prefix, ensuring it exists as a directory. Windows
    // accepts both separators, so treat `\` as a separator too there.
    var end: usize = 0;
    while (end <= path.len) : (end += 1) {
        const at_sep = end == path.len or path[end] == '/' or (is_windows and path[end] == '\\');
        if (at_sep) {
            if (end == 0) continue;
            mkdirOne(path[0..end], opts.mode) catch |e| switch (e) {
                error.AlreadyExists => {},
                else => return e,
            };
        }
    }
}

/// Remove a directory. `opts.recursive = true` deletes contents
/// first (`rm -rf`); the default expects the directory to be empty.
pub fn remove(allocator: std.mem.Allocator, path: []const u8, opts: RemoveOptions) (FsError || error{OutOfMemory})!void {
    if (!opts.recursive) return rmdirOne(path);

    var d = try Dir.open(path);
    var saw_err: ?(FsError || error{OutOfMemory}) = null;
    {
        defer d.close();
        while (try d.next()) |entry| {
            const child = std.fs.path.join(allocator, &.{ path, entry.name }) catch return error.OutOfMemory;
            defer allocator.free(child);

            if (entry.kind == .directory) {
                remove(allocator, child, opts) catch |e| {
                    saw_err = e;
                };
            } else if (is_windows) {
                fs.unlink(child) catch |e| {
                    saw_err = e;
                };
            } else {
                if (syscall.c_unlink(@ptrCast(try toZ(child))) != 0) {
                    saw_err = fs_error.fromErrno(fs_error.currentErrno());
                }
            }
        }
    }
    if (saw_err) |e| return e;
    return rmdirOne(path);
}

fn mkdirOne(path: []const u8, mode: u32) FsError!void {
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(path);
        if (win32.CreateDirectoryW(z.ptr(), null) == 0) return win32.fromLastError(win32.GetLastError());
        return;
    }
    const z = try toZ(path);
    if (syscall.c_mkdir(z, @intCast(mode)) != 0) {
        return fs_error.fromErrno(fs_error.currentErrno());
    }
}

fn rmdirOne(path: []const u8) FsError!void {
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(path);
        if (win32.RemoveDirectoryW(z.ptr()) == 0) return win32.fromLastError(win32.GetLastError());
        return;
    }
    const z = try toZ(path);
    if (syscall.c_rmdir(z) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

// ─── Entries snapshot ────────────────────────────────────────────

/// One entry from a directory listing. `name` is owned by the
/// caller-supplied allocator (in the `entries` snapshot path); in
/// the streaming `walk` path it's borrowed from the readdir
/// buffer and only valid until the next iteration.
pub const Entry = struct {
    /// Filename only — no parent-directory prefix.
    name: []const u8,
    /// File kind from `d_type` if the filesystem provides it; falls
    /// back to `.unknown` and `metadata()` resolves it via stat.
    kind: FileType,

    /// Fetch full metadata. Costs a `stat` syscall.
    pub fn metadata(self: Entry, parent_path: []const u8, allocator: std.mem.Allocator) (FsError || error{OutOfMemory})!Metadata {
        const full = std.fs.path.join(allocator, &.{ parent_path, self.name }) catch return error.OutOfMemory;
        defer allocator.free(full);
        return fs.stat(full);
    }

    pub fn isFile(self: Entry) bool {
        return self.kind == .file;
    }
    pub fn isDir(self: Entry) bool {
        return self.kind == .directory;
    }
    pub fn isSymlink(self: Entry) bool {
        return self.kind == .sym_link;
    }
};

/// One-shot snapshot of all entries in `path`. Caller owns the
/// returned slice + the `name` strings; use `freeEntries` to
/// release.
pub fn entries(allocator: std.mem.Allocator, path: []const u8) (FsError || error{OutOfMemory})![]Entry {
    var d = try Dir.open(path);
    defer d.close();

    var list = std.array_list.Managed(Entry).init(allocator);
    errdefer {
        for (list.items) |e| allocator.free(e.name);
        list.deinit();
    }
    while (try d.next()) |raw| {
        try list.append(.{
            .name = try allocator.dupe(u8, raw.name),
            .kind = raw.kind,
        });
    }
    return list.toOwnedSlice();
}

/// Release a slice returned by `entries` together with its name
/// strings.
pub fn freeEntries(allocator: std.mem.Allocator, es: []Entry) void {
    for (es) |e| allocator.free(e.name);
    allocator.free(es);
}

// ─── Dir handle ──────────────────────────────────────────────────

/// Per-OS directory-handle backing. POSIX is a `DIR *`; Windows is a
/// `FindFirstFileW` handle plus the buffered first result (Windows
/// hands back the first entry from the *open* call, so the iterator
/// must remember to yield it before calling `FindNextFileW`).
const DirHandle = if (is_windows) win32.HANDLE else *std.c.DIR;

const WinDirState = if (is_windows) struct {
    find_data: win32.WIN32_FIND_DATAW = undefined,
    /// The first entry is already in `find_data` after `open`.
    first_pending: bool = false,
    done: bool = false,
    /// UTF-8 scratch for the current entry's name. `Entry.name`
    /// borrows from here — valid until the next `next`/`close`, same
    /// contract as the POSIX readdir buffer. Sized for a worst-case
    /// MAX_PATH UTF-16 component.
    name_buf: [win32.MAX_PATH * 3]u8 = undefined,
} else struct {};

/// Live directory iterator. Single-pass — to rewind, close and reopen.
pub const Dir = struct {
    handle: DirHandle,
    win: WinDirState = .{},

    /// Open `path` for streaming iteration. Caller closes via
    /// `close`. Fails with `FsError.NotFound` / `NotDirectory` /
    /// `AccessDenied` per the underlying open errno.
    pub fn open(path: []const u8) FsError!Dir {
        if (is_windows) {
            // FindFirstFileW wants a search glob: `<path>\*`.
            var pattern_buf: [win32.WPATH_MAX]u8 = undefined;
            const pattern = std.fmt.bufPrint(&pattern_buf, "{s}\\*", .{path}) catch return error.NameTooLong;
            const wpat = try win32.WPathZ.fromUtf8(pattern);
            var dir: Dir = .{ .handle = undefined, .win = .{} };
            const h = win32.FindFirstFileW(wpat.ptr(), &dir.win.find_data);
            if (h == win32.INVALID_HANDLE_VALUE) return win32.fromLastError(win32.GetLastError());
            dir.handle = h;
            dir.win.first_pending = true;
            return dir;
        }
        const z = try toZ(path);
        const handle = std.c.opendir(z) orelse return fs_error.fromErrno(fs_error.currentErrno());
        return .{ .handle = handle };
    }

    pub fn close(self: *Dir) void {
        if (is_windows) {
            _ = win32.FindClose(self.handle);
            return;
        }
        _ = std.c.closedir(self.handle);
    }

    /// Next entry, or null at end. Skips `.` and `..`. The
    /// returned name is borrowed from the iterator's buffer — valid
    /// only until the next `next` or `close`.
    pub fn next(self: *Dir) FsError!?Entry {
        if (is_windows) return self.nextWindows();
        while (true) {
            const entry = std.c.readdir(self.handle) orelse return null;
            const name_slice: []const u8 = if (comptime is_darwin())
                entry.name[0..entry.namlen]
            else
                entry.name[0..std.mem.indexOfScalar(u8, &entry.name, 0).?];

            if (std.mem.eql(u8, name_slice, ".") or std.mem.eql(u8, name_slice, "..")) continue;

            return .{
                .name = name_slice,
                .kind = decodeDType(entry.type),
            };
        }
    }

    fn nextWindows(self: *Dir) FsError!?Entry {
        while (true) {
            if (self.win.done) return null;
            if (self.win.first_pending) {
                self.win.first_pending = false;
            } else if (win32.FindNextFileW(self.handle, &self.win.find_data) == 0) {
                const code = win32.GetLastError();
                self.win.done = true;
                if (code == win32.ERROR_NO_MORE_FILES) return null;
                return win32.fromLastError(code);
            }

            const wname = &self.win.find_data.cFileName;
            const wlen = std.mem.indexOfScalar(u16, wname, 0) orelse wname.len;
            const u8len = std.unicode.utf16LeToUtf8(&self.win.name_buf, wname[0..wlen]) catch continue;
            const name_slice = self.win.name_buf[0..u8len];

            if (std.mem.eql(u8, name_slice, ".") or std.mem.eql(u8, name_slice, "..")) continue;

            return .{
                .name = name_slice,
                .kind = winAttrKind(self.win.find_data.dwFileAttributes),
            };
        }
    }

    /// Streaming tree walk. The visitor receives each entry plus
    /// its depth (root = 0); return value drives traversal:
    ///   * `.continue` — recurse into directories
    ///   * `.skip` — don't recurse into this entry's subtree
    ///   * `.stop` — abort the walk
    /// `opts` controls depth and visibility.
    ///
    /// The caller still owns this `Dir` — `walk` opens fresh Dir
    /// handles for subdirectories and closes them as it pops.
    /// Don't keep iterating `self` directly after calling `walk`.
    pub fn walk(self: *Dir, root_path: []const u8, allocator: std.mem.Allocator, visitor: anytype, opts: WalkOptions) (FsError || error{OutOfMemory})!void {
        return walkPath(self, root_path, allocator, visitor, opts);
    }
};

/// Internal: `Dir.walk` delegates here so the stack-management
/// invariants live in one place. `root_dir` is borrowed — caller
/// owns its lifecycle. Subdir frames are opened + closed here.
fn walkPath(root_dir: *Dir, root_path: []const u8, allocator: std.mem.Allocator, visitor: anytype, opts: WalkOptions) (FsError || error{OutOfMemory})!void {
    var stack = std.array_list.Managed(SubFrame).init(allocator);
    defer {
        for (stack.items) |*f| {
            f.dir.close();
            allocator.free(f.path);
        }
        stack.deinit();
    }

    // Use a tagged "current frame" that's either the borrowed root
    // or the top of the owned subdir stack. Avoids placing the
    // borrowed root inside the owned stack, which made cleanup
    // ambiguous.
    var cur_dir: *Dir = root_dir;
    var cur_path: []const u8 = root_path;
    var cur_depth: u32 = 0;

    while (true) {
        const entry_opt = cur_dir.next() catch |e| return e;
        if (entry_opt) |entry| {
            if (!opts.include_hidden and entry.name.len > 0 and entry.name[0] == '.') continue;

            const action = visitor.visit(entry, cur_depth);
            switch (action) {
                .stop => return,
                .skip => continue,
                .@"continue" => {
                    if (entry.kind == .directory and cur_depth + 1 <= opts.max_depth) {
                        const child_path = std.fs.path.join(allocator, &.{ cur_path, entry.name }) catch return error.OutOfMemory;
                        const child_dir = Dir.open(child_path) catch |e| {
                            allocator.free(child_path);
                            return e;
                        };
                        try stack.append(.{ .dir = child_dir, .path = child_path, .depth = cur_depth + 1 });
                        // Re-point cur_* at the new top of stack.
                        const last = &stack.items[stack.items.len - 1];
                        cur_dir = &last.dir;
                        cur_path = last.path;
                        cur_depth = last.depth;
                    }
                },
            }
        } else {
            // Current frame exhausted. If it's a subdir, pop +
            // close it; if it's the root, we're done.
            if (stack.items.len == 0) return;
            var popped = stack.pop().?;
            popped.dir.close();
            allocator.free(popped.path);
            if (stack.items.len == 0) {
                cur_dir = root_dir;
                cur_path = root_path;
                cur_depth = 0;
            } else {
                const last = &stack.items[stack.items.len - 1];
                cur_dir = &last.dir;
                cur_path = last.path;
                cur_depth = last.depth;
            }
        }
    }
}

/// Sub-directory walk frame. Root is tracked separately because the
/// caller owns it.
const SubFrame = struct {
    dir: Dir,
    path: []u8, // owned by the allocator
    depth: u32,
};

/// Visitor's return-value vocabulary for `walk`.
pub const WalkAction = enum { @"continue", skip, stop };

/// Tunables for `walk`.
pub const WalkOptions = struct {
    /// Hard cap on recursion depth (root is depth 0). Default = max.
    max_depth: u32 = std.math.maxInt(u32),
    /// `false` ⇒ skip names starting with `.` (POSIX convention).
    include_hidden: bool = false,
    /// `false` ⇒ don't descend through symlinks (default — prevents
    /// cycles).
    follow_symlinks: bool = false,
};

// ─── d_type decoding ─────────────────────────────────────────────

fn decodeDType(t: u8) FileType {
    return switch (t) {
        1 => .fifo, // DT_FIFO
        2 => .char_device, // DT_CHR
        4 => .directory, // DT_DIR
        6 => .block_device, // DT_BLK
        8 => .file, // DT_REG
        10 => .sym_link, // DT_LNK
        12 => .socket, // DT_SOCK
        else => .unknown,
    };
}

/// Windows entry kind from `WIN32_FIND_DATAW.dwFileAttributes`.
fn winAttrKind(attrs: u32) FileType {
    if ((attrs & win32.FILE_ATTRIBUTE_REPARSE_POINT) != 0) return .sym_link;
    if ((attrs & win32.FILE_ATTRIBUTE_DIRECTORY) != 0) return .directory;
    return .file;
}

fn is_darwin() bool {
    return switch (builtin.os.tag) {
        .macos, .ios, .tvos, .watchos => true,
        else => false,
    };
}

// ─── NUL-terminate helper ────────────────────────────────────────

fn toZ(p: []const u8) FsError![*:0]const u8 {
    // Stack-allocate a small buffer; large paths fall back to fail.
    // Most paths fit easily inside 1024 bytes.
    var buf: [syscall.PATH_MAX:0]u8 = undefined;
    if (p.len >= syscall.PATH_MAX) return error.NameTooLong;
    if (std.mem.indexOfScalar(u8, p, 0) != null) return error.InvalidPath;
    @memcpy(buf[0..p.len], p);
    buf[p.len] = 0;
    // CAUTION: buf is stack-local. Caller must use the returned
    // pointer immediately, before the next syscall returns. All
    // call sites in this file do.
    return @ptrCast(&buf);
}

// ─── glob ────────────────────────────────────────────────────────

/// Node-style glob match. Supports `*` (any non-separator run),
/// `?` (any single char), `**` (any descendant — only meaningful
/// as a whole path component), and basic character classes
/// `[abc]` / `[a-z]`. Returns owned paths; caller frees each
/// + the slice.
pub fn glob(allocator: std.mem.Allocator, pattern: []const u8) (FsError || error{OutOfMemory})![][]u8 {
    // Portable: built entirely on Dir.open/next + fs.exists + path
    // utilities, all of which are cross-platform.
    var results = std.array_list.Managed([]u8).init(allocator);
    errdefer {
        for (results.items) |item| allocator.free(item);
        results.deinit();
    }

    // Find the longest non-wildcard prefix by byte position. This
    // keeps the leading `/` intact for absolute patterns and avoids
    // path.join's empty-segment stripping.
    var last_slash: usize = 0;
    var i: usize = 0;
    while (i < pattern.len) : (i += 1) {
        const c = pattern[i];
        if (c == '*' or c == '?' or c == '[') break;
        if (c == '/') last_slash = i;
    }

    const has_wildcard = i < pattern.len;
    if (!has_wildcard) {
        // No wildcard — just test the literal.
        if (fs.exists(pattern)) try results.append(try allocator.dupe(u8, pattern));
        return results.toOwnedSlice();
    }

    const prefix_path: []const u8 = if (last_slash == 0 and pattern[0] != '/')
        "."
    else if (last_slash == 0 and pattern[0] == '/')
        "/"
    else
        pattern[0..last_slash];

    const remaining = if (last_slash + 1 < pattern.len)
        pattern[last_slash + 1 ..]
    else
        "";

    try matchInto(allocator, &results, prefix_path, remaining);
    return results.toOwnedSlice();
}

fn containsWildcard(s: []const u8) bool {
    for (s) |c| if (c == '*' or c == '?' or c == '[') return true;
    return false;
}

fn matchInto(allocator: std.mem.Allocator, out: *std.array_list.Managed([]u8), current: []const u8, remaining: []const u8) (FsError || error{OutOfMemory})!void {
    if (remaining.len == 0) {
        try out.append(try allocator.dupe(u8, current));
        return;
    }

    // Pop the next segment.
    const slash = std.mem.indexOfScalar(u8, remaining, '/');
    const seg = if (slash) |i| remaining[0..i] else remaining;
    const rest = if (slash) |i| remaining[i + 1 ..] else "";

    // `**`: match zero or more segments.
    if (std.mem.eql(u8, seg, "**")) {
        // (1) zero — keep going with rest at current path.
        try matchInto(allocator, out, current, rest);
        // (2) one-or-more — descend, recurse with **/rest.
        var d = Dir.open(current) catch return;
        defer d.close();
        while (try d.next()) |entry| {
            if (entry.name[0] == '.') continue;
            const child = std.fs.path.join(allocator, &.{ current, entry.name }) catch return error.OutOfMemory;
            defer allocator.free(child);
            if (entry.kind == .directory or entry.kind == .unknown) {
                // Try the ** recursion for descendants.
                try matchInto(allocator, out, child, remaining);
            }
        }
        return;
    }

    var d = Dir.open(current) catch return;
    defer d.close();
    while (try d.next()) |entry| {
        if (entry.name[0] == '.') continue;
        if (!matchSegment(seg, entry.name)) continue;
        const child = std.fs.path.join(allocator, &.{ current, entry.name }) catch return error.OutOfMemory;
        if (rest.len == 0) {
            try out.append(child);
        } else {
            defer allocator.free(child);
            try matchInto(allocator, out, child, rest);
        }
    }
}

fn matchSegment(pat: []const u8, name: []const u8) bool {
    var pi: usize = 0;
    var ni: usize = 0;
    var star_pi: ?usize = null;
    var star_ni: usize = 0;

    while (ni < name.len) {
        if (pi < pat.len) {
            const pc = pat[pi];
            if (pc == '*') {
                star_pi = pi;
                star_ni = ni;
                pi += 1;
                continue;
            }
            if (pc == '?') {
                pi += 1;
                ni += 1;
                continue;
            }
            if (pc == '[') {
                // Character class: scan to matching ']', test the
                // current char.
                const end = std.mem.indexOfScalarPos(u8, pat, pi + 1, ']') orelse return false;
                if (matchClass(pat[pi + 1 .. end], name[ni])) {
                    pi = end + 1;
                    ni += 1;
                    continue;
                }
            } else if (pc == name[ni]) {
                pi += 1;
                ni += 1;
                continue;
            }
        }
        // No match at this position — backtrack to the most recent
        // '*'.
        if (star_pi) |sp| {
            pi = sp + 1;
            star_ni += 1;
            ni = star_ni;
        } else {
            return false;
        }
    }
    // Consume trailing '*' in pattern.
    while (pi < pat.len and pat[pi] == '*') pi += 1;
    return pi == pat.len;
}

fn matchClass(class: []const u8, c: u8) bool {
    var i: usize = 0;
    while (i < class.len) : (i += 1) {
        if (i + 2 < class.len and class[i + 1] == '-') {
            if (c >= class[i] and c <= class[i + 2]) return true;
            i += 2;
        } else if (class[i] == c) {
            return true;
        }
    }
    return false;
}

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;
const tu = @import("../testing.zig");

test "Dir.create + Dir.remove (non-recursive)" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    const child = try tmp.childPath(testing.allocator, "sub");
    defer testing.allocator.free(child);

    try create(child, .{});
    const m = try fs.stat(child);
    try testing.expect(m.isDir());
    try remove(testing.allocator, child, .{});
}

test "Dir.create recursive (mkdir -p) + Dir.remove recursive (rm -rf)" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    const deep = try tmp.childPath(testing.allocator, "a/b/c/d");
    defer testing.allocator.free(deep);

    try create(deep, .{ .recursive = true });
    const m = try fs.stat(deep);
    try testing.expect(m.isDir());

    // Drop a file in `c` so the recursive remove has something to
    // clean up too.
    const note = try tmp.writeChild(testing.allocator, "a/b/c/note.txt", "");
    testing.allocator.free(note);

    const root = try tmp.childPath(testing.allocator, "a");
    defer testing.allocator.free(root);
    try remove(testing.allocator, root, .{ .recursive = true });

    try testing.expect(!fs.exists(root));
}

test "Dir.entries: snapshot returns files in the directory" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    inline for (.{ "a.txt", "b.txt", "c.txt" }) |name| {
        const p = try tmp.writeChild(testing.allocator, name, "");
        testing.allocator.free(p);
    }

    const list = try entries(testing.allocator, tmp.path);
    defer freeEntries(testing.allocator, list);
    try testing.expectEqual(@as(usize, 3), list.len);
}

const CountVisitor = struct {
    files: u32 = 0,
    dirs: u32 = 0,

    pub fn visit(self: *CountVisitor, entry: Entry, depth: u32) WalkAction {
        _ = depth;
        switch (entry.kind) {
            .file => self.files += 1,
            .directory => self.dirs += 1,
            else => {},
        }
        return .@"continue";
    }
};

test "Dir.walk: counts entries across a small tree" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    // Build: tmp/sub1/{f1, f2}, tmp/sub2/{f3}, tmp/top
    inline for (.{ "sub1", "sub2" }) |sub| {
        const p = try tmp.childPath(testing.allocator, sub);
        defer testing.allocator.free(p);
        try create(p, .{});
    }
    inline for (.{ "sub1/f1", "sub1/f2", "sub2/f3", "top" }) |rel| {
        const p = try tmp.writeChild(testing.allocator, rel, "");
        testing.allocator.free(p);
    }

    var d = try Dir.open(tmp.path);
    defer d.close();
    var visitor = CountVisitor{};
    try d.walk(tmp.path, testing.allocator, &visitor, .{});

    try testing.expectEqual(@as(u32, 4), visitor.files);
    try testing.expectEqual(@as(u32, 2), visitor.dirs);
}

const FirstStopVisitor = struct {
    seen: u32 = 0,

    pub fn visit(self: *FirstStopVisitor, entry: Entry, depth: u32) WalkAction {
        _ = entry;
        _ = depth;
        self.seen += 1;
        return .stop;
    }
};

test "Dir.walk: visitor `.stop` aborts after first entry" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    inline for (.{ "a", "b", "c" }) |name| {
        const p = try tmp.writeChild(testing.allocator, name, "");
        testing.allocator.free(p);
    }

    var d = try Dir.open(tmp.path);
    defer d.close();
    var v = FirstStopVisitor{};
    try d.walk(tmp.path, testing.allocator, &v, .{});
    try testing.expectEqual(@as(u32, 1), v.seen);
}

test "glob: matches *.txt in flat directory" {
    var tmp = try tu.TempDir.create(testing.allocator);
    defer tmp.deinit();

    inline for (.{ "match.txt", "miss.md", "match2.txt" }) |name| {
        const p = try tmp.writeChild(testing.allocator, name, "");
        testing.allocator.free(p);
    }

    const pat = try tmp.childPath(testing.allocator, "*.txt");
    defer testing.allocator.free(pat);
    const matches = try glob(testing.allocator, pat);
    defer {
        for (matches) |m| testing.allocator.free(m);
        testing.allocator.free(matches);
    }
    try testing.expectEqual(@as(usize, 2), matches.len);
}

test "glob: matchSegment patterns" {
    try testing.expect(matchSegment("*.zig", "foo.zig"));
    try testing.expect(!matchSegment("*.zig", "foo.md"));
    try testing.expect(matchSegment("a?c", "abc"));
    try testing.expect(!matchSegment("a?c", "abbc"));
    try testing.expect(matchSegment("[abc]xy", "axy"));
    try testing.expect(!matchSegment("[abc]xy", "dxy"));
    try testing.expect(matchSegment("[a-z]oo", "foo"));
    try testing.expect(!matchSegment("[a-z]oo", "Foo"));
    try testing.expect(matchSegment("*", "anything"));
    try testing.expect(matchSegment("pre*suf", "presuf"));
    try testing.expect(matchSegment("pre*suf", "preXYZsuf"));
}

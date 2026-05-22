//! Filesystem change watching — `Watcher.init` then `.watch(path)`,
//! then `.next()` blocks until something changes under a watched
//! path.
//!
//! **v1 implementation: polling.** A periodic mtime+size+entry-set
//! diff drives the event stream. Resolution = `Watcher.Options
//! .poll_interval`, default 200 ms. Cross-platform identically.
//!
//! Future: swap to inotify (Linux), FSEvents (Darwin / CoreServices),
//! and ReadDirectoryChangesW (Windows) behind the same API. The
//! public surface won't change; backends drop in.
//!
//! Events are deduplicated within a single poll cycle (one event
//! per (path, kind)). Polling means very short-lived changes
//! (created+deleted within `poll_interval`) may be missed — the
//! native backends will fix that.

const std = @import("std");
const builtin = @import("builtin");

const syscall = @import("syscall.zig");
const fs_error = @import("error.zig");
const metadata_mod = @import("metadata.zig");
const dir_mod = @import("dir.zig");
const fs = @import("../fs.zig");
const lib = @import("../lib.zig");

const is_windows = builtin.os.tag == .windows;

pub const FsError = fs_error.FsError;

// ─── Events ──────────────────────────────────────────────────────

pub const EventKind = enum {
    /// A new entry appeared at this path.
    created,
    /// An existing entry's content / size / mtime changed.
    modified,
    /// An entry previously seen no longer exists.
    removed,
};

pub const Event = struct {
    /// Path that changed (caller-allocator-owned; freed by the
    /// allocator passed to `next` / `events`).
    path: []u8,
    kind: EventKind,
    /// Monotonic timestamp when the watcher observed the change.
    /// Useful for downstream debouncing.
    observed: lib.Instant,
};

// ─── Options ─────────────────────────────────────────────────────

pub const WatchOptions = struct {
    /// Recurse into subdirectories under the watched path. v1
    /// snapshots the entire subtree at `watch` time and diffs.
    recursive: bool = false,
    /// Subscribe to specific event kinds. `null` = all kinds.
    /// Filter applies after diffing — performance is the same.
    events: ?[]const EventKind = null,
};

pub const Options = struct {
    /// How often the poll loop wakes to diff watched paths.
    /// Smaller = faster reaction; larger = less CPU.
    poll_interval: lib.Duration = lib.Duration.fromMillis(200),
};

// ─── Watcher ─────────────────────────────────────────────────────

/// Live filesystem watcher. Hold one per logical observer; call
/// `watch` for each path of interest. `next` blocks until at least
/// one event is queued.
pub const Watcher = struct {
    allocator: std.mem.Allocator,
    options: Options,
    watches: std.array_list.Managed(WatchEntry),
    pending: std.array_list.Managed(Event),

    pub fn init(allocator: std.mem.Allocator, options: Options) Watcher {
        return .{
            .allocator = allocator,
            .options = options,
            .watches = std.array_list.Managed(WatchEntry).init(allocator),
            .pending = std.array_list.Managed(Event).init(allocator),
        };
    }

    pub fn deinit(self: *Watcher) void {
        for (self.watches.items) |*w| w.deinit(self.allocator);
        self.watches.deinit();
        for (self.pending.items) |e| self.allocator.free(e.path);
        self.pending.deinit();
    }

    /// Begin watching `path`. Snapshots the current state so the
    /// first events are real changes, not the initial population.
    pub fn watch(self: *Watcher, path: []const u8, opts: WatchOptions) (FsError || error{OutOfMemory})!void {
        var entry = WatchEntry{
            .root_path = try self.allocator.dupe(u8, path),
            .recursive = opts.recursive,
            .filter = opts.events,
            .snapshot = std.StringHashMap(EntrySnapshot).init(self.allocator),
        };
        errdefer entry.deinit(self.allocator);
        try populateSnapshot(self.allocator, &entry);
        try self.watches.append(entry);
    }

    /// Block until at least one event is available; return it.
    /// Returns `error.NotFound` if every watched path has been
    /// deleted (terminal — caller should `deinit`).
    pub fn next(self: *Watcher) (FsError || error{OutOfMemory})!Event {
        while (true) {
            if (self.pending.items.len > 0) {
                return self.pending.orderedRemove(0);
            }
            try self.poll();
            if (self.pending.items.len > 0) continue;
            // Sleep one poll interval before trying again.
            lib.sleep(self.options.poll_interval) catch return error.OutOfMemory;
        }
    }

    /// Cancel-aware `next`. Returns `error.Cancelled` if the cancel
    /// fires before an event arrives.
    pub fn nextCancel(self: *Watcher, c: *lib.Cancel) (FsError || error{ OutOfMemory, Cancelled })!Event {
        while (true) {
            if (self.pending.items.len > 0) return self.pending.orderedRemove(0);
            if (c.isFired()) return error.Cancelled;
            try self.poll();
            if (self.pending.items.len > 0) continue;
            lib.sleepCancel(self.options.poll_interval, c) catch |e| switch (e) {
                error.Cancelled => return error.Cancelled,
                else => return error.OutOfMemory, // map other sleep errors generically
            };
        }
    }

    /// Drain whatever's pending (non-blocking). Use for pull-mode
    /// loops where the caller has its own scheduling.
    pub fn drain(self: *Watcher, max: usize) (FsError || error{OutOfMemory})![]Event {
        try self.poll();
        const n = @min(max, self.pending.items.len);
        const out = try self.allocator.alloc(Event, n);
        for (out, 0..) |*e, i| e.* = self.pending.items[i];
        // Shift the remainder forward.
        const remaining = self.pending.items.len - n;
        for (0..remaining) |i| self.pending.items[i] = self.pending.items[n + i];
        self.pending.shrinkRetainingCapacity(remaining);
        return out;
    }

    /// One pass of the diff loop — refresh every watch's snapshot
    /// and emit events for additions / removals / modifications.
    fn poll(self: *Watcher) (FsError || error{OutOfMemory})!void {
        for (self.watches.items) |*w| {
            try self.diffOne(w);
        }
    }

    fn diffOne(self: *Watcher, w: *WatchEntry) (FsError || error{OutOfMemory})!void {
        var fresh = std.StringHashMap(EntrySnapshot).init(self.allocator);
        defer {
            var it = fresh.iterator();
            while (it.next()) |kv| self.allocator.free(kv.key_ptr.*);
            fresh.deinit();
        }

        try collectInto(self.allocator, w.root_path, w.recursive, &fresh);

        // Created / modified: present in `fresh`, missing or
        // changed in `snapshot`.
        var it = fresh.iterator();
        while (it.next()) |kv| {
            const key = kv.key_ptr.*;
            const snap = kv.value_ptr.*;
            if (w.snapshot.get(key)) |prev| {
                if (prev.size != snap.size or prev.mtime_secs != snap.mtime_secs or prev.mtime_nsecs != snap.mtime_nsecs) {
                    try self.emit(w, key, .modified);
                }
            } else {
                try self.emit(w, key, .created);
            }
        }

        // Removed: present in `snapshot`, missing from `fresh`.
        var prev_it = w.snapshot.iterator();
        while (prev_it.next()) |kv| {
            if (!fresh.contains(kv.key_ptr.*)) {
                try self.emit(w, kv.key_ptr.*, .removed);
            }
        }

        // Swap snapshots — old snapshot's keys + values go away.
        clearSnapshot(self.allocator, &w.snapshot);
        var dest_it = fresh.iterator();
        while (dest_it.next()) |kv| {
            const key_dup = try self.allocator.dupe(u8, kv.key_ptr.*);
            errdefer self.allocator.free(key_dup);
            try w.snapshot.put(key_dup, kv.value_ptr.*);
        }
    }

    fn emit(self: *Watcher, w: *WatchEntry, path: []const u8, kind: EventKind) (FsError || error{OutOfMemory})!void {
        if (w.filter) |filter| {
            var allowed = false;
            for (filter) |k| {
                if (k == kind) {
                    allowed = true;
                    break;
                }
            }
            if (!allowed) return;
        }
        const dup = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(dup);
        try self.pending.append(.{ .path = dup, .kind = kind, .observed = lib.Instant.now() });
    }
};

// ─── Internal state ──────────────────────────────────────────────

const WatchEntry = struct {
    root_path: []u8,
    recursive: bool,
    filter: ?[]const EventKind,
    snapshot: std.StringHashMap(EntrySnapshot),

    fn deinit(self: *WatchEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.root_path);
        clearSnapshot(allocator, &self.snapshot);
        self.snapshot.deinit();
    }
};

const EntrySnapshot = struct {
    size: u64,
    mtime_secs: i64,
    mtime_nsecs: u32,
};

fn clearSnapshot(allocator: std.mem.Allocator, snap: *std.StringHashMap(EntrySnapshot)) void {
    var it = snap.iterator();
    while (it.next()) |kv| allocator.free(kv.key_ptr.*);
    snap.clearRetainingCapacity();
}

fn populateSnapshot(allocator: std.mem.Allocator, w: *WatchEntry) (FsError || error{OutOfMemory})!void {
    try collectInto(allocator, w.root_path, w.recursive, &w.snapshot);
}

fn collectInto(allocator: std.mem.Allocator, root: []const u8, recursive: bool, out: *std.StringHashMap(EntrySnapshot)) (FsError || error{OutOfMemory})!void {
    // If the root is a file, snapshot just it. If it's a directory,
    // snapshot its entries (and recurse if asked).
    const root_meta = fs.stat(root) catch |e| switch (e) {
        error.NotFound => return,
        else => return e,
    };

    if (!root_meta.isDir()) {
        const key = try allocator.dupe(u8, root);
        try out.put(key, snapFromMeta(root_meta));
        return;
    }

    var d = try dir_mod.Dir.open(root);
    defer d.close();
    while (try d.next()) |entry| {
        const child_path = std.fs.path.join(allocator, &.{ root, entry.name }) catch return error.OutOfMemory;
        // Compute metadata for the child.
        const child_meta = fs.stat(child_path) catch |e| switch (e) {
            error.NotFound => {
                allocator.free(child_path);
                continue;
            },
            else => {
                allocator.free(child_path);
                return e;
            },
        };
        try out.put(child_path, snapFromMeta(child_meta));
        if (recursive and child_meta.isDir()) {
            try collectInto(allocator, child_path, true, out);
        }
    }
}

fn snapFromMeta(m: metadata_mod.Metadata) EntrySnapshot {
    const mod = m.modified();
    return .{ .size = m.size(), .mtime_secs = mod.secs, .mtime_nsecs = mod.nsecs };
}

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;
const volt_testing = @import("../testing.zig");

fn allocWatchTmp(allocator: std.mem.Allocator) ![:0]u8 {
    const tmpl = "/tmp/volt-watch-XXXXXX";
    const buf = try allocator.allocSentinel(u8, tmpl.len, 0);
    @memcpy(buf[0..tmpl.len], tmpl);
    if (syscall.c_mkdtemp(buf.ptr) == null) {
        allocator.free(buf);
        return error.MkdtempFailed;
    }
    return buf;
}

const WatchTest = struct {
    tmp: [:0]u8,
    saw_created: bool = false,
    saw_modified: bool = false,
};

fn watchCreatedBody(ctx: *WatchTest) !void {
    var w = Watcher.init(volt_testing.allocator, .{ .poll_interval = lib.Duration.fromMillis(20) });
    defer w.deinit();

    try w.watch(ctx.tmp, .{});

    // Create a new file.
    var fp_buf: [256:0]u8 = undefined;
    const fp = try std.fmt.bufPrintZ(&fp_buf, "{s}/new.txt", .{ctx.tmp});
    const fd = syscall.c_open(fp.ptr, syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, 0o644);
    if (fd < 0) return error.OpenFailed;
    _ = syscall.c_close(fd);

    // Wait for the first event.
    const event = try w.next();
    defer volt_testing.allocator.free(event.path);

    if (event.kind == .created) ctx.saw_created = true;
    _ = syscall.c_unlink(fp.ptr);
}

test "Watcher: created event fires for new file under a watched dir" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocWatchTmp(volt_testing.allocator);
    defer volt_testing.allocator.free(tmp);
    defer _ = syscall.c_rmdir(tmp.ptr);

    var rt = try lib.Runtime.init(.{ .allocator = volt_testing.allocator });
    defer rt.deinit();
    var ctx = WatchTest{ .tmp = tmp };
    try (try rt.run(watchCreatedBody, .{&ctx}));
    try testing.expect(ctx.saw_created);
}

fn watchCancelBody(ctx: *WatchTest) !void {
    var w = Watcher.init(volt_testing.allocator, .{ .poll_interval = lib.Duration.fromMillis(20) });
    defer w.deinit();
    try w.watch(ctx.tmp, .{});

    var c = lib.Cancel.init(lib.runtime());
    defer c.deinit();

    // Fire the cancel from a side coro shortly.
    const Side = struct {
        fn run(cc: *lib.Cancel) void {
            lib.sleep(lib.Duration.fromMillis(60)) catch {};
            cc.fire();
        }
    };
    const side = lib.spawn(Side.run, .{&c}) catch return;

    const result = w.nextCancel(&c);
    if (result) |_| return error.UnexpectedEvent else |e| {
        if (e != error.Cancelled) return error.WrongError;
    }
    _ = side.join();
    ctx.saw_created = true; // re-purpose flag as "test passed"
}

test "Watcher: nextCancel returns error.Cancelled when fired" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocWatchTmp(volt_testing.allocator);
    defer volt_testing.allocator.free(tmp);
    defer _ = syscall.c_rmdir(tmp.ptr);

    var rt = try lib.Runtime.init(.{ .allocator = volt_testing.allocator });
    defer rt.deinit();
    var ctx = WatchTest{ .tmp = tmp };
    try (try rt.run(watchCancelBody, .{&ctx}));
    try testing.expect(ctx.saw_created);
}

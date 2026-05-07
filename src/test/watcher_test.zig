//! P3.x.5 — Watcher integration test.
//!
//! Exercises the Darwin path (kqueue EVFILT_VNODE on a watched
//! directory). Linux path needs a Linux runner — same shape, native
//! per-file granularity.
//!
//! Pattern: open watcher on a temp dir, spawn a coroutine that
//! creates a file inside, expect the watcher's next() to return
//! an event.

const std = @import("std");
const builtin = @import("builtin");
const volt = @import("../lib.zig");

const WatchCtx = struct {
    dir: []const u8,
    file: []const u8,
    got_event: bool = false,
};

fn rmRf(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
    _ = std.posix.system.rmdir(path_z.ptr);
}

fn watcherCoro(ctx: *WatchCtx) !void {
    var w = try volt.fs.Watcher.open(ctx.dir);
    defer w.deinit();

    // Yield once so the trigger coro has a chance to register the
    // file change. With the reactor edge-triggered mode, our next()
    // call still wakes on subsequent changes, but ordering matters
    // for the test.
    try volt.yield();

    if (try w.next()) |ev| {
        // Any event counts as a pass — Darwin's coarse-grained
        // mode reports `.modified` for the directory; Linux gives
        // us `.created` with name="touch.txt".
        _ = ev;
        ctx.got_event = true;
    }
}

fn triggerCoro(ctx: *WatchCtx) !void {
    // Wait briefly so the watcher registers first.
    try volt.yield();
    try volt.yield();
    try volt.yield();

    var f = try volt.fs.File.create(ctx.file);
    try f.writeAll("touch");
    f.close();
}

fn watcherRoot(ctx: *WatchCtx) !void {
    rmRf(ctx.file);
    rmRf(ctx.dir);

    try volt.fs.tree.makeDir(ctx.dir, 0o755);
    defer rmRf(ctx.dir);
    defer rmRf(ctx.file);

    var watcher = try volt.spawn(watcherCoro, .{ctx});
    defer volt.destroyTask(watcher);
    var trigger = try volt.spawn(triggerCoro, .{ctx});
    defer volt.destroyTask(trigger);

    try trigger.join();
    try watcher.join();
}

test "P3.x.5: Watcher fires an event on file creation in watched dir" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos and builtin.os.tag != .ios and
        builtin.os.tag != .tvos and builtin.os.tag != .watchos)
    {
        return error.SkipZigTest;
    }

    var dir_buf: [128]u8 = undefined;
    var file_buf: [256]u8 = undefined;
    const pid = std.posix.system.getpid();
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/volt-w-{d}", .{pid});
    const file = try std.fmt.bufPrint(&file_buf, "{s}/touch.txt", .{dir});

    var ctx = WatchCtx{ .dir = dir, .file = file };
    try volt.run(.{ .allocator = std.testing.allocator }, watcherRoot, .{&ctx});
    try std.testing.expect(ctx.got_event);
}

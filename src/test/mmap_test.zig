//! P4 — Mmap integration tests.

const std = @import("std");
const volt = @import("../lib.zig");

fn cleanup(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

// ─────────────────────────────────────────────────────────────────────
// P4.A — mapFile + slice round-trip
// ─────────────────────────────────────────────────────────────────────

const MapCtx = struct {
    path: []const u8,
    payload: []const u8,
    seen: [64]u8 = undefined,
    seen_len: usize = 0,
};

fn mapFileRoot(ctx: *MapCtx) !void {
    cleanup(ctx.path);
    {
        var f = try volt.fs.File.create(ctx.path);
        try f.writeAll(ctx.payload);
        f.close();
    }
    defer cleanup(ctx.path);

    var f = try volt.fs.File.open(ctx.path, .{ .read = true });
    defer f.close();

    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_only,
        .perms = .{ .read = true },
    });
    defer m.deinit();

    const view = m.sliceConst();
    @memcpy(ctx.seen[0..view.len], view);
    ctx.seen_len = view.len;
}

test "P4.A: Mmap.mapFile slice() yields the file's bytes" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mmap-{d}.txt", .{std.posix.system.getpid()});
    var ctx = MapCtx{ .path = path, .payload = "memory-mapped payload" };
    try volt.run(.{ .allocator = std.testing.allocator }, mapFileRoot, .{&ctx});
    try std.testing.expectEqualStrings(ctx.payload, ctx.seen[0..ctx.seen_len]);
}

// ─────────────────────────────────────────────────────────────────────
// P4.A — anonymous mmap is read/write-able
// ─────────────────────────────────────────────────────────────────────

fn anonRoot() !void {
    var m = try volt.fs.Mmap.anonymous(4096, .{
        .perms = .{ .read = true, .write = true, .exec = false },
    });
    defer m.deinit();

    const buf = m.slice();
    @memset(buf, 0);
    buf[0] = 'V';
    buf[1] = 'o';
    buf[2] = 'l';
    buf[3] = 't';
    try std.testing.expectEqualStrings("Volt", buf[0..4]);
}

test "P4.A: Mmap.anonymous gives a writable region" {
    try volt.run(.{ .allocator = std.testing.allocator }, anonRoot, .{});
}

// ─────────────────────────────────────────────────────────────────────
// P4.B — advise / flush / protect smoke tests
// ─────────────────────────────────────────────────────────────────────

fn opsRoot(path: []const u8) !void {
    cleanup(path);
    {
        var f = try volt.fs.File.create(path);
        try f.writeAll("operations payload " ** 64); // ~1.2 KiB
        f.close();
    }
    defer cleanup(path);

    var f = try volt.fs.File.open(path, .{ .read = true, .write = true });
    defer f.close();

    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_write,
        .perms = .{ .read = true, .write = true },
    });
    defer m.deinit();

    const range = volt.fs.MmapRange{ .offset = 0, .length = m.len };

    // advise: hint sequential access. Should succeed without error.
    try m.advise(range, .will_need);
    try m.advise(range, .sequential);

    // Mutate a byte then flush async.
    m.slice()[0] = 'X';
    try m.flush(range, .async_);

    // Make the mapping read-only via protect; reading still works.
    try m.protect(range, .{ .read = true, .write = false });
    try std.testing.expectEqual(@as(u8, 'X'), m.sliceConst()[0]);
}

test "P4.B: advise / flush / protect" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mops-{d}.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, opsRoot, .{path});
}

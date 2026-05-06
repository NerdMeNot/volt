//! P3.A — File integration tests via the runtime.
//!
//! Round-trip an open / write / read / seek / metadata sequence;
//! exercise positional I/O; verify the trait surface composes with
//! BufReader and copy().

const std = @import("std");
const volt = @import("../lib.zig");

fn unique_path(buf: []u8, prefix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}.tmp", .{ prefix, std.posix.system.getpid() });
}

fn cleanup(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

// ─────────────────────────────────────────────────────────────────────
// Test 1 — open / write / read round-trip
// ─────────────────────────────────────────────────────────────────────

const RoundtripResult = struct {
    written: usize,
    read_back: [64]u8,
    read_back_len: usize,
    size_after_write: u64,
};

fn writeAndReadRoot(path: []const u8) !RoundtripResult {
    cleanup(path);

    var f = try volt.fs.File.create(path);
    defer cleanup(path);
    try f.writeAll("Volt fs.File round-trip");
    const meta = try f.metadata();
    f.close();

    var f2 = try volt.fs.File.openRead(path);
    defer f2.close();
    var buf: [64]u8 = undefined;
    const n = try f2.read(&buf);

    var result = RoundtripResult{
        .written = "Volt fs.File round-trip".len,
        .read_back = undefined,
        .read_back_len = n,
        .size_after_write = meta.size,
    };
    @memcpy(result.read_back[0..n], buf[0..n]);
    return result;
}

test "P3.A: File open / writeAll / read round-trip" {
    var path_buf: [128]u8 = undefined;
    const path = try unique_path(&path_buf, "/tmp/volt-fs-rt");
    const r = try volt.run(.{ .allocator = std.testing.allocator }, writeAndReadRoot, .{path});
    try std.testing.expectEqualStrings("Volt fs.File round-trip", r.read_back[0..r.read_back_len]);
    try std.testing.expectEqual(@as(u64, 23), r.size_after_write);
}

// ─────────────────────────────────────────────────────────────────────
// Test 2 — pread / pwrite (positional I/O)
// ─────────────────────────────────────────────────────────────────────

fn positionalIoRoot(path: []const u8) ![16]u8 {
    cleanup(path);

    var f = try volt.fs.File.open(path, .{ .read = true, .write = true, .create = true, .truncate = true });
    defer f.close();
    defer cleanup(path);

    // Write at offset 0, then positional-write at offset 5 — middle gap stays zero.
    _ = try f.pwrite("alpha", 0);
    _ = try f.pwrite("ZZZ", 5);

    // pread back from offset 0; expect "alphaZZZ" (8 bytes total).
    var buf: [16]u8 = .{0} ** 16;
    _ = try f.pread(buf[0..8], 0);
    return buf;
}

test "P3.A: File pread / pwrite positional I/O" {
    var path_buf: [128]u8 = undefined;
    const path = try unique_path(&path_buf, "/tmp/volt-fs-pio");
    const buf = try volt.run(.{ .allocator = std.testing.allocator }, positionalIoRoot, .{path});
    try std.testing.expectEqualStrings("alphaZZZ", buf[0..8]);
}

// ─────────────────────────────────────────────────────────────────────
// Test 3 — File.reader() composes with BufReader
// ─────────────────────────────────────────────────────────────────────

fn fileBufReaderRoot(path: []const u8) ![32]u8 {
    cleanup(path);

    {
        var f = try volt.fs.File.create(path);
        defer f.close();
        try f.writeAll("line one\nline two\n");
    }

    var f = try volt.fs.File.openRead(path);
    defer f.close();
    defer cleanup(path);

    var br = try volt.io.BufReader.init(std.testing.allocator, f.reader(), 8);
    defer br.deinit();

    var line_buf: [32]u8 = .{0} ** 32;
    const n = try br.readUntil('\n', &line_buf);
    var result: [32]u8 = .{0} ** 32;
    @memcpy(result[0..n], line_buf[0..n]);
    return result;
}

test "P3.A: File.reader() composes with BufReader for line parsing" {
    var path_buf: [128]u8 = undefined;
    const path = try unique_path(&path_buf, "/tmp/volt-fs-br");
    const buf = try volt.run(.{ .allocator = std.testing.allocator }, fileBufReaderRoot, .{path});
    try std.testing.expectEqualStrings("line one", std.mem.sliceTo(&buf, 0));
}

// ─────────────────────────────────────────────────────────────────────
// Test 4 — getEndPos / seekTo
// ─────────────────────────────────────────────────────────────────────

fn seekRoot(path: []const u8) !u64 {
    cleanup(path);

    var f = try volt.fs.File.create(path);
    defer f.close();
    defer cleanup(path);
    try f.writeAll("0123456789");

    const end = try f.getEndPos();
    try f.seekTo(3);
    const pos = try f.getPos();
    try std.testing.expectEqual(@as(u64, 3), pos);
    return end;
}

test "P3.A: File.seek / getEndPos" {
    var path_buf: [128]u8 = undefined;
    const path = try unique_path(&path_buf, "/tmp/volt-fs-seek");
    const end = try volt.run(.{ .allocator = std.testing.allocator }, seekRoot, .{path});
    try std.testing.expectEqual(@as(u64, 10), end);
}

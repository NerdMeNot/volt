//! `volt.fs` — async filesystem facade.
//!
//! `readFile` and `writeFile` are convenience wrappers over the
//! `volt.fs.File` handle. As of P3.E (v1.1), they're built on top
//! of `File`, NOT bypassing it via raw syscalls — pre-allocates
//! based on file size, then loops `read` / `writeAll`. Calling
//! coroutine parks during the underlying blocking-pool dispatch.
//!
//! For streaming patterns (large files, line iteration, framed
//! protocol bodies), reach for `File` directly + `BufReader` /
//! `lineIterator`. `readFile` slurps the whole file; bounded by
//! the 1 GiB safety cap.

const std = @import("std");
const File = @import("File.zig").File;

const MAX_FILE_SIZE: usize = 1 * 1024 * 1024 * 1024; // 1 GiB safety cap

/// Read the entire file at `path` into a freshly-allocated slice.
/// Caller owns the returned slice.
pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var f = try File.openRead(path);
    defer f.close();

    const meta = try f.metadata();
    const size_u64 = meta.size;
    if (size_u64 > MAX_FILE_SIZE) return error.FileTooLarge;
    const size: usize = @intCast(size_u64);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var got: usize = 0;
    while (got < size) {
        const n = try f.read(buf[got..]);
        if (n == 0) break; // truncated since fstat — return what we got
        got += n;
    }
    if (got < size) {
        return allocator.realloc(buf, got);
    }
    return buf;
}

/// Write `data` to `path`, creating or truncating the file.
pub fn writeFile(path: []const u8, data: []const u8) !void {
    var f = try File.create(path);
    defer f.close();
    try f.writeAll(data);
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn rwRoundTripRoot() !void {
    const tmp_path = "/tmp/volt_fs_test.txt";
    const data = "hello volt fs\n";

    try writeFile(tmp_path, data);
    const read_back = try readFile(std.testing.allocator, tmp_path);
    defer std.testing.allocator.free(read_back);

    try std.testing.expectEqualStrings(data, read_back);
    var z_buf: [128]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{tmp_path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

test "fs: writeFile + readFile round-trip via File facade" {
    try volt.run(.{ .allocator = std.testing.allocator }, rwRoundTripRoot, .{});
}

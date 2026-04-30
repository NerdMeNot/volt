//! `volt.fs` — async filesystem operations.
//!
//! v0.7 first cut: `readFile` and `writeFile`, both wrapping blocking
//! posix syscalls via `spawnBlocking`. The blocking pool keeps the
//! filesystem call off the worker thread; the calling coroutine parks
//! and resumes when the syscall returns.
//!
//! v0.9+ Linux build will route these through io_uring when the kernel
//! supports it (kernel 5.6+ for IORING_OP_OPENAT/READ/WRITE), keeping
//! the same public API.

const std = @import("std");
const posix = std.posix;
const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;

const MAX_FILE_SIZE: usize = 1 * 1024 * 1024 * 1024; // 1 GiB safety cap

const ReadFileArgs = struct {
    allocator: std.mem.Allocator,
    path: [:0]const u8,
};

fn readFileBlocking(args: *ReadFileArgs) ![]u8 {
    const fd = try posix.openatZ(posix.AT.FDCWD, args.path.ptr, .{ .ACCMODE = .RDONLY }, 0);
    defer syscall.close(fd);

    // Read into a growing buffer; we don't fstat (avoid the syscall
    // and a stale-size race) — read until EOF.
    var buf: std.array_list.Managed(u8) = std.array_list.Managed(u8).init(args.allocator);
    errdefer buf.deinit();
    var chunk: [64 * 1024]u8 = undefined;
    while (true) {
        const n = try syscall.read(fd, &chunk);
        if (n == 0) break;
        if (buf.items.len + n > MAX_FILE_SIZE) return error.FileTooLarge;
        try buf.appendSlice(chunk[0..n]);
    }
    return buf.toOwnedSlice();
}

/// Read the entire file at `path` into a freshly-allocated slice.
/// The calling coroutine parks while the read completes on the
/// blocking pool; the worker thread is free to run other coroutines.
///
/// `path` is null-terminated for direct use with `openZ`. Caller owns
/// the returned slice.
pub fn readFile(allocator: std.mem.Allocator, path: [:0]const u8) ![]u8 {
    var args = ReadFileArgs{ .allocator = allocator, .path = path };
    return try spawnBlocking(readFileBlocking, .{&args});
}

const WriteFileArgs = struct {
    path: [:0]const u8,
    data: []const u8,
};

fn writeFileBlocking(args: *WriteFileArgs) !void {
    const fd = try posix.openatZ(
        posix.AT.FDCWD,
        args.path.ptr,
        .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true },
        0o644,
    );
    defer syscall.close(fd);

    var written: usize = 0;
    while (written < args.data.len) {
        const n = try syscall.write(fd, args.data[written..]);
        if (n == 0) return error.WriteFailed;
        written += n;
    }
}

/// Write `data` to `path`, creating or truncating the file.
pub fn writeFile(path: [:0]const u8, data: []const u8) !void {
    var args = WriteFileArgs{ .path = path, .data = data };
    return try spawnBlocking(writeFileBlocking, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn rwRoundTripRoot() !void {
    const tmp_path: [:0]const u8 = "/tmp/volt_fs_test.txt";
    const data = "hello volt fs\n";

    try writeFile(tmp_path, data);
    const read_back = try readFile(std.testing.allocator, tmp_path);
    defer std.testing.allocator.free(read_back);

    try std.testing.expectEqualStrings(data, read_back);
    // Best-effort cleanup; ignore failures.
    _ = std.posix.system.unlink(tmp_path.ptr);
}

test "fs: writeFile + readFile round-trip" {
    try volt.run(.{ .allocator = std.testing.allocator }, rwRoundTripRoot, .{});
}

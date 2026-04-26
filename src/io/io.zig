//! `volt.io.read` / `write` — coroutine-aware fd I/O.
//!
//! Performs a non-blocking read or write. On `WouldBlock`, parks the current
//! coroutine on reactor readiness and retries when woken.
//!
//! These wrappers assume the fd has already been put in non-blocking mode by
//! the caller (typically via `volt.io.setNonblock(fd)`). Future versions may
//! detect that automatically.

const std = @import("std");
const posix = std.posix;
const syscall = @import("../internal/syscall.zig");
const wait = @import("wait.zig");

pub const ReadError = syscall.ReadError || wait.WaitError;
pub const WriteError = syscall.WriteError || wait.WaitError;

/// Async read. Returns 0 only on EOF; otherwise the bytes copied.
pub fn read(fd: posix.fd_t, buf: []u8) ReadError!usize {
    while (true) {
        const r = syscall.read(fd, buf) catch |err| switch (err) {
            error.WouldBlock => {
                try wait.waitReadable(fd);
                continue;
            },
            else => return err,
        };
        return r;
    }
}

/// Async write. Returns the number of bytes written (may be less than buf.len
/// for streams under backpressure; callers that need full delivery should use
/// `writeAll`).
pub fn write(fd: posix.fd_t, buf: []const u8) WriteError!usize {
    while (true) {
        const r = syscall.write(fd, buf) catch |err| switch (err) {
            error.WouldBlock => {
                try wait.waitWritable(fd);
                continue;
            },
            else => return err,
        };
        return r;
    }
}

/// Async write that doesn't return until all bytes are written or the fd
/// fails. Loops on partial writes.
pub fn writeAll(fd: posix.fd_t, buf: []const u8) WriteError!void {
    var written: usize = 0;
    while (written < buf.len) {
        written += try write(fd, buf[written..]);
    }
}

/// Put `fd` into non-blocking mode (O_NONBLOCK). Idempotent.
pub fn setNonblock(fd: posix.fd_t) syscall.FcntlError!void {
    const F = posix.system.F;
    const flags_raw = try syscall.fcntl(fd, F.GETFL, 0);
    const O_NONBLOCK: u32 = @bitCast(posix.O{ .NONBLOCK = true });
    if ((flags_raw & O_NONBLOCK) != 0) return; // already set
    _ = try syscall.fcntl(fd, F.SETFL, flags_raw | O_NONBLOCK);
}

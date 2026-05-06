//! `volt.io.lowlevel.read` / `write` / `writeAll` — coroutine-aware fd I/O.
//!
//! These are the low-level fd-taking primitives. Most application code
//! should reach for `TcpStream.read` / `writeAll` (or filesystem
//! equivalents) instead — those wrap these calls with the typed handle.
//!
//! Performs a non-blocking read or write. On `WouldBlock`, parks the current
//! coroutine on reactor readiness and retries when woken.
//!
//! Assumes the fd has already been put in non-blocking mode by the caller
//! (typically via `volt.io.lowlevel.setNonblock(fd)`).

const std = @import("std");
const posix = std.posix;
const syscall = @import("../internal/syscall.zig");
const wait = @import("wait.zig");
const io_errors = @import("errors.zig");
const traits = @import("traits/traits.zig");

pub const ReadError = io_errors.ReadError;
pub const WriteError = io_errors.WriteError;
pub const FcntlError = io_errors.FcntlError;

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

/// Put `fd` into non-blocking mode (O_NONBLOCK). Idempotent — returns early
/// if the flag is already set.
pub fn setNonblock(fd: posix.fd_t) FcntlError!void {
    const flags_raw = try syscall.fcntl(fd, posix.F.GETFL, 0);
    const nonblock_bits: u32 = @bitCast(posix.O{ .NONBLOCK = true });
    if ((flags_raw & nonblock_bits) != 0) return;
    _ = try syscall.fcntl(fd, posix.F.SETFL, flags_raw | nonblock_bits);
}

// ─────────────────────────────────────────────────────────────────────
// Fd — generic non-blocking-fd-as-trait wrapper.
//
// Purpose: wrap a non-blocking fd Volt didn't open itself (a pipe,
// an inherited socketpair, an FFI fd) as a Reader/Writer/Closer.
// Useful for tests, FFI integrations, and the bench harnesses.
//
// Lifetime: Fd does NOT take ownership at init. `closer().close()`
// DOES call close(), so don't hand a Closer for a borrowed fd. If
// you only need read/write, skip closer() entirely.
//
// The fd MUST already be in non-blocking mode — the trait's read /
// write delegate to volt.io.lowlevel.{read,write}, which assume
// non-blocking and park on EAGAIN. Use setNonblock() before wrapping.
// ─────────────────────────────────────────────────────────────────────

pub const Fd = struct {
    fd: posix.fd_t,

    pub fn init(fd: posix.fd_t) Fd {
        return .{ .fd = fd };
    }

    pub fn reader(self: *Fd) traits.Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &reader_vtable };
    }

    pub fn writer(self: *Fd) traits.Writer {
        return .{ .ctx = @ptrCast(self), .vtable = &writer_vtable };
    }

    pub fn closer(self: *Fd) traits.Closer {
        return .{ .ctx = @ptrCast(self), .vtable = &closer_vtable };
    }

    const reader_vtable: traits.Reader.VTable = .{
        .read = &traitRead,
        .as_fd = &traitAsFd,
    };

    const writer_vtable: traits.Writer.VTable = .{
        .write = &traitWrite,
        .as_fd = &traitAsFd,
    };

    const closer_vtable: traits.Closer.VTable = .{ .close = &traitClose };

    fn traitRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
        const self: *Fd = @ptrCast(@alignCast(ctx));
        return read(self.fd, buf);
    }

    fn traitWrite(ctx: *anyopaque, buf: []const u8) WriteError!usize {
        const self: *Fd = @ptrCast(@alignCast(ctx));
        return write(self.fd, buf);
    }

    fn traitClose(ctx: *anyopaque) void {
        const self: *Fd = @ptrCast(@alignCast(ctx));
        syscall.close(self.fd);
    }

    fn traitAsFd(ctx: *anyopaque) ?posix.fd_t {
        const self: *Fd = @ptrCast(@alignCast(ctx));
        return self.fd;
    }
};

test "Fd: trait surface compiles" {
    var f: Fd = .init(-1);
    const r = f.reader();
    const w = f.writer();
    const c = f.closer();
    try std.testing.expect(r.vtable.as_fd != null);
    try std.testing.expect(w.vtable.as_fd != null);
    _ = c;
}

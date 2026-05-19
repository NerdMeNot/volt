//! POSIX I/O helpers shared across the kqueue, epoll, and io_uring
//! reactors.
//!
//! Each reactor backend used to carry its own copy of these externs
//! and helpers (`read`/`write`/`fcntl`/`setNonblock`/`readAsync`/
//! `writeAsync`/`readFull`/`writeAll`) — four identical files
//! drifting independently. Centralised here so a fix is applied
//! once.
//!
//! Per-OS values (errno accessor, `EAGAIN`, `O_NONBLOCK`) are
//! comptime-switched on `builtin.os.tag`. The reactor-facing helpers
//! (`readAsync` etc.) take the reactor as `anytype` so each backend
//! stays free of a shared base type while the syscall plumbing is
//! identical.

const std = @import("std");
const builtin = @import("builtin");

// ─── libc externs ────────────────────────────────────────────────

pub extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
pub extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
pub extern "c" fn close(fd: c_int) c_int;
pub extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;

// `errno` lives in different symbols per libc. Darwin (and BSDs)
// expose `__error()`; glibc uses `__errno_location`. Both return
// `*c_int`.
const c_error = if (builtin.os.tag.isDarwin() or
    builtin.os.tag == .freebsd or
    builtin.os.tag == .openbsd or
    builtin.os.tag == .netbsd or
    builtin.os.tag == .dragonfly)
    @extern(*const fn () callconv(.c) *c_int, .{ .name = "__error" })
else
    @extern(*const fn () callconv(.c) *c_int, .{ .name = "__errno_location" });

pub inline fn errnoVal() c_int {
    return c_error().*;
}

// ─── Per-OS constants ────────────────────────────────────────────

pub const F_GETFL: c_int = 3;
pub const F_SETFL: c_int = 4;

pub const O_NONBLOCK: c_int = switch (builtin.os.tag) {
    .linux => 0o4000,
    .macos, .ios, .tvos, .watchos => 4,
    .freebsd, .openbsd, .netbsd, .dragonfly => 4,
    else => @compileError("O_NONBLOCK not defined for this OS"),
};

pub const EAGAIN: c_int = switch (builtin.os.tag) {
    .linux => 11,
    .macos, .ios, .tvos, .watchos => 35,
    .freebsd, .openbsd, .netbsd, .dragonfly => 35,
    else => @compileError("EAGAIN not defined for this OS"),
};

// ─── Helpers ─────────────────────────────────────────────────────

pub fn setNonblock(fd: i32) !void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

/// Read up to `buf.len` bytes from `fd`, yielding to `rx` on
/// `EAGAIN`. `rx` is taken as `anytype` so each reactor backend can
/// supply its own concrete type without a shared base class — the
/// only requirement is a `waitReadable(fd: i32)` method.
pub fn readAsync(rx: anytype, fd: i32, buf: []u8) !usize {
    while (true) {
        const r = read(@intCast(fd), buf.ptr, buf.len);
        if (r >= 0) return @intCast(r);
        if (errnoVal() != EAGAIN) return error.ReadFailed;
        rx.waitReadable(fd);
    }
}

pub fn writeAsync(rx: anytype, fd: i32, buf: []const u8) !usize {
    while (true) {
        const w = write(@intCast(fd), buf.ptr, buf.len);
        if (w >= 0) return @intCast(w);
        if (errnoVal() != EAGAIN) return error.WriteFailed;
        rx.waitWritable(fd);
    }
}

/// Read EXACTLY `buf.len` bytes (loop until EOF or full). Returns the
/// total bytes read — may be < buf.len if the peer closed early.
pub fn readFull(rx: anytype, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

/// Write EXACTLY `buf.len` bytes (loop on partial writes).
pub fn writeAll(rx: anytype, fd: i32, buf: []const u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, buf[total..]);
        total += w;
    }
}

comptime {
    if (builtin.os.tag == .windows) {
        @compileError("reactor_posix.zig is POSIX-only; Windows uses reactor_iocp.zig directly");
    }
}

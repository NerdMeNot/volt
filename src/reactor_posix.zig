//! POSIX I/O helpers shared across the kqueue, epoll, and io_uring
//! reactors.
//!
//! Each reactor backend used to carry its own copy of these externs
//! and helpers (`read`/`write`/`fcntl`/`setNonblock`/`readAsync`/
//! `writeAsync`/`readFull`/`writeAll`) — four identical files
//! drifting independently. Centralised here so a fix is applied
//! once.
//!
//! Per-OS values (errno accessor, `EAGAIN`, `O_NONBLOCK`, the
//! `errno → IoError` mapping) are comptime-switched on
//! `builtin.os.tag`. The reactor-facing helpers (`readAsync` etc.)
//! take the reactor as `anytype` so each backend stays free of a
//! shared base type while the syscall plumbing is identical.

const std = @import("std");
const builtin = @import("builtin");
const reactor_mod = @import("reactor.zig");
const poll_desc = @import("poll_desc.zig");

pub const IoError = reactor_mod.IoError;
pub const ReactorSetupError = reactor_mod.ReactorSetupError;
pub const ReactorWaitError = reactor_mod.ReactorWaitError;

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

// Errno values used for the IoError mapping. Linux glibc + Darwin
// disagree on the numbers; we keep both branches in one place.
pub const Errno = switch (builtin.os.tag) {
    .linux => struct {
        pub const ECONNRESET: c_int = 104;
        pub const EPIPE: c_int = 32;
        pub const ECONNABORTED: c_int = 103;
        pub const ENOTCONN: c_int = 107;
        pub const ECONNREFUSED: c_int = 111;
        pub const ETIMEDOUT: c_int = 110;
        pub const EBADF: c_int = 9;
        pub const ENOTSOCK: c_int = 88;
        pub const EMFILE: c_int = 24;
        pub const ENFILE: c_int = 23;
        pub const ENOMEM: c_int = 12;
        pub const ENOBUFS: c_int = 105;
        pub const EINTR: c_int = 4;
    },
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => struct {
        pub const ECONNRESET: c_int = 54;
        pub const EPIPE: c_int = 32;
        pub const ECONNABORTED: c_int = 53;
        pub const ENOTCONN: c_int = 57;
        pub const ECONNREFUSED: c_int = 61;
        pub const ETIMEDOUT: c_int = 60;
        pub const EBADF: c_int = 9;
        pub const ENOTSOCK: c_int = 38;
        pub const EMFILE: c_int = 24;
        pub const ENFILE: c_int = 23;
        pub const ENOMEM: c_int = 12;
        pub const ENOBUFS: c_int = 55;
        pub const EINTR: c_int = 4;
    },
    else => @compileError("Errno table missing for this OS"),
};

/// Translate a POSIX errno into the categorical `IoError` set.
/// Unknown values collapse to `error.Unexpected`. Callers can
/// still `std.debug.print("errno={d}", .{e})` in their own
/// diagnostics path; this helper just produces the typed error.
pub fn errnoToIoError(e: c_int) IoError {
    return switch (e) {
        Errno.ECONNRESET => error.ConnectionReset,
        Errno.EPIPE => error.BrokenPipe,
        Errno.ECONNABORTED => error.ConnectionAborted,
        Errno.ENOTCONN => error.NotConnected,
        Errno.ECONNREFUSED => error.ConnectionRefused,
        Errno.ETIMEDOUT => error.Timeout,
        Errno.EBADF, Errno.ENOTSOCK => error.BadDescriptor,
        Errno.EMFILE, Errno.ENFILE => error.OutOfDescriptors,
        Errno.ENOMEM, Errno.ENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

/// Same mapping projected onto the smaller `ReactorSetupError`
/// set (no connection-state cases — those don't apply to register-
/// type syscalls). Resource-exhaustion + Unexpected only.
pub fn errnoToSetupError(e: c_int) ReactorSetupError {
    return switch (e) {
        Errno.EMFILE, Errno.ENFILE => error.OutOfDescriptors,
        Errno.ENOMEM, Errno.ENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

// ─── Helpers ─────────────────────────────────────────────────────

pub fn setNonblock(fd: i32) ReactorSetupError!void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return errnoToSetupError(errnoVal());
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return errnoToSetupError(errnoVal());
}

/// Read up to `buf.len` bytes from `fd`, yielding to `rx` on
/// `EAGAIN`. `rx` is taken as `anytype` so each reactor backend can
/// supply its own concrete type without a shared base class — the
/// only requirement is a `waitFd(fd, *PollDesc, Mode) !void` method
/// (the Step 2b PollDesc-aware reactor interface).
///
/// `pd` is the per-socket PollDesc the kqueue backend dispatches
/// kernel events to. Shim backends ignore it and fall back to their
/// per-wait registration path. Net.zig allocates one PollDesc per
/// socket and threads it through here.
///
/// Non-EAGAIN errnos map through `errnoToIoError` into the typed
/// `IoError` set. Library callers can match on `error.ConnectionReset`
/// vs `error.BrokenPipe` etc. instead of a single coarse
/// `ReadFailed`.
pub fn readAsync(rx: anytype, fd: i32, pd: *poll_desc.PollDesc, buf: []u8) IoError!usize {
    while (true) {
        const r = read(@intCast(fd), buf.ptr, buf.len);
        if (r >= 0) return @intCast(r);
        const e = errnoVal();
        if (e == EAGAIN) {
            try rx.waitFd(fd, pd, .read);
            continue;
        }
        if (e == Errno.EINTR) continue; // retry transparently
        return errnoToIoError(e);
    }
}

pub fn writeAsync(rx: anytype, fd: i32, pd: *poll_desc.PollDesc, buf: []const u8) IoError!usize {
    while (true) {
        const w = write(@intCast(fd), buf.ptr, buf.len);
        if (w >= 0) return @intCast(w);
        const e = errnoVal();
        if (e == EAGAIN) {
            try rx.waitFd(fd, pd, .write);
            continue;
        }
        if (e == Errno.EINTR) continue;
        return errnoToIoError(e);
    }
}

/// Read EXACTLY `buf.len` bytes (loop until EOF or full). Returns the
/// total bytes read — may be < buf.len if the peer closed early.
pub fn readFull(rx: anytype, fd: i32, pd: *poll_desc.PollDesc, buf: []u8) IoError!usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, pd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

/// Write EXACTLY `buf.len` bytes (loop on partial writes).
pub fn writeAll(rx: anytype, fd: i32, pd: *poll_desc.PollDesc, buf: []const u8) IoError!void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, pd, buf[total..]);
        total += w;
    }
}

comptime {
    if (builtin.os.tag == .windows) {
        @compileError("reactor_posix.zig is POSIX-only; Windows uses reactor_iocp.zig directly");
    }
}

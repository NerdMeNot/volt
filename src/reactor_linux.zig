//! Linux reactor dispatch — tagged union of epoll + io_uring.
//!
//! Linux is the only platform Volt ships with two backends. The
//! choice is `Runtime.Config.io_backend`:
//!
//!   * `.auto` (default) — probe for io_uring at `Runtime.init`;
//!     use it on Linux ≥ 5.10, fall back to epoll otherwise.
//!   * `.epoll` — force epoll.
//!   * `.io_uring` — force io_uring (errors if unavailable).
//!
//! The dispatch is a tagged union; method calls forward via a
//! runtime branch (one indirect jump per reactor op). If profiling
//! shows the dispatch overhead matters (>2% on bench-yield or
//! bench-tcp-echo), the alternative is a comptime build flag
//! (`-Dio_backend=epoll` / `-Dio_backend=io_uring`), forcing users
//! to pick at build time. For now we accept the runtime cost in
//! exchange for one binary that works on every Linux.
//!
//! All other platforms compile-pick a single backend in
//! `src/reactor.zig`; this dispatch wrapper exists only on Linux.

const std = @import("std");
const builtin = @import("builtin");
const epoll = @import("reactor_epoll.zig");
const io_uring = @import("reactor_io_uring.zig");

pub const Backend = enum { epoll, io_uring };

/// Runtime probe: does `io_uring_setup` succeed on this kernel?
/// Linux ≥ 5.10 (Tokio's compatibility floor) supports the ops
/// Volt uses. Older kernels or sysctl-disabled environments (some
/// distros disable io_uring) return false; we fall back to epoll.
pub fn probeIoUring() bool {
    var ring = std.os.linux.IoUring.init(8, 0) catch return false;
    ring.deinit();
    return true;
}

/// The Linux Reactor — tagged union dispatch over both backends.
/// Same 7-method surface as kqueue / IOCP / the stub backends.
pub const Reactor = union(Backend) {
    epoll: epoll.Reactor,
    io_uring: io_uring.Reactor,

    /// Default constructor matches the other platforms' shape — no
    /// args, no config-injection. `Runtime.init` calls
    /// `initBackend(backend)` directly when it wants to override.
    pub fn init() !Reactor {
        return initBackend(if (probeIoUring()) .io_uring else .epoll);
    }

    /// Explicit-backend constructor. Used by `Runtime.init` when
    /// `Config.io_backend != .auto`.
    pub fn initBackend(b: Backend) !Reactor {
        return switch (b) {
            .epoll => Reactor{ .epoll = try epoll.Reactor.init() },
            .io_uring => Reactor{ .io_uring = try io_uring.Reactor.init() },
        };
    }

    pub fn deinit(self: *Reactor) void {
        switch (self.*) {
            .epoll => |*r| r.deinit(),
            .io_uring => |*r| r.deinit(),
        }
    }

    pub fn waitReadable(self: *Reactor, fd: i32) void {
        switch (self.*) {
            .epoll => |*r| r.waitReadable(fd),
            .io_uring => |*r| r.waitReadable(fd),
        }
    }

    pub fn waitWritable(self: *Reactor, fd: i32) void {
        switch (self.*) {
            .epoll => |*r| r.waitWritable(fd),
            .io_uring => |*r| r.waitWritable(fd),
        }
    }

    pub fn waitTimer(self: *Reactor, ns: u64) void {
        switch (self.*) {
            .epoll => |*r| r.waitTimer(ns),
            .io_uring => |*r| r.waitTimer(ns),
        }
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return switch (self.*) {
            .epoll => |*r| r.pendingCount(),
            .io_uring => |*r| r.pendingCount(),
        };
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        return switch (self.*) {
            .epoll => |*r| r.poll(blocking),
            .io_uring => |*r| r.poll(blocking),
        };
    }
};

// ─── I/O helpers ─────────────────────────────────────────────────
//
// The I/O helpers (`setNonblock`, `readAsync`, etc.) don't depend on
// which reactor backend is active — both use the same libc
// `fcntl`/`read`/`write` syscalls. Forward to the epoll module's
// versions; they'd be identical if defined here.
//
// Wait — they take `*Reactor`, but the Reactor type is the tagged
// union here, not `epoll.Reactor`. The free-function helpers route
// through the union's `waitReadable` / `waitWritable` methods, so
// they need to be redefined locally.

const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const O_NONBLOCK: c_int = 0o4000;

extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;
extern "c" fn __errno_location() *c_int;

inline fn errnoVal() c_int {
    return __errno_location().*;
}

const EAGAIN: c_int = 11;

pub fn setNonblock(fd: i32) !void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) !usize {
    while (true) {
        const r = read(@intCast(fd), buf.ptr, buf.len);
        if (r >= 0) return @intCast(r);
        const e = errnoVal();
        if (e != EAGAIN) return error.ReadFailed;
        rx.waitReadable(fd);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) !usize {
    while (true) {
        const w = write(@intCast(fd), buf.ptr, buf.len);
        if (w >= 0) return @intCast(w);
        const e = errnoVal();
        if (e != EAGAIN) return error.WriteFailed;
        rx.waitWritable(fd);
    }
}

pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, buf[total..]);
        total += w;
    }
}

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_linux.zig is Linux-only");
    }
}

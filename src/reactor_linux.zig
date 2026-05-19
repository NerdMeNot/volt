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
///
/// Returns the actual error from `IoUring.init` on failure (rather
/// than coercing to `bool`), so callers can distinguish kernel-too-
/// old (`ENOSYS` → `error.SystemOutdated`), sysctl-disabled
/// (`EPERM` → `error.PermissionDenied`), and friends from a generic
/// "not available". The default `Reactor.init` discards the error
/// and falls back to epoll — but a user diagnosing "why is io_uring
/// not being used" can call `probeIoUring` directly to see.
pub fn probeIoUring() !void {
    var ring = try std.os.linux.IoUring.init(8, 0);
    ring.deinit();
}

/// The Linux Reactor — tagged union dispatch over both backends.
/// Same 7-method surface as kqueue / IOCP / the stub backends.
pub const Reactor = union(Backend) {
    epoll: epoll.Reactor,
    io_uring: io_uring.Reactor,

    /// Default constructor matches the other platforms' shape — no
    /// args, no config-injection. `Runtime.init` calls
    /// `initBackend(backend)` directly when it wants to override.
    /// Silently falls back to epoll if io_uring is unavailable;
    /// callers wanting the failure reason use `probeIoUring`.
    pub fn init() !Reactor {
        if (probeIoUring()) {
            return initBackend(.io_uring);
        } else |_| {
            return initBackend(.epoll);
        }
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

    pub fn waitReadable(self: *Reactor, fd: i32) posix_helpers.ReactorWaitError!void {
        return switch (self.*) {
            .epoll => |*r| r.waitReadable(fd),
            .io_uring => |*r| r.waitReadable(fd),
        };
    }

    pub fn waitWritable(self: *Reactor, fd: i32) posix_helpers.ReactorWaitError!void {
        return switch (self.*) {
            .epoll => |*r| r.waitWritable(fd),
            .io_uring => |*r| r.waitWritable(fd),
        };
    }

    pub fn waitTimer(self: *Reactor, ns: u64) posix_helpers.ReactorWaitError!void {
        return switch (self.*) {
            .epoll => |*r| r.waitTimer(ns),
            .io_uring => |*r| r.waitTimer(ns),
        };
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
// The I/O helpers don't care which backend is active — they call
// libc read/write and yield to `rx.waitReadable` / `.waitWritable`
// on EAGAIN. The shared POSIX helpers take the reactor as
// `anytype`, so the tagged-union `Reactor` here binds cleanly with
// no per-backend dispatch glue.
const posix_helpers = @import("reactor_posix.zig");
pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_linux.zig is Linux-only");
    }
}

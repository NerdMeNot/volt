//! Linux reactor dispatch — tagged union of epoll + io_uring.
//!
//! Linux is the only platform Volt ships with two backends. The
//! choice is `Runtime.Config.io_backend`:
//!
//!   * `.auto` (default) — use epoll. Same default as Go's runtime
//!     and Tokio: epoll is universally available since Linux 2.5
//!     and has 20+ years of mature production use, while io_uring's
//!     persistent-registration model (POLL_ADD_MULTI + cancellation
//!     drain) is younger and has subtler kernel-version-dependent
//!     behaviour. Volt's persistent-registration epoll backend
//!     (`reactor_epoll.zig`, Step 2f) is a direct port of Go's
//!     `runtime/netpoll_epoll.go`.
//!   * `.epoll` — explicit epoll (same as `.auto`).
//!   * `.io_uring` — explicit io_uring (the legacy per-wait
//!     `Step 2b` shim, retained for future migration work). Users
//!     opting in should be aware of the close-vs-register race
//!     documented as audit `§4A`.
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
    ///
    /// Defaults to epoll — the same default that Go's runtime and
    /// Tokio use on Linux. epoll is universally available since
    /// Linux 2.5 and is what Volt's persistent-registration Step
    /// 2f migration ports directly from Go's
    /// `runtime/netpoll_epoll.go`. io_uring's persistent-
    /// registration story (POLL_ADD_MULTI + cancel-drain) is
    /// younger; it can be opted into explicitly via
    /// `Config.io_backend = .io_uring`, which uses the legacy
    /// per-wait Step 2b shim with the close-vs-register race
    /// documented as audit §4A — caveat emptor.
    pub fn init(allocator: std.mem.Allocator) !Reactor {
        return initBackend(allocator, .epoll);
    }

    /// Explicit-backend constructor. Used by `Runtime.init` when
    /// `Config.io_backend != .auto`.
    pub fn initBackend(allocator: std.mem.Allocator, b: Backend) !Reactor {
        return switch (b) {
            .epoll => Reactor{ .epoll = try epoll.Reactor.init(allocator) },
            .io_uring => Reactor{ .io_uring = try io_uring.Reactor.init(allocator) },
        };
    }

    pub fn deinit(self: *Reactor) void {
        switch (self.*) {
            .epoll => |*r| r.deinit(),
            .io_uring => |*r| r.deinit(),
        }
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

    pub fn interrupt(self: *Reactor) void {
        switch (self.*) {
            .epoll => |*r| r.interrupt(),
            .io_uring => |*r| r.interrupt(),
        }
    }

    pub fn cancelCoro(self: *Reactor, c: *@import("coroutine.zig").Coroutine) void {
        switch (self.*) {
            .epoll => |*r| r.cancelCoro(c),
            .io_uring => |*r| r.cancelCoro(c),
        }
    }

    pub fn closeFd(self: *Reactor, fd: i32) void {
        switch (self.*) {
            .epoll => |*r| r.closeFd(fd),
            .io_uring => |*r| r.closeFd(fd),
        }
    }

    pub fn waitTimerCancel(
        self: *Reactor,
        ns: u64,
        c: *@import("cancel.zig").Cancel,
    ) (posix_helpers.ReactorWaitError || @import("cancel.zig").Error)!void {
        return switch (self.*) {
            .epoll => |*r| r.waitTimerCancel(ns, c),
            .io_uring => |*r| r.waitTimerCancel(ns, c),
        };
    }

    // ─── PollDesc-aware interface ────────────────────────────────────

    pub fn registerFd(
        self: *Reactor,
        fd: i32,
        pd: *@import("poll_desc.zig").PollDesc,
    ) posix_helpers.ReactorWaitError!void {
        return switch (self.*) {
            .epoll => |*r| r.registerFd(fd, pd),
            .io_uring => |*r| r.registerFd(fd, pd),
        };
    }

    pub fn unregisterFd(
        self: *Reactor,
        fd: i32,
        pd: *@import("poll_desc.zig").PollDesc,
    ) void {
        switch (self.*) {
            .epoll => |*r| r.unregisterFd(fd, pd),
            .io_uring => |*r| r.unregisterFd(fd, pd),
        }
    }

    pub fn waitFd(
        self: *Reactor,
        fd: i32,
        pd: *@import("poll_desc.zig").PollDesc,
        mode: @import("poll_desc.zig").Mode,
    ) posix_helpers.ReactorWaitError!void {
        return switch (self.*) {
            .epoll => |*r| r.waitFd(fd, pd, mode),
            .io_uring => |*r| r.waitFd(fd, pd, mode),
        };
    }

    pub fn waitFdCancel(
        self: *Reactor,
        fd: i32,
        pd: *@import("poll_desc.zig").PollDesc,
        mode: @import("poll_desc.zig").Mode,
        c: *@import("cancel.zig").Cancel,
    ) (posix_helpers.ReactorWaitError || @import("cancel.zig").Error)!void {
        return switch (self.*) {
            .epoll => |*r| r.waitFdCancel(fd, pd, mode, c),
            .io_uring => |*r| r.waitFdCancel(fd, pd, mode, c),
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

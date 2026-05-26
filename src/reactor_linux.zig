//! Linux reactor dispatch — tagged union of epoll + io_uring.
//!
//! **The public Linux backend is epoll.** `Runtime.Config` has no
//! `io_backend` knob; `Reactor.init` always returns the epoll
//! variant. This matches Go's runtime and Tokio (both epoll on
//! Linux): epoll has 20+ years of production maturity, while
//! io_uring's persistent-registration model (POLL_ADD_MULTI +
//! cancellation drain) is younger and has subtler kernel-version-
//! dependent behaviour. Volt's persistent-registration epoll
//! backend (`reactor_epoll.zig`, Step 2f) is a direct port of Go's
//! `runtime/netpoll_epoll.go`.
//!
//! **io_uring is kept in tree for one specific future use:** the
//! async-file-I/O path. Today `src/fs/*` is spawnBlocking-backed
//! (thread-pool dispatch around blocking pread/pwrite/fsync); the
//! real win from io_uring is on storage I/O, where file fds don't
//! churn the way socket fds do and the §4A close-vs-register race
//! doesn't bite. When that migration lands, io_uring becomes the
//! Linux backend *for the fs path only* — net stays on epoll. The
//! tagged-union dispatch here is what makes that split possible
//! without forking the reactor surface.
//!
//! Until then, `initBackend(.io_uring)` is reachable only from
//! internal callers (conformance tests, future fs work). The
//! Step 2b per-wait io_uring shim it builds has the §4A race; that
//! is why it never sits behind a public Config flag.
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
    /// args, no config injection. Always returns the epoll variant;
    /// the public Linux backend has no choice to make.
    pub fn init(allocator: std.mem.Allocator) !Reactor {
        return initBackend(allocator, .epoll);
    }

    /// Internal: explicit-backend constructor. Reserved for the
    /// future async-file-I/O path (io_uring on the storage side)
    /// and for conformance tests that exercise both backends. Not
    /// called from public API surfaces — `Runtime.init` always uses
    /// `init` above.
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

//! `volt.io.lowlevel.waitReadable(fd)` and `waitWritable(fd)` — async readiness.
//!
//! Allocates a per-call Park on the calling coroutine's stack, registers
//! it with the reactor as the wake target for (fd, kind), and parks the
//! coroutine on it. When the kernel reports the fd ready, the reactor's
//! wake callback in `worker.idleStep` calls `park.unpark()`, which
//! atomically takes the parked coroutine and routes it via `Runtime.schedule`.

const std = @import("std");
const posix = std.posix;
const runtime_mod = @import("../runtime.zig");
const current = @import("../scheduler/current.zig");
const Park = @import("../scheduler/park.zig").Park;
const reactor_mod = @import("reactor.zig");
const io_errors = @import("errors.zig");

/// Errors `Reactor.registerWait` can produce on any backend (kqueue or
/// epoll), plus `Cancelled` from `parkCurrent`. Backend-specific names
/// (`EventNotFound` from kqueue's ENOENT, `EpollCtlFailed` from epoll)
/// translate to the platform-neutral `WaitRegistrationFailed` here so
/// public types don't leak the active backend.
pub const WaitError = io_errors.WaitError;

fn waitOn(fd: posix.fd_t, kind: reactor_mod.EventKind) WaitError!void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.io.wait* called outside a runtime");
    const coro = current.currentCoroutine() orelse
        @panic("volt.io.wait* called outside a coroutine");

    if (coro.isCancelled()) return error.Cancelled;

    // Park lives on the calling coroutine's stack. Stable for the
    // duration of the wait — the coroutine can't return while parked.
    var park: Park = .{};

    rt.reactor.registerWait(fd, kind, @ptrCast(&park)) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.AccessDenied => return error.AccessDenied,
        error.SystemResources => return error.SystemResources,
        error.Unexpected => return error.Unexpected,
        // Kqueue's `EventNotFound`, epoll's `EpollCtlFailed` /
        // `TimerfdSettimeFailed`, and io_uring's analogues all collapse
        // to a platform-neutral name here. Public types don't leak
        // which reactor backend is active.
        else => return error.WaitRegistrationFailed,
    };
    // From here on, the reactor will call park.unpark() when the fd is
    // ready. parkCurrent suspends us; the wake brings us back here.
    try park.parkCurrent();
}

pub fn waitReadable(fd: posix.fd_t) WaitError!void {
    return waitOn(fd, .readable);
}

pub fn waitWritable(fd: posix.fd_t) WaitError!void {
    return waitOn(fd, .writable);
}

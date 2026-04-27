//! `volt.io.waitReadable(fd)` and `waitWritable(fd)` — async readiness.
//!
//! Allocates a per-call Park on the calling coroutine's stack, registers
//! it with the reactor as the wake target for (fd, kind), and parks the
//! coroutine on it. When the kernel reports the fd ready, the reactor's
//! wake callback in `worker.idleStep` calls `park.unpark()`, which
//! atomically takes the parked coroutine and routes it via `Runtime.schedule`.

const std = @import("std");
const posix = std.posix;
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const Park = @import("../scheduler/park.zig").Park;
const reactor_mod = @import("reactor.zig");

pub const WaitError = error{ Cancelled, OutOfMemory } ||
    @import("../internal/syscall.zig").KeventError;

fn waitOn(fd: posix.fd_t, kind: reactor_mod.EventKind) WaitError!void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.io.wait* called outside a runtime");
    const coro = tls.currentCoroutine() orelse
        @panic("volt.io.wait* called outside a coroutine");

    if (coro.isCancelled()) return error.Cancelled;

    // Park lives on the calling coroutine's stack. Stable for the
    // duration of the wait — the coroutine can't return while parked.
    var park: Park = .{};

    try rt.reactor.registerWait(fd, kind, @ptrCast(&park));
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

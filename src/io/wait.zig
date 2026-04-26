//! `volt.io.waitReadable(fd)` and `waitWritable(fd)` — async readiness.
//!
//! Register interest in `fd` becoming readable/writable, park the current
//! coroutine, and return when the reactor delivers the wake-up. Returns
//! `error.Cancelled` if the coroutine is cancelled before or during the wait.
//!
//! These are the foundational primitives — `volt.io.read/write` build on
//! them by retrying after a wait when the underlying syscall returned
//! `WouldBlock`.

const std = @import("std");
const posix = std.posix;
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const park = @import("../scheduler/park.zig");
const reactor_mod = @import("reactor.zig");

pub const WaitError = error{
    Cancelled,
    OutOfMemory,
} || @typeInfo(@typeInfo(@TypeOf(reactor_mod.Reactor.registerWait)).@"fn".return_type.?).error_union.error_set;

fn waitOn(fd: posix.fd_t, kind: reactor_mod.EventKind) WaitError!void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.io.wait* called outside a runtime — use volt.run(...) first");
    const coro = tls.currentCoroutine() orelse
        @panic("volt.io.wait* called outside a coroutine");

    // Pre-park cancellation check — short-circuit before going to the kernel.
    if (coro.isCancelled()) return error.Cancelled;

    try rt.reactor.registerWait(fd, kind, coro);
    // After this point, only the reactor (or cancellation) can resume us.
    // Note: we don't have cancel-aware unregistration yet — if a parked
    // coroutine is cancelled, it still has a kqueue registration that will
    // fire when the fd later becomes ready. The wake just re-enqueues the
    // coroutine, which then observes its cancel flag at the next yield.
    // v0.5 will do explicit unregister-on-cancel via the structured-concurrency
    // scope cleanup.
    try park.parkCurrent();
}

pub fn waitReadable(fd: posix.fd_t) WaitError!void {
    return waitOn(fd, .readable);
}

pub fn waitWritable(fd: posix.fd_t) WaitError!void {
    return waitOn(fd, .writable);
}

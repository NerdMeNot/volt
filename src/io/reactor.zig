//! Reactor — OS-level readiness/completion source for coroutine wake-ups.
//!
//! Volt's I/O wait protocol is the same on every platform:
//!   1. Coroutine calls a non-blocking syscall (`read`, `accept`, ...).
//!   2. On `WouldBlock`, it calls `reactor.registerWait(fd, kind, *Park)`
//!      and parks on the Park.
//!   3. The next worker to idle claims the reactor, calls `poll()`,
//!      which blocks on the kernel queue and `Park.unpark()`s every
//!      coroutine whose I/O is ready.
//!
//! This file is the platform dispatcher. Concrete implementations:
//!   - Darwin/BSD: `reactor_kqueue.zig`  (kqueue + EVFILT_TIMER).
//!   - Linux:      `reactor_epoll.zig`   (epoll + eventfd + timerfd).
//!   - Linux/io_uring (parallel):  `reactor_iouring.zig`.
//!   - Windows:    `reactor_iocp.zig`    (IOCP — see status note below).
//!
//! All backends export the same public surface (Reactor, EventKind,
//! WaitKey, init/deinit/registerWait/registerTimer/poll/tickle/...).
//!
//! ### Windows status (v1.0)
//!
//! `reactor_iocp.zig` cross-compiles cleanly for Windows targets, but
//! Volt as a whole is NOT yet runtime-validated on Windows. The
//! remaining blockers are:
//!   1. `coroutine/stack.zig` uses POSIX `mmap`/`mprotect`. A parallel
//!      `stack_windows.zig` (VirtualAlloc reserve+commit + PAGE_GUARD)
//!      is required.
//!   2. `coroutine/stack_overflow.zig` uses SIGSEGV+sigsetjmp. A
//!      Structured Exception Handling (SEH) variant is required.
//!   3. `internal/thread/Futex.zig` and `internal/thread/sleep.zig`
//!      need their Windows arms updated for the current `std`
//!      (`WaitOnAddress` / `WaitForSingleObject`).
//!
//! When those land, drop the Windows arm of the @compileError below
//! and flip on a Windows CI runner. The IOCP backend is ready to wire
//! in immediately.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

pub const impl = blk: {
    // Linux gets a build-flag choice between epoll (default) and
    // io_uring. Other platforms have a single canonical backend.
    if (builtin.os.tag == .linux) switch (build_options.reactor_choice) {
        .iouring => break :blk @import("reactor_iouring.zig"),
        .epoll, .default => break :blk @import("reactor_epoll.zig"),
    };
    if (build_options.reactor_choice == .iouring) {
        @compileError("Volt: -Dreactor=iouring is Linux-only; current target is " ++ @tagName(builtin.os.tag));
    }
    break :blk switch (builtin.os.tag) {
        .macos, .ios, .freebsd, .netbsd, .dragonfly, .openbsd => @import("reactor_kqueue.zig"),
        .windows => @compileError(
            "Volt: Windows runtime support pending. The IOCP reactor itself " ++
                "(reactor_iocp.zig) cross-compiles cleanly and Windows arms exist for " ++
                "stack reservation (VirtualAlloc), Futex (WaitOnAddress), sleep, and " ++
                "monotonic time (QPC). Remaining work to flip Windows on by default: " ++
                "Windows arms for io/net.zig (ioctlsocket FIONBIO), io/io.zig " ++
                "(WriteFile vs posix.write), process/Command.zig (CreateProcess), " ++
                "observability/tracing.zig stderr writer, and a Windows CI runner. " ++
                "See `reactor_iocp.zig` header for the IOCP-side status note.",
        ),
        else => @compileError("Volt reactor: unsupported OS '" ++ @tagName(builtin.os.tag) ++ "'"),
    };
};

pub const Reactor = impl.Reactor;
pub const EventKind = impl.EventKind;

test {
    _ = impl;
}

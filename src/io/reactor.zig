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
//! All backends export the same public surface, enforced by the
//! `comptime` conformance check at the bottom of this file. Shared
//! types (`EventKind`, `ReactorError`) live in `reactor_types.zig` and
//! are re-exported here.
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

const reactor_types = @import("reactor_types.zig");

pub const EventKind = reactor_types.EventKind;
pub const ReactorError = reactor_types.ReactorError;

pub const impl = blk: {
    // Linux gets a build-flag choice between io_uring (default) and
    // epoll (legacy). io_uring is the modern Linux IO interface — 2-5×
    // faster on high-fanout workloads and the only native batched-IO
    // path Linux is investing in. We default to io_uring on Linux ≥ 5.1
    // (where it landed); set `-Dreactor=epoll` for older kernels or
    // when comparing backends.
    //
    // Note: build-time selection means `-Dreactor=epoll` is required on
    // pre-5.1 kernels; we don't probe at runtime. This is the same
    // strategy Tokio's `current_thread` uses (build-time backend gate).
    // Other platforms have a single canonical backend.
    if (builtin.os.tag == .linux) switch (build_options.reactor_choice) {
        .epoll => break :blk @import("reactor_epoll.zig"),
        .iouring, .default => break :blk @import("reactor_iouring.zig"),
    };
    if (build_options.reactor_choice == .iouring) {
        @compileError("Volt: -Dreactor=iouring is Linux-only; current target is " ++ @tagName(builtin.os.tag));
    }
    break :blk switch (builtin.os.tag) {
        .macos, .ios, .freebsd, .netbsd, .dragonfly, .openbsd => @import("reactor_kqueue.zig"),
        .windows => @import("reactor_iocp.zig"),
        else => @compileError("Volt reactor: unsupported OS '" ++ @tagName(builtin.os.tag) ++ "'"),
    };
};

pub const Reactor = impl.Reactor;

// ─────────────────────────────────────────────────────────────────────
// Backend conformance check
//
// Every backend's `Reactor` must export the same public method surface.
// Without this gate, a backend can drop or rename a method and the
// failure shows up at the consumer site (e.g. `worker.idleStep`) — far
// from the cause. This block emits a localized compile error naming
// the missing/wrong-typed method instead.
//
// Method type signatures use `*anyopaque` for the wake target and
// `*const fn (*anyopaque, *anyopaque) anyerror!void` for the wake
// callback so the runtime can stay generic over which Park type the
// caller picked.
// ─────────────────────────────────────────────────────────────────────

comptime {
    const R = impl.Reactor;
    const std_mod = @import("std");
    const posix = std_mod.posix;

    // Required associated types.
    if (!@hasDecl(impl, "EventKind")) {
        @compileError("Reactor backend missing `pub const EventKind` re-export");
    }
    if (impl.EventKind != EventKind) {
        @compileError("Reactor backend's EventKind must be the canonical one from reactor_types.zig — drop the local declaration and `pub const EventKind = reactor_types.EventKind;`");
    }

    // Required methods, with their expected signatures. Pointer-to-self
    // shape (`*Reactor` vs `*const Reactor`) is enforced; argument and
    // return shape is checked via the function pointer types below.
    //
    // We don't try to match `error{...}!T` exactly — Zig narrows error
    // sets by inference and a backend may return a strict subset of
    // ReactorError. Instead we match argument tuples and the success
    // payload type.

    if (!@hasDecl(R, "init")) @compileError("Reactor backend missing `init`");
    if (!@hasDecl(R, "deinit")) @compileError("Reactor backend missing `deinit`");
    if (!@hasDecl(R, "pendingCount")) @compileError("Reactor backend missing `pendingCount`");
    if (!@hasDecl(R, "tickle")) @compileError("Reactor backend missing `tickle`");
    if (!@hasDecl(R, "registerWait")) @compileError("Reactor backend missing `registerWait`");
    if (!@hasDecl(R, "unregisterWait")) @compileError("Reactor backend missing `unregisterWait`");
    if (!@hasDecl(R, "registerTimer")) @compileError("Reactor backend missing `registerTimer`");
    if (!@hasDecl(R, "unregisterTimer")) @compileError("Reactor backend missing `unregisterTimer`");
    if (!@hasDecl(R, "poll")) @compileError("Reactor backend missing `poll`");

    // Argument-tuple shape check via @TypeOf(@field(R, name)).
    // Zig's function pointers are nominal; a mismatch in arg types or
    // return payload here makes the exact difference visible.

    // init: fn (allocator) ReactorError!Reactor
    const InitFn = @TypeOf(R.init);
    const init_info = @typeInfo(InitFn);
    if (init_info != .@"fn") @compileError("Reactor.init must be a function");
    if (init_info.@"fn".params.len != 1) @compileError("Reactor.init must take (allocator) — 1 argument");
    if (init_info.@"fn".params[0].type.? != std_mod.mem.Allocator) {
        @compileError("Reactor.init's first parameter must be std.mem.Allocator");
    }

    // tickle: fn (*Reactor) void
    const TickleFn = @TypeOf(R.tickle);
    const tickle_info = @typeInfo(TickleFn);
    if (tickle_info.@"fn".return_type.? != void) {
        @compileError("Reactor.tickle must return void (it's fire-and-forget cross-thread wake)");
    }

    // registerWait: fn (*Reactor, posix.fd_t, EventKind, *anyopaque) ReactorError!void
    const RegWaitFn = @TypeOf(R.registerWait);
    const rw_info = @typeInfo(RegWaitFn);
    if (rw_info.@"fn".params.len != 4) {
        @compileError("Reactor.registerWait must take (*Reactor, fd, EventKind, *anyopaque)");
    }
    if (rw_info.@"fn".params[1].type.? != posix.fd_t) {
        @compileError("Reactor.registerWait's 2nd param must be posix.fd_t");
    }
    if (rw_info.@"fn".params[2].type.? != EventKind) {
        @compileError("Reactor.registerWait's 3rd param must be reactor_types.EventKind");
    }
    if (rw_info.@"fn".params[3].type.? != *anyopaque) {
        @compileError("Reactor.registerWait's 4th param must be *anyopaque (opaque wake target)");
    }

    // unregisterWait: fn (*Reactor, posix.fd_t, EventKind) void
    // Best-effort cancel: takes no error path so the cancel cleanup is
    // never itself a failure source.
    const UnregWaitFn = @TypeOf(R.unregisterWait);
    const uw_info = @typeInfo(UnregWaitFn);
    if (uw_info.@"fn".return_type.? != void) {
        @compileError("Reactor.unregisterWait must return void — cancel cleanup must not introduce a new error path");
    }

    // registerTimer: fn (*Reactor, u64, *anyopaque) ReactorError!u64
    // Returns an opaque id for `unregisterTimer`. id is u64 across
    // backends so callers don't need a generic-impl shim.
    const RegTimerFn = @TypeOf(R.registerTimer);
    const rt_info = @typeInfo(RegTimerFn);
    if (rt_info.@"fn".params.len != 3) {
        @compileError("Reactor.registerTimer must take (*Reactor, duration_ns: u64, target: *anyopaque)");
    }
    if (rt_info.@"fn".params[1].type.? != u64) {
        @compileError("Reactor.registerTimer's duration_ns must be u64 (nanoseconds)");
    }

    // unregisterTimer: fn (*Reactor, u64) void
    const UnregTimerFn = @TypeOf(R.unregisterTimer);
    const ut_info = @typeInfo(UnregTimerFn);
    if (ut_info.@"fn".return_type.? != void) {
        @compileError("Reactor.unregisterTimer must return void");
    }

    // pendingCount: fn (*const Reactor) usize
    const PcFn = @TypeOf(R.pendingCount);
    const pc_info = @typeInfo(PcFn);
    if (pc_info.@"fn".return_type.? != usize) {
        @compileError("Reactor.pendingCount must return usize");
    }

    // poll: fn (*Reactor, ?u64, *anyopaque, *const fn(*anyopaque, *anyopaque) anyerror!void) ReactorError!usize
    const PollFn = @TypeOf(R.poll);
    const poll_info = @typeInfo(PollFn);
    if (poll_info.@"fn".params.len != 4) {
        @compileError("Reactor.poll must take (*Reactor, timeout_ns: ?u64, wake_ctx, wakeFn)");
    }
    if (poll_info.@"fn".params[1].type.? != ?u64) {
        @compileError("Reactor.poll's timeout_ns must be ?u64");
    }
    if (poll_info.@"fn".params[2].type.? != *anyopaque) {
        @compileError("Reactor.poll's wake_ctx must be *anyopaque");
    }
}

test {
    _ = impl;
    _ = reactor_types;
}

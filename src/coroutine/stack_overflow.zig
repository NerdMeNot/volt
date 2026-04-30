//! SIGSEGV / SIGBUS handler that turns guard-page hits into either:
//!   - **Stack growth** (commit one more page, retry the instruction), or
//!   - **Graceful coroutine failure** when the reserved range is exhausted
//!     (mark the coroutine, `siglongjmp` back to the scheduler).
//!
//! Either way, the process keeps running.
//!
//! Signal-safety: handler calls only `mprotect`, atomic stores, TLS
//! reads, and `siglongjmp`. Never allocates, locks, or recurses into
//! runtime code that might.
//!
//! Layered architecture:
//!   - `stack.zig` owns the per-coroutine reservation and the
//!     `tryGrow` primitive (the `mprotect`).
//!   - This module owns the process-wide signal handler and the
//!     per-thread `sigaltstack` + per-dispatch `sigsetjmp` checkpoint.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Coroutine = @import("coroutine.zig").Coroutine;
const stack_mod = @import("stack.zig");
const tls = @import("../scheduler/tls.zig");

const supported = switch (builtin.os.tag) {
    .macos, .ios, .linux, .freebsd, .netbsd, .dragonfly => true,
    else => false,
};

/// Sigjmp_buf is opaque across libc implementations. 256 bytes covers
/// every platform we target with margin (macOS arm64: 196 B, glibc
/// aarch64: ~120 B). Aligned to 16 for AAPCS64.
pub const SigJmpBuf = extern struct {
    bytes: [256]u8 align(16) = [_]u8{0} ** 256,
};

// glibc exposes `sigsetjmp` as a macro that calls `__sigsetjmp`; the
// real symbol is `__sigsetjmp`. macOS / musl / BSD expose `sigsetjmp`
// directly. Pick the right linker name per target so cross-compile to
// glibc Linux resolves.
const is_glibc = builtin.os.tag == .linux and builtin.abi.isGnu();

extern "c" fn sigsetjmp(env: *SigJmpBuf, savesigs: c_int) c_int;
extern "c" fn __sigsetjmp(env: *SigJmpBuf, savesigs: c_int) c_int;
extern "c" fn siglongjmp(env: *SigJmpBuf, val: c_int) noreturn;

inline fn sigSetjmp(env: *SigJmpBuf, savesigs: c_int) c_int {
    return if (is_glibc) __sigsetjmp(env, savesigs) else sigsetjmp(env, savesigs);
}

/// Per-dispatch overflow recovery checkpoint, set by the worker before
/// each coroutine swap. The signal handler `siglongjmp`s here when a
/// terminal overflow is detected.
pub const DispatchCheckpoint = struct {
    jmp_buf: SigJmpBuf = .{},
};

/// One-shot install of the process-wide handler + per-thread alt stack.
/// Idempotent. Call from each worker thread on entry.
pub fn installPerThread() !void {
    if (!supported) return;
    InstallProcessHandler.callOnce();
    try installAltStack();
}

const InstallProcessHandler = struct {
    var installed = std.atomic.Value(bool).init(false);

    /// Idempotent — first caller installs, others are no-ops. Uses a CAS
    /// rather than `std.once` (which Zig 0.16 doesn't expose).
    fn callOnce() void {
        if (!supported) return;
        if (installed.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        const sa: posix.Sigaction = .{
            .handler = .{ .sigaction = &handleSegv },
            .mask = posix.sigemptyset(),
            .flags = posix.SA.SIGINFO | posix.SA.ONSTACK,
        };
        posix.sigaction(.SEGV, &sa, null);
        posix.sigaction(.BUS, &sa, null);
    }
};

/// macOS arm64 requires `MINSIGSTKSZ` (32 KiB) and recommends
/// `SIGSTKSZ` (32 KiB+); allocate 64 KiB to be safe across platforms.
const ALT_STACK_SIZE: usize = 64 * 1024;

threadlocal var alt_stack_buf: [ALT_STACK_SIZE]u8 align(16) = undefined;
threadlocal var alt_stack_installed: bool = false;

fn installAltStack() !void {
    if (alt_stack_installed) return;
    const ss = posix.stack_t{
        .sp = &alt_stack_buf,
        .size = ALT_STACK_SIZE,
        .flags = 0,
    };
    try posix.sigaltstack(&ss, null);
    alt_stack_installed = true;
}

threadlocal var current_checkpoint: ?*DispatchCheckpoint = null;

pub fn beginDispatch(cp: *DispatchCheckpoint) void {
    current_checkpoint = cp;
}

pub fn endDispatch() void {
    current_checkpoint = null;
}

/// Read the current dispatch checkpoint for this thread, or null if
/// not inside a dispatch. Used by the preemption signal handler to
/// jump back to the scheduler.
pub fn currentCheckpoint() ?*DispatchCheckpoint {
    return current_checkpoint;
}

/// `siglongjmp` to a dispatch checkpoint. Wrapped here so other
/// modules (e.g., the preemption handler) don't need to import the
/// extern declaration.
pub fn longjmpDispatch(cp: *DispatchCheckpoint) noreturn {
    siglongjmp(&cp.jmp_buf, 1);
}

/// Wrap dispatch in a setjmp checkpoint. Returns true if a terminal
/// overflow was just caught (caller should mark the coro overflowed and
/// route through Done); false on the normal-entry path.
pub inline fn setjmpDispatch(cp: *DispatchCheckpoint) bool {
    if (!supported) return false;
    return sigSetjmp(&cp.jmp_buf, 1) != 0;
}

fn faultAddr(info: *const posix.siginfo_t) usize {
    return switch (builtin.os.tag) {
        .macos, .ios => @intFromPtr(info.addr),
        .linux => @intFromPtr(info.fields.sigfault.addr),
        else => 0,
    };
}

fn handleSegv(sig: posix.SIG, info: *const posix.siginfo_t, ctx: ?*anyopaque) callconv(.c) void {
    _ = ctx;
    const fault: usize = faultAddr(info);

    const coro = tls.currentCoroutine() orelse {
        chainToDefault(sig);
        return;
    };

    // Recover the GrowableStack metadata embedded in the coroutine.
    var gs = stack_mod.GrowableStack{
        .base = @intFromPtr(coro.stack.ptr),
        .top = @intFromPtr(coro.stack.ptr) + coro.stack.len,
        .reserved_size = coro.stack.len,
        .committed_bottom = coro.stack_committed_bottom.load(.acquire),
        .floor = @intFromPtr(coro.stack.ptr) + stack_mod.pageSize(),
    };

    switch (stack_mod.tryGrow(&gs, fault)) {
        .grew => {
            // Publish the new committed bottom for the next overflow.
            coro.stack_committed_bottom.store(gs.committed_bottom, .release);
            // Return from the handler — the kernel resumes the
            // coroutine, the faulting instruction retries, and now
            // succeeds because the page is committed.
            return;
        },
        .terminal => {
            coro.overflow_flag.store(true, .release);
            const cp = current_checkpoint orelse {
                chainToDefault(sig);
                return;
            };
            siglongjmp(&cp.jmp_buf, 1);
        },
        .not_my_stack => {
            chainToDefault(sig);
            return;
        },
    }
}

fn chainToDefault(sig: posix.SIG) void {
    const default_action: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.DFL },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(sig, &default_action, null);
    _ = std.c.raise(sig);
}

test "stack_overflow: SigJmpBuf is large enough" {
    try std.testing.expect(@sizeOf(SigJmpBuf) >= 196);
}

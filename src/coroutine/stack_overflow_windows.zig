//! Windows implementation of the stack-overflow handler. See
//! `stack_overflow.zig` for the cross-platform dispatcher and contract.
//!
//! Strategy:
//!   - Process-wide `Vectored Exception Handler` installed via
//!     `AddVectoredExceptionHandler`. Catches `STATUS_ACCESS_VIOLATION`
//!     (0xC0000005) — what a `PAGE_NOACCESS` touch raises — and
//!     `STATUS_STACK_OVERFLOW` (0xC00000FD).
//!   - Per-thread `SetThreadStackGuarantee` reserves 64 KiB of stack
//!     headroom for the handler itself. Windows's analogue of POSIX
//!     `sigaltstack` — without it, an exception triggered by stack
//!     overflow has nowhere to actually run the handler.
//!   - Per-dispatch `_setjmp` checkpoint; `longjmp` on terminal
//!     overflow back to the scheduler. Windows has no signal mask, so
//!     plain `setjmp`/`longjmp` from MSVCRT suffice.
//!
//! ## Why VEH (vectored), not SEH (`__try`/`__except`)
//!
//! Zig 0.16 has no surface for `__try`/`__except`. VEH is the
//! frame-independent equivalent: registered process-wide via Win32
//! and invoked before the unwinder walks frames. Same effect for our
//! use case (catch the AV, decide grow vs. terminal, decide
//! continue-execution vs. unwind).
//!
//! Reference: libuv `src/win/error.c` for the VEH pattern; Microsoft
//! docs on `_setjmp` jmp_buf size for the buffer dimensions.

const std = @import("std");
const builtin = @import("builtin");
const Coroutine = @import("coroutine.zig").Coroutine;
const stack_mod = @import("stack.zig");
const current = @import("../scheduler/current.zig");

comptime {
    if (builtin.os.tag != .windows) {
        @compileError("stack_overflow_windows.zig is for Windows only");
    }
}

pub const supported = true;

// ─────────────────────────────────────────────────────────────────────
// Win32 / NTSTATUS bindings
// ─────────────────────────────────────────────────────────────────────

const DWORD = std.os.windows.DWORD;
const ULONG = std.os.windows.ULONG;
const ULONG_PTR = usize;
const PVOID = ?*anyopaque;
const BOOL = std.os.windows.BOOL;

const STATUS_ACCESS_VIOLATION: DWORD = 0xC0000005;
const STATUS_STACK_OVERFLOW: DWORD = 0xC00000FD;
const STATUS_GUARD_PAGE_VIOLATION: DWORD = 0x80000001;

const EXCEPTION_CONTINUE_EXECUTION: c_long = -1;
const EXCEPTION_CONTINUE_SEARCH: c_long = 0;

const EXCEPTION_MAXIMUM_PARAMETERS: usize = 15;

const EXCEPTION_RECORD = extern struct {
    ExceptionCode: DWORD,
    ExceptionFlags: DWORD,
    ExceptionRecord: ?*EXCEPTION_RECORD,
    ExceptionAddress: PVOID,
    NumberParameters: DWORD,
    ExceptionInformation: [EXCEPTION_MAXIMUM_PARAMETERS]ULONG_PTR,
};

const EXCEPTION_POINTERS = extern struct {
    ExceptionRecord: *EXCEPTION_RECORD,
    ContextRecord: PVOID, // CONTEXT — we never read it
};

extern "kernel32" fn AddVectoredExceptionHandler(
    First: ULONG,
    Handler: *const fn (*EXCEPTION_POINTERS) callconv(.winapi) c_long,
) callconv(.winapi) PVOID;

extern "kernel32" fn SetThreadStackGuarantee(StackSizeInBytes: *ULONG) callconv(.winapi) BOOL;

// MSVCRT's `_setjmp`/`longjmp`. The Windows jmp_buf size is platform-
// specific; 256 bytes covers x86_64 (256) and aarch64 (~204) with
// margin. Same shape as the POSIX SigJmpBuf so the dispatcher can
// expose a single name.
pub const SigJmpBuf = extern struct {
    bytes: [256]u8 align(16) = [_]u8{0} ** 256,
};

extern "c" fn _setjmp(env: *SigJmpBuf) c_int;
extern "c" fn longjmp(env: *SigJmpBuf, val: c_int) noreturn;

pub const DispatchCheckpoint = struct {
    jmp_buf: SigJmpBuf = .{},
};

// ─────────────────────────────────────────────────────────────────────
// Per-thread + per-process install
// ─────────────────────────────────────────────────────────────────────

const HANDLER_STACK_GUARANTEE_BYTES: ULONG = 64 * 1024;

const InstallProcessHandler = struct {
    var installed = std.atomic.Value(bool).init(false);

    fn callOnce() void {
        if (installed.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        // First = 1 → handler runs BEFORE all other VEH handlers and
        // before the SEH unwinder. Necessary because we want first
        // shot at the AV before any frame-based __except chain.
        _ = AddVectoredExceptionHandler(1, &handleException);
    }
};

threadlocal var stack_guarantee_set: bool = false;

pub fn installPerThread() !void {
    InstallProcessHandler.callOnce();
    if (stack_guarantee_set) return;
    var bytes: ULONG = HANDLER_STACK_GUARANTEE_BYTES;
    // Failure here is non-fatal — without the guarantee the handler
    // may still run on most threads, but a stack-overflow that
    // exhausts the OS thread stack will crash. Best-effort.
    _ = SetThreadStackGuarantee(&bytes);
    stack_guarantee_set = true;
}

threadlocal var current_checkpoint: ?*DispatchCheckpoint = null;

pub fn beginDispatch(cp: *DispatchCheckpoint) void {
    current_checkpoint = cp;
}

pub fn endDispatch() void {
    current_checkpoint = null;
}

pub fn currentCheckpoint() ?*DispatchCheckpoint {
    return current_checkpoint;
}

pub fn longjmpDispatch(cp: *DispatchCheckpoint) noreturn {
    longjmp(&cp.jmp_buf, 1);
}

pub inline fn setjmpDispatch(cp: *DispatchCheckpoint) bool {
    return _setjmp(&cp.jmp_buf) != 0;
}

// ─────────────────────────────────────────────────────────────────────
// Vectored exception handler
// ─────────────────────────────────────────────────────────────────────

fn handleException(info: *EXCEPTION_POINTERS) callconv(.winapi) c_long {
    const rec = info.ExceptionRecord;
    const code = rec.ExceptionCode;

    // Only interested in faults that look like stack growth. AV is
    // raised when a coroutine touches a PAGE_NOACCESS page (our guard
    // region). STACK_OVERFLOW is raised when the OS thread stack
    // itself runs out — for our coroutines this means terminal
    // overflow even before they hit the floor (very rare; mostly
    // happens for the bootstrap thread itself).
    if (code != STATUS_ACCESS_VIOLATION and
        code != STATUS_STACK_OVERFLOW and
        code != STATUS_GUARD_PAGE_VIOLATION)
    {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    // For AV, ExceptionInformation[0] is read/write flag and [1] is
    // the violation address. For STACK_OVERFLOW the kernel doesn't
    // populate an address; we'll treat that as terminal.
    const fault: usize = if (code == STATUS_ACCESS_VIOLATION or code == STATUS_GUARD_PAGE_VIOLATION)
        rec.ExceptionInformation[1]
    else
        0;

    const coro = current.currentCoroutine() orelse return EXCEPTION_CONTINUE_SEARCH;

    var gs = stack_mod.GrowableStack{
        .base = @intFromPtr(coro.stack.ptr),
        .top = @intFromPtr(coro.stack.ptr) + coro.stack.len,
        .reserved_size = coro.stack.len,
        .committed_bottom = coro.stack_committed_bottom.load(.acquire),
        .floor = @intFromPtr(coro.stack.ptr) + stack_mod.pageSize(),
    };

    // Hard stack-overflow (OS thread stack exhausted) goes terminal
    // immediately — we can't grow what's not ours.
    if (code == STATUS_STACK_OVERFLOW) {
        return goTerminal(coro);
    }

    switch (stack_mod.tryGrow(&gs, fault)) {
        .grew => {
            coro.stack_committed_bottom.store(gs.committed_bottom, .release);
            // The instruction retries on EXCEPTION_CONTINUE_EXECUTION
            // and now succeeds because the page is committed.
            return EXCEPTION_CONTINUE_EXECUTION;
        },
        .terminal => return goTerminal(coro),
        .not_my_stack => return EXCEPTION_CONTINUE_SEARCH,
    }
}

fn goTerminal(coro: *Coroutine) c_long {
    coro.overflow_flag.store(true, .release);
    const cp = current_checkpoint orelse return EXCEPTION_CONTINUE_SEARCH;
    longjmp(&cp.jmp_buf, 1);
}

test "stack_overflow_windows: SigJmpBuf is large enough" {
    try std.testing.expect(@sizeOf(SigJmpBuf) >= 256);
}

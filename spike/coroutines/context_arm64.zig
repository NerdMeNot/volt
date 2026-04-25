//! ARM64 (AAPCS64) context switch for stackful coroutines.
//!
//! Saves/restores the AAPCS64 callee-saved register set:
//!   x19-x28 (general-purpose)
//!   x29 (fp), x30 (lr)
//!   sp
//!   d8-d15 (lower 64 bits of v8-v15)
//!
//! Total: 13 u64 GPR slots + 8 u64 float slots = 168 bytes per Context.
//!
//! `swap(from, to)` is a naked function. Caller passes from-ptr in x0,
//! to-ptr in x1. We save current state to *from, load *to, then `ret`,
//! which jumps to the lr we just loaded — landing in to's coroutine.
//!
//! For a freshly-spawned coroutine, the initial Context is populated with:
//!   - lr = entry trampoline address
//!   - sp = top of allocated stack (16-byte aligned)
//!   - x19 = pointer to the user-provided closure data
//! When we first swap to it, `ret` jumps to the trampoline, which runs
//! the user fn, then yields back to the scheduler when done.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch != .aarch64) {
        @compileError("context_arm64.zig: this file is ARM64-only. Pick the right arch file.");
    }
}

/// Callee-saved register state, layout fixed for asm offsets.
/// Offset constants below MUST stay in sync with the struct layout.
pub const Context = extern struct {
    x19: u64 = 0, x20: u64 = 0,
    x21: u64 = 0, x22: u64 = 0,
    x23: u64 = 0, x24: u64 = 0,
    x25: u64 = 0, x26: u64 = 0,
    x27: u64 = 0, x28: u64 = 0,
    fp:  u64 = 0, lr:  u64 = 0,    // x29, x30
    sp:  u64 = 0,
    d8:  u64 = 0, d9:  u64 = 0,
    d10: u64 = 0, d11: u64 = 0,
    d12: u64 = 0, d13: u64 = 0,
    d14: u64 = 0, d15: u64 = 0,
};

// Sanity: check the layout. If asm offsets ever drift from the struct,
// this fails at compile time.
comptime {
    std.debug.assert(@offsetOf(Context, "x19") == 0);
    std.debug.assert(@offsetOf(Context, "x21") == 16);
    std.debug.assert(@offsetOf(Context, "x23") == 32);
    std.debug.assert(@offsetOf(Context, "x25") == 48);
    std.debug.assert(@offsetOf(Context, "x27") == 64);
    std.debug.assert(@offsetOf(Context, "fp")  == 80);
    std.debug.assert(@offsetOf(Context, "sp")  == 96);
    std.debug.assert(@offsetOf(Context, "d8")  == 104);
    std.debug.assert(@offsetOf(Context, "d10") == 120);
    std.debug.assert(@offsetOf(Context, "d12") == 136);
    std.debug.assert(@offsetOf(Context, "d14") == 152);
    std.debug.assert(@sizeOf(Context) == 168);
}

/// Swap from current context (saved into *from) to *to.
/// Implemented as an extern symbol — body is module-level asm below.
pub extern fn voltCtxSwap(from: *Context, to: *Context) callconv(.c) void;

/// Naked trampoline — first thing a freshly-spawned coroutine runs.
/// Reads x19 (closure ptr), loads the run_fn from offset 0 of the closure,
/// calls run_fn(closure). On unexpected return, traps.
///
/// This must be naked — any compiler-inserted prologue would trample x19
/// before we get to read it. By implementing it in asm directly, we
/// guarantee x19 is preserved.
pub extern fn voltCoroEntry() callconv(.c) void;

// Module-level asm: define `_voltCtxSwap` + `_voltCoroEntry`.
comptime {
    asm (
        \\.global _voltCtxSwap
        \\.p2align 2
        \\_voltCtxSwap:
        // Save callee-saved GPRs to *from (x0)
        \\  stp x19, x20, [x0, #0]
        \\  stp x21, x22, [x0, #16]
        \\  stp x23, x24, [x0, #32]
        \\  stp x25, x26, [x0, #48]
        \\  stp x27, x28, [x0, #64]
        \\  stp x29, x30, [x0, #80]
        // Save sp via x9 (sp can't be source of `str`)
        \\  mov x9, sp
        \\  str x9, [x0, #96]
        // Save callee-saved FP/SIMD low 64 bits
        \\  stp d8,  d9,  [x0, #104]
        \\  stp d10, d11, [x0, #120]
        \\  stp d12, d13, [x0, #136]
        \\  stp d14, d15, [x0, #152]
        // Load callee-saved GPRs from *to (x1)
        \\  ldp x19, x20, [x1, #0]
        \\  ldp x21, x22, [x1, #16]
        \\  ldp x23, x24, [x1, #32]
        \\  ldp x25, x26, [x1, #48]
        \\  ldp x27, x28, [x1, #64]
        \\  ldp x29, x30, [x1, #80]
        \\  ldr x9,  [x1, #96]
        \\  mov sp, x9
        \\  ldp d8,  d9,  [x1, #104]
        \\  ldp d10, d11, [x1, #120]
        \\  ldp d12, d13, [x1, #136]
        \\  ldp d14, d15, [x1, #152]
        // ret jumps to lr (x30) which we just loaded — lands in `to` context.
        \\  ret
        // ─────────────────────────────────────────────────────────────────
        // Coroutine trampoline. x19 holds the closure pointer.
        // Closure layout: first u64 is the run_fn pointer, then opaque data.
        // We call run_fn(closure_ptr).
        // ─────────────────────────────────────────────────────────────────
        \\.global _voltCoroEntry
        \\.p2align 2
        \\_voltCoroEntry:
        \\  mov x0, x19          // arg0 = closure pointer
        \\  ldr x1, [x19]        // x1   = closure.run_fn (offset 0)
        \\  blr x1                // call run_fn(closure)
        \\  brk #1                // run_fn shouldn't return; trap if it does
    );
}

/// Public name we'll use elsewhere.
pub const swap = voltCtxSwap;

/// Initialize a fresh Context for a newly-spawned coroutine.
/// The trampoline (`voltCoroEntry`) reads the closure pointer from x19
/// and invokes closure.run_fn(closure).
pub fn initContext(ctx_ptr: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    ctx_ptr.* = .{};
    const sp_int: u64 = @intFromPtr(stack_top);
    std.debug.assert(sp_int & 0xF == 0);
    ctx_ptr.sp = sp_int;
    ctx_ptr.lr = @intFromPtr(&voltCoroEntry);
    ctx_ptr.x19 = @intFromPtr(closure);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — directly verify the asm with a closure-style trampoline.
// The closure's first u64 is the run_fn pointer (matches ClosureBase layout).
// ─────────────────────────────────────────────────────────────────────────────

const TestClosure = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
    main_ctx: *Context,
    coro_ctx: *Context,
    counter: u32,
    done: bool,
};

fn testRun(opaque_ptr: *anyopaque) callconv(.c) void {
    const c: *TestClosure = @ptrCast(@alignCast(opaque_ptr));
    while (c.counter < 5) {
        c.counter += 1;
        swap(c.coro_ctx, c.main_ctx);
    }
    c.done = true;
    swap(c.coro_ctx, c.main_ctx);
    unreachable;
}

test "swap: ping-pong via trampoline + closure" {
    var main_ctx: Context = .{};
    var coro_ctx: Context = .{};

    var closure: TestClosure = .{
        .run_fn = &testRun,
        .main_ctx = &main_ctx,
        .coro_ctx = &coro_ctx,
        .counter = 0,
        .done = false,
    };

    const stack_size = 16 * 1024;
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", stack_size);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + stack_size;

    initContext(&coro_ctx, stack_top, &closure);

    var rounds: u32 = 0;
    while (!closure.done) : (rounds += 1) {
        if (rounds > 100) @panic("coro never finished — stuck");
        swap(&main_ctx, &coro_ctx);
    }

    try std.testing.expectEqual(@as(u32, 5), closure.counter);
    try std.testing.expect(closure.done);
}

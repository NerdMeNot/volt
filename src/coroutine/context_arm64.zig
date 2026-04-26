//! ARM64 (AAPCS64) context switch primitive for stackful coroutines.
//!
//! Saves and restores the AAPCS64 callee-saved register set:
//!   x19-x28, x29 (fp), x30 (lr), sp, d8-d15
//!
//! Two extern symbols are emitted via module-level asm:
//!   `voltCtxSwap(from, to)` — save current, load target, ret
//!   `voltCoroEntry`         — naked trampoline; reads x19 (closure ptr),
//!                              calls (*x19).run_fn(closure)
//!
//! The naked trampoline is the critical piece — any compiler-inserted
//! prologue would trample x19 before user code reads it.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch != .aarch64) {
        @compileError("context_arm64.zig is ARM64-only");
    }
}

/// Callee-saved register state. Layout fixed for asm offsets — do not reorder.
pub const Context = extern struct {
    x19: u64 = 0,
    x20: u64 = 0,
    x21: u64 = 0,
    x22: u64 = 0,
    x23: u64 = 0,
    x24: u64 = 0,
    x25: u64 = 0,
    x26: u64 = 0,
    x27: u64 = 0,
    x28: u64 = 0,
    fp: u64 = 0,
    lr: u64 = 0,
    sp: u64 = 0,
    d8: u64 = 0,
    d9: u64 = 0,
    d10: u64 = 0,
    d11: u64 = 0,
    d12: u64 = 0,
    d13: u64 = 0,
    d14: u64 = 0,
    d15: u64 = 0,
};

comptime {
    std.debug.assert(@offsetOf(Context, "x19") == 0);
    std.debug.assert(@offsetOf(Context, "fp") == 80);
    std.debug.assert(@offsetOf(Context, "sp") == 96);
    std.debug.assert(@offsetOf(Context, "d8") == 104);
    std.debug.assert(@sizeOf(Context) == 168);
}

/// Save current state to *from, load *to, jump into *to's lr.
pub extern fn voltCtxSwap(from: *Context, to: *Context) callconv(.c) void;

/// Naked trampoline run by every freshly-spawned coroutine.
/// Reads closure pointer from x19, calls (*closure).run_fn(closure).
pub extern fn voltCoroEntry() callconv(.c) void;

comptime {
    asm (
        \\.global _voltCtxSwap
        \\.p2align 2
        \\_voltCtxSwap:
        \\  stp x19, x20, [x0, #0]
        \\  stp x21, x22, [x0, #16]
        \\  stp x23, x24, [x0, #32]
        \\  stp x25, x26, [x0, #48]
        \\  stp x27, x28, [x0, #64]
        \\  stp x29, x30, [x0, #80]
        \\  mov x9, sp
        \\  str x9, [x0, #96]
        \\  stp d8,  d9,  [x0, #104]
        \\  stp d10, d11, [x0, #120]
        \\  stp d12, d13, [x0, #136]
        \\  stp d14, d15, [x0, #152]
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
        \\  ret
        \\.global _voltCoroEntry
        \\.p2align 2
        \\_voltCoroEntry:
        \\  mov x0, x19
        \\  ldr x1, [x19]
        \\  blr x1
        \\  brk #1
    );
}

/// Public re-export for clarity at call sites.
pub const swap = voltCtxSwap;

/// Initialize a fresh Context for a newly-spawned coroutine. The trampoline
/// reads the closure pointer from x19 and invokes (*closure).run_fn(closure).
///
/// `stack_top` is the highest address of the allocated stack region — sp grows
/// downward from there. Must be 16-byte aligned per AAPCS64.
pub fn initContext(ctx: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    ctx.* = .{};
    const sp_int: u64 = @intFromPtr(stack_top);
    std.debug.assert(sp_int & 0xF == 0);
    ctx.sp = sp_int;
    ctx.lr = @intFromPtr(&voltCoroEntry);
    ctx.x19 = @intFromPtr(closure);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — direct ping-pong between two contexts using a closure-style trampoline.
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

test "context_arm64: ping-pong via trampoline" {
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
        if (rounds > 100) @panic("coro never finished");
        swap(&main_ctx, &coro_ctx);
    }
    try std.testing.expectEqual(@as(u32, 5), closure.counter);
}

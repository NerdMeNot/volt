//! x86_64 (System V ABI) context switch primitive for stackful coroutines.
//!
//! Saves and restores the System V callee-saved register set:
//!   rbx, rbp, r12, r13, r14, r15, rsp
//! The "PC" is implicit — it sits on the stack as the return address
//! left there by the `call` that invoked `voltCtxSwap`. We save SP
//! including that ret addr, restore SP to the target's stack, and
//! `ret` pops the target's saved ret addr to resume there.
//!
//! Two extern symbols are emitted via module-level asm:
//!   `voltCtxSwap(from, to)` — save current state into *from, load *to,
//!                              ret (pops *to's saved ret addr).
//!   `voltCoroEntry`         — naked trampoline; reads closure pointer
//!                              from %rbx (we initialize ctx.rbx with
//!                              the closure ptr in `initContext`),
//!                              calls (*closure).run_fn(closure).
//!
//! ## Preemption (FullContext)
//!
//! For symmetry with arm64 we declare `FullContext` + `voltCtxResumeFull`
//! + `voltCtxSwapFull` so any preemption-based code that links these
//! compiles. The functions panic if called — async preemption on x86_64
//! is not yet implemented.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch != .x86_64) {
        @compileError("context_x86_64.zig is x86_64-only");
    }
}

/// Callee-saved register state. Layout fixed for asm offsets.
pub const Context = extern struct {
    rbx: u64 = 0,
    rbp: u64 = 0,
    r12: u64 = 0,
    r13: u64 = 0,
    r14: u64 = 0,
    r15: u64 = 0,
    rsp: u64 = 0,
};

comptime {
    std.debug.assert(@offsetOf(Context, "rbx") == 0);
    std.debug.assert(@offsetOf(Context, "rsp") == 48);
    std.debug.assert(@sizeOf(Context) == 56);
}

/// Full register state for asynchronous preemption. Stub on x86_64 —
/// the watchdog-based preemption ships in async preemption pending.
pub const FullContext = extern struct {
    /// Placeholder — actual layout TBD when preempt rewire lands.
    rip: u64 = 0,
    rsp: u64 = 0,
    rflags: u64 = 0,
    /// 16 GPRs (rax, rbx, ..., r15).
    gpr: [16]u64 align(16) = [_]u64{0} ** 16,
    /// 16 SSE/AVX registers stored as two u64 halves each (placeholder
    /// for xmm0..xmm15).
    xmm: [16][2]u64 align(16) = [_][2]u64{[_]u64{ 0, 0 }} ** 16,
};

/// Save current state to *from, load *to, jump into *to (pops ret addr).
pub extern fn voltCtxSwap(from: *Context, to: *Context) callconv(.c) void;

/// Naked trampoline — reads %rbx (closure ptr), calls run_fn(closure).
pub extern fn voltCoroEntry() callconv(.c) void;

// Asm bodies live in `context_x86_64.S` and are linked in by build.zig
// for x86_64 targets. We tried module-level `comptime { asm(...) }`
// blocks but Zig 0.16 doesn't reliably emit them into the object file
// for x86_64-linux ELF (works on Darwin Mach-O). Naked Zig functions
// with inline asm hit a separate operand-syntax restriction in 0.16.
// A separate .S file is the most portable workaround.

/// Stub — preempt resume is arm64-only.
pub fn voltCtxResumeFull(ctx: *const FullContext) callconv(.c) noreturn {
    _ = ctx;
    @panic("voltCtxResumeFull: x86_64 async preemption not yet implemented (async preemption pending)");
}

/// Stub — see voltCtxResumeFull.
pub fn voltCtxSwapFull(from: *Context, to: *const FullContext) callconv(.c) void {
    _ = from;
    _ = to;
    @panic("voltCtxSwapFull: x86_64 async preemption not yet implemented (async preemption pending)");
}

pub const swap = voltCtxSwap;

/// Initialize a fresh Context. The trampoline reads the closure ptr
/// from %rbx and invokes (*closure).run_fn(closure).
///
/// `stack_top` is the highest address of the allocated stack region —
/// rsp grows downward from there. Must be 16-byte aligned (System V
/// requires 16-byte alignment AT the call instruction; we set up so
/// the trampoline's `call run_fn` has 16-byte-aligned %rsp).
pub fn initContext(ctx: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    ctx.* = .{};
    var sp_int: u64 = @intFromPtr(stack_top);
    std.debug.assert(sp_int & 0xF == 0);

    // Push voltCoroEntry as the "ret address" the first ctx_swap will
    // pop. After we push, %rsp is 8 below stack_top (mis-aligned).
    // System V wants %rsp 16-byte aligned BEFORE the `call`. The
    // trampoline's `callq *%rax` pushes 8 bytes for the new ret addr;
    // for that call to land on aligned %rsp, our pre-call %rsp must
    // be at (multiple-of-16 - 8) — which is exactly stack_top - 8.
    // So we're good: just push voltCoroEntry once.
    sp_int -= 8;
    const slot: *u64 = @ptrFromInt(sp_int);
    slot.* = @intFromPtr(&voltCoroEntry);

    ctx.rsp = sp_int;
    ctx.rbx = @intFromPtr(closure);
}

// ─────────────────────────────────────────────────────────────────────
// Tests — direct ping-pong between two contexts.
// ─────────────────────────────────────────────────────────────────────

const TestClosure = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
    main_ctx: *Context,
    coro_ctx: *Context,
    counter: u32 = 0,

    fn run(opaque_ptr: *anyopaque) callconv(.c) void {
        const self: *TestClosure = @ptrCast(@alignCast(opaque_ptr));
        var i: u32 = 0;
        while (i < 3) : (i += 1) {
            self.counter += 1;
            voltCtxSwap(self.coro_ctx, self.main_ctx);
        }
        // Final swap; main expects to come back here.
        voltCtxSwap(self.coro_ctx, self.main_ctx);
        unreachable;
    }
};

test "context_x86_64: ping-pong via trampoline" {
    if (builtin.cpu.arch != .x86_64) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const stack = try allocator.alignedAlloc(u8, .@"16", 64 * 1024);
    defer allocator.free(stack);

    var main_ctx: Context = .{};
    var coro_ctx: Context = .{};

    var closure = TestClosure{
        .run_fn = &TestClosure.run,
        .main_ctx = &main_ctx,
        .coro_ctx = &coro_ctx,
    };

    const stack_top: [*]u8 = stack.ptr + stack.len;
    initContext(&coro_ctx, stack_top, @ptrCast(&closure));

    // First swap-in: coro runs to first ctx_swap-back.
    voltCtxSwap(&main_ctx, &coro_ctx);
    try std.testing.expectEqual(@as(u32, 1), closure.counter);

    // Continue ping-pong.
    voltCtxSwap(&main_ctx, &coro_ctx);
    try std.testing.expectEqual(@as(u32, 2), closure.counter);

    voltCtxSwap(&main_ctx, &coro_ctx);
    try std.testing.expectEqual(@as(u32, 3), closure.counter);
}

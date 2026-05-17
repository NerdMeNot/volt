//! x86_64 stackful context switch.
//!
//! Two ABIs share this file:
//!   * **System V x86-64** (Linux, macOS-Intel, BSDs) — 6 callee-
//!     saved GPRs (`rbx`, `rbp`, `r12`-`r15`) + `rsp`. No XMM
//!     callee-saves. Context = 56 bytes.
//!   * **Microsoft x64** (Windows) — 8 callee-saved GPRs (`rbx`,
//!     `rbp`, `rdi`, `rsi`, `r12`-`r15`) + `rsp` + XMM6-XMM15
//!     (10 × 128-bit). Context ≈ 232 bytes, 16-byte aligned for
//!     `movdqa`. Lands in L6b — currently a `@compileError`.
//!
//! ## Why `.S` files instead of inline asm
//!
//! Zig 0.16 module-level `comptime { asm(...) }` doesn't reliably
//! emit global symbols on x86_64-linux ELF (documented in
//! `CLAUDE.md`'s "Zig 0.16 API notes" and the prior ARM64
//! context-switch source). The ARM64 file gets away with comptime
//! asm because aarch64-linux + aarch64-macos both emit symbols
//! correctly. For x86_64 we ship a separate `.S` file linked via
//! `build.zig`. Mach-O vs ELF symbol-prefix differences are
//! handled inside the `.S` with `#if defined(__APPLE__)` (same
//! Mach-O `_`-prefix invariant as L5a's comptime trick in the
//! ARM64 file).
//!
//! ## Layout invariants
//!
//! Offset constants below MUST stay in sync with the `.S` file's
//! `[from + N]` / `[to + N]` accesses. The `comptime` asserts at
//! the bottom of this file catch drift at compile time.
//!
//! ## Fresh-coroutine bootstrap
//!
//! Standard libco / Go-runtime idiom: write the entry-trampoline
//! address into the slot one below `stack_top`, set
//! `ctx.rsp = stack_top - 8`, stash the closure pointer in `rbx`
//! (callee-saved, so the trampoline sees it). First `swap` does
//! `mov rsp, [to+rsp_off]; ret` — `ret` pops the trampoline
//! address.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch != .x86_64) {
        @compileError("context_x86_64.zig: x86_64-only. Other arches have their own file.");
    }
}

// ─── Per-OS context shape ────────────────────────────────────────

pub const Context = switch (builtin.os.tag) {
    .windows => @compileError("Volt: Windows x86_64 ctx switch lands in L6b. " ++
        "See ~/.claude/plans/what-would-it-take-structured-panda.md"),
    else => SysVContext,
};

// System V x86-64 callee-saved set:
//   GPRs: rbx, rbp, r12, r13, r14, r15 — 6 × u64 = 48 bytes
//   Stack pointer: rsp — 1 × u64 = 8 bytes
//   Total: 56 bytes. No XMM (SysV makes them caller-saved).
//
// Layout is fixed for the `.S` file's hardcoded offsets — see the
// comptime asserts at the bottom of this file.
const SysVContext = extern struct {
    rbx: u64 = 0,
    rbp: u64 = 0,
    r12: u64 = 0,
    r13: u64 = 0,
    r14: u64 = 0,
    r15: u64 = 0,
    rsp: u64 = 0,
};

// Asm-side offset constants. If a `.S` file drifts from these the
// `comptime` block below fails to compile.
const SYSV_OFF_RBX: usize = 0;
const SYSV_OFF_RBP: usize = 8;
const SYSV_OFF_R12: usize = 16;
const SYSV_OFF_R13: usize = 24;
const SYSV_OFF_R14: usize = 32;
const SYSV_OFF_R15: usize = 40;
const SYSV_OFF_RSP: usize = 48;
const SYSV_SIZE: usize = 56;

comptime {
    if (builtin.os.tag != .windows) {
        std.debug.assert(@offsetOf(SysVContext, "rbx") == SYSV_OFF_RBX);
        std.debug.assert(@offsetOf(SysVContext, "rbp") == SYSV_OFF_RBP);
        std.debug.assert(@offsetOf(SysVContext, "r12") == SYSV_OFF_R12);
        std.debug.assert(@offsetOf(SysVContext, "r13") == SYSV_OFF_R13);
        std.debug.assert(@offsetOf(SysVContext, "r14") == SYSV_OFF_R14);
        std.debug.assert(@offsetOf(SysVContext, "r15") == SYSV_OFF_R15);
        std.debug.assert(@offsetOf(SysVContext, "rsp") == SYSV_OFF_RSP);
        std.debug.assert(@sizeOf(SysVContext) == SYSV_SIZE);
    }
}

// ─── Extern symbols backed by the .S file ────────────────────────

pub extern fn voltCtxSwap(from: *Context, to: *Context) callconv(.c) void;
pub extern fn voltCoroEntry() callconv(.c) void;

pub const swap = voltCtxSwap;

// ─── Fresh-coroutine bootstrap ───────────────────────────────────

/// Initialize a Context for first dispatch. Assumes `ctx_ptr` is
/// already zeroed by the caller (`Coroutine` struct's default
/// initializer does this; pool-recycled `Coroutine`s also pass
/// through `c.* = .{...}` which resets ctx to default).
///
/// Layout of the initial stack:
///   stack_top - 0    ← initial rsp will point here AFTER the
///                       trampoline's first `ret`
///   stack_top - 8    ← trampoline address; `ret` pops this
///
/// We set `ctx.rsp = stack_top - 8`. On first `swap`, the swap
/// asm loads `rsp` from the context, then `ret` pops the
/// trampoline address and jumps to it. The closure pointer rides
/// in `rbx` (callee-saved); the trampoline reads it from there
/// and calls `closure.run_fn(closure)`.
pub fn initContext(ctx_ptr: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    const top_int: usize = @intFromPtr(stack_top);
    std.debug.assert(top_int & 0xF == 0); // stack_top must be 16-byte aligned

    // Write the trampoline address into stack[top - 8].
    const ret_slot: *usize = @ptrFromInt(top_int - @sizeOf(usize));
    ret_slot.* = @intFromPtr(&voltCoroEntry);

    ctx_ptr.rsp = @intCast(top_int - @sizeOf(usize));
    ctx_ptr.rbx = @intFromPtr(closure);
}

// ─── Tests ───────────────────────────────────────────────────────
//
// Mirror `context_arm64.zig`'s ping-pong test. Only meaningful on
// x86_64 hosts; on ARM64 the dispatch shim at `src/context.zig`
// picks the ARM64 file instead and these tests don't compile in.

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

test "swap: x86_64 ping-pong via trampoline + closure" {
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

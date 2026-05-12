//! POC-A — narrow-save context switch.
//!
//! Strips d8–d15 (NEON callee-saves) from the AAPCS64 save set. Keeps only
//! x19–x28, fp/lr, sp.
//!
//! Layout: 13 u64 = 104 bytes per Context (vs 168 B in spike/coroutines).
//!
//! Hypothesis: removing 4 stp/ldp NEON pairs saves ~2 ns/swap on M-series.
//!
//! Risk: if the Zig calling convention assumes d8–d15 are preserved across
//! a function call, any caller that yields then re-enters a SIMD-using
//! callee will see corrupted FP regs. Per the AAPCS64 spec, d8–d15 ARE
//! callee-saved across normal calls. So we're betting that the *coroutine
//! resume point* (the user's yield/await call site) doesn't have live SIMD
//! regs — which is true for any code not using @Vector / @vector intrinsics
//! across a yield. We add a test that resumes with simd state live to
//! confirm the breakage shape.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch != .aarch64) {
        @compileError("ctx_narrow.zig: ARM64-only");
    }
}

pub const Context = extern struct {
    x19: u64 = 0, x20: u64 = 0,
    x21: u64 = 0, x22: u64 = 0,
    x23: u64 = 0, x24: u64 = 0,
    x25: u64 = 0, x26: u64 = 0,
    x27: u64 = 0, x28: u64 = 0,
    fp:  u64 = 0, lr:  u64 = 0,
    sp:  u64 = 0,
};

comptime {
    std.debug.assert(@offsetOf(Context, "x19") == 0);
    std.debug.assert(@offsetOf(Context, "fp")  == 80);
    std.debug.assert(@offsetOf(Context, "sp")  == 96);
    std.debug.assert(@sizeOf(Context) == 104);
}

pub extern fn voltCtxSwapNarrow(from: *Context, to: *Context) callconv(.c) void;
pub extern fn voltCoroEntryNarrow() callconv(.c) void;

comptime {
    asm (
        \\.global _voltCtxSwapNarrow
        \\.p2align 2
        \\_voltCtxSwapNarrow:
        \\  stp x19, x20, [x0, #0]
        \\  stp x21, x22, [x0, #16]
        \\  stp x23, x24, [x0, #32]
        \\  stp x25, x26, [x0, #48]
        \\  stp x27, x28, [x0, #64]
        \\  stp x29, x30, [x0, #80]
        \\  mov x9, sp
        \\  str x9, [x0, #96]
        // NO NEON saves here.
        \\  ldp x19, x20, [x1, #0]
        \\  ldp x21, x22, [x1, #16]
        \\  ldp x23, x24, [x1, #32]
        \\  ldp x25, x26, [x1, #48]
        \\  ldp x27, x28, [x1, #64]
        \\  ldp x29, x30, [x1, #80]
        \\  ldr x9,  [x1, #96]
        \\  mov sp, x9
        \\  ret
        \\.global _voltCoroEntryNarrow
        \\.p2align 2
        \\_voltCoroEntryNarrow:
        \\  mov x0, x19
        \\  ldr x1, [x19]
        \\  blr x1
        \\  brk #1
    );
}

pub const swap = voltCtxSwapNarrow;

pub fn initContext(ctx_ptr: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    ctx_ptr.* = .{};
    const sp_int: u64 = @intFromPtr(stack_top);
    std.debug.assert(sp_int & 0xF == 0);
    ctx_ptr.sp = sp_int;
    ctx_ptr.lr = @intFromPtr(&voltCoroEntryNarrow);
    ctx_ptr.x19 = @intFromPtr(closure);
}

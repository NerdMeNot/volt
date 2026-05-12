//! Shared ctx switch primitive for POC-B (wide save, same as `src/`).
const std = @import("std");

pub const Context = extern struct {
    x19: u64 = 0, x20: u64 = 0,
    x21: u64 = 0, x22: u64 = 0,
    x23: u64 = 0, x24: u64 = 0,
    x25: u64 = 0, x26: u64 = 0,
    x27: u64 = 0, x28: u64 = 0,
    fp: u64 = 0, lr: u64 = 0,
    sp: u64 = 0,
    d8: u64 = 0, d9: u64 = 0,
    d10: u64 = 0, d11: u64 = 0,
    d12: u64 = 0, d13: u64 = 0,
    d14: u64 = 0, d15: u64 = 0,
};

pub extern fn voltCtxSwap(from: *Context, to: *Context) callconv(.c) void;
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
        \\  ldr x9, [x1, #96]
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

pub const swap = voltCtxSwap;

pub fn initContext(ctx_ptr: *Context, stack_top: [*]u8, closure: *anyopaque) void {
    ctx_ptr.* = .{};
    const sp_int: u64 = @intFromPtr(stack_top);
    std.debug.assert(sp_int & 0xF == 0);
    ctx_ptr.sp = sp_int;
    ctx_ptr.lr = @intFromPtr(&voltCoroEntry);
    ctx_ptr.x19 = @intFromPtr(closure);
}

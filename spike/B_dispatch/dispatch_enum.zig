//! ENUM dispatch — proposed v2 model.
//! Coro stores a `pending_kind: PendingKind` enum. Worker switches on
//! the kind. No vtable, no indirect call. Inlinable, branch-predictable.

const std = @import("std");
const ctx_mod = @import("ctx.zig");

pub const PendingKind = enum(u8) {
    yield,
    done,
    // park, sleep, ... — comptime extensible
};

pub const Coroutine = struct {
    ctx: ctx_mod.Context = .{},
    main_ctx: *ctx_mod.Context = undefined,
    pending_kind: PendingKind = .yield,
    done_flag: bool = false,
    closure: extern struct {
        run_fn: *const fn (*anyopaque) callconv(.c) void = &runFn,
        coro: ?*Coroutine = null,
    } = .{},
};

fn runFn(opaque_ptr: *anyopaque) callconv(.c) void {
    const closure: *@TypeOf(@as(Coroutine, undefined).closure) = @ptrCast(@alignCast(opaque_ptr));
    const coro = closure.coro.?;
    var i: u32 = 0;
    while (i < bench_yields) : (i += 1) {
        coro.pending_kind = .yield;
        ctx_mod.swap(&coro.ctx, coro.main_ctx);
    }
    coro.pending_kind = .done;
    ctx_mod.swap(&coro.ctx, coro.main_ctx);
    unreachable;
}

pub var bench_yields: u32 = 0;

pub fn runLoop(coro: *Coroutine, stack_top: [*]u8) void {
    coro.closure.coro = coro;
    ctx_mod.initContext(&coro.ctx, stack_top, &coro.closure);
    var main_ctx: ctx_mod.Context = .{};
    coro.main_ctx = &main_ctx;
    while (!coro.done_flag) {
        ctx_mod.swap(&main_ctx, &coro.ctx);
        // ENUM direct switch:
        switch (coro.pending_kind) {
            .yield => {
                // The worker's outer loop continues, which re-swaps.
                // Same semantic as the vtable's yield_singleton: re-dispatch.
            },
            .done => {
                coro.done_flag = true;
            },
        }
    }
}

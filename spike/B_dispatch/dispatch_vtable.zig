//! VTABLE dispatch — the current Volt model.
//! After a coro swaps back to the worker, the worker reads
//! `coro.pending_event.subscribe_fn` (function pointer) and calls it.
//! For the yield path this re-pushes the coro to the deque.

const std = @import("std");
const ctx_mod = @import("ctx.zig");

pub const SubscribeFn = *const fn (*anyopaque, *Coroutine) void;

pub const EventSource = struct {
    subscribe_fn: SubscribeFn,
};

pub const Coroutine = struct {
    ctx: ctx_mod.Context = .{},
    main_ctx: *ctx_mod.Context = undefined,
    pending_event: *const EventSource = &yield_singleton,
    done: bool = false,
    closure: extern struct {
        run_fn: *const fn (*anyopaque) callconv(.c) void = &runFn,
        coro: ?*Coroutine = null,
    } = .{},
};

pub var yield_singleton: EventSource = .{ .subscribe_fn = &yieldSubscribe };
pub var done_singleton: EventSource = .{ .subscribe_fn = &doneSubscribe };

fn yieldSubscribe(_: *anyopaque, coro: *Coroutine) void {
    // The worker re-enqueues `coro` after this returns. We just record
    // intent — the dispatch loop drives the actual re-push.
    _ = coro;
}

fn doneSubscribe(_: *anyopaque, coro: *Coroutine) void {
    coro.done = true;
}

/// Coroutine body. Yields N times then sets pending to done.
fn runFn(opaque_ptr: *anyopaque) callconv(.c) void {
    const closure: *@TypeOf(@as(Coroutine, undefined).closure) = @ptrCast(@alignCast(opaque_ptr));
    const coro = closure.coro.?;
    var i: u32 = 0;
    while (i < bench_yields) : (i += 1) {
        coro.pending_event = &yield_singleton;
        ctx_mod.swap(&coro.ctx, coro.main_ctx);
    }
    coro.pending_event = &done_singleton;
    ctx_mod.swap(&coro.ctx, coro.main_ctx);
    unreachable;
}

pub var bench_yields: u32 = 0;

/// Run the dispatch loop until coro.done. Returns ns_per_cycle.
pub fn runLoop(coro: *Coroutine, stack_top: [*]u8) void {
    coro.closure.coro = coro;
    ctx_mod.initContext(&coro.ctx, stack_top, &coro.closure);
    var main_ctx: ctx_mod.Context = .{};
    coro.main_ctx = &main_ctx;
    while (!coro.done) {
        ctx_mod.swap(&main_ctx, &coro.ctx);
        // VTABLE indirect call:
        coro.pending_event.subscribe_fn(@ptrCast(@constCast(coro.pending_event)), coro);
    }
}

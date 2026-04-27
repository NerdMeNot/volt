//! Coroutine: the runtime entity for one stackful task.
//!
//! Composition:
//!   - Context (callee-saved register state, swapped on suspension)
//!   - Stack (mmap'd with PROT_NONE guard page at the bottom)
//!   - `pending_event`: where the worker hands the coroutine after each yield
//!   - `done_flag`: terminal state, set by Done.subscribe
//!   - `join_park`: where joiners park to wait for completion
//!   - `cancel_flag`: cancellation, observed at suspension points
//!   - Closure pointers (the comptime-generated user-fn wrapper)
//!
//! There is NO atomic lifecycle enum on the coroutine. Possession of the
//! `*Coroutine` pointer IS the state — see `docs/design/scheduler-protocol.md`.

const std = @import("std");
const ctx = @import("context_arm64.zig");
const event_source = @import("event_source.zig");
const Park = @import("../scheduler/park.zig").Park;

pub const EventSource = event_source.EventSource;

/// First field of every closure must match this layout — the asm trampoline
/// reads `run_fn` from offset 0 and calls run_fn(closure_ptr).
pub const ClosureBase = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
};

pub const Coroutine = struct {
    /// Saved register state for context switching.
    ctx: ctx.Context = .{},

    /// Where to yield back to. Set by the worker on every dispatch (a
    /// coroutine may resume on a different worker than it last suspended on).
    scheduler_ctx: *ctx.Context,

    /// EventSource the coroutine handed itself off to. Set by the
    /// coroutine before each swap-to-scheduler; read by the worker after
    /// swap-back. The worker calls `subscribe(coro)` which transfers
    /// ownership.
    ///
    /// Initialized to `&yield_singleton` so the field is always valid —
    /// every yield/park overwrites it before the swap.
    pending_event: *const EventSource = &event_source.yield_singleton,

    /// Terminal flag — set by Done.subscribe when the trampoline finishes.
    /// Observable by handles via Job.isCompleted, and by `until_done` checks
    /// in the bootstrap loop.
    done_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Parking slot for `Job.join` / `Task.join`. Joiners parkCurrent on
    /// this; Done.subscribe unparks it when the coroutine completes.
    join_park: Park = .{},

    /// Cancellation flag. Set by Job.cancel(); checked at suspension
    /// points (volt.yield, Park.parkCurrent) which return error.Cancelled.
    cancel_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Owned stack allocation (mmap'd with PROT_NONE guard at the bottom).
    stack: []align(16) u8,

    /// Per-(user_fn, args) destroy callback — knows the type, can free
    /// closure + args correctly.
    destroy_extras_fn: *const fn (std.mem.Allocator, *Coroutine) void,

    /// Type-erased pointer to the closure (heap-allocated, stable).
    closure_ptr: *anyopaque,

    /// Type-erased pointer to the args storage. Carried separately
    /// because args are tuples and tuples can't live inline in extern.
    args_ptr: *anyopaque,

    pub fn isCancelled(self: *const Coroutine) bool {
        return self.cancel_flag.load(.acquire);
    }

    pub fn cancel(self: *Coroutine) void {
        self.cancel_flag.store(true, .release);
        // Note: cancel does NOT unpark the coroutine. A coroutine parked on
        // an unrelated wake source (Mutex, channel, reactor, join) needs
        // the structured-concurrency cleanup at v0.5 to deterministically
        // observe cancellation. For now, the cancelled coro observes the
        // flag at its next yield/park point AFTER it's woken normally.
    }

    pub fn isDone(self: *const Coroutine) bool {
        return self.done_flag.load(.acquire);
    }
};

test "coroutine: initial flags are clear" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    try std.testing.expect(!coro.isCancelled());
    try std.testing.expect(!coro.isDone());
    try std.testing.expectEqual(&event_source.yield_singleton, coro.pending_event);
}

test "coroutine: cancel flag round-trip" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    coro.cancel();
    try std.testing.expect(coro.isCancelled());
}

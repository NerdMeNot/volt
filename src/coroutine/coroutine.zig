//! Coroutine: the runtime entity for one stackful task.
//!
//! Composition:
//!   - Context (callee-saved register state, swapped on suspension)
//!   - Stack (owned heap allocation)
//!   - Lifecycle state (runnable / running / parked / done)
//!   - Cancellation flag (atomic, observable from another thread/coro)
//!   - Type-erased pointer to the closure that runs user code
//!
//! Coroutines are heap-allocated and owned by the scheduler. Spawn returns
//! a `*Coroutine` that's stable for the lifetime of the coroutine. The
//! scheduler frees the Coroutine after the coroutine finishes (or after
//! whoever holds the public handle has observed the result).

const std = @import("std");
const ctx = @import("context_arm64.zig");

/// Lifecycle states. Encoded as enum so transitions are explicit.
pub const State = enum(u8) {
    /// Ready to run when the scheduler picks it up.
    runnable,
    /// Currently executing on a worker.
    running,
    /// Suspended on something (channel, mutex, child join, sleep).
    /// Wakeup is the responsibility of whatever the coroutine is parked on.
    parked,
    /// Function returned normally OR was cancelled and unwound.
    /// At this point the result (if any) is in the closure's result slot.
    done,
};

/// First field of every closure must match this layout — the asm trampoline
/// reads `run_fn` from offset 0 and calls run_fn(closure_ptr).
pub const ClosureBase = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
};

pub const Coroutine = struct {
    /// Saved register state for context switching.
    ctx: ctx.Context = .{},

    /// Where to yield back to. Set by the scheduler before EVERY dispatch —
    /// in multi-worker mode a coroutine may resume on a different worker than
    /// it last suspended on, and must yield back to that worker's main_ctx.
    scheduler_ctx: *ctx.Context,

    /// Lifecycle. Atomic because:
    ///   - Owner worker writes on dispatch (.running) and after swap-back
    ///   - The coroutine itself writes on park (.parked) and on done (.done)
    ///   - Sibling workers read during work-stealing
    ///   - The reactor / completing children write .runnable when waking us
    ///
    /// Use `loadState`, `storeState`, `casState` instead of touching the
    /// atomic value directly — keeps the orderings consistent.
    state: std.atomic.Value(State),

    /// Atomic cancellation flag. Set by Job.cancel(); checked at suspension
    /// points (yield, sleep, channel ops) which then return error.Cancelled.
    cancel_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Owned stack allocation.
    stack: []align(16) u8,

    /// Per-(user_fn, args) destroy callback — knows the type, can free
    /// closure + args correctly.
    destroy_extras_fn: *const fn (std.mem.Allocator, *Coroutine) void,

    /// Type-erased pointer to the closure (heap-allocated, stable).
    /// First field of the closure is run_fn (per ClosureBase).
    closure_ptr: *anyopaque,

    /// Type-erased pointer to the args storage. Carried separately because
    /// args are tuples and tuples can't live inline in extern structs.
    args_ptr: *anyopaque,

    /// One coroutine waiting for this one to complete (e.g., parent in
    /// `Task.join`). When this coroutine transitions to `.done`, the
    /// completing worker CAS-detaches the waiter and re-enqueues it.
    /// Atomic so parent-attach (in Job.join) and child-detach (in the
    /// done-handler) don't race. v0.4 will replace this with a multi-waiter
    /// queue when sync primitives need broadcast wake.
    waiter: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    /// Worker on which this coroutine should be re-enqueued by default
    /// (typically the worker that spawned it). Stealers may move it elsewhere
    /// transparently. v0.3 single-worker leaves this null and uses the
    /// runtime's only worker.
    home_worker: ?*anyopaque = null,

    pub fn loadState(self: *const Coroutine) State {
        return self.state.load(.acquire);
    }

    pub fn storeState(self: *Coroutine, new: State) void {
        self.state.store(new, .release);
    }

    /// CAS the state. Returns null on success, the observed value on failure.
    pub fn casState(self: *Coroutine, expected: State, new: State) ?State {
        return self.state.cmpxchgStrong(expected, new, .acq_rel, .acquire);
    }

    /// Did cancellation request arrive? Atomically observable.
    pub fn isCancelled(self: *const Coroutine) bool {
        return self.cancel_flag.load(.acquire);
    }

    /// Request cancellation. Idempotent. The coroutine observes this at its
    /// next suspension point and unwinds via error.Cancelled.
    pub fn cancel(self: *Coroutine) void {
        self.cancel_flag.store(true, .release);
    }
};

test "coroutine: state enum has expected values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(State.runnable));
    try std.testing.expectEqual(@as(u8, 3), @intFromEnum(State.done));
}

test "coroutine: cancel flag is initially false" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .state = std.atomic.Value(State).init(.runnable),
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    try std.testing.expect(!coro.isCancelled());
    coro.cancel();
    try std.testing.expect(coro.isCancelled());
}

test "coroutine: state CAS round-trip" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .state = std.atomic.Value(State).init(.runnable),
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };

    try std.testing.expectEqual(State.runnable, coro.loadState());
    try std.testing.expectEqual(@as(?State, null), coro.casState(.runnable, .running));
    try std.testing.expectEqual(State.running, coro.loadState());

    // CAS with wrong expected fails and returns observed.
    try std.testing.expectEqual(@as(?State, .running), coro.casState(.runnable, .parked));
    try std.testing.expectEqual(State.running, coro.loadState());
}

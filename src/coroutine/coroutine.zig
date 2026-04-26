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
const stack_mod = @import("stack.zig");

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

    /// Where to yield back to. Set by the scheduler before the first swap-in;
    /// every subsequent suspension also points at this.
    scheduler_ctx: *ctx.Context,

    /// Lifecycle.
    state: State,

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
    /// scheduler unparks the waiter. v0.2 supports a single waiter — that's
    /// enough for join semantics. Broadcast-style multi-waiter wake comes
    /// at v0.4 when sync primitives need it.
    waiter: ?*Coroutine = null,

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
        .state = .runnable,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    try std.testing.expect(!coro.isCancelled());
    coro.cancel();
    try std.testing.expect(coro.isCancelled());
}

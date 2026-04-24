//! Barrier - Synchronization Point for Multiple Tasks
//!
//! A barrier allows N tasks to synchronize at a common point. All tasks
//! must reach the barrier before any can proceed past it.
//!
//! ## Usage
//!
//! ```zig
//! var barrier = Barrier.init(3);  // 3 tasks must reach
//!
//! // Async (returns Future):
//! var future = barrier.waitFuture();
//!
//! // Low-level waiter API:
//! var waiter = Waiter.init();
//! if (!barrier.waitWith(&waiter)) {
//!     // Yield, wait for notification
//! }
//! // All tasks proceed together
//!
//! // Check if this task was the leader (last to arrive)
//! if (waiter.is_leader.load(.acquire)) {
//!     // Do one-time work
//! }
//! ```
//!
//! ## Design
//!
//! - Counter tracks arrivals
//! - Last arrival wakes all waiters
//! - Barrier can be reused after all tasks pass
//!

const std = @import("std");
const thr = @import("../internal/thread.zig");

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const WakeList = @import("../internal/util/wake_list.zig").WakeList;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for barrier synchronization
pub const Waiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Whether this waiter has been released (atomic for cross-thread visibility)
    released: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Whether this waiter was the leader (last to arrive) - atomic for cross-thread visibility
    is_leader: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Intrusive list pointers
    pointers: Pointers(Waiter) = .{},

    /// Debug-mode invocation tracking (detects use-after-free)
    invocation: InvocationId = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Create a waiter with waker already configured.
    /// Reduces the 3-step ceremony (init + setWaker + wait) to 2 steps.
    pub fn initWithWaker(ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{
            .waker_ctx = ctx,
            .waker = wake_fn,
        };
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    pub fn wake(self: *Self) void {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    /// Check if the waiter is ready (released from barrier).
    /// This is the unified completion check method across all sync primitives.
    pub fn isReady(self: *const Self) bool {
        return self.released.load(.acquire);
    }

    /// Check if released (alias for isReady).
    pub const isReleased = isReady;

    /// Get invocation token (for debug tracking)
    pub fn token(self: *const Self) InvocationId.Id {
        return self.invocation.get();
    }

    /// Verify invocation token matches (debug mode)
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.invocation.verify(tok);
    }

    /// Reset for reuse (generates new invocation ID)
    pub fn reset(self: *Self) void {
        self.released.store(false, .release);
        self.is_leader.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }
};

const WaiterList = LinkedList(Waiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Barrier
// ─────────────────────────────────────────────────────────────────────────────

/// A synchronization barrier for N tasks.
pub const Barrier = struct {
    /// Number of tasks required to reach the barrier
    num_tasks: usize,

    /// Current generation (increments each time barrier is released)
    generation: usize,

    /// Number of tasks that have arrived at current generation
    arrived: usize,

    /// Mutex protecting internal state
    mutex: thr.Mutex,

    /// Waiting tasks
    waiters: WaiterList,

    const Self = @This();

    /// Create a barrier for N tasks.
    pub fn init(num_tasks: usize) Self {
        std.debug.assert(num_tasks > 0);
        return .{
            .num_tasks = num_tasks,
            .generation = 0,
            .arrived = 0,
            .mutex = .{},
            .waiters = .{},
        };
    }

    /// Wait at the barrier (low-level waiter API).
    /// Returns true if this is the leader (last to arrive) and all released immediately.
    /// Returns false if waiting for others (task should yield).
    ///
    /// After waking, check waiter.is_leader to see if this task was last.
    /// For the async Future API, use `wait()` instead.
    pub fn waitWith(self: *Self, waiter: *Waiter) bool {
        var wake_list: WakeList(64) = .{};
        var is_leader = false;

        self.mutex.lock();

        const my_generation = self.generation;
        self.arrived += 1;

        if (self.arrived >= self.num_tasks) {
            // We're the last one - release everyone
            is_leader = true;

            // Wake all waiters
            // CRITICAL: Copy waker info BEFORE setting released flag to avoid use-after-free
            while (self.waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.released.store(true, .release);
                w.is_leader.store(false, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }

            // Reset for next generation
            self.arrived = 0;
            self.generation +%= 1;

            self.mutex.unlock();

            // Wake all outside lock
            wake_list.wakeAll();

            // Leader returns immediately
            waiter.released.store(true, .release);
            waiter.is_leader.store(true, .release);
            return true;
        }

        // Not the last one - wait
        waiter.released.store(false, .release);
        waiter.is_leader.store(false, .release);
        self.waiters.pushBack(waiter);

        self.mutex.unlock();

        _ = my_generation;
        return false;
    }

    /// Get the number of tasks that have arrived at the current barrier.
    pub fn arrivedCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.arrived;
    }

    /// Get the total number of tasks required.
    pub fn totalTasks(self: *const Self) usize {
        return self.num_tasks;
    }

    /// Get the current generation (number of times barrier has been released).
    pub fn currentGeneration(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.generation;
    }

    /// Get number of waiters.
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Async API
    // ═══════════════════════════════════════════════════════════════════════

    /// Wait at the barrier, blocking until all tasks have arrived.
    pub fn wait(self: *Self, io: @import("../Io.zig")) BarrierWaitResult {
        var f = io.awaitFuture(self.waitFuture()) catch return .{ .is_leader = false };
        const result = f.await(io);
        return result;
    }

    /// Wait at the barrier.
    ///
    /// Returns a `WaitFuture` that resolves when all tasks have arrived.
    /// This integrates with the scheduler for true async behavior - the task
    /// will be suspended (not spin-waiting) until the barrier is released.
    ///
    /// ## Example
    ///
    /// ```zig
    /// var future = barrier.waitFuture();
    /// // Poll through your runtime...
    /// // When future.poll() returns .ready, barrier has been released
    /// const result = ...; // BarrierWaitResult
    /// if (result.is_leader) {
    ///     // This task was the last to arrive
    /// }
    /// ```
    pub fn waitFuture(self: *Self) WaitFuture {
        return WaitFuture.init(self);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// BarrierWaitResult - for typed returns
// ─────────────────────────────────────────────────────────────────────────────

/// Result of waiting at a barrier.
pub const BarrierWaitResult = struct {
    /// Whether this task was the leader (last to arrive)
    is_leader: bool,
};

// ─────────────────────────────────────────────────────────────────────────────
// WaitFuture - Async barrier wait
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when all tasks have arrived at the barrier.
///
/// This integrates with the scheduler's Future system for true async behavior.
/// The task will be suspended (not spin-waiting or blocking) until the barrier
/// is released.
///
/// ## Usage
///
/// ```zig
/// var future = barrier.waitFuture();
/// // ... poll the future through your runtime ...
/// // When ready, the barrier has been released
/// const result = future.poll(&ctx).ready;
/// if (result.is_leader) {
///     // This task was the last to arrive
/// }
/// ```
pub const WaitFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = BarrierWaitResult;

    /// Reference to the barrier we're waiting on
    barrier: *Barrier,

    /// Our waiter node (embedded to avoid allocation)
    waiter: Waiter,

    /// State machine for the future
    state: State,

    /// Stored waker for when we're woken by barrier release
    stored_waker: ?Waker,

    const State = enum {
        /// Haven't tried to wait yet
        init,
        /// Waiting for barrier release
        waiting,
        /// Barrier released
        ready,
    };

    /// Initialize a new wait future
    pub fn init(barrier: *Barrier) Self {
        return .{
            .barrier = barrier,
            .waiter = Waiter.init(),
            .state = .init,
            .stored_waker = null,
        };
    }

    /// Poll the future - implements Future trait
    ///
    /// Returns `.pending` if barrier not yet released (task will be woken when released).
    /// Returns `.{ .ready = BarrierWaitResult }` when barrier is released.
    pub fn poll(self: *Self, ctx: *Context) PollResult(BarrierWaitResult) {
        switch (self.state) {
            .init => {
                // Cooperative budgeting: yield if this task has consumed its budget
                if (!ctx.pollProceed()) return .pending;

                // First poll - try to wait at the barrier
                // Store the waker so we can be woken when barrier releases
                self.stored_waker = ctx.getWaker().clone();

                // Set up the waiter's callback to wake us via our stored waker
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                // Try to wait
                if (self.barrier.waitWith(&self.waiter)) {
                    // We're the leader - barrier released immediately
                    self.state = .ready;
                    // Clean up stored waker since we don't need it
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = BarrierWaitResult{ .is_leader = true } };
                } else {
                    // Added to wait queue - will be woken when barrier releases
                    self.state = .waiting;
                    return .pending;
                }
            },

            .waiting => {
                // Check if we've been released
                if (self.waiter.isReleased()) {
                    self.state = .ready;
                    // Clean up stored waker
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = BarrierWaitResult{ .is_leader = self.waiter.is_leader.load(.acquire) } };
                }

                // Not yet - update waker in case it changed (task migration)
                const new_waker = ctx.getWaker();
                if (self.stored_waker) |*old| {
                    if (!old.willWakeSame(new_waker)) {
                        old.deinit();
                        self.stored_waker = new_waker.clone();
                    }
                } else {
                    self.stored_waker = new_waker.clone();
                }

                return .pending;
            },

            .ready => {
                // Already released
                return .{ .ready = BarrierWaitResult{ .is_leader = self.waiter.is_leader.load(.acquire) } };
            },
        }
    }

    /// Cancel the wait
    ///
    /// Note: Barrier doesn't support cancellation - once you've arrived,
    /// you must wait for all others. This just cleans up the waker.
    pub fn cancel(self: *Self) void {
        // Clean up stored waker
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    /// Deinit the future
    ///
    /// Cleans up resources. Note that barrier waits cannot be cancelled
    /// once started - the task count has already been incremented.
    pub fn deinit(self: *Self) void {
        self.cancel();
    }

    /// Callback invoked by Barrier when barrier is released
    /// This bridges the Barrier's WakerFn to the Future's Waker
    fn wakeCallback(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.stored_waker) |*w| {
            // Wake the task through the Future system's Waker
            w.wakeByRef();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier - single task" {
    var barrier = Barrier.init(1);

    var waiter = Waiter.init();
    const immediate = barrier.waitWith(&waiter);

    // Single task is always leader and returns immediately
    try std.testing.expect(immediate);
    try std.testing.expect(waiter.is_leader.load(.acquire));
    try std.testing.expect(waiter.isReleased());
}

test "Barrier - multiple tasks sync" {
    var barrier = Barrier.init(3);
    var woken: [2]bool = .{ false, false };

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // First two tasks wait
    var waiters: [2]Waiter = undefined;
    for (&waiters, 0..) |*w, i| {
        w.* = Waiter.init();
        w.setWaker(@ptrCast(&woken[i]), TestWaker.wake);
        const immediate = barrier.waitWith(w);
        try std.testing.expect(!immediate);
    }

    try std.testing.expectEqual(@as(usize, 2), barrier.arrivedCount());
    try std.testing.expectEqual(@as(usize, 2), barrier.waiterCount());

    // Third task arrives - all released
    var leader_waiter = Waiter.init();
    const immediate = barrier.waitWith(&leader_waiter);

    try std.testing.expect(immediate);
    try std.testing.expect(leader_waiter.is_leader.load(.acquire));

    // All others woken
    for (woken) |w| {
        try std.testing.expect(w);
    }
    for (&waiters) |*w| {
        try std.testing.expect(w.isReleased());
        try std.testing.expect(!w.is_leader.load(.acquire));
    }

    // Barrier reset
    try std.testing.expectEqual(@as(usize, 0), barrier.arrivedCount());
    try std.testing.expectEqual(@as(usize, 1), barrier.currentGeneration());
}

test "Barrier - reusable" {
    var barrier = Barrier.init(2);

    // First round
    var w1 = Waiter.init();
    try std.testing.expect(!barrier.waitWith(&w1));

    var w2 = Waiter.init();
    try std.testing.expect(barrier.waitWith(&w2));
    try std.testing.expect(w2.is_leader.load(.acquire));

    try std.testing.expectEqual(@as(usize, 1), barrier.currentGeneration());

    // Second round
    var w3 = Waiter.init();
    try std.testing.expect(!barrier.waitWith(&w3));

    var w4 = Waiter.init();
    try std.testing.expect(barrier.waitWith(&w4));
    try std.testing.expect(w4.is_leader.load(.acquire));

    try std.testing.expectEqual(@as(usize, 2), barrier.currentGeneration());
}

test "Barrier - only one leader" {
    var barrier = Barrier.init(3);

    var waiters: [3]Waiter = undefined;
    var leaders: usize = 0;

    for (&waiters) |*w| {
        w.* = Waiter.init();
        _ = barrier.waitWith(w);
        if (w.is_leader.load(.acquire)) leaders += 1;
    }

    try std.testing.expectEqual(@as(usize, 1), leaders);
}

test "Barrier - large task count" {
    // Test with many tasks arriving at the barrier
    const num_tasks = 100;
    var barrier = Barrier.init(num_tasks);
    var woken_count: usize = 0;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const count: *usize = @ptrCast(@alignCast(ctx));
            count.* += 1;
        }
    };

    var waiters: [num_tasks]Waiter = undefined;

    // First N-1 tasks wait
    for (0..num_tasks - 1) |i| {
        waiters[i] = Waiter.init();
        waiters[i].setWaker(@ptrCast(&woken_count), TestWaker.wake);
        const immediate = barrier.waitWith(&waiters[i]);
        try std.testing.expect(!immediate);
        try std.testing.expectEqual(i + 1, barrier.arrivedCount());
    }

    try std.testing.expectEqual(@as(usize, num_tasks - 1), barrier.waiterCount());
    try std.testing.expectEqual(@as(usize, 0), woken_count);

    // Last task arrives - all released
    waiters[num_tasks - 1] = Waiter.init();
    const immediate = barrier.waitWith(&waiters[num_tasks - 1]);

    try std.testing.expect(immediate);
    try std.testing.expect(waiters[num_tasks - 1].is_leader.load(.acquire));

    // All N-1 waiters should have been woken
    try std.testing.expectEqual(@as(usize, num_tasks - 1), woken_count);

    // All waiters should be released
    for (&waiters) |*w| {
        try std.testing.expect(w.isReleased());
    }

    // Exactly one leader
    var leaders: usize = 0;
    for (&waiters) |*w| {
        if (w.is_leader.load(.acquire)) leaders += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), leaders);

    // Barrier reset for next generation
    try std.testing.expectEqual(@as(usize, 0), barrier.arrivedCount());
    try std.testing.expectEqual(@as(usize, 1), barrier.currentGeneration());
}

test "Barrier - multiple generations leader consistency" {
    // Verify exactly one leader per generation across multiple rounds
    var barrier = Barrier.init(5);
    const num_generations = 10;

    for (0..num_generations) |gen| {
        var waiters: [5]Waiter = undefined;
        var leaders: usize = 0;

        for (&waiters) |*w| {
            w.* = Waiter.init();
            _ = barrier.waitWith(w);
        }

        // Count leaders in this generation
        for (&waiters) |*w| {
            if (w.is_leader.load(.acquire)) leaders += 1;
        }

        try std.testing.expectEqual(@as(usize, 1), leaders);
        try std.testing.expectEqual(gen + 1, barrier.currentGeneration());
    }
}

test "Barrier - arrived count tracking" {
    var barrier = Barrier.init(5);

    // Verify arrived count increments correctly
    try std.testing.expectEqual(@as(usize, 0), barrier.arrivedCount());

    var w1 = Waiter.init();
    _ = barrier.waitWith(&w1);
    try std.testing.expectEqual(@as(usize, 1), barrier.arrivedCount());

    var w2 = Waiter.init();
    _ = barrier.waitWith(&w2);
    try std.testing.expectEqual(@as(usize, 2), barrier.arrivedCount());

    var w3 = Waiter.init();
    _ = barrier.waitWith(&w3);
    try std.testing.expectEqual(@as(usize, 3), barrier.arrivedCount());

    var w4 = Waiter.init();
    _ = barrier.waitWith(&w4);
    try std.testing.expectEqual(@as(usize, 4), barrier.arrivedCount());

    // Last task triggers reset
    var w5 = Waiter.init();
    const immediate = barrier.waitWith(&w5);
    try std.testing.expect(immediate);
    try std.testing.expectEqual(@as(usize, 0), barrier.arrivedCount());
}

test "Barrier - waiter reset and reuse" {
    var barrier = Barrier.init(2);

    // First usage
    var waiter = Waiter.init();
    try std.testing.expect(!barrier.waitWith(&waiter));

    var leader = Waiter.init();
    try std.testing.expect(barrier.waitWith(&leader));

    try std.testing.expect(waiter.isReleased());
    try std.testing.expect(!waiter.is_leader.load(.acquire));

    // Reset waiter for reuse
    waiter.reset();
    try std.testing.expect(!waiter.isReleased());
    try std.testing.expect(!waiter.is_leader.load(.acquire));
    try std.testing.expect(waiter.waker == null);

    // Second usage with same waiter
    try std.testing.expect(!barrier.waitWith(&waiter));

    var leader2 = Waiter.init();
    try std.testing.expect(barrier.waitWith(&leader2));

    try std.testing.expect(waiter.isReleased());
    try std.testing.expect(!waiter.is_leader.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), barrier.currentGeneration());
}

// ─────────────────────────────────────────────────────────────────────────────
// WaitFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "WaitFuture - single task immediate" {
    var barrier = Barrier.init(1);

    // Create future and poll - should complete immediately as leader
    var future = barrier.waitFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.ready.is_leader);
}

test "WaitFuture - waits when not all arrived" {
    var barrier = Barrier.init(2);
    var waker_called = false;

    // Create a test waker that tracks calls
    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) Waker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // First task waits
    var future = barrier.waitFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);
    try std.testing.expectEqual(@as(usize, 1), barrier.waiterCount());

    // Second task arrives - should wake the first
    var waiter2 = Waiter.init();
    const immediate = barrier.waitWith(&waiter2);
    try std.testing.expect(immediate);
    try std.testing.expect(waiter2.is_leader.load(.acquire));
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(!result2.ready.is_leader); // First waiter is not leader
}

test "WaitFuture - leader detection" {
    var barrier = Barrier.init(2);

    // First task waits (not leader)
    var waiter1 = Waiter.init();
    try std.testing.expect(!barrier.waitWith(&waiter1));

    // Second task arrives via future (is leader)
    var future = barrier.waitFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.ready.is_leader);

    // First waiter should also be released
    try std.testing.expect(waiter1.isReleased());
    try std.testing.expect(!waiter1.is_leader.load(.acquire));
}

test "WaitFuture - multiple futures" {
    var barrier = Barrier.init(3);

    // Create multiple futures
    var future1 = barrier.waitFuture();
    var future2 = barrier.waitFuture();
    var future3 = barrier.waitFuture();

    var ctx = Context{ .waker = &future_mod.noop_waker };

    // First two should return pending
    try std.testing.expect(future1.poll(&ctx).isPending());
    try std.testing.expect(future2.poll(&ctx).isPending());

    // Third should trigger release and return ready as leader
    const result3 = future3.poll(&ctx);
    try std.testing.expect(result3.isReady());
    try std.testing.expect(result3.ready.is_leader);

    // Now poll others - should be ready and not leaders
    const result1 = future1.poll(&ctx);
    try std.testing.expect(result1.isReady());
    try std.testing.expect(!result1.ready.is_leader);

    const result2 = future2.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(!result2.ready.is_leader);

    future1.deinit();
    future2.deinit();
    future3.deinit();
}

test "WaitFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(WaitFuture));
    try std.testing.expect(WaitFuture.Output == BarrierWaitResult);
}

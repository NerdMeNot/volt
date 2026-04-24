//! Notify - Task Notification Primitive
//!
//! A synchronization primitive for notifying one or many waiting tasks.
//! Similar to a condition variable but designed for async task coordination.
//!
//! ## Usage
//!
//! ```zig
//! var notify = Notify.init();
//!
//! // Async (returns Future):
//! var future = notify.waitFuture();
//!
//! // Low-level waiter API:
//! var waiter = Waiter.init();
//! notify.waitWith(&waiter);
//!
//! // Notifying task:
//! notify.notifyOne();  // Wakes one waiter
//! // or
//! notify.notifyAll();  // Wakes all waiters
//! ```
//!
//! ## Design
//!
//! - Waiters are stored in an intrusive linked list (zero allocation)
//! - Batch waking after releasing lock to avoid holding lock during wake
//! - Permit-based semantics to handle notify-before-wait race
//!

const std = @import("std");
const thr = @import("../internal/thread.zig");

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const WakeList = @import("../internal/util/wake_list.zig").WakeList;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// A waiter node that embeds its own list pointers.
/// Designed to be stack-allocated by the waiting task.
pub const Waiter = struct {
    /// Waker to invoke when notified
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Whether this waiter has been notified (atomic for cross-thread visibility)
    notified: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Intrusive list pointers
    pointers: Pointers(Waiter) = .{},

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

    /// Set the waker for this waiter
    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    /// Wake this waiter (SAFE: copies waker info before setting flag)
    pub fn wake(self: *Self) void {
        // CRITICAL: Copy waker info BEFORE setting notified flag.
        // Once notified is true, the waiting thread may destroy this waiter.
        const waker_fn = self.waker;
        const waker_ctx = self.waker_ctx;

        // Now safe to set the flag - we have our own copies
        // Release pairs with acquire in isReady()
        self.notified.store(true, .release);

        // Wake using copied function pointers
        if (waker_fn) |wf| {
            if (waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    /// Check if the waiter is ready (notified).
    /// This is the unified completion check method across all sync primitives.
    pub fn isReady(self: *const Self) bool {
        return self.notified.load(.acquire);
    }

    /// Check if notified (alias for isReady, uses SeqCst for cross-thread visibility).
    pub const isNotified = isReady;

    /// Reset for reuse
    pub fn reset(self: *Self) void {
        self.notified.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Intrusive list of waiters
const WaiterList = LinkedList(Waiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Notify State
// ─────────────────────────────────────────────────────────────────────────────

/// State bits for Notify
const State = struct {
    /// A permit is available (notify was called with no waiters)
    const PERMIT: u8 = 1 << 0;
    /// Waiters list is not empty
    const WAITING: u8 = 1 << 1;
};

// ─────────────────────────────────────────────────────────────────────────────
// Notify
// ─────────────────────────────────────────────────────────────────────────────

/// A synchronization primitive for notifying waiting tasks.
pub const Notify = struct {
    /// State flags (PERMIT, WAITING)
    state: std.atomic.Value(u8),

    /// Mutex protecting the waiters list
    mutex: thr.Mutex,

    /// Waiters list
    waiters: WaiterList,

    const Self = @This();

    /// Create a new Notify.
    pub fn init() Self {
        return .{
            .state = std.atomic.Value(u8).init(0),
            .mutex = .{},
            .waiters = .{},
        };
    }

    /// Notify one waiting task.
    /// If no task is waiting, stores a permit that will be consumed by the next wait.
    pub fn notifyOne(self: *Self) void {
        // Fast path: try to transition EMPTY -> NOTIFIED
        var curr = self.state.load(.acquire);

        while (curr & State.WAITING == 0) {
            // No waiters - try to store a permit via CAS
            const new = curr | State.PERMIT;
            const result = self.state.cmpxchgWeak(curr, new, .acq_rel, .acquire);
            if (result == null) {
                // Successfully stored permit
                return;
            }
            // CAS failed, reload and retry
            curr = result.?;
        }

        // Slow path: there are waiters, must acquire lock
        self.mutex.lock();

        // CRITICAL: Reload state while holding lock.
        // State can only transition OUT of WAITING while lock is held
        curr = self.state.load(.acquire);

        if (curr & State.WAITING == 0) {
            // No waiters anymore, store permit (under lock, release sufficient)
            _ = self.state.fetchOr(State.PERMIT, .release);
            self.mutex.unlock();
            return;
        }

        // Pop one waiter
        const waiter = self.waiters.popFront();

        // Update state if list is now empty (under lock, release sufficient)
        if (self.waiters.isEmpty()) {
            _ = self.state.fetchAnd(~State.WAITING, .release);
        }

        self.mutex.unlock();

        // Wake outside the lock
        if (waiter) |w| {
            w.wake();
        }
    }

    /// Notify all waiting tasks.
    /// Does NOT store a permit if no tasks are waiting.
    pub fn notifyAll(self: *Self) void {
        // Fast path: check if anyone is waiting
        const state = self.state.load(.acquire);
        if (state & State.WAITING == 0) {
            return;
        }

        // Collect waiters to wake
        var wake_list: WakeList(32) = .{};

        self.mutex.lock();

        // Drain all waiters
        // CRITICAL: Copy waker info BEFORE setting notified flag to avoid use-after-free
        while (self.waiters.popFront()) |waiter| {
            const waker_fn = waiter.waker;
            const waker_ctx = waiter.waker_ctx;

            // Now safe to set the flag — release pairs with acquire in isReady()
            waiter.notified.store(true, .release);

            if (waker_fn) |wf| {
                if (waker_ctx) |ctx| {
                    wake_list.push(.{ .context = ctx, .wake_fn = wf });
                }
            }
        }

        // Clear WAITING flag (under lock, release sufficient)
        _ = self.state.fetchAnd(~State.WAITING, .release);

        self.mutex.unlock();

        // Wake all outside the lock
        wake_list.wakeAll();
    }

    /// Wait for notification (low-level waiter API).
    /// Returns true if a permit was consumed (immediate return).
    /// Returns false if the waiter was added to the queue (task should yield).
    ///
    /// The waiter must be kept alive until notified or cancelled.
    /// For the async Future API, use `wait()` instead.
    pub fn waitWith(self: *Self, waiter: *Waiter) bool {
        // Fast path: check for permit
        var state = self.state.load(.acquire);
        if (state & State.PERMIT != 0) {
            // Try to consume permit
            const result = self.state.cmpxchgWeak(
                state,
                state & ~State.PERMIT,
                .acq_rel,
                .acquire,
            );
            if (result == null) {
                // Consumed permit
                waiter.notified.store(true, .release);
                return true;
            }
            // CAS failed, fall through to slow path
        }

        // Slow path: add to waiters list
        self.mutex.lock();

        // Re-check for permit under lock
        state = self.state.load(.acquire);
        if (state & State.PERMIT != 0) {
            _ = self.state.fetchAnd(~State.PERMIT, .release);
            self.mutex.unlock();
            waiter.notified.store(true, .release);
            return true;
        }

        // Add to waiters list
        waiter.notified.store(false, .release);
        self.waiters.pushBack(waiter);

        // Set WAITING flag (under lock, release sufficient)
        _ = self.state.fetchOr(State.WAITING, .release);

        self.mutex.unlock();

        return false;
    }

    /// Cancel a wait operation.
    /// Removes the waiter from the queue if it was added.
    /// Safe to call even if the waiter was already notified.
    pub fn cancelWait(self: *Self, waiter: *Waiter) void {
        // Quick check if already notified
        if (waiter.isNotified()) {
            return;
        }

        self.mutex.lock();

        // Check again under lock
        if (waiter.isNotified()) {
            self.mutex.unlock();
            return;
        }

        // Remove from list if linked
        if (WaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();

            // Update state if list is now empty
            if (self.waiters.isEmpty()) {
                _ = self.state.fetchAnd(~State.WAITING, .release);
            }
        }

        self.mutex.unlock();
    }

    /// Check if there are pending permits (for testing/debugging).
    pub fn hasPermit(self: *const Self) bool {
        return self.state.load(.acquire) & State.PERMIT != 0;
    }

    /// Get the number of waiters (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Async API
    // ═══════════════════════════════════════════════════════════════════════

    /// Wait for a notification, blocking the current task.
    pub fn wait(self: *Self, io: @import("../Io.zig")) void {
        var f = io.awaitFuture(self.waitFuture()) catch return;
        _ = f.await(io);
    }

    /// Wait for notification.
    ///
    /// Returns a `WaitFuture` that resolves when the notification is received.
    /// This integrates with the scheduler for true async behavior - the task
    /// will be suspended (not spin-waiting) until notified.
    ///
    /// ## Example
    ///
    /// ```zig
    /// var future = notify.waitFuture();
    /// // Poll through your runtime...
    /// // When future.poll() returns .ready, you've been notified
    /// ```
    pub fn waitFuture(self: *Self) WaitFuture {
        return WaitFuture.init(self);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// WaitFuture - Async notification wait
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when a notification is received.
///
/// This integrates with the scheduler's Future system for true async behavior.
/// The task will be suspended (not spin-waiting or blocking) until notified.
///
/// ## Usage
///
/// ```zig
/// var future = notify.waitFuture();
/// // ... poll the future through your runtime ...
/// // When ready, you've been notified
/// ```
pub const WaitFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = void;

    /// Reference to the notify we're waiting on
    notify: *Notify,

    /// Our waiter node (embedded to avoid allocation)
    waiter: Waiter,

    /// State machine for the future
    state: FutureState,

    /// Stored waker for when we're woken by notify
    stored_waker: ?Waker,

    const FutureState = enum {
        /// Haven't tried to wait yet
        init,
        /// Waiting in queue for notification
        waiting,
        /// Notified
        notified,
    };

    /// Initialize a new wait future
    pub fn init(notify_ptr: *Notify) Self {
        return .{
            .notify = notify_ptr,
            .waiter = Waiter.init(),
            .state = .init,
            .stored_waker = null,
        };
    }

    /// Poll the future - implements Future trait
    ///
    /// Returns `.pending` if not yet notified (task will be woken when notified).
    /// Returns `.{ .ready = {} }` when notification is received.
    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        switch (self.state) {
            .init => {
                // Cooperative budgeting: yield if this task has consumed its budget
                if (!ctx.pollProceed()) return .pending;

                // First poll - try to wait
                // Store the waker so we can be woken when notified
                self.stored_waker = ctx.getWaker().clone();

                // Set up the waiter's callback to wake us via our stored waker
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                // Try to wait (may consume permit immediately)
                if (self.notify.waitWith(&self.waiter)) {
                    // Got notification immediately (permit consumed)
                    self.state = .notified;
                    // Clean up stored waker since we don't need it
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                } else {
                    // Added to wait queue - will be woken when notified
                    self.state = .waiting;
                    return .pending;
                }
            },

            .waiting => {
                // Check if we've been notified
                if (self.waiter.isNotified()) {
                    self.state = .notified;
                    // Clean up stored waker
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
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

            .notified => {
                // Already notified
                return .{ .ready = {} };
            },
        }
    }

    /// Cancel the wait operation
    ///
    /// If not yet notified, removes us from the wait queue.
    /// If already notified, this is a no-op.
    pub fn cancel(self: *Self) void {
        if (self.state == .waiting) {
            self.notify.cancelWait(&self.waiter);
        }
        // Clean up stored waker
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    /// Deinit the future
    ///
    /// If waiting, cancels the wait operation.
    pub fn deinit(self: *Self) void {
        self.cancel();
    }

    /// Callback invoked by Notify when notification is received
    /// This bridges the Notify's WakerFn to the Future's Waker
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

test "Notify - notify before wait consumes permit" {
    var notify = Notify.init();

    // Notify with no waiters
    notify.notifyOne();
    try std.testing.expect(notify.hasPermit());

    // Wait should consume permit immediately
    var waiter = Waiter.init();
    const consumed = notify.waitWith(&waiter);

    try std.testing.expect(consumed);
    try std.testing.expect(waiter.isNotified());
    try std.testing.expect(!notify.hasPermit());
}

test "Notify - wait then notify" {
    var notify = Notify.init();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var waiter = Waiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Wait should return false (added to queue)
    const consumed = notify.waitWith(&waiter);
    try std.testing.expect(!consumed);
    try std.testing.expect(!waiter.isNotified());
    try std.testing.expectEqual(@as(usize, 1), notify.waiterCount());

    // Notify should wake the waiter
    notify.notifyOne();
    try std.testing.expect(waiter.isNotified());
    try std.testing.expect(woken);
    try std.testing.expectEqual(@as(usize, 0), notify.waiterCount());
}

test "Notify - notifyAll wakes all waiters" {
    var notify = Notify.init();
    var woken: [3]bool = .{ false, false, false };

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var waiters: [3]Waiter = undefined;
    for (&waiters, 0..) |*w, i| {
        w.* = Waiter.init();
        w.setWaker(@ptrCast(&woken[i]), TestWaker.wake);
        _ = notify.waitWith(w);
    }

    try std.testing.expectEqual(@as(usize, 3), notify.waiterCount());

    notify.notifyAll();

    for (woken) |w| {
        try std.testing.expect(w);
    }
    try std.testing.expectEqual(@as(usize, 0), notify.waiterCount());
}

test "Notify - cancel removes waiter" {
    var notify = Notify.init();

    var waiter1 = Waiter.init();
    var waiter2 = Waiter.init();

    _ = notify.waitWith(&waiter1);
    _ = notify.waitWith(&waiter2);

    try std.testing.expectEqual(@as(usize, 2), notify.waiterCount());

    // Cancel first waiter
    notify.cancelWait(&waiter1);
    try std.testing.expectEqual(@as(usize, 1), notify.waiterCount());

    // Notify should wake remaining waiter
    notify.notifyOne();
    try std.testing.expect(waiter2.isNotified());
    try std.testing.expect(!waiter1.isNotified());
}

test "Notify - notifyAll with no waiters is no-op" {
    var notify = Notify.init();

    // Should not store permit
    notify.notifyAll();
    try std.testing.expect(!notify.hasPermit());
}

test "Notify - notifyOne wakes exactly one waiter" {
    var notify = Notify.init();
    var woken: [3]bool = .{ false, false, false };

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Add three waiters
    var waiters: [3]Waiter = undefined;
    for (&waiters, 0..) |*w, i| {
        w.* = Waiter.init();
        w.setWaker(@ptrCast(&woken[i]), TestWaker.wake);
        _ = notify.waitWith(w);
    }

    try std.testing.expectEqual(@as(usize, 3), notify.waiterCount());

    // notifyOne should wake exactly one (the first in FIFO order)
    notify.notifyOne();

    try std.testing.expect(woken[0]);
    try std.testing.expect(!woken[1]);
    try std.testing.expect(!woken[2]);
    try std.testing.expectEqual(@as(usize, 2), notify.waiterCount());

    // Clean up remaining waiters
    notify.notifyAll();
}

test "Notify - permit does not accumulate" {
    var notify = Notify.init();

    // Multiple notifyOne calls should store only one permit
    notify.notifyOne();
    notify.notifyOne();
    notify.notifyOne();

    try std.testing.expect(notify.hasPermit());

    // First wait consumes the permit
    var waiter1 = Waiter.init();
    const consumed1 = notify.waitWith(&waiter1);
    try std.testing.expect(consumed1);
    try std.testing.expect(waiter1.isNotified());

    // Second wait should block (no permit left)
    var waiter2 = Waiter.init();
    const consumed2 = notify.waitWith(&waiter2);
    try std.testing.expect(!consumed2);
    try std.testing.expect(!waiter2.isNotified());

    // Clean up
    notify.notifyOne();
}

test "Notify - rapid notify wait cycles" {
    var notify = Notify.init();

    for (0..100) |_| {
        // Notify first (stores permit)
        notify.notifyOne();
        try std.testing.expect(notify.hasPermit());

        // Wait consumes permit
        var waiter = Waiter.init();
        const consumed = notify.waitWith(&waiter);
        try std.testing.expect(consumed);
        try std.testing.expect(waiter.isNotified());
        try std.testing.expect(!notify.hasPermit());
    }

    // Final state should be clean
    try std.testing.expect(!notify.hasPermit());
    try std.testing.expectEqual(@as(usize, 0), notify.waiterCount());
}

test "Notify - many waiters notify one at a time" {
    var notify = Notify.init();
    const num_waiters = 100;
    var woken_count: usize = 0;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const count: *usize = @ptrCast(@alignCast(ctx));
            count.* += 1;
        }
    };

    // Add many waiters
    var waiters: [num_waiters]Waiter = undefined;
    for (&waiters) |*w| {
        w.* = Waiter.init();
        w.setWaker(@ptrCast(&woken_count), TestWaker.wake);
        _ = notify.waitWith(w);
    }

    try std.testing.expectEqual(@as(usize, num_waiters), notify.waiterCount());
    try std.testing.expectEqual(@as(usize, 0), woken_count);

    // Notify one at a time
    for (0..num_waiters) |i| {
        notify.notifyOne();
        try std.testing.expectEqual(i + 1, woken_count);
        try std.testing.expectEqual(num_waiters - i - 1, notify.waiterCount());
    }

    // All should be woken in FIFO order
    for (&waiters) |*w| {
        try std.testing.expect(w.isNotified());
    }
}

test "Notify - waiter reset and reuse" {
    var notify = Notify.init();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // First use
    var waiter = Waiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    _ = notify.waitWith(&waiter);

    notify.notifyOne();
    try std.testing.expect(waiter.isNotified());
    try std.testing.expect(woken);

    // Reset for reuse
    waiter.reset();
    woken = false;

    try std.testing.expect(!waiter.isNotified());
    try std.testing.expect(waiter.waker == null);

    // Second use
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    _ = notify.waitWith(&waiter);

    notify.notifyOne();
    try std.testing.expect(waiter.isNotified());
    try std.testing.expect(woken);
}

test "Notify.Waiter - initWithWaker convenience" {
    var notify = Notify.init();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Create waiter with initWithWaker (fewer steps than init + setWaker)
    var waiter = Waiter.initWithWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!notify.waitWith(&waiter));
    try std.testing.expect(!waiter.isReady()); // Using isReady (unified API)

    notify.notifyOne();
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(woken);
}

test "Notify.Waiter - isReady is alias for isNotified" {
    var notify = Notify.init();
    var waiter = Waiter.init();

    // Both should return false initially
    try std.testing.expect(!waiter.isReady());
    try std.testing.expect(!waiter.isNotified());

    // Notify with permit
    notify.notifyOne();

    // Wait should consume permit
    try std.testing.expect(notify.waitWith(&waiter));

    // Both should return true
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(waiter.isNotified());
}

//! Interval Timer
//!
//! Provides a recurring timer that fires at regular intervals.
//! Uses the waiter pattern consistent with other async primitives.
//!
//! ## Usage
//!
//! ```zig
//! const time = @import("volt").time;
//!
//! // Create an interval that ticks every 100ms
//! var interval = time.Interval.init(time.Duration.fromMillis(100));
//!
//! // In async context
//! while (true) {
//!     var waiter = time.IntervalWaiter.init();
//!     if (!interval.tick(&waiter)) {
//!         // Yield to scheduler
//!     }
//!     // Handle tick
//!     doPeriodicWork();
//! }
//! ```
//!
//! ## Design
//!
//! - Fixed interval between ticks
//! - Compensates for drift (missed ticks don't accumulate delay)
//! - Uses waiter pattern for async integration
//!

const std = @import("std");
const thr = @import("../internal/thread.zig");
const builtin = @import("builtin");

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;

const time_types = @import("../time.zig");
pub const Duration = time_types.Duration;
pub const Instant = time_types.Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for interval tick.
pub const IntervalWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether the tick has completed (atomic for cross-thread visibility).
    /// Set by timer driver thread, read by the waiting task.
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    pointers: Pointers(IntervalWaiter) = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
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

    pub fn isComplete(self: *const Self) bool {
        return self.complete.load(.acquire);
    }

    pub fn reset(self: *Self) void {
        self.complete.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

const IntervalWaiterList = LinkedList(IntervalWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// MissedTickBehavior
// ─────────────────────────────────────────────────────────────────────────────

/// How to handle missed ticks when the interval falls behind.
pub const MissedTickBehavior = enum {
    /// Fire immediately for each missed tick (burst mode).
    burst,

    /// Skip missed ticks and schedule from now.
    skip,

    /// Delay the next tick (default).
    delay,
};

// ─────────────────────────────────────────────────────────────────────────────
// Interval
// ─────────────────────────────────────────────────────────────────────────────

/// A recurring timer that fires at regular intervals.
pub const Interval = struct {
    /// Period between ticks
    period: Duration,

    /// Next tick deadline
    next_tick: Instant,

    /// How to handle missed ticks
    missed_tick_behavior: MissedTickBehavior,

    /// Waiters list
    waiters: IntervalWaiterList,

    /// Whether a tick is ready
    tick_ready: bool,

    /// Mutex protecting state
    mutex: thr.Mutex,

    const Self = @This();

    /// Create an interval with the given period.
    /// First tick will be after `period` time has passed.
    pub fn init(period: Duration) Self {
        return .{
            .period = period,
            .next_tick = Instant.now().add(period),
            .missed_tick_behavior = .delay,
            .waiters = .{},
            .tick_ready = false,
            .mutex = .{},
        };
    }

    /// Create an interval starting at a specific instant.
    pub fn initAt(start: Instant, period: Duration) Self {
        return .{
            .period = period,
            .next_tick = start,
            .missed_tick_behavior = .delay,
            .waiters = .{},
            .tick_ready = false,
            .mutex = .{},
        };
    }

    /// Create an interval that fires immediately first.
    pub fn initImmediate(period: Duration) Self {
        return .{
            .period = period,
            // Set next_tick to future so after consuming tick_ready,
            // subsequent ticks wait the full period
            .next_tick = Instant.now().add(period),
            .missed_tick_behavior = .delay,
            .waiters = .{},
            .tick_ready = true,
            .mutex = .{},
        };
    }

    /// Set missed tick behavior.
    pub fn setMissedTickBehavior(self: *Self, behavior: MissedTickBehavior) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.missed_tick_behavior = behavior;
    }

    /// Get the period.
    pub fn getPeriod(self: *const Self) Duration {
        return self.period;
    }

    /// Reset the interval to start from now.
    pub fn reset(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.next_tick = Instant.now().add(self.period);
        self.tick_ready = false;
    }

    /// Reset the interval immediately (next tick fires now).
    pub fn resetImmediate(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.next_tick = Instant.now();
        self.tick_ready = true;
    }

    /// Try to get a tick without waiting.
    /// Returns true if a tick is ready.
    pub fn tryTick(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.checkAndAdvance();
    }

    /// Wait for the next tick.
    /// Returns true if a tick is ready immediately.
    /// Returns false if the waiter was added (task should yield).
    pub fn tick(self: *Self, waiter: *IntervalWaiter) bool {
        self.mutex.lock();

        // Check if tick is ready
        if (self.checkAndAdvance()) {
            self.mutex.unlock();
            waiter.complete.store(true, .release);
            return true;
        }

        // Not ready - add waiter
        waiter.complete.store(false, .release);
        self.waiters.pushBack(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Cancel a pending tick wait.
    pub fn cancelTick(self: *Self, waiter: *IntervalWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();

        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        if (IntervalWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        self.mutex.unlock();
    }

    /// Poll for tick and wake waiters.
    /// Call this periodically from the event loop.
    pub fn poll(self: *Self) void {
        self.mutex.lock();

        if (self.checkAndAdvance()) {
            // Wake one waiter (one tick = one wake)
            if (self.waiters.popFront()) |w| {
                w.complete.store(true, .release);
                self.mutex.unlock();
                w.wake();
                return;
            }
            // No waiter, mark tick as ready for next call
            self.tick_ready = true;
        }

        self.mutex.unlock();
    }

    /// Check if deadline passed and advance to next.
    /// Returns true if a tick should fire.
    /// Caller must hold mutex.
    fn checkAndAdvance(self: *Self) bool {
        // If a tick is already ready, consume it
        if (self.tick_ready) {
            self.tick_ready = false;
            return true;
        }

        const now = Instant.now();

        if (!now.isBefore(self.next_tick)) {
            // Tick!
            switch (self.missed_tick_behavior) {
                .burst => {
                    // Just advance by one period (caller should loop to catch up)
                    self.next_tick = self.next_tick.add(self.period);
                },
                .skip => {
                    // Skip to the next future tick
                    const elapsed = now.durationSince(self.next_tick);
                    const periods = elapsed.nanos / self.period.nanos + 1;
                    self.next_tick = self.next_tick.add(self.period.mul(periods));
                },
                .delay => {
                    // Schedule from now
                    self.next_tick = now.add(self.period);
                },
            }
            return true;
        }

        return false;
    }

    /// Get time until next tick.
    pub fn remaining(self: *const Self) Duration {
        const now = Instant.now();
        if (!now.isBefore(self.next_tick)) {
            return Duration.ZERO;
        }
        return self.next_tick.durationSince(now);
    }

    /// Get waiter count (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Create an interval with the given period.
pub fn interval(period: Duration) Interval {
    return Interval.init(period);
}

/// Create an interval that fires immediately first.
pub fn intervalImmediate(period: Duration) Interval {
    return Interval.initImmediate(period);
}

// ─────────────────────────────────────────────────────────────────────────────
// TimerDriver Integration
// ─────────────────────────────────────────────────────────────────────────────

const driver_mod = @import("driver.zig");
const TimerDriver = driver_mod.TimerDriver;
const TimerHandle = driver_mod.TimerHandle;

const timer_mod = @import("../internal/scheduler/TimerWheel.zig");
const TimerEntry = timer_mod.TimerEntry;

/// Tick using the TimerDriver for automatic wakeup.
/// The waiter will be woken when the next tick fires.
/// Returns a handle that can be used to cancel.
///
/// Note: For recurring intervals, re-register after each tick.
pub fn tickWithDriver(
    driver: *TimerDriver,
    int: *Interval,
    waiter: *IntervalWaiter,
) !TimerHandle {
    int.mutex.lock();
    const remaining = int.remaining();
    int.mutex.unlock();

    return driver.registerSleep(remaining, @ptrCast(waiter), intervalWakerFn);
}

/// Tick using a stack-allocated TimerEntry (zero allocation).
pub fn tickWithDriverEntry(
    driver: *TimerDriver,
    int: *Interval,
    waiter: *IntervalWaiter,
    entry: *TimerEntry,
) void {
    int.mutex.lock();
    const remaining = int.remaining();
    int.mutex.unlock();

    driver.registerSleepEntry(entry, remaining, @ptrCast(waiter), intervalWakerFn);
}

/// Waker function for IntervalWaiter.
fn intervalWakerFn(ctx: *anyopaque) void {
    const waiter: *IntervalWaiter = @ptrCast(@alignCast(ctx));
    waiter.complete.store(true, .release);
    waiter.wake();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Interval - immediate fires first" {
    var int = Interval.initImmediate(Duration.fromSecs(100)); // Very long period

    // First tick should be immediate
    try std.testing.expect(int.tryTick());

    // Second tick should not be ready (period is 100 seconds)
    try std.testing.expect(!int.tryTick());
}

test "Interval - wait returns immediately when ready" {
    var int = Interval.initImmediate(Duration.fromSecs(10));
    var waiter = IntervalWaiter.init();

    const immediate = int.tick(&waiter);
    try std.testing.expect(immediate);
    try std.testing.expect(waiter.isComplete());
}

test "Interval - wait adds waiter when not ready" {
    var int = Interval.init(Duration.fromSecs(10));
    var waiter = IntervalWaiter.init();

    const immediate = int.tick(&waiter);
    try std.testing.expect(!immediate);
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expectEqual(@as(usize, 1), int.waiterCount());

    // Cancel
    int.cancelTick(&waiter);
    try std.testing.expectEqual(@as(usize, 0), int.waiterCount());
}

test "Interval - poll wakes waiter" {
    var int = Interval.init(Duration.fromNanos(100));
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var waiter = IntervalWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    const immediate = int.tick(&waiter);

    // If immediate, waiter should already be complete
    if (immediate) {
        try std.testing.expect(waiter.isComplete());
        return;
    }

    // Wait longer than the interval and poll
    thr.sleep(10_000); // 10 microseconds
    int.poll();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.isComplete());
}

test "Interval - multiple ticks" {
    var int = Interval.init(Duration.fromNanos(1));

    // Wait for interval to be ready
    thr.sleep(1000);

    // First tick
    try std.testing.expect(int.tryTick());

    // Wait again
    thr.sleep(1000);

    // Second tick
    try std.testing.expect(int.tryTick());
}

test "Interval - reset" {
    var int = Interval.initImmediate(Duration.fromSecs(10));

    // Consume immediate tick
    try std.testing.expect(int.tryTick());

    // Reset to immediate
    int.resetImmediate();

    // Should have another immediate tick
    try std.testing.expect(int.tryTick());
}

test "Interval - remaining time" {
    var int = Interval.init(Duration.fromMillis(100));

    const rem = int.remaining();
    try std.testing.expect(rem.nanos > 0);
    try std.testing.expect(rem.nanos <= Duration.fromMillis(100).nanos);
}

test "MissedTickBehavior - skip mode" {
    // Use a period that's long enough to not fire during test setup
    var int = Interval.init(Duration.fromMillis(100));
    int.setMissedTickBehavior(.skip);

    // Manually set next_tick to the past to simulate being behind
    int.mutex.lock();
    int.next_tick = Instant.now().sub(Duration.fromMillis(500));
    int.mutex.unlock();

    // Should get one tick (we're behind)
    try std.testing.expect(int.tryTick());

    // Next tick should be scheduled for the future (skip mode jumps ahead)
    try std.testing.expect(!int.tryTick());
}

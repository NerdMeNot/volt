//! Async Sleep Primitives
//!
//! Provides async-aware sleep that integrates with the timer wheel.
//! Uses the waiter pattern consistent with sync primitives.
//!
//! ## Usage (Standalone - polling based)
//!
//! ```zig
//! const time = @import("volt").time;
//!
//! // Create a sleep
//! var sleep = time.Sleep.init(time.Duration.fromMillis(100));
//!
//! // In async context
//! var waiter = time.SleepWaiter.init();
//! if (!sleep.wait(&waiter)) {
//!     // Yield to scheduler, will be woken when time elapses
//! }
//! // Sleep complete!
//! ```
//!
//! ## Usage (With TimerDriver - event loop integrated)
//!
//! ```zig
//! // Register with timer driver for automatic wakeup
//! var entry: TimerEntry = undefined;
//! const handle = try sleepWithDriver(driver, duration, &waiter, &entry);
//! // Waiter will be woken automatically when timer fires
//! ```
//!
//! ## Design
//!
//! - Uses waiter pattern (tryWait, wait, cancelWait)
//! - Integrates with TimerWheel for efficient scheduling
//! - Supports deadline-based sleep (sleepUntil)
//! - Optional TimerDriver integration for true async wakeup
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

/// Waiter for sleep completion.
pub const SleepWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether the sleep has completed (atomic for cross-thread visibility).
    /// Set by timer driver thread, read by the waiting task.
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    pointers: Pointers(SleepWaiter) = .{},

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

const SleepWaiterList = LinkedList(SleepWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Sleep
// ─────────────────────────────────────────────────────────────────────────────

/// A sleep future that completes after a duration.
pub const Sleep = struct {
    /// Deadline when the sleep completes (monotonic nanoseconds)
    deadline: Instant,

    /// Waiters list
    waiters: SleepWaiterList,

    /// Whether the sleep has elapsed
    elapsed: bool,

    /// Mutex protecting state
    mutex: thr.Mutex,

    const Self = @This();

    /// Create a sleep for the given duration.
    pub fn init(duration: Duration) Self {
        return .{
            .deadline = Instant.now().add(duration),
            .waiters = .{},
            .elapsed = false,
            .mutex = .{},
        };
    }

    /// Create a sleep until the given instant.
    pub fn initUntil(deadline: Instant) Self {
        return .{
            .deadline = deadline,
            .waiters = .{},
            .elapsed = false,
            .mutex = .{},
        };
    }

    /// Reset the sleep with a new duration from now.
    pub fn reset(self: *Self, duration: Duration) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.deadline = Instant.now().add(duration);
        self.elapsed = false;
    }

    /// Reset the sleep to a new deadline.
    pub fn resetUntil(self: *Self, deadline: Instant) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.deadline = deadline;
        self.elapsed = false;
    }

    /// Get the deadline.
    pub fn getDeadline(self: *const Self) Instant {
        return self.deadline;
    }

    /// Check if the sleep has elapsed.
    pub fn isElapsed(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.elapsed) return true;

        if (!Instant.now().isBefore(self.deadline)) {
            self.elapsed = true;
            return true;
        }
        return false;
    }

    /// Try to complete without waiting.
    /// Returns true if the sleep has elapsed.
    pub fn tryWait(self: *Self) bool {
        return self.isElapsed();
    }

    /// Wait for the sleep to complete.
    /// Returns true if already elapsed (immediate return).
    /// Returns false if the waiter was added (task should yield).
    pub fn wait(self: *Self, waiter: *SleepWaiter) bool {
        self.mutex.lock();

        // Check if already elapsed
        if (self.elapsed) {
            self.mutex.unlock();
            waiter.complete.store(true, .release);
            return true;
        }

        // Check deadline
        if (!Instant.now().isBefore(self.deadline)) {
            self.elapsed = true;
            self.mutex.unlock();
            waiter.complete.store(true, .release);
            return true;
        }

        // Not yet - add waiter
        waiter.complete.store(false, .release);
        self.waiters.pushBack(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Cancel a pending wait.
    pub fn cancelWait(self: *Self, waiter: *SleepWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();

        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        if (SleepWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        self.mutex.unlock();
    }

    /// Poll for completion and wake waiters.
    /// Call this periodically from the event loop.
    pub fn poll(self: *Self) void {
        if (self.elapsed) return;

        self.mutex.lock();

        // Check if deadline passed
        if (!Instant.now().isBefore(self.deadline)) {
            self.elapsed = true;

            // Wake all waiters
            while (self.waiters.popFront()) |w| {
                w.complete.store(true, .release);
                w.wake();
            }
        }

        self.mutex.unlock();
    }

    /// Get time remaining until deadline.
    pub fn remaining(self: *const Self) Duration {
        const now = Instant.now();
        if (!now.isBefore(self.deadline)) {
            return Duration.ZERO;
        }
        return self.deadline.durationSince(now);
    }

    /// Get waiter count (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// SleepManager
// ─────────────────────────────────────────────────────────────────────────────

/// Manages multiple sleeps and polls them efficiently.
/// Use this in the runtime's event loop.
pub const SleepManager = struct {
    /// Active sleeps
    sleeps: std.ArrayListUnmanaged(*Sleep),

    /// Allocator
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .sleeps = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.sleeps.deinit(self.allocator);
    }

    /// Register a sleep to be polled.
    pub fn register(self: *Self, slp: *Sleep) !void {
        try self.sleeps.append(self.allocator, slp);
    }

    /// Unregister a sleep.
    pub fn unregister(self: *Self, slp: *Sleep) void {
        for (self.sleeps.items, 0..) |s, i| {
            if (s == slp) {
                _ = self.sleeps.swapRemove(i);
                return;
            }
        }
    }

    /// Poll all registered sleeps.
    /// Returns number of sleeps that completed.
    pub fn pollAll(self: *Self) usize {
        var completed: usize = 0;
        var i: usize = 0;

        while (i < self.sleeps.items.len) {
            const slp = self.sleeps.items[i];
            slp.poll();

            if (slp.elapsed) {
                completed += 1;
                // Remove completed sleep
                _ = self.sleeps.swapRemove(i);
                // Don't increment i since we swapped
            } else {
                i += 1;
            }
        }

        return completed;
    }

    /// Get time until next sleep expires.
    pub fn nextExpiration(self: *const Self) ?Duration {
        var min_remaining: ?Duration = null;

        for (self.sleeps.items) |slp| {
            const rem = slp.remaining();
            if (min_remaining == null or rem.nanos < min_remaining.?.nanos) {
                min_remaining = rem;
            }
        }

        return min_remaining;
    }

    /// Get active sleep count.
    pub fn len(self: *const Self) usize {
        return self.sleeps.items.len;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Create a sleep for the given duration.
pub fn sleep(duration: Duration) Sleep {
    return Sleep.init(duration);
}

/// Create a sleep until the given instant.
pub fn sleepUntil(deadline: Instant) Sleep {
    return Sleep.initUntil(deadline);
}

/// Blocking sleep (for non-async contexts).
/// Prefer async sleep when running inside the runtime.
pub fn blockingSleep(duration: Duration) void {
    thr.sleep(duration.asNanos());
}

// ─────────────────────────────────────────────────────────────────────────────
// TimerDriver Integration
// ─────────────────────────────────────────────────────────────────────────────

const driver_mod = @import("driver.zig");
const TimerDriver = driver_mod.TimerDriver;
const TimerHandle = driver_mod.TimerHandle;

const timer_mod = @import("../internal/scheduler/TimerWheel.zig");
const TimerEntry = timer_mod.TimerEntry;

/// Waiter wrapper that bridges to TimerDriver.
/// When the timer fires, it wakes the waiter.
const WaiterBridge = struct {
    waiter: *SleepWaiter,

    fn wake(ctx: *anyopaque) void {
        const self: *WaiterBridge = @ptrCast(@alignCast(ctx));
        self.waiter.complete.store(true, .release);
        self.waiter.wake();
    }
};

/// Sleep using the TimerDriver for automatic wakeup.
/// This is the preferred method when running inside a runtime.
///
/// Returns a handle that can be used to cancel the timer.
/// The waiter will be woken automatically when the timer fires.
pub fn sleepWithDriver(
    driver: *TimerDriver,
    duration: Duration,
    waiter: *SleepWaiter,
) !TimerHandle {
    return driver.registerSleep(duration, @ptrCast(waiter), waiterWakeFn);
}

/// Sleep until deadline using the TimerDriver.
pub fn sleepUntilWithDriver(
    driver: *TimerDriver,
    deadline: Instant,
    waiter: *SleepWaiter,
) !TimerHandle {
    return driver.registerDeadline(deadline, @ptrCast(waiter), waiterWakeFn);
}

/// Register a sleep with a stack-allocated TimerEntry (zero allocation).
pub fn sleepWithDriverEntry(
    driver: *TimerDriver,
    duration: Duration,
    waiter: *SleepWaiter,
    entry: *TimerEntry,
) void {
    driver.registerSleepEntry(entry, duration, @ptrCast(waiter), waiterWakeFn);
}

/// Register a deadline sleep with a stack-allocated TimerEntry.
pub fn sleepUntilWithDriverEntry(
    driver: *TimerDriver,
    deadline: Instant,
    waiter: *SleepWaiter,
    entry: *TimerEntry,
) void {
    driver.registerDeadlineEntry(entry, deadline, @ptrCast(waiter), waiterWakeFn);
}

/// Waker function for SleepWaiter.
fn waiterWakeFn(ctx: *anyopaque) void {
    const waiter: *SleepWaiter = @ptrCast(@alignCast(ctx));
    waiter.complete.store(true, .release);
    waiter.wake();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Sleep - immediate check" {
    // Sleep for 0 should be immediately elapsed
    var s = Sleep.init(Duration.ZERO);
    try std.testing.expect(s.tryWait());
    try std.testing.expect(s.isElapsed());
}

test "Sleep - wait returns immediately when elapsed" {
    var s = Sleep.init(Duration.ZERO);
    var waiter = SleepWaiter.init();

    const immediate = s.wait(&waiter);
    try std.testing.expect(immediate);
    try std.testing.expect(waiter.isComplete());
}

test "Sleep - wait adds waiter when not elapsed" {
    // Sleep for 1 second (won't elapse during test)
    var s = Sleep.init(Duration.fromSecs(10));
    var waiter = SleepWaiter.init();

    const immediate = s.wait(&waiter);
    try std.testing.expect(!immediate);
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expectEqual(@as(usize, 1), s.waiterCount());

    // Cancel
    s.cancelWait(&waiter);
    try std.testing.expectEqual(@as(usize, 0), s.waiterCount());
}

test "Sleep - poll wakes waiters" {
    var s = Sleep.init(Duration.fromNanos(100));
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var waiter = SleepWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    const immediate = s.wait(&waiter);

    // If already elapsed, waiter is complete
    if (immediate) {
        try std.testing.expect(waiter.isComplete());
        try std.testing.expect(s.isElapsed());
        return;
    }

    // Wait longer and poll
    thr.sleep(10_000); // 10 microseconds
    s.poll();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.isComplete());
    try std.testing.expect(s.isElapsed());
}

test "Sleep - remaining time" {
    var s = Sleep.init(Duration.fromMillis(100));

    const rem = s.remaining();
    try std.testing.expect(rem.nanos > 0);
    try std.testing.expect(rem.nanos <= Duration.fromMillis(100).nanos);
}

test "Sleep - reset extends deadline" {
    var s = Sleep.init(Duration.fromNanos(1));
    thr.sleep(100);

    try std.testing.expect(s.tryWait()); // Should be elapsed

    s.reset(Duration.fromSecs(10));
    try std.testing.expect(!s.tryWait()); // Should not be elapsed anymore
}

test "SleepManager - basic" {
    var manager = SleepManager.init(std.testing.allocator);
    defer manager.deinit();

    var s1 = Sleep.init(Duration.fromNanos(1));
    var s2 = Sleep.init(Duration.fromSecs(10));

    try manager.register(&s1);
    try manager.register(&s2);

    try std.testing.expectEqual(@as(usize, 2), manager.len());

    // Wait for s1 to expire
    thr.sleep(1000);
    const completed = manager.pollAll();

    try std.testing.expect(completed >= 1);
    try std.testing.expect(s1.isElapsed());
}

test "SleepWaiter - reset" {
    var waiter = SleepWaiter.init();
    waiter.complete.store(true, .release);

    try std.testing.expect(waiter.isComplete());

    waiter.reset();
    try std.testing.expect(!waiter.isComplete());
}

test "sleepWithDriver - basic" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    var woken = false;
    var waiter = SleepWaiter.init();

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Register sleep with driver
    _ = try sleepWithDriver(&driver, Duration.fromNanos(100), &waiter);

    try std.testing.expectEqual(@as(usize, 1), driver.len());

    // Wait and poll
    thr.sleep(10_000); // 10 microseconds
    _ = driver.poll();

    try std.testing.expect(waiter.isComplete());
    try std.testing.expect(woken);
}

test "sleepWithDriverEntry - zero allocation" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    var waiter = SleepWaiter.init();
    var entry: TimerEntry = undefined;

    // Register with stack-allocated entry
    sleepWithDriverEntry(&driver, Duration.fromNanos(100), &waiter, &entry);

    try std.testing.expectEqual(@as(usize, 1), driver.len());

    // Wait and poll
    thr.sleep(10_000);
    _ = driver.poll();

    try std.testing.expect(waiter.isComplete());
}

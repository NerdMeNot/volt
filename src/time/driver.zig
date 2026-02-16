//! Timer Driver
//!
//! Bridges the async time primitives (Sleep, Interval, Deadline) with the
//! TimerWheel for efficient scheduling and true async wakeups.
//!
//! ## Usage
//!
//! The TimerDriver is typically owned by the Runtime and polled in the event loop:
//!
//! ```zig
//! var driver = TimerDriver.init(allocator);
//! defer driver.deinit();
//!
//! // Register a sleep
//! const handle = try driver.registerSleep(duration, waiter);
//!
//! // In event loop
//! while (running) {
//!     _ = driver.poll(); // Wakes expired timers
//!     // ... poll I/O ...
//! }
//! ```
//!
//! ## Design
//!
//! - Wraps TimerWheel for O(1) timer operations
//! - Supports both task-based (Header) and waiter-based (WakerFn) waking
//! - Thread-safe via internal mutex
//! - Zero-allocation for timer registration (entries are stack-allocated by callers)
//!

const std = @import("std");
const Allocator = std.mem.Allocator;

const timer_mod = @import("../internal/scheduler/TimerWheel.zig");
const TimerWheel = timer_mod.TimerWheel;
const TimerEntry = timer_mod.TimerEntry;
/// Function pointer type for waking a suspended task.
pub const WakerFn = timer_mod.WakerFn;

const time_types = @import("../time.zig");
pub const Duration = time_types.Duration;
pub const Instant = time_types.Instant;

// ─────────────────────────────────────────────────────────────────────────────
// TimerHandle
// ─────────────────────────────────────────────────────────────────────────────

/// Handle to a registered timer. Used to cancel the timer.
pub const TimerHandle = struct {
    entry: *TimerEntry,
    driver: *TimerDriver,

    /// Cancel this timer.
    pub fn cancel(self: *TimerHandle) void {
        self.driver.cancel(self.entry);
    }

    /// Check if this timer has been cancelled.
    pub fn isCancelled(self: *const TimerHandle) bool {
        return self.entry.cancelled;
    }

    /// Get the deadline of this timer.
    pub fn getDeadline(self: *const TimerHandle) u64 {
        return self.entry.deadline_ns;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// TimerDriver
// ─────────────────────────────────────────────────────────────────────────────

/// Driver that manages timers and integrates with the event loop.
pub const TimerDriver = struct {
    const Self = @This();

    /// The underlying timer wheel
    wheel: TimerWheel,

    /// Mutex protecting the wheel
    mutex: std.Thread.Mutex,

    /// Allocator for heap-allocated entries
    allocator: Allocator,

    /// Create a new timer driver.
    pub fn init(allocator: Allocator) Self {
        return .{
            .wheel = TimerWheel.init(allocator),
            .mutex = .{},
            .allocator = allocator,
        };
    }

    /// Clean up all timers.
    pub fn deinit(self: *Self) void {
        self.wheel.deinit();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Registration (with allocation)
    // ═══════════════════════════════════════════════════════════════════════

    /// Register a sleep timer with a waker.
    /// The driver allocates the TimerEntry internally.
    /// Returns a handle that can be used to cancel.
    pub fn registerSleep(
        self: *Self,
        duration: Duration,
        ctx: *anyopaque,
        waker: WakerFn,
    ) !TimerHandle {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = try timer_mod.sleepWithWaker(
            &self.wheel,
            duration.asNanos(),
            ctx,
            waker,
        );

        return .{ .entry = entry, .driver = self };
    }

    /// Register a deadline timer with a waker.
    /// The driver allocates the TimerEntry internally.
    pub fn registerDeadline(
        self: *Self,
        deadline_instant: Instant,
        ctx: *anyopaque,
        waker: WakerFn,
    ) !TimerHandle {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Convert Instant to nanoseconds relative to wheel's start
        const now_ns = self.wheel.now();
        const now_instant = Instant.now();
        const deadline_ns = if (deadline_instant.isAfter(now_instant))
            now_ns + deadline_instant.durationSince(now_instant).asNanos()
        else
            now_ns; // Already past

        const entry = try timer_mod.deadlineWithWaker(
            &self.wheel,
            deadline_ns,
            ctx,
            waker,
        );

        return .{ .entry = entry, .driver = self };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Registration (external entry - zero allocation)
    // ═══════════════════════════════════════════════════════════════════════

    /// Register an externally-allocated timer entry.
    /// The caller owns the entry and must ensure it lives until cancelled or fired.
    pub fn registerEntry(self: *Self, entry: *TimerEntry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.wheel.insert(entry);
    }

    /// Set up an entry for a sleep duration and register it.
    pub fn registerSleepEntry(
        self: *Self,
        entry: *TimerEntry,
        duration: Duration,
        ctx: *anyopaque,
        waker: WakerFn,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        entry.* = TimerEntry.initWithWaker(
            self.wheel.now() + duration.asNanos(),
            ctx,
            waker,
            0,
        );
        self.wheel.insert(entry);
    }

    /// Set up an entry for a deadline and register it.
    pub fn registerDeadlineEntry(
        self: *Self,
        entry: *TimerEntry,
        deadline_instant: Instant,
        ctx: *anyopaque,
        waker: WakerFn,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now_ns = self.wheel.now();
        const now_instant = Instant.now();
        const deadline_ns = if (deadline_instant.isAfter(now_instant))
            now_ns + deadline_instant.durationSince(now_instant).asNanos()
        else
            now_ns;

        entry.* = TimerEntry.initWithWaker(deadline_ns, ctx, waker, 0);
        self.wheel.insert(entry);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Cancellation
    // ═══════════════════════════════════════════════════════════════════════

    /// Cancel a timer.
    pub fn cancel(self: *Self, entry: *TimerEntry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.wheel.remove(entry);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Polling
    // ═══════════════════════════════════════════════════════════════════════

    /// Poll for expired timers and wake them.
    /// Returns the number of timers that fired.
    pub fn poll(self: *Self) usize {
        self.mutex.lock();
        const expired = self.wheel.poll();
        self.mutex.unlock();

        // Process expired outside the lock to avoid holding it during wakes
        return timer_mod.processExpired(&self.wheel, expired);
    }

    /// Get time until next timer expires.
    /// Returns null if no timers are registered.
    pub fn nextExpiration(self: *Self) ?Duration {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.wheel.nextExpiration()) |ns| {
            return Duration.fromNanos(ns);
        }
        return null;
    }

    /// Get the number of active timers.
    pub fn len(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.wheel.len();
    }

    /// Check if there are any active timers.
    pub fn isEmpty(self: *Self) bool {
        return self.len() == 0;
    }

    /// Get current time (monotonic nanoseconds).
    pub fn now(self: *Self) u64 {
        return self.wheel.now();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Global Driver Access
// ─────────────────────────────────────────────────────────────────────────────

/// Thread-local timer driver reference.
/// Set by the runtime when running tasks.
threadlocal var current_driver: ?*TimerDriver = null;

/// Get the current timer driver (if inside a runtime).
pub fn getDriver() ?*TimerDriver {
    return current_driver;
}

/// Set the current timer driver (called by runtime).
pub fn setDriver(driver: ?*TimerDriver) void {
    current_driver = driver;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "TimerDriver - init and deinit" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    try std.testing.expect(driver.isEmpty());
}

test "TimerDriver - register and poll" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Register a very short timer
    _ = try driver.registerSleep(
        Duration.fromNanos(100),
        @ptrCast(&woken),
        TestWaker.wake,
    );

    try std.testing.expectEqual(@as(usize, 1), driver.len());

    // Wait and poll
    std.Thread.sleep(10_000); // 10 microseconds
    const fired = driver.poll();

    try std.testing.expect(fired >= 1);
    try std.testing.expect(woken);
}

test "TimerDriver - cancel" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Register a timer
    var handle = try driver.registerSleep(
        Duration.fromSecs(10),
        @ptrCast(&woken),
        TestWaker.wake,
    );

    try std.testing.expectEqual(@as(usize, 1), driver.len());

    // Cancel it
    handle.cancel();
    try std.testing.expect(handle.isCancelled());

    // Poll should not wake
    _ = driver.poll();
    try std.testing.expect(!woken);
}

test "TimerDriver - external entry" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Stack-allocated entry
    var entry: TimerEntry = undefined;
    driver.registerSleepEntry(
        &entry,
        Duration.fromNanos(100),
        @ptrCast(&woken),
        TestWaker.wake,
    );

    try std.testing.expectEqual(@as(usize, 1), driver.len());

    // Wait and poll
    std.Thread.sleep(10_000);
    _ = driver.poll();

    try std.testing.expect(woken);
}

test "TimerDriver - nextExpiration" {
    var driver = TimerDriver.init(std.testing.allocator);
    defer driver.deinit();

    // No timers - should return null
    try std.testing.expect(driver.nextExpiration() == null);

    var woken = false;
    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Add a timer
    _ = try driver.registerSleep(
        Duration.fromMillis(100),
        @ptrCast(&woken),
        TestWaker.wake,
    );

    // Should have next expiration
    const next = driver.nextExpiration();
    try std.testing.expect(next != null);
    try std.testing.expect(next.?.asNanos() > 0);
}

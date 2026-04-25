//! Time primitives for volt
//!
//! Provides Duration and Instant types for timeouts, sleeps, and measurements.
//!
//! ## Types
//!
//! - `Duration` - A span of time (nanosecond precision)
//! - `Instant` - A point in time (monotonic clock)
//!
//! ## Async Primitives
//!
//! - `Sleep` - Async sleep for a duration
//! - `Interval` - Recurring timer
//! - `Deadline` - Timeout tracking

const std = @import("std");
const thr = @import("internal/thread.zig");

/// Monotonic-clock nanosecond timestamp. Replaces std.time.nanoTimestamp
/// (removed in Zig 0.16, where time queries moved into std.Io).
pub fn nanoTimestamp() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

/// A duration of time (nanosecond precision).
pub const Duration = struct {
    nanos: u64,

    pub const ZERO = Duration{ .nanos = 0 };
    pub const MAX = Duration{ .nanos = std.math.maxInt(u64) };

    /// Create duration from nanoseconds.
    pub fn fromNanos(n: u64) Duration {
        return .{ .nanos = n };
    }

    /// Create duration from microseconds.
    pub fn fromMicros(n: u64) Duration {
        return .{ .nanos = n *| std.time.ns_per_us };
    }

    /// Create duration from milliseconds.
    pub fn fromMillis(n: u64) Duration {
        return .{ .nanos = n *| std.time.ns_per_ms };
    }

    /// Create duration from seconds.
    pub fn fromSecs(n: u64) Duration {
        return .{ .nanos = n *| std.time.ns_per_s };
    }

    /// Alias for fromSecs (more readable).
    pub const fromSeconds = fromSecs;

    /// Create duration from minutes.
    pub fn fromMins(n: u64) Duration {
        return .{ .nanos = n *| (60 * std.time.ns_per_s) };
    }

    /// Alias for fromMins (more readable).
    pub const fromMinutes = fromMins;

    /// Create duration from hours.
    pub fn fromHours(n: u64) Duration {
        return .{ .nanos = n *| (60 * 60 * std.time.ns_per_s) };
    }

    /// Create duration from days.
    pub fn fromDays(n: u64) Duration {
        return .{ .nanos = n *| (24 * 60 * 60 * std.time.ns_per_s) };
    }

    /// Get as nanoseconds.
    pub fn asNanos(self: Duration) u64 {
        return self.nanos;
    }

    /// Get as microseconds (truncated).
    pub fn asMicros(self: Duration) u64 {
        return self.nanos / std.time.ns_per_us;
    }

    /// Get as milliseconds (truncated).
    pub fn asMillis(self: Duration) u64 {
        return self.nanos / std.time.ns_per_ms;
    }

    /// Get as seconds (truncated).
    pub fn asSecs(self: Duration) u64 {
        return self.nanos / std.time.ns_per_s;
    }

    /// Alias for asSecs (more readable).
    pub const asSeconds = asSecs;

    /// Get as minutes (truncated).
    pub fn asMins(self: Duration) u64 {
        return self.nanos / (60 * std.time.ns_per_s);
    }

    /// Alias for asMins (more readable).
    pub const asMinutes = asMins;

    /// Get as hours (truncated).
    pub fn asHours(self: Duration) u64 {
        return self.nanos / (60 * 60 * std.time.ns_per_s);
    }

    /// Get as days (truncated).
    pub fn asDays(self: Duration) u64 {
        return self.nanos / (24 * 60 * 60 * std.time.ns_per_s);
    }

    /// Add two durations (saturating).
    pub fn add(self: Duration, other: Duration) Duration {
        return .{ .nanos = self.nanos +| other.nanos };
    }

    /// Subtract duration (saturating at zero).
    pub fn sub(self: Duration, other: Duration) Duration {
        return .{ .nanos = self.nanos -| other.nanos };
    }

    /// Multiply by scalar (saturating).
    pub fn mul(self: Duration, n: u64) Duration {
        return .{ .nanos = self.nanos *| n };
    }

    /// Divide by scalar.
    pub fn div(self: Duration, n: u64) Duration {
        return .{ .nanos = self.nanos / n };
    }

    /// Compare durations.
    pub fn cmp(self: Duration, other: Duration) std.math.Order {
        return std.math.order(self.nanos, other.nanos);
    }

    /// Check if zero.
    pub fn isZero(self: Duration) bool {
        return self.nanos == 0;
    }

    /// Convert to timespec for syscalls.
    pub fn toTimespec(self: Duration) std.posix.timespec {
        const secs = self.nanos / std.time.ns_per_s;
        const nsecs = self.nanos % std.time.ns_per_s;
        return .{
            .sec = @intCast(secs),
            .nsec = @intCast(nsecs),
        };
    }
};

/// A point in time (monotonic clock).
pub const Instant = struct {
    timestamp: i128,

    /// Get current instant.
    pub fn now() Instant {
        return .{ .timestamp = nanoTimestamp() };
    }

    /// Duration elapsed since this instant.
    pub fn elapsed(self: Instant) Duration {
        const current = nanoTimestamp();
        const diff = current - self.timestamp;
        return .{ .nanos = if (diff < 0) 0 else @intCast(diff) };
    }

    /// Duration until another instant (zero if in the past).
    pub fn durationSince(self: Instant, earlier: Instant) Duration {
        const diff = self.timestamp - earlier.timestamp;
        return .{ .nanos = if (diff < 0) 0 else @intCast(diff) };
    }

    /// Nanoseconds since `earlier` (saturating). Mirrors the std.time.Instant API.
    pub fn since(self: Instant, earlier: Instant) u64 {
        return self.durationSince(earlier).nanos;
    }

    /// Add duration to instant.
    pub fn add(self: Instant, duration: Duration) Instant {
        return .{ .timestamp = self.timestamp + @as(i128, duration.nanos) };
    }

    /// Subtract duration from instant.
    pub fn sub(self: Instant, duration: Duration) Instant {
        return .{ .timestamp = self.timestamp - @as(i128, duration.nanos) };
    }

    /// Check if this instant is before another.
    pub fn isBefore(self: Instant, other: Instant) bool {
        return self.timestamp < other.timestamp;
    }

    /// Check if this instant is after another.
    pub fn isAfter(self: Instant, other: Instant) bool {
        return self.timestamp > other.timestamp;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Get the current instant (monotonic clock).
///
/// ## Example
///
/// ```zig
/// const start = io.time.now();
/// doWork();
/// const elapsed = start.elapsed();
/// ```
pub fn now() Instant {
    return Instant.now();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Duration - from and as conversions" {
    const d = Duration.fromSecs(5);
    try std.testing.expectEqual(@as(u64, 5_000_000_000), d.asNanos());
    try std.testing.expectEqual(@as(u64, 5_000_000), d.asMicros());
    try std.testing.expectEqual(@as(u64, 5_000), d.asMillis());
    try std.testing.expectEqual(@as(u64, 5), d.asSecs());
}

test "Duration - minutes, hours, days" {
    // Minutes
    const mins = Duration.fromMins(90);
    try std.testing.expectEqual(@as(u64, 90), mins.asMins());
    try std.testing.expectEqual(@as(u64, 1), mins.asHours());
    try std.testing.expectEqual(@as(u64, 5400), mins.asSecs());

    // Alias
    const mins2 = Duration.fromMinutes(30);
    try std.testing.expectEqual(@as(u64, 30), mins2.asMinutes());

    // Hours
    const hrs = Duration.fromHours(2);
    try std.testing.expectEqual(@as(u64, 2), hrs.asHours());
    try std.testing.expectEqual(@as(u64, 120), hrs.asMins());

    // Days
    const days = Duration.fromDays(1);
    try std.testing.expectEqual(@as(u64, 1), days.asDays());
    try std.testing.expectEqual(@as(u64, 24), days.asHours());
    try std.testing.expectEqual(@as(u64, 86400), days.asSecs());
}

test "Duration - arithmetic saturates" {
    const max = Duration.MAX;
    const one = Duration.fromSecs(1);

    // Add saturates
    const sum = max.add(one);
    try std.testing.expectEqual(Duration.MAX.nanos, sum.nanos);

    // Sub saturates at zero
    const zero = Duration.ZERO;
    const diff = zero.sub(one);
    try std.testing.expectEqual(@as(u64, 0), diff.nanos);
}

test "Duration - multiply and divide" {
    const d = Duration.fromMillis(100);
    try std.testing.expectEqual(@as(u64, 300), d.mul(3).asMillis());
    try std.testing.expectEqual(@as(u64, 50), d.div(2).asMillis());
}

test "Instant - elapsed increases" {
    const start = Instant.now();
    thr.sleep(1_000_000); // 1ms
    const elapsed = start.elapsed();
    try std.testing.expect(elapsed.asNanos() >= 1_000_000);
}

test "Instant - ordering" {
    const a = Instant.now();
    thr.sleep(100);
    const b = Instant.now();

    try std.testing.expect(a.isBefore(b));
    try std.testing.expect(b.isAfter(a));
}

// ─────────────────────────────────────────────────────────────────────────────
// Async Time Primitives (re-exports from time/)
// ─────────────────────────────────────────────────────────────────────────────

/// Async sleep primitive.
pub const sleep_mod = @import("time/sleep.zig");
pub const Sleep = sleep_mod.Sleep;
pub const SleepWaiter = sleep_mod.SleepWaiter;
pub const SleepManager = sleep_mod.SleepManager;

/// Recurring interval timer.
pub const interval_mod = @import("time/interval.zig");
pub const Interval = interval_mod.Interval;
pub const IntervalWaiter = interval_mod.IntervalWaiter;
pub const MissedTickBehavior = interval_mod.MissedTickBehavior;

/// Deadline and timeout utilities.
pub const timeout_mod = @import("time/timeout.zig");
pub const Deadline = timeout_mod.Deadline;
pub const DeadlineWaiter = timeout_mod.DeadlineWaiter;
pub const TimeoutError = timeout_mod.TimeoutError;

/// Convenience: create a sleep for the given duration.
pub const sleep = sleep_mod.sleep;

/// Convenience: create a sleep until the given instant.
pub const sleepUntil = sleep_mod.sleepUntil;

/// Convenience: blocking sleep (for non-async contexts).
pub const blockingSleep = sleep_mod.blockingSleep;

/// Convenience: create an interval timer.
pub const interval = interval_mod.interval;

/// Convenience: create a deadline.
pub const deadline = timeout_mod.deadline;

/// Timer driver for runtime integration.
pub const driver_mod = @import("time/driver.zig");
pub const TimerDriver = driver_mod.TimerDriver;
pub const TimerHandle = driver_mod.TimerHandle;
pub const getDriver = driver_mod.getDriver;
pub const setDriver = driver_mod.setDriver;

// Include async time module tests
test {
    _ = @import("time/sleep.zig");
    _ = @import("time/interval.zig");
    _ = @import("time/timeout.zig");
    _ = @import("time/driver.zig");
}

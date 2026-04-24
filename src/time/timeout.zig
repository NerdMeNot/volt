//! Timeout Utilities
//!
//! Provides deadline and timeout tracking for async operations.
//!
//! ## Usage
//!
//! ```zig
//! const time = @import("volt").time;
//!
//! // Create a deadline 100ms from now
//! var deadline = time.Deadline.init(time.Duration.fromMillis(100));
//!
//! // Check if expired
//! if (deadline.isExpired()) {
//!     return error.TimedOut;
//! }
//!
//! // Or use with operations
//! const result = try myOperation();
//! if (deadline.isExpired()) {
//!     return error.TimedOut;
//! }
//! ```
//!
//! ## Design
//!
//! - Deadline tracks a point in time
//! - Provides isExpired() check
//! - Can be used with waiter pattern for async cancellation
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
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// Error returned when a timeout expires.
pub const TimeoutError = error{
    TimedOut,
};

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for deadline expiration.
pub const DeadlineWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether the deadline has expired (atomic for cross-thread visibility).
    /// Set by timer driver thread, read by the waiting task.
    expired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    pointers: Pointers(DeadlineWaiter) = .{},

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
        return self.expired.load(.acquire);
    }

    pub fn reset(self: *Self) void {
        self.expired.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

const DeadlineWaiterList = LinkedList(DeadlineWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Deadline
// ─────────────────────────────────────────────────────────────────────────────

/// A point in time after which an operation should abort.
pub const Deadline = struct {
    /// When the deadline expires
    expires_at: Instant,

    /// Whether already checked and found expired
    expired: bool,

    /// Waiters for expiration notification
    waiters: DeadlineWaiterList,

    /// Mutex protecting state
    mutex: thr.Mutex,

    const Self = @This();

    /// Create a deadline that expires after the given duration.
    pub fn init(timeout: Duration) Self {
        return .{
            .expires_at = Instant.now().add(timeout),
            .expired = false,
            .waiters = .{},
            .mutex = .{},
        };
    }

    /// Create a deadline at a specific instant.
    pub fn initAt(expires_at: Instant) Self {
        return .{
            .expires_at = expires_at,
            .expired = false,
            .waiters = .{},
            .mutex = .{},
        };
    }

    /// Create an already-expired deadline (for testing).
    pub fn initExpired() Self {
        return .{
            .expires_at = Instant.now(),
            .expired = true,
            .waiters = .{},
            .mutex = .{},
        };
    }

    /// Check if the deadline has expired.
    pub fn isExpired(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.expired) return true;

        if (!Instant.now().isBefore(self.expires_at)) {
            self.expired = true;
            return true;
        }
        return false;
    }

    /// Check if expired (without locking, for read-only access).
    pub fn isExpiredConst(self: *const Self) bool {
        if (self.expired) return true;
        return !Instant.now().isBefore(self.expires_at);
    }

    /// Get the expiration instant.
    pub fn getExpiresAt(self: *const Self) Instant {
        return self.expires_at;
    }

    /// Get time remaining until expiration.
    pub fn remaining(self: *const Self) Duration {
        const now = Instant.now();
        if (!now.isBefore(self.expires_at)) {
            return Duration.ZERO;
        }
        return self.expires_at.durationSince(now);
    }

    /// Extend the deadline by the given duration.
    pub fn extend(self: *Self, duration: Duration) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.expires_at = self.expires_at.add(duration);
        self.expired = false;
    }

    /// Reset the deadline to a new duration from now.
    pub fn reset(self: *Self, timeout: Duration) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.expires_at = Instant.now().add(timeout);
        self.expired = false;
    }

    /// Wait for the deadline to expire.
    /// Returns true if already expired (immediate).
    /// Returns false if waiter was added (should yield).
    pub fn waitExpiry(self: *Self, waiter: *DeadlineWaiter) bool {
        self.mutex.lock();

        if (self.expired) {
            self.mutex.unlock();
            waiter.expired.store(true, .release);
            return true;
        }

        if (!Instant.now().isBefore(self.expires_at)) {
            self.expired = true;
            self.mutex.unlock();
            waiter.expired.store(true, .release);
            return true;
        }

        // Not expired - add waiter
        waiter.expired.store(false, .release);
        self.waiters.pushBack(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Cancel waiting for expiry.
    pub fn cancelWait(self: *Self, waiter: *DeadlineWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();

        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        if (DeadlineWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        self.mutex.unlock();
    }

    /// Poll for expiration and wake waiters.
    pub fn poll(self: *Self) void {
        self.mutex.lock();

        if (!self.expired and !Instant.now().isBefore(self.expires_at)) {
            self.expired = true;

            // Wake all waiters
            while (self.waiters.popFront()) |w| {
                w.expired.store(true, .release);
                w.wake();
            }
        }

        self.mutex.unlock();
    }

    /// Check result: returns error if expired.
    pub fn check(self: *Self) TimeoutError!void {
        if (self.isExpired()) {
            return error.TimedOut;
        }
    }

    /// Get waiter count (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Timeout Guard
// ─────────────────────────────────────────────────────────────────────────────

/// A guard that checks timeout on scope exit.
/// Useful for ensuring timeout is checked even on early returns.
pub const TimeoutGuard = struct {
    dl: *Deadline,
    timed_out: *bool,

    pub fn init(dl: *Deadline, timed_out: *bool) TimeoutGuard {
        return .{
            .dl = dl,
            .timed_out = timed_out,
        };
    }

    pub fn check(self: *TimeoutGuard) void {
        if (self.dl.isExpired()) {
            self.timed_out.* = true;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Create a deadline that expires after the given duration.
pub fn deadline(timeout: Duration) Deadline {
    return Deadline.init(timeout);
}

/// Create a deadline at a specific instant.
pub fn deadlineAt(expires_at: Instant) Deadline {
    return Deadline.initAt(expires_at);
}

/// Run a function with a timeout.
/// Returns error.TimedOut if the deadline expires before the function returns.
/// Note: This doesn't actually cancel the operation - it just checks the deadline.
pub fn withTimeout(
    comptime T: type,
    timeout_duration: Duration,
    comptime func: fn () T,
) TimeoutError!T {
    var dl = Deadline.init(timeout_duration);

    const result = func();

    if (dl.isExpired()) {
        return error.TimedOut;
    }

    return result;
}

/// Check a deadline and return error if expired.
pub fn checkTimeout(dl: *Deadline) TimeoutError!void {
    return dl.check();
}

/// Result of a try timeout operation.
pub fn TryTimeoutResult(comptime T: type, comptime E: type) type {
    return union(enum) {
        /// Operation completed within the deadline.
        ok: T,
        /// Operation returned an error.
        err: E,
        /// Deadline expired before operation completed.
        timeout,

        const Self = @This();

        /// Returns true if the operation succeeded.
        pub fn isOk(self: Self) bool {
            return self == .ok;
        }

        /// Returns true if the operation timed out.
        pub fn isTimeout(self: Self) bool {
            return self == .timeout;
        }

        /// Unwrap the result, returning the value or the error.
        /// If timed out, returns error.TimedOut.
        pub fn unwrap(self: Self) (E || TimeoutError)!T {
            return switch (self) {
                .ok => |v| v,
                .err => |e| e,
                .timeout => error.TimedOut,
            };
        }

        /// Get the value if ok, null otherwise.
        pub fn getValue(self: Self) ?T {
            return switch (self) {
                .ok => |v| v,
                else => null,
            };
        }
    };
}

/// Try to run a function within a timeout duration.
///
/// Returns a result union that distinguishes between:
/// - `.ok`: Operation completed successfully
/// - `.err`: Operation returned an error
/// - `.timeout`: Deadline expired
///
/// Note: This does not cancel the operation - it just checks if the deadline
/// expired after the operation completes. For true async timeout with
/// cancellation, use the Timeout combinator.
///
/// ## Example
///
/// ```zig
/// const result = io.tryTimeout(io.Duration.fromSecs(5), doExpensiveWork, .{arg1, arg2});
///
/// switch (result) {
///     .ok => |value| handleSuccess(value),
///     .err => |e| handleError(e),
///     .timeout => log.warn("Operation timed out", .{}),
/// }
///
/// // Or use unwrap for error propagation:
/// const value = try result.unwrap();
/// ```
pub fn tryTimeout(
    timeout_duration: Duration,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) TryTimeoutResult(
    ReturnPayload(@TypeOf(func)),
    ErrorPayload(@TypeOf(func)),
) {
    var dl = Deadline.init(timeout_duration);

    const result = @call(.auto, func, args);

    if (dl.isExpired()) {
        return .timeout;
    }

    const ReturnType = @TypeOf(result);
    if (@typeInfo(ReturnType) == .error_union) {
        if (result) |v| {
            return .{ .ok = v };
        } else |e| {
            return .{ .err = e };
        }
    } else {
        return .{ .ok = result };
    }
}

/// Helper to extract the payload type from a function's return type.
fn ReturnPayload(comptime F: type) type {
    const info = @typeInfo(F);
    if (info != .@"fn") @compileError("Expected function type");

    const ret = info.@"fn".return_type orelse void;
    if (@typeInfo(ret) == .error_union) {
        return @typeInfo(ret).error_union.payload;
    }
    return ret;
}

/// Helper to extract the error type from a function's return type.
fn ErrorPayload(comptime F: type) type {
    const info = @typeInfo(F);
    if (info != .@"fn") @compileError("Expected function type");

    const ret = info.@"fn".return_type orelse void;
    if (@typeInfo(ret) == .error_union) {
        return @typeInfo(ret).error_union.error_set;
    }
    return error{};
}

/// Run a function with a deadline, using a callback-style API.
///
/// ## Example
///
/// ```zig
/// withDeadline(Duration.fromSecs(5), struct {
///     fn run(deadline: *Deadline) !Result {
///         while (!deadline.isExpired()) {
///             if (try pollForData()) |data| {
///                 return processData(data);
///             }
///         }
///         return error.TimedOut;
///     }
/// }.run);
/// ```
pub fn withDeadline(
    timeout_duration: Duration,
    comptime func: fn (*Deadline) anyerror!void,
) anyerror!void {
    var dl = Deadline.init(timeout_duration);
    return func(&dl);
}

// ─────────────────────────────────────────────────────────────────────────────
// TimerDriver Integration
// ─────────────────────────────────────────────────────────────────────────────

const driver_mod = @import("driver.zig");
const TimerDriver = driver_mod.TimerDriver;
const TimerHandle = driver_mod.TimerHandle;

const timer_mod = @import("../internal/scheduler/TimerWheel.zig");
const TimerEntry = timer_mod.TimerEntry;

/// Wait for deadline expiry using TimerDriver.
/// The waiter will be woken automatically when the deadline expires.
pub fn waitExpiryWithDriver(
    driver: *TimerDriver,
    dl: *Deadline,
    waiter: *DeadlineWaiter,
) !TimerHandle {
    return driver.registerDeadline(dl.expires_at, @ptrCast(waiter), deadlineWakerFn);
}

/// Wait for deadline expiry using a stack-allocated TimerEntry.
pub fn waitExpiryWithDriverEntry(
    driver: *TimerDriver,
    dl: *Deadline,
    waiter: *DeadlineWaiter,
    entry: *TimerEntry,
) void {
    driver.registerDeadlineEntry(entry, dl.expires_at, @ptrCast(waiter), deadlineWakerFn);
}

/// Waker function for DeadlineWaiter.
fn deadlineWakerFn(ctx: *anyopaque) void {
    const waiter: *DeadlineWaiter = @ptrCast(@alignCast(ctx));
    waiter.expired.store(true, .release);
    waiter.wake();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Deadline - not expired initially" {
    var dl = Deadline.init(Duration.fromSecs(10));
    try std.testing.expect(!dl.isExpired());
}

test "Deadline - expires after duration" {
    var dl = Deadline.init(Duration.fromNanos(1));
    thr.sleep(1000); // 1 microsecond
    try std.testing.expect(dl.isExpired());
}

test "Deadline - remaining time" {
    var dl = Deadline.init(Duration.fromMillis(100));
    const rem = dl.remaining();
    try std.testing.expect(rem.nanos > 0);
    try std.testing.expect(rem.nanos <= Duration.fromMillis(100).nanos);
}

test "Deadline - check returns error when expired" {
    var dl = Deadline.initExpired();
    try std.testing.expectError(error.TimedOut, dl.check());
}

test "Deadline - check succeeds when not expired" {
    var dl = Deadline.init(Duration.fromSecs(10));
    try dl.check();
}

test "Deadline - extend prevents expiration" {
    var dl = Deadline.init(Duration.fromNanos(1));
    dl.extend(Duration.fromSecs(10));
    try std.testing.expect(!dl.isExpired());
}

test "Deadline - reset resets timer" {
    var dl = Deadline.init(Duration.fromNanos(1));
    thr.sleep(1000);
    try std.testing.expect(dl.isExpired());

    dl.reset(Duration.fromSecs(10));
    try std.testing.expect(!dl.isExpired());
}

test "Deadline - wait returns immediately when expired" {
    var dl = Deadline.initExpired();
    var waiter = DeadlineWaiter.init();

    const immediate = dl.waitExpiry(&waiter);
    try std.testing.expect(immediate);
    try std.testing.expect(waiter.isComplete());
}

test "Deadline - wait adds waiter when not expired" {
    var dl = Deadline.init(Duration.fromSecs(10));
    var waiter = DeadlineWaiter.init();

    const immediate = dl.waitExpiry(&waiter);
    try std.testing.expect(!immediate);
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expectEqual(@as(usize, 1), dl.waiterCount());

    dl.cancelWait(&waiter);
    try std.testing.expectEqual(@as(usize, 0), dl.waiterCount());
}

test "Deadline - poll wakes waiters" {
    var dl = Deadline.init(Duration.fromNanos(100));
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var waiter = DeadlineWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    const immediate = dl.waitExpiry(&waiter);

    // If deadline already expired, waiter is complete
    if (immediate) {
        try std.testing.expect(waiter.expired.load(.acquire));
        return;
    }

    // Wait longer and poll
    thr.sleep(10_000); // 10 microseconds
    dl.poll();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.expired.load(.acquire));
}

test "DeadlineWaiter - reset" {
    var waiter = DeadlineWaiter.init();
    waiter.expired.store(true, .release);

    try std.testing.expect(waiter.isComplete());

    waiter.reset();
    try std.testing.expect(!waiter.isComplete());
}

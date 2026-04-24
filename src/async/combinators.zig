//! Async Combinators - Timeout, Select, Join
//!
//! Provides composable async operations for coordinating futures:
//!
//! - `Timeout`: Wrap a future with a deadline, cancel if time expires
//! - `Select`: Race multiple futures, return first to complete
//! - `Join`: Run multiple futures concurrently, wait for all to complete
//!
//! ## Timeout Example
//!
//! ```zig
//! const async_ops = @import("volt").async_ops;
//!
//! // Wrap an accept with a 5 second timeout
//! var timeout = async_ops.Timeout(AcceptFuture).init(
//!     listener.accept(),
//!     Duration.fromSecs(5),
//! );
//!
//! switch (timeout.poll(waker)) {
//!     .ready => |result| handleConnection(result),
//!     .timeout => log.warn("Accept timed out", .{}),
//!     .pending => {}, // Still waiting
//!     .err => |e| return e,
//! }
//! ```
//!
//! ## Select Example
//!
//! ```zig
//! // Race between accept and shutdown signal
//! var select = async_ops.Select2(AcceptFuture, SignalFuture).init(
//!     listener.accept(),
//!     shutdown.wait(),
//! );
//!
//! switch (select.poll(waker)) {
//!     .first => |conn| handleConnection(conn),
//!     .second => |_| break,  // Shutdown received
//!     .pending => {},
//! }
//! ```
//!
//! ## Join Example
//!
//! ```zig
//! // Fetch user and posts concurrently
//! var join = async_ops.Join2(UserFuture, PostsFuture).init(
//!     fetchUser(id),
//!     fetchPosts(id),
//! );
//!
//! switch (join.poll(waker)) {
//!     .ready => |results| {
//!         const user = results[0];
//!         const posts = results[1];
//!     },
//!     .pending => {},
//!     .err => |e| return e,
//! }
//! ```

const std = @import("std");
const thr = @import("../internal/thread.zig");
const time_mod = @import("../time.zig");
const Duration = time_mod.Duration;
const Instant = time_mod.Instant;

// ═══════════════════════════════════════════════════════════════════════════════
// Common Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Generic poll result for futures.
pub fn PollResult(comptime T: type) type {
    return union(enum) {
        ready: T,
        pending: void,
        err: anyerror,
    };
}

/// Timeout-aware poll result.
pub fn TimeoutResult(comptime T: type) type {
    return union(enum) {
        ready: T,
        timeout: void,
        pending: void,
        err: anyerror,
    };
}

/// Waker function type (matches our pattern).
pub const WakerFn = *const fn (*anyopaque) void;

/// Simple waker that can be stored.
pub const Waker = struct {
    ctx: ?*anyopaque = null,
    wake_fn: ?WakerFn = null,

    pub fn wake(self: Waker) void {
        if (self.wake_fn) |wf| {
            if (self.ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    pub fn isSet(self: Waker) bool {
        return self.wake_fn != null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Timeout - Wrap a future with a deadline
// ═══════════════════════════════════════════════════════════════════════════════

/// Wraps a future with a timeout.
///
/// If the inner future doesn't complete before the deadline, the timeout
/// expires and `.timeout` is returned. The inner future is effectively
/// cancelled (no longer polled).
///
/// ## Example
///
/// ```zig
/// var timeout = Timeout(ReadFuture).init(
///     stream.read(&buf),
///     Duration.fromSecs(5),
/// );
///
/// while (true) {
///     switch (timeout.poll(waker)) {
///         .ready => |n| break,
///         .timeout => return error.TimedOut,
///         .pending => suspend(),
///         .err => |e| return e,
///     }
/// }
/// ```
pub fn Timeout(comptime FutureType: type) type {
    // Infer the result type from the future's poll method
    const ResultType = FutureResultType(FutureType);

    return struct {
        inner: FutureType,
        deadline: Instant,
        expired: bool,

        const Self = @This();

        /// Create a timeout wrapper.
        pub fn init(future: FutureType, duration: Duration) Self {
            return .{
                .inner = future,
                .deadline = Instant.now().add(duration),
                .expired = false,
            };
        }

        /// Create a timeout that expires at a specific instant.
        pub fn initAt(future: FutureType, deadline: Instant) Self {
            return .{
                .inner = future,
                .deadline = deadline,
                .expired = false,
            };
        }

        /// Poll the wrapped future with timeout checking.
        pub fn poll(self: *Self, waker: Waker) TimeoutResult(ResultType) {
            // Check if already expired
            if (self.expired) {
                return .timeout;
            }

            // Check deadline before polling
            if (!Instant.now().isBefore(self.deadline)) {
                self.expired = true;
                return .timeout;
            }

            // Poll the inner future
            const result = self.inner.poll(waker);
            return switch (result) {
                .ready => |v| .{ .ready = v },
                .pending => blk: {
                    // Check again after poll (future might have taken time)
                    if (!Instant.now().isBefore(self.deadline)) {
                        self.expired = true;
                        break :blk .timeout;
                    }
                    break :blk .pending;
                },
                .err => |e| .{ .err = e },
            };
        }

        /// Get remaining time until timeout.
        pub fn remaining(self: *const Self) Duration {
            const now = Instant.now();
            if (!now.isBefore(self.deadline)) {
                return Duration.ZERO;
            }
            return self.deadline.durationSince(now);
        }

        /// Check if timeout has expired.
        pub fn isExpired(self: *const Self) bool {
            return self.expired or !Instant.now().isBefore(self.deadline);
        }

        /// Extend the deadline by the given duration.
        pub fn extend(self: *Self, duration: Duration) void {
            self.deadline = self.deadline.add(duration);
            self.expired = false;
        }

        /// Reset the deadline to a new duration from now.
        pub fn reset(self: *Self, duration: Duration) void {
            self.deadline = Instant.now().add(duration);
            self.expired = false;
        }

        /// Get the inner future (consumes the timeout wrapper).
        pub fn intoInner(self: Self) FutureType {
            return self.inner;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Select2 - Race two futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Select result for two futures.
pub fn Select2Result(comptime T1: type, comptime T2: type) type {
    return union(enum) {
        first: T1,
        second: T2,
        pending: void,
        first_err: anyerror,
        second_err: anyerror,
    };
}

/// Race two futures, returning whichever completes first.
///
/// The other future is effectively cancelled (no longer polled) once
/// one completes.
///
/// ## Example
///
/// ```zig
/// var select = Select2(AcceptFuture, ShutdownFuture).init(
///     listener.accept(),
///     shutdown.wait(),
/// );
///
/// switch (select.poll(waker)) {
///     .first => |conn| handleConnection(conn),
///     .second => |_| return,  // Shutdown
///     .pending => {},
///     .first_err => |e| log.err("Accept failed: {}", .{e}),
///     .second_err => |e| log.err("Signal failed: {}", .{e}),
/// }
/// ```
pub fn Select2(comptime F1: type, comptime F2: type) type {
    const R1 = FutureResultType(F1);
    const R2 = FutureResultType(F2);

    return struct {
        future1: F1,
        future2: F2,
        completed: ?enum { first, second } = null,

        const Self = @This();

        pub fn init(f1: F1, f2: F2) Self {
            return .{ .future1 = f1, .future2 = f2 };
        }

        pub fn poll(self: *Self, waker: Waker) Select2Result(R1, R2) {
            // If already completed, return the cached result
            if (self.completed) |which| {
                return switch (which) {
                    .first => .pending, // Already returned
                    .second => .pending,
                };
            }

            // Poll first future
            const r1 = self.future1.poll(waker);
            switch (r1) {
                .ready => |v| {
                    self.completed = .first;
                    return .{ .first = v };
                },
                .err => |e| {
                    self.completed = .first;
                    return .{ .first_err = e };
                },
                .pending => {},
            }

            // Poll second future
            const r2 = self.future2.poll(waker);
            switch (r2) {
                .ready => |v| {
                    self.completed = .second;
                    return .{ .second = v };
                },
                .err => |e| {
                    self.completed = .second;
                    return .{ .second_err = e };
                },
                .pending => {},
            }

            return .pending;
        }

        /// Check if selection has completed.
        pub fn isComplete(self: *const Self) bool {
            return self.completed != null;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Select3 - Race three futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Select result for three futures.
pub fn Select3Result(comptime T1: type, comptime T2: type, comptime T3: type) type {
    return union(enum) {
        first: T1,
        second: T2,
        third: T3,
        pending: void,
        first_err: anyerror,
        second_err: anyerror,
        third_err: anyerror,
    };
}

/// Race three futures.
pub fn Select3(comptime F1: type, comptime F2: type, comptime F3: type) type {
    const R1 = FutureResultType(F1);
    const R2 = FutureResultType(F2);
    const R3 = FutureResultType(F3);

    return struct {
        future1: F1,
        future2: F2,
        future3: F3,
        completed: ?enum { first, second, third } = null,

        const Self = @This();

        pub fn init(f1: F1, f2: F2, f3: F3) Self {
            return .{ .future1 = f1, .future2 = f2, .future3 = f3 };
        }

        pub fn poll(self: *Self, waker: Waker) Select3Result(R1, R2, R3) {
            if (self.completed != null) return .pending;

            // Poll all futures
            const r1 = self.future1.poll(waker);
            switch (r1) {
                .ready => |v| {
                    self.completed = .first;
                    return .{ .first = v };
                },
                .err => |e| {
                    self.completed = .first;
                    return .{ .first_err = e };
                },
                .pending => {},
            }

            const r2 = self.future2.poll(waker);
            switch (r2) {
                .ready => |v| {
                    self.completed = .second;
                    return .{ .second = v };
                },
                .err => |e| {
                    self.completed = .second;
                    return .{ .second_err = e };
                },
                .pending => {},
            }

            const r3 = self.future3.poll(waker);
            switch (r3) {
                .ready => |v| {
                    self.completed = .third;
                    return .{ .third = v };
                },
                .err => |e| {
                    self.completed = .third;
                    return .{ .third_err = e };
                },
                .pending => {},
            }

            return .pending;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Join2 - Wait for two futures to complete
// ═══════════════════════════════════════════════════════════════════════════════

/// Join result for two futures.
pub fn Join2Result(comptime T1: type, comptime T2: type) type {
    return union(enum) {
        ready: struct { T1, T2 },
        pending: void,
        first_err: anyerror,
        second_err: anyerror,
    };
}

/// Wait for two futures to complete concurrently.
///
/// Both futures are polled on each poll() call. Returns when both
/// have completed.
///
/// ## Example
///
/// ```zig
/// var join = Join2(UserFuture, PostsFuture).init(
///     fetchUser(id),
///     fetchPosts(id),
/// );
///
/// switch (join.poll(waker)) {
///     .ready => |results| {
///         const user = results[0];
///         const posts = results[1];
///         renderPage(user, posts);
///     },
///     .pending => {},
///     .first_err, .second_err => |e| return e,
/// }
/// ```
pub fn Join2(comptime F1: type, comptime F2: type) type {
    const R1 = FutureResultType(F1);
    const R2 = FutureResultType(F2);

    return struct {
        future1: F1,
        future2: F2,
        result1: ?R1 = null,
        result2: ?R2 = null,
        error1: ?anyerror = null,
        error2: ?anyerror = null,

        const Self = @This();

        pub fn init(f1: F1, f2: F2) Self {
            return .{ .future1 = f1, .future2 = f2 };
        }

        pub fn poll(self: *Self, waker: Waker) Join2Result(R1, R2) {
            // Check for errors first
            if (self.error1) |e| return .{ .first_err = e };
            if (self.error2) |e| return .{ .second_err = e };

            // Poll first if not done
            if (self.result1 == null) {
                const r1 = self.future1.poll(waker);
                switch (r1) {
                    .ready => |v| self.result1 = v,
                    .err => |e| {
                        self.error1 = e;
                        return .{ .first_err = e };
                    },
                    .pending => {},
                }
            }

            // Poll second if not done
            if (self.result2 == null) {
                const r2 = self.future2.poll(waker);
                switch (r2) {
                    .ready => |v| self.result2 = v,
                    .err => |e| {
                        self.error2 = e;
                        return .{ .second_err = e };
                    },
                    .pending => {},
                }
            }

            // Check if both done
            if (self.result1) |r1| {
                if (self.result2) |r2| {
                    return .{ .ready = .{ r1, r2 } };
                }
            }

            return .pending;
        }

        /// Check if join has completed (both futures done).
        pub fn isComplete(self: *const Self) bool {
            return (self.result1 != null and self.result2 != null) or
                self.error1 != null or self.error2 != null;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Join3 - Wait for three futures to complete
// ═══════════════════════════════════════════════════════════════════════════════

/// Join result for three futures.
pub fn Join3Result(comptime T1: type, comptime T2: type, comptime T3: type) type {
    return union(enum) {
        ready: struct { T1, T2, T3 },
        pending: void,
        first_err: anyerror,
        second_err: anyerror,
        third_err: anyerror,
    };
}

/// Wait for three futures to complete concurrently.
pub fn Join3(comptime F1: type, comptime F2: type, comptime F3: type) type {
    const R1 = FutureResultType(F1);
    const R2 = FutureResultType(F2);
    const R3 = FutureResultType(F3);

    return struct {
        future1: F1,
        future2: F2,
        future3: F3,
        result1: ?R1 = null,
        result2: ?R2 = null,
        result3: ?R3 = null,
        error1: ?anyerror = null,
        error2: ?anyerror = null,
        error3: ?anyerror = null,

        const Self = @This();

        pub fn init(f1: F1, f2: F2, f3: F3) Self {
            return .{ .future1 = f1, .future2 = f2, .future3 = f3 };
        }

        pub fn poll(self: *Self, waker: Waker) Join3Result(R1, R2, R3) {
            if (self.error1) |e| return .{ .first_err = e };
            if (self.error2) |e| return .{ .second_err = e };
            if (self.error3) |e| return .{ .third_err = e };

            if (self.result1 == null) {
                switch (self.future1.poll(waker)) {
                    .ready => |v| self.result1 = v,
                    .err => |e| {
                        self.error1 = e;
                        return .{ .first_err = e };
                    },
                    .pending => {},
                }
            }

            if (self.result2 == null) {
                switch (self.future2.poll(waker)) {
                    .ready => |v| self.result2 = v,
                    .err => |e| {
                        self.error2 = e;
                        return .{ .second_err = e };
                    },
                    .pending => {},
                }
            }

            if (self.result3 == null) {
                switch (self.future3.poll(waker)) {
                    .ready => |v| self.result3 = v,
                    .err => |e| {
                        self.error3 = e;
                        return .{ .third_err = e };
                    },
                    .pending => {},
                }
            }

            if (self.result1) |r1| {
                if (self.result2) |r2| {
                    if (self.result3) |r3| {
                        return .{ .ready = .{ r1, r2, r3 } };
                    }
                }
            }

            return .pending;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SelectWithTimeout - Race a future against a timeout
// ═══════════════════════════════════════════════════════════════════════════════

/// Convenience: Select between a future and a timeout.
///
/// This is a common pattern, so we provide a dedicated type that's
/// simpler than using Select2 with a timer future.
///
/// ## Example
///
/// ```zig
/// var select = SelectWithTimeout(AcceptFuture).init(
///     listener.accept(),
///     Duration.fromSecs(30),
/// );
///
/// switch (select.poll(waker)) {
///     .ready => |conn| handleConnection(conn),
///     .timeout => log.info("No connection in 30s", .{}),
///     .pending => {},
///     .err => |e| return e,
/// }
/// ```
pub fn SelectWithTimeout(comptime FutureType: type) type {
    return Timeout(FutureType);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper: Extract result type from a Future's poll method
// ═══════════════════════════════════════════════════════════════════════════════

fn FutureResultType(comptime FutureType: type) type {
    const poll_fn = @typeInfo(@TypeOf(@field(FutureType, "poll"))).@"fn";
    const ReturnType = poll_fn.return_type.?;

    // ReturnType is PollResult(T), we need to extract T
    const type_info = @typeInfo(ReturnType);
    if (type_info == .@"union") {
        // Find the 'ready' field and get its type
        for (type_info.@"union".fields) |field| {
            if (std.mem.eql(u8, field.name, "ready")) {
                return field.type;
            }
        }
    }

    @compileError("Future type must have poll() returning PollResult(T)");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

// Mock future for testing
const MockFuture = struct {
    value: u32,
    polls_until_ready: u32,
    polls: u32 = 0,

    pub fn init(value: u32, polls: u32) MockFuture {
        return .{ .value = value, .polls_until_ready = polls };
    }

    pub fn poll(self: *MockFuture, _: Waker) PollResult(u32) {
        self.polls += 1;
        if (self.polls >= self.polls_until_ready) {
            return .{ .ready = self.value };
        }
        return .pending;
    }
};

const MockErrorFuture = struct {
    polls_until_error: u32,
    polls: u32 = 0,

    pub fn poll(self: *MockErrorFuture, _: Waker) PollResult(u32) {
        self.polls += 1;
        if (self.polls >= self.polls_until_error) {
            return .{ .err = error.TestError };
        }
        return .pending;
    }
};

test "Timeout - immediate completion" {
    const future = MockFuture.init(42, 1);
    var timeout = Timeout(MockFuture).init(future, Duration.fromSecs(10));

    const result = timeout.poll(.{});
    try std.testing.expect(result == .ready);
    try std.testing.expectEqual(@as(u32, 42), result.ready);
}

test "Timeout - expires" {
    const future = MockFuture.init(42, 1000); // Would take many polls
    var timeout = Timeout(MockFuture).init(future, Duration.fromNanos(1));

    // Wait for timeout
    thr.sleep(1000);

    const result = timeout.poll(.{});
    try std.testing.expect(result == .timeout);
}

test "Timeout - remaining" {
    const future = MockFuture.init(42, 1);
    const timeout = Timeout(MockFuture).init(future, Duration.fromMillis(100));

    const rem = timeout.remaining();
    try std.testing.expect(rem.asMillis() <= 100);
    try std.testing.expect(rem.asMillis() > 0);
}

test "Select2 - first completes" {
    const f1 = MockFuture.init(1, 1);
    const f2 = MockFuture.init(2, 100);
    var select = Select2(MockFuture, MockFuture).init(f1, f2);

    const result = select.poll(.{});
    try std.testing.expect(result == .first);
    try std.testing.expectEqual(@as(u32, 1), result.first);
}

test "Select2 - second completes" {
    const f1 = MockFuture.init(1, 100);
    const f2 = MockFuture.init(2, 1);
    var select = Select2(MockFuture, MockFuture).init(f1, f2);

    // On first poll:
    // f1.poll() -> polls=1, needs 100, pending
    // f2.poll() -> polls=1, needs 1, 1>=1 -> ready!
    // So we should get .second on first poll
    _ = select.poll(.{});
    try std.testing.expect(select.isComplete());
}

test "Join2 - both complete" {
    const f1 = MockFuture.init(1, 1);
    const f2 = MockFuture.init(2, 1);
    var join = Join2(MockFuture, MockFuture).init(f1, f2);

    const result = join.poll(.{});
    try std.testing.expect(result == .ready);
    try std.testing.expectEqual(@as(u32, 1), result.ready[0]);
    try std.testing.expectEqual(@as(u32, 2), result.ready[1]);
}

test "Join2 - waits for both" {
    const f1 = MockFuture.init(1, 1);
    const f2 = MockFuture.init(2, 3);
    var join = Join2(MockFuture, MockFuture).init(f1, f2);

    // First poll - f1 ready, f2 pending
    var result = join.poll(.{});
    try std.testing.expect(result == .pending);

    // Second poll - f2 still pending
    result = join.poll(.{});
    try std.testing.expect(result == .pending);

    // Third poll - f2 ready
    result = join.poll(.{});
    try std.testing.expect(result == .ready);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SelectN - Race N homogeneous futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Result type for SelectN - returns winning index and value.
pub fn SelectNResult(comptime T: type) type {
    return union(enum) {
        /// One of the futures completed successfully.
        /// Contains the index of the completed future and its value.
        ready: struct { index: usize, value: T },
        /// All futures still pending.
        pending: void,
        /// One of the futures returned an error.
        err: struct { index: usize, @"error": anyerror },
    };
}

/// Race N futures of the same type, returning whichever completes first.
///
/// Unlike Select2/Select3 which handle heterogeneous futures, SelectN
/// requires all futures to be the same type. This enables array storage
/// and is more ergonomic when racing many similar operations.
///
/// ## Example
///
/// ```zig
/// // Race multiple fetch operations
/// var futures = [_]FetchFuture{
///     fetch("server1"),
///     fetch("server2"),
///     fetch("server3"),
/// };
///
/// var select = SelectN(FetchFuture, 3).init(&futures);
///
/// while (true) {
///     switch (select.poll(waker)) {
///         .ready => |result| {
///             std.debug.print("Server {} responded: {}\n", .{result.index, result.value});
///             break;
///         },
///         .err => |e| return e.@"error",
///         .pending => suspend(),
///     }
/// }
/// ```
pub fn SelectN(comptime FutureType: type, comptime N: usize) type {
    const ResultType = FutureResultType(FutureType);

    return struct {
        futures: *[N]FutureType,
        winner: ?usize = null,

        const Self = @This();

        /// Create a SelectN from an array of futures.
        pub fn init(futures: *[N]FutureType) Self {
            return .{ .futures = futures };
        }

        /// Poll all futures, returning the first to complete.
        pub fn poll(self: *Self, waker: Waker) SelectNResult(ResultType) {
            // If already completed, return pending (already consumed)
            if (self.winner != null) {
                return .pending;
            }

            // Poll each future in order
            inline for (0..N) |i| {
                const result = self.futures[i].poll(waker);
                switch (result) {
                    .ready => |v| {
                        self.winner = i;
                        return .{ .ready = .{ .index = i, .value = v } };
                    },
                    .err => |e| {
                        self.winner = i;
                        return .{ .err = .{ .index = i, .@"error" = e } };
                    },
                    .pending => {},
                }
            }

            return .pending;
        }

        /// Check if selection has completed.
        pub fn isComplete(self: *const Self) bool {
            return self.winner != null;
        }

        /// Get the winning index (if complete).
        pub fn getWinner(self: *const Self) ?usize {
            return self.winner;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// JoinN - Wait for N homogeneous futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Result type for JoinN.
pub fn JoinNResult(comptime T: type, comptime N: usize) type {
    return union(enum) {
        /// All futures completed successfully.
        ready: [N]T,
        /// Some futures still pending.
        pending: void,
        /// One of the futures returned an error.
        err: struct { index: usize, @"error": anyerror },
    };
}

/// Wait for N futures of the same type to complete concurrently.
///
/// All futures are polled on each poll() call. Returns when all
/// have completed, or when any returns an error.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{
///     fetch(url1),
///     fetch(url2),
///     fetch(url3),
/// };
///
/// var join = JoinN(FetchFuture, 3).init(&futures);
///
/// while (true) {
///     switch (join.poll(waker)) {
///         .ready => |results| {
///             for (results, 0..) |result, i| {
///                 std.debug.print("Result {}: {}\n", .{i, result});
///             }
///             break;
///         },
///         .err => |e| return e.@"error",
///         .pending => suspend(),
///     }
/// }
/// ```
pub fn JoinN(comptime FutureType: type, comptime N: usize) type {
    const ResultType = FutureResultType(FutureType);
    const Result = JoinNResult(ResultType, N);

    return struct {
        futures: *[N]FutureType,
        results: [N]?ResultType = [_]?ResultType{null} ** N,
        error_index: ?usize = null,
        error_value: ?anyerror = null,
        completed_count: usize = 0,

        const Self = @This();

        /// Create a JoinN from an array of futures.
        pub fn init(futures: *[N]FutureType) Self {
            return .{ .futures = futures };
        }

        /// Poll all incomplete futures.
        pub fn poll(self: *Self, waker: Waker) Result {
            // Check for previous error
            if (self.error_index) |idx| {
                return .{ .err = .{ .index = idx, .@"error" = self.error_value.? } };
            }

            // Poll each incomplete future
            inline for (0..N) |i| {
                if (self.results[i] == null) {
                    const result = self.futures[i].poll(waker);
                    switch (result) {
                        .ready => |v| {
                            self.results[i] = v;
                            self.completed_count += 1;
                        },
                        .err => |e| {
                            self.error_index = i;
                            self.error_value = e;
                            return .{ .err = .{ .index = i, .@"error" = e } };
                        },
                        .pending => {},
                    }
                }
            }

            // Check if all done
            if (self.completed_count == N) {
                var final_results: [N]ResultType = undefined;
                inline for (0..N) |i| {
                    final_results[i] = self.results[i].?;
                }
                return .{ .ready = final_results };
            }

            return .pending;
        }

        /// Check if join has completed (all futures done).
        pub fn isComplete(self: *const Self) bool {
            return self.completed_count == N or self.error_index != null;
        }

        /// Get the number of completed futures.
        pub fn completedCount(self: *const Self) usize {
            return self.completed_count;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Convenience: race() - Like Promise.race()
// ═══════════════════════════════════════════════════════════════════════════════

/// Result type for race operation.
pub fn RaceResult(comptime T: type) type {
    return union(enum) {
        /// First future to complete succeeded.
        ok: T,
        /// First future to complete returned an error.
        err: anyerror,
        /// All futures still pending.
        pending: void,
    };
}

/// Race multiple futures, returning the value of the first to complete.
///
/// This is a simpler interface than SelectN when you only care about the
/// value, not which future produced it.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2), fetch(url3) };
/// var racer = race(FetchFuture, 3, &futures);
///
/// while (true) {
///     switch (racer.poll(waker)) {
///         .ok => |value| return value,
///         .err => |e| return e,
///         .pending => suspend(),
///     }
/// }
/// ```
pub fn Race(comptime FutureType: type, comptime N: usize) type {
    const ResultType = FutureResultType(FutureType);

    return struct {
        inner: SelectN(FutureType, N),

        const Self = @This();

        pub fn init(futures: *[N]FutureType) Self {
            return .{ .inner = SelectN(FutureType, N).init(futures) };
        }

        pub fn poll(self: *Self, waker: Waker) RaceResult(ResultType) {
            const result = self.inner.poll(waker);
            return switch (result) {
                .ready => |r| .{ .ok = r.value },
                .err => |e| .{ .err = e.@"error" },
                .pending => .pending,
            };
        }

        pub fn isComplete(self: *const Self) bool {
            return self.inner.isComplete();
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Convenience: all() - Like Promise.all()
// ═══════════════════════════════════════════════════════════════════════════════

/// Result type for all operation.
pub fn AllResult(comptime T: type, comptime N: usize) type {
    return union(enum) {
        /// All futures completed successfully.
        ok: [N]T,
        /// One future returned an error.
        err: anyerror,
        /// Some futures still pending.
        pending: void,
    };
}

/// Wait for all futures to complete, returning all values.
///
/// This is a simpler interface than JoinN when you don't need the
/// error index information.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2), fetch(url3) };
/// var joiner = all(FetchFuture, 3, &futures);
///
/// while (true) {
///     switch (joiner.poll(waker)) {
///         .ok => |values| {
///             for (values) |v| process(v);
///             break;
///         },
///         .err => |e| return e,
///         .pending => suspend(),
///     }
/// }
/// ```
pub fn All(comptime FutureType: type, comptime N: usize) type {
    const ResultType = FutureResultType(FutureType);

    return struct {
        inner: JoinN(FutureType, N),

        const Self = @This();

        pub fn init(futures: *[N]FutureType) Self {
            return .{ .inner = JoinN(FutureType, N).init(futures) };
        }

        pub fn poll(self: *Self, waker: Waker) AllResult(ResultType, N) {
            const result = self.inner.poll(waker);
            return switch (result) {
                .ready => |values| .{ .ok = values },
                .err => |e| .{ .err = e.@"error" },
                .pending => .pending,
            };
        }

        pub fn isComplete(self: *const Self) bool {
            return self.inner.isComplete();
        }

        pub fn completedCount(self: *const Self) usize {
            return self.inner.completedCount();
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SelectN/JoinN Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "SelectN - first wins" {
    var futures = [_]MockFuture{
        MockFuture.init(1, 1), // Ready on first poll
        MockFuture.init(2, 10),
        MockFuture.init(3, 10),
    };

    var select = SelectN(MockFuture, 3).init(&futures);
    const result = select.poll(.{});

    try std.testing.expect(result == .ready);
    try std.testing.expectEqual(@as(usize, 0), result.ready.index);
    try std.testing.expectEqual(@as(u32, 1), result.ready.value);
}

test "SelectN - middle wins" {
    var futures = [_]MockFuture{
        MockFuture.init(1, 10),
        MockFuture.init(2, 1), // Ready on first poll
        MockFuture.init(3, 10),
    };

    var select = SelectN(MockFuture, 3).init(&futures);
    const result = select.poll(.{});

    try std.testing.expect(result == .ready);
    try std.testing.expectEqual(@as(usize, 1), result.ready.index);
    try std.testing.expectEqual(@as(u32, 2), result.ready.value);
}

test "JoinN - all complete" {
    var futures = [_]MockFuture{
        MockFuture.init(1, 1),
        MockFuture.init(2, 1),
        MockFuture.init(3, 1),
    };

    var join = JoinN(MockFuture, 3).init(&futures);
    const result = join.poll(.{});

    try std.testing.expect(result == .ready);
    try std.testing.expectEqual(@as(u32, 1), result.ready[0]);
    try std.testing.expectEqual(@as(u32, 2), result.ready[1]);
    try std.testing.expectEqual(@as(u32, 3), result.ready[2]);
}

test "JoinN - waits for all" {
    var futures = [_]MockFuture{
        MockFuture.init(1, 1), // Ready on poll 1
        MockFuture.init(2, 2), // Ready on poll 2
        MockFuture.init(3, 3), // Ready on poll 3
    };

    var join = JoinN(MockFuture, 3).init(&futures);

    // First poll - only first ready
    var result = join.poll(.{});
    try std.testing.expect(result == .pending);
    try std.testing.expectEqual(@as(usize, 1), join.completedCount());

    // Second poll - first two ready
    result = join.poll(.{});
    try std.testing.expect(result == .pending);
    try std.testing.expectEqual(@as(usize, 2), join.completedCount());

    // Third poll - all ready
    result = join.poll(.{});
    try std.testing.expect(result == .ready);
}

test "Race - returns value directly" {
    var futures = [_]MockFuture{
        MockFuture.init(42, 1),
        MockFuture.init(99, 10),
    };

    var racer = Race(MockFuture, 2).init(&futures);
    const result = racer.poll(.{});

    try std.testing.expect(result == .ok);
    try std.testing.expectEqual(@as(u32, 42), result.ok);
}

test "All - returns all values" {
    var futures = [_]MockFuture{
        MockFuture.init(1, 1),
        MockFuture.init(2, 1),
    };

    var joiner = All(MockFuture, 2).init(&futures);
    const result = joiner.poll(.{});

    try std.testing.expect(result == .ok);
    try std.testing.expectEqual(@as(u32, 1), result.ok[0]);
    try std.testing.expectEqual(@as(u32, 2), result.ok[1]);
}

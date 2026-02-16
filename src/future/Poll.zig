//! Poll - Async Operation Result
//!
//! The fundamental result type for stackless async operations.
//! A future returns Poll when polled:
//! - `.pending` - Not ready, will wake when progress can be made
//! - `.ready` - Complete with result
//!
//! Follows the standard two-state async polling convention.

const std = @import("std");

/// Poll result without a value (for void futures)
pub const Poll = enum {
    /// Operation is not ready, waker has been registered
    pending,
    /// Operation completed successfully
    ready,
};

/// Poll result with a value
pub fn PollResult(comptime T: type) type {
    return union(enum) {
        /// Operation is not ready, waker has been registered
        pending: void,
        /// Operation completed with this value
        ready: T,

        const Self = @This();

        /// Check if ready
        pub fn isReady(self: Self) bool {
            return self == .ready;
        }

        /// Check if pending
        pub fn isPending(self: Self) bool {
            return self == .pending;
        }

        /// Get the value (panics if pending)
        pub fn unwrap(self: Self) T {
            return switch (self) {
                .pending => @panic("called unwrap on pending Poll"),
                .ready => |v| v,
            };
        }

        /// Get the value or null
        pub fn value(self: Self) ?T {
            return switch (self) {
                .pending => null,
                .ready => |v| v,
            };
        }

        /// Map the ready value
        pub fn map(self: Self, comptime U: type, comptime f: fn (T) U) PollResult(U) {
            return switch (self) {
                .pending => .pending,
                .ready => |v| .{ .ready = f(v) },
            };
        }
    };
}

/// Poll result that can also carry an error
pub fn PollError(comptime T: type, comptime E: type) type {
    return union(enum) {
        pending: void,
        ready: T,
        err: E,

        const Self = @This();

        pub fn isReady(self: Self) bool {
            return self == .ready;
        }

        pub fn isPending(self: Self) bool {
            return self == .pending;
        }

        pub fn isError(self: Self) bool {
            return self == .err;
        }

        pub fn unwrap(self: Self) !T {
            return switch (self) {
                .pending => @panic("called unwrap on pending Poll"),
                .ready => |v| v,
                .err => |e| e,
            };
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Poll - basic" {
    const p1: Poll = .pending;
    const p2: Poll = .ready;

    try std.testing.expect(p1 == .pending);
    try std.testing.expect(p2 == .ready);
}

test "PollResult - ready value" {
    const p: PollResult(i32) = .{ .ready = 42 };

    try std.testing.expect(p.isReady());
    try std.testing.expect(!p.isPending());
    try std.testing.expectEqual(@as(i32, 42), p.unwrap());
    try std.testing.expectEqual(@as(?i32, 42), p.value());
}

test "PollResult - pending" {
    const p: PollResult(i32) = .pending;

    try std.testing.expect(!p.isReady());
    try std.testing.expect(p.isPending());
    try std.testing.expectEqual(@as(?i32, null), p.value());
}

test "PollResult - map" {
    const p: PollResult(i32) = .{ .ready = 21 };
    const doubled = p.map(i32, struct {
        fn f(x: i32) i32 {
            return x * 2;
        }
    }.f);

    try std.testing.expectEqual(@as(i32, 42), doubled.unwrap());
}

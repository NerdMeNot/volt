//! Custom Test Assertions
//!
//! Provides specialized assertions for testing async primitives,
//! inspired by Tokio's tokio-test crate macros.
//!
//! Reference: tokio-test crate (assert_pending!, assert_ready!)

const std = @import("std");
const testing = std.testing;

/// Assert that a poll result is Pending
pub fn assertPending(poll_result: anytype) void {
    const T = @TypeOf(poll_result);
    const type_info = @typeInfo(T);

    if (type_info == .optional) {
        if (poll_result) |_| {
            std.debug.panic("Expected null (pending), got value", .{});
        }
    } else if (type_info == .@"union") {
        const tag_name = @tagName(poll_result);
        if (!std.mem.eql(u8, tag_name, "pending")) {
            std.debug.panic("Expected .pending, got .{s}", .{tag_name});
        }
    } else {
        @compileError("assertPending requires optional or union type");
    }
}

/// Assert that a poll result is Ready and return the value
pub fn assertReady(poll_result: anytype) ExtractReadyType(@TypeOf(poll_result)) {
    const T = @TypeOf(poll_result);
    const type_info = @typeInfo(T);

    if (type_info == .optional) {
        return poll_result orelse std.debug.panic("Expected value (ready), got null", .{});
    } else if (type_info == .@"union") {
        return switch (poll_result) {
            .ready => |v| v,
            else => std.debug.panic("Expected .ready, got .{s}", .{@tagName(poll_result)}),
        };
    } else {
        @compileError("assertReady requires optional or union type");
    }
}

fn ExtractReadyType(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .optional) {
        return type_info.optional.child;
    } else if (type_info == .@"union") {
        inline for (type_info.@"union".fields) |field| {
            if (std.mem.eql(u8, field.name, "ready")) {
                return field.type;
            }
        }
        @compileError("Union has no 'ready' field");
    } else {
        @compileError("ExtractReadyType requires optional or union type");
    }
}

/// Assert that a result is Ok and return the value
pub fn assertOk(result: anytype) ExtractOkType(@TypeOf(result)) {
    return result catch |err| {
        std.debug.panic("Expected Ok, got error: {}", .{err});
    };
}

fn ExtractOkType(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .error_union) {
        return type_info.error_union.payload;
    } else {
        @compileError("assertOk requires error union type");
    }
}

/// Assert that a result is an error
pub fn assertErr(result: anytype) ExtractErrType(@TypeOf(result)) {
    if (result) |_| {
        std.debug.panic("Expected error, got Ok", .{});
    } else |err| {
        return err;
    }
}

fn ExtractErrType(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .error_union) {
        return type_info.error_union.error_set;
    } else {
        @compileError("assertErr requires error union type");
    }
}

/// Assert that a receive operation got a value
pub fn assertRecv(result: anytype) ExtractValueType(@TypeOf(result)) {
    const T = @TypeOf(result);
    const type_info = @typeInfo(T);

    if (type_info == .optional) {
        return result orelse std.debug.panic("Expected value, got empty/closed", .{});
    } else if (type_info == .@"union") {
        return switch (result) {
            .value => |v| v,
            .ok => |v| v,
            else => std.debug.panic("Expected .value/.ok, got .{s}", .{@tagName(result)}),
        };
    } else {
        return result;
    }
}

fn ExtractValueType(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .optional) {
        return type_info.optional.child;
    } else if (type_info == .@"union") {
        inline for (type_info.@"union".fields) |field| {
            if (std.mem.eql(u8, field.name, "value") or std.mem.eql(u8, field.name, "ok")) {
                return field.type;
            }
        }
        @compileError("Union has no 'value' or 'ok' field");
    } else {
        return T;
    }
}

/// Assert that a receive operation returned empty
pub fn assertEmpty(result: anytype) void {
    const T = @TypeOf(result);
    const type_info = @typeInfo(T);

    if (type_info == .optional) {
        if (result != null) {
            std.debug.panic("Expected empty (null), got value", .{});
        }
    } else if (type_info == .@"union") {
        const tag_name = @tagName(result);
        if (!std.mem.eql(u8, tag_name, "empty")) {
            std.debug.panic("Expected .empty, got .{s}", .{tag_name});
        }
    } else {
        @compileError("assertEmpty requires optional or union type");
    }
}

/// Assert that a channel operation indicates closed
pub fn assertClosed(result: anytype) void {
    const T = @TypeOf(result);
    const type_info = @typeInfo(T);

    if (type_info == .@"union") {
        const tag_name = @tagName(result);
        if (!std.mem.eql(u8, tag_name, "closed")) {
            std.debug.panic("Expected .closed, got .{s}", .{tag_name});
        }
    } else if (type_info == .error_union) {
        if (result) |_| {
            std.debug.panic("Expected closed error, got Ok", .{});
        } else |err| {
            if (err != error.Closed and err != error.ChannelClosed) {
                std.debug.panic("Expected Closed error, got {}", .{err});
            }
        }
    } else {
        @compileError("assertClosed requires union or error_union type");
    }
}

/// Assert that a broadcast receiver lagged
pub fn assertLagged(result: anytype, expected_lag: u64) void {
    const T = @TypeOf(result);
    const type_info = @typeInfo(T);

    if (type_info == .@"union") {
        switch (result) {
            .lagged => |actual_lag| {
                if (actual_lag != expected_lag) {
                    std.debug.panic("Expected lag of {}, got {}", .{ expected_lag, actual_lag });
                }
            },
            else => std.debug.panic("Expected .lagged, got .{s}", .{@tagName(result)}),
        }
    } else {
        @compileError("assertLagged requires union type with 'lagged' variant");
    }
}

/// Assert a boolean is true with a message
pub fn assertTrue(value: bool, comptime msg: []const u8) void {
    if (!value) {
        std.debug.panic("Assertion failed: {s}", .{msg});
    }
}

/// Assert a boolean is false with a message
pub fn assertFalse(value: bool, comptime msg: []const u8) void {
    if (value) {
        std.debug.panic("Assertion failed (expected false): {s}", .{msg});
    }
}

/// Assert two values are equal
pub fn assertEqual(comptime T: type, expected: T, actual: T) void {
    const type_info = @typeInfo(T);
    const are_equal = blk: {
        if (type_info == .pointer and type_info.pointer.size == .slice) {
            break :blk std.mem.eql(type_info.pointer.child, expected, actual);
        }
        break :blk expected == actual;
    };
    if (!are_equal) {
        std.debug.panic("Expected {any}, got {any}", .{ expected, actual });
    }
}

/// Assert value is greater than
pub fn assertGt(comptime T: type, value: T, threshold: T) void {
    if (value <= threshold) {
        std.debug.panic("Expected {} > {}", .{ value, threshold });
    }
}

/// Assert value is less than
pub fn assertLt(comptime T: type, value: T, threshold: T) void {
    if (value >= threshold) {
        std.debug.panic("Expected {} < {}", .{ value, threshold });
    }
}

/// Assert value is within range [min, max]
pub fn assertInRange(comptime T: type, value: T, min: T, max: T) void {
    if (value < min or value > max) {
        std.debug.panic("Expected {} in range [{}, {}]", .{ value, min, max });
    }
}

/// Assert that execution completed within timeout
pub fn assertTimeout(comptime timeout_ns: u64, comptime func: fn () void) void {
    const start = std.time.nanoTimestamp();
    func();
    const elapsed = std.time.nanoTimestamp() - start;

    if (elapsed > timeout_ns) {
        std.debug.panic(
            "Timeout: took {}ns, expected < {}ns",
            .{ elapsed, timeout_ns },
        );
    }
}

/// RAII-style timeout checker
pub const TimeoutChecker = struct {
    start: i128,
    timeout_ns: u64,
    name: []const u8,

    pub fn init(timeout_ns: u64, name: []const u8) TimeoutChecker {
        return .{
            .start = std.time.nanoTimestamp(),
            .timeout_ns = timeout_ns,
            .name = name,
        };
    }

    pub fn check(self: *const TimeoutChecker) void {
        const elapsed_ns = std.time.nanoTimestamp() - self.start;
        if (elapsed_ns > self.timeout_ns) {
            std.debug.panic(
                "Timeout in '{s}': took {}ns, limit {}ns",
                .{ self.name, elapsed_ns, self.timeout_ns },
            );
        }
    }

    pub fn elapsed(self: *const TimeoutChecker) u64 {
        const e = std.time.nanoTimestamp() - self.start;
        return if (e < 0) 0 else @intCast(e);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "assertPending with optional" {
    const maybe: ?u32 = null;
    assertPending(maybe);
}

test "assertReady with optional" {
    const maybe: ?u32 = 42;
    const value = assertReady(maybe);
    try testing.expectEqual(@as(u32, 42), value);
}

test "assertOk" {
    const result: error{Failed}!u32 = 42;
    const value = assertOk(result);
    try testing.expectEqual(@as(u32, 42), value);
}

test "assertTrue and assertFalse" {
    assertTrue(true, "should be true");
    assertFalse(false, "should be false");
}

test "assertEqual" {
    assertEqual(u32, 42, 42);
    assertEqual([]const u8, "hello", "hello");
}

test "assertInRange" {
    assertInRange(u32, 5, 0, 10);
    assertInRange(u32, 0, 0, 10);
    assertInRange(u32, 10, 0, 10);
}

test "TimeoutChecker" {
    const checker = TimeoutChecker.init(1_000_000_000, "test");
    std.Thread.sleep(1000);
    checker.check();
    try testing.expect(checker.elapsed() > 0);
}

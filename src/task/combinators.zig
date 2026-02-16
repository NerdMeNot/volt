//! # Task Coordination Helpers
//!
//! Helper functions for coordinating multiple concurrent tasks.
//!
//! ## Available Helpers
//!
//! | Function | Description |
//! |----------|-------------|
//! | `joinAll` | Wait for all tasks, fail on first error |
//! | `tryJoinAll` | Wait for all tasks, collect all results/errors |
//! | `race` | First to complete wins, cancel others |
//! | `select` | First to complete, keep others running |
//!
//! ## Example
//!
//! ```zig
//! const user_h = try io.spawn(fetchUser, .{id});
//! const posts_h = try io.spawn(fetchPosts, .{id});
//!
//! // Wait for both
//! const user, const posts = try volt.task.joinAll(.{user_h, posts_h});
//! ```
//!
//! ## Design Note
//!
//! These helpers are syntactic sugar over the async combinators in `volt.async_ops`.
//! They provide a more ergonomic interface for common patterns.

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Join All - Wait for all tasks
// ═══════════════════════════════════════════════════════════════════════════════

/// Wait for all tasks to complete. Returns tuple of results.
/// Fails on first error (remaining tasks continue running).
///
/// ## Example
///
/// ```zig
/// const h1 = try io.spawn(task1, .{});
/// const h2 = try io.spawn(task2, .{});
///
/// const result1, const result2 = try volt.task.joinAll(.{h1, h2});
/// ```
pub fn joinAll(handles: anytype) JoinAllResult(@TypeOf(handles)) {
    const T = @TypeOf(handles);
    const info = @typeInfo(T);

    if (info != .@"struct" or !info.@"struct".is_tuple) {
        @compileError("joinAll expects a tuple of JoinHandles");
    }

    var results: JoinAllResultType(T) = undefined;

    inline for (info.@"struct".fields, 0..) |field, i| {
        var handle = @field(handles, field.name);
        results[i] = handle.join() catch |err| return err;
    }

    return results;
}

pub fn JoinAllResult(comptime T: type) type {
    return JoinAllError(T)!JoinAllResultType(T);
}

fn JoinAllResultType(comptime T: type) type {
    const info = @typeInfo(T).@"struct";
    var result_types: [info.fields.len]type = undefined;

    inline for (info.fields, 0..) |field, i| {
        const HandleType = field.type;
        // Extract the output type from JoinHandle
        result_types[i] = HandleOutputType(HandleType);
    }

    return std.meta.Tuple(&result_types);
}

fn JoinAllError(comptime T: type) type {
    // Combine all possible errors from the handles
    const info = @typeInfo(T).@"struct";
    var error_set: ?type = null;

    inline for (info.fields) |field| {
        const HandleType = field.type;
        const join_ret = @typeInfo(@TypeOf(@field(@as(HandleType, undefined), "join"))).@"fn".return_type.?;
        if (@typeInfo(join_ret) == .error_union) {
            const err = @typeInfo(join_ret).error_union.error_set;
            if (error_set) |e| {
                error_set = e || err;
            } else {
                error_set = err;
            }
        }
    }

    return error_set orelse error{};
}

fn HandleOutputType(comptime HandleType: type) type {
    // JoinHandle(T) has a join() method that returns !T
    const join_ret = @typeInfo(@TypeOf(@field(@as(HandleType, undefined), "join"))).@"fn".return_type.?;
    if (@typeInfo(join_ret) == .error_union) {
        return @typeInfo(join_ret).error_union.payload;
    }
    return join_ret;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Try Join All - Wait for all, collect errors
// ═══════════════════════════════════════════════════════════════════════════════

/// Result type for tryJoinAll - either success or error for each task.
pub fn TaskResult(comptime T: type, comptime E: type) type {
    return union(enum) {
        ok: T,
        err: E,

        pub fn unwrap(self: @This()) !T {
            return switch (self) {
                .ok => |v| v,
                .err => |e| e,
            };
        }

        pub fn isOk(self: @This()) bool {
            return self == .ok;
        }

        pub fn isErr(self: @This()) bool {
            return self == .err;
        }
    };
}

/// Wait for all tasks, collecting both successes and errors.
/// Does not fail early - waits for all tasks to complete.
///
/// ## Example
///
/// ```zig
/// const results = volt.task.tryJoinAll(.{h1, h2, h3});
///
/// for (results) |result| {
///     switch (result) {
///         .ok => |value| handleSuccess(value),
///         .err => |e| handleError(e),
///     }
/// }
/// ```
pub fn tryJoinAll(handles: anytype) TryJoinAllResult(@TypeOf(handles)) {
    const T = @TypeOf(handles);
    const info = @typeInfo(T);

    if (info != .@"struct" or !info.@"struct".is_tuple) {
        @compileError("tryJoinAll expects a tuple of JoinHandles");
    }

    var results: TryJoinAllResult(T) = undefined;

    inline for (info.@"struct".fields, 0..) |field, i| {
        var handle = @field(handles, field.name);
        if (handle.join()) |value| {
            results[i] = .{ .ok = value };
        } else |err| {
            results[i] = .{ .err = err };
        }
    }

    return results;
}

pub fn TryJoinAllResult(comptime T: type) type {
    const info = @typeInfo(T).@"struct";
    var result_types: [info.fields.len]type = undefined;

    inline for (info.fields, 0..) |field, i| {
        const HandleType = field.type;
        const OutputType = HandleOutputType(HandleType);
        const ErrorType = HandleErrorType(HandleType);
        result_types[i] = TaskResult(OutputType, ErrorType);
    }

    return std.meta.Tuple(&result_types);
}

fn HandleErrorType(comptime HandleType: type) type {
    const join_ret = @typeInfo(@TypeOf(@field(@as(HandleType, undefined), "join"))).@"fn".return_type.?;
    if (@typeInfo(join_ret) == .error_union) {
        return @typeInfo(join_ret).error_union.error_set;
    }
    return error{};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Race - First wins, cancel others
// ═══════════════════════════════════════════════════════════════════════════════

/// First task to complete wins. Cancels all other tasks.
///
/// ## Example
///
/// ```zig
/// const h1 = try io.spawn(fetchFromServer1, .{});
/// const h2 = try io.spawn(fetchFromServer2, .{});
///
/// // First response wins
/// const result = try volt.task.race(.{h1, h2});
/// ```
///
/// ## Note
///
/// This is a simplified implementation that polls in order.
/// For true concurrent racing, use the async combinators directly.
pub fn race(handles: anytype) RaceResult(@TypeOf(handles)) {
    const T = @TypeOf(handles);
    const info = @typeInfo(T);

    if (info != .@"struct" or !info.@"struct".is_tuple) {
        @compileError("race expects a tuple of JoinHandles");
    }

    // Simple implementation: try each in order
    // TODO: Use async combinators for true concurrent racing
    inline for (info.@"struct".fields) |field| {
        var handle = @field(handles, field.name);
        if (handle.tryJoin()) |result| {
            // Cancel remaining handles
            inline for (info.@"struct".fields) |other_field| {
                if (!std.mem.eql(u8, other_field.name, field.name)) {
                    var other = @field(handles, other_field.name);
                    other.cancel();
                }
            }
            return result;
        }
    }

    // None ready yet - wait for first one
    var mutable_handles = handles;
    inline for (info.@"struct".fields) |field| {
        var handle = &@field(mutable_handles, field.name);
        const result = handle.join() catch |err| return err;
        // Cancel remaining
        inline for (info.@"struct".fields) |other_field| {
            if (!std.mem.eql(u8, other_field.name, field.name)) {
                var other = &@field(mutable_handles, other_field.name);
                other.cancel();
            }
        }
        return result;
    }

    unreachable;
}

pub fn RaceResult(comptime T: type) type {
    const info = @typeInfo(T).@"struct";
    // All handles must have the same output type for race
    const first_field = info.fields[0];
    return JoinAllError(T)!HandleOutputType(first_field.type);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Select - First wins, keep others
// ═══════════════════════════════════════════════════════════════════════════════

/// First task to complete wins. Other tasks keep running.
/// Returns the result and an index indicating which task completed.
///
/// ## Example
///
/// ```zig
/// const h1 = try io.spawn(task1, .{});
/// const h2 = try io.spawn(task2, .{});
///
/// const result, const index = try volt.task.select(.{h1, h2});
/// // Other task is still running
/// ```
pub fn select(handles: anytype) SelectResult(@TypeOf(handles)) {
    const T = @TypeOf(handles);
    const info = @typeInfo(T);

    if (info != .@"struct" or !info.@"struct".is_tuple) {
        @compileError("select expects a tuple of JoinHandles");
    }

    // Simple implementation: try each in order
    inline for (info.@"struct".fields, 0..) |field, i| {
        var handle = @field(handles, field.name);
        if (handle.tryJoin()) |result| {
            return .{ result, i };
        }
    }

    // None ready yet - wait for first one
    var mutable_handles = handles;
    inline for (info.@"struct".fields, 0..) |field, i| {
        var handle = &@field(mutable_handles, field.name);
        const result = handle.join() catch |err| return err;
        return .{ result, i };
    }

    unreachable;
}

pub fn SelectResult(comptime T: type) type {
    const info = @typeInfo(T).@"struct";
    const first_field = info.fields[0];
    const OutputType = HandleOutputType(first_field.type);
    return JoinAllError(T)!struct { OutputType, usize };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "helpers compile" {
    // Type-level tests only - runtime tests require a running runtime
    _ = joinAll;
    _ = tryJoinAll;
    _ = race;
    _ = select;
}

test "TaskResult - ok variant" {
    const TR = TaskResult(u32, error{TestError});
    const result: TR = .{ .ok = 42 };
    try std.testing.expect(result.isOk());
    try std.testing.expect(!result.isErr());
    try std.testing.expectEqual(@as(u32, 42), try result.unwrap());
}

test "TaskResult - err variant" {
    const TR = TaskResult(u32, error{TestError});
    const result: TR = .{ .err = error.TestError };
    try std.testing.expect(!result.isOk());
    try std.testing.expect(result.isErr());
    try std.testing.expectError(error.TestError, result.unwrap());
}

test "TaskResult - void ok" {
    const TR = TaskResult(void, error{TestError});
    const result: TR = .{ .ok = {} };
    try std.testing.expect(result.isOk());
    _ = try result.unwrap();
}

test "TaskResult - type instantiation" {
    comptime {
        _ = TaskResult(u32, error{A});
        _ = TaskResult(void, error{ A, B });
        _ = TaskResult([]const u8, error{Overflow});
        _ = TaskResult(bool, error{});
    }
}

test "helper type functions compile" {
    comptime {
        _ = JoinAllResult;
        _ = TryJoinAllResult;
        _ = RaceResult;
        _ = SelectResult;
    }
}

//! Blocking Pool Integration Tests
//!
//! Tests the async-to-blocking bridge through io.concurrent().
//! Validates the spin → futex wait strategy and proper wakeup.

const std = @import("std");
const testing = std.testing;

const volt = @import("volt");
const Runtime = volt.Runtime;

// ─────────────────────────────────────────────────────────────────────────────
// Blocking task functions (plain functions, no Io parameter)
// ─────────────────────────────────────────────────────────────────────────────

fn computeSquare(x: u32) u32 {
    return x * x;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Blocking pool - single task" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    const Inner = struct {
        fn run(inner_io: Runtime) !void {
            var f = try inner_io.concurrent(computeSquare, .{@as(u32, 7)});
            const result = try f.await(inner_io);
            try testing.expectEqual(@as(u32, 49), result);
        }
    };

    try io.run(Inner.run);
}

test "Blocking pool - 10 concurrent tasks" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    const Inner = struct {
        fn run(inner_io: Runtime) !void {
            const num_tasks = 10;
            var futures: [num_tasks]Runtime.ConcurrentFuture(u32) = undefined;

            for (0..num_tasks) |i| {
                futures[i] = try inner_io.concurrent(computeSquare, .{@as(u32, @intCast(i))});
            }

            var sum: u32 = 0;
            for (&futures) |*f| {
                sum += try f.await(inner_io);
            }

            // sum of i^2 for i = 0..9 = 285
            try testing.expectEqual(@as(u32, 285), sum);
        }
    };

    try io.run(Inner.run);
}

test "Blocking pool - from async context" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    const Inner = struct {
        fn run(inner_io: Runtime) !void {
            // Use concurrent from within io.run context
            var f1 = try inner_io.concurrent(computeSquare, .{@as(u32, 10)});
            var f2 = try inner_io.concurrent(computeSquare, .{@as(u32, 5)});

            const r1 = try f1.await(inner_io);
            const r2 = try f2.await(inner_io);

            try testing.expectEqual(@as(u32, 100), r1);
            try testing.expectEqual(@as(u32, 25), r2);
        }
    };

    try io.run(Inner.run);
}

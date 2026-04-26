//! v0.1 integration tests — exercise the full public API end-to-end.
//!
//! These tests are the v0.1 acceptance criteria. They cover:
//!   - volt.run bootstrap with a return value
//!   - volt.launch fire-and-forget
//!   - volt.spawn with a typed Task and join
//!   - volt.yield round-robin scheduling
//!   - Job.cancel observed via error.Cancelled at the next yield

const std = @import("std");
const volt = @import("../lib.zig");

// ─────────────────────────────────────────────────────────────────────────────
// 1. volt.run with a value-returning root
// ─────────────────────────────────────────────────────────────────────────────

fn rootReturnsValue() i32 {
    return 42;
}

test "v0.1: volt.run returns root fn value" {
    const result = volt.run(std.testing.allocator, rootReturnsValue, .{});
    try std.testing.expectEqual(@as(i32, 42), result);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. volt.run with an error-returning root
// ─────────────────────────────────────────────────────────────────────────────

const TestError = error{Boom};

fn rootMaybeErrors(should_fail: bool) !i32 {
    if (should_fail) return error.Boom;
    return 7;
}

test "v0.1: volt.run propagates errors from root fn" {
    try std.testing.expectError(error.Boom, volt.run(std.testing.allocator, rootMaybeErrors, .{true}));
    const r = try volt.run(std.testing.allocator, rootMaybeErrors, .{false});
    try std.testing.expectEqual(@as(i32, 7), r);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. volt.spawn + Task.join — pass a value back through a child
// ─────────────────────────────────────────────────────────────────────────────

fn child(seed: i32) i32 {
    return seed * 2;
}

fn parentSpawn() !i32 {
    var t = try volt.spawn(child, .{@as(i32, 21)});
    defer volt.destroyTask(t);
    return try t.join();
}

test "v0.1: spawn + Task.join returns child's value" {
    const r = try volt.run(std.testing.allocator, parentSpawn, .{});
    try std.testing.expectEqual(@as(i32, 42), r);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. volt.launch + Job.join — fire-and-forget with explicit join
// ─────────────────────────────────────────────────────────────────────────────

fn voidWorker(counter: *i32) void {
    counter.* += 1;
}

fn parentLaunch() !void {
    var counter: i32 = 0;
    const j = try volt.launch(voidWorker, .{&counter});
    defer volt.destroyJob(j);
    try j.join();
    if (counter != 1) return error.UnexpectedCount;
}

test "v0.1: launch + Job.join executes the child" {
    try volt.run(std.testing.allocator, parentLaunch, .{});
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. volt.yield — round-robin between coroutines
// ─────────────────────────────────────────────────────────────────────────────

const YieldTrace = struct {
    items: [12]u32 = undefined,
    len: usize = 0,
    fn record(self: *YieldTrace, v: u32) void {
        self.items[self.len] = v;
        self.len += 1;
    }
};

fn yielder(trace: *YieldTrace, id: u32, ticks: u32) !void {
    var i: u32 = 0;
    while (i < ticks) : (i += 1) {
        trace.record(id * 10 + i);
        try volt.yield();
    }
}

fn yieldRoot() !YieldTrace {
    var trace = YieldTrace{};
    var t1 = try volt.spawn(yielder, .{ &trace, @as(u32, 1), @as(u32, 4) });
    defer volt.destroyTask(t1);
    var t2 = try volt.spawn(yielder, .{ &trace, @as(u32, 2), @as(u32, 4) });
    defer volt.destroyTask(t2);
    var t3 = try volt.spawn(yielder, .{ &trace, @as(u32, 3), @as(u32, 4) });
    defer volt.destroyTask(t3);

    try t1.join();
    try t2.join();
    try t3.join();
    return trace;
}

test "v0.1: three coroutines yield round-robin" {
    const trace = try volt.run(std.testing.allocator, yieldRoot, .{});
    // Each yielder records 4 events; total 12.
    try std.testing.expectEqual(@as(usize, 12), trace.len);
    // First three events are id=1,2,3 with i=0 (recorded before first yield).
    try std.testing.expectEqual(@as(u32, 10), trace.items[0]);
    try std.testing.expectEqual(@as(u32, 20), trace.items[1]);
    try std.testing.expectEqual(@as(u32, 30), trace.items[2]);
    // Then i=1 from each.
    try std.testing.expectEqual(@as(u32, 11), trace.items[3]);
    try std.testing.expectEqual(@as(u32, 21), trace.items[4]);
    try std.testing.expectEqual(@as(u32, 31), trace.items[5]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Cancellation — Job.cancel observed via error.Cancelled
// ─────────────────────────────────────────────────────────────────────────────

fn cancellable(observed: *bool) !void {
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try volt.yield();
    }
    observed.* = true; // shouldn't reach if cancelled before all yields
}

fn cancelRoot() !bool {
    var observed = false;
    var t = try volt.spawn(cancellable, .{&observed});
    defer volt.destroyTask(t);

    // Let it run a bit, then cancel.
    try volt.yield();
    try volt.yield();
    t.cancel();

    const result = t.join();
    // Should observe error.Cancelled.
    if (result) |_| return error.ExpectedCancelled else |e| {
        if (e != error.Cancelled) return error.WrongError;
    }

    return observed;
}

test "v0.1: cancellation propagates via error.Cancelled" {
    const observed = try volt.run(std.testing.allocator, cancelRoot, .{});
    try std.testing.expect(!observed); // cancelled before reaching the end
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Many coroutines — stress shape
// ─────────────────────────────────────────────────────────────────────────────

fn smallWorker(counter: *std.atomic.Value(u32)) void {
    _ = counter.fetchAdd(1, .monotonic);
}

fn manyRoot() !u32 {
    var counter = std.atomic.Value(u32).init(0);
    const N = 100;
    var jobs: [N]*volt.Job = undefined;
    for (&jobs) |*j| {
        j.* = try volt.launch(smallWorker, .{&counter});
    }
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return counter.load(.monotonic);
}

test "v0.1: 100 launched coroutines all complete" {
    const result = try volt.run(std.testing.allocator, manyRoot, .{});
    try std.testing.expectEqual(@as(u32, 100), result);
}

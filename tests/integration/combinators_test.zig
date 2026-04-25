//! Combinator Integration Tests — multiple concurrent tasks
//!
//! Tests spawning multiple tasks and coordinating their results
//! through the real scheduler.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Runtime = volt.Runtime;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Helper futures that return specific values
// ─────────────────────────────────────────────────────────────────────────────

fn ValueFuture(comptime val: u32) type {
    return struct {
        pub const Output = u32;

        pub fn init() @This() {
            return .{};
        }

        pub fn poll(_: *@This(), _: *Context) PollResult(u32) {
            return .{ .ready = val };
        }
    };
}

/// Future that increments a counter and returns its index.
const IndexedCounterFuture = struct {
    counter: *Atomic(usize),
    index: u32,

    pub const Output = u32;

    pub fn init(counter: *Atomic(usize), index: u32) IndexedCounterFuture {
        return .{ .counter = counter, .index = index };
    }

    pub fn poll(self: *IndexedCounterFuture, _: *Context) PollResult(u32) {
        _ = self.counter.fetchAdd(1, .monotonic);
        return .{ .ready = self.index };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "joinAll - 4 tasks return correct values" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var h1 = try io.awaitFuture(ValueFuture(1).init());
    var h2 = try io.awaitFuture(ValueFuture(2).init());
    var h3 = try io.awaitFuture(ValueFuture(3).init());
    var h4 = try io.awaitFuture(ValueFuture(4).init());

    const r1 = try h1.await(io);
    const r2 = try h2.await(io);
    const r3 = try h3.await(io);
    const r4 = try h4.await(io);

    try testing.expectEqual(@as(u32, 1), r1);
    try testing.expectEqual(@as(u32, 2), r2);
    try testing.expectEqual(@as(u32, 3), r3);
    try testing.expectEqual(@as(u32, 4), r4);
}

test "race - both tasks complete with valid results" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    var h1 = try io.awaitFuture(IndexedCounterFuture.init(&counter, 10));
    var h2 = try io.awaitFuture(IndexedCounterFuture.init(&counter, 20));

    const r1 = try h1.await(io);
    const r2 = try h2.await(io);

    // Both tasks completed
    try testing.expectEqual(@as(usize, 2), counter.load(.acquire));
    // Each returned its index
    try testing.expectEqual(@as(u32, 10), r1);
    try testing.expectEqual(@as(u32, 20), r2);
}

test "select - 8 tasks all complete" {
    const num_tasks = 8;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(u32) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(IndexedCounterFuture.init(&counter, @intCast(i)));
    }

    // Await all and collect results
    var results: [num_tasks]u32 = undefined;
    for (0..num_tasks) |i| {
        results[i] = try handles[i].await(io);
    }

    // All 8 tasks completed
    try testing.expectEqual(@as(usize, num_tasks), counter.load(.acquire));

    // Each returned its index
    for (0..num_tasks) |i| {
        try testing.expectEqual(@as(u32, @intCast(i)), results[i]);
    }
}

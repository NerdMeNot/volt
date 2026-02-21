//! Scheduler Integration Tests
//!
//! Validates work distribution, dynamic spawn, and task lifecycle
//! through the real work-stealing scheduler.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Io = volt.Io;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Helper futures
// ─────────────────────────────────────────────────────────────────────────────

/// Future that records which OS thread executed it.
/// Yields once (returns .pending) to force a reschedule, then completes.
/// This creates real scheduler backpressure so work-stealing can kick in.
const ThreadIdFuture = struct {
    result: *Atomic(usize),
    yielded: bool = false,

    pub const Output = void;

    pub fn init(result: *Atomic(usize)) ThreadIdFuture {
        return .{ .result = result };
    }

    pub fn poll(self: *ThreadIdFuture, ctx: *Context) PollResult(void) {
        if (!self.yielded) {
            // First poll: record tid, request reschedule, return pending.
            // This creates backpressure — tasks sit in queues long enough
            // for the second worker to steal some.
            const tid = std.Thread.getCurrentId();
            self.result.store(@intCast(tid), .release);
            self.yielded = true;
            ctx.waker.wakeByRef();
            return .pending;
        }
        return .{ .ready = {} };
    }
};

/// Future that increments an atomic counter.
const CounterFuture = struct {
    counter: *Atomic(usize),

    pub const Output = void;

    pub fn init(counter: *Atomic(usize)) CounterFuture {
        return .{ .counter = counter };
    }

    pub fn poll(self: *CounterFuture, _: *Context) PollResult(void) {
        _ = self.counter.fetchAdd(1, .monotonic);
        return .{ .ready = {} };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Work stealing - 100 tasks on 2 workers" {
    const num_tasks = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 2 });
    defer io.deinit();

    var tids: [num_tasks]Atomic(usize) = undefined;
    for (&tids) |*t| t.* = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(ThreadIdFuture.init(&tids[i]));
    }
    for (&handles) |*h| _ = h.await(io);

    // Verify all 100 completed (tid != 0)
    var completed: usize = 0;
    var unique_tids = std.AutoHashMap(usize, void).init(testing.allocator);
    defer unique_tids.deinit();

    for (&tids) |*t| {
        const tid = t.load(.acquire);
        if (tid != 0) {
            completed += 1;
            try unique_tids.put(tid, {});
        }
    }

    try testing.expectEqual(@as(usize, num_tasks), completed);
    // At least 2 distinct threads used (proves work was distributed)
    try testing.expect(unique_tids.count() >= 2);
}

test "Spawn batch - 100 tasks all complete" {
    const num_tasks = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(CounterFuture.init(&counter));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks), counter.load(.acquire));
}

test "Sequential spawn+await - 50 rounds" {
    const rounds = 50;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    for (0..rounds) |_| {
        var h = try io.awaitFuture(CounterFuture.init(&counter));
        _ = h.await(io);
    }

    try testing.expectEqual(@as(usize, rounds), counter.load(.acquire));
}

test "Dynamic spawn under load" {
    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    // Spawn 10 tasks in first batch
    const batch_size = 10;
    var batch1: [batch_size]volt.Future(void) = undefined;
    for (0..batch_size) |i| {
        batch1[i] = try io.awaitFuture(CounterFuture.init(&counter));
    }

    // Spawn 10 more while first batch is running
    var batch2: [batch_size]volt.Future(void) = undefined;
    for (0..batch_size) |i| {
        batch2[i] = try io.awaitFuture(CounterFuture.init(&counter));
    }

    // Await all
    for (&batch1) |*h| _ = h.await(io);
    for (&batch2) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, batch_size * 2), counter.load(.acquire));
}

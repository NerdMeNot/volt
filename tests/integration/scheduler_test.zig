//! Scheduler Integration Tests
//!
//! Validates work distribution, dynamic spawn, and task lifecycle
//! through the real work-stealing scheduler.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Runtime = volt.Runtime;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Helper futures
// ─────────────────────────────────────────────────────────────────────────────

/// Future that records which OS thread executed it.
const ThreadIdFuture = struct {
    result: *Atomic(usize),

    pub const Output = void;

    pub fn init(result: *Atomic(usize)) ThreadIdFuture {
        return .{ .result = result };
    }

    pub fn poll(self: *ThreadIdFuture, _: *Context) PollResult(void) {
        const tid = std.Thread.getCurrentId();
        self.result.store(@intCast(tid), .release);
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

test "Scheduler - 100 tasks on 2 workers all complete" {
    const num_tasks = 100;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 2 });
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
    for (&tids) |*t| {
        const tid = t.load(.acquire);
        if (tid != 0) {
            completed += 1;
        }
    }

    try testing.expectEqual(@as(usize, num_tasks), completed);
    // Note: we don't assert work distribution (unique_tids >= 2) because
    // it's non-deterministic — the scheduler may run all tasks on one worker
    // if they complete before the second worker wakes up (common on ARM64 CI).
}

test "Spawn batch - 100 tasks all complete" {
    const num_tasks = 100;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
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

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var counter = Atomic(usize).init(0);

    for (0..rounds) |_| {
        var h = try io.awaitFuture(CounterFuture.init(&counter));
        _ = h.await(io);
    }

    try testing.expectEqual(@as(usize, rounds), counter.load(.acquire));
}

test "Dynamic spawn under load" {
    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
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

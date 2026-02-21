//! Barrier Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Barrier using randomized thread interleavings.
//!
//! Invariants tested:
//! - Exactly 1 leader per generation
//! - All waiters released together
//! - Barrier is reusable across generations

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;
const ConcurrentCounter = common.ConcurrentCounter;
const Latch = common.Latch;

const busyWaitYield = common.busyWaitYield;

const sync = @import("volt").sync;
const Barrier = sync.Barrier;
const Waiter = sync.barrier.Waiter;

// ─────────────────────────────────────────────────────────────────────────────
// Context types and work functions for runWithModel
// ─────────────────────────────────────────────────────────────────────────────

const LeaderTestContext = struct {
    barrier: *Barrier,
    leader_count: *Atomic(usize),
    arrived: *Atomic(usize),
};

fn leaderTestWork(ctx: LeaderTestContext, m: *Model, _: usize) void {
    _ = m;
    var waiter = Waiter.init();
    const is_leader = ctx.barrier.waitWith(&waiter);

    if (!is_leader) {
        while (!waiter.isReleased()) {
            busyWaitYield();
        }
    }

    if (waiter.is_leader.load(.acquire)) {
        _ = ctx.leader_count.fetchAdd(1, .monotonic);
    }

    _ = ctx.arrived.fetchAdd(1, .release);
}

const ReleaseTestContext = struct {
    barrier: *Barrier,
    released_count: *Atomic(usize),
};

fn releaseTestWork(ctx: ReleaseTestContext, m: *Model, _: usize) void {
    _ = m;
    var waiter = Waiter.init();
    const immediate = ctx.barrier.waitWith(&waiter);

    if (!immediate) {
        while (!waiter.isReleased()) {
            busyWaitYield();
        }
    }

    _ = ctx.released_count.fetchAdd(1, .release);
}

const ReuseTestContext = struct {
    barrier: *Barrier,
    leader_per_gen: *[3]Atomic(usize),
};

fn reuseTestWork(ctx: ReuseTestContext, m: *Model, _: usize) void {
    _ = m;
    const generations: usize = 3;

    for (0..generations) |gen| {
        var waiter = Waiter.init();
        const immediate = ctx.barrier.waitWith(&waiter);

        if (!immediate) {
            while (!waiter.isReleased()) {
                busyWaitYield();
            }
        }

        if (waiter.is_leader.load(.acquire)) {
            _ = ctx.leader_per_gen[gen].fetchAdd(1, .monotonic);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Exactly One Leader
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - exactly one leader per generation" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const num_tasks: usize = 4;
            var barrier = Barrier.init(num_tasks);
            var leader_count = Atomic(usize).init(0);
            var arrived = Atomic(usize).init(0);

            const ctx = LeaderTestContext{
                .barrier = &barrier,
                .leader_count = &leader_count,
                .arrived = &arrived,
            };

            runWithModel(model, num_tasks, ctx, leaderTestWork);

            // Wait for all to complete
            while (arrived.load(.acquire) < num_tasks) {
                busyWaitYield();
            }

            // Exactly one leader
            try testing.expectEqual(@as(usize, 1), leader_count.load(.acquire));
            // Generation advanced
            try testing.expectEqual(@as(usize, 1), barrier.currentGeneration());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: All Waiters Released Together
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - all waiters released together" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const num_tasks: usize = 4;
            var barrier = Barrier.init(num_tasks);
            var released_count = Atomic(usize).init(0);

            const ctx = ReleaseTestContext{
                .barrier = &barrier,
                .released_count = &released_count,
            };

            runWithModel(model, num_tasks, ctx, releaseTestWork);

            // All should be released
            while (released_count.load(.acquire) < num_tasks) {
                busyWaitYield();
            }
            try testing.expectEqual(num_tasks, released_count.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Single Task Barrier
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - single task returns immediately as leader" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var barrier = Barrier.init(1);

            var waiter = Waiter.init();
            const immediate = barrier.waitWith(&waiter);

            try testing.expect(immediate);
            try testing.expect(waiter.is_leader.load(.acquire));
            try testing.expect(waiter.isReleased());
            try testing.expectEqual(@as(usize, 1), barrier.currentGeneration());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Barrier Reuse
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - barrier reusable across generations" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const num_tasks: usize = 3;
            const generations: usize = 3;
            var barrier = Barrier.init(num_tasks);
            var leader_per_gen: [generations]Atomic(usize) = undefined;
            for (&leader_per_gen) |*l| {
                l.* = Atomic(usize).init(0);
            }

            const ctx = ReuseTestContext{
                .barrier = &barrier,
                .leader_per_gen = &leader_per_gen,
            };

            runWithModel(model, num_tasks, ctx, reuseTestWork);

            // Exactly one leader per generation
            for (0..generations) |gen| {
                try testing.expectEqual(@as(usize, 1), leader_per_gen[gen].load(.acquire));
            }

            try testing.expectEqual(generations, barrier.currentGeneration());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Large Task Count
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - large task count" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const num_tasks: usize = 8;
            var barrier = Barrier.init(num_tasks);
            var leader_count = Atomic(usize).init(0);
            var woken_count = Atomic(usize).init(0);

            var threads: [num_tasks]std.Thread = undefined;
            var spawned: usize = 0;

            for (0..num_tasks) |_| {
                threads[spawned] = std.Thread.spawn(.{}, struct {
                    fn run(b: *Barrier, leaders: *Atomic(usize), woken: *Atomic(usize)) void {
                        var waiter = Waiter.init();
                        const immediate = b.waitWith(&waiter);

                        if (!immediate) {
                            while (!waiter.isReleased()) {
                                busyWaitYield();
                            }
                        }

                        if (waiter.is_leader.load(.acquire)) {
                            _ = leaders.fetchAdd(1, .monotonic);
                        }
                        _ = woken.fetchAdd(1, .monotonic);
                    }
                }.run, .{ &barrier, &leader_count, &woken_count }) catch continue;
                spawned += 1;
            }

            for (threads[0..spawned]) |t| {
                t.join();
            }

            try testing.expectEqual(@as(usize, 1), leader_count.load(.acquire));
            try testing.expectEqual(spawned, woken_count.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Waiter Wakeup
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - waiters are woken via callback" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const num_tasks: usize = 3;
            var barrier = Barrier.init(num_tasks);
            var woken: [num_tasks]Atomic(bool) = undefined;
            for (&woken) |*w| {
                w.* = Atomic(bool).init(false);
            }

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const w: *Atomic(bool) = @ptrCast(@alignCast(ctx));
                    w.store(true, .release);
                }
            };

            var threads: [num_tasks]std.Thread = undefined;
            var spawned: usize = 0;

            for (0..num_tasks) |i| {
                threads[i] = std.Thread.spawn(.{}, struct {
                    fn run(b: *Barrier, w: *Atomic(bool)) void {
                        var waiter = Waiter.initWithWaker(@ptrCast(w), Waker.wake);
                        const immediate = b.waitWith(&waiter);
                        if (immediate) {
                            // Leader - waker won't be called for leader
                        } else {
                            while (!waiter.isReleased()) {
                                busyWaitYield();
                            }
                        }
                    }
                }.run, .{ &barrier, &woken[i] }) catch continue;
                spawned += 1;
            }

            for (threads[0..spawned]) |t| {
                t.join();
            }

            // At least num_tasks-1 should have waker called (all except leader)
            var woke_count: usize = 0;
            for (woken[0..spawned]) |w| {
                if (w.load(.acquire)) woke_count += 1;
            }
            try testing.expect(woke_count >= spawned - 1);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Arrived Count Tracking
// ─────────────────────────────────────────────────────────────────────────────

test "Barrier loom - arrived count tracking" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var barrier = Barrier.init(5);

            try testing.expectEqual(@as(usize, 0), barrier.arrivedCount());

            var waiters: [4]Waiter = undefined;
            for (0..4) |i| {
                waiters[i] = Waiter.init();
                _ = barrier.waitWith(&waiters[i]);
                try testing.expectEqual(i + 1, barrier.arrivedCount());
            }

            // Fifth arrival should trigger release
            var leader = Waiter.init();
            const immediate = barrier.waitWith(&leader);
            try testing.expect(immediate);

            // Reset for next generation
            try testing.expectEqual(@as(usize, 0), barrier.arrivedCount());
        }
    }.run);
}

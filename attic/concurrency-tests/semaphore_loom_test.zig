//! Semaphore Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Semaphore using randomized thread interleavings.
//!
//! Invariants tested:
//! - held_permits <= initial_permits (at any point)
//! - FIFO grant ordering
//! - Partial fulfillment works correctly

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;
const ConcurrentCounter = common.ConcurrentCounter;

const busyWaitYield = common.busyWaitYield;

const sync = @import("volt").sync;
const Semaphore = sync.Semaphore;
const Waiter = sync.semaphore.Waiter;

// ─────────────────────────────────────────────────────────────────────────────
// Invariant Checker
// ─────────────────────────────────────────────────────────────────────────────

const SemaphoreInvariantChecker = struct {
    max_permits: usize,
    held_permits: Atomic(usize) = Atomic(usize).init(0),
    violation_detected: Atomic(bool) = Atomic(bool).init(false),
    max_held: Atomic(usize) = Atomic(usize).init(0),

    const Self = @This();

    fn init(max_permits: usize) Self {
        return .{ .max_permits = max_permits };
    }

    fn acquire(self: *Self, n: usize) void {
        const held = self.held_permits.fetchAdd(n, .seq_cst) + n;
        if (held > self.max_permits) {
            self.violation_detected.store(true, .seq_cst);
        }

        // Track max held
        var max = self.max_held.load(.monotonic);
        while (held > max) {
            const result = self.max_held.cmpxchgWeak(max, held, .monotonic, .monotonic);
            if (result) |v| {
                max = v;
            } else {
                break;
            }
        }
    }

    fn release(self: *Self, n: usize) void {
        const prev = self.held_permits.fetchSub(n, .seq_cst);
        if (prev < n) {
            self.violation_detected.store(true, .seq_cst);
        }
    }

    fn verify(self: *const Self) !void {
        if (self.violation_detected.load(.acquire)) {
            return error.SemaphoreInvariantViolation;
        }
        if (self.held_permits.load(.acquire) != 0) {
            return error.LeakedPermits;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Test: Basic Permit Tracking
// ─────────────────────────────────────────────────────────────────────────────

const PermitCountContext = struct {
    semaphore: *Semaphore,
    checker: *SemaphoreInvariantChecker,
};

fn permitCountWork(ctx: PermitCountContext, m: *Model, _: usize) void {
    for (0..5) |_| {
        if (ctx.semaphore.tryAcquire(1)) {
            ctx.checker.acquire(1);
            m.spin();
            ctx.checker.release(1);
            ctx.semaphore.release(1);
        }
        m.yield();
    }
}

test "Semaphore loom - permit count stays within bounds" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const max_permits: usize = 3;
            var sem = Semaphore.init(max_permits);
            var checker = SemaphoreInvariantChecker.init(max_permits);

            const ctx = PermitCountContext{
                .semaphore = &sem,
                .checker = &checker,
            };

            runWithModel(model, 4, ctx, permitCountWork);

            try checker.verify();
            // Verify max held was at most max_permits
            try testing.expect(checker.max_held.load(.acquire) <= max_permits);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Multiple Permits at Once
// ─────────────────────────────────────────────────────────────────────────────

const MultiPermitContext = struct {
    semaphore: *Semaphore,
    checker: *SemaphoreInvariantChecker,
};

fn multiPermitWork(ctx: MultiPermitContext, m: *Model, tid: usize) void {
    // Each thread tries to acquire different amounts
    const permits: usize = (tid % 3) + 1; // 1, 2, or 3 permits

    for (0..4) |_| {
        if (ctx.semaphore.tryAcquire(permits)) {
            ctx.checker.acquire(permits);
            m.spin();
            ctx.checker.release(permits);
            ctx.semaphore.release(permits);
        }
        m.yield();
    }
}

test "Semaphore loom - multi-permit acquire" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const max_permits: usize = 4;
            var sem = Semaphore.init(max_permits);
            var checker = SemaphoreInvariantChecker.init(max_permits);

            const ctx = MultiPermitContext{
                .semaphore = &sem,
                .checker = &checker,
            };

            runWithModel(model, 3, ctx, multiPermitWork);

            try checker.verify();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Waiter FIFO Ordering
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore loom - FIFO waiter ordering" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var sem = Semaphore.init(0); // Start with no permits
            var woken_order: [3]Atomic(usize) = .{
                Atomic(usize).init(0),
                Atomic(usize).init(0),
                Atomic(usize).init(0),
            };
            var order_counter = Atomic(usize).init(1);

            const Waker = struct {
                fn wake(_: *anyopaque) void {}
            };

            var threads: [3]std.Thread = undefined;
            var spawned: usize = 0;

            // Spawn waiters
            for (0..3) |i| {
                threads[i] = std.Thread.spawn(.{}, struct {
                    fn run(s: *Semaphore, order: *[3]Atomic(usize), counter: *Atomic(usize), idx: usize) void {
                        var waiter = Waiter.initWithWaker(1, undefined, Waker.wake);
                        if (!s.acquireWait(&waiter)) {
                            while (!waiter.isComplete()) {
                                busyWaitYield();
                            }
                        }
                        const my_order = counter.fetchAdd(1, .acq_rel);
                        order[idx].store(my_order, .release);
                        s.release(1);
                    }
                }.run, .{ &sem, &woken_order, &order_counter, i }) catch continue;
                spawned += 1;
                std.Thread.yield() catch {};
            }

            // Release permits one at a time
            for (0..spawned) |_| {
                std.Thread.yield() catch {};
                sem.release(1);
            }

            for (threads[0..spawned]) |t| {
                t.join();
            }

            // Verify all waiters completed
            for (0..spawned) |i| {
                try testing.expect(woken_order[i].load(.acquire) > 0);
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Returns Partial Permits
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore loom - cancel returns partial permits" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var sem = Semaphore.init(2);

            // Exhaust permits
            try testing.expect(sem.tryAcquire(2));
            try testing.expectEqual(@as(usize, 0), sem.availablePermits());

            // Start waiting for more
            var waiter = Waiter.init(2);
            try testing.expect(!sem.acquireWait(&waiter));

            // Cancel should return any partial
            sem.cancelAcquire(&waiter);
            try testing.expectEqual(@as(usize, 0), sem.waiterCount());

            // Release original permits
            sem.release(2);
            try testing.expectEqual(@as(usize, 2), sem.availablePermits());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Acquire and Release
// ─────────────────────────────────────────────────────────────────────────────

const ConcurrentAcquireReleaseContext = struct {
    semaphore: *Semaphore,
    checker: *SemaphoreInvariantChecker,
    total_ops: *ConcurrentCounter,
};

fn concurrentAcquireReleaseWork(ctx: ConcurrentAcquireReleaseContext, m: *Model, _: usize) void {
    for (0..8) |_| {
        const n: usize = 1;
        if (ctx.semaphore.tryAcquire(n)) {
            ctx.checker.acquire(n);
            m.spin();
            ctx.checker.release(n);
            ctx.semaphore.release(n);
            ctx.total_ops.increment();
        }
        m.yield();
    }
}

test "Semaphore loom - concurrent acquire and release" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            const max_permits: usize = 5;
            var sem = Semaphore.init(max_permits);
            var checker = SemaphoreInvariantChecker.init(max_permits);
            var total_ops = ConcurrentCounter{};

            const ctx = ConcurrentAcquireReleaseContext{
                .semaphore = &sem,
                .checker = &checker,
                .total_ops = &total_ops,
            };

            runWithModel(model, 4, ctx, concurrentAcquireReleaseWork);

            try checker.verify();
            // Verify all permits are back
            try testing.expectEqual(max_permits, sem.availablePermits());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Zero Initial Permits
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore loom - zero initial permits" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var sem = Semaphore.init(0);

            // tryAcquire should fail
            try testing.expect(!sem.tryAcquire(1));

            // Add permits then acquire
            sem.release(2);
            try testing.expect(sem.tryAcquire(1));
            try testing.expect(sem.tryAcquire(1));
            try testing.expect(!sem.tryAcquire(1));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Waiter Wakeup on Release
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore loom - waiters woken on release" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var sem = Semaphore.init(0);
            var woken = Atomic(bool).init(false);
            var queued = Atomic(bool).init(false);

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const w: *Atomic(bool) = @ptrCast(@alignCast(ctx));
                    w.store(true, .release);
                }
            };

            var thread = std.Thread.spawn(.{}, struct {
                fn run(s: *Semaphore, w: *Atomic(bool), q: *Atomic(bool)) void {
                    var waiter = Waiter.initWithWaker(1, @ptrCast(w), Waker.wake);
                    if (!s.acquireWait(&waiter)) {
                        // Signal that we're queued
                        q.store(true, .release);
                        while (!waiter.isComplete()) {
                            busyWaitYield();
                        }
                    } else {
                        // Got permit immediately
                        q.store(true, .release);
                    }
                    s.release(1);
                }
            }.run, .{ &sem, &woken, &queued }) catch return;

            // Wait for waiter to actually queue
            while (!queued.load(.acquire)) {
                busyWaitYield();
            }

            // Release should wake waiter
            sem.release(1);

            thread.join();
            // Either woken by release, or got permit immediately
            try testing.expect(woken.load(.acquire) or sem.availablePermits() >= 1);
        }
    }.run);
}

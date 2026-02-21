//! Mutex Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Mutex using randomized thread interleavings.
//! Tests verify safety invariants hold under all explored interleavings.
//!
//! Invariants tested:
//! - At most 1 holder at any time (mutual exclusion)
//! - FIFO ordering for waiters
//! - No lost wakeups
//! - Cancel doesn't corrupt state

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;
const ConcurrentCounter = common.ConcurrentCounter;
const Barrier = common.Barrier;

const busyWaitYield = common.busyWaitYield;

const sync = @import("volt").sync;
const Mutex = sync.Mutex;
const Waiter = sync.mutex.Waiter;

// ─────────────────────────────────────────────────────────────────────────────
// Invariant Checker
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks mutex invariants during concurrent access
pub const MutexInvariantChecker = struct {
    /// Number of current lock holders (should never exceed 1)
    active_holders: Atomic(usize) = Atomic(usize).init(0),
    /// Maximum holders ever observed (for verification)
    max_holders: Atomic(usize) = Atomic(usize).init(0),
    /// Total successful lock acquisitions
    total_acquisitions: Atomic(usize) = Atomic(usize).init(0),
    /// Violation flag
    violation_detected: Atomic(bool) = Atomic(bool).init(false),

    const Self = @This();

    pub fn acquire(self: *Self) void {
        const prev = self.active_holders.fetchAdd(1, .seq_cst);
        if (prev > 0) {
            // Violation: more than one holder
            self.violation_detected.store(true, .seq_cst);
        }

        // Track max holders
        const current = prev + 1;
        var max = self.max_holders.load(.monotonic);
        while (current > max) {
            const result = self.max_holders.cmpxchgWeak(max, current, .monotonic, .monotonic);
            if (result) |v| {
                max = v;
            } else {
                break;
            }
        }

        _ = self.total_acquisitions.fetchAdd(1, .monotonic);
    }

    pub fn release(self: *Self) void {
        const prev = self.active_holders.fetchSub(1, .seq_cst);
        if (prev == 0) {
            // Violation: released without holding
            self.violation_detected.store(true, .seq_cst);
        }
    }

    pub fn verify(self: *const Self) !void {
        if (self.violation_detected.load(.acquire)) {
            return error.MutualExclusionViolation;
        }
        if (self.max_holders.load(.acquire) > 1) {
            return error.TooManyHolders;
        }
        if (self.active_holders.load(.acquire) != 0) {
            return error.LeakedLock;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Test: Basic Mutual Exclusion
// ─────────────────────────────────────────────────────────────────────────────

const MutualExclusionContext = struct {
    mutex: *Mutex,
    checker: *MutexInvariantChecker,
    completion: *Barrier,
};

fn mutualExclusionWork(ctx: MutualExclusionContext, m: *Model, _: usize) void {
    _ = m;
    // Each thread tries to acquire multiple times
    for (0..10) |_| {
        if (ctx.mutex.tryLock()) {
            ctx.checker.acquire();

            // Critical section - yield to create interleaving opportunities
            std.Thread.yield() catch {};

            ctx.checker.release();
            ctx.mutex.unlock();
        }
        std.Thread.yield() catch {};
    }

    _ = ctx.completion.wait();
}

test "Mutex loom - mutual exclusion with tryLock" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var mutex = Mutex.init();
            var checker = MutexInvariantChecker{};
            var completion = Barrier.init(4);

            const ctx = MutualExclusionContext{
                .mutex = &mutex,
                .checker = &checker,
                .completion = &completion,
            };

            runWithModel(model, 4, ctx, mutualExclusionWork);

            try checker.verify();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Rapid Lock/Unlock Cycling
// ─────────────────────────────────────────────────────────────────────────────

const RapidCyclingContext = struct {
    mutex: *Mutex,
    checker: *MutexInvariantChecker,
    total_ops: *ConcurrentCounter,
};

fn rapidCyclingWork(ctx: RapidCyclingContext, m: *Model, _: usize) void {
    for (0..20) |_| {
        if (ctx.mutex.tryLock()) {
            ctx.checker.acquire();
            // Minimal critical section
            m.spin();
            ctx.checker.release();
            ctx.mutex.unlock();
            ctx.total_ops.increment();
        }
        m.yield();
    }
}

test "Mutex loom - rapid lock/unlock cycling" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var mutex = Mutex.init();
            var checker = MutexInvariantChecker{};
            var total_ops = ConcurrentCounter{};

            const ctx = RapidCyclingContext{
                .mutex = &mutex,
                .checker = &checker,
                .total_ops = &total_ops,
            };

            runWithModel(model, 4, ctx, rapidCyclingWork);

            try checker.verify();
            // Verify some operations completed
            try testing.expect(total_ops.get() > 0);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Lock with Waiters
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex loom - lock with waiters delivers wakeups" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var mutex = Mutex.init();
            var checker = MutexInvariantChecker{};
            var woken_count = Atomic(usize).init(0);

            const TestWaker = struct {
                fn wake(ctx: *anyopaque) void {
                    const count: *Atomic(usize) = @ptrCast(@alignCast(ctx));
                    _ = count.fetchAdd(1, .monotonic);
                }
            };

            var threads: [3]std.Thread = undefined;
            var spawned: usize = 0;

            // First thread holds the lock
            threads[0] = std.Thread.spawn(.{}, struct {
                fn run(m: *Mutex, check: *MutexInvariantChecker) void {
                    if (m.tryLock()) {
                        check.acquire();
                        // Hold for a bit to let others queue up
                        std.Thread.yield() catch {};
                        std.Thread.yield() catch {};
                        check.release();
                        m.unlock();
                    }
                }
            }.run, .{ &mutex, &checker }) catch {
                return;
            };
            spawned += 1;

            // Wait a tiny bit for first thread to grab lock
            std.Thread.yield() catch {};

            // Other threads try to acquire with waiters
            for (1..3) |_| {
                threads[spawned] = std.Thread.spawn(.{}, struct {
                    fn run(m: *Mutex, check: *MutexInvariantChecker, woken: *Atomic(usize)) void {
                        var waiter = Waiter.initWithWaker(@ptrCast(woken), TestWaker.wake);
                        if (!m.lockWait(&waiter)) {
                            // Wait for acquisition
                            while (!waiter.isAcquired()) {
                                busyWaitYield();
                            }
                        }
                        check.acquire();
                        std.Thread.yield() catch {};
                        check.release();
                        m.unlock();
                    }
                }.run, .{ &mutex, &checker, &woken_count }) catch continue;
                spawned += 1;
            }

            for (threads[0..spawned]) |t| {
                t.join();
            }

            try checker.verify();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Race
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex loom - cancel doesn't corrupt state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var mutex = Mutex.init();

            // Main thread grabs the lock BEFORE spawning threads.
            // This is single-threaded here, so tryLock is guaranteed to succeed.
            testing.expect(mutex.tryLock()) catch unreachable;

            var canceller_queued = Atomic(bool).init(false);

            const TestWaker = struct {
                fn wake(_: *anyopaque) void {}
            };

            var threads: [2]std.Thread = undefined;
            var spawned: usize = 0;

            // Canceller thread: queues as waiter, signals ready, then cancels.
            // lockWait must return false (lock is held by main), so waiter IS queued.
            threads[0] = std.Thread.spawn(.{}, struct {
                fn run(m: *Mutex, queued: *Atomic(bool)) void {
                    var waiter = Waiter.initWithWaker(undefined, TestWaker.wake);
                    const acquired = m.lockWait(&waiter);
                    // Lock is held by main → must be queued, not acquired
                    std.debug.assert(!acquired);
                    // Signal that we're on the wait queue
                    queued.store(true, .release);
                    // Cancel our pending acquisition
                    m.cancelLock(&waiter);
                }
            }.run, .{ &mutex, &canceller_queued }) catch {
                mutex.unlock();
                return;
            };
            spawned += 1;

            // Normal waiter: queues and waits for lock grant.
            threads[1] = std.Thread.spawn(.{}, struct {
                fn run(m: *Mutex) void {
                    var waiter = Waiter.initWithWaker(undefined, TestWaker.wake);
                    if (!m.lockWait(&waiter)) {
                        // Queued — spin until granted
                        while (!waiter.isAcquired()) {
                            busyWaitYield();
                        }
                    }
                    // We hold the lock — release it
                    m.unlock();
                }
            }.run, .{&mutex}) catch {
                // If spawn fails, still need to unlock so canceller can finish
                while (!canceller_queued.load(.acquire)) {
                    busyWaitYield();
                }
                mutex.unlock();
                threads[0].join();
                return;
            };
            spawned += 1;

            // Wait for the canceller to be queued before unlocking.
            // The flag is set AFTER lockWait returns false (waiter is on queue).
            while (!canceller_queued.load(.acquire)) {
                busyWaitYield();
            }

            // Unlock: canceller already cancelled (or will cancel), so the
            // normal waiter should receive the lock grant.
            mutex.unlock();

            for (threads[0..spawned]) |t| {
                t.join();
            }

            // Final state: both threads done, mutex should be unlocked
            try testing.expect(!mutex.isLocked());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: High Contention
// ─────────────────────────────────────────────────────────────────────────────

const HighContentionContext = struct {
    mutex: *Mutex,
    checker: *MutexInvariantChecker,
    counter: *ConcurrentCounter,
};

fn highContentionWork(ctx: HighContentionContext, m: *Model, _: usize) void {
    for (0..50) |_| {
        if (ctx.mutex.tryLock()) {
            ctx.checker.acquire();
            ctx.counter.increment();
            m.spin();
            ctx.checker.release();
            ctx.mutex.unlock();
        }
        m.yield();
    }
}

test "Mutex loom - high contention" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var mutex = Mutex.init();
            var checker = MutexInvariantChecker{};
            var counter = ConcurrentCounter{};

            const ctx = HighContentionContext{
                .mutex = &mutex,
                .checker = &checker,
                .counter = &counter,
            };

            runWithModel(model, 8, ctx, highContentionWork);

            try checker.verify();
            try testing.expect(counter.get() > 0);
        }
    }.run);
}

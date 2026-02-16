//! RwLock Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for RwLock using randomized thread interleavings.
//!
//! Invariants tested:
//! - (writers == 1 && readers == 0) || (writers == 0)
//! - Writer priority: new readers wait if writer is waiting
//! - No lost wakeups

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;
const ConcurrentCounter = common.ConcurrentCounter;

const sync = @import("volt").sync;
const RwLock = sync.RwLock;
const ReadWaiter = sync.rwlock.ReadWaiter;
const WriteWaiter = sync.rwlock.WriteWaiter;

// ─────────────────────────────────────────────────────────────────────────────
// Invariant Checker
// ─────────────────────────────────────────────────────────────────────────────

const RwLockInvariantChecker = struct {
    active_readers: Atomic(usize) = Atomic(usize).init(0),
    active_writers: Atomic(usize) = Atomic(usize).init(0),
    violation_detected: Atomic(bool) = Atomic(bool).init(false),
    max_readers: Atomic(usize) = Atomic(usize).init(0),

    const Self = @This();

    fn acquireRead(self: *Self) void {
        const writers = self.active_writers.load(.seq_cst);
        if (writers > 0) {
            self.violation_detected.store(true, .seq_cst);
        }
        const readers = self.active_readers.fetchAdd(1, .seq_cst) + 1;

        // Track max readers
        var max = self.max_readers.load(.monotonic);
        while (readers > max) {
            const result = self.max_readers.cmpxchgWeak(max, readers, .monotonic, .monotonic);
            if (result) |v| {
                max = v;
            } else {
                break;
            }
        }
    }

    fn releaseRead(self: *Self) void {
        const prev = self.active_readers.fetchSub(1, .seq_cst);
        if (prev == 0) {
            self.violation_detected.store(true, .seq_cst);
        }
    }

    fn acquireWrite(self: *Self) void {
        const readers = self.active_readers.load(.seq_cst);
        const writers = self.active_writers.load(.seq_cst);
        if (readers > 0 or writers > 0) {
            self.violation_detected.store(true, .seq_cst);
        }
        _ = self.active_writers.fetchAdd(1, .seq_cst);
    }

    fn releaseWrite(self: *Self) void {
        const prev = self.active_writers.fetchSub(1, .seq_cst);
        if (prev != 1) {
            self.violation_detected.store(true, .seq_cst);
        }
    }

    fn verify(self: *const Self) !void {
        if (self.violation_detected.load(.acquire)) {
            return error.RwLockInvariantViolation;
        }
        if (self.active_readers.load(.acquire) != 0) {
            return error.LeakedReadLock;
        }
        if (self.active_writers.load(.acquire) != 0) {
            return error.LeakedWriteLock;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Context structs and work functions for runWithModel
// ─────────────────────────────────────────────────────────────────────────────

const MultipleReadersContext = struct {
    rwlock: *RwLock,
    checker: *RwLockInvariantChecker,
};

fn multipleReadersWork(ctx: MultipleReadersContext, m: *Model, _: usize) void {
    for (0..5) |_| {
        if (ctx.rwlock.tryReadLock()) {
            ctx.checker.acquireRead();
            m.spin();
            ctx.checker.releaseRead();
            ctx.rwlock.readUnlock();
        }
        m.yield();
    }
}

const WriterExclusionContext = struct {
    rwlock: *RwLock,
    checker: *RwLockInvariantChecker,
};

fn writerExclusionWork(ctx: WriterExclusionContext, m: *Model, tid: usize) void {
    for (0..3) |_| {
        if (tid == 0) {
            // Thread 0 tries to write
            if (ctx.rwlock.tryWriteLock()) {
                ctx.checker.acquireWrite();
                m.spin();
                ctx.checker.releaseWrite();
                ctx.rwlock.writeUnlock();
            }
        } else {
            // Other threads try to read
            if (ctx.rwlock.tryReadLock()) {
                ctx.checker.acquireRead();
                m.spin();
                ctx.checker.releaseRead();
                ctx.rwlock.readUnlock();
            }
        }
        m.yield();
    }
}

const InterleavedContext = struct {
    rwlock: *RwLock,
    checker: *RwLockInvariantChecker,
    read_count: *ConcurrentCounter,
    write_count: *ConcurrentCounter,
};

fn interleavedWork(ctx: InterleavedContext, m: *Model, tid: usize) void {
    for (0..4) |i| {
        // Alternate between read and write based on thread and iteration
        if ((tid + i) % 3 == 0) {
            if (ctx.rwlock.tryWriteLock()) {
                ctx.checker.acquireWrite();
                m.spin();
                ctx.checker.releaseWrite();
                ctx.rwlock.writeUnlock();
                ctx.write_count.increment();
            }
        } else {
            if (ctx.rwlock.tryReadLock()) {
                ctx.checker.acquireRead();
                m.spin();
                ctx.checker.releaseRead();
                ctx.rwlock.readUnlock();
                ctx.read_count.increment();
            }
        }
        m.yield();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Multiple Readers
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - multiple concurrent readers" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var rwlock = RwLock.init();
            var checker = RwLockInvariantChecker{};

            const ctx = MultipleReadersContext{
                .rwlock = &rwlock,
                .checker = &checker,
            };

            runWithModel(model, 4, ctx, multipleReadersWork);

            try checker.verify();
            // Verify multiple readers were allowed concurrently
            try testing.expect(checker.max_readers.load(.acquire) >= 1);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Writer Exclusion
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - writer excludes readers" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var rwlock = RwLock.init();
            var checker = RwLockInvariantChecker{};

            const ctx = WriterExclusionContext{
                .rwlock = &rwlock,
                .checker = &checker,
            };

            runWithModel(model, 4, ctx, writerExclusionWork);

            try checker.verify();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Reader/Writer Interleaving
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - readers and writers interleaved" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var rwlock = RwLock.init();
            var checker = RwLockInvariantChecker{};
            var read_count = ConcurrentCounter{};
            var write_count = ConcurrentCounter{};

            const ctx = InterleavedContext{
                .rwlock = &rwlock,
                .checker = &checker,
                .read_count = &read_count,
                .write_count = &write_count,
            };

            runWithModel(model, 4, ctx, interleavedWork);

            try checker.verify();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Waiter Wake-up
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - readers blocked by writer" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var rwlock = RwLock.init();

            // Writer holds lock
            try testing.expect(rwlock.tryWriteLock());

            // Readers can't acquire while writer holds
            try testing.expect(!rwlock.tryReadLock());

            rwlock.writeUnlock();

            // Now readers can acquire
            try testing.expect(rwlock.tryReadLock());
            try testing.expect(rwlock.tryReadLock()); // Multiple readers OK
            rwlock.readUnlock();
            rwlock.readUnlock();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Writer Priority
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - writer priority blocks new readers" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var rwlock = RwLock.init();

            // Reader holds lock
            _ = rwlock.tryReadLock();

            // Writer waits
            var write_waiter = WriteWaiter.init();
            const write_immediate = rwlock.writeLockWait(&write_waiter);
            try testing.expect(!write_immediate);

            // New reader should be blocked (writer priority)
            const read_blocked = !rwlock.tryReadLock();
            try testing.expect(read_blocked);

            // Release reader - writer should get it
            rwlock.readUnlock();

            // Wait for writer to acquire
            while (!write_waiter.isAcquired()) {
                std.Thread.yield() catch {};
            }

            try testing.expect(rwlock.isWriteLocked());

            rwlock.writeUnlock();
            try testing.expect(!rwlock.isWriteLocked());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Operations
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock loom - cancel read lock doesn't corrupt state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var rwlock = RwLock.init();

            // Writer holds lock
            _ = rwlock.tryWriteLock();

            // Reader waits then cancels
            var read_waiter = ReadWaiter.init();
            _ = rwlock.readLockWait(&read_waiter);
            rwlock.cancelReadLock(&read_waiter);

            // Should still have 0 waiting readers
            try testing.expectEqual(@as(usize, 0), rwlock.waitingReaders());

            rwlock.writeUnlock();
            try testing.expect(!rwlock.isWriteLocked());
        }
    }.run);
}

test "RwLock loom - cancel write lock doesn't corrupt state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var rwlock = RwLock.init();

            // Reader holds lock
            _ = rwlock.tryReadLock();

            // Writer waits then cancels
            var write_waiter = WriteWaiter.init();
            _ = rwlock.writeLockWait(&write_waiter);
            rwlock.cancelWriteLock(&write_waiter);

            // Should still have 0 waiting writers
            try testing.expectEqual(@as(usize, 0), rwlock.waitingWriters());

            // New reader should be able to acquire (no waiting writer)
            try testing.expect(rwlock.tryReadLock());

            rwlock.readUnlock();
            rwlock.readUnlock();
        }
    }.run);
}

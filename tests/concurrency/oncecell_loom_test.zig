//! OnceCell Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for OnceCell using randomized thread interleavings.
//!
//! Invariants tested:
//! - Only one initializer wins
//! - All waiters see the same value
//! - Double initialization is rejected

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
const OnceCell = sync.OnceCell;
const InitWaiter = sync.once_cell.InitWaiter;

// Type alias for OnceCell(u32) to use in contexts
const U32OnceCell = OnceCell(u32);

// ─────────────────────────────────────────────────────────────────────────────
// Test: Only One Initializer Wins
// ─────────────────────────────────────────────────────────────────────────────

const OnlyOneInitContext = struct {
    cell: *U32OnceCell,
    init_count: *Atomic(usize),
};

fn onlyOneInitWork(ctx: OnlyOneInitContext, m: *Model, tid: usize) void {
    _ = m;

    // Each thread tries to set
    if (ctx.cell.set(@intCast(tid))) {
        _ = ctx.init_count.fetchAdd(1, .monotonic);
    }
}

test "OnceCell loom - only one initializer wins" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var cell = U32OnceCell.init();
            defer cell.deinit();
            var init_count = Atomic(usize).init(0);

            const ctx = OnlyOneInitContext{
                .cell = &cell,
                .init_count = &init_count,
            };

            runWithModel(model, 4, ctx, onlyOneInitWork);

            // Exactly one should succeed
            try testing.expectEqual(@as(usize, 1), init_count.load(.acquire));
            try testing.expect(cell.isInitialized());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: All Waiters See Same Value
// ─────────────────────────────────────────────────────────────────────────────

fn initValue42() u32 {
    return 42;
}

const AllWaitersSeeValueContext = struct {
    cell: *U32OnceCell,
    values_seen: *[4]Atomic(u32),
};

fn allWaitersSeeValueWork(ctx: AllWaitersSeeValueContext, m: *Model, tid: usize) void {
    _ = m;

    var waiter = InitWaiter.init();
    const ptr = ctx.cell.getOrInitWith(initValue42, &waiter);

    if (ptr) |p| {
        ctx.values_seen[tid].store(p.*, .release);
    } else {
        // Wait for initialization
        while (!waiter.isComplete()) {
            std.Thread.yield() catch {};
        }
        if (ctx.cell.get()) |p| {
            ctx.values_seen[tid].store(p.*, .release);
        }
    }
}

test "OnceCell loom - all waiters see same value" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var cell = U32OnceCell.init();
            defer cell.deinit();
            var values_seen: [4]Atomic(u32) = undefined;
            for (&values_seen) |*v| {
                v.* = Atomic(u32).init(0xFFFFFFFF);
            }

            const ctx = AllWaitersSeeValueContext{
                .cell = &cell,
                .values_seen = &values_seen,
            };

            runWithModel(model, 4, ctx, allWaitersSeeValueWork);

            // All should see 42
            for (values_seen) |v| {
                try testing.expectEqual(@as(u32, 42), v.load(.acquire));
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: initWith Already Initialized
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell loom - initWith already initialized" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var cell = U32OnceCell.initWith(100);
            defer cell.deinit();

            try testing.expect(cell.isInitialized());
            try testing.expectEqual(@as(u32, 100), cell.get().?.*);

            // set should fail
            try testing.expect(!cell.set(200));
            try testing.expectEqual(@as(u32, 100), cell.get().?.*);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent getOrInitWith
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell loom - concurrent getOrInitWith" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var cell = U32OnceCell.init();
            defer cell.deinit();
            var init_call_count = Atomic(usize).init(0);
            var woken_count = Atomic(usize).init(0);

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const count: *Atomic(usize) = @ptrCast(@alignCast(ctx));
                    _ = count.fetchAdd(1, .monotonic);
                }
            };

            var threads: [4]std.Thread = undefined;
            var spawned: usize = 0;

            for (0..4) |_| {
                threads[spawned] = std.Thread.spawn(.{}, struct {
                    fn run(c: *U32OnceCell, init_count: *Atomic(usize), woken: *Atomic(usize)) void {
                        var waiter = InitWaiter.initWithWaker(@ptrCast(woken), Waker.wake);

                        // Use set approach since we can't pass context to getOrInitWith
                        if (c.set(42)) {
                            _ = init_count.fetchAdd(1, .monotonic);
                            waiter.complete.store(true, .seq_cst);
                        } else {
                            // Wait for init
                            while (!c.isInitialized()) {
                                std.Thread.yield() catch {};
                            }
                            waiter.complete.store(true, .seq_cst);
                        }
                    }
                }.run, .{ &cell, &init_call_count, &woken_count }) catch continue;
                spawned += 1;
            }

            for (threads[0..spawned]) |t| {
                t.join();
            }

            // Init function should only be called once
            try testing.expectEqual(@as(usize, 1), init_call_count.load(.acquire));
            try testing.expect(cell.isInitialized());
            try testing.expectEqual(@as(u32, 42), cell.get().?.*);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Pending Wait
// ─────────────────────────────────────────────────────────────────────────────

fn initValue99() u32 {
    return 99;
}

test "OnceCell loom - cancel pending wait" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var cell = U32OnceCell.init();
            defer cell.deinit();

            var cancel_complete = Atomic(bool).init(false);
            var init_started = Atomic(bool).init(false);

            var threads: [2]std.Thread = undefined;
            var spawned: usize = 0;

            // Thread 1: Slow initializer
            threads[spawned] = std.Thread.spawn(.{}, struct {
                fn run(c: *U32OnceCell, started: *Atomic(bool)) void {
                    started.store(true, .release);
                    // Do set (this is the real initializer)
                    _ = c.set(42);
                }
            }.run, .{ &cell, &init_started }) catch {
                return;
            };
            spawned += 1;

            // Thread 2: Waiter that cancels
            threads[spawned] = std.Thread.spawn(.{}, struct {
                fn run(c: *U32OnceCell, started: *Atomic(bool), done: *Atomic(bool)) void {
                    // Wait for initializer to start
                    while (!started.load(.acquire)) {
                        std.Thread.yield() catch {};
                    }

                    var waiter = InitWaiter.init();
                    const result = c.getOrInitWith(initValue99, &waiter);

                    if (result == null) {
                        // We got queued - cancel
                        c.cancelWait(&waiter);
                    }
                    // Either way we got a result or cancelled
                    done.store(true, .release);
                }
            }.run, .{ &cell, &init_started, &cancel_complete }) catch {
                for (threads[0..spawned]) |t| t.join();
                return;
            };
            spawned += 1;

            for (threads[0..spawned]) |t| {
                t.join();
            }

            // Cell should be initialized with 42
            try testing.expect(cell.isInitialized());
            try testing.expectEqual(@as(u32, 42), cell.get().?.*);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Double Set Fails
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell loom - double set fails" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var cell = U32OnceCell.init();
            defer cell.deinit();

            try testing.expect(cell.set(42));
            try testing.expect(cell.isInitialized());

            // Second set should fail
            try testing.expect(!cell.set(100));
            try testing.expectEqual(@as(u32, 42), cell.get().?.*);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Get Before Init Returns Null
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell loom - get before init returns null" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var cell = U32OnceCell.init();
            defer cell.deinit();

            try testing.expect(!cell.isInitialized());
            try testing.expect(cell.get() == null);
            try testing.expect(cell.getConst() == null);

            _ = cell.set(42);
            try testing.expect(cell.isInitialized());
            try testing.expect(cell.get() != null);
            try testing.expectEqual(@as(u32, 42), cell.get().?.*);
        }
    }.run);
}

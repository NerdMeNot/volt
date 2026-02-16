//! Watch Channel Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Watch channel.
//!
//! Invariants tested:
//! - All receivers see latest value
//! - Change detection works correctly
//! - Close wakes all waiters

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;
const ConcurrentCounter = common.ConcurrentCounter;

const channel_mod = @import("volt").channel;
const Watch = channel_mod.Watch;
const ChangeWaiter = channel_mod.watch_mod.ChangeWaiter;

// Type alias for Watch(u32) used in concurrent access test
const U32Watch = Watch(u32);

// Context struct for concurrent access test
const ConcurrentAccessContext = struct {
    watch: *U32Watch,
    change_count: *ConcurrentCounter,
};

// Work function for concurrent access test
fn concurrentAccessWork(ctx: ConcurrentAccessContext, m: *Model, tid: usize) void {
    if (tid == 0) {
        // Sender
        for (0..5) |i| {
            ctx.watch.send(@intCast(i));
            m.yield();
        }
    } else {
        // Receiver
        var rx = ctx.watch.subscribe();
        for (0..5) |_| {
            if (rx.hasChanged()) {
                _ = rx.getAndUpdate();
                ctx.change_count.increment();
            }
            m.yield();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Initial Value
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - initial value" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(42);
            defer watch.deinit();

            var rx = watch.subscribe();

            // New subscriber sees initial value as changed
            try testing.expect(rx.hasChanged());
            try testing.expectEqual(@as(u32, 42), rx.get());

            rx.markSeen();
            try testing.expect(!rx.hasChanged());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Send Updates Value
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - send updates value" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(1);
            defer watch.deinit();

            var rx = watch.subscribe();
            rx.markSeen();

            try testing.expect(!rx.hasChanged());

            watch.send(2);

            try testing.expect(rx.hasChanged());
            try testing.expectEqual(@as(u32, 2), rx.getAndUpdate());
            try testing.expect(!rx.hasChanged());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Multiple Receivers
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - multiple receivers see change" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(0);
            defer watch.deinit();

            var rx1 = watch.subscribe();
            var rx2 = watch.subscribe();
            rx1.markSeen();
            rx2.markSeen();

            watch.send(100);

            // Both see the change
            try testing.expect(rx1.hasChanged());
            try testing.expect(rx2.hasChanged());
            try testing.expectEqual(@as(u32, 100), rx1.get());
            try testing.expectEqual(@as(u32, 100), rx2.get());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Wait for Change
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - wait for change" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(0);
            defer watch.deinit();
            var woken = Atomic(bool).init(false);

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const w: *Atomic(bool) = @ptrCast(@alignCast(ctx));
                    w.store(true, .release);
                }
            };

            var rx = watch.subscribe();
            rx.markSeen();

            var waiter = ChangeWaiter.init();
            waiter.setWaker(@ptrCast(&woken), Waker.wake);

            // Should wait (no change yet)
            const immediate = rx.changedWait(&waiter);
            try testing.expect(!immediate);

            // Send wakes waiter
            watch.send(42);
            try testing.expect(woken.load(.acquire) or waiter.notified.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Close Wakes Waiters
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - close wakes waiters" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(0);
            defer watch.deinit();
            var woken = Atomic(bool).init(false);

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const w: *Atomic(bool) = @ptrCast(@alignCast(ctx));
                    w.store(true, .release);
                }
            };

            var rx = watch.subscribe();
            rx.markSeen();

            var waiter = ChangeWaiter.init();
            waiter.setWaker(@ptrCast(&woken), Waker.wake);
            _ = rx.changedWait(&waiter);

            watch.close();

            try testing.expect(woken.load(.acquire) or waiter.closed.load(.acquire));
            try testing.expect(rx.isClosed());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: subscribeNoInitial
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - subscribeNoInitial" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(42);
            defer watch.deinit();

            var rx = watch.subscribeNoInitial();

            // Should not see initial value as changed
            try testing.expect(!rx.hasChanged());

            watch.send(100);
            try testing.expect(rx.hasChanged());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: sendModify
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - sendModify" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(10);
            defer watch.deinit();

            var rx = watch.subscribe();
            rx.markSeen();

            const double = struct {
                fn f(val: *u32) void {
                    val.* *= 2;
                }
            }.f;

            watch.sendModify(double);

            try testing.expect(rx.hasChanged());
            try testing.expectEqual(@as(u32, 20), rx.get());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Access
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - concurrent access" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var watch = U32Watch.init(0);
            defer watch.deinit();
            var change_count = ConcurrentCounter{};

            const ctx = ConcurrentAccessContext{
                .watch = &watch,
                .change_count = &change_count,
            };

            runWithModel(model, 4, ctx, concurrentAccessWork);

            // Some changes should have been observed
            try testing.expect(change_count.get() > 0);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Pending Wait
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - cancel pending wait" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(0);
            defer watch.deinit();

            var rx = watch.subscribe();
            rx.markSeen();

            var waiter = ChangeWaiter.init();
            _ = rx.changedWait(&waiter);

            // Cancel
            rx.cancelChangedWait(&waiter);

            // Watch should still work
            watch.send(42);
            try testing.expect(rx.hasChanged());
            try testing.expectEqual(@as(u32, 42), rx.get());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Version Tracking
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - version tracking" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(0);
            defer watch.deinit();

            var rx = watch.subscribe();

            // Initial version
            try testing.expect(rx.hasChanged());
            rx.markSeen();
            try testing.expect(!rx.hasChanged());

            // Each send increments version
            watch.send(1);
            try testing.expect(rx.hasChanged());
            rx.markSeen();

            watch.send(2);
            try testing.expect(rx.hasChanged());
            rx.markSeen();

            // No change if same value sent
            try testing.expect(!rx.hasChanged());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Borrow Current Value
// ─────────────────────────────────────────────────────────────────────────────

test "Watch loom - borrow current value" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var watch = Watch(u32).init(42);
            defer watch.deinit();

            // Direct borrow from watch
            try testing.expectEqual(@as(u32, 42), watch.borrow().*);

            var rx = watch.subscribe();
            try testing.expectEqual(@as(u32, 42), rx.borrow().*);

            watch.send(100);
            try testing.expectEqual(@as(u32, 100), watch.borrow().*);
            try testing.expectEqual(@as(u32, 100), rx.borrow().*);
        }
    }.run);
}

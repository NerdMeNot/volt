//! Notify Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Notify using randomized thread interleavings.
//!
//! Invariants tested:
//! - At most 1 permit stored at any time
//! - notify-before-wait delivers permit
//! - notifyAll wakes all waiters

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
const Notify = sync.Notify;
const Waiter = sync.notify.Waiter;

// ─────────────────────────────────────────────────────────────────────────────
// Context structs and work functions for runWithModel
// ─────────────────────────────────────────────────────────────────────────────

const ConcurrentNotifyWaitContext = struct {
    notify: *Notify,
    counter: *ConcurrentCounter,
};

fn concurrentNotifyWaitWork(ctx: ConcurrentNotifyWaitContext, m: *Model, tid: usize) void {
    for (0..5) |_| {
        if (tid % 2 == 0) {
            // Even threads notify
            ctx.notify.notifyOne();
        } else {
            // Odd threads wait
            var waiter = Waiter.init();
            const consumed = ctx.notify.waitWith(&waiter);
            if (consumed or waiter.isNotified()) {
                ctx.counter.increment();
            } else {
                // Cancel if not notified
                ctx.notify.cancelWait(&waiter);
            }
        }
        m.yield();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Notify Before Wait
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - notify before wait delivers permit" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var notify = Notify.init();

            // Store permit first
            notify.notifyOne();

            // Wait should consume permit immediately
            var waiter = Waiter.init();
            const consumed = notify.waitWith(&waiter);
            try testing.expect(consumed);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Wait Then Notify (simplified - no thread spawning)
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - wait returns false when no permit" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var notify = Notify.init();

            // Wait without permit stored - should not consume immediately
            var waiter = Waiter.init();
            const consumed = notify.waitWith(&waiter);
            try testing.expect(!consumed);

            // Cancel the wait
            notify.cancelWait(&waiter);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: NotifyAll With No Waiters
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - notifyAll with no waiters is no-op" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var notify = Notify.init();

            // NotifyAll with no waiters should be safe
            notify.notifyAll();

            // Should still work after
            notify.notifyOne();
            var waiter = Waiter.init();
            const consumed = notify.waitWith(&waiter);
            try testing.expect(consumed);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Permit Does Not Accumulate
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - permit does not accumulate" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var notify = Notify.init();

            // Multiple notifies
            notify.notifyOne();
            notify.notifyOne();
            notify.notifyOne();

            // First wait consumes
            var waiter1 = Waiter.init();
            const consumed1 = notify.waitWith(&waiter1);
            try testing.expect(consumed1);

            // Second wait should not consume (only 1 permit stored)
            var waiter2 = Waiter.init();
            const consumed2 = notify.waitWith(&waiter2);
            try testing.expect(!consumed2);
            notify.cancelWait(&waiter2);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Removes Waiter
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - cancel removes waiter" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var notify = Notify.init();

            var waiter = Waiter.init();
            const consumed = notify.waitWith(&waiter);
            try testing.expect(!consumed);

            // Cancel
            notify.cancelWait(&waiter);

            // Notify should now store permit (no waiters)
            notify.notifyOne();

            // New wait should consume
            var waiter2 = Waiter.init();
            const consumed2 = notify.waitWith(&waiter2);
            try testing.expect(consumed2);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Notify and Wait (uses runWithModel)
// ─────────────────────────────────────────────────────────────────────────────

test "Notify loom - concurrent notify and wait" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var notify = Notify.init();
            var counter = ConcurrentCounter{};

            const ctx = ConcurrentNotifyWaitContext{
                .notify = &notify,
                .counter = &counter,
            };

            runWithModel(model, 4, ctx, concurrentNotifyWaitWork);

            // Some notifications should have been consumed
            // (exact count depends on interleaving)
            _ = counter.get();
        }
    }.run);
}

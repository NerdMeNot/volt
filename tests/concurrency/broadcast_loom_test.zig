//! Broadcast Channel Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for BroadcastChannel.
//!
//! Invariants tested:
//! - All receivers get every message (unless lagged)
//! - Lag detection works correctly
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
const BroadcastChannel = channel_mod.BroadcastChannel;
const RecvWaiter = channel_mod.broadcast_mod.RecvWaiter;

// Type alias for the broadcast channel used in tests
const U32Broadcast = BroadcastChannel(u32);

// ─────────────────────────────────────────────────────────────────────────────
// Context and work function for concurrent senders and receivers test
// ─────────────────────────────────────────────────────────────────────────────

const ConcurrentSendRecvContext = struct {
    channel: *U32Broadcast,
    receivers: *[2]U32Broadcast.Receiver,
    received_total: *ConcurrentCounter,
};

fn concurrentSendRecvWork(ctx: ConcurrentSendRecvContext, m: *Model, tid: usize) void {
    if (tid < 2) {
        // Sender
        for (0..4) |i| {
            _ = ctx.channel.send(@intCast(tid * 10 + i));
            m.yield();
        }
    } else {
        // Receiver
        const rx_idx = tid - 2;
        var rx = &ctx.receivers[rx_idx];
        for (0..10) |_| {
            const result = rx.tryRecv();
            switch (result) {
                .value => ctx.received_total.increment(),
                .lagged => {},
                else => {},
            }
            m.yield();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Single Receiver
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - single receiver" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            var rx = ch.subscribe();

            _ = ch.send(1);
            _ = ch.send(2);

            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 1 }, rx.tryRecv());
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 2 }, rx.tryRecv());
            try testing.expect(rx.tryRecv() == .empty);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Multiple Receivers
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - multiple receivers get same value" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            var rx1 = ch.subscribe();
            var rx2 = ch.subscribe();
            var rx3 = ch.subscribe();

            _ = ch.send(42);

            // All receivers get the same value
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 42 }, rx1.tryRecv());
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 42 }, rx2.tryRecv());
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 42 }, rx3.tryRecv());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Lagging Receiver
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - lagging receiver detects lag" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 2);
            defer ch.deinit();

            var rx = ch.subscribe();

            // Fill and overflow buffer
            _ = ch.send(1);
            _ = ch.send(2);
            _ = ch.send(3); // Overwrites slot 0
            _ = ch.send(4); // Overwrites slot 1

            // Receiver should detect lag
            const result = rx.tryRecv();
            switch (result) {
                .lagged => |missed| try testing.expect(missed >= 2),
                else => try testing.expect(false),
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: New Subscriber Starts at Current
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - new subscriber starts at current" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            _ = ch.send(1);
            _ = ch.send(2);

            // New subscriber doesn't see old messages
            var rx = ch.subscribe();
            try testing.expect(rx.tryRecv() == .empty);

            // But sees new ones
            _ = ch.send(3);
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 3 }, rx.tryRecv());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Close Wakes Waiters
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - close wakes waiters" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();
            var woken = Atomic(bool).init(false);

            const Waker = struct {
                fn wake(ctx: *anyopaque) void {
                    const w: *Atomic(bool) = @ptrCast(@alignCast(ctx));
                    w.store(true, .release);
                }
            };

            var rx = ch.subscribe();
            var waiter = RecvWaiter.init();
            waiter.setWaker(@ptrCast(&woken), Waker.wake);

            _ = rx.recvWait(&waiter);

            ch.close();

            try testing.expect(woken.load(.acquire) or waiter.closed.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Send Returns Receiver Count
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - send returns receiver count" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            _ = ch.subscribe();
            _ = ch.subscribe();

            const result = ch.send(42);
            switch (result) {
                .ok => |count| try testing.expectEqual(@as(usize, 2), count),
                .closed => try testing.expect(false),
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Senders and Receivers
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - concurrent senders and receivers" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var ch = try U32Broadcast.init(testing.allocator, 16);
            defer ch.deinit();
            var received_total = ConcurrentCounter{};

            // Subscribe before concurrent access
            var receivers: [2]U32Broadcast.Receiver = undefined;
            for (&receivers) |*rx| {
                rx.* = ch.subscribe();
            }

            const ctx = ConcurrentSendRecvContext{
                .channel = &ch,
                .receivers = &receivers,
                .received_total = &received_total,
            };

            runWithModel(model, 4, ctx, concurrentSendRecvWork);

            // Drain any remaining
            for (&receivers) |*rx| {
                while (true) {
                    const result = rx.tryRecv();
                    switch (result) {
                        .value => received_total.increment(),
                        .lagged => {},
                        else => break,
                    }
                }
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Pending Receive
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - cancel pending receive" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            var rx = ch.subscribe();
            var waiter = RecvWaiter.init();

            // Start waiting
            _ = rx.recvWait(&waiter);

            // Cancel
            rx.cancelRecv(&waiter);

            // Channel should still work
            _ = ch.send(42);
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 42 }, rx.tryRecv());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Closed Channel Behavior
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast loom - closed channel behavior" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Broadcast.init(testing.allocator, 4);
            defer ch.deinit();

            var rx = ch.subscribe();

            _ = ch.send(1);
            ch.close();

            // Can still receive pending
            try testing.expectEqual(U32Broadcast.Receiver.RecvResult{ .value = 1 }, rx.tryRecv());

            // Then closed
            try testing.expect(rx.tryRecv() == .closed);

            // Send returns closed
            const result = ch.send(2);
            try testing.expect(result == .closed);
        }
    }.run);
}

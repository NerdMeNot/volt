//! Channel Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Channel using randomized thread interleavings.
//!
//! Invariants tested:
//! - received <= sent (at any point)
//! - received == sent (at end, if not closed)
//! - FIFO ordering preserved
//! - No lost messages

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
const Channel = channel_mod.Channel;
const SendWaiter = channel_mod.channel_mod.SendWaiter;
const RecvWaiter = channel_mod.channel_mod.RecvWaiter;

// ─────────────────────────────────────────────────────────────────────────────
// Type Aliases
// ─────────────────────────────────────────────────────────────────────────────

const U32Channel = Channel(u32);

// ─────────────────────────────────────────────────────────────────────────────
// Invariant Checker
// ─────────────────────────────────────────────────────────────────────────────

const ChannelInvariantChecker = struct {
    sent: Atomic(usize) = Atomic(usize).init(0),
    received: Atomic(usize) = Atomic(usize).init(0),
    violation: Atomic(bool) = Atomic(bool).init(false),

    const Self = @This();

    fn recordSend(self: *Self) void {
        _ = self.sent.fetchAdd(1, .seq_cst);
    }

    fn recordRecv(self: *Self) void {
        const recv = self.received.fetchAdd(1, .seq_cst) + 1;
        const sent = self.sent.load(.seq_cst);
        if (recv > sent) {
            self.violation.store(true, .seq_cst);
        }
    }

    fn verify(self: *const Self) !void {
        if (self.violation.load(.acquire)) {
            return error.ReceivedMoreThanSent;
        }
    }

    fn verifyClosed(self: *const Self) !void {
        try self.verify();
        // When closed, some messages might be lost if senders got .closed
        // So we just verify received <= sent
    }

    fn verifyComplete(self: *const Self) !void {
        try self.verify();
        const sent = self.sent.load(.acquire);
        const received = self.received.load(.acquire);
        if (sent != received) {
            return error.MessageLost;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Context Types and Work Functions for runWithModel
// ─────────────────────────────────────────────────────────────────────────────

// Context for concurrent senders test
const ConcurrentSendersContext = struct {
    channel: *U32Channel,
    checker: *ChannelInvariantChecker,
};

fn concurrentSendersWork(ctx: ConcurrentSendersContext, m: *Model, tid: usize) void {
    for (0..4) |i| {
        const value: u32 = @intCast(tid * 100 + i);
        if (ctx.channel.trySend(value) == .ok) {
            ctx.checker.recordSend();
        }
        m.yield();
    }
}

// Context for producer/consumer test
const ProducerConsumerContext = struct {
    channel: *U32Channel,
    produced: *Atomic(usize),
    consumed: *Atomic(usize),
};

fn producerConsumerWork(ctx: ProducerConsumerContext, m: *Model, tid: usize) void {
    if (tid % 2 == 0) {
        // Producer
        for (0..5) |_| {
            if (ctx.channel.trySend(@intCast(tid)) == .ok) {
                _ = ctx.produced.fetchAdd(1, .monotonic);
            }
            m.yield();
        }
    } else {
        // Consumer
        for (0..10) |_| {
            if (ctx.channel.tryRecv() == .value) {
                _ = ctx.consumed.fetchAdd(1, .monotonic);
            }
            m.yield();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Basic Send/Recv
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - basic send and receive" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 4);
            defer ch.deinit();

            // Send values
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(1));
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(2));
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(3));

            // Receive values
            try testing.expectEqual(U32Channel.RecvResult{ .value = 1 }, ch.tryRecv());
            try testing.expectEqual(U32Channel.RecvResult{ .value = 2 }, ch.tryRecv());
            try testing.expectEqual(U32Channel.RecvResult{ .value = 3 }, ch.tryRecv());
            try testing.expect(ch.tryRecv() == .empty);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: FIFO Ordering
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - FIFO ordering preserved" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 16);
            defer ch.deinit();

            // Send sequence
            for (0..10) |i| {
                try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(@intCast(i)));
            }

            // Receive and verify order
            for (0..10) |i| {
                const result = ch.tryRecv();
                try testing.expectEqual(U32Channel.RecvResult{ .value = @intCast(i) }, result);
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Senders
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - concurrent senders" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var ch = try U32Channel.init(testing.allocator, 16);
            defer ch.deinit();
            var checker = ChannelInvariantChecker{};

            const ctx = ConcurrentSendersContext{
                .channel = &ch,
                .checker = &checker,
            };

            runWithModel(model, 4, ctx, concurrentSendersWork);

            // Drain channel
            while (true) {
                const result = ch.tryRecv();
                switch (result) {
                    .value => checker.recordRecv(),
                    .empty => break,
                    .closed => break,
                }
            }

            try checker.verifyComplete();
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Producer/Consumer Pattern
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - producer/consumer" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var ch = try U32Channel.init(testing.allocator, 8);
            defer ch.deinit();
            var produced = Atomic(usize).init(0);
            var consumed = Atomic(usize).init(0);

            const ctx = ProducerConsumerContext{
                .channel = &ch,
                .produced = &produced,
                .consumed = &consumed,
            };

            runWithModel(model, 4, ctx, producerConsumerWork);

            // Drain remaining
            while (ch.tryRecv() == .value) {
                _ = consumed.fetchAdd(1, .monotonic);
            }

            // Consumed should equal produced
            try testing.expectEqual(produced.load(.acquire), consumed.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Close Wakes Waiters
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - close allows recv to complete" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 2);
            defer ch.deinit();

            // Send a value first
            _ = ch.trySend(42);

            // Close
            ch.close();

            // Should be able to recv the pending value
            const result = ch.tryRecv();
            try testing.expectEqual(U32Channel.RecvResult{ .value = 42 }, result);

            // Next recv should get closed
            try testing.expect(ch.tryRecv() == .closed);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Backpressure
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - backpressure when full" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 2);
            defer ch.deinit();

            // Fill channel
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(1));
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(2));
            try testing.expect(ch.isFull());

            // Next send should fail
            try testing.expectEqual(U32Channel.SendResult.full, ch.trySend(3));

            // Receive one
            try testing.expectEqual(U32Channel.RecvResult{ .value = 1 }, ch.tryRecv());

            // Now can send again
            try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(3));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Operations
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - cancel send doesn't corrupt state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 1);
            defer ch.deinit();

            // Fill channel
            _ = ch.trySend(1);

            // Send waiter
            var waiter = SendWaiter.init();
            try testing.expect(!ch.sendWait(2, &waiter));

            // Cancel
            ch.cancelSend(&waiter);

            // Channel state should be consistent
            try testing.expect(ch.isFull());
            try testing.expectEqual(U32Channel.RecvResult{ .value = 1 }, ch.tryRecv());
        }
    }.run);
}

test "Channel loom - cancel recv doesn't corrupt state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 2);
            defer ch.deinit();

            // Empty channel
            var waiter = RecvWaiter.init();
            try testing.expect(ch.recvWait(&waiter) == null);

            // Cancel
            ch.cancelRecv(&waiter);

            // Channel should still work
            _ = ch.trySend(42);
            try testing.expectEqual(U32Channel.RecvResult{ .value = 42 }, ch.tryRecv());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Closed Channel Behavior
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - closed channel behavior" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 4);
            defer ch.deinit();

            // Send some values
            _ = ch.trySend(1);
            _ = ch.trySend(2);

            // Close
            ch.close();
            try testing.expect(ch.isClosed());

            // Can still receive pending values
            try testing.expectEqual(U32Channel.RecvResult{ .value = 1 }, ch.tryRecv());
            try testing.expectEqual(U32Channel.RecvResult{ .value = 2 }, ch.tryRecv());

            // After draining, get closed
            try testing.expect(ch.tryRecv() == .closed);

            // Send returns closed
            try testing.expectEqual(U32Channel.SendResult.closed, ch.trySend(3));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Ring Buffer Wrap-Around
// ─────────────────────────────────────────────────────────────────────────────

test "Channel loom - ring buffer wrap-around" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            var ch = try U32Channel.init(testing.allocator, 4);
            defer ch.deinit();

            // Multiple fill/drain cycles
            for (0..5) |cycle| {
                // Fill
                for (0..4) |i| {
                    const value: u32 = @intCast(cycle * 4 + i);
                    try testing.expectEqual(U32Channel.SendResult.ok, ch.trySend(value));
                }

                // Drain
                for (0..4) |i| {
                    const expected: u32 = @intCast(cycle * 4 + i);
                    const result = ch.tryRecv();
                    try testing.expectEqual(U32Channel.RecvResult{ .value = expected }, result);
                }
            }
        }
    }.run);
}

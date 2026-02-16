//! Oneshot Loom Tests - Concurrency Testing
//!
//! Systematic concurrency testing for Oneshot channel.
//!
//! Invariants tested:
//! - Exactly one value can be sent
//! - Send-before-recv delivers value
//! - Recv-before-send waits and gets value
//! - Close wakes waiting receiver

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;
const loomRun = common.loomRun;
const loomQuick = common.loomQuick;
const runWithModel = common.runWithModel;

const channel_mod = @import("volt").channel;
const Oneshot = channel_mod.Oneshot;
const OneshotState = channel_mod.oneshot_mod.State;

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for concrete Oneshot types
// ─────────────────────────────────────────────────────────────────────────────

const U32Oneshot = Oneshot(u32);

// ─────────────────────────────────────────────────────────────────────────────
// Context and work function for concurrent send/receive test
// ─────────────────────────────────────────────────────────────────────────────

const ConcurrentSendRecvContext = struct {
    sender: *U32Oneshot.Sender,
    receiver: *U32Oneshot.Receiver,
    shared: *U32Oneshot.Shared,
    receiver_value: *Atomic(u32),
};

fn concurrentSendRecvWork(ctx: ConcurrentSendRecvContext, m: *Model, tid: usize) void {
    _ = m;

    if (tid == 0) {
        // Sender
        _ = ctx.sender.send(42);
    } else {
        // Receiver
        var waiter = U32Oneshot.RecvWaiter.init();
        const immediate = ctx.receiver.recvWait(&waiter);
        if (immediate) {
            if (waiter.value) |v| {
                ctx.receiver_value.store(v, .release);
            }
        } else {
            while (!waiter.isComplete()) {
                std.Thread.yield() catch {};
            }
            if (ctx.shared.value) |v| {
                ctx.receiver_value.store(v, .release);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Send Then Receive
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - send then receive" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            // Send value
            try testing.expect(pair.sender.send(42));

            // Receive should get it immediately
            const value = pair.receiver.tryRecv();
            try testing.expectEqual(@as(?u32, 42), value);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Receive Then Send
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - tryRecv before send returns null" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            // tryRecv before send returns null
            try testing.expectEqual(@as(?u32, null), pair.receiver.tryRecv());

            // Send value
            try testing.expect(pair.sender.send(42));

            // Now tryRecv returns the value
            try testing.expectEqual(@as(?u32, 42), pair.receiver.tryRecv());
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Sender Close Wakes Receiver
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - sender close sets closed state" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            // Close without sending
            pair.sender.close();

            // tryRecvResult should return closed
            const result = pair.receiver.tryRecvResult();
            try testing.expect(result == .closed);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Double Send Fails
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - double send fails" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            try testing.expect(pair.sender.send(1));
            try testing.expect(!pair.sender.send(2)); // Should fail
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: tryRecvResult
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - tryRecvResult states" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);

            // Test empty
            {
                var shared = Ch.Shared.init();
                var pair = Ch.fromShared(&shared);
                const result = pair.receiver.tryRecvResult();
                try testing.expect(result == .empty);
            }

            // Test value
            {
                var shared = Ch.Shared.init();
                var pair = Ch.fromShared(&shared);
                _ = pair.sender.send(42);
                const result = pair.receiver.tryRecvResult();
                try testing.expectEqual(Ch.Receiver.TryRecvResult{ .value = 42 }, result);
            }

            // Test closed
            {
                var shared = Ch.Shared.init();
                var pair = Ch.fromShared(&shared);
                pair.sender.close();
                const result = pair.receiver.tryRecvResult();
                try testing.expect(result == .closed);
            }
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Send and Receive
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - concurrent send and receive" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            var shared = U32Oneshot.Shared.init();
            var pair = U32Oneshot.fromShared(&shared);
            var receiver_value = Atomic(u32).init(0xFFFFFFFF);

            const ctx = ConcurrentSendRecvContext{
                .sender = &pair.sender,
                .receiver = &pair.receiver,
                .shared = &shared,
                .receiver_value = &receiver_value,
            };

            runWithModel(model, 2, ctx, concurrentSendRecvWork);

            // Receiver should have gotten value
            try testing.expectEqual(@as(u32, 42), receiver_value.load(.acquire));
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Cancel Receive
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - cancel receive" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            // Start waiting
            var waiter = Ch.RecvWaiter.init();
            const immediate = pair.receiver.recvWait(&waiter);
            try testing.expect(!immediate); // Should wait

            // Cancel
            pair.receiver.cancelRecv();

            // Should be back to empty state
            const state: OneshotState =
                @enumFromInt(shared.state.load(.acquire));
            try testing.expectEqual(@as(OneshotState, .empty), state);
        }
    }.run);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Receiver Close
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot loom - receiver close" {
    try loomQuick(struct {
        fn run(model: *Model) !void {
            _ = model;
            const Ch = Oneshot(u32);
            var shared = Ch.Shared.init();
            var pair = Ch.fromShared(&shared);

            // Receiver closes before sender sends
            pair.receiver.close();

            // Sender should see receiver is gone
            try testing.expect(!pair.sender.isReceiverAlive());
        }
    }.run);
}

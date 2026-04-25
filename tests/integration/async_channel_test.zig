//! Async MPMC Channel Integration Tests
//!
//! Exercises SendFuture / RecvFuture through the real scheduler.
//! Catches the MPMC spurious wake hang where a consumer stolen value
//! caused orphaned tasks, and the SendFuture MPMC correctness bug
//! that silently dropped values.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Runtime = volt.Runtime;
const Channel = volt.channel.Channel;
const SendFuture = volt.channel.channel_mod.SendFuture;
const RecvFuture = volt.channel.channel_mod.RecvFuture;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Future state machines
// ─────────────────────────────────────────────────────────────────────────────

fn MpmcSendFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;
        const COOP_BUDGET: usize = 128;

        channel: *Channel(T),
        msgs_remaining: usize,
        next_value: T,
        send_future: ?SendFuture(T) = null,

        pub fn init(ch: *Channel(T), msgs_count: usize, start_value: T) Self {
            return .{ .channel = ch, .msgs_remaining = msgs_count, .next_value = start_value };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            if (self.send_future != null) {
                switch (self.send_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => {
                        self.send_future = null;
                        self.next_value += 1;
                        self.msgs_remaining -= 1;
                    },
                }
            }

            var budget: usize = COOP_BUDGET;
            while (self.msgs_remaining > 0) {
                if (budget == 0) {
                    ctx.getWaker().wakeByRef();
                    return .pending;
                }
                switch (self.channel.trySend(self.next_value)) {
                    .ok => {
                        self.next_value += 1;
                        self.msgs_remaining -= 1;
                        budget -= 1;
                    },
                    .closed => return .{ .ready = {} },
                    .full => {
                        self.send_future = self.channel.sendFuture(self.next_value);
                        switch (self.send_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => {
                                self.send_future = null;
                                self.next_value += 1;
                                self.msgs_remaining -= 1;
                            },
                        }
                    },
                }
            }
            return .{ .ready = {} };
        }
    };
}

/// MPMC consumer that counts received values via atomic counter.
fn CountingRecvFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;
        const COOP_BUDGET: usize = 128;

        channel: *Channel(T),
        received: *Atomic(usize),
        recv_future: ?RecvFuture(T) = null,

        pub fn init(ch: *Channel(T), received: *Atomic(usize)) Self {
            return .{ .channel = ch, .received = received };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            if (self.recv_future != null) {
                switch (self.recv_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => |val| {
                        self.recv_future = null;
                        if (val == null) return .{ .ready = {} };
                        _ = self.received.fetchAdd(1, .monotonic);
                    },
                }
            }

            var budget: usize = COOP_BUDGET;
            while (true) {
                if (budget == 0) {
                    ctx.getWaker().wakeByRef();
                    return .pending;
                }
                switch (self.channel.tryRecv()) {
                    .value => {
                        _ = self.received.fetchAdd(1, .monotonic);
                        budget -= 1;
                    },
                    .closed => return .{ .ready = {} },
                    .empty => {
                        self.recv_future = self.channel.recvFuture();
                        switch (self.recv_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => |val| {
                                self.recv_future = null;
                                if (val == null) return .{ .ready = {} };
                                _ = self.received.fetchAdd(1, .monotonic);
                            },
                        }
                    },
                }
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "MPMC - 2P 4C capacity=1" {
    // Capacity=1 forces every op through the waiter slow path
    const num_producers = 2;
    const num_consumers = 4;
    const total_msgs = 1000;
    const msgs_per_producer = total_msgs / num_producers;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var ch = try Channel(u64).init(testing.allocator, 1);
    var received = Atomic(usize).init(0);

    // Spawn producers
    var prod_handles: [num_producers]volt.Future(void) = undefined;
    for (0..num_producers) |p| {
        const start_val: u64 = @intCast(p * msgs_per_producer);
        prod_handles[p] = try io.awaitFuture(
            MpmcSendFuture(u64).init(&ch, msgs_per_producer, start_val),
        );
    }

    // Spawn consumers
    var cons_handles: [num_consumers]volt.Future(void) = undefined;
    for (0..num_consumers) |c| {
        cons_handles[c] = try io.awaitFuture(
            CountingRecvFuture(u64).init(&ch, &received),
        );
    }

    // Wait for producers, then close so consumers exit
    for (&prod_handles) |*h| _ = h.await(io);
    ch.close();
    for (&cons_handles) |*h| _ = h.await(io);

    ch.deinit();

    try testing.expectEqual(@as(usize, total_msgs), received.load(.acquire));
}

test "MPMC - 4P 4C capacity=64" {
    const num_producers = 4;
    const num_consumers = 4;
    const total_msgs = 2000;
    const msgs_per_producer = total_msgs / num_producers;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var ch = try Channel(u64).init(testing.allocator, 64);
    var received = Atomic(usize).init(0);

    var prod_handles: [num_producers]volt.Future(void) = undefined;
    for (0..num_producers) |p| {
        const start_val: u64 = @intCast(p * msgs_per_producer);
        prod_handles[p] = try io.awaitFuture(
            MpmcSendFuture(u64).init(&ch, msgs_per_producer, start_val),
        );
    }

    var cons_handles: [num_consumers]volt.Future(void) = undefined;
    for (0..num_consumers) |c| {
        cons_handles[c] = try io.awaitFuture(
            CountingRecvFuture(u64).init(&ch, &received),
        );
    }

    for (&prod_handles) |*h| _ = h.await(io);
    ch.close();
    for (&cons_handles) |*h| _ = h.await(io);

    ch.deinit();

    try testing.expectEqual(@as(usize, total_msgs), received.load(.acquire));
}

test "Channel close racing with waiters" {
    // 4 consumers start on empty channel, producer sends 10 then closes
    const num_consumers = 4;
    const num_msgs = 10;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var ch = try Channel(u64).init(testing.allocator, 4);
    var received = Atomic(usize).init(0);

    // Spawn consumers first (they'll block on empty channel)
    var cons_handles: [num_consumers]volt.Future(void) = undefined;
    for (0..num_consumers) |c| {
        cons_handles[c] = try io.awaitFuture(
            CountingRecvFuture(u64).init(&ch, &received),
        );
    }

    // Spawn producer
    var prod_handle = try io.awaitFuture(
        MpmcSendFuture(u64).init(&ch, num_msgs, 0),
    );

    _ = prod_handle.await(io);
    ch.close();
    for (&cons_handles) |*h| _ = h.await(io);

    ch.deinit();

    try testing.expectEqual(@as(usize, num_msgs), received.load(.acquire));
}

test "MPMC - close with no messages" {
    // All consumers should return cleanly on immediate close
    const num_consumers = 4;

    var io = try Runtime.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var ch = try Channel(u64).init(testing.allocator, 4);
    var received = Atomic(usize).init(0);

    var cons_handles: [num_consumers]volt.Future(void) = undefined;
    for (0..num_consumers) |c| {
        cons_handles[c] = try io.awaitFuture(
            CountingRecvFuture(u64).init(&ch, &received),
        );
    }

    // Close immediately — consumers should all exit cleanly
    ch.close();
    for (&cons_handles) |*h| _ = h.await(io);

    ch.deinit();

    try testing.expectEqual(@as(usize, 0), received.load(.acquire));
}

//! Minimal MPMC reproduction — 1P 2C with instrumentation
//!
//! Uses the public async API: io.awaitFuture() + volt.Future.

const std = @import("std");
const volt = @import("volt");

const Io = volt.Io;
const Channel = volt.channel.Channel;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;
const SendFuture = volt.channel.channel_mod.SendFuture;
const RecvFuture = volt.channel.channel_mod.RecvFuture;
const Atomic = std.atomic.Value;

var total_received: Atomic(usize) = Atomic(usize).init(0);
var consumer_polls: [2]Atomic(usize) = .{ Atomic(usize).init(0), Atomic(usize).init(0) };
var consumer_done: [2]Atomic(bool) = .{ Atomic(bool).init(false), Atomic(bool).init(false) };

fn MpmcSendFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;
        channel: *Channel(T),
        msgs_remaining: usize,
        next_value: T,
        send_future: ?SendFuture(T) = null,

        pub fn init(channel: *Channel(T), msgs_count: usize, start_value: T) Self {
            return .{ .channel = channel, .msgs_remaining = msgs_count, .next_value = start_value };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            while (self.msgs_remaining > 0) {
                if (self.send_future == null) {
                    self.send_future = self.channel.sendFuture(self.next_value);
                }
                switch (self.send_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => {
                        self.send_future = null;
                        self.next_value += 1;
                        self.msgs_remaining -= 1;
                    },
                }
            }
            return .{ .ready = {} };
        }
    };
}

fn InstrumentedRecvFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;
        channel: *Channel(T),
        recv_future: ?RecvFuture(T) = null,
        consumer_id: usize,

        pub fn init(channel: *Channel(T), id: usize) Self {
            return .{ .channel = channel, .consumer_id = id };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            _ = consumer_polls[self.consumer_id].fetchAdd(1, .monotonic);

            while (true) {
                if (self.recv_future == null) {
                    self.recv_future = self.channel.recvFuture();
                }
                switch (self.recv_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => |val| {
                        self.recv_future = null;
                        if (val == null) {
                            consumer_done[self.consumer_id].store(true, .release);
                            return .{ .ready = {} };
                        }
                        _ = total_received.fetchAdd(1, .monotonic);
                    },
                }
            }
        }
    };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("Test: 1P 2C 20msg buf=5 workers=2\n", .{});
    var io = try Runtime.init(allocator, .{ .num_workers = 2 });
    defer io.deinit();

    var ch = try Channel(u64).init(allocator, 5);

    // Spawn via io.awaitFuture — returns volt.Future(void)
    var prod = try io.awaitFuture(MpmcSendFuture(u64).init(&ch, 20, 0));
    var c0 = try io.awaitFuture(InstrumentedRecvFuture(u64).init(&ch, 0));
    var c1 = try io.awaitFuture(InstrumentedRecvFuture(u64).init(&ch, 1));

    // Await producer
    _ = prod.await(io);
    std.debug.print("  producer done, total_recv={d}\n", .{total_received.load(.acquire)});

    // Close channel
    ch.close();
    std.debug.print("  channel closed\n", .{});

    // Monitor consumers
    var attempts: usize = 0;
    while (attempts < 100) : (attempts += 1) {
        const d0 = consumer_done[0].load(.acquire);
        const d1 = consumer_done[1].load(.acquire);
        const p0 = consumer_polls[0].load(.acquire);
        const p1 = consumer_polls[1].load(.acquire);
        const recv = total_received.load(.acquire);

        if (d0 and d1) break;

        if (attempts % 10 == 0) {
            std.debug.print("  attempt {d}: recv={d} c0_polls={d} c0_done={} c1_polls={d} c1_done={}\n", .{
                attempts, recv, p0, d0, p1, d1,
            });
        }

        std.Thread.sleep(50 * std.time.ns_per_ms);
    }

    if (consumer_done[0].load(.acquire) and consumer_done[1].load(.acquire)) {
        _ = c0.await(io);
        _ = c1.await(io);
        std.debug.print("  PASSED! total_recv={d}\n", .{total_received.load(.acquire)});
    } else {
        std.debug.print("  HUNG! c0_polls={d} c0_done={} c1_polls={d} c1_done={}\n", .{
            consumer_polls[0].load(.acquire),
            consumer_done[0].load(.acquire),
            consumer_polls[1].load(.acquire),
            consumer_done[1].load(.acquire),
        });
    }

    ch.deinit();
}

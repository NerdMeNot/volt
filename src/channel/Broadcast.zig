//! Broadcast(T) — multi-receiver fan-out channel.
//!
//! One sender, many receivers; every receiver gets every message — until
//! it falls behind. The ring buffer is fixed-size: when a slow receiver
//! lags by more than `capacity`, the next `recv()` returns `.lagged(N)`
//! instead of a value, signaling the receiver that N messages were
//! dropped from its perspective. Slow consumers don't backpressure
//! producers; they just lose history.
//!
//! ## Usage
//!
//! ```zig
//! var bcast = try Broadcast(Event).init(allocator, 64);
//! defer bcast.deinit();
//!
//! // Producer:
//! try bcast.send(event);
//!
//! // Each receiver subscribes and reads independently:
//! var rx = bcast.subscribe();
//! while (true) {
//!     switch (try rx.recv()) {
//!         .value => |v| handle(v),
//!         .lagged => |n| std.log.warn("dropped {} events", .{n}),
//!         .closed => return,
//!     }
//! }
//! ```
//!
//! ## Design
//!
//! - Single `mutex` protects the ring + all receivers' position lookups.
//!   Producers hold it briefly (write one slot, drain the waiter list,
//!   release before unpark). Receivers hold it briefly (copy one slot,
//!   advance cursor, release before park).
//! - Wakes go to ALL parked receivers on `send` (true broadcast).
//!   Receivers contend for the lock when woken, take their own value, and
//!   return; subsequent re-park if no further messages.
//! - Capacity is rounded up to a power of two for bitmask wrap-around.
//!   Floor of 2 (cap=1 has degenerate edge cases — see `Channel.zig`).

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

// Broadcast carries "closed" and "lagged" as successful return tags on
// `Recv` (the consumer needs to distinguish them from a value), so the
// only error path on `recv` is `Cancelled`. `SendError` matches the rest
// of the channel family.
const errors = @import("errors.zig");
pub const SendError = errors.SendError;
pub const RecvError = error{Cancelled};

pub fn Broadcast(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const Recv = union(enum) {
            value: T,
            /// N messages were dropped from this receiver's perspective.
            /// The receiver's cursor has been reset to the oldest still-
            /// available message; the NEXT `recv()` returns that value.
            lagged: u64,
            closed,
        };

        const Slot = struct { value: T = undefined };

        const Waiter = struct {
            park: Park = .{},
            next: ?*Waiter = null,
        };

        const WaiterList = struct {
            head: ?*Waiter = null,
            tail: ?*Waiter = null,

            fn pushBack(self: *@This(), w: *Waiter) void {
                w.next = null;
                if (self.tail) |t| t.next = w else self.head = w;
                self.tail = w;
            }

            fn drain(self: *@This()) ?*Waiter {
                const head = self.head;
                self.head = null;
                self.tail = null;
                return head;
            }
        };

        mutex: thread.Mutex = .{},
        slots: []Slot,
        cap: u64,
        mask: u64,
        tail: u64 = 0,
        closed: bool = false,
        waiters: WaiterList = .{},
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, requested_capacity: usize) !Self {
            assert(requested_capacity > 0);
            const floored = @max(requested_capacity, @as(usize, 2));
            const cap = std.math.ceilPowerOfTwo(usize, floored) catch
                return error.CapacityTooLarge;
            const slots = try allocator.alloc(Slot, cap);
            errdefer allocator.free(slots);
            for (slots) |*s| s.* = .{};
            return .{
                .slots = slots,
                .cap = @intCast(cap),
                .mask = @as(u64, @intCast(cap)) - 1,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            assert(self.waiters.head == null);
            self.allocator.free(self.slots);
            self.* = undefined;
        }

        pub fn capacity(self: *const Self) usize {
            return @intCast(self.cap);
        }

        pub fn send(self: *Self, val: T) SendError!void {
            self.mutex.lock();
            if (self.closed) {
                self.mutex.unlock();
                return error.Closed;
            }
            self.slots[self.tail & self.mask].value = val;
            self.tail +%= 1;
            const drained = self.waiters.drain();
            self.mutex.unlock();
            unparkAll(drained);
        }

        pub fn close(self: *Self) void {
            self.mutex.lock();
            self.closed = true;
            const drained = self.waiters.drain();
            self.mutex.unlock();
            unparkAll(drained);
        }

        /// Subscribe a receiver synced to the current tail. The receiver
        /// will see messages sent AFTER subscribe — never history.
        pub fn subscribe(self: *Self) Receiver {
            self.mutex.lock();
            defer self.mutex.unlock();
            return .{ .b = self, .cursor = self.tail };
        }

        pub const Receiver = struct {
            b: *Self,
            cursor: u64,

            /// Suspend the calling coroutine until a value is available
            /// or the channel is closed. Returns `.lagged(N)` if the
            /// receiver fell more than `capacity` behind — N is the
            /// number of skipped messages, and the cursor has been
            /// reset; the next `recv()` returns the oldest still-
            /// available value.
            pub fn recv(self: *Receiver) RecvError!Recv {
                while (true) {
                    var waiter: Waiter = .{};
                    self.b.mutex.lock();

                    if (self.cursor != self.b.tail) {
                        // Have something to read.
                        const ahead = self.b.tail -% self.cursor;
                        if (ahead > self.b.cap) {
                            // Lagged. Reset to oldest available.
                            const skipped = ahead - self.b.cap;
                            self.cursor = self.b.tail -% self.b.cap;
                            self.b.mutex.unlock();
                            return .{ .lagged = skipped };
                        }
                        const value = self.b.slots[self.cursor & self.b.mask].value;
                        self.cursor +%= 1;
                        self.b.mutex.unlock();
                        return .{ .value = value };
                    }
                    if (self.b.closed) {
                        self.b.mutex.unlock();
                        return .closed;
                    }
                    self.b.waiters.pushBack(&waiter);
                    self.b.mutex.unlock();

                    waiter.park.parkCurrent() catch |err| switch (err) {
                        error.Cancelled => {
                            self.b.removeWaiter(&waiter);
                            return error.Cancelled;
                        },
                    };
                }
            }
        };

        fn removeWaiter(self: *Self, target: *Waiter) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            var prev: ?*Waiter = null;
            var cur = self.waiters.head;
            while (cur) |w| : (cur = w.next) {
                if (w == target) {
                    if (prev) |p| p.next = w.next else self.waiters.head = w.next;
                    if (self.waiters.tail == w) self.waiters.tail = prev;
                    w.next = null;
                    return;
                }
                prev = w;
            }
        }

        fn unparkAll(head: ?*Waiter) void {
            var cur = head;
            while (cur) |w| {
                const next = w.next;
                w.next = null;
                w.park.unpark();
                cur = next;
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "broadcast: capacity rounds up + floors at 2" {
    var b1 = try Broadcast(u32).init(std.testing.allocator, 1);
    defer b1.deinit();
    try std.testing.expectEqual(@as(usize, 2), b1.capacity());

    var b2 = try Broadcast(u32).init(std.testing.allocator, 5);
    defer b2.deinit();
    try std.testing.expectEqual(@as(usize, 8), b2.capacity());
}

const FanCtx = struct {
    b: *Broadcast(u32),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn fanReceiver(ctx: *FanCtx, expected: u32) !void {
    var rx = ctx.b.subscribe();
    var got: u32 = 0;
    while (got < expected) {
        switch (try rx.recv()) {
            .value => |v| {
                _ = ctx.sum.fetchAdd(v, .monotonic);
                got += 1;
            },
            .lagged => continue,
            .closed => return,
        }
    }
}

fn fanSender(ctx: *FanCtx, n: u32) !void {
    var i: u32 = 1;
    while (i <= n) : (i += 1) {
        try ctx.b.send(i);
        try volt.yield(); // pace so receivers stay caught up
    }
    ctx.b.close();
}

fn fanRoot() !u64 {
    // Capacity sized so the sender can buffer all messages without ever
    // overrunning the slowest receiver — this test verifies the
    // "all-deliveries" invariant, not lag handling (covered separately).
    var b = try Broadcast(u32).init(std.testing.allocator, 128);
    defer b.deinit();
    var ctx = FanCtx{ .b = &b };

    const N_MSG: u32 = 50;
    const N_RX: u32 = 4;

    // Subscribe receivers FIRST (so they see all messages from msg #1).
    var receivers: [N_RX]*volt.Task(@TypeOf(fanReceiver)) = undefined;
    for (&receivers) |*t| t.* = try volt.spawn(fanReceiver, .{ &ctx, N_MSG });
    defer for (receivers) |t| volt.destroyTask(t);

    var sender = try volt.spawn(fanSender, .{ &ctx, N_MSG });
    defer volt.destroyTask(sender);

    try sender.join();
    for (receivers) |t| try t.join();

    return ctx.sum.load(.acquire);
}

test "broadcast: 4 receivers each see 50 messages, sum invariant" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, fanRoot, .{});
    // Each receiver sums 1..50 = 1275; 4 receivers → 5100.
    try std.testing.expectEqual(@as(u64, 5100), sum);
}

const LaggedCtx = struct {
    b: *Broadcast(u32),
    lag_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn slowReceiver(ctx: *LaggedCtx) !void {
    var rx = ctx.b.subscribe();
    while (true) {
        // Yield every recv so the sender (which doesn't yield) gets
        // dispatched and outruns us. Without this, scheduler can keep
        // sender + receiver in lockstep on most runs — the test then
        // races on whether sender ever pulls ahead by more than `cap`.
        // The deterministic yield ensures we ARE slow.
        try volt.yield();
        switch (try rx.recv()) {
            .value => continue,
            .lagged => {
                _ = ctx.lag_count.fetchAdd(1, .monotonic);
                continue;
            },
            .closed => return,
        }
    }
}

fn fastSender(ctx: *LaggedCtx) !void {
    // Subscribe a slow receiver, then blast 100 messages without
    // yielding so it lags past capacity (cap=4).
    var i: u32 = 0;
    while (i < 100) : (i += 1) try ctx.b.send(i);
    ctx.b.close();
}

fn laggedRoot() !u32 {
    var b = try Broadcast(u32).init(std.testing.allocator, 4);
    defer b.deinit();
    var ctx = LaggedCtx{ .b = &b };

    var rx = try volt.spawn(slowReceiver, .{&ctx});
    defer volt.destroyTask(rx);

    // Yield so the receiver has a chance to subscribe before sender starts.
    try volt.yield();

    var sender = try volt.spawn(fastSender, .{&ctx});
    defer volt.destroyTask(sender);

    try sender.join();
    try rx.join();
    return ctx.lag_count.load(.acquire);
}

test "broadcast: slow receiver gets .lagged at least once" {
    const lags = try volt.run(.{ .allocator = std.testing.allocator }, laggedRoot, .{});
    try std.testing.expect(lags >= 1);
}

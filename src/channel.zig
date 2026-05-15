//! Channels.
//!
//! `Spsc(T, cap)` — comptime-specialized single-producer single-consumer
//! ring buffer. "SPSC" means the channel takes ONE producer and ONE
//! consumer (a *placement* constraint), NOT "single thread." The
//! producer and consumer can be coroutines running on any two OS
//! threads.
//!
//! Memory model:
//!   - `ring[i]`: producer writes; `head` release-store carries the
//!     write to any consumer doing `head` acquire-load.
//!   - `head`: producer-only writer, atomic for cross-thread visibility.
//!   - `tail`: consumer-only writer, atomic for cross-thread visibility.
//!   - `head` and `tail` on separate cache lines (no false sharing).
//!
//! Block-on-full / block-on-empty uses the parking lot keyed on
//! `&head` (consumer parks here waiting for the producer to send) and
//! `&tail` (producer parks here waiting for the consumer to drain).
//! The validator-under-lock pattern closes the wait/wake race that
//! the old single-slot `recv_waiter` / `send_waiter` design suffered
//! from under sustained multi-worker load (caught by `zig build stress`).
//!
//! All operations must be called from inside a coroutine — `current.require()`
//! is used to find the Runtime for the unpark call.
//!
//! `Mpmc(T, cap)` — Vyukov bounded MPMC queue. Each ring cell carries
//! a sequence counter that gates producer/consumer access; the CAS is
//! on the global enqueue/dequeue position counters. See the type's
//! docstring for the algorithm.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const park = @import("park.zig");

pub const ChannelError = error{Closed};

const CACHE_LINE: usize = 128;

pub fn Spsc(comptime T: type, comptime cap: usize) type {
    comptime {
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0);
    }
    return struct {
        const Self = @This();
        const MASK: u64 = cap - 1;

        ring: [cap]T align(CACHE_LINE) = undefined,

        /// Producer-owned cache line. Consumers park on `&head`
        /// when the channel is empty.
        head: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        /// Consumer-owned cache line. Producers park on `&tail`
        /// when the channel is full.
        tail: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        closed: std.atomic.Value(bool) align(CACHE_LINE) = std.atomic.Value(bool).init(false),

        /// Validator for producer parked on full channel: keep
        /// parking if (still full) AND (not closed). Runs UNDER the
        /// parking-lot bucket lock — atomic with the consumer's
        /// state change.
        fn fullValidator(addr: *const anyopaque) bool {
            const tail_ptr: *align(CACHE_LINE) const std.atomic.Value(u64) =
                @ptrCast(@alignCast(addr));
            const self: *const Self = @fieldParentPtr("tail", tail_ptr);
            if (self.closed.load(.acquire)) return false;
            const h = self.head.load(.acquire);
            const t = tail_ptr.load(.acquire);
            return (h - t) >= cap;
        }

        /// Validator for consumer parked on empty channel: keep
        /// parking if (still empty) AND (not closed).
        fn emptyValidator(addr: *const anyopaque) bool {
            const head_ptr: *align(CACHE_LINE) const std.atomic.Value(u64) =
                @ptrCast(@alignCast(addr));
            const self: *const Self = @fieldParentPtr("head", head_ptr);
            if (self.closed.load(.acquire)) return false;
            const h = head_ptr.load(.acquire);
            const t = self.tail.load(.acquire);
            return h == t;
        }

        inline fn currentRuntime() *runtime.Runtime {
            return @ptrCast(@alignCast(current.require().runtime));
        }

        /// Send `v`. Blocks (parks current coroutine) if the channel
        /// is full. Returns `error.Closed` if the channel was closed.
        pub fn send(self: *Self, v: T) ChannelError!void {
            while (true) {
                if (self.closed.load(.acquire)) return error.Closed;
                const h = self.head.load(.monotonic);
                const t = self.tail.load(.acquire);
                if (h - t < cap) {
                    self.ring[h & MASK] = v;
                    self.head.store(h + 1, .release);
                    // Wake the consumer (if parked on head).
                    _ = park.unparkOne(currentRuntime(), &self.head);
                    return;
                }
                // Full — park on `tail`. Consumer's recv will
                // unpark us when it advances `tail`.
                park.parkOn(&self.tail, fullValidator);
            }
        }

        /// Receive a value. Blocks if empty. Returns `error.Closed`
        /// if the channel was closed AND no buffered values remain.
        pub fn recv(self: *Self) ChannelError!T {
            while (true) {
                const t = self.tail.load(.monotonic);
                const h = self.head.load(.acquire);
                if (h != t) {
                    const v = self.ring[t & MASK];
                    self.tail.store(t + 1, .release);
                    // Wake the producer (if parked on tail).
                    _ = park.unparkOne(currentRuntime(), &self.tail);
                    return v;
                }
                if (self.closed.load(.acquire)) return error.Closed;
                // Empty — park on `head`. Producer's send will
                // unpark us when it advances `head`.
                park.parkOn(&self.head, emptyValidator);
            }
        }

        /// Close the channel. Future sends fail; pending recvs drain
        /// then fail. Wakes both ends if either is parked.
        /// Must be called from inside a coroutine.
        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
            const rt = currentRuntime();
            _ = park.unparkOne(rt, &self.head);
            _ = park.unparkOne(rt, &self.tail);
        }
    };
}

/// Bounded MPMC channel — Vyukov's lock-free queue with parking-lot
/// blocking on full / empty.
///
/// Each ring cell carries a `seq` counter. The protocol:
///
///   enqueue at position P:
///     cell = ring[P & mask]
///     if cell.seq == P:           cell is free for our enqueue
///       CAS enqueue_pos: P → P+1   claim the slot
///       store data; cell.seq = P+1 publish
///     elif cell.seq < P:          ring is full (one full lap ahead)
///       return error.Full
///     else:                        another producer took it; retry
///
///   dequeue at position P:
///     cell = ring[P & mask]
///     if cell.seq == P+1:         cell has a value for our dequeue
///       CAS dequeue_pos: P → P+1   claim it
///       read data; cell.seq = P + cap  release for the next lap
///     elif cell.seq < P+1:        empty
///       return error.Empty
///     else:                        another consumer took it; retry
///
/// Blocking variants park on `enqueue_pos` / `dequeue_pos` exactly
/// like Spsc; validators re-check the lock-free try-path under the
/// bucket lock to close the wait/wake race.
///
/// Cells are initialised so `ring[i].seq == i` — comptime-baked.
pub fn Mpmc(comptime T: type, comptime cap: usize) type {
    comptime {
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0);
    }
    return struct {
        const Self = @This();
        const MASK: u64 = cap - 1;

        const Cell = struct {
            seq: std.atomic.Value(u64),
            data: T,
        };

        ring: [cap]Cell align(CACHE_LINE) = blk: {
            @setEvalBranchQuota(cap * 16);
            var r: [cap]Cell = undefined;
            var i: usize = 0;
            while (i < cap) : (i += 1) {
                r[i] = .{
                    .seq = std.atomic.Value(u64).init(i),
                    .data = undefined,
                };
            }
            break :blk r;
        },

        /// Producer-shared cache line. Producers CAS on this when
        /// claiming a slot; consumers park here when full.
        enqueue_pos: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        /// Consumer-shared cache line. Consumers CAS on this when
        /// claiming a slot; producers park here when empty.
        dequeue_pos: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        closed: std.atomic.Value(bool) align(CACHE_LINE) = std.atomic.Value(bool).init(false),

        /// Non-blocking send. Returns `error.Full` if the ring is at
        /// capacity, `error.Closed` if closed. Otherwise inserts `v`.
        pub fn trySend(self: *Self, v: T) error{ Full, Closed }!void {
            if (self.closed.load(.acquire)) return error.Closed;
            var pos = self.enqueue_pos.load(.monotonic);
            while (true) {
                const cell = &self.ring[pos & MASK];
                const seq = cell.seq.load(.acquire);
                const diff = @as(i64, @bitCast(seq)) - @as(i64, @bitCast(pos));
                if (diff == 0) {
                    if (self.enqueue_pos.cmpxchgWeak(pos, pos + 1, .monotonic, .monotonic)) |observed| {
                        pos = observed;
                        continue;
                    }
                    cell.data = v;
                    cell.seq.store(pos + 1, .release);
                    return;
                } else if (diff < 0) {
                    return error.Full;
                } else {
                    pos = self.enqueue_pos.load(.monotonic);
                }
            }
        }

        /// Non-blocking recv. Returns `error.Empty` if the ring has
        /// no values, `error.Closed` if closed AND drained.
        pub fn tryRecv(self: *Self) error{ Empty, Closed }!T {
            var pos = self.dequeue_pos.load(.monotonic);
            while (true) {
                const cell = &self.ring[pos & MASK];
                const seq = cell.seq.load(.acquire);
                const want = pos + 1;
                const diff = @as(i64, @bitCast(seq)) - @as(i64, @bitCast(want));
                if (diff == 0) {
                    if (self.dequeue_pos.cmpxchgWeak(pos, pos + 1, .monotonic, .monotonic)) |observed| {
                        pos = observed;
                        continue;
                    }
                    const v = cell.data;
                    cell.seq.store(pos + cap, .release);
                    return v;
                } else if (diff < 0) {
                    if (self.closed.load(.acquire)) return error.Closed;
                    return error.Empty;
                } else {
                    pos = self.dequeue_pos.load(.monotonic);
                }
            }
        }

        /// Validator for a producer parked on `enqueue_pos`. Re-runs
        /// `trySend`-equivalent observation under the bucket lock:
        /// keep parking if (still full) AND (not closed).
        fn fullValidator(addr: *const anyopaque) bool {
            const enq_ptr: *align(CACHE_LINE) const std.atomic.Value(u64) =
                @ptrCast(@alignCast(addr));
            const self: *const Self = @fieldParentPtr("enqueue_pos", enq_ptr);
            if (self.closed.load(.acquire)) return false;
            const pos = enq_ptr.load(.acquire);
            const cell = &self.ring[pos & MASK];
            const seq = cell.seq.load(.acquire);
            // Still full iff the cell at the head producer position
            // is one-lap behind (seq < pos), meaning no consumer has
            // freed it yet.
            return @as(i64, @bitCast(seq)) - @as(i64, @bitCast(pos)) < 0;
        }

        /// Validator for a consumer parked on `dequeue_pos`. Keep
        /// parking if (still empty) AND (not closed).
        fn emptyValidator(addr: *const anyopaque) bool {
            const deq_ptr: *align(CACHE_LINE) const std.atomic.Value(u64) =
                @ptrCast(@alignCast(addr));
            const self: *const Self = @fieldParentPtr("dequeue_pos", deq_ptr);
            if (self.closed.load(.acquire)) return false;
            const pos = deq_ptr.load(.acquire);
            const cell = &self.ring[pos & MASK];
            const seq = cell.seq.load(.acquire);
            const want = pos + 1;
            return @as(i64, @bitCast(seq)) - @as(i64, @bitCast(want)) < 0;
        }

        inline fn currentRt() *runtime.Runtime {
            return @ptrCast(@alignCast(current.require().runtime));
        }

        /// Send `v`. Blocks (parks current coroutine) if the ring is
        /// full. Returns `error.Closed` if the channel was closed.
        pub fn send(self: *Self, v: T) ChannelError!void {
            while (true) {
                self.trySend(v) catch |e| switch (e) {
                    error.Closed => return error.Closed,
                    error.Full => {
                        // Park on enqueue_pos. A consumer's recv will
                        // bump dequeue_pos and unpark us.
                        park.parkOn(&self.enqueue_pos, fullValidator);
                        continue;
                    },
                };
                // Wake one consumer that might be parked on empty.
                _ = park.unparkOne(currentRt(), &self.dequeue_pos);
                return;
            }
        }

        /// Receive a value. Blocks if empty. Returns `error.Closed`
        /// if the channel was closed AND has no buffered values.
        pub fn recv(self: *Self) ChannelError!T {
            while (true) {
                if (self.tryRecv()) |v| {
                    // Wake one producer that might be parked on full.
                    _ = park.unparkOne(currentRt(), &self.enqueue_pos);
                    return v;
                } else |e| switch (e) {
                    error.Closed => return error.Closed,
                    error.Empty => park.parkOn(&self.dequeue_pos, emptyValidator),
                }
            }
        }

        /// Close. Future sends fail; pending recvs drain then fail.
        /// Wakes every parked producer and consumer so they observe
        /// the close — `unparkAll` rather than `unparkOne` because
        /// with N consumers all blocked on empty we have to wake all
        /// of them, not just one, to drain.
        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
            const rt = currentRt();
            _ = park.unparkAll(rt, &self.enqueue_pos);
            _ = park.unparkAll(rt, &self.dequeue_pos);
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;
const Runtime = runtime.Runtime;

const TestCtx = struct {
    ch: *Spsc(u64, 4),
    sum: u64 = 0,
    n: u64,
};

fn producer(ctx: *TestCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn consumer(ctx: *TestCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch return;
        ctx.sum +%= v;
    }
}

fn spscTestRoot(ctx: *TestCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var prod = try rt.spawn(producer, .{ctx});
    var cons = try rt.spawn(consumer, .{ctx});
    _ = prod.join() catch unreachable;
    _ = cons.join() catch unreachable;
}

test "spsc: small unbuffered pipeline (multi-worker default)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Spsc(u64, 4){};
    var ctx = TestCtx{ .ch = &ch, .n = 100 };
    try rt.run(spscTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u64, 4950), ctx.sum);
}

test "spsc: cap=2 stresses block-on-full, multi-worker" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Spsc(u64, 2){};
    var ctx = TestCtx{ .ch = &ch, .n = 1000 };
    try rt.run(spscTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u64, 499500), ctx.sum);
}

test "spsc: works at workers=1 (configuration check)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var ch = Spsc(u64, 4){};
    var ctx = TestCtx{ .ch = &ch, .n = 1000 };
    try rt.run(spscTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u64, 499500), ctx.sum);
}

// ─── Mpmc tests ──────────────────────────────────────────────────────

const MpmcCh = Mpmc(u64, 8);

const MpmcCtx = struct {
    ch: *MpmcCh,
    n_per_producer: u64,
    received: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn mpmcProducer(ctx: *MpmcCtx, id: u64) !void {
    var i: u64 = 0;
    while (i < ctx.n_per_producer) : (i += 1) {
        // Encode (producer_id, sequence_no) into one u64 so the
        // consumer can verify both that every producer produced
        // every value and that no duplicates / drops occurred.
        const v: u64 = (id << 32) | i;
        try ctx.ch.send(v);
    }
}

fn mpmcConsumer(ctx: *MpmcCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch return;
        _ = ctx.received.fetchAdd(1, .acq_rel);
        _ = ctx.sum.fetchAdd(v, .acq_rel);
    }
}

fn mpmcRoot4x4(ctx: *MpmcCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var producers: [4]*runtime.Task(@typeInfo(@TypeOf(mpmcProducer)).@"fn".return_type.?) = undefined;
    var consumers: [4]*runtime.Task(@typeInfo(@TypeOf(mpmcConsumer)).@"fn".return_type.?) = undefined;
    for (&producers, 0..) |*t, i| t.* = try rt.spawn(mpmcProducer, .{ ctx, @as(u64, @intCast(i)) });
    for (&consumers) |*t| t.* = try rt.spawn(mpmcConsumer, .{ctx});
    for (producers) |t| _ = t.join() catch unreachable;
    // All producers done — close the channel so consumers drain
    // their remaining backlog and exit.
    ctx.ch.close();
    for (consumers) |t| _ = t.join() catch unreachable;
}

test "mpmc: 4 producers × 4 consumers, all messages delivered exactly once" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = MpmcCh{};
    var ctx = MpmcCtx{ .ch = &ch, .n_per_producer = 250 };
    try rt.run(mpmcRoot4x4, .{&ctx});

    const total_msgs = 4 * 250;
    try std.testing.expectEqual(@as(u64, total_msgs), ctx.received.load(.acquire));

    // Sum check: each producer i sends i<<32 | k for k in [0, n).
    // Total = sum_i(n_per_producer * (i<<32)) + 4 * sum_k(k for k<n).
    const n = ctx.n_per_producer;
    var expected: u64 = 0;
    for (0..4) |i| expected +%= n *% (@as(u64, @intCast(i)) << 32);
    expected +%= 4 *% (n *% (n - 1) / 2);
    try std.testing.expectEqual(expected, ctx.sum.load(.acquire));
}

test "mpmc: trySend fills to cap, then returns error.Full" {
    var ch = Mpmc(u64, 4){};
    try ch.trySend(0);
    try ch.trySend(1);
    try ch.trySend(2);
    try ch.trySend(3);
    try std.testing.expectError(error.Full, ch.trySend(4));
}

test "mpmc: tryRecv on empty returns error.Empty" {
    var ch = Mpmc(u64, 4){};
    try std.testing.expectError(error.Empty, ch.tryRecv());
}

test "mpmc: trySend/tryRecv pipeline (single thread, no runtime needed)" {
    var ch = Mpmc(u64, 4){};
    try ch.trySend(10);
    try ch.trySend(20);
    try std.testing.expectEqual(@as(u64, 10), try ch.tryRecv());
    try std.testing.expectEqual(@as(u64, 20), try ch.tryRecv());
    try std.testing.expectError(error.Empty, ch.tryRecv());
}

test "mpmc: works at workers=1 (configuration check)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var ch = MpmcCh{};
    var ctx = MpmcCtx{ .ch = &ch, .n_per_producer = 50 };
    try rt.run(mpmcRoot4x4, .{&ctx});
    try std.testing.expectEqual(@as(u64, 4 * 50), ctx.received.load(.acquire));
}

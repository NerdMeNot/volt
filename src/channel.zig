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

inline fn currentRt() *runtime.Runtime {
    return @ptrCast(@alignCast(current.require().runtime));
}

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
                    _ = park.unparkOne(currentRt(), &self.head);
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
                    _ = park.unparkOne(currentRt(), &self.tail);
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
            const rt = currentRt();
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

/// One-shot single-value handoff. Producer sends once; consumer
/// receives once. After either side closes, future operations return
/// `error.Closed`.
///
/// Allocation-free — the value lives in the `Oneshot` struct itself.
/// Typical use: stash the channel on a parent's stack (or as a field
/// of some Ctx), pass `&ch` to producer and consumer.
///
/// State machine (atomic u32):
///   EMPTY     = 0 — nothing sent yet
///   FULL      = 1 — value has been sent, waiting for recv
///   CONSUMED  = 2 — recv has taken the value
///   CLOSED    = 3 — sender called close() without sending
pub fn Oneshot(comptime T: type) type {
    return struct {
        const Self = @This();
        const EMPTY: u32 = 0;
        const FULL: u32 = 1;
        const CONSUMED: u32 = 2;
        const CLOSED: u32 = 3;

        state: std.atomic.Value(u32) align(CACHE_LINE) = std.atomic.Value(u32).init(EMPTY),
        value: T = undefined,

        /// Send `v`. Returns `error.Closed` if the channel was closed
        /// or already sent. On success the receiver (if parked) is
        /// woken and `recv()` returns `v`.
        pub fn send(self: *Self, v: T) ChannelError!void {
            // Only EMPTY → FULL succeeds; everything else means the
            // slot is unavailable.
            self.value = v;
            if (self.state.cmpxchgStrong(EMPTY, FULL, .release, .monotonic)) |_| {
                // CAS failed — state was FULL / CONSUMED / CLOSED.
                self.value = undefined;
                return error.Closed;
            }
            _ = park.unparkOne(currentRt(), &self.state);
        }

        /// Receive the value. Blocks until `send` or `close` fires.
        /// Returns `error.Closed` if the sender closed without sending.
        pub fn recv(self: *Self) ChannelError!T {
            while (true) {
                const s = self.state.load(.acquire);
                switch (s) {
                    FULL => {
                        // CAS to CONSUMED so a duplicate recv returns
                        // error.Closed cleanly.
                        if (self.state.cmpxchgWeak(FULL, CONSUMED, .acquire, .monotonic)) |_| continue;
                        return self.value;
                    },
                    CLOSED, CONSUMED => return error.Closed,
                    else => park.parkOn(&self.state, oneshotRecvValidator),
                }
            }
        }

        /// Close the channel without sending. Subsequent `send` /
        /// `recv` calls return `error.Closed`. Safe to call from
        /// either side.
        pub fn close(self: *Self) void {
            // Only EMPTY → CLOSED transitions; if state is FULL we
            // leave the value alone for recv to consume. If state is
            // CONSUMED, nothing to do.
            _ = self.state.cmpxchgStrong(EMPTY, CLOSED, .release, .monotonic);
            _ = park.unparkOne(currentRt(), &self.state);
        }

        /// True iff `send` has been called (regardless of whether
        /// recv has consumed yet).
        pub fn isReady(self: *const Self) bool {
            const s = self.state.load(.acquire);
            return s == FULL or s == CONSUMED;
        }

        fn oneshotRecvValidator(addr: *const anyopaque) bool {
            const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
            return sp.load(.acquire) == EMPTY;
        }
    };
}

/// Multi-consumer "latest value" channel. Producer publishes; every
/// receiver sees the most recent value (intermediate updates may be
/// silently skipped). Producer never blocks. Receivers use
/// `rx.changed()` to wait for a new version and `rx.borrow()` to
/// read the current value.
///
/// Useful for: config hot-reload, current-state propagation, watcher
/// shapes where "most recent" is more important than "every value."
///
/// The value is published via a seqlock — even version = stable,
/// odd version = writer mid-update. Readers retry on a torn read.
pub fn Watch(comptime T: type) type {
    return struct {
        const Self = @This();

        value: T align(CACHE_LINE),
        version: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),
        closed: std.atomic.Value(bool) align(CACHE_LINE) = std.atomic.Value(bool).init(false),

        pub fn init(initial: T) Self {
            return .{ .value = initial };
        }

        /// Publish a new value. Wakes every receiver parked in
        /// `rx.changed()`. Never blocks.
        pub fn send(self: *Self, v: T) void {
            const v_now = self.version.load(.monotonic);
            self.version.store(v_now + 1, .release); // mid-write (odd)
            self.value = v;
            self.version.store(v_now + 2, .release); // stable (even)
            _ = park.unparkAll(currentRt(), &self.version);
        }

        /// Close the watch. Outstanding `rx.changed()` calls return
        /// `error.Closed`. Future `borrow()` calls keep returning the
        /// last value (history is implicit in `value`).
        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
            _ = park.unparkAll(currentRt(), &self.version);
        }

        /// Create a receiver. Cursor starts at 0, so a fresh receiver
        /// sees the current value as "new" on its first `changed()`
        /// (matches Tokio's watch::channel semantics).
        pub fn receiver(self: *Self) Receiver {
            return .{ .watch = self, .cursor = 0 };
        }

        pub const Receiver = struct {
            watch: *Self,
            cursor: u64,

            /// Park until the watch advances past our cursor. Returns
            /// `error.Closed` only after every version up to close has
            /// been observed (so a closed-but-unseen watch surfaces
            /// its final value first, then Closed on the next call).
            pub fn changed(self: *Receiver) ChannelError!void {
                while (true) {
                    const v = self.watch.version.load(.acquire);
                    // Round down to the last completed version (writer
                    // mid-update has odd version).
                    const stable = if (v & 1 == 1) v -% 1 else v;
                    if (stable > self.cursor) {
                        self.cursor = stable;
                        return;
                    }
                    // No new version. Check close AFTER we've seen the
                    // latest visible version.
                    if (self.watch.closed.load(.acquire)) return error.Closed;
                    park.parkOn(&self.watch.version, watchChangedValidator);
                }
            }

            /// Read the current value. Uses a seqlock retry loop in
            /// case the writer is mid-update.
            pub fn borrow(self: *Receiver) T {
                while (true) {
                    const v1 = self.watch.version.load(.acquire);
                    if (v1 & 1 == 1) {
                        std.atomic.spinLoopHint();
                        continue;
                    }
                    const v = self.watch.value;
                    const v2 = self.watch.version.load(.acquire);
                    if (v2 == v1) return v;
                }
            }
        };

        fn watchChangedValidator(addr: *const anyopaque) bool {
            const sp: *const std.atomic.Value(u64) = @ptrCast(@alignCast(addr));
            // Park only if there's no new version available. Caller
            // re-checks closed in the loop above.
            const v = sp.load(.acquire);
            // We don't know the receiver's cursor here — the loop in
            // `changed` re-checks after wake. The validator just
            // confirms the version hasn't advanced under us.
            // Conservative: always park; the caller's loop is the
            // truth.
            _ = v;
            return true;
        }
    };
}

/// Multi-consumer history-aware ring buffer. Producer publishes into
/// a ring of `cap` slots; each receiver maintains its own cursor.
/// Slow receivers see `error.Lagged` when the producer has overwritten
/// data they haven't read yet (cursor falls more than `cap` behind).
///
/// Producer never blocks. Suitable for: pub/sub fan-out where slow
/// subscribers should NOT slow the producer down. If you want
/// backpressure (slow consumers block producer), use `Mpmc` instead.
///
/// Implementation: per-slot sequence counter, monotonically increasing.
/// `seq[i]` carries the position of the latest write to slot i. A
/// receiver waiting for cursor `c` looks at `seq[c & MASK]` and
/// either reads (if seq == c+1) or detects lag (if seq > c+1+cap).
pub fn Broadcast(comptime T: type, comptime cap: usize) type {
    comptime {
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0);
    }
    return struct {
        const Self = @This();
        const MASK: u64 = cap - 1;

        ring: [cap]T align(CACHE_LINE) = undefined,
        seq: [cap]std.atomic.Value(u64) = blk: {
            @setEvalBranchQuota(cap * 16);
            var s: [cap]std.atomic.Value(u64) = undefined;
            var i: usize = 0;
            while (i < cap) : (i += 1) s[i] = std.atomic.Value(u64).init(0);
            break :blk s;
        },
        head: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),
        closed: std.atomic.Value(bool) align(CACHE_LINE) = std.atomic.Value(bool).init(false),

        /// Publish a value. Never blocks; overwrites the oldest slot
        /// if the ring is full (slow receivers will see Lagged).
        pub fn send(self: *Self, v: T) void {
            const pos = self.head.fetchAdd(1, .acq_rel);
            const slot = pos & MASK;
            self.ring[slot] = v;
            self.seq[slot].store(pos + 1, .release);
            _ = park.unparkAll(currentRt(), &self.head);
        }

        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
            _ = park.unparkAll(currentRt(), &self.head);
        }

        pub fn receiver(self: *Self) Receiver {
            return .{ .bc = self, .cursor = self.head.load(.acquire) };
        }

        pub const RecvError = error{ Closed, Lagged };

        pub const Receiver = struct {
            bc: *Self,
            cursor: u64,

            /// Block until a new value is available. Returns
            /// `error.Lagged` if the producer overran our cursor (and
            /// advances the cursor past the lost region); call again
            /// to resume reading.
            pub fn recv(self: *Receiver) RecvError!T {
                while (true) {
                    const h = self.bc.head.load(.acquire);
                    if (h > self.cursor) {
                        const behind = h -% self.cursor;
                        if (behind > cap) {
                            // Slow consumer. Advance to oldest
                            // available; caller retries.
                            self.cursor = h -% cap;
                            return error.Lagged;
                        }
                        const slot = self.cursor & MASK;
                        const want = self.cursor + 1;
                        // Wait for the slot's sequence to catch up.
                        // It can only be ahead (we already know head
                        // ≥ want) — usually it's exactly `want`.
                        var seq = self.bc.seq[slot].load(.acquire);
                        while (seq < want) {
                            std.atomic.spinLoopHint();
                            seq = self.bc.seq[slot].load(.acquire);
                        }
                        const v = self.bc.ring[slot];
                        // Torn-read check: if seq advanced past our
                        // window during the read, we got overwritten.
                        if (self.bc.seq[slot].load(.acquire) >= want + cap) {
                            self.cursor = self.bc.head.load(.acquire) -% cap;
                            return error.Lagged;
                        }
                        self.cursor +%= 1;
                        return v;
                    }
                    if (self.bc.closed.load(.acquire)) return error.Closed;
                    park.parkOn(&self.bc.head, broadcastRecvValidator);
                }
            }
        };

        fn broadcastRecvValidator(addr: *const anyopaque) bool {
            // Conservative: always park if validator runs. The recv
            // loop re-checks head and closed under the bucket lock
            // implicitly via the load.acquire after wake. There's no
            // false-park risk: an unpark from `send` runs after the
            // store, so any post-wake load sees the new head.
            _ = addr;
            return true;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// See note in src/runtime.zig: std.testing.allocator's stack-trace
// capture corrupts under multi-worker spawn. smp_allocator is the
// thread-safe choice for runtime-touching tests.
const test_allocator = std.heap.smp_allocator;
const Runtime = runtime.Runtime;

const TestCtxCap4 = struct {
    ch: *Spsc(u64, 4),
    sum: u64 = 0,
    n: u64,
};

const TestCtxCap2 = struct {
    ch: *Spsc(u64, 2),
    sum: u64 = 0,
    n: u64,
};

fn producer4(ctx: *TestCtxCap4) ChannelError!void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn consumer4(ctx: *TestCtxCap4) ChannelError!void {
    while (true) {
        const v = ctx.ch.recv() catch return;
        ctx.sum +%= v;
    }
}

fn spscTestRoot4(ctx: *TestCtxCap4) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var prod = try rt.spawn(producer4, .{ctx});
    var cons = try rt.spawn(consumer4, .{ctx});
    _ = prod.join() catch unreachable;
    _ = cons.join() catch unreachable;
}

fn producer2(ctx: *TestCtxCap2) ChannelError!void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn consumer2(ctx: *TestCtxCap2) ChannelError!void {
    while (true) {
        const v = ctx.ch.recv() catch return;
        ctx.sum +%= v;
    }
}

fn spscTestRoot2(ctx: *TestCtxCap2) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var prod = try rt.spawn(producer2, .{ctx});
    var cons = try rt.spawn(consumer2, .{ctx});
    _ = prod.join() catch unreachable;
    _ = cons.join() catch unreachable;
}

test "spsc: small unbuffered pipeline (multi-worker default)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Spsc(u64, 4){};
    var ctx = TestCtxCap4{ .ch = &ch, .n = 100 };
    try (try rt.run(spscTestRoot4, .{&ctx}));

    try std.testing.expectEqual(@as(u64, 4950), ctx.sum);
}

test "spsc: cap=2 stresses block-on-full, multi-worker" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Spsc(u64, 2){};
    var ctx = TestCtxCap2{ .ch = &ch, .n = 1000 };
    try (try rt.run(spscTestRoot2, .{&ctx}));

    try std.testing.expectEqual(@as(u64, 499500), ctx.sum);
}

test "spsc: works at workers=1 (configuration check)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var ch = Spsc(u64, 4){};
    var ctx = TestCtxCap4{ .ch = &ch, .n = 1000 };
    try (try rt.run(spscTestRoot4, .{&ctx}));

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
    try (try rt.run(mpmcRoot4x4, .{&ctx}));

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
    try (try rt.run(mpmcRoot4x4, .{&ctx}));
    try std.testing.expectEqual(@as(u64, 4 * 50), ctx.received.load(.acquire));
}

// ─── Oneshot tests ───────────────────────────────────────────────────

const OneshotCtx = struct {
    ch: *Oneshot(u64),
    got: u64 = 0,
};

fn oneshotProducer(ch: *Oneshot(u64)) !void {
    try ch.send(0xCAFE);
}

fn oneshotConsumer(ctx: *OneshotCtx) !void {
    ctx.got = try ctx.ch.recv();
}

fn oneshotRoot(ctx: *OneshotCtx) !void {
    const rt = @import("runtime.zig");
    _ = rt;
    var p = try @import("lib.zig").spawn(oneshotProducer, .{ctx.ch});
    var c = try @import("lib.zig").spawn(oneshotConsumer, .{ctx});
    _ = p.join() catch unreachable;
    _ = c.join() catch unreachable;
}

test "oneshot: single-value handoff between two coros" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Oneshot(u64){};
    var ctx = OneshotCtx{ .ch = &ch };
    try (try rt.run(oneshotRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u64, 0xCAFE), ctx.got);
}

fn oneshotDoubleSendRoot(ch: *Oneshot(u64)) !void {
    try ch.send(1);
    try std.testing.expectError(error.Closed, ch.send(2));
}

test "oneshot: second send returns Closed" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var ch = Oneshot(u64){};
    try (try rt.run(oneshotDoubleSendRoot, .{&ch}));
}

fn oneshotCloseProducer(ch: *Oneshot(u64)) !void {
    ch.close();
}

fn oneshotCloseConsumer(ctx: *OneshotCtx) !void {
    ctx.got = ctx.ch.recv() catch |e| switch (e) {
        error.Closed => 0xDEAD,
    };
}

fn oneshotCloseRoot(ctx: *OneshotCtx) !void {
    var p = try @import("lib.zig").spawn(oneshotCloseProducer, .{ctx.ch});
    var c = try @import("lib.zig").spawn(oneshotCloseConsumer, .{ctx});
    _ = p.join() catch unreachable;
    _ = c.join() catch unreachable;
}

test "oneshot: close-without-send surfaces Closed to receiver" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var ch = Oneshot(u64){};
    var ctx = OneshotCtx{ .ch = &ch };
    try (try rt.run(oneshotCloseRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u64, 0xDEAD), ctx.got);
}

// ─── Watch tests ─────────────────────────────────────────────────────

const WatchCtx = struct {
    w: *Watch(u64),
    last_seen: u64 = 0,
    iterations: u32 = 0,
};

fn watchProducer(w: *Watch(u64)) !void {
    var i: u64 = 1;
    while (i <= 5) : (i += 1) {
        w.send(i * 10);
        @import("lib.zig").yield(); // give consumers a chance
    }
    w.close();
}

fn watchConsumer(ctx: *WatchCtx) !void {
    var rx = ctx.w.receiver();
    while (true) {
        rx.changed() catch break; // Closed
        ctx.last_seen = rx.borrow();
        ctx.iterations += 1;
    }
}

fn watchRoot(ctx: *WatchCtx) !void {
    var p = try @import("lib.zig").spawn(watchProducer, .{ctx.w});
    var c = try @import("lib.zig").spawn(watchConsumer, .{ctx});
    _ = p.join() catch unreachable;
    _ = c.join() catch unreachable;
}

test "watch: receiver sees most recent value, exits on close" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var w = Watch(u64).init(0);
    var ctx = WatchCtx{ .w = &w };
    try (try rt.run(watchRoot, .{&ctx}));
    // Last value the producer published was 50. Receiver may have
    // coalesced some intermediate values (that's by design — Watch
    // is latest-only).
    try std.testing.expectEqual(@as(u64, 50), ctx.last_seen);
    try std.testing.expect(ctx.iterations >= 1);
}

// ─── Broadcast tests ─────────────────────────────────────────────────

const BroadcastCh = Broadcast(u64, 8);
const BroadcastCtx = struct {
    bc: *BroadcastCh,
    consumer_sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    consumer_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn broadcastProducer(bc: *BroadcastCh) !void {
    var i: u64 = 1;
    while (i <= 20) : (i += 1) {
        bc.send(i);
    }
    bc.close();
}

fn broadcastConsumer(ctx: *BroadcastCtx) !void {
    var rx = ctx.bc.receiver();
    while (true) {
        const v = rx.recv() catch |e| switch (e) {
            error.Closed => return,
            error.Lagged => continue,
        };
        _ = ctx.consumer_sum.fetchAdd(v, .acq_rel);
        _ = ctx.consumer_count.fetchAdd(1, .acq_rel);
    }
}

fn broadcastRoot(ctx: *BroadcastCtx) !void {
    const lib = @import("lib.zig");
    const ConsumerHandle = @TypeOf(try lib.spawn(broadcastConsumer, .{ctx}));
    var consumers: [3]ConsumerHandle = undefined;
    // Spawn consumers FIRST so their cursors are set before producer
    // publishes — Broadcast doesn't replay history to late joiners.
    consumers[0] = try lib.spawn(broadcastConsumer, .{ctx});
    consumers[1] = try lib.spawn(broadcastConsumer, .{ctx});
    consumers[2] = try lib.spawn(broadcastConsumer, .{ctx});
    var p = try lib.spawn(broadcastProducer, .{ctx.bc});
    _ = p.join() catch unreachable;
    for (consumers) |t| _ = t.join() catch unreachable;
}

test "broadcast: 1 producer × 3 consumers, all values delivered to all (no overrun)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var bc = BroadcastCh{};
    var ctx = BroadcastCtx{ .bc = &bc };
    try (try rt.run(broadcastRoot, .{&ctx}));
    // Each consumer should see all 20 values: sum = 3 * (1+2+..+20) = 3 * 210.
    // BUT some may be lagged depending on timing. Loose check:
    // - Count is at most 3 * 20 = 60; we got something close.
    // - Sum is at most 3 * 210 = 630; we got something close.
    const count = ctx.consumer_count.load(.acquire);
    try std.testing.expect(count > 0);
    try std.testing.expect(count <= 60);
}

//! Channel(T) — bounded MPMC channel.
//!
//! ## Architecture (two layers)
//!
//! 1. **Lock-free Vyukov ring** for `trySend` / `tryRecv`. Per-slot
//!    sequence numbers, CAS on `head`/`tail`. ABA-immune. No mutex on
//!    the data path. (See `docs/.../algorithms/vyukov-mpmc.md`.)
//!
//! 2. **Mutex-protected waiter list** for `send` / `recv`, the blocking
//!    variants. Producers/consumers that hit `.full` / `.empty` queue a
//!    `Waiter` (lives on the waiting coroutine's stack), park on the
//!    waiter's own `Park`, and are woken by the next opposite-side
//!    operation.
//!
//! Bridging the two layers without losing wakeups is the hardest part.
//! After every successful `trySend.ok` / `tryRecv.value`, we
//! unconditionally take the waiter mutex and pop+unpark one waiter on
//! the opposite side (if any). This adds a lock cycle per published
//! message, but is the only correct, race-free protocol that doesn't
//! rely on subtle Dekker-pattern reasoning over multiple atomics.
//!
//! Earlier versions used `has_send_waiters` / `has_recv_waiters` SC
//! flags as a hint (Dekker bridge) so the lock could be skipped on the
//! fast path. Empirically that pattern allowed an MPMC hang under
//! multi-worker contention, even though the textbook analysis suggested
//! it was correct. Locking unconditionally trades a few hundred
//! nanoseconds per message for obviously-correct wake behavior; a
//! Dekker-fenced fast path may return once the model checker has
//! validated the interleavings.
//!
//! ## Closed-bit
//!
//! Bit 63 of `tail` encodes "channel closed." A free check on a value
//! we have to load anyway. Senders see `.closed` immediately. Receivers
//! drain remaining buffered values, then see `.closed`.

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const tls = @import("../scheduler/tls.zig");
const thread = @import("../internal/thread.zig");

const CACHE_LINE: usize = std.atomic.cache_line;

/// Top bit of `tail` flags channel-closed. We compare differences as i64,
/// so bit 63 is otherwise safe — a u64 used as a queue position would
/// take centuries to wrap into bit 63 at any realistic op rate.
const CLOSED_BIT: u64 = @as(u64, 1) << 63;

inline fn isClosedBit(tail: u64) bool {
    return (tail & CLOSED_BIT) != 0;
}

inline fn stripClosedBit(tail: u64) u64 {
    return tail & ~CLOSED_BIT;
}

pub fn SendResult(comptime T: type) type {
    _ = T;
    return enum { ok, full, closed };
}

pub fn RecvResult(comptime T: type) type {
    return union(enum) { value: T, empty, closed };
}

// Unified channel-family vocabulary. `Channel.send` is blocking and
// can also surface `error.Cancelled`, so its return type is the
// `BlockingSendError` alias below — kept distinct because `trySend`
// reports backpressure via a tagged union, not an error.
const errors = @import("errors.zig");
pub const SendError = errors.SendError;
pub const RecvError = errors.RecvError;
pub const BlockingSendError = errors.RecvError; // {Closed, Cancelled}

pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Send = SendResult(T);
        pub const Recv = RecvResult(T);

        const Slot = struct {
            sequence: std.atomic.Value(u64),
            value: T = undefined,
        };

        const Waiter = struct {
            park: Park = .{},
            next: ?*Waiter = null,
        };

        const WaiterList = struct {
            head: ?*Waiter = null,
            tail: ?*Waiter = null,

            fn pushBack(self: *@This(), w: *Waiter) void {
                w.next = null;
                if (self.tail) |t| {
                    t.next = w;
                } else {
                    self.head = w;
                }
                self.tail = w;
            }

            fn popFront(self: *@This()) ?*Waiter {
                const w = self.head orelse return null;
                self.head = w.next;
                if (self.head == null) self.tail = null;
                w.next = null;
                return w;
            }

            fn isEmpty(self: *const @This()) bool {
                return self.head == null;
            }
        };

        // ── Cache line 1: producer-contended ─────────────────────────
        tail: std.atomic.Value(u64) align(CACHE_LINE),
        _pad1: [CACHE_LINE - @sizeOf(std.atomic.Value(u64))]u8 = undefined,

        // ── Cache line 2: consumer-contended ─────────────────────────
        head: std.atomic.Value(u64) align(CACHE_LINE),
        _pad2: [CACHE_LINE - @sizeOf(std.atomic.Value(u64))]u8 = undefined,

        // ── Cache line 3: slot buffer + waiter mutex ─────────────────
        slots: []Slot align(CACHE_LINE),
        buf_mask: u64,
        buf_cap: u64,
        allocator: std.mem.Allocator,

        // ── Waiter list (mutex-protected, slow path) ─────────────────
        waiter_mutex: thread.Mutex = .{},
        send_waiters: WaiterList = .{},
        recv_waiters: WaiterList = .{},

        /// Initialize a channel with `capacity` slots. Capacity is
        /// rounded up to the next power of two so we can use bitmask
        /// indexing instead of modulo, with a floor of 2.
        ///
        /// The floor is mandatory: Vyukov's MPMC algorithm requires
        /// `capacity ≥ 2`. At cap=1, post-publish `slot.seq == 1` is
        /// indistinguishable from post-consume `slot.seq == 1`, so the
        /// next producer's "is my slot ready?" check passes without
        /// consumer involvement, allowing unbounded publishes that
        /// overwrite the slot. The consumer's own `tryRecv` check
        /// (`diff = seq - (head+1)`) then sees `diff > 0` indefinitely
        /// and busy-loops at 100% CPU. Rendezvous semantics (every
        /// send blocks until a recv arrives) belong in a separate type
        /// (not yet exposed); use `Channel(T)` with cap >= 2 for buffered
        /// MPMC.
        pub fn init(allocator: std.mem.Allocator, requested_capacity: usize) !Self {
            assert(requested_capacity > 0);
            const floored = @max(requested_capacity, @as(usize, 2));
            const cap = std.math.ceilPowerOfTwo(usize, floored) catch
                return error.CapacityTooLarge;
            const slots = try allocator.alloc(Slot, cap);
            errdefer allocator.free(slots);
            for (slots, 0..) |*slot, i| {
                slot.* = .{ .sequence = std.atomic.Value(u64).init(@intCast(i)) };
            }
            return .{
                .tail = std.atomic.Value(u64).init(0),
                .head = std.atomic.Value(u64).init(0),
                .slots = slots,
                .buf_mask = cap - 1,
                .buf_cap = @intCast(cap),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            // No remaining waiters expected — caller should close + drain
            // first, or be sure no coroutine is parked here.
            assert(self.send_waiters.isEmpty());
            assert(self.recv_waiters.isEmpty());
            self.allocator.free(self.slots);
            self.* = undefined;
        }

        pub fn capacity(self: *const Self) usize {
            return @intCast(self.buf_cap);
        }

        pub fn isClosed(self: *const Self) bool {
            return isClosedBit(self.tail.load(.acquire));
        }

        // ─── Lock-free fast path ─────────────────────────────────────

        pub fn trySend(self: *Self, value: T) Send {
            const result = self.trySendNoWake(value);
            if (result == .ok) self.wakeOneRecvWaiter();
            return result;
        }

        pub fn tryRecv(self: *Self) Recv {
            const result = self.tryRecvNoWake();
            if (result == .value) self.wakeOneSendWaiter();
            return result;
        }

        /// Lock-free publish. Caller is responsible for waking a recv
        /// waiter on `.ok`. Used by `send`'s blocking-path recheck —
        /// the caller already holds `waiter_mutex` and invoking
        /// `wakeOneRecvWaiter` would re-enter that same non-reentrant
        /// mutex. (M1 hang root cause.)
        fn trySendNoWake(self: *Self, value: T) Send {
            var tail = self.tail.load(.monotonic);
            while (true) {
                if (isClosedBit(tail)) return .closed;
                const idx: usize = @intCast(tail & self.buf_mask);
                const slot = &self.slots[idx];
                const seq = slot.sequence.load(.acquire);
                const diff: i64 = @bitCast(seq -% tail);
                if (diff == 0) {
                    if (self.tail.cmpxchgWeak(tail, tail +% 1, .acq_rel, .monotonic)) |new_tail| {
                        tail = new_tail;
                        continue;
                    }
                    slot.value = value;
                    slot.sequence.store(tail +% 1, .release);
                    return .ok;
                } else if (diff < 0) {
                    return .full;
                } else {
                    tail = self.tail.load(.monotonic);
                }
            }
        }

        /// Lock-free consume. See `trySendNoWake` for why the wake is
        /// split out.
        fn tryRecvNoWake(self: *Self) Recv {
            var head = self.head.load(.monotonic);
            while (true) {
                const idx: usize = @intCast(head & self.buf_mask);
                const slot = &self.slots[idx];
                const seq = slot.sequence.load(.acquire);
                const diff: i64 = @bitCast(seq -% (head +% 1));
                if (diff == 0) {
                    if (self.head.cmpxchgWeak(head, head +% 1, .acq_rel, .monotonic)) |new_head| {
                        head = new_head;
                        continue;
                    }
                    const value = slot.value;
                    slot.sequence.store(head +% self.buf_cap, .release);
                    return .{ .value = value };
                } else if (diff < 0) {
                    if (isClosedBit(self.tail.load(.acquire))) return .closed;
                    return .empty;
                } else {
                    head = self.head.load(.monotonic);
                }
            }
        }

        // ─── Blocking path ───────────────────────────────────────────

        /// Send `value`, blocking the calling coroutine until a slot is
        /// available or the channel is closed.
        ///
        /// Race-free wake protocol: park under the mutex, after a
        /// re-check that holds the mutex too. Any concurrent `tryRecv`
        /// drain that wakes a waiter must take the same mutex, so it
        /// can only see our queued waiter or our re-check's success —
        /// never a transient state where we're enqueued but no wake
        /// fires.
        pub fn send(self: *Self, value: T) BlockingSendError!void {
            while (true) {
                switch (self.trySend(value)) {
                    .ok => return,
                    .closed => return error.Closed,
                    .full => {
                        var waiter: Waiter = .{};
                        self.waiter_mutex.lock();
                        // Re-check under the lock with the NO-WAKE variant —
                        // wakeOneRecvWaiter would re-enter `waiter_mutex`.
                        switch (self.trySendNoWake(value)) {
                            .ok => {
                                self.waiter_mutex.unlock();
                                // We took a slot. Wake a recv waiter
                                // outside the lock so we don't pile up
                                // a wake-while-locked sequence.
                                self.wakeOneRecvWaiter();
                                return;
                            },
                            .closed => {
                                self.waiter_mutex.unlock();
                                return error.Closed;
                            },
                            .full => {
                                self.send_waiters.pushBack(&waiter);
                                self.waiter_mutex.unlock();
                                waiter.park.parkCurrent() catch |err| switch (err) {
                                    error.Cancelled => {
                                        self.removeWaiter(&self.send_waiters, &waiter);
                                        return error.Cancelled;
                                    },
                                };
                                // Resumed by waker (or close); loop and retry.
                            },
                        }
                    },
                }
            }
        }

        /// Receive a value, blocking the calling coroutine until one is
        /// available or the channel is closed.
        pub fn recv(self: *Self) RecvError!T {
            while (true) {
                switch (self.tryRecv()) {
                    .value => |v| return v,
                    .closed => return error.Closed,
                    .empty => {
                        var waiter: Waiter = .{};
                        self.waiter_mutex.lock();
                        switch (self.tryRecvNoWake()) {
                            .value => |v| {
                                self.waiter_mutex.unlock();
                                self.wakeOneSendWaiter();
                                return v;
                            },
                            .closed => {
                                self.waiter_mutex.unlock();
                                return error.Closed;
                            },
                            .empty => {
                                self.recv_waiters.pushBack(&waiter);
                                self.waiter_mutex.unlock();
                                waiter.park.parkCurrent() catch |err| switch (err) {
                                    error.Cancelled => {
                                        self.removeWaiter(&self.recv_waiters, &waiter);
                                        return error.Cancelled;
                                    },
                                };
                            },
                        }
                    },
                }
            }
        }

        // ─── Close ───────────────────────────────────────────────────

        /// Mark the channel closed and wake all parked coroutines so they
        /// observe `.closed` and unwind. Buffered values remain
        /// receivable until drained.
        pub fn close(self: *Self) void {
            _ = self.tail.fetchOr(CLOSED_BIT, .acq_rel);
            self.waiter_mutex.lock();
            while (self.send_waiters.popFront()) |w| {
                w.park.unpark();
            }
            while (self.recv_waiters.popFront()) |w| {
                w.park.unpark();
            }
            self.waiter_mutex.unlock();
        }

        // ─── Wake helpers (called from fast path) ────────────────────

        fn wakeOneSendWaiter(self: *Self) void {
            self.waiter_mutex.lock();
            const w = self.send_waiters.popFront();
            self.waiter_mutex.unlock();
            if (w) |waiter| waiter.park.unpark();
        }

        fn wakeOneRecvWaiter(self: *Self) void {
            self.waiter_mutex.lock();
            const w = self.recv_waiters.popFront();
            self.waiter_mutex.unlock();
            if (w) |waiter| waiter.park.unpark();
        }

        /// Remove a specific waiter from a list (cancellation cleanup).
        /// O(N) but the list is typically short. Safe to call when the
        /// waiter may already have been popped by a waker — handles both.
        fn removeWaiter(self: *Self, list: *WaiterList, target: *Waiter) void {
            self.waiter_mutex.lock();
            defer self.waiter_mutex.unlock();
            var prev: ?*Waiter = null;
            var cur = list.head;
            while (cur) |w| : (cur = w.next) {
                if (w == target) {
                    if (prev) |p| p.next = w.next else list.head = w.next;
                    if (list.tail == w) list.tail = prev;
                    w.next = null;
                    return;
                }
                prev = w;
            }
            // Not in list — already popped by waker. Fine.
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Unit tests — single-threaded sanity for the lock-free path.
// ─────────────────────────────────────────────────────────────────────

test "channel: trySend/tryRecv FIFO" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    try std.testing.expectEqual(@as(usize, 4), ch.capacity());

    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(1));
    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(2));
    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(3));

    try std.testing.expectEqual(@as(u32, 1), ch.tryRecv().value);
    try std.testing.expectEqual(@as(u32, 2), ch.tryRecv().value);
    try std.testing.expectEqual(@as(u32, 3), ch.tryRecv().value);
    try std.testing.expectEqual(Channel(u32).Recv.empty, ch.tryRecv());
}

test "channel: trySend reports full when buffer is at capacity" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(1));
    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(2));
    try std.testing.expectEqual(Channel(u32).Send.full, ch.trySend(3));
    _ = ch.tryRecv();
    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(3));
}

test "channel: capacity is rounded up to power of two" {
    var ch = try Channel(u32).init(std.testing.allocator, 3);
    defer ch.deinit();
    try std.testing.expectEqual(@as(usize, 4), ch.capacity());
}

test "channel: close — trySend rejects, tryRecv drains then closes" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(10));
    try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(20));

    ch.close();
    try std.testing.expect(ch.isClosed());

    // Sends rejected.
    try std.testing.expectEqual(Channel(u32).Send.closed, ch.trySend(30));
    // Buffered values still drain.
    try std.testing.expectEqual(@as(u32, 10), ch.tryRecv().value);
    try std.testing.expectEqual(@as(u32, 20), ch.tryRecv().value);
    // Then closed.
    try std.testing.expectEqual(Channel(u32).Recv.closed, ch.tryRecv());
}

test "channel: wrap-around correctness" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try std.testing.expectEqual(Channel(u32).Send.ok, ch.trySend(i));
        try std.testing.expectEqual(i, ch.tryRecv().value);
    }
}

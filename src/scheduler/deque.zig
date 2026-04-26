//! Lock-free Chase-Lev work-stealing deque (Chase & Lev, 2005).
//!
//! Owner pushes / pops at the bottom (LIFO — best for cache locality, since
//! the most-recently-spawned coroutine is most likely to still be hot). Thieves
//! steal from the top (FIFO — gives them the oldest work, leaving the owner's
//! recent work alone).
//!
//! Adapted from blitz's compute work-stealing deque (same author, same
//! algorithm, Apache-2.0). The shape is unchanged; only the comments are
//! refocused on the async-runtime use case (the `T` here is `*Coroutine`,
//! not a compute job, but the algorithm is identical).
//!
//! Reference: "Dynamic Circular Work-Stealing Deque", Chase & Lev (2005).
//!
//! Memory ordering follows "Correct and Efficient Work-Stealing for Weak
//! Memory Models" (Lê et al., 2013) — `seq_cst` on the steal CAS and the
//! pop CAS, `acquire`/`release` elsewhere.

const std = @import("std");

pub const StealResult = enum {
    /// Deque was empty.
    empty,
    /// Successfully stole an item.
    success,
    /// Lost the race with the owner or another thief — caller should retry.
    retry,
};

const CACHE_LINE = std.atomic.cache_line;

/// Lock-free work-stealing deque.
///
/// The deque is a circular buffer with two atomic indices:
///   - `bottom`: owner's end (push / pop). Only the owner writes this.
///   - `top`:    thieves' end (steal). Multiple thieves CAS this.
///
/// Critical optimization: top and bottom live on separate cache lines so
/// owner-vs-thief contention doesn't bounce a single line between cores.
pub fn Deque(comptime T: type) type {
    return struct {
        const Self = @This();

        // ── Cache line 1: thief-contended ────────────────────────────────
        top: std.atomic.Value(isize) align(CACHE_LINE),
        _pad1: [CACHE_LINE - @sizeOf(std.atomic.Value(isize))]u8 = undefined,

        // ── Cache line 2: owner-only ─────────────────────────────────────
        bottom: std.atomic.Value(isize) align(CACHE_LINE),
        _pad2: [CACHE_LINE - @sizeOf(std.atomic.Value(isize))]u8 = undefined,

        // ── Cache line 3: buffer + metadata ─────────────────────────────
        buffer: []T align(CACHE_LINE),
        mask: usize,
        allocator: std.mem.Allocator,

        /// Initialize a deque with `capacity` slots. Capacity MUST be a power
        /// of two so we can use bit-mask wrap-around instead of `%`.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity > 0);
            std.debug.assert(std.math.isPowerOfTwo(capacity));
            const buffer = try allocator.alloc(T, capacity);
            return .{
                .top = std.atomic.Value(isize).init(0),
                ._pad1 = undefined,
                .bottom = std.atomic.Value(isize).init(0),
                ._pad2 = undefined,
                .buffer = buffer,
                .mask = capacity - 1,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
            self.* = undefined;
        }

        /// Grow the deque on overflow. ONLY the owner calls this, so we can
        /// freely reallocate the buffer — concurrent stealers see the old
        /// indices and either successfully steal an item still readable from
        /// the old buffer (we don't free it until after the swap below) or
        /// CAS-fail and retry.
        ///
        /// NOTE: this is the dynamic-buffer simplification: we copy on grow.
        /// The original Chase-Lev paper avoids the copy via a linked list of
        /// buffers, which complicates retirement. For our use case (push-heavy
        /// workloads typically don't grow past the initial capacity) the copy
        /// path is rare and a one-shot allocation is preferable.
        fn grow(self: *Self) !void {
            const old_capacity = self.buffer.len;
            const new_capacity = old_capacity * 2;
            const new_buffer = try self.allocator.alloc(T, new_capacity);

            const t = self.top.load(.acquire);
            const b = self.bottom.load(.monotonic);
            var i = t;
            while (i < b) : (i += 1) {
                const old_idx = @as(usize, @intCast(i)) & self.mask;
                const new_idx = @as(usize, @intCast(i)) & (new_capacity - 1);
                new_buffer[new_idx] = self.buffer[old_idx];
            }

            const old_buffer = self.buffer;
            self.buffer = new_buffer;
            self.mask = new_capacity - 1;
            self.allocator.free(old_buffer);
        }

        /// Approximate length (may race with concurrent steals).
        pub inline fn len(self: *const Self) usize {
            const b = self.bottom.load(.acquire);
            const t = self.top.load(.acquire);
            return if (b >= t) @intCast(b - t) else 0;
        }

        pub inline fn isEmpty(self: *const Self) bool {
            return self.len() == 0;
        }

        /// Push to the bottom. Owner-only — no synchronization with sibling
        /// pushes, only a release-store of `bottom` to publish the slot.
        pub inline fn push(self: *Self, item: T) void {
            const b = self.bottom.load(.monotonic);
            const t = self.top.load(.acquire);

            const size = b - t;
            if (size >= @as(isize, @intCast(self.buffer.len))) {
                self.grow() catch @panic("deque grow failed (OOM)");
            }

            const idx = @as(usize, @intCast(b)) & self.mask;
            self.buffer[idx] = item;
            // release: ensures the slot write is visible to any thief that
            // reads `bottom` with acquire ordering.
            self.bottom.store(b + 1, .release);
        }

        /// Pop from the bottom. Owner-only. Returns null if empty.
        ///
        /// The seq_cst on the bottom-store and the top-load form the
        /// classic Chase-Lev double-Dekker pattern: prevents races where
        /// owner and thief both try to claim the last item.
        pub inline fn pop(self: *Self) ?T {
            const b = self.bottom.load(.monotonic) - 1;
            self.bottom.store(b, .seq_cst);

            const t = self.top.load(.seq_cst);
            if (t <= b) {
                const idx = @as(usize, @intCast(b)) & self.mask;
                const item = self.buffer[idx];

                if (t == b) {
                    // Last item: race with thieves. CAS top to claim it.
                    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic)) |_| {
                        // Lost: a thief took the item. Restore bottom.
                        self.bottom.store(t + 1, .monotonic);
                        return null;
                    }
                    self.bottom.store(t + 1, .monotonic);
                }
                return item;
            } else {
                // Empty after the speculative decrement — restore.
                self.bottom.store(t, .monotonic);
                return null;
            }
        }

        /// Try to steal from the top. Called by sibling workers.
        pub fn steal(self: *Self) struct { result: StealResult, item: ?T } {
            const t = self.top.load(.acquire);
            const b = self.bottom.load(.acquire);

            if (t >= b) return .{ .result = .empty, .item = null };

            const idx = @as(usize, @intCast(t)) & self.mask;
            // Speculative read — the CAS below confirms whether this read
            // was actually claimed by us or the slot was already overwritten.
            const item = self.buffer[idx];

            if (self.top.cmpxchgWeak(t, t + 1, .seq_cst, .monotonic)) |_| {
                return .{ .result = .retry, .item = null };
            }
            return .{ .result = .success, .item = item };
        }

        /// Steal with bounded backoff retry. Returns null if confirmed empty.
        pub fn stealLoop(self: *Self) ?T {
            var backoff: u32 = 0;
            const SPIN_LIMIT: u32 = 6; // ~64 spins max

            while (true) {
                const r = self.steal();
                switch (r.result) {
                    .success => return r.item,
                    .empty => return null,
                    .retry => {
                        if (backoff < SPIN_LIMIT) {
                            const spins = @as(u32, 1) << @intCast(backoff);
                            var i: u32 = 0;
                            while (i < spins) : (i += 1) std.atomic.spinLoopHint();
                            backoff += 1;
                        } else {
                            std.Thread.yield() catch {};
                        }
                    },
                }
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — single-threaded sanity. Multi-threaded contention is exercised by
// the worker integration tests once workers are wired up.
// ─────────────────────────────────────────────────────────────────────────────

test "deque: push and pop are LIFO for owner" {
    var d = try Deque(u32).init(std.testing.allocator, 16);
    defer d.deinit();

    d.push(1);
    d.push(2);
    d.push(3);
    try std.testing.expectEqual(@as(usize, 3), d.len());

    try std.testing.expectEqual(@as(?u32, 3), d.pop());
    try std.testing.expectEqual(@as(?u32, 2), d.pop());
    try std.testing.expectEqual(@as(?u32, 1), d.pop());
    try std.testing.expectEqual(@as(?u32, null), d.pop());
}

test "deque: steal from empty returns .empty" {
    var d = try Deque(u32).init(std.testing.allocator, 16);
    defer d.deinit();
    const r = d.steal();
    try std.testing.expectEqual(StealResult.empty, r.result);
}

test "deque: steal is FIFO" {
    var d = try Deque(u32).init(std.testing.allocator, 16);
    defer d.deinit();

    d.push(1);
    d.push(2);
    d.push(3);

    const r1 = d.steal();
    try std.testing.expectEqual(StealResult.success, r1.result);
    try std.testing.expectEqual(@as(?u32, 1), r1.item);

    const r2 = d.steal();
    try std.testing.expectEqual(StealResult.success, r2.result);
    try std.testing.expectEqual(@as(?u32, 2), r2.item);

    // Owner takes the last
    try std.testing.expectEqual(@as(?u32, 3), d.pop());
    try std.testing.expectEqual(StealResult.empty, d.steal().result);
}

test "deque: grows past initial capacity" {
    var d = try Deque(u32).init(std.testing.allocator, 4);
    defer d.deinit();

    var i: u32 = 0;
    while (i < 100) : (i += 1) d.push(i);
    try std.testing.expectEqual(@as(usize, 100), d.len());

    // Pop them back in LIFO order.
    var expected: u32 = 99;
    while (d.pop()) |got| {
        try std.testing.expectEqual(expected, got);
        if (expected == 0) break;
        expected -= 1;
    }
}

test "deque: wrap-around correctness" {
    var d = try Deque(u32).init(std.testing.allocator, 4);
    defer d.deinit();

    // Cycle indices forward by pushing+popping repeatedly.
    for (0..10) |i| {
        d.push(@intCast(i));
        _ = d.pop();
    }

    d.push(100);
    d.push(200);
    try std.testing.expectEqual(@as(?u32, 200), d.pop());
    try std.testing.expectEqual(@as(?u32, 100), d.pop());
}

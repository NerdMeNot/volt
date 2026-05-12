//! Chase-Lev lock-free work-stealing deque.
//!
//! Owner pushes/pops on the bottom end (LIFO from owner's view).
//! Thieves steal from the top end (FIFO of stealable items).
//!
//! Critical layout: top (thief-contended) and bottom (owner-exclusive)
//! sit on separate cache lines to avoid false sharing on every push/pop.
//!
//! Memory ordering follows Le et al.'s "Correct and Efficient
//! Work-Stealing for Weak Memory Models" (PPoPP'13):
//!   * push: bottom store with release
//!   * pop: bottom decrement seq_cst, top load seq_cst, last-item CAS seq_cst
//!   * steal: top acquire, bottom acquire, CAS seq_cst
//!
//! Adapted from `blitz/src/Deque.zig` (compute-focused workstealing
//! pool). Same algorithm; Volt-side naming and a few ergonomic
//! tweaks for the Coroutine use case.

const std = @import("std");

const CACHE_LINE = std.atomic.cache_line;

pub const StealResult = enum {
    empty,
    success,
    /// CAS lost — caller should retry (or back off).
    retry,
};

pub fn Deque(comptime T: type) type {
    return struct {
        const Self = @This();

        // Cache line 1 — thief-contended.
        top: std.atomic.Value(isize) align(CACHE_LINE),
        _pad1: [CACHE_LINE - @sizeOf(std.atomic.Value(isize))]u8 = undefined,

        // Cache line 2 — owner-exclusive.
        bottom: std.atomic.Value(isize) align(CACHE_LINE),
        _pad2: [CACHE_LINE - @sizeOf(std.atomic.Value(isize))]u8 = undefined,

        // Cache line 3 — buffer metadata.
        buffer: []T align(CACHE_LINE),
        mask: usize,
        allocator: std.mem.Allocator,

        /// Capacity must be a power of 2.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity > 0);
            std.debug.assert(std.math.isPowerOfTwo(capacity));
            const buffer = try allocator.alloc(T, capacity);
            return .{
                .top = std.atomic.Value(isize).init(0),
                .bottom = std.atomic.Value(isize).init(0),
                .buffer = buffer,
                .mask = capacity - 1,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        /// Approximate length. Racy; only use for diagnostics or hints.
        pub inline fn len(self: *const Self) usize {
            const b = self.bottom.load(.acquire);
            const t = self.top.load(.acquire);
            return if (b >= t) @intCast(b - t) else 0;
        }

        pub inline fn isEmpty(self: *const Self) bool {
            return self.len() == 0;
        }

        /// Owner only. Grows the buffer if full.
        ///
        /// Note: grow currently frees the old buffer immediately. If a
        /// thief is mid-`steal` reading from the old buffer when the
        /// owner grows, the thief's `buffer[idx]` read is technically
        /// a use-after-free. This is the same caveat as blitz's
        /// implementation and the original Chase-Lev paper without
        /// epoch reclamation. For Volt's expected steady-state load
        /// (queue rarely overflows after warmup), this is acceptable.
        /// A v2.x hardening can swap in hazard pointers / epochs.
        pub fn push(self: *Self, item: T) void {
            const b = self.bottom.load(.monotonic);
            const t = self.top.load(.acquire);
            const size = b - t;
            if (size >= @as(isize, @intCast(self.buffer.len))) {
                self.grow() catch @panic("Deque grow OOM");
            }
            const idx = @as(usize, @intCast(b)) & self.mask;
            self.buffer[idx] = item;
            // Release: item-write happens-before bottom-store as seen
            // by stealers.
            self.bottom.store(b + 1, .release);
        }

        /// Owner only. Returns null if empty.
        pub fn pop(self: *Self) ?T {
            // Decrement bottom first (seq_cst — must be visible to
            // stealers before we observe their top).
            const b = self.bottom.load(.monotonic) - 1;
            self.bottom.store(b, .seq_cst);
            const t = self.top.load(.seq_cst);
            if (t <= b) {
                const idx = @as(usize, @intCast(b)) & self.mask;
                const item = self.buffer[idx];
                if (t == b) {
                    // Last item — race with stealers. CAS for ownership.
                    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic)) |_| {
                        // Lost the race — thief got it.
                        self.bottom.store(t + 1, .monotonic);
                        return null;
                    }
                    self.bottom.store(t + 1, .monotonic);
                }
                return item;
            } else {
                // Empty (we observed t > b after our decrement, meaning
                // a stealer claimed it). Restore bottom to top.
                self.bottom.store(t, .monotonic);
                return null;
            }
        }

        /// Any thread. Single attempt; returns retry on CAS loss.
        pub fn steal(self: *Self) struct { result: StealResult, item: ?T } {
            const t = self.top.load(.acquire);
            const b = self.bottom.load(.acquire);
            if (t >= b) return .{ .result = .empty, .item = null };
            const idx = @as(usize, @intCast(t)) & self.mask;
            const item = self.buffer[idx];
            if (self.top.cmpxchgWeak(t, t + 1, .seq_cst, .monotonic)) |_| {
                return .{ .result = .retry, .item = null };
            }
            return .{ .result = .success, .item = item };
        }

        /// Steal with exponential-backoff retry, returning null on empty.
        pub fn stealLoop(self: *Self) ?T {
            var backoff: u32 = 0;
            const SPIN_LIMIT: u32 = 6;
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

        fn grow(self: *Self) !void {
            const old_cap = self.buffer.len;
            const new_cap = old_cap * 2;
            const new_buffer = try self.allocator.alloc(T, new_cap);
            const t = self.top.load(.acquire);
            const b = self.bottom.load(.monotonic);
            var i = t;
            while (i < b) : (i += 1) {
                const old_idx = @as(usize, @intCast(i)) & self.mask;
                const new_idx = @as(usize, @intCast(i)) & (new_cap - 1);
                new_buffer[new_idx] = self.buffer[old_idx];
            }
            const old_buffer = self.buffer;
            self.buffer = new_buffer;
            self.mask = new_cap - 1;
            self.allocator.free(old_buffer);
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "Deque: push then pop (LIFO from owner)" {
    var d = try Deque(u32).init(std.testing.allocator, 16);
    defer d.deinit();
    d.push(1);
    d.push(2);
    d.push(3);
    try std.testing.expectEqual(@as(?u32, 3), d.pop());
    try std.testing.expectEqual(@as(?u32, 2), d.pop());
    try std.testing.expectEqual(@as(?u32, 1), d.pop());
    try std.testing.expectEqual(@as(?u32, null), d.pop());
}

test "Deque: steal FIFO order" {
    var d = try Deque(u32).init(std.testing.allocator, 16);
    defer d.deinit();
    d.push(1);
    d.push(2);
    d.push(3);
    const r1 = d.steal();
    try std.testing.expectEqual(StealResult.success, r1.result);
    try std.testing.expectEqual(@as(?u32, 1), r1.item);
    const r2 = d.steal();
    try std.testing.expectEqual(@as(?u32, 2), r2.item);
    try std.testing.expectEqual(@as(?u32, 3), d.pop());
}

test "Deque: wrap-around with grow" {
    var d = try Deque(u32).init(std.testing.allocator, 2);
    defer d.deinit();
    var i: u32 = 0;
    while (i < 10) : (i += 1) d.push(i);
    var seen: u32 = 0;
    while (d.pop()) |_| seen += 1;
    try std.testing.expectEqual(@as(u32, 10), seen);
}

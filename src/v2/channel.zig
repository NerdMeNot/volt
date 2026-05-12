//! v2 Channels.
//!
//! `Spsc(T, cap)` — comptime-specialized single-producer single-consumer
//! ring buffer. "SPSC" means the channel takes ONE producer and ONE
//! consumer (a *placement* constraint), NOT "single thread." The
//! producer and consumer can be coroutines running on any two OS
//! threads. The atomics + release/acquire fences carry the data
//! correctly across the thread boundary.
//!
//! Memory model:
//!   - `ring[i]`: producer writes; `head` release-store carries the
//!     write to any consumer doing `head` acquire-load.
//!   - `head`: producer-only writer, atomic for cross-thread visibility.
//!   - `tail`: consumer-only writer, atomic for cross-thread visibility.
//!   - `head` and `tail` on separate cache lines (no false sharing).
//!   - `recv_waiter` / `send_waiter`: atomic slots for the parked
//!     opposite-side coroutine. Wait protocol uses the standard
//!     "store waiter; re-check condition; park or undo" pattern, which
//!     closes the race with the matching side under multi-thread
//!     interleaving (see WaitGroup.wait for the same pattern).
//!
//! `Mpmc(T, cap)` — TODO (#150). Vyukov ring or similar for the
//! general multi-producer / multi-consumer case.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");

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

        // Producer-owned cache line.
        head: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),
        /// Receiver coroutine waiting on empty channel (if any).
        recv_waiter: std.atomic.Value(?*coroutine.Coroutine) align(CACHE_LINE) =
            std.atomic.Value(?*coroutine.Coroutine).init(null),

        // Consumer-owned cache line.
        tail: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),
        /// Sender coroutine waiting on full channel (if any).
        send_waiter: std.atomic.Value(?*coroutine.Coroutine) align(CACHE_LINE) =
            std.atomic.Value(?*coroutine.Coroutine).init(null),

        closed: std.atomic.Value(bool) align(CACHE_LINE) = std.atomic.Value(bool).init(false),

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
                    // Wake parked receiver, if any.
                    if (self.recv_waiter.swap(null, .acq_rel)) |waiter| {
                        runtime.unpark(waiter);
                    }
                    return;
                }
                // Full — park self.
                const me = @import("current.zig").require();
                self.send_waiter.store(me, .release);
                // Re-check capacity AFTER registering, in case the
                // consumer drained between our load and our store.
                const t2 = self.tail.load(.acquire);
                if (h - t2 < cap) {
                    // Spurious — undo the registration and retry.
                    _ = self.send_waiter.swap(null, .acq_rel);
                    continue;
                }
                runtime.park();
                // Resume on unpark: loop and re-check.
            }
        }

        /// Receive a value. Blocks (parks current coroutine) if the
        /// channel is empty. Returns `error.Closed` if the channel
        /// was closed AND no buffered values remain.
        pub fn recv(self: *Self) ChannelError!T {
            while (true) {
                const t = self.tail.load(.monotonic);
                const h = self.head.load(.acquire);
                if (h != t) {
                    const v = self.ring[t & MASK];
                    self.tail.store(t + 1, .release);
                    // Wake parked sender, if any.
                    if (self.send_waiter.swap(null, .acq_rel)) |waiter| {
                        runtime.unpark(waiter);
                    }
                    return v;
                }
                if (self.closed.load(.acquire)) return error.Closed;
                // Empty — park self.
                const me = @import("current.zig").require();
                self.recv_waiter.store(me, .release);
                // Re-check, since producer may have sent between our
                // load and registration.
                const h2 = self.head.load(.acquire);
                if (h2 != t) {
                    _ = self.recv_waiter.swap(null, .acq_rel);
                    continue;
                }
                if (self.closed.load(.acquire)) {
                    _ = self.recv_waiter.swap(null, .acq_rel);
                    return error.Closed;
                }
                runtime.park();
            }
        }

        /// Close the channel. Future sends fail; pending recvs drain
        /// then fail. Wakes both waiters if present.
        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
            if (self.recv_waiter.swap(null, .acq_rel)) |waiter| {
                runtime.unpark(waiter);
            }
            if (self.send_waiter.swap(null, .acq_rel)) |waiter| {
                runtime.unpark(waiter);
            }
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
    const current_mod = @import("current.zig");
    const rt: *Runtime = @ptrCast(@alignCast(current_mod.require().runtime));
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

test "spsc: works at workers=1 (degenerate configuration)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var ch = Spsc(u64, 4){};
    var ctx = TestCtx{ .ch = &ch, .n = 1000 };
    try rt.run(spscTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u64, 499500), ctx.sum);
}

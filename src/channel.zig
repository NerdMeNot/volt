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
//! `Mpmc(T, cap)` — TODO (#150). Vyukov ring or similar for the
//! general multi-producer / multi-consumer case.

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

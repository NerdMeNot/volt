//! Oneshot(T) — single-sender, single-receiver, single-value channel.
//!
//! Zero allocations. Built directly on Park. The lightest weight wakeable
//! primitive in the channel family — useful when you just need to hand
//! one value (or one error) from producer to consumer and be done.
//!
//! ## Usage
//!
//! ```zig
//! var oneshot = Oneshot(u32){};
//! // Sender side (any worker / coroutine):
//! try oneshot.send(42);
//! // Receiver side (in a coroutine):
//! const v = try oneshot.recv();
//! ```
//!
//! ## Semantics
//!
//! - At most one `send`. Subsequent calls return `error.Closed`.
//! - At most one `recv`. The receiver consumes the value.
//! - `close` (without `send`) wakes the receiver with `error.Closed`.
//! - Single-shot: a Oneshot instance cannot be reused after a successful
//!   send/recv exchange. Construct a new one.
//!
//! ## State machine (single atomic, packed)
//!
//!   `0`              — empty (no value, no waiter, not closed)
//!   `READY` (=1)     — sender wrote `value`; receiver may consume
//!   `CLOSED` (=2)    — sender called close() before sending
//!   `coro_ptr`       — receiver is parked (low bits clear because of alignment)
//!   `coro_ptr|READY` — racing transition: sender filled the value while the
//!                      receiver was registering; the receiver fast-takes on
//!                      its post-park check
//!
//! Only ONE of READY / CLOSED can ever fire (single-sender). Concurrent
//! send + close from the same sender is a programmer error.

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const ctx_mod = @import("../coroutine/context.zig");
const event_source = @import("../coroutine/event_source.zig");
// Import the function directly (not the module as `current`) because
// this file uses `current` as a local variable name in CAS loops.
const currentCoroutine = @import("../scheduler/current.zig").currentCoroutine;
const runtime_mod = @import("../runtime.zig");

const READY: usize = 1;
const CLOSED: usize = 2;
const PTR_MASK: usize = ~@as(usize, 0b11);

// Unified channel-family vocabulary. From the second-sender's
// perspective a Oneshot that has already delivered its value is
// "closed"; we no longer split that into a separate `AlreadySent`.
const errors = @import("errors.zig");
pub const SendError = errors.SendError;
pub const RecvError = errors.RecvError;

pub fn Oneshot(comptime T: type) type {
    return struct {
        const Self = @This();

        es: event_source.EventSource = .{ .subscribe_fn = &subscribe },
        state: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        value: T = undefined,

        /// Send the value. Returns `error.Closed` if a previous
        /// send succeeded; `error.Closed` if `close` was called.
        pub fn send(self: *Self, val: T) SendError!void {
            // Snapshot the state and decide. If we see a coro pointer,
            // we still have to publish the value AND wake the parker.
            // If we see READY/CLOSED, we lose.
            const current = self.state.load(.acquire);
            if (current == READY) return error.Closed;
            if (current == CLOSED) return error.Closed;

            // Write the value first. Receiver's load is acquire-paired
            // with our release CAS below.
            self.value = val;

            // CAS state to READY (preserving any waiter pointer in the
            // upper bits — we use OR to layer READY on top of ptr).
            var observed = current;
            while (true) {
                if (observed == READY) return error.Closed;
                if (observed == CLOSED) return error.Closed;
                const new = if (observed == 0) READY else (observed | READY);
                if (self.state.cmpxchgWeak(observed, new, .acq_rel, .acquire)) |seen| {
                    observed = seen;
                    continue;
                }
                break;
            }

            // If a receiver was parked, take its pointer and schedule it.
            if (observed != 0) {
                const ptr = observed & PTR_MASK;
                const coro: *Coroutine = @ptrFromInt(ptr);
                // Clear the ptr bits so a subsequent stale read doesn't
                // see a coro_ptr|READY shape we've already drained.
                _ = self.state.fetchAnd(~PTR_MASK, .release);
                scheduleCoro(coro);
            }
        }

        /// Mark the channel closed without sending. Wakes any parked
        /// receiver, who gets `error.Closed`.
        pub fn close(self: *Self) void {
            var observed = self.state.load(.acquire);
            while (true) {
                // Already terminal.
                if (observed == READY or observed == CLOSED) return;
                if ((observed & READY) != 0) return;
                const new = if (observed == 0) CLOSED else (observed | CLOSED);
                if (self.state.cmpxchgWeak(observed, new, .acq_rel, .acquire)) |seen| {
                    observed = seen;
                    continue;
                }
                break;
            }
            if (observed != 0) {
                const ptr = observed & PTR_MASK;
                const coro: *Coroutine = @ptrFromInt(ptr);
                _ = self.state.fetchAnd(~PTR_MASK, .release);
                scheduleCoro(coro);
            }
        }

        /// Receive the value. Suspends the calling coroutine until the
        /// sender calls `send` (returns the value) or `close` (returns
        /// `error.Closed`). Returns `error.Cancelled` if the calling
        /// coroutine has been cancelled.
        pub fn recv(self: *Self) RecvError!T {
            const coro = currentCoroutine() orelse
                @panic("Oneshot.recv called outside a coroutine");

            if (coro.isCancelled()) return error.Cancelled;

            // Fast path: value or close already pending.
            const fast = self.state.load(.acquire);
            if (fast == READY) return self.value;
            if (fast == CLOSED) return error.Closed;

            // Subscribe and yield.
            coro.pending_event = &self.es;
            ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);

            // On resume, sender (or close) cleared our ptr and signaled.
            const after = self.state.load(.acquire);
            if (coro.isCancelled()) return error.Cancelled;
            if ((after & READY) != 0) return self.value;
            return error.Closed;
        }

        /// Worker post-yield: install the coroutine pointer into the state,
        /// or fast-take if a value/close arrived first.
        fn subscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
            const es: *event_source.EventSource = @ptrCast(@alignCast(opaque_self));
            const self: *Self = @fieldParentPtr("es", es);
            const ptr = @intFromPtr(coro);
            assert((ptr & 0b11) == 0);
            assert(ptr != 0);

            var observed = self.state.load(.acquire);
            while (true) {
                if (observed == READY or observed == CLOSED) {
                    // Sender beat us. Schedule directly.
                    scheduleCoro(coro);
                    return;
                }
                if (self.state.cmpxchgWeak(observed, ptr, .acq_rel, .acquire)) |seen| {
                    observed = seen;
                    continue;
                }
                return;
            }
        }
    };
}

fn scheduleCoro(coro: *Coroutine) void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("Oneshot: scheduleCoro outside a runtime");
    rt.schedule(coro);
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "oneshot: alignof Coroutine sufficient for two-bit tag" {
    try std.testing.expect(@alignOf(Coroutine) >= 4);
}

const SimpleCtx = struct { ch: *Oneshot(u32) };

fn simpleSender(ctx: *SimpleCtx) !void {
    try ctx.ch.send(42);
}

fn simpleReceiver(ctx: *SimpleCtx) !u32 {
    return ctx.ch.recv();
}

fn simpleRoot() !u32 {
    var ch = Oneshot(u32){};
    var ctx = SimpleCtx{ .ch = &ch };
    var sender = try volt.spawn(simpleSender, .{&ctx});
    defer volt.destroyTask(sender);
    var receiver = try volt.spawn(simpleReceiver, .{&ctx});
    defer volt.destroyTask(receiver);
    try sender.join();
    return try receiver.join();
}

test "oneshot: send + recv across coroutines" {
    const v = try volt.run(.{ .allocator = std.testing.allocator }, simpleRoot, .{});
    try std.testing.expectEqual(@as(u32, 42), v);
}

const CloseCtx = struct { ch: *Oneshot(u32) };

fn closeSender(ctx: *CloseCtx) void {
    ctx.ch.close();
}

fn closeReceiver(ctx: *CloseCtx) RecvError!u32 {
    return ctx.ch.recv();
}

fn closeRoot() !void {
    var ch = Oneshot(u32){};
    var ctx = CloseCtx{ .ch = &ch };
    var sender = try volt.launch(closeSender, .{&ctx});
    defer volt.destroyJob(sender);
    var receiver = try volt.spawn(closeReceiver, .{&ctx});
    defer volt.destroyTask(receiver);
    try sender.join();
    const result = receiver.join();
    try std.testing.expectError(error.Closed, result);
}

test "oneshot: close without send → recv returns error.Closed" {
    try volt.run(.{ .allocator = std.testing.allocator }, closeRoot, .{});
}

const FastPathCtx = struct { ch: *Oneshot(u32) };

fn fastPathRoot() !u32 {
    var ch = Oneshot(u32){};
    // Send before any receiver registers.
    try ch.send(99);
    // Now spawn a receiver — its first state.load should see READY.
    var ctx = FastPathCtx{ .ch = &ch };
    var receiver = try volt.spawn(simpleReceiver, .{@as(*SimpleCtx, @ptrCast(&ctx))});
    defer volt.destroyTask(receiver);
    return try receiver.join();
}

test "oneshot: send-before-recv hits the fast path" {
    const v = try volt.run(.{ .allocator = std.testing.allocator }, fastPathRoot, .{});
    try std.testing.expectEqual(@as(u32, 99), v);
}

fn doubleSendRoot() !void {
    var ch = Oneshot(u32){};
    try ch.send(1);
    try std.testing.expectError(error.Closed, ch.send(2));
}

test "oneshot: second send returns error.Closed" {
    try volt.run(.{ .allocator = std.testing.allocator }, doubleSendRoot, .{});
}

// Fan stress: N independent Oneshots, N senders, N receivers — each
// pair handing off across (possibly distinct) worker threads. Verifies
// no missed wakes under multi-worker scheduling.

const FanStressCtx = struct {
    chs: []Oneshot(u32),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn fanStressSender(ctx: *FanStressCtx, idx: usize) !void {
    try ctx.chs[idx].send(@intCast(idx));
}

fn fanStressReceiver(ctx: *FanStressCtx, idx: usize) !void {
    const v = try ctx.chs[idx].recv();
    _ = ctx.sum.fetchAdd(v, .monotonic);
}

fn fanStressRoot() !u64 {
    const N: u32 = 256;
    const allocator = std.testing.allocator;

    const chs = try allocator.alloc(Oneshot(u32), N);
    defer allocator.free(chs);
    for (chs) |*c| c.* = .{};

    var ctx = FanStressCtx{ .chs = chs };

    var senders: [N]*volt.Task(@TypeOf(fanStressSender)) = undefined;
    var receivers: [N]*volt.Task(@TypeOf(fanStressReceiver)) = undefined;

    for (0..N) |i| receivers[i] = try volt.spawn(fanStressReceiver, .{ &ctx, i });
    defer for (receivers) |t| volt.destroyTask(t);
    for (0..N) |i| senders[i] = try volt.spawn(fanStressSender, .{ &ctx, i });
    defer for (senders) |t| volt.destroyTask(t);

    for (senders) |t| try t.join();
    for (receivers) |t| try t.join();

    return ctx.sum.load(.acquire);
}

test "oneshot stress: 256 cross-coroutine handoffs, every value delivered" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, fanStressRoot, .{});
    // 0+1+...+255 = 255*256/2 = 32640
    try std.testing.expectEqual(@as(u64, 32640), sum);
}

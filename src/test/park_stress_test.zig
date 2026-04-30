//! Park stress — multi-worker hammer test for Park.parkCurrent / Park.unpark.
//!
//! Park is the substrate for every blocking primitive (channel waiter list,
//! Mutex queue, Job.join, future Notify/Semaphore/RwLock waiters, etc.).
//! A lost wake here = a silent hang there. The M2/M3 sync primitives all
//! replicate the same waiter-list pattern, so the bedrock has to be
//! ironclad before we layer on top.
//!
//! Coverage:
//!   1. Notify-before-park: unpark fired before parkCurrent installs the
//!      coro pointer — the next parkCurrent must consume the NOTIFIED
//!      state on its fast path and return immediately.
//!   2. Cross-coroutine ping-pong: thousands of round trips, no missed
//!      wakes — exercises the racy interleaving of subscribe vs. unpark
//!      under multi-worker dispatch.
//!   3. Fan-out: N concurrent park/unpark pairs (parker on its own Park,
//!      another coroutine wakes it). Every parker must wake.

const std = @import("std");
const volt = @import("../lib.zig");
const Park = volt.scheduler.park.Park;

// ─────────────────────────────────────────────────────────────────────
// 1. Notify-before-park — fast path consumes NOTIFIED on next park.
// ─────────────────────────────────────────────────────────────────────

fn notifyFirstBody() !void {
    var park: Park = .{};
    park.unpark(); // No waiter yet — stores NOTIFIED.
    try park.parkCurrent(); // Must consume the NOTIFIED and return.
}

test "park: notify-before-park is consumed by next parkCurrent (fast path)" {
    try volt.run(.{ .allocator = std.testing.allocator }, notifyFirstBody, .{});
}

// ─────────────────────────────────────────────────────────────────────
// 2. Cross-coroutine ping-pong — many cycles, no lost wakes.
// ─────────────────────────────────────────────────────────────────────

const PingPongCtx = struct {
    rounds: u32,
    a_park: Park = .{},
    b_park: Park = .{},
    a_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    b_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn pingA(ctx: *PingPongCtx) !void {
    var i: u32 = 0;
    while (i < ctx.rounds) : (i += 1) {
        ctx.b_park.unpark();
        try ctx.a_park.parkCurrent();
        _ = ctx.a_count.fetchAdd(1, .monotonic);
    }
}

fn pingB(ctx: *PingPongCtx) !void {
    var i: u32 = 0;
    while (i < ctx.rounds) : (i += 1) {
        try ctx.b_park.parkCurrent();
        _ = ctx.b_count.fetchAdd(1, .monotonic);
        ctx.a_park.unpark();
    }
}

fn pingPongRoot() !PingPongCtx {
    var ctx: PingPongCtx = .{ .rounds = 1000 };
    var a = try volt.spawn(pingA, .{&ctx});
    defer volt.destroyTask(a);
    var b = try volt.spawn(pingB, .{&ctx});
    defer volt.destroyTask(b);
    try a.join();
    try b.join();
    return ctx;
}

test "park stress: 1000 cross-coroutine ping-pongs, no lost wakes" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, pingPongRoot, .{});
    try std.testing.expectEqual(ctx.rounds, ctx.a_count.load(.acquire));
    try std.testing.expectEqual(ctx.rounds, ctx.b_count.load(.acquire));
}

// ─────────────────────────────────────────────────────────────────────
// 3. Fan-out — 256 concurrent park/unpark pairs, every parker wakes.
// ─────────────────────────────────────────────────────────────────────

const FanCtx = struct {
    parks: []Park,
    woke: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn fanParker(ctx: *FanCtx, idx: usize) !void {
    try ctx.parks[idx].parkCurrent();
    _ = ctx.woke.fetchAdd(1, .monotonic);
}

fn fanWaker(ctx: *FanCtx, idx: usize) void {
    // A short variable spin so the unpark sometimes lands before the
    // parker has installed itself, sometimes after — exercises both
    // orderings of subscribe vs. unpark.
    var spin: u32 = 0;
    const burn: u32 = @intCast(idx & 0x3F);
    while (spin < burn) : (spin += 1) std.atomic.spinLoopHint();
    ctx.parks[idx].unpark();
}

fn fanRootBody() !u32 {
    const N: u32 = 256;
    const allocator = std.testing.allocator;

    const parks = try allocator.alloc(Park, N);
    defer allocator.free(parks);
    for (parks) |*p| p.* = .{};

    var ctx = FanCtx{ .parks = parks };

    var parkers: [N]*volt.Task(@TypeOf(fanParker)) = undefined;
    var wakers: [N]*volt.Job = undefined;

    for (0..N) |i| parkers[i] = try volt.spawn(fanParker, .{ &ctx, i });
    defer for (parkers) |t| volt.destroyTask(t);
    for (0..N) |i| wakers[i] = try volt.launch(fanWaker, .{ &ctx, i });
    defer for (wakers) |j| volt.destroyJob(j);

    for (parkers) |t| try t.join();
    for (wakers) |j| try j.join();

    return ctx.woke.load(.acquire);
}

test "park stress: 256 fan-out park/unpark pairs, every parker wakes" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, fanRootBody, .{});
    try std.testing.expectEqual(@as(u32, 256), woke);
}

// ─────────────────────────────────────────────────────────────────────
// 4. Idempotent unpark — second unpark on already-NOTIFIED state is a no-op,
//    third call after consume should restart cleanly.
// ─────────────────────────────────────────────────────────────────────

fn idempotentBody() !void {
    var park: Park = .{};
    park.unpark();
    park.unpark(); // Idempotent — still NOTIFIED, no extra effect.
    try park.parkCurrent(); // Consumes; state goes to 0.
    park.unpark(); // Fresh cycle.
    try park.parkCurrent(); // Consumes again.
}

test "park: redundant unpark is idempotent (NOTIFIED stays NOTIFIED)" {
    try volt.run(.{ .allocator = std.testing.allocator }, idempotentBody, .{});
}

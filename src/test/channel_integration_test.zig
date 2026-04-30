//! v0.3.x integration tests — Channel(T) end-to-end across the
//! multi-worker scheduler.

const std = @import("std");
const volt = @import("../lib.zig");
const Channel = volt.channel.Channel;

// ─────────────────────────────────────────────────────────────────────
// 1. Basic SPSC: producer sends N values, consumer receives them.
// ─────────────────────────────────────────────────────────────────────

const SpscCtx = struct {
    ch: *Channel(u32),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn spscProducer(ctx: *SpscCtx, n: u32) !void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try ctx.ch.send(i);
    }
    ctx.ch.close();
}

fn spscConsumer(ctx: *SpscCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return,
            else => return err,
        };
        _ = ctx.sum.fetchAdd(v, .monotonic);
    }
}

fn spscRoot() !u64 {
    var ch = try Channel(u32).init(std.testing.allocator, 8);
    defer ch.deinit();
    var ctx = SpscCtx{ .ch = &ch };

    const N: u32 = 1000;

    var prod = try volt.spawn(spscProducer, .{ &ctx, N });
    defer volt.destroyTask(prod);
    var cons = try volt.spawn(spscConsumer, .{&ctx});
    defer volt.destroyTask(cons);

    try prod.join();
    try cons.join();

    return ctx.sum.load(.acquire);
}

test "v0.3.x channel: SPSC, 1000 messages, sum invariant" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, spscRoot, .{});
    // 0+1+...+999 = 999*1000/2 = 499500
    try std.testing.expectEqual(@as(u64, 499_500), total);
}

// ─────────────────────────────────────────────────────────────────────
// 2. MPMC: M producers each send N values, K consumers drain. The
//    total received must equal M*N. Tests that messages aren't lost
//    or duplicated under multi-worker contention.
// ─────────────────────────────────────────────────────────────────────

const MpmcCtx = struct {
    ch: *Channel(u32),
    count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn mpmcProducer(ctx: *MpmcCtx, base: u32, per_producer: u32) !void {
    var i: u32 = 0;
    while (i < per_producer) : (i += 1) {
        try ctx.ch.send(base + i);
    }
}

fn mpmcConsumer(ctx: *MpmcCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return,
            else => return err,
        };
        _ = ctx.count.fetchAdd(1, .monotonic);
        _ = ctx.sum.fetchAdd(v, .monotonic);
    }
}

fn mpmcRoot() !MpmcCtx {
    var ch = try Channel(u32).init(std.testing.allocator, 16);
    defer ch.deinit();
    var ctx = MpmcCtx{ .ch = &ch };

    const M: u32 = 4; // producers
    const K: u32 = 4; // consumers
    const N: u32 = 250; // each producer sends N

    var producers: [M]*volt.Task(@TypeOf(mpmcProducer)) = undefined;
    var consumers: [K]*volt.Task(@TypeOf(mpmcConsumer)) = undefined;

    for (&producers, 0..) |*t, i| {
        t.* = try volt.spawn(mpmcProducer, .{ &ctx, @as(u32, @intCast(i * 10000)), N });
    }
    for (&consumers) |*t| {
        t.* = try volt.spawn(mpmcConsumer, .{&ctx});
    }
    defer for (producers) |t| volt.destroyTask(t);
    defer for (consumers) |t| volt.destroyTask(t);

    // Wait for all producers, then close.
    for (producers) |t| try t.join();
    ch.close();
    // Now consumers will see closed once drained.
    for (consumers) |t| try t.join();

    return ctx;
}

test "v0.3.x channel: MPMC 4×4, 1000 messages, no loss/duplication" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, mpmcRoot, .{});
    try std.testing.expectEqual(@as(u64, 1000), ctx.count.load(.acquire));

    // Sum of all sent values:
    //   producer i sends i*10000 .. i*10000+249
    //   sum_i = sum_{j=0..249}(i*10000 + j) = 250*i*10000 + 250*249/2
    //         = 2_500_000*i + 31125
    //   total = sum_{i=0..3}(2_500_000*i + 31125)
    //         = 2_500_000*6 + 4*31125 = 15_000_000 + 124_500 = 15_124_500
    try std.testing.expectEqual(@as(u64, 15_124_500), ctx.sum.load(.acquire));
}

// ─────────────────────────────────────────────────────────────────────
// 3. Backpressure: tiny buffer + producer faster than consumer.
//    Producer must repeatedly hit `.full` and park; consumer wakes it.
//    (Vyukov MPMC requires cap ≥ 2 — see Channel.init doc. Rendezvous
//    semantics ship as a separate type.)
// ─────────────────────────────────────────────────────────────────────

const BackpressureCtx = struct {
    ch: *Channel(u32),
    received: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn bpProducer(ctx: *BackpressureCtx) !void {
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try ctx.ch.send(i);
    }
    ctx.ch.close();
}

fn bpConsumer(ctx: *BackpressureCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return,
            else => return err,
        };
        _ = ctx.received.fetchAdd(v, .monotonic);
        try volt.yield();
    }
}

fn bpRoot() !u64 {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();
    var ctx = BackpressureCtx{ .ch = &ch };

    var prod = try volt.spawn(bpProducer, .{&ctx});
    defer volt.destroyTask(prod);
    var cons = try volt.spawn(bpConsumer, .{&ctx});
    defer volt.destroyTask(cons);

    try prod.join();
    try cons.join();
    return ctx.received.load(.acquire);
}

test "v0.3.x channel: capacity-2 backpressure, sender blocks on full" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, bpRoot, .{});
    try std.testing.expectEqual(@as(u64, 4950), total); // 0+1+...+99
}

// ─────────────────────────────────────────────────────────────────────
// 4. Close wakes parked receivers — they unwind with `error.Closed`.
// ─────────────────────────────────────────────────────────────────────

const CloseWakeCtx = struct {
    ch: *Channel(u32),
    woke_with_closed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn waitingConsumer(ctx: *CloseWakeCtx) void {
    _ = ctx.ch.recv() catch |err| switch (err) {
        error.Closed => {
            _ = ctx.woke_with_closed.fetchAdd(1, .monotonic);
            return;
        },
        else => return,
    };
    // If we got a value (shouldn't happen), do nothing.
}

fn closeWakeRoot() !u32 {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();
    var ctx = CloseWakeCtx{ .ch = &ch };

    // Spawn 8 consumers that block on an empty channel.
    var consumers: [8]*volt.Job = undefined;
    for (&consumers) |*j| j.* = try volt.launch(waitingConsumer, .{&ctx});
    defer for (consumers) |j| volt.destroyJob(j);

    // Yield a few times so consumers all park.
    var k: u32 = 0;
    while (k < 8) : (k += 1) try volt.yield();

    // Close — every consumer must unwind with .closed.
    ch.close();

    for (consumers) |j| try j.join();
    return ctx.woke_with_closed.load(.acquire);
}

test "v0.3.x channel: close wakes all parked receivers with error.Closed" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, closeWakeRoot, .{});
    try std.testing.expectEqual(@as(u32, 8), woke);
}

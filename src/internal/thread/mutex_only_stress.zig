//! Bisecting test — Mutex stress + producer/consumer.
//! If only mutex_isolation passes, deadlock is in producer/consumer.

const std = @import("std");
const Mutex = @import("Mutex.zig");
const Condition = @import("Condition.zig");

test "Mutex isolation - 16 threads x 100k iters" {
    const NUM = 16;
    const ITERS = 100_000;

    const Shared = struct { mu: Mutex = .{}, c: u64 = 0 };
    var s = Shared{};

    const W = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            var i: u64 = 0;
            while (i < ITERS) : (i += 1) {
                ctx.s.mu.lock();
                ctx.s.c += 1;
                ctx.s.mu.unlock();
            }
        }
    };

    var ts: [NUM]std.Thread = undefined;
    for (&ts) |*t| t.* = try std.Thread.spawn(.{}, W.run, .{W{ .s = &s }});
    for (ts) |t| t.join();

    try std.testing.expectEqual(@as(u64, ITERS) * NUM, s.c);
    std.debug.print("OK: {} ops\n", .{s.c});
}

test "Mutex producer/consumer (full scale - 4x4, 50k each)" {
    const NUM_P = 4;
    const NUM_C = 4;
    const ITERS = 50_000;

    const Shared = struct {
        mu: Mutex = .{},
        queue: std.array_list.Managed(u64),
        produced: u64 = 0,
        consumed: u64 = 0,
        producers_done: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };

    var s = Shared{ .queue = std.array_list.Managed(u64).init(std.testing.allocator) };
    defer s.queue.deinit();

    const Producer = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            var i: u64 = 0;
            while (i < ITERS) : (i += 1) {
                ctx.s.mu.lock();
                ctx.s.queue.append(i) catch {
                    ctx.s.mu.unlock();
                    return;
                };
                ctx.s.produced += 1;
                ctx.s.mu.unlock();
            }
            _ = ctx.s.producers_done.fetchAdd(1, .release);
        }
    };

    const Consumer = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            while (true) {
                ctx.s.mu.lock();
                if (ctx.s.queue.items.len > 0) {
                    _ = ctx.s.queue.orderedRemove(0);
                    ctx.s.consumed += 1;
                    ctx.s.mu.unlock();
                } else {
                    const all_done = ctx.s.producers_done.load(.acquire) == NUM_P;
                    const empty = ctx.s.queue.items.len == 0;
                    ctx.s.mu.unlock();
                    if (all_done and empty) return;
                    std.Thread.yield() catch {};
                }
            }
        }
    };

    var ts: [NUM_P + NUM_C]std.Thread = undefined;
    var idx: usize = 0;
    for (0..NUM_P) |_| {
        ts[idx] = try std.Thread.spawn(.{}, Producer.run, .{Producer{ .s = &s }});
        idx += 1;
    }
    for (0..NUM_C) |_| {
        ts[idx] = try std.Thread.spawn(.{}, Consumer.run, .{Consumer{ .s = &s }});
        idx += 1;
    }
    for (ts) |t| t.join();

    const expected = @as(u64, ITERS) * NUM_P;
    try std.testing.expectEqual(expected, s.produced);
    try std.testing.expectEqual(expected, s.consumed);
    std.debug.print("PROD/CONS OK: {} produced, {} consumed\n", .{ s.produced, s.consumed });
}

test "Condition isolation - large two-cv coordination" {
    // Scaled-up version matching the original stress test.
    const NUM_W = 16;
    const NUM_R = 1000;

    const Shared = struct {
        mu: Mutex = .{},
        cv_round: Condition = .{},
        cv_ack: Condition = .{},
        epoch: u64 = 0,
        acks: u32 = 0,
        all_done: bool = false,
    };
    var s = Shared{};

    const W = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            var seen: u64 = 0;
            while (true) {
                ctx.s.mu.lock();
                while (ctx.s.epoch == seen and !ctx.s.all_done) {
                    ctx.s.cv_round.wait(&ctx.s.mu);
                }
                if (ctx.s.all_done) {
                    ctx.s.mu.unlock();
                    return;
                }
                seen = ctx.s.epoch;
                ctx.s.acks += 1;
                ctx.s.mu.unlock();
                ctx.s.cv_ack.signal();
            }
        }
    };

    var ts: [NUM_W]std.Thread = undefined;
    for (&ts) |*t| t.* = try std.Thread.spawn(.{}, W.run, .{W{ .s = &s }});

    var r: u64 = 0;
    while (r < NUM_R) : (r += 1) {
        s.mu.lock();
        s.acks = 0;
        s.epoch += 1;
        s.mu.unlock();
        s.cv_round.broadcast();

        s.mu.lock();
        while (s.acks < NUM_W) s.cv_ack.wait(&s.mu);
        s.mu.unlock();
    }

    s.mu.lock();
    s.all_done = true;
    s.mu.unlock();
    s.cv_round.broadcast();
    for (ts) |t| t.join();

    try std.testing.expectEqual(@as(u64, NUM_R), s.epoch);
    std.debug.print("COND OK: {} rounds\n", .{s.epoch});
}

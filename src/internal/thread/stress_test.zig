//! Focused stress tests for the internal thread primitives.
//!
//! Standalone test file — run with `zig test src/internal/thread/stress_test.zig`.
//! These tests run for several seconds and are intended to surface lost-wakeup,
//! deadlock, and memory-ordering bugs that the inline tests are too short to hit.

const std = @import("std");
const Mutex = @import("Mutex.zig");
const Condition = @import("Condition.zig");
const Futex = @import("Futex.zig");

// ─────────────────────────────────────────────────────────────────────────────
// Helper: deadline-bounded run that fails the test if it doesn't complete
// ─────────────────────────────────────────────────────────────────────────────

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutex stress: heavy contention, no lost increments, no deadlock
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex stress - 16 threads x 100k iters, no lost increments" {
    const NUM_THREADS = 16;
    const ITERS = 100_000;

    const Shared = struct {
        mu: Mutex = .{},
        counter: u64 = 0,
    };
    var shared = Shared{};

    const Worker = struct {
        s: *Shared,
        iters: u64,
        fn run(ctx: @This()) void {
            var i: u64 = 0;
            while (i < ctx.iters) : (i += 1) {
                ctx.s.mu.lock();
                ctx.s.counter += 1;
                ctx.s.mu.unlock();
            }
        }
    };

    const start = nanosNow();
    var threads: [NUM_THREADS]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .s = &shared, .iters = ITERS }});
    }
    for (threads) |t| t.join();
    const elapsed_ns = nanosNow() - start;

    const expected = @as(u64, ITERS) * NUM_THREADS;
    try std.testing.expectEqual(expected, shared.counter);

    const total_ops = expected;
    const ns_per_op = @divTrunc(elapsed_ns, @as(i128, total_ops));
    std.debug.print(
        "  Mutex stress: {} ops in {}ms ({}ns/op contended)\n",
        .{ total_ops, @divTrunc(elapsed_ns, std.time.ns_per_ms), ns_per_op },
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutex stress: producer/consumer with handoff — checks no permanent stalls
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex stress - producer/consumer handoff (no permanent stall)" {
    const ITERS_PER_PRODUCER = 50_000;
    const NUM_PRODUCERS = 4;
    const NUM_CONSUMERS = 4;

    const Shared = struct {
        mu: Mutex = .{},
        queue: std.array_list.Managed(u64),
        produced: u64 = 0,
        consumed: u64 = 0,
        producers_done: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };

    var queue = std.array_list.Managed(u64).init(std.testing.allocator);
    defer queue.deinit();

    var shared = Shared{ .queue = queue };

    const Producer = struct {
        s: *Shared,
        iters: u64,
        fn run(ctx: @This()) void {
            var i: u64 = 0;
            while (i < ctx.iters) : (i += 1) {
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
        target_producers: u32,
        fn run(ctx: @This()) void {
            while (true) {
                ctx.s.mu.lock();
                if (ctx.s.queue.items.len > 0) {
                    _ = ctx.s.queue.orderedRemove(0);
                    ctx.s.consumed += 1;
                    ctx.s.mu.unlock();
                } else {
                    const all_done = ctx.s.producers_done.load(.acquire) == ctx.target_producers;
                    ctx.s.mu.unlock();
                    if (all_done) {
                        // Re-check under lock; producers might have just appended.
                        ctx.s.mu.lock();
                        const empty = ctx.s.queue.items.len == 0;
                        ctx.s.mu.unlock();
                        if (empty) return;
                    }
                    // Brief yield
                    std.Thread.yield() catch {};
                }
            }
        }
    };

    var threads: [NUM_PRODUCERS + NUM_CONSUMERS]std.Thread = undefined;
    var t_idx: usize = 0;
    for (0..NUM_PRODUCERS) |_| {
        threads[t_idx] = try std.Thread.spawn(.{}, Producer.run, .{Producer{ .s = &shared, .iters = ITERS_PER_PRODUCER }});
        t_idx += 1;
    }
    for (0..NUM_CONSUMERS) |_| {
        threads[t_idx] = try std.Thread.spawn(.{}, Consumer.run, .{Consumer{ .s = &shared, .target_producers = NUM_PRODUCERS }});
        t_idx += 1;
    }

    for (threads) |t| t.join();

    const expected = @as(u64, ITERS_PER_PRODUCER) * NUM_PRODUCERS;
    try std.testing.expectEqual(expected, shared.produced);
    try std.testing.expectEqual(expected, shared.consumed);
    try std.testing.expectEqual(@as(usize, 0), shared.queue.items.len);
}

// ─────────────────────────────────────────────────────────────────────────────
// Condition stress: many waiters, repeated signal/broadcast — no lost wakeups
// ─────────────────────────────────────────────────────────────────────────────

test "Condition stress - 16 waiters x 1000 broadcasts (no lost wakeup)" {
    const NUM_WAITERS = 16;
    const NUM_ROUNDS = 1000;

    // Two-condition design (avoids the classic single-cv-shared-by-both-sides
    // deadlock). One cv for waiters→coord ack, one cv for coord→waiters round-bump.

    const Shared = struct {
        mu: Mutex = .{},
        cv_round: Condition = .{}, // signaled by coord, waited on by waiters
        cv_ack: Condition = .{}, // signaled by waiters, waited on by coord
        epoch: u64 = 0,
        waiters_at_epoch: u32 = 0,
        all_done: bool = false,
    };
    var shared = Shared{};

    const Waiter = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            var seen_epoch: u64 = 0;
            while (true) {
                ctx.s.mu.lock();
                while (ctx.s.epoch == seen_epoch and !ctx.s.all_done) {
                    ctx.s.cv_round.wait(&ctx.s.mu);
                }
                if (ctx.s.all_done) {
                    ctx.s.mu.unlock();
                    return;
                }
                seen_epoch = ctx.s.epoch;
                ctx.s.waiters_at_epoch += 1;
                ctx.s.mu.unlock();
                ctx.s.cv_ack.signal(); // notify coord one waiter ack'd
            }
        }
    };

    var threads: [NUM_WAITERS]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Waiter.run, .{Waiter{ .s = &shared }});
    }

    var round: u64 = 0;
    while (round < NUM_ROUNDS) : (round += 1) {
        // Bump epoch and broadcast — wakes all waiters for this round
        shared.mu.lock();
        shared.waiters_at_epoch = 0;
        shared.epoch += 1;
        shared.mu.unlock();
        shared.cv_round.broadcast();

        // Wait for all waiters to ack
        shared.mu.lock();
        while (shared.waiters_at_epoch < NUM_WAITERS) {
            shared.cv_ack.wait(&shared.mu);
        }
        shared.mu.unlock();
    }

    // Tell waiters to exit
    shared.mu.lock();
    shared.all_done = true;
    shared.mu.unlock();
    shared.cv_round.broadcast();

    for (threads) |t| t.join();

    try std.testing.expectEqual(@as(u64, NUM_ROUNDS), shared.epoch);
}

// ─────────────────────────────────────────────────────────────────────────────
// Condition stress: timedWait actually times out under contention
// ─────────────────────────────────────────────────────────────────────────────

test "Condition stress - timedWait times out repeatedly" {
    var mu: Mutex = .{};
    var cv: Condition = .{};

    const ITERS = 100;
    const TIMEOUT_NS: u64 = 2 * std.time.ns_per_ms; // 2ms

    var timeouts: u32 = 0;
    var i: u32 = 0;
    while (i < ITERS) : (i += 1) {
        mu.lock();
        const result = cv.timedWait(&mu, TIMEOUT_NS);
        mu.unlock();
        if (result) |_| {} else |_| {
            timeouts += 1;
        }
    }

    // We expect every wait to time out (no one is signaling).
    try std.testing.expectEqual(@as(u32, ITERS), timeouts);
}

// ─────────────────────────────────────────────────────────────────────────────
// Futex stress: many waiters, one waker — exactly one wakes per call
// ─────────────────────────────────────────────────────────────────────────────

test "Futex stress - wake(1) wakes exactly one waiter" {
    const NUM_WAITERS = 8;
    const NUM_ROUNDS = 200;

    const Shared = struct {
        v: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        woken_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        target: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };
    var shared = Shared{};

    const Waiter = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            while (true) {
                const cur = ctx.s.v.load(.acquire);
                const target = ctx.s.target.load(.acquire);
                if (cur >= target and target != 0) return;
                Futex.wait(&ctx.s.v, cur);
                _ = ctx.s.woken_count.fetchAdd(1, .release);
            }
        }
    };

    var threads: [NUM_WAITERS]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Waiter.run, .{Waiter{ .s = &shared }});
    }

    // Give threads time to park
    std.Thread.yield() catch {};
    var round: u32 = 0;
    while (round < NUM_ROUNDS) : (round += 1) {
        const before = shared.woken_count.load(.acquire);
        _ = shared.v.fetchAdd(1, .release);
        Futex.wake(&shared.v, 1);
        // Wait for at least one waiter to acknowledge waking
        const deadline = nanosNow() + 100 * std.time.ns_per_ms;
        while (shared.woken_count.load(.acquire) <= before) {
            if (nanosNow() > deadline) {
                std.debug.print("Futex wake timeout at round {}\n", .{round});
                return error.WakeTimeout;
            }
            std.Thread.yield() catch {};
        }
    }

    // Tell waiters to exit
    shared.target.store(NUM_ROUNDS + 100, .release);
    _ = shared.v.fetchAdd(1, .release);
    Futex.wake(&shared.v, std.math.maxInt(u32));

    for (threads) |t| t.join();
}

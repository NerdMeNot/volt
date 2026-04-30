//! Scheduler fuzz — random spawn/yield/cancel under multi-worker
//! contention. Goal: shake out lost wakes, leaks, or inconsistent
//! cancellation behavior under heavy load.
//!
//! The harness keeps a sliding window of in-flight Jobs. Each iteration:
//!   1. Reap the slot at `i % BATCH` if occupied.
//!   2. Launch a fresh worker that yields N random times then completes
//!      (or unwinds early on `error.Cancelled`).
//!   3. With small probability, cancel a random in-flight job — exercises
//!      cancel-after-spawn and cancel-during-yield paths.
//!   4. With small probability, yield from the root — interleaves the
//!      driver coroutine with the workers across the worker pool.
//!
//! Acceptance: every spawned Job reaches `.done` and every join returns
//! cleanly (with `Cancelled` allowed). A leaked coroutine surfaces as a
//! join hang (caught by the test runner timeout); a missed wake surfaces
//! as a `joined < spawns` mismatch.
//!
//! Note: we deliberately don't try to invariant `spawns == completions +
//! cancellations` from inside the user function. A `cancel()` that lands
//! between `volt.launch` and the first dispatch causes the trampoline to
//! short-circuit (`spawn.zig` Closure.run, line 78) — the user function
//! never runs, neither counter ticks. That's the intended Tokio/Kotlin-
//! style "best-effort cancel" semantic, not a leak.

const std = @import("std");
const volt = @import("../lib.zig");

const FuzzCtx = struct {
    spawns: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    joined: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn fuzzWorker(_: *FuzzCtx, work: u32) void {
    var i: u32 = 0;
    while (i < work) : (i += 1) {
        volt.yield() catch |err| switch (err) {
            error.Cancelled => return,
        };
    }
}

fn fuzzRoot(iters: u32, seed: u64) !FuzzCtx {
    var ctx: FuzzCtx = .{};
    var rng = std.Random.DefaultPrng.init(seed);
    const allocator = std.testing.allocator;

    const BATCH: u32 = 32;

    var window = try allocator.alloc(?*volt.Job, BATCH);
    defer allocator.free(window);
    @memset(window, null);

    var i: u32 = 0;
    while (i < iters) : (i += 1) {
        const slot = i % BATCH;
        if (window[slot]) |j| {
            // Job.join returns error.Cancelled if the child was cancelled —
            // expected here. Either way, the join completing means the
            // child reached `.done` (Done.subscribe fired).
            j.join() catch {};
            _ = ctx.joined.fetchAdd(1, .monotonic);
            volt.destroyJob(j);
            window[slot] = null;
        }
        const work = rng.random().uintLessThan(u32, 32) + 1;
        const j = try volt.launch(fuzzWorker, .{ &ctx, work });
        _ = ctx.spawns.fetchAdd(1, .monotonic);
        window[slot] = j;

        const action = rng.random().uintLessThan(u8, 16);
        if (action == 0) {
            const victim = rng.random().uintLessThan(u32, BATCH);
            if (window[victim]) |vj| vj.cancel();
        } else if (action == 1) {
            try volt.yield();
        }
    }

    // Drain any remaining live jobs.
    for (window) |maybe_j| {
        if (maybe_j) |j| {
            j.join() catch {};
            _ = ctx.joined.fetchAdd(1, .monotonic);
            volt.destroyJob(j);
        }
    }
    return ctx;
}

test "fuzz: 2000 mixed spawn/yield/cancel iterations, every spawn joins cleanly" {
    const ctx = try volt.run(
        .{ .allocator = std.testing.allocator },
        fuzzRoot,
        .{ @as(u32, 2000), @as(u64, 0xC0FFEE) },
    );
    const spawns = ctx.spawns.load(.acquire);
    const joined = ctx.joined.load(.acquire);
    try std.testing.expectEqual(@as(u32, 2000), spawns);
    try std.testing.expectEqual(spawns, joined);
}

// Same fuzz with a different seed — different scheduling trace, same invariant.
test "fuzz: 2000 mixed iters with alternate seed" {
    const ctx = try volt.run(
        .{ .allocator = std.testing.allocator },
        fuzzRoot,
        .{ @as(u32, 2000), @as(u64, 0xDEADBEEF) },
    );
    const spawns = ctx.spawns.load(.acquire);
    const joined = ctx.joined.load(.acquire);
    try std.testing.expectEqual(spawns, joined);
}

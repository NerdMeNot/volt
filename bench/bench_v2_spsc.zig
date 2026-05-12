//! Bench — v2 Spsc channel send+recv pairs across two coroutines.
//!
//! Reference numbers:
//!   POC-F (two threads, busy-spin): 29 ns/op
//!   Volt v0.x Vyukov MPMC:         180 ns/op
//!   Go chan cap=16:                 33 ns/op
//!
//! v2 single-worker uses park/unpark when the channel fills or
//! empties (rather than busy spin), which actually amortizes well at
//! cap=16: ~1 park per 16 sends.

const std = @import("std");
const volt2 = @import("volt2");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const Channel = volt2.Spsc(u64, 16);

const BenchCtx = struct {
    ch: *Channel,
    n: u64,
    sum: u64 = 0,
    wall_ns: i128 = 0,
};

fn producer(ctx: *BenchCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn consumer(ctx: *BenchCtx) !void {
    while (true) {
        const v = ctx.ch.recv() catch return;
        ctx.sum +%= v;
    }
}

fn benchRoot(ctx: *BenchCtx) !void {
    const rt: *volt2.Runtime = @ptrCast(@alignCast(volt2.current.require().runtime));
    const start = nanosNow();
    var prod = try rt.spawn(producer, .{ctx});
    var cons = try rt.spawn(consumer, .{ctx});
    _ = prod.join() catch unreachable;
    _ = cons.join() catch unreachable;
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, n: u64) !i128 {
    var rt = try volt2.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    var ch = Channel{};
    var ctx = BenchCtx{ .ch = &ch, .n = n };
    try (try rt.run(benchRoot, .{&ctx}));
    return @divTrunc(ctx.wall_ns, n);
}

const REPS = 11;
const WARMUPS = 3;
const N: u64 = 200_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== v2 Spsc channel bench ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n={d}, reps={d}, cap=16\n\n", .{ N, REPS });

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N);
    for (&samples) |*s| s.* = try benchOnce(smp, N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];

    std.debug.print("  v2 Spsc send+recv: {d} ns/op (median, min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt v0.x Vyukov MPMC: 180 ns/op (5.6× Go)\n", .{});
    std.debug.print("  POC-F (2 threads):      29 ns/op (1.14× faster than Go)\n", .{});
    std.debug.print("  Go chan cap=16:         33 ns/op (1.0×)\n", .{});

    std.debug.print("\nVerdict: {s}\n", .{
        if (median <= 35) "PASS — at or below Go" else if (median <= 70) "CLOSE — within 2× of Go" else "INVESTIGATE — too much overhead",
    });
}

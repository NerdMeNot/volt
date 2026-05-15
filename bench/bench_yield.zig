//! Bench — yield ping-pong (one-way ctx-switch cost via cooperative
//! scheduler dispatch).
//!
//! Reference numbers:
//!   POC-A wide-save raw ctx switch:    6 ns/swap
//!   Go runtime.Gosched:               42 ns/swap one-way

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn yieldNTimes(n: u32) void {
    var i: u32 = 0;
    while (i < n) : (i += 1) volt.yield();
}

const BenchCtx = struct {
    n: u32,
    wall_ns: i128 = 0,
};

fn benchRoot(ctx: *BenchCtx) !void {
    const rt: *volt.Runtime = @ptrCast(@alignCast(volt.current.require().runtime));
    var task = try rt.spawn(yieldNTimes, .{ctx.n});
    const start = nanosNow();
    task.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    // workers=1: yield is a cooperative same-worker primitive; the
    // ctx-switch cost has nothing to do with multi-thread parallelism.
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    var ctx = BenchCtx{ .n = n };
    try (try rt.run(benchRoot, .{&ctx}));
    return @divTrunc(ctx.wall_ns, @as(i128, n) * 2);
}

const REPS = 11;
const WARMUPS = 3;
const N: u32 = 1_000_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== yield (one-way ctx switch) bench ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n={d}, reps={d}\n\n", .{ N, REPS });

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N);
    for (&samples) |*s| s.* = try benchOnce(smp, N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    std.debug.print("  yield: {d} ns/op one-way (median of {d})\n", .{ median, REPS });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  POC-A wide ctx swap:         6 ns/swap\n", .{});
    std.debug.print("  Go runtime.Gosched:         42 ns/op\n", .{});
}

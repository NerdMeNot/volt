//! Bench — v2 Runtime spawn+join (multi-worker scheduler).
//!
//! Compares against:
//!   * Go spawn+waitgroup:                                      149 ns/op
//!   * Volt v0.x current:                                     4,163 ns/op
//!
//! v2 typed API: heap-alloc per-spawn Frame + Coroutine + Task,
//! comptime-monomorphized trampoline, WaitGroup auto-decrement.

const std = @import("std");
const volt2 = @import("volt2");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn nopFn() void {}

const BenchCtx = struct {
    n: u32,
    wall_ns: i128 = 0,
};

fn benchRoot(ctx: *BenchCtx) !void {
    const rt: *volt2.Runtime = @ptrCast(@alignCast(volt2.current.require().runtime));
    const tasks = try rt.allocator.alloc(*volt2.Task(void), ctx.n);
    defer rt.allocator.free(tasks);

    const start = nanosNow();
    for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
    for (tasks) |t| t.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, n: u32, workers: usize) !i128 {
    var rt = try volt2.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();
    var ctx = BenchCtx{ .n = n };
    try (try rt.run(benchRoot, .{&ctx}));
    return @divTrunc(ctx.wall_ns, n);
}

const REPS = 11;
const WARMUPS = 3;
const N: u32 = 10_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== v2 spawn+join (multi-worker) ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n={d}, reps={d}\n\n", .{ N, REPS });

    inline for (.{ 1, 2, 4, 8 }) |w_count| {
        var samples: [REPS]i128 = undefined;
        var w: u32 = 0;
        while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N, w_count);
        for (&samples) |*s| s.* = try benchOnce(smp, N, w_count);
        std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
        const median = samples[REPS / 2];
        const min = samples[0];
        const max = samples[REPS - 1];
        std.debug.print("  workers={d:>2}  median {d:>5} ns/op (min {d}, max {d})\n", .{ w_count, median, min, max });
    }

    std.debug.print("\nReference: Volt v0.x = 4,163 ns/op; Go = 149 ns/op.\n", .{});
}

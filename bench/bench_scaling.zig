//! Bench — multi-worker scaling curve.
//!
//! Runs the spawn+join hot loop at workers ∈ {1, 2, 4, 8, NumCPU} and
//! prints a table with per-op latency and the ratio vs workers=1.
//!
//! This is the **receipt bench** for scheduler changes that claim to
//! improve multi-worker scaling. Treat the workers=1→11 ratio as the
//! single-number success metric: lower is better, target ≤ 1.5×.
//!
//! See `docs/internals/multi-worker-profile.md` for the analysis that
//! drove this bench.

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn nopFn() void {}

const Ctx = struct {
    batch: u32,
    target_ns: i128,
    total_ops: u64 = 0,
    elapsed_ns: i128 = 0,
};

fn benchRoot(ctx: *Ctx) !void {
    const rt: *volt.Runtime = @ptrCast(@alignCast(volt.current.require().runtime));
    const tasks = try rt.allocator.alloc(*volt.Task(void), ctx.batch);
    defer rt.allocator.free(tasks);

    // Warmup — fill the pools.
    var w: u32 = 0;
    while (w < 3) : (w += 1) {
        for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
        for (tasks) |t| t.join();
    }

    var total_ops: u64 = 0;
    const start = nanosNow();
    while (true) {
        for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
        for (tasks) |t| t.join();
        total_ops += ctx.batch;
        if (nanosNow() - start >= ctx.target_ns) break;
    }
    ctx.elapsed_ns = nanosNow() - start;
    ctx.total_ops = total_ops;
}

const BATCH: u32 = 1000;
const DURATION_S: u32 = 4;

const Sample = struct { workers: usize, ns_per_op: i128, ops_per_sec: i128 };

fn runAtWorkers(allocator: std.mem.Allocator, workers: usize) !Sample {
    var ctx = Ctx{
        .batch = BATCH,
        .target_ns = @as(i128, DURATION_S) * std.time.ns_per_s,
    };
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();

    try (try rt.run(benchRoot, .{&ctx}));

    const ns_per_op = @divTrunc(ctx.elapsed_ns, @as(i128, ctx.total_ops));
    const ops_per_sec = @divTrunc(@as(i128, ctx.total_ops) * std.time.ns_per_s, ctx.elapsed_ns);
    return .{ .workers = workers, .ns_per_op = ns_per_op, .ops_per_sec = ops_per_sec };
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const cpu_count = try std.Thread.getCpuCount();

    std.debug.print("=== multi-worker scaling curve ===\n", .{});
    std.debug.print("Platform: ReleaseFast, batch={d}, duration={d}s/worker count, ncpu={d}\n", .{ BATCH, DURATION_S, cpu_count });
    std.debug.print("\n", .{});

    const worker_counts = [_]usize{ 1, 2, 4, 8, cpu_count };
    var samples: [worker_counts.len]Sample = undefined;
    for (worker_counts, 0..) |w, i| {
        samples[i] = try runAtWorkers(allocator, w);
        std.debug.print("  workers={d:>3}  median {d:>6} ns/op  {d:>10} ops/sec\n", .{
            samples[i].workers,
            samples[i].ns_per_op,
            samples[i].ops_per_sec,
        });
    }

    std.debug.print("\n", .{});
    std.debug.print("Scaling ratios (vs workers=1):\n", .{});
    const base = samples[0].ns_per_op;
    for (samples) |s| {
        const ratio = @as(f64, @floatFromInt(s.ns_per_op)) / @as(f64, @floatFromInt(base));
        std.debug.print("  workers={d:>3}  {d:>6.2}×\n", .{ s.workers, ratio });
    }

    std.debug.print("\n", .{});
    std.debug.print("Success metric: workers={d}/workers=1 ratio should be ≤ 1.5×.\n", .{cpu_count});
    const final_ratio = @as(f64, @floatFromInt(samples[samples.len - 1].ns_per_op)) / @as(f64, @floatFromInt(base));
    if (final_ratio <= 1.5) {
        std.debug.print("  PASS — ratio is {d:.2}×\n", .{final_ratio});
    } else {
        std.debug.print("  FAIL — ratio is {d:.2}× (target ≤ 1.5×)\n", .{final_ratio});
    }
}

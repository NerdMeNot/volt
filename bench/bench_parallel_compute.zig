//! Bench — parallel CPU-bound workload across N workers.
//!
//! Demonstrates where multi-worker WINS. Each coroutine does
//! ~50 us of CPU work (XorShift mix); spawn cost is amortized.
//!
//! Ideal scaling: 2x workers → 2x throughput. Real-world scaling
//! is bounded by:
//!   * Serial spawn loop on the driver coro (one thread allocates
//!     all coros + pushes them; even at parallel pace this caps speedup)
//!   * Cross-thread bookkeeping (parker bitmap, pending counter,
//!     reactor poller CAS)

const std = @import("std");
const volt = @import("volt");

const N_TASKS: u32 = 256;
const WORK_ITERS: u32 = 50_000;

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

/// XorShift mix — keeps the compiler from constant-folding.
fn doWork(seed: u64) u64 {
    var x = seed;
    var i: u32 = 0;
    while (i < WORK_ITERS) : (i += 1) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    return x;
}

const TaskCtx = struct {
    seed: u64,
    sink: *std.atomic.Value(u64),
};

fn task(ctx: *TaskCtx) void {
    const r = doWork(ctx.seed);
    _ = ctx.sink.fetchAdd(r, .acq_rel);
}

const RootCtx = struct {
    sink: *std.atomic.Value(u64),
    wall_ns: i128 = 0,
};

fn root(ctx: *RootCtx) !void {
    const rt = volt.runtime();
    const ctxs = try rt.allocator.alloc(TaskCtx, N_TASKS);
    defer rt.allocator.free(ctxs);
    const tasks = try rt.allocator.alloc(*volt.Task(void), N_TASKS);
    defer rt.allocator.free(tasks);

    for (ctxs, 0..) |*c, i| c.* = .{ .seed = @as(u64, i) * 0xDEADBEEF +% 1, .sink = ctx.sink };
    const start = nanosNow();
    for (tasks, 0..) |*t, i| t.* = try rt.spawn(task, .{&ctxs[i]});
    for (tasks) |t| t.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, workers: usize) !i128 {
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();
    var sink = std.atomic.Value(u64).init(0);
    var ctx = RootCtx{ .sink = &sink };
    try (try rt.run(root, .{&ctx}));
    std.mem.doNotOptimizeAway(sink.load(.acquire));
    return ctx.wall_ns;
}

const REPS = 7;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== parallel-compute bench ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n_tasks={d}, work_iters/task={d}, reps={d}\n", .{ N_TASKS, WORK_ITERS, REPS });
    std.debug.print("Each task: XorShift({d} rounds) — ~CPU-bound work\n\n", .{WORK_ITERS});

    var baseline: ?i128 = null;
    inline for (.{ 1, 2, 4, 8 }) |w_count| {
        var samples: [REPS]i128 = undefined;
        var w: u32 = 0;
        while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, w_count);
        for (&samples) |*s| s.* = try benchOnce(smp, w_count);
        std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
        const median = samples[REPS / 2];
        if (baseline == null) baseline = median;
        const speedup = @as(f64, @floatFromInt(baseline.?)) / @as(f64, @floatFromInt(median));
        const per_task_us = @divTrunc(median, N_TASKS * 1000);
        std.debug.print(
            "  workers={d:>2}  wall {d:>6} us  per-task {d:>4} us  speedup {d:.2}x\n",
            .{ w_count, @divTrunc(median, 1000), per_task_us, speedup },
        );
    }
    std.debug.print("\nIdeal scaling = workers× speedup. Real scaling capped by serial spawn loop + cross-thread bookkeeping.\n", .{});
}

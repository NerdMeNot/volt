//! Bench — multi-driver fan-out scaling.
//!
//! Spawns N **driver coroutines**, each running its own spawn-join hot
//! loop. With N drivers and W workers, work-stealing has actual parallel
//! work to distribute (each driver's spawns are independent).
//!
//! Contrast with `bench-scaling`: that has ONE driver and measures
//! coordination overhead of multi-worker on serial work. This has
//! N drivers and measures whether the scheduler can keep multiple
//! independent producer/consumer pairs hot in parallel.
//!
//! Expected behavior: with `drivers >= workers`, total throughput
//! should scale with worker count. If it doesn't, the scheduler has
//! cross-driver coordination bleed that needs fixing.
//!
//! ## What this answers
//!
//! 1. Does Volt's scheduler scale linearly with cores when there's
//!    actual parallel work? (Should be yes — that's what work-stealing
//!    is for.)
//! 2. What's the per-spawn-join cost in the best case (multiple Ms
//!    each running their own driver, minimal cross-M traffic)?

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn nopFn() void {}

const DriverCtx = struct {
    batch: u32,
    deadline_ns: i128,
    ops: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn driverCoro(ctx: *DriverCtx) void {
    const rt = volt.runtime();
    const tasks = rt.allocator.alloc(*volt.Task(void), ctx.batch) catch @panic("driver alloc");
    defer rt.allocator.free(tasks);

    var local_ops: u64 = 0;
    while (nanosNow() < ctx.deadline_ns) {
        for (tasks) |*t| t.* = rt.spawn(nopFn, .{}) catch @panic("driver spawn");
        for (tasks) |t| t.join();
        local_ops += ctx.batch;
    }
    _ = ctx.ops.fetchAdd(local_ops, .acq_rel);
}

const RootCtx = struct {
    drivers: u32,
    batch: u32,
    deadline_ns: i128,
    total_ops: u64 = 0,
    elapsed_ns: i128 = 0,
};

fn benchRoot(rctx: *RootCtx) !void {
    const rt = volt.runtime();
    var dctx = DriverCtx{ .batch = rctx.batch, .deadline_ns = rctx.deadline_ns };

    const drivers = try rt.allocator.alloc(*volt.Task(void), rctx.drivers);
    defer rt.allocator.free(drivers);

    const start = nanosNow();
    for (drivers) |*d| d.* = try rt.spawn(driverCoro, .{&dctx});
    for (drivers) |d| d.join();
    rctx.elapsed_ns = nanosNow() - start;
    rctx.total_ops = dctx.ops.load(.acquire);
}

const Sample = struct { drivers: u32, workers: usize, total_ops: u64, elapsed_ns: i128 };

fn runOnce(allocator: std.mem.Allocator, drivers: u32, workers: usize, duration_s: u32, batch: u32) !Sample {
    var rctx = RootCtx{
        .drivers = drivers,
        .batch = batch,
        .deadline_ns = nanosNow() + @as(i128, duration_s) * std.time.ns_per_s,
    };
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();
    try (try rt.run(benchRoot, .{&rctx}));
    return .{ .drivers = drivers, .workers = workers, .total_ops = rctx.total_ops, .elapsed_ns = rctx.elapsed_ns };
}

const DURATION_S: u32 = 4;
const BATCH: u32 = 100;

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const ncpu = try std.Thread.getCpuCount();

    std.debug.print("=== fan-out scaling (multiple drivers) ===\n", .{});
    std.debug.print("Platform: ReleaseFast, batch={d}, duration={d}s, ncpu={d}\n", .{ BATCH, DURATION_S, ncpu });
    std.debug.print("Each driver: spawn {d} tasks, join all, repeat for {d}s.\n", .{ BATCH, DURATION_S });
    std.debug.print("\n", .{});

    // Sweep workers, with drivers == workers (peak parallelism).
    std.debug.print("Sweep with drivers = workers (peak parallel work):\n", .{});
    std.debug.print("  {s:>8} {s:>10} {s:>14} {s:>12} {s:>10}\n", .{ "workers", "drivers", "total_ops", "ns/op", "ratio" });
    var base_ns_per_op: f64 = 0;
    const widths = [_]usize{ 1, 2, 4, 8, ncpu };
    for (widths) |w| {
        const s = try runOnce(allocator, @intCast(w), w, DURATION_S, BATCH);
        const ns_per_op = @as(f64, @floatFromInt(s.elapsed_ns)) / @as(f64, @floatFromInt(s.total_ops));
        if (base_ns_per_op == 0) base_ns_per_op = ns_per_op;
        const ratio = ns_per_op / base_ns_per_op;
        std.debug.print("  {d:>8} {d:>10} {d:>14} {d:>10.1} {d:>9.2}×\n", .{ s.workers, s.drivers, s.total_ops, ns_per_op, ratio });
    }

    std.debug.print("\n", .{});
    std.debug.print("Throughput sweep (drivers > workers — oversubscribed):\n", .{});
    std.debug.print("  {s:>8} {s:>10} {s:>14} {s:>12}\n", .{ "workers", "drivers", "total_ops", "ops/sec" });
    for ([_]usize{ 1, 4, ncpu }) |w| {
        const s = try runOnce(allocator, @intCast(w * 4), w, DURATION_S, BATCH);
        const ops_per_sec = @divTrunc(@as(i128, s.total_ops) * std.time.ns_per_s, s.elapsed_ns);
        std.debug.print("  {d:>8} {d:>10} {d:>14} {d:>12}\n", .{ s.workers, s.drivers, s.total_ops, ops_per_sec });
    }

    std.debug.print("\n", .{});
    std.debug.print("Interpretation:\n", .{});
    std.debug.print("  - 'drivers = workers': each worker has its own driver; near-zero stealing needed.\n", .{});
    std.debug.print("  - If ratio stays near 1.0× as workers grow, the scheduler scales linearly.\n", .{});
    std.debug.print("  - If ratio grows, there's coordination bleed even with independent work.\n", .{});
}

//! Bench — v2 Mutex contended (8 coros × 50k acquires).
//!
//! Reference (BENCHMARKS.md):
//!   Volt v0.x:  41 ns/op (0.51× Go — already faster than Go via
//!               MCS + unparkLocal)
//!   Go sync.Mutex: 81 ns/op (1.0×)

const std = @import("std");
const volt2 = @import("volt2");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const Ctx = struct {
    mu: *volt2.Mutex,
    counter: *u64,
    iters: u32,
};

fn incLoop(ctx: *Ctx) void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        ctx.mu.lock();
        ctx.counter.* += 1;
        ctx.mu.unlock();
    }
}

fn benchOnce(allocator: std.mem.Allocator, workers: u32, iters: u32) !i128 {
    var rt = try volt2.Runtime.init(allocator);
    defer rt.deinit();

    var mu = volt2.Mutex{};
    var counter: u64 = 0;
    var ctx = Ctx{ .mu = &mu, .counter = &counter, .iters = iters };

    const tasks = try allocator.alloc(*volt2.Task(void), workers);
    defer allocator.free(tasks);

    const start = nanosNow();
    for (tasks) |*t| t.* = try rt.spawn(incLoop, .{&ctx});
    rt.run();
    const end = nanosNow();
    for (tasks) |t| t.join();

    std.debug.assert(counter == @as(u64, workers) * iters);
    return @divTrunc(end - start, @as(i128, workers) * @as(i128, iters));
}

const REPS = 11;
const WARMUPS = 3;
const WORKERS: u32 = 8;
const ITERS: u32 = 50_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== v2 Mutex contended ({d} coros × {d} acquires) ===\n", .{ WORKERS, ITERS });
    std.debug.print("Platform: ReleaseFast, reps={d}\n\n", .{REPS});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, WORKERS, ITERS);
    for (&samples) |*s| s.* = try benchOnce(smp, WORKERS, ITERS);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    std.debug.print("  v2 Mutex (contended): {d} ns/op (median of {d})\n", .{ median, REPS });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt v0.x: 41 ns/op (0.51× Go)\n", .{});
    std.debug.print("  Go:        81 ns/op (1.0×)\n", .{});
}

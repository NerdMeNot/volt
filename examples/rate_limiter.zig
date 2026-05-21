//! Rate-limited fan-out — bound concurrency via Semaphore.
//!
//! Use case: 100 jobs to process, but only 4 concurrent allowed.
//! Each worker acquires a permit, does the job, releases.
//!
//! Run: zig build run-rate-limiter

const std = @import("std");
const volt = @import("volt");

const CONCURRENCY_LIMIT = 4;
const TOTAL_JOBS = 100;

const Ctx = struct {
    sem: *volt.Semaphore,
    counter: *std.atomic.Value(u32),
    in_flight: *std.atomic.Value(u32),
    peak_in_flight: *std.atomic.Value(u32),
};

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    var sem = volt.Semaphore.init(CONCURRENCY_LIMIT);
    defer sem.deinit();
    var counter = std.atomic.Value(u32).init(0);
    var in_flight = std.atomic.Value(u32).init(0);
    var peak = std.atomic.Value(u32).init(0);

    var ctx = Ctx{
        .sem = &sem,
        .counter = &counter,
        .in_flight = &in_flight,
        .peak_in_flight = &peak,
    };

    const rt = volt.runtime();
    const tasks = try rt.allocator.alloc(*volt.Task(void), TOTAL_JOBS);
    defer rt.allocator.free(tasks);

    for (tasks) |*t| t.* = try volt.spawn(worker, .{&ctx});
    for (tasks) |t| _ = t.join();

    std.debug.print(
        \\rate-limiter:
        \\  total jobs        = {d}
        \\  concurrency cap   = {d}
        \\  peak in-flight    = {d}  (≤ cap proves the semaphore did its job)
        \\  jobs completed    = {d}
        \\
    , .{ TOTAL_JOBS, CONCURRENCY_LIMIT, peak.load(.acquire), counter.load(.acquire) });
}

fn worker(ctx: *Ctx) void {
    ctx.sem.acquire();
    defer ctx.sem.release();

    // Track concurrent in-flight count and update peak. This is the
    // assertion the semaphore is doing what we want.
    const now = ctx.in_flight.fetchAdd(1, .acq_rel) + 1;
    while (true) {
        const cur_peak = ctx.peak_in_flight.load(.acquire);
        if (now <= cur_peak) break;
        if (ctx.peak_in_flight.cmpxchgWeak(cur_peak, now, .acq_rel, .acquire) == null) break;
    }
    defer _ = ctx.in_flight.fetchSub(1, .acq_rel);

    // Simulate work — a short sleep on the reactor's timer (no
    // worker-thread time, fully cooperative).
    volt.sleep(volt.Duration.fromMillis(1)) catch {};
    _ = ctx.counter.fetchAdd(1, .acq_rel);
}

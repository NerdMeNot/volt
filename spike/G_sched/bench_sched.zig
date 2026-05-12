//! POC-G bench — spawn+join 10 K tasks via two schedulers.
//!
//! Task = decrement an atomic counter. Main "joins" by spin-loading
//! the counter to 0 (this isn't WaitGroup-with-park; it's bare polling
//! so we isolate the scheduler architecture, not the join primitive).
//!
//! Compare:
//!   sched_park: always-park-on-empty (current Volt-shaped)
//!   sched_spin: spin-then-park (Go-shaped)
//!
//! Target: spin variant ≤ 200 ns/task (Go does 145 ns/op for raw
//! spawn+done via WaitGroup; we expect close).

const std = @import("std");
const task_mod = @import("task.zig");
const park_mod = @import("sched_park.zig");
const spin_mod = @import("sched_spin.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

var remaining: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

fn benchTaskRun(_: *task_mod.Task) void {
    _ = remaining.fetchSub(1, .acq_rel);
}

fn benchParkOnce(allocator: std.mem.Allocator, n_workers: usize, n_tasks: u32) !i128 {
    var sched = try park_mod.Scheduler.init(allocator, n_workers);
    defer sched.deinit();
    try sched.start();

    const tasks = try allocator.alloc(task_mod.Task, n_tasks);
    defer allocator.free(tasks);
    for (tasks) |*t| t.* = .{ .run_fn = &benchTaskRun };
    remaining.store(n_tasks, .release);

    const start = nanosNow();
    for (tasks) |*t| sched.submit(t);
    while (remaining.load(.acquire) > 0) std.atomic.spinLoopHint();
    const end = nanosNow();

    sched.shutdownAndJoin();
    return @divTrunc(end - start, n_tasks);
}

fn benchSpinOnce(allocator: std.mem.Allocator, n_workers: usize, n_tasks: u32) !i128 {
    var sched = try spin_mod.Scheduler.init(allocator, n_workers);
    defer sched.deinit();
    try sched.start();

    const tasks = try allocator.alloc(task_mod.Task, n_tasks);
    defer allocator.free(tasks);
    for (tasks) |*t| t.* = .{ .run_fn = &benchTaskRun };
    remaining.store(n_tasks, .release);

    const start = nanosNow();
    for (tasks) |*t| sched.submit(t);
    while (remaining.load(.acquire) > 0) std.atomic.spinLoopHint();
    const end = nanosNow();

    sched.shutdownAndJoin();
    return @divTrunc(end - start, n_tasks);
}

const REPS = 7;
const WARMUPS = 2;
const N_TASKS: u32 = 10_000;

fn medianPark(allocator: std.mem.Allocator, n_workers: usize) !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchParkOnce(allocator, n_workers, N_TASKS);
    for (&samples) |*s| s.* = try benchParkOnce(allocator, n_workers, N_TASKS);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

fn medianSpin(allocator: std.mem.Allocator, n_workers: usize) !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchSpinOnce(allocator, n_workers, N_TASKS);
    for (&samples) |*s| s.* = try benchSpinOnce(allocator, n_workers, N_TASKS);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== POC-G: park vs spin-then-park scheduler ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, {d} tasks, {d} reps\n\n", .{ N_TASKS, REPS });

    inline for (.{ 1, 2, 4, 8, 11 }) |nw| {
        const park_ns = try medianPark(allocator, nw);
        const spin_ns = try medianSpin(allocator, nw);
        std.debug.print(
            "  workers={d:>2}  park: {d:>6} ns/task   spin: {d:>6} ns/task   spin/park: {d:.2}×\n",
            .{ nw, park_ns, spin_ns, @as(f64, @floatFromInt(spin_ns)) / @as(f64, @floatFromInt(park_ns)) },
        );
    }

    std.debug.print("\nReference: Go spawn+waitgroup = 149 ns/op (3.6× slower than POC-G's task bench would imply, because Go also has stackful goroutines).\n", .{});
    std.debug.print("Target: spin ≤ 200 ns/task at workers≥4 — see table for verdict.\n", .{});
}

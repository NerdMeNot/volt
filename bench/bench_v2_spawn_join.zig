//! Bench — v2 Runtime spawn+join with comptime-typed API.
//!
//! Compares against:
//!   * POC-C bare-floor (using raw spawn, no Task layer):       93 ns/op
//!   * Go spawn+waitgroup:                                      149 ns/op
//!   * Volt v0.x current:                                     4,163 ns/op
//!
//! v2 with the typed API adds:
//!   * Heap-alloc per-spawn Frame (sizeof depends on user fn args)
//!   * Heap-alloc per-spawn Task(T) handle
//!   * 1 extra atomic decrement (WG)
//!
//! Target: ≤ 150 ns/op (match Go even with full API).

const std = @import("std");
const volt2 = @import("volt2");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn nopFn() void {}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    var rt = volt2.Runtime.init(allocator);
    defer rt.deinit();
    const Task = volt2.Task(void);

    const tasks = try allocator.alloc(*Task, n);
    defer allocator.free(tasks);

    const start = nanosNow();
    for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
    rt.run();
    for (tasks) |t| t.join();
    const end = nanosNow();
    return @divTrunc(end - start, n);
}

const REPS = 11;
const WARMUPS = 3;
const N: u32 = 10_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== v2 spawn+join bench (typed API) ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n={d}, reps={d}\n", .{ N, REPS });
    std.debug.print("Runtime: single-worker, smp_allocator, 16 KiB stacks, typed spawn\n\n", .{});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N);
    for (&samples) |*s| s.* = try benchOnce(smp, N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];

    std.debug.print("  v2 spawn+task.join(): median {d} ns/op (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt v0.x:  4,163 ns/op (28.5× Go)\n", .{});
    std.debug.print("  POC-C raw:     93 ns/op (1.6× faster than Go)\n", .{});
    std.debug.print("  Go:           149 ns/op (1.0×)\n", .{});

    std.debug.print("\nVerdict: {s}\n", .{
        if (median <= 150) "PASS — typed API at or below Go" else if (median <= 250) "CLOSE — within ~1.7× Go" else "INVESTIGATE — too much typed-API overhead",
    });
}

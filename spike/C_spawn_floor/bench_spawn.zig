//! POC-C bench — bare-floor stackful spawn+join.
//!
//! Allocates a tiny runtime, spawns N no-op coroutines, runs to completion.
//! Single worker. Reports wall / N.
//!
//! This is the apples-to-apples comparison to Go's spawn+waitgroup
//! at 149 ns/op (Go is multi-worker; we'll add multi-worker in POC-C2 if needed).

const std = @import("std");
const rt_mod = @import("minirt.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn noopUser() callconv(.c) void {}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    var rt = rt_mod.Runtime.init(allocator);

    const start = nanosNow();
    var i: u32 = 0;
    while (i < n) : (i += 1) try rt.spawn(&noopUser);
    rt.run();
    const end = nanosNow();
    return @divTrunc(end - start, n);
}

const REPS = 11;
const WARMUPS = 3;
const N: u32 = 10_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== POC-C: bare-floor stackful spawn+join ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, n={d}, reps={d}\n", .{ N, REPS });
    std.debug.print("Stack: 16 KiB heap (no growth, no guard). Worker: spin only (no park).\n", .{});
    std.debug.print("Allocator: std.heap.smp_allocator (production, no leak tracking)\n\n", .{});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N);
    for (&samples) |*s| s.* = try benchOnce(smp, N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const min = samples[0];
    const median = samples[REPS / 2];
    const max = samples[REPS - 1];

    std.debug.print("  spawn+complete (smp_allocator): median {d} ns/op (min {d}, max {d})\n", .{ median, min, max });

    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt today (current src/):  4,163 ns/op (28.5× Go)\n", .{});
    std.debug.print("  Go (spawn+waitgroup):         149 ns/op (1.0× Go)\n", .{});
    std.debug.print("  POC-G task scheduler:          22 ns/task (no coros)\n", .{});

    std.debug.print("\nTarget: bare-floor stackful spawn+join ≤ 200 ns/op — {s}\n", .{
        if (median <= 200) "PASS — Go-class architecture is achievable"
        else if (median <= 500) "CLOSE — needs stack recycling"
        else "FAIL — investigate per-spawn overhead",
    });
}

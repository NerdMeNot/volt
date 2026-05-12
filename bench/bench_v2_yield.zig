//! Bench — v2 yield ping-pong (one-way ctx-switch cost via cooperative
//! scheduler dispatch).
//!
//! Pattern: one coroutine yields N times. Each yield = swap to main,
//! re-queue, pop, swap back. So per yield = 2 ctx swaps + dispatch.
//! ns/op divided by 2 for one-way.
//!
//! Reference numbers:
//!   POC-A wide-save raw ctx switch:    6 ns/swap
//!   Volt v0.x yield (BENCHMARKS.md):  11 ns/swap one-way
//!   Go runtime.Gosched:               42 ns/swap one-way

const std = @import("std");
const volt2 = @import("volt2");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn yieldNTimes(n: u32) void {
    var i: u32 = 0;
    while (i < n) : (i += 1) volt2.yield();
}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    var rt = try volt2.Runtime.init(allocator);
    defer rt.deinit();

    var task = try rt.spawn(yieldNTimes, .{n});

    const start = nanosNow();
    rt.run();
    const end = nanosNow();
    task.join();
    // Each yield = 2 swaps; divide for one-way cost.
    return @divTrunc(end - start, @as(i128, n) * 2);
}

const REPS = 11;
const WARMUPS = 3;
const N: u32 = 1_000_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== v2 yield (one-way ctx switch) bench ===\n", .{});
    std.debug.print("Platform: ReleaseFast, n={d}, reps={d}\n\n", .{ N, REPS });

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N);
    for (&samples) |*s| s.* = try benchOnce(smp, N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    std.debug.print("  v2 yield: {d} ns/op one-way (median of {d})\n", .{ median, REPS });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  POC-A wide ctx swap:         6 ns/swap\n", .{});
    std.debug.print("  Volt v0.x yield:            11 ns/op\n", .{});
    std.debug.print("  Go runtime.Gosched:         42 ns/op\n", .{});
}

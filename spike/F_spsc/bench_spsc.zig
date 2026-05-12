//! POC-F bench — SPSC channel send/recv pairs across two threads.
//!
//! Producer sends N monotonic u64s. Consumer drains N. Measure wall
//! time. Per-pair = wall / N.
//!
//! Baseline:
//!   Volt's Vyukov MPMC at cap=16:  180 ns/op
//!   Go's chan at cap=16:            33 ns/op
//! Target: POC-F SPSC ≤ 35 ns/op (i.e. match Go).

const std = @import("std");
const spsc_mod = @import("spsc.zig");

const Channel = spsc_mod.Spsc(u64, 16);

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn producer(ch: *Channel, n: u64) void {
    var i: u64 = 0;
    while (i < n) : (i += 1) ch.send(i);
}

fn consumer(ch: *Channel, n: u64, sum_out: *u64) void {
    var s: u64 = 0;
    var i: u64 = 0;
    while (i < n) : (i += 1) s +%= ch.recv();
    sum_out.* = s;
}

fn benchOnce(n: u64) !i128 {
    var ch = Channel{};
    var sum: u64 = 0;
    const start = nanosNow();
    const t_prod = try std.Thread.spawn(.{}, producer, .{ &ch, n });
    const t_cons = try std.Thread.spawn(.{}, consumer, .{ &ch, n, &sum });
    t_prod.join();
    t_cons.join();
    const end = nanosNow();
    std.mem.doNotOptimizeAway(sum);
    return @divTrunc(end - start, n);
}

const REPS = 11;
const WARMUPS = 2;
const N: u64 = 200_000;

pub fn main() !void {
    std.debug.print("\n=== POC-F: SPSC channel fast path ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, n={d}, reps={d}\n", .{ N, REPS });
    std.debug.print("Channel: cap=16, head/tail cache-line-padded, busy-spin on full/empty\n\n", .{});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(N);
    for (&samples) |*s| s.* = try benchOnce(N);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const min = samples[0];
    const median = samples[REPS / 2];
    const max = samples[REPS - 1];

    std.debug.print("  spsc send+recv (cap=16): median {d} ns/op (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt today (Vyukov MPMC): 180 ns/op (5.6× Go)\n", .{});
    std.debug.print("  Go (chan cap=16):          33 ns/op (1.0×)\n", .{});
    std.debug.print("\nTarget: POC-F SPSC ≤ 35 ns/op — {s}\n", .{
        if (median <= 35) "PASS — match Go on SPSC"
        else if (median <= 70) "CLOSE — within 2× of Go"
        else "FAIL — investigate per-op cost",
    });
}

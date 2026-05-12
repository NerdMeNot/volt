//! POC-D bench — parker ping-pong across two threads.
//!
//! Setup:
//!   * Two parkers (A, B), two threads.
//!   * Thread T_a alternates: park(A), unpark(B).
//!   * Thread T_b alternates: park(B), unpark(A).
//!   * Initial unpark from main thread kicks off T_a.
//!
//! Measure wall time over 1 M ping-pong cycles. Each "cycle" =
//! one park + one unpark on each side = 2 syscalls per side.
//! Report ns per park+unpark RT (wall / iters / 2).

const std = @import("std");
const pthread_mod = @import("parker_pthread.zig");
const ulock_mod = @import("parker_ulock.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn ParkerCtx(comptime Parker: type) type {
    return struct {
        my_parker: *Parker,
        peer_parker: *Parker,
        iters_remaining: std.atomic.Value(u64),
    };
}

fn pthreadThread(ctx: *ParkerCtx(pthread_mod.Parker)) void {
    while (ctx.iters_remaining.load(.acquire) > 0) {
        ctx.my_parker.park();
        if (ctx.iters_remaining.fetchSub(1, .acq_rel) <= 1) {
            ctx.peer_parker.unpark();
            return;
        }
        ctx.peer_parker.unpark();
    }
}

fn ulockThread(ctx: *ParkerCtx(ulock_mod.Parker)) void {
    while (ctx.iters_remaining.load(.acquire) > 0) {
        ctx.my_parker.park();
        if (ctx.iters_remaining.fetchSub(1, .acq_rel) <= 1) {
            ctx.peer_parker.unpark();
            return;
        }
        ctx.peer_parker.unpark();
    }
}

fn benchPthread(iters: u64) !i128 {
    var pa = pthread_mod.Parker{};
    pa.init();
    defer pa.deinit();
    var pb = pthread_mod.Parker{};
    pb.init();
    defer pb.deinit();
    var ctx_a = ParkerCtx(pthread_mod.Parker){
        .my_parker = &pa,
        .peer_parker = &pb,
        .iters_remaining = std.atomic.Value(u64).init(iters),
    };
    var ctx_b = ParkerCtx(pthread_mod.Parker){
        .my_parker = &pb,
        .peer_parker = &pa,
        .iters_remaining = std.atomic.Value(u64).init(iters),
    };
    const ta = try std.Thread.spawn(.{}, pthreadThread, .{&ctx_a});
    const tb = try std.Thread.spawn(.{}, pthreadThread, .{&ctx_b});

    // Tiny settle so both threads reach their first park.
    nanoSleep(1_000_000);

    const start = nanosNow();
    pa.unpark(); // kick off T_a
    ta.join();
    tb.join();
    const end = nanosNow();
    return @divTrunc(end - start, @as(i128, iters) * 2);
}

fn benchUlock(iters: u64) !i128 {
    var pa = ulock_mod.Parker{};
    var pb = ulock_mod.Parker{};
    var ctx_a = ParkerCtx(ulock_mod.Parker){
        .my_parker = &pa,
        .peer_parker = &pb,
        .iters_remaining = std.atomic.Value(u64).init(iters),
    };
    var ctx_b = ParkerCtx(ulock_mod.Parker){
        .my_parker = &pb,
        .peer_parker = &pa,
        .iters_remaining = std.atomic.Value(u64).init(iters),
    };
    const ta = try std.Thread.spawn(.{}, ulockThread, .{&ctx_a});
    const tb = try std.Thread.spawn(.{}, ulockThread, .{&ctx_b});

    nanoSleep(1_000_000);

    const start = nanosNow();
    pa.unpark();
    ta.join();
    tb.join();
    const end = nanosNow();
    return @divTrunc(end - start, @as(i128, iters) * 2);
}

fn nanoSleep(ns: u64) void {
    var ts = std.posix.timespec{ .sec = @intCast(@divTrunc(ns, std.time.ns_per_s)), .nsec = @intCast(@mod(ns, std.time.ns_per_s)) };
    var rem: std.posix.timespec = undefined;
    _ = std.posix.system.nanosleep(&ts, &rem);
}

const REPS = 11;
const WARMUPS = 2;
const ITERS_PER_REP: u64 = 100_000;

fn medianPthread() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchPthread(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchPthread(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

fn medianUlock() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchUlock(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchUlock(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

pub fn main() !void {
    std.debug.print("\n=== POC-D: Parker pthread_cond vs Darwin __ulock ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, iters/rep={d}, reps={d}\n\n", .{ ITERS_PER_REP, REPS });

    const pthread_ns = try medianPthread();
    const ulock_ns = try medianUlock();

    std.debug.print("  pthread_cond + mutex: {d} ns per park+unpark RT (median of {d})\n", .{ pthread_ns, REPS });
    std.debug.print("  __ulock_wait/_wake:   {d} ns per park+unpark RT (median of {d})\n", .{ ulock_ns, REPS });

    const delta = pthread_ns - ulock_ns;
    const pct: i128 = if (pthread_ns > 0) @divTrunc(delta * 100, pthread_ns) else 0;
    std.debug.print("  ulock is {d} ns faster ({d}% reduction)\n", .{ delta, pct });

    std.debug.print("\nTarget: ulock ≤ 150 ns/cycle — {s}\n", .{
        if (ulock_ns <= 150) "PASS" else if (ulock_ns <= 300) "CLOSE" else "FAIL",
    });
}

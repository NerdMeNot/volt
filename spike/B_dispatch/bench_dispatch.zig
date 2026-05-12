//! POC-B bench — N dispatch cycles via vtable vs enum.
//!
//! One coro yields N times. Each yield = swap into worker, dispatch
//! decision, swap back into coro. Bench measures wall / N.

const std = @import("std");
const vtable_mod = @import("dispatch_vtable.zig");
const enum_mod = @import("dispatch_enum.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn benchVtable(iters: u32) !i128 {
    var coro = vtable_mod.Coroutine{};
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", 16 * 1024);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + 16 * 1024;

    vtable_mod.bench_yields = iters;
    const start = nanosNow();
    vtable_mod.runLoop(&coro, stack_top);
    const end = nanosNow();
    return @divTrunc(end - start, iters);
}

fn benchEnum(iters: u32) !i128 {
    var coro = enum_mod.Coroutine{};
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", 16 * 1024);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + 16 * 1024;

    enum_mod.bench_yields = iters;
    const start = nanosNow();
    enum_mod.runLoop(&coro, stack_top);
    const end = nanosNow();
    return @divTrunc(end - start, iters);
}

const REPS = 11;
const WARMUPS = 3;
const ITERS_PER_REP: u32 = 1_000_000;

fn medianVtable() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchVtable(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchVtable(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

fn medianEnum() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchEnum(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchEnum(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

pub fn main() !void {
    std.debug.print("\n=== POC-B: vtable vs enum dispatch ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, iters/rep={d}, reps={d}\n\n", .{ ITERS_PER_REP, REPS });

    const vtable_ns = try medianVtable();
    const enum_ns = try medianEnum();

    std.debug.print("  vtable (subscribe_fn indirect): {d} ns/dispatch cycle (median of {d})\n", .{ vtable_ns, REPS });
    std.debug.print("  enum   (PendingKind switch):    {d} ns/dispatch cycle (median of {d})\n", .{ enum_ns, REPS });

    const delta = vtable_ns - enum_ns;
    const pct: i128 = if (vtable_ns > 0) @divTrunc(delta * 100, vtable_ns) else 0;
    std.debug.print("  enum saves {d} ns ({d}%)\n", .{ delta, pct });

    // "Dispatch cycle" = 2 ctx swaps + 1 dispatch call. We already know
    // 2 swaps = 12 ns from POC-A. So the dispatch overhead is the
    // residual: vtable_ns - 12.
    std.debug.print("\n  estimated dispatch overhead (subtract 2× ctx swap = ~12 ns):\n", .{});
    std.debug.print("    vtable: {d} ns   enum: {d} ns\n", .{ vtable_ns - 12, enum_ns - 12 });

    std.debug.print("\nTarget: enum ≤ 100 ns/dispatch cycle (= ≤ 88 ns dispatch overhead) — {s}\n", .{
        if (enum_ns <= 100) "PASS" else if (enum_ns <= 200) "CLOSE" else "FAIL",
    });
}

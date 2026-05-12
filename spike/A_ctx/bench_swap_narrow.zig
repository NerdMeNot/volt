//! POC-A bench — narrow-save ctx switch ping-pong, with the wide-save
//! version as a control measured in the same binary.
//!
//! 1 M iterations × 2 swaps each = 2 M one-way swaps per variant.
//! Reports ns/switch (one-way) for both. Run ≥ 11 iters; print median.

const std = @import("std");
const narrow = @import("ctx_narrow.zig");
const wide = @import("ctx_wide.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

// ─────────────────────────────────────────────────────────────────────
// Narrow
// ─────────────────────────────────────────────────────────────────────

const NarrowClosure = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
    main_ctx: *narrow.Context,
    coro_ctx: *narrow.Context,
    iters_remaining: u32,
    _pad: u32 = 0,
};

fn narrowRun(opaque_ptr: *anyopaque) callconv(.c) void {
    const c: *NarrowClosure = @ptrCast(@alignCast(opaque_ptr));
    while (c.iters_remaining > 0) {
        c.iters_remaining -= 1;
        narrow.swap(c.coro_ctx, c.main_ctx);
    }
    narrow.swap(c.coro_ctx, c.main_ctx);
    unreachable;
}

fn benchNarrow(iters: u32) !i128 {
    var main_ctx: narrow.Context = .{};
    var coro_ctx: narrow.Context = .{};
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", 16 * 1024);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + 16 * 1024;

    var closure = NarrowClosure{
        .run_fn = &narrowRun,
        .main_ctx = &main_ctx,
        .coro_ctx = &coro_ctx,
        .iters_remaining = iters,
    };
    narrow.initContext(&coro_ctx, stack_top, &closure);

    const start = nanosNow();
    while (closure.iters_remaining > 0) {
        narrow.swap(&main_ctx, &coro_ctx);
    }
    const end = nanosNow();
    narrow.swap(&main_ctx, &coro_ctx); // drive terminal swap
    return @divTrunc(end - start, @as(i128, iters) * 2);
}

// ─────────────────────────────────────────────────────────────────────
// Wide (control)
// ─────────────────────────────────────────────────────────────────────

const WideClosure = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
    main_ctx: *wide.Context,
    coro_ctx: *wide.Context,
    iters_remaining: u32,
    _pad: u32 = 0,
};

fn wideRun(opaque_ptr: *anyopaque) callconv(.c) void {
    const c: *WideClosure = @ptrCast(@alignCast(opaque_ptr));
    while (c.iters_remaining > 0) {
        c.iters_remaining -= 1;
        wide.swap(c.coro_ctx, c.main_ctx);
    }
    wide.swap(c.coro_ctx, c.main_ctx);
    unreachable;
}

fn benchWide(iters: u32) !i128 {
    var main_ctx: wide.Context = .{};
    var coro_ctx: wide.Context = .{};
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", 16 * 1024);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + 16 * 1024;

    var closure = WideClosure{
        .run_fn = &wideRun,
        .main_ctx = &main_ctx,
        .coro_ctx = &coro_ctx,
        .iters_remaining = iters,
    };
    wide.initContext(&coro_ctx, stack_top, &closure);

    const start = nanosNow();
    while (closure.iters_remaining > 0) {
        wide.swap(&main_ctx, &coro_ctx);
    }
    const end = nanosNow();
    wide.swap(&main_ctx, &coro_ctx);
    return @divTrunc(end - start, @as(i128, iters) * 2);
}

// ─────────────────────────────────────────────────────────────────────

const REPS = 11;
const WARMUPS = 3;
const ITERS_PER_REP: u32 = 1_000_000;

fn medianOfNarrow() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchNarrow(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchNarrow(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

fn medianOfWide() !i128 {
    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchWide(ITERS_PER_REP);
    for (&samples) |*s| s.* = try benchWide(ITERS_PER_REP);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    return samples[REPS / 2];
}

pub fn main() !void {
    std.debug.print("\n=== POC-A: narrow-save vs wide-save context switch ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, iters/rep={d}, reps={d}\n\n", .{ ITERS_PER_REP, REPS });

    const wide_ns = try medianOfWide();
    const narrow_ns = try medianOfNarrow();

    std.debug.print("  wide   (168 B Context, 14 saves): {d} ns/switch (one-way median of {d})\n", .{ wide_ns, REPS });
    std.debug.print("  narrow (104 B Context, 10 saves): {d} ns/switch (one-way median of {d})\n", .{ narrow_ns, REPS });

    const delta = wide_ns - narrow_ns;
    const pct = @divTrunc(delta * 100, wide_ns);
    std.debug.print("  delta: {d} ns ({d} %% faster with narrow)\n", .{ delta, pct });
    std.debug.print("\nTarget: narrow ≤ 7 ns/switch — {s}\n", .{
        if (narrow_ns <= 7) "PASS" else if (narrow_ns <= 10) "CLOSE" else "FAIL",
    });
}

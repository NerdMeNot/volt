//! Benchmarks for the spike's two key claims:
//!   1. Context switch latency  (target: ≤ 200ns on M-series ARM64)
//!   2. Spawn cost (target: ≤ 1µs)
//!
//! Run: `zig run -O ReleaseFast bench_switch.zig`

const std = @import("std");
const ctx = @import("context_arm64.zig");
const rt_mod = @import("runtime.zig");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 1: raw context switch (two contexts, no scheduler)
// ─────────────────────────────────────────────────────────────────────────────

const PingClosure = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
    main_ctx: *ctx.Context,
    coro_ctx: *ctx.Context,
    iters_remaining: u32,
    _pad: u32 = 0,
};

fn pingRun(opaque_ptr: *anyopaque) callconv(.c) void {
    const c: *PingClosure = @ptrCast(@alignCast(opaque_ptr));
    while (c.iters_remaining > 0) {
        c.iters_remaining -= 1;
        ctx.swap(c.coro_ctx, c.main_ctx);
    }
    ctx.swap(c.coro_ctx, c.main_ctx);
    unreachable;
}

fn benchContextSwitch(iters: u32) !void {
    var main_ctx: ctx.Context = .{};
    var coro_ctx: ctx.Context = .{};

    const stack_size = 16 * 1024;
    const stack = try std.heap.page_allocator.alignedAlloc(u8, .@"16", stack_size);
    defer std.heap.page_allocator.free(stack);
    const stack_top: [*]u8 = stack.ptr + stack_size;

    var closure = PingClosure{
        .run_fn = &pingRun,
        .main_ctx = &main_ctx,
        .coro_ctx = &coro_ctx,
        .iters_remaining = iters,
    };

    ctx.initContext(&coro_ctx, stack_top, &closure);

    // Warmup
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        if (closure.iters_remaining == 0) break;
        ctx.swap(&main_ctx, &coro_ctx);
    }

    // Reset
    closure.iters_remaining = iters;

    const start = nanosNow();
    while (closure.iters_remaining > 0) {
        ctx.swap(&main_ctx, &coro_ctx);
    }
    const end = nanosNow();
    // Drive coro to its terminal swap (not counted).
    ctx.swap(&main_ctx, &coro_ctx);

    const total_ns = end - start;
    // Each iter does TWO swaps (out and back), so divide by 2 for one-way cost.
    const ns_per_switch = @divTrunc(total_ns, @as(i128, iters) * 2);

    std.debug.print(
        "  context switch: {} iters in {}µs = {}ns/switch (one-way)\n",
        .{ iters, @divTrunc(total_ns, std.time.ns_per_us), ns_per_switch },
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 2: spawn cost via the runtime
// ─────────────────────────────────────────────────────────────────────────────

fn noopWorker() void {}

fn benchSpawnCost(allocator: std.mem.Allocator, count: u32) !void {
    var rt = rt_mod.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Warmup
    var w: u32 = 0;
    while (w < 100) : (w += 1) {
        try rt.spawn(noopWorker, .{});
    }
    rt.run();

    // Measure: spawn N coroutines, then run them all to completion.
    // We measure the spawn loop only (not the run).
    const start_spawn = nanosNow();
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        try rt.spawn(noopWorker, .{});
    }
    const end_spawn = nanosNow();

    const start_run = nanosNow();
    rt.run();
    const end_run = nanosNow();

    const spawn_ns_total = end_spawn - start_spawn;
    const ns_per_spawn = @divTrunc(spawn_ns_total, count);

    const run_ns_total = end_run - start_run;
    const ns_per_dispatch = @divTrunc(run_ns_total, count);

    std.debug.print(
        "  spawn:    {} coros in {}µs = {}ns/spawn\n",
        .{ count, @divTrunc(spawn_ns_total, std.time.ns_per_us), ns_per_spawn },
    );
    std.debug.print(
        "  dispatch: {} coros run in {}µs = {}ns/dispatch (spawn→entry→exit)\n",
        .{ count, @divTrunc(run_ns_total, std.time.ns_per_us), ns_per_dispatch },
    );
}

pub fn main() !void {
    std.debug.print("\n=== Volt stackful coroutine benchmarks ===\n", .{});
    std.debug.print("Platform: Darwin-ARM64, ReleaseFast\n\n", .{});

    std.debug.print("[1] Raw context switch (best case, two ctxs ping-pong):\n", .{});
    try benchContextSwitch(1_000_000);

    std.debug.print("\n[2] Spawn + dispatch cost via DebugAllocator (gpa):\n", .{});
    {
        var gpa: std.heap.DebugAllocator(.{}) = .init;
        defer _ = gpa.deinit();
        try benchSpawnCost(gpa.allocator(), 10_000);
    }

    std.debug.print("\n[3] Spawn + dispatch cost via page_allocator (slow but no debug check):\n", .{});
    try benchSpawnCost(std.heap.page_allocator, 10_000);

    std.debug.print("\n=== Targets ===\n", .{});
    std.debug.print("  context switch: target ≤200ns/switch    (Go ~150ns)\n", .{});
    std.debug.print("  spawn:          target ≤1000ns/spawn   (Go ~3µs)\n", .{});
}

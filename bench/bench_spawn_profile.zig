//! Spawn+join decomposition bench.
//!
//! The combined `spawn+join` bench measures ~4 µs/op but we don't know
//! where that time actually goes. This bench breaks the operation into
//! phases and measures each independently:
//!
//! 1. **Launch phase** — time only the launch loop (10k coros spawned,
//!    measure end-to-end of launches, NOT waiting for completion).
//! 2. **Join phase** — time only the join loop (assumes coros already
//!    finished; measures join-fast-path cost).
//! 3. **Combined wall** — same as bench_core's spawn+join; for sanity.
//! 4. **Single-worker** — same with workers=1. Tells us how much of
//!    the cost is work-stealing overhead vs intrinsic per-spawn cost.
//! 5. **Dispatch-only** — spawn 1 coro, time its dispatch + run + done
//!    cycle. The minimum cost to fire a no-op coroutine through the
//!    runtime.
//!
//! Run with: zig build bench-profile

const std = @import("std");
const volt = @import("volt");
const bench_allocator = std.heap.smp_allocator;

fn nop() void {}

// ─────────────────────────────────────────────────────────────────────
// 1+2. Launch vs join split.
// ─────────────────────────────────────────────────────────────────────

const SplitCtx = struct {
    n: u32,
    launch_ns: i128 = 0,
    join_ns: i128 = 0,
    total_ns: i128 = 0,
};

fn splitRoot(ctx: *SplitCtx) !void {
    const n = ctx.n;
    const jobs = try bench_allocator.alloc(*volt.Job, n);
    defer bench_allocator.free(jobs);
    defer for (jobs) |j| volt.destroyJob(j);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(nop, .{});
    const t_launch_done = volt.time.nanoTimestamp();
    for (jobs) |j| try j.join();
    const t_join_done = volt.time.nanoTimestamp();

    ctx.launch_ns = t_launch_done - t0;
    ctx.join_ns = t_join_done - t_launch_done;
    ctx.total_ns = t_join_done - t0;
}

fn benchSplit(label: []const u8, n: u32, workers: ?usize) !void {
    var ctx = SplitCtx{ .n = n };
    try volt.run(
        .{ .allocator = bench_allocator, .workers = workers },
        splitRoot,
        .{&ctx},
    );
    std.debug.print(
        "  {s:32}  launch: {d:>6} ns/op  join: {d:>6} ns/op  total: {d:>6} ns/op\n",
        .{
            label,
            @divFloor(ctx.launch_ns, n),
            @divFloor(ctx.join_ns, n),
            @divFloor(ctx.total_ns, n),
        },
    );
}

// ─────────────────────────────────────────────────────────────────────
// 5. Single-spawn dispatch cost.
// ─────────────────────────────────────────────────────────────────────
//
// Bench: spawn 1 coro, join it, measure wall time. The MINIMUM cost
// to fire a no-op coroutine through the runtime (allocate Frame + Job,
// dispatch on a worker, run nop, fire Done, lifecycle release).
//
// Repeat N times to average out timer noise.

fn dispatchOnceRoot(_: u32) !u64 {
    const j = try volt.launch(nop, .{});
    defer volt.destroyJob(j);
    try j.join();
    return 0;
}

fn benchDispatchOnce(iters: u32) !void {
    const t0 = volt.time.nanoTimestamp();
    var i: u32 = 0;
    while (i < iters) : (i += 1) {
        _ = try volt.run(
            .{ .allocator = bench_allocator, .workers = 1 },
            dispatchOnceRoot,
            .{i},
        );
    }
    const t1 = volt.time.nanoTimestamp();
    std.debug.print(
        "  {s:32}  per iter: {d:>6} ns  (includes Runtime.init/deinit, workers=1)\n",
        .{ "single-spawn-single-worker", @divFloor(t1 - t0, iters) },
    );
}

// ─────────────────────────────────────────────────────────────────────
// Entry
// ─────────────────────────────────────────────────────────────────────

pub fn main() !void {
    std.debug.print(
        "=== Volt spawn+join profile ({s} {s}) ===\n",
        .{ @tagName(@import("builtin").cpu.arch), @tagName(@import("builtin").os.tag) },
    );

    std.debug.print("\n[10k coros, default workers]\n", .{});
    try benchSplit("default-workers", 10_000, null);
    try benchSplit("default-workers", 10_000, null);
    try benchSplit("default-workers", 10_000, null);

    std.debug.print("\n[10k coros, single-worker]\n", .{});
    try benchSplit("workers=1", 10_000, 1);
    try benchSplit("workers=1", 10_000, 1);
    try benchSplit("workers=1", 10_000, 1);

    std.debug.print("\n[10k coros, 2 workers]\n", .{});
    try benchSplit("workers=2", 10_000, 2);
    try benchSplit("workers=2", 10_000, 2);

    std.debug.print("\n[Runtime spin-up amortization — 100 single-spawn cycles]\n", .{});
    try benchDispatchOnce(100);
}

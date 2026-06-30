//! Regression test — spawnBlocking closure-lifetime deadlock.
//!
//! Guards the fix in `lib.zig`'s `spawnBlocking`: the pool thread's
//! `unparkOne(self.rt, &self.done)` dereferences the caller's
//! stack-allocated closure AFTER setting `done`. Before the fix, a
//! coroutine could observe `done` (via parkOn's validator), return,
//! and free that stack frame before `unparkOne` ran — a use-after-
//! free that woke the wrong coroutine and deadlocked the scheduler
//! (all workers parked, all run queues empty, N coroutines lost).
//!
//! The bug only surfaced under bursty concurrency (many coroutines
//! each issuing hundreds of blocking ops) and was intermittent
//! (~40% per burst), so this driver runs MANY fresh-Runtime bursts
//! and a watchdog OS thread aborts non-zero if forward progress
//! stalls. fs-free on purpose — isolates the scheduler ↔
//! spawnBlocking wakeup path. Run: `zig build bench-blocking-deadlock-repro`.

const std = @import("std");
const volt = @import("volt");

extern "c" fn usleep(usec: c_uint) c_int;

const N_COROS: u32 = 64;
const OPS_PER_CORO: u32 = 200;
const BLOCK_US: c_uint = 40; // ~ a cold 4 KiB read
// Fresh Runtime + burst per iteration. The two bugs this guards were
// timing races (~40% per burst for the closure UAF; far rarer for the
// parkWorker lost-wakeup, ~1/8 only on an 11-core host). No finite
// count is a hard guarantee, so this runs many bursts; run it a few
// times in CI for a strong probabilistic gate.
const ITERATIONS: u32 = 300;

const Ctx = struct {
    completed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn workerCoro(ctx: *Ctx) void {
    var i: u32 = 0;
    while (i < OPS_PER_CORO) : (i += 1) {
        volt.spawnBlocking(slowBlock, .{}) catch {};
    }
    _ = ctx.completed.fetchAdd(1, .acq_rel);
}

fn slowBlock() void {
    _ = usleep(BLOCK_US);
}

const RootCtx = struct {
    ctx: *Ctx,
};

fn root(rc: *RootCtx) !void {
    const rt = volt.runtime();
    const WorkerTask = @TypeOf(try rt.spawn(workerCoro, .{@as(*Ctx, undefined)}));
    var tasks: [N_COROS]WorkerTask = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(workerCoro, .{rc.ctx});
    for (tasks) |t| t.join();
}

// Watchdog: bumps-tracked global iteration counter; if it doesn't
// advance for 20s the workload is wedged → dump + abort non-zero.
var g_iter = std.atomic.Value(u32).init(0);

fn watchdog() void {
    var last: u32 = 0;
    var stalls: u32 = 0;
    while (true) {
        _ = usleep(2_000_000);
        const cur = g_iter.load(.acquire);
        if (cur >= ITERATIONS) return;
        if (cur == last) stalls += 1 else {
            stalls = 0;
            last = cur;
        }
        if (stalls >= 10) {
            std.debug.print("\n!!! WATCHDOG: spawnBlocking burst wedged at iteration {d}/{d} — DEADLOCK REGRESSION !!!\n", .{ cur, ITERATIONS });
            std.process.exit(99);
        }
    }
}

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    const wd = try std.Thread.spawn(.{}, watchdog, .{});
    wd.detach();

    std.debug.print(
        "regression: {d} iterations × {d} coros × {d} spawnBlocking ops\n",
        .{ ITERATIONS, N_COROS, OPS_PER_CORO },
    );

    var iter: u32 = 0;
    while (iter < ITERATIONS) : (iter += 1) {
        var rt = try volt.Runtime.init(.{ .allocator = smp });
        defer rt.deinit();
        var ctx = Ctx{};
        var rc = RootCtx{ .ctx = &ctx };
        try (try rt.run(root, .{&rc}));
        if (ctx.completed.load(.acquire) != N_COROS) {
            std.debug.print("iteration {d}: only {d}/{d} coros completed\n", .{ iter, ctx.completed.load(.acquire), N_COROS });
            std.process.exit(98);
        }
        _ = g_iter.store(iter + 1, .release);
    }

    std.debug.print("PASS — {d} iterations, no deadlock\n", .{ITERATIONS});
}

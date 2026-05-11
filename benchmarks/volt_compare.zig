//! Volt vs Go comparative benchmarks (Volt side).
//!
//! Emits JSON with median over N iterations so the orchestrator can
//! compare against the Go side that runs the same workloads.
//!
//! Workloads:
//!   yield_one_way:        2-coroutine ping-pong (one-way ctx switch)
//!   spawn_join:           10k spawns + joins
//!   channel_spsc_16:      100k sends/recvs through cap-16 channel
//!   mutex_contended_8:    8 coroutines × 50k acquires on shared mutex
//!
//! Build/run (handled by compare.zig): see benchmarks/compare.zig.

const std = @import("std");
const volt = @import("volt");

const WARMUP_ITERS = 3;
const BENCH_ITERS = 11; // odd → single median sample

const bench_allocator = std.heap.smp_allocator;

// ─────────────────────────────────────────────────────────────────────
// Workload: yield ping-pong
// ─────────────────────────────────────────────────────────────────────

fn yielder(iters: u32) !void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) try volt.yield();
}

fn yieldRoot(iters: u32) !u64 {
    const t0 = volt.time.nanoTimestamp();
    var t = try volt.spawn(yielder, .{iters});
    defer volt.destroyTask(t);
    try t.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchYield(iters: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, yieldRoot, .{iters});
    // Each yield = 2 ctx swaps; divide by 2 for one-way.
    return wall / (iters * 2);
}

// ─────────────────────────────────────────────────────────────────────
// Workload: spawn + join
// ─────────────────────────────────────────────────────────────────────

fn spawnNop() void {}

fn spawnJoinRoot(n: u32) !u64 {
    const jobs = try bench_allocator.alloc(*volt.Job, n);
    defer bench_allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(spawnNop, .{});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchSpawnJoin(n: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, spawnJoinRoot, .{n});
    return wall / n;
}

// Apples-to-apples with Go's `go func + wg.Wait`: spawn N coros that
// `defer wg.done()`, parent calls `wg.wait()`. ONE park-unpark cycle
// regardless of N, vs the 10k×park-unpark of `for (jobs) |j| j.join()`.

fn wgCoro(wg: *volt.sync.WaitGroup) void {
    wg.done();
}
fn spawnWaitgroupRoot(n: u32) !u64 {
    var wg = volt.sync.WaitGroup{};
    wg.add(n);
    const jobs = try bench_allocator.alloc(*volt.Job, n);
    defer bench_allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(wgCoro, .{&wg});
    defer for (jobs) |j| volt.destroyJob(j);
    try wg.wait();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}
fn benchSpawnWaitgroup(n: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, spawnWaitgroupRoot, .{n});
    return wall / n;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: channel SPSC cap=16
// ─────────────────────────────────────────────────────────────────────

const ChCtx = struct { ch: *volt.channel.Channel(u64), n: u64 };

fn chProd(ctx: *ChCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}
fn chCons(ctx: *ChCtx) !u64 {
    var sum: u64 = 0;
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return sum,
            else => return err,
        };
        sum +%= v;
    }
}
fn channelRoot(n: u64) !u64 {
    var ch = try volt.channel.Channel(u64).init(bench_allocator, 16);
    defer ch.deinit();
    var ctx = ChCtx{ .ch = &ch, .n = n };
    const t0 = volt.time.nanoTimestamp();
    var prod = try volt.spawn(chProd, .{&ctx});
    defer volt.destroyTask(prod);
    var cons = try volt.spawn(chCons, .{&ctx});
    defer volt.destroyTask(cons);
    try prod.join();
    _ = try cons.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}
fn benchChannelSpsc(n: u64) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, channelRoot, .{n});
    return wall / n;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: mutex contended (8 × 50k)
// ─────────────────────────────────────────────────────────────────────

const MuCtx = struct { mu: *volt.sync.Mutex, counter: u64 = 0, iters: u32 };

fn muInc(ctx: *MuCtx) !void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        try ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn mutexRoot(workers: u32, iters: u32) !u64 {
    var mu = volt.sync.Mutex{};
    var ctx = MuCtx{ .mu = &mu, .iters = iters };
    const jobs = try bench_allocator.alloc(*volt.Job, workers);
    defer bench_allocator.free(jobs);
    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(muInc, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.assert(ctx.counter == @as(u64, workers) * iters);
    return @intCast(wall);
}
fn benchMutex(workers: u32, iters: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, mutexRoot, .{ workers, iters });
    return wall / (@as(u64, workers) * iters);
}

// ─────────────────────────────────────────────────────────────────────
// Runner — median over N iterations
// ─────────────────────────────────────────────────────────────────────

fn runMedian(comptime fn_ptr: anytype, args: anytype) !u64 {
    var samples: [BENCH_ITERS]u64 = undefined;
    var w: u32 = 0;
    while (w < WARMUP_ITERS) : (w += 1) _ = try @call(.auto, fn_ptr, args);
    for (&samples) |*s| s.* = try @call(.auto, fn_ptr, args);
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    return samples[BENCH_ITERS / 2];
}

pub fn main() !void {
    const yield_ns = try runMedian(benchYield, .{@as(u32, 100_000)});
    const spawn_ns = try runMedian(benchSpawnJoin, .{@as(u32, 10_000)});
    const spawn_wg_ns = try runMedian(benchSpawnWaitgroup, .{@as(u32, 10_000)});
    const channel_ns = try runMedian(benchChannelSpsc, .{@as(u64, 100_000)});
    const mutex_ns = try runMedian(benchMutex, .{ @as(u32, 8), @as(u32, 50_000) });

    // JSON to stderr — std.debug.print is the simplest cross-version
    // stdio path in Zig 0.16. The orchestrator captures stderr.
    std.debug.print(
        "{{\n" ++
            "  \"yield_one_way_ns\": {d},\n" ++
            "  \"spawn_join_ns\": {d},\n" ++
            "  \"spawn_waitgroup_ns\": {d},\n" ++
            "  \"channel_spsc_16_ns\": {d},\n" ++
            "  \"mutex_contended_8_ns\": {d}\n" ++
            "}}\n",
        .{ yield_ns, spawn_ns, spawn_wg_ns, channel_ns, mutex_ns },
    );
}

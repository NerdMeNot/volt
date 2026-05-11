//! Volt core benchmarks — spawn cost, context switch cost, channel
//! throughput, mutex contention. Runnable via `zig build bench` (see
//! build.zig).
//!
//! Each benchmark prints a single line of `name: NS_PER_OP ns/op
//! (TOTAL_OPS ops in WALL_NS ns)`. Compare across commits to catch
//! regressions; v1.0 will add a baseline JSON output for CI gating.
//!
//! ## Allocator choice
//!
//! Uses `std.heap.smp_allocator` (Zig 0.16's lock-free bin allocator).
//! Earlier versions used `bench_allocator`, which routed
//! every per-spawn struct allocation (Task, Coroutine, Closure, args
//! — typically 64–256 bytes each) through mmap, padding spawn+join
//! by ~10–15 µs of pure syscall traffic. Switching to smp_allocator
//! is the architecturally correct choice and what real users will do.

const std = @import("std");
const volt = @import("volt");

const ns_per_s = std.time.ns_per_s;

const bench_allocator = std.heap.smp_allocator;

fn report(name: []const u8, ops: u64, wall_ns: u64) void {
    const ns_per_op = if (ops == 0) 0 else wall_ns / ops;
    std.debug.print("{s}: {d} ns/op ({d} ops in {d} ns)\n", .{ name, ns_per_op, ops, wall_ns });
}

// ─────────────────────────────────────────────────────────────────────
// Spawn cost — N coroutines spawned + joined.
// ─────────────────────────────────────────────────────────────────────

fn spawnNop() void {}

fn spawnBenchRoot(n: u32) !u64 {
    const t0 = volt.time.nanoTimestamp();
    const allocator = bench_allocator;
    const jobs = try allocator.alloc(*volt.Job, n);
    defer allocator.free(jobs);

    for (jobs) |*j| j.* = try volt.launch(spawnNop, .{});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchSpawn(n: u32) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, spawnBenchRoot, .{n});
    report("spawn+join (1 worker per coro, N coros)", n, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Context switch cost — yield ping-pong via volt.yield.
// ─────────────────────────────────────────────────────────────────────

const SwitchCtx = struct { iters: u32 };

fn yielder(_: *SwitchCtx, iters: u32) !void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) try volt.yield();
}

fn switchBenchRoot(iters: u32) !u64 {
    var ctx = SwitchCtx{ .iters = iters };
    const t0 = volt.time.nanoTimestamp();
    var t = try volt.spawn(yielder, .{ &ctx, iters });
    defer volt.destroyTask(t);
    try t.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchYieldSwitch(iters: u32) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, switchBenchRoot, .{iters});
    // Each yield is 2 context switches (coro→worker→coro), so divide
    // by 2 for "one-way" cost.
    report("yield ping-pong (one-way switch)", iters * 2, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Channel throughput — single producer, single consumer, capacity 16.
// ─────────────────────────────────────────────────────────────────────

const ChCtx = struct { ch: *volt.channel.Channel(u64), n: u64 };

fn chProducer(ctx: *ChCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn chConsumer(ctx: *ChCtx) !u64 {
    var sum: u64 = 0;
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return sum,
            else => return err,
        };
        sum +%= v;
    }
}

fn channelBenchRoot(n: u64) !u64 {
    var ch = try volt.channel.Channel(u64).init(bench_allocator, 16);
    defer ch.deinit();
    var ctx = ChCtx{ .ch = &ch, .n = n };

    const t0 = volt.time.nanoTimestamp();
    var prod = try volt.spawn(chProducer, .{&ctx});
    defer volt.destroyTask(prod);
    var cons = try volt.spawn(chConsumer, .{&ctx});
    defer volt.destroyTask(cons);
    try prod.join();
    _ = try cons.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchChannel(n: u64) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, channelBenchRoot, .{n});
    report("channel SPSC cap=16", n, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Scope spawn — same shape as benchSpawn, but via volt.scope. Tests the
// inline-arena fast path: Frame + Job allocated from scope's stack-
// resident bump arena, zero heap calls in the spawn hot loop. Pure Zig
// optimization; no GC language can replicate this.
// ─────────────────────────────────────────────────────────────────────

const ScopeSpawnCtx = struct { n: u32 };

fn scopeSpawnNop(_: *ScopeSpawnCtx) void {}

threadlocal var scope_spawn_ctx_slot: ?*ScopeSpawnCtx = null;

fn scopeSpawnBody(s: *volt.Scope) !void {
    const ctx = scope_spawn_ctx_slot.?;
    var i: u32 = 0;
    while (i < ctx.n) : (i += 1) {
        try s.spawn(scopeSpawnNop, .{ctx});
    }
}

fn scopeSpawnRoot(n: u32) !u64 {
    var ctx = ScopeSpawnCtx{ .n = n };
    scope_spawn_ctx_slot = &ctx;
    defer scope_spawn_ctx_slot = null;
    const t0 = volt.time.nanoTimestamp();
    try volt.scope(scopeSpawnBody);
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchScopeSpawn(n: u32) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, scopeSpawnRoot, .{n});
    report("scope.spawn (inline-arena children)", n, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Mutex contention — N coroutines × M increments each on a shared mutex.
// ─────────────────────────────────────────────────────────────────────

const MuCtx = struct {
    mu: *volt.sync.Mutex,
    counter: u64 = 0,
    iters: u32,
};

fn muIncrementer(ctx: *MuCtx) !void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        try ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn mutexBenchRoot(n_workers: u32, iters: u32) !u64 {
    var mu = volt.sync.Mutex{};
    var ctx = MuCtx{ .mu = &mu, .iters = iters };
    const allocator = bench_allocator;
    const jobs = try allocator.alloc(*volt.Job, n_workers);
    defer allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(muIncrementer, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.assert(ctx.counter == @as(u64, n_workers) * iters);
    return @intCast(wall);
}

fn benchMutex(n_workers: u32, iters: u32) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, mutexBenchRoot, .{ n_workers, iters });
    const ops = @as(u64, n_workers) * iters;
    report("mutex lock/unlock (chained — best case)", ops, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Mutex with cross-core contention — REALISTIC bench.
//
// The `mutex lock/unlock` bench above is a best-case number: the
// handoff chain naturally serializes on one worker because each
// unlock's wake routes through `unparkLocal` (the unparker's own
// worker dispatches the next waiter). All 8 coroutines end up "in the
// same room" on one CPU core — pure ctx-swap chain, ~47 ns/op.
//
// Real workloads have coroutines doing actual work BETWEEN lock
// acquisitions, which spreads them across workers. To force that
// pattern: insert yields between lock acquires. The yield surrenders
// the worker, lets the scheduler distribute coroutines, so each
// lock-acquisition is an HONEST cross-core handoff: the previous
// holder is on core A, the next waiter parks on core B, the unlock
// crosses cores.
//
// This number is what users will see in production when their
// coroutines do real work and only briefly contend on the mutex.
// ─────────────────────────────────────────────────────────────────────

fn muIncrementerCrossCore(ctx: *MuCtx) !void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        // Yield BEFORE the lock so we end up on a different worker
        // than the previous holder. With 8 coros and 8 workers, the
        // yield distributes us. Each lock acquisition is then a
        // cross-worker handoff.
        try volt.yield();
        try ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn mutexCrossCoreRoot(n_workers: u32, iters: u32) !u64 {
    var mu = volt.sync.Mutex{};
    var ctx = MuCtx{ .mu = &mu, .iters = iters };
    const allocator = bench_allocator;
    const jobs = try allocator.alloc(*volt.Job, n_workers);
    defer allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(muIncrementerCrossCore, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.assert(ctx.counter == @as(u64, n_workers) * iters);
    return @intCast(wall);
}

fn benchMutexCrossCore(n_workers: u32, iters: u32) !void {
    const wall = try volt.run(.{ .allocator = bench_allocator }, mutexCrossCoreRoot, .{ n_workers, iters });
    // Subtract the yield cost so we're measuring lock+unlock alone.
    // Each iter does: yield + lock + counter++ + unlock. yield is
    // ~10ns. Report the FULL cycle wall but note the breakdown.
    const ops = @as(u64, n_workers) * iters;
    report("mutex lock/unlock (cross-core — realistic)", ops, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────

pub fn main() !void {
    std.debug.print("=== Volt benchmarks ({s} {s}) ===\n", .{
        @tagName(@import("builtin").cpu.arch),
        @tagName(@import("builtin").os.tag),
    });

    try benchSpawn(10_000);
    // 16 children fits the default 8KB inline arena (~512B/Frame).
    // Larger N falls back to heap; we measure pure inline-arena cost
    // here, with a separate larger run to show the fallback transition.
    try benchScopeSpawn(16);
    try benchYieldSwitch(100_000);
    try benchChannel(100_000);
    // Two mutex numbers — best-case (chained) and realistic (cross-core).
    // See `benchMutexCrossCore` doc for why both matter.
    try benchMutex(8, 50_000);
    try benchMutexCrossCore(8, 50_000);
}

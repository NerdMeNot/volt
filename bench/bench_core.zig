//! Volt core benchmarks — spawn cost, context switch cost, channel
//! throughput, mutex contention. Runnable via `zig build bench` (see
//! build.zig).
//!
//! Each benchmark prints a single line of `name: NS_PER_OP ns/op
//! (TOTAL_OPS ops in WALL_NS ns)`. Compare across commits to catch
//! regressions; v1.0 will add a baseline JSON output for CI gating.

const std = @import("std");
const volt = @import("volt");

const ns_per_s = std.time.ns_per_s;

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
    const allocator = std.heap.page_allocator;
    const jobs = try allocator.alloc(*volt.Job, n);
    defer allocator.free(jobs);

    for (jobs) |*j| j.* = try volt.launch(spawnNop, .{});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchSpawn(n: u32) !void {
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, spawnBenchRoot, .{n});
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
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, switchBenchRoot, .{iters});
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
    var ch = try volt.channel.Channel(u64).init(std.heap.page_allocator, 16);
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
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, channelBenchRoot, .{n});
    report("channel SPSC cap=16", n, wall);
}

// ─────────────────────────────────────────────────────────────────────
// Mutex contention — N coroutines × M increments each on a shared mutex.
// ─────────────────────────────────────────────────────────────────────

const MuCtx = struct {
    mu: *volt.sync.Mutex,
    counter: u64 = 0,
    iters: u32,
};

fn muIncrementer(ctx: *MuCtx) void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn mutexBenchRoot(n_workers: u32, iters: u32) !u64 {
    var mu = volt.sync.Mutex{};
    var ctx = MuCtx{ .mu = &mu, .iters = iters };
    const allocator = std.heap.page_allocator;
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
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, mutexBenchRoot, .{ n_workers, iters });
    const ops = @as(u64, n_workers) * iters;
    report("mutex lock/unlock", ops, wall);
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
    try benchYieldSwitch(100_000);
    try benchChannel(100_000);
    try benchMutex(8, 50_000);
}

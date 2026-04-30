//! Targeted stress harness for the multi-worker scheduler. The goal is to
//! reproduce the spawn/join/IO hang reliably so we can trace it.

const std = @import("std");
const volt = @import("../lib.zig");
const syscall = volt.internal.syscall;

// ─── 1. spawn → join hammering ────────────────────────────────────────────────

fn smallReturn(seed: u64) u64 {
    return seed *% 0x9E3779B97F4A7C15;
}

fn spawnJoinBatch(n: u32) !u64 {
    var sum: u64 = 0;
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        var t = try volt.spawn(smallReturn, .{@as(u64, i)});
        defer volt.destroyTask(t);
        sum +%= try t.join();
    }
    return sum;
}

fn spawnJoinManyRoot(iters: u32) !u64 {
    var total: u64 = 0;
    var i: u32 = 0;
    while (i < iters) : (i += 1) {
        total +%= try spawnJoinBatch(64);
    }
    return total;
}

test "stress: 200x spawn/join batches" {
    _ = try volt.run(.{ .allocator = std.testing.allocator }, spawnJoinManyRoot, .{@as(u32, 200)});
}

// ─── 2. parallel spawn + join all at end ──────────────────────────────────────

fn parallelBatchRoot(n: u32) !u64 {
    var counter = std.atomic.Value(u64).init(0);
    const Worker = struct {
        fn go(c: *std.atomic.Value(u64)) void {
            _ = c.fetchAdd(1, .monotonic);
        }
    };
    const jobs = try std.testing.allocator.alloc(*volt.Job, n);
    defer std.testing.allocator.free(jobs);
    for (jobs) |*j| j.* = try volt.launch(Worker.go, .{&counter});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return counter.load(.acquire);
}

test "stress: 2000 parallel coroutines, all joined" {
    const n = try volt.run(.{ .allocator = std.testing.allocator }, parallelBatchRoot, .{@as(u32, 2000)});
    try std.testing.expectEqual(@as(u64, 2000), n);
}

// ─── 3. pipe ping-pong, repeated ─────────────────────────────────────────────

const PipeCtx = struct {
    rfd: i32,
    wfd: i32,
    received: usize = 0,
};

fn pipeReader(ctx: *PipeCtx) !void {
    var buf: [4]u8 = undefined;
    const n = try volt.io.lowlevel.read(ctx.rfd, &buf);
    ctx.received = n;
}

fn pipeWriter(ctx: *PipeCtx) !void {
    try volt.yield();
    try volt.io.lowlevel.writeAll(ctx.wfd, "ping");
}

fn pipeOnce() !void {
    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);

    var ctx = PipeCtx{ .rfd = fds[0], .wfd = fds[1] };
    var reader = try volt.spawn(pipeReader, .{&ctx});
    defer volt.destroyTask(reader);
    var writer = try volt.spawn(pipeWriter, .{&ctx});
    defer volt.destroyTask(writer);

    try reader.join();
    try writer.join();
    try std.testing.expectEqual(@as(usize, 4), ctx.received);
}

fn pipeStressRoot(iters: u32) !void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) try pipeOnce();
}

test "stress: 100x pipe ping-pong inside one runtime" {
    try volt.run(.{ .allocator = std.testing.allocator }, pipeStressRoot, .{@as(u32, 100)});
}

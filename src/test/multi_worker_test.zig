//! v0.3 multi-worker tests — verify the work-stealing scheduler is actually
//! running coroutines on multiple OS threads in parallel.

const std = @import("std");
const volt = @import("../lib.zig");

// ─────────────────────────────────────────────────────────────────────────────
// 1. Distinct OS thread IDs observed across coroutines.
//
//    Each child stamps its `std.Thread.getCurrentId()` into a slot, then
//    yields a few times and stamps again. With N>1 workers, at least two
//    distinct thread IDs should show up — proving that the scheduler is
//    not just running everything on the bootstrap thread.
// ─────────────────────────────────────────────────────────────────────────────

const ThreadIdProbe = struct {
    ids: [16]std.atomic.Value(u64),

    fn record(self: *ThreadIdProbe, idx: usize) void {
        const id: u64 = @intCast(std.Thread.getCurrentId());
        self.ids[idx].store(id, .release);
    }
};

fn probeWorker(probe: *ThreadIdProbe, slot: usize) !void {
    probe.record(slot);
    // Give other workers a chance to schedule too.
    var k: u32 = 0;
    while (k < 8) : (k += 1) try volt.yield();
    probe.record(slot);
}

fn probeRoot() !ThreadIdProbe {
    var probe: ThreadIdProbe = .{ .ids = undefined };
    for (&probe.ids) |*v| v.* = std.atomic.Value(u64).init(0);

    const N = 16;
    var tasks: [N]*volt.Task(@TypeOf(probeWorker)) = undefined;
    for (&tasks, 0..) |*t, i| {
        t.* = try volt.spawn(probeWorker, .{ &probe, i });
    }
    defer for (tasks) |t| volt.destroyTask(t);
    for (tasks) |t| try t.join();

    return probe;
}

test "v0.3: coroutines observe at least two distinct OS thread IDs" {
    // Skip on machines reporting 1 CPU — not a runtime bug, just a test
    // that needs multiple cores to be meaningful.
    const cpus = std.Thread.getCpuCount() catch 1;
    if (cpus < 2) return error.SkipZigTest;

    const probe = try volt.run(.{ .allocator = std.testing.allocator }, probeRoot, .{});

    var seen = std.AutoHashMap(u64, void).init(std.testing.allocator);
    defer seen.deinit();
    for (probe.ids) |a| {
        const id = a.load(.acquire);
        if (id != 0) try seen.put(id, {});
    }
    try std.testing.expect(seen.count() >= 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Stress: many coroutines + cross-worker join.
//
//    Spawn N coroutines that each do a small amount of CPU work and increment
//    a shared atomic counter. With work-stealing, the spawning worker hands
//    them out across the pool. Verify all N completions land.
// ─────────────────────────────────────────────────────────────────────────────

fn cpuTouch(counter: *std.atomic.Value(u64), iters: u64) void {
    var sum: u64 = 0;
    var i: u64 = 0;
    while (i < iters) : (i += 1) sum +%= i *% 0x9E3779B97F4A7C15;
    // Use sum so the optimizer can't elide the loop.
    if (sum == 0xDEADBEEFCAFEBABE) std.debug.panic("unreachable: sum collision", .{});
    _ = counter.fetchAdd(1, .monotonic);
}

fn stressRoot() !u64 {
    var counter = std.atomic.Value(u64).init(0);
    const N = 256;
    var jobs: [N]*volt.Job = undefined;
    for (&jobs) |*j| {
        j.* = try volt.launch(cpuTouch, .{ &counter, @as(u64, 10_000) });
    }
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return counter.load(.acquire);
}

test "v0.3: 256 CPU-bound coroutines all complete across the worker pool" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, stressRoot, .{});
    try std.testing.expectEqual(@as(u64, 256), total);
}

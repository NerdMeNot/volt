//! Same as bench_spawn_hot_loop but workers=1 — isolates per-spawn
//! intrinsic cost from multi-worker coordination overhead.

const std = @import("std");
const volt = @import("volt");

const bench_allocator = std.heap.smp_allocator;

fn nop() void {}

fn hotLoopRoot(total: u32) !void {
    const batch: u32 = 10_000;
    const batches = total / batch;
    const jobs = try bench_allocator.alloc(*volt.Job, batch);
    defer bench_allocator.free(jobs);

    var b: u32 = 0;
    while (b < batches) : (b += 1) {
        for (jobs) |*j| j.* = try volt.launch(nop, .{});
        for (jobs) |j| try j.join();
        for (jobs) |j| volt.destroyJob(j);
    }
}

pub fn main() !void {
    const total: u32 = 1_000_000;
    const t0 = volt.time.nanoTimestamp();
    try volt.run(.{ .allocator = bench_allocator, .workers = 1 }, hotLoopRoot, .{total});
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.print(
        "spawn+join hot loop (workers=1): {d} coros in {d} ms = {d} ns/op\n",
        .{ total, @divFloor(wall, 1_000_000), @divFloor(wall, total) },
    );
}

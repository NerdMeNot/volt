//! Tight spawn+join hot-loop for sampling profilers (samply, Instruments).
//!
//! Runs ONE Runtime, then spawns 1,000,000 no-op coroutines in batches
//! of 10k and joins them. Wall-clock target: 4-5 seconds so a 1 kHz
//! sampler gets ~4-5k samples on the hot path.
//!
//! Run via:   samply record ./zig-out/bin/volt-bench-spawn-hot-loop

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
    try volt.run(.{ .allocator = bench_allocator }, hotLoopRoot, .{total});
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.print(
        "spawn+join hot loop: {d} coros in {d} ms = {d} ns/op\n",
        .{ total, @divFloor(wall, 1_000_000), @divFloor(wall, total) },
    );
}

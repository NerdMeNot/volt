//! Cookbook: offload CPU-bound work to the blocking thread pool.
//!
//! `volt.spawnBlocking` submits a function to a separate thread pool
//! so the async workers can keep dispatching coroutines while the CPU
//! work runs. The coroutine that called `spawnBlocking` parks on the
//! returned task and resumes when it finishes.
//!
//! Use when:
//!   - You have synchronous, CPU-heavy code (parsing, hashing,
//!     compression) that would otherwise block an async worker for a
//!     long time.
//!   - You're calling into a library that does its own blocking I/O.
//!
//! Don't use when:
//!   - The work is already async-friendly (just spawn a normal task).
//!   - The work is microsecond-scale (dispatching to the pool costs
//!     more than the work).

const std = @import("std");
const volt = @import("volt");

fn cpuHash(seed: u64) u64 {
    var s = seed;
    var i: u64 = 0;
    while (i < 5_000_000) : (i += 1) {
        s = s *% 6364136223846793005 +% 1442695040888963407;
    }
    return s;
}

// `spawnBlocking` parks the *calling* coroutine until the work
// finishes, so to run multiple blocking jobs concurrently we spawn one
// async wrapper per job and join them.

const Sink = struct {
    out: [3]u64 = .{ 0, 0, 0 },
};

fn worker(sink: *Sink, idx: usize, seed: u64) !void {
    sink.out[idx] = try volt.spawnBlocking(cpuHash, .{seed});
}

fn root() !void {
    const start = volt.Instant.now();
    var sink = Sink{};
    const j1 = try volt.launch(worker, .{ &sink, @as(usize, 0), @as(u64, 1) });
    const j2 = try volt.launch(worker, .{ &sink, @as(usize, 1), @as(u64, 2) });
    const j3 = try volt.launch(worker, .{ &sink, @as(usize, 2), @as(u64, 3) });
    defer volt.destroyJob(j1);
    defer volt.destroyJob(j2);
    defer volt.destroyJob(j3);

    try j1.join();
    try j2.join();
    try j3.join();

    const elapsed_ms = start.elapsed().asMillis();
    std.debug.print("3 hashes done in {d} ms: {x} {x} {x}\n", .{
        elapsed_ms, sink.out[0], sink.out[1], sink.out[2],
    });
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}

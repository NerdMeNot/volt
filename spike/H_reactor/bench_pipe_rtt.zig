//! POC-H bench — pipe ping-pong RTT.
//!
//! Two coroutines connected by a pair of pipes (A→B, B→A). They
//! exchange N rounds of 1 KB each. Same IO mechanics as TCP echo
//! (non-blocking fds, kqueue wait, coroutine yield on EAGAIN) but
//! without the socket overhead.
//!
//! Reference (from BENCHMARKS.md):
//!   TCP echo Volt today (multi-client, kqueue): 10,960 ns/RTT
//!   TCP echo Go (multi-client, netpoll):          9,050 ns/RTT
//!   Volt gap is 21% — believed-to-be reactor wakeFn + scheduler overhead.
//!
//! POC-H bench is serial (1 producer + 1 consumer), so per-RTT is
//! lower than the parallel TCP echo. But the shape is the same: each
//! RTT requires read-side EAGAIN+wait, kqueue wake, resume, plus
//! write-side EAGAIN+wait, kqueue wake, resume. Multiplied by 2 (both
//! directions per RTT).

const std = @import("std");
const rt_mod = @import("minirt.zig");
const posix = std.posix;

const MSG_SIZE: usize = 1024;
const N_RTT: u32 = 10_000;

fn nanosNow() i128 {
    var ts: posix.timespec = undefined;
    _ = posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const Ctx = extern struct {
    rd_fd: i32,
    wr_fd: i32,
    n: u32,
};

var g_ctx_server: Ctx = undefined;
var g_ctx_client: Ctx = undefined;

fn serverCoro(coro: *rt_mod.Coroutine) callconv(.c) void {
    // Read 1 KB, write 1 KB back. N rounds.
    var buf: [MSG_SIZE]u8 = undefined;
    var i: u32 = 0;
    while (i < g_ctx_server.n) : (i += 1) {
        _ = rt_mod.readAsync(coro, g_ctx_server.rd_fd, &buf) catch return;
        _ = rt_mod.writeAsync(coro, g_ctx_server.wr_fd, &buf) catch return;
    }
}

fn clientCoro(coro: *rt_mod.Coroutine) callconv(.c) void {
    var send_buf: [MSG_SIZE]u8 = undefined;
    for (&send_buf, 0..) |*b, idx| b.* = @intCast(idx & 0xff);
    var recv_buf: [MSG_SIZE]u8 = undefined;
    var i: u32 = 0;
    while (i < g_ctx_client.n) : (i += 1) {
        _ = rt_mod.writeAsync(coro, g_ctx_client.wr_fd, &send_buf) catch return;
        _ = rt_mod.readAsync(coro, g_ctx_client.rd_fd, &recv_buf) catch return;
    }
}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    // pipe A (client write → server read)
    var fds_a: [2]i32 = undefined;
    if (std.c.pipe(&fds_a) != 0) return error.PipeFailed;
    defer _ = std.c.close(fds_a[0]);
    defer _ = std.c.close(fds_a[1]);
    const pipe_a: [2]i32 = fds_a;
    // pipe B (server write → client read)
    var fds_b: [2]i32 = undefined;
    if (std.c.pipe(&fds_b) != 0) return error.PipeFailed;
    defer _ = std.c.close(fds_b[0]);
    defer _ = std.c.close(fds_b[1]);
    const pipe_b: [2]i32 = fds_b;
    try rt_mod.setNonblock(pipe_a[0]);
    try rt_mod.setNonblock(pipe_a[1]);
    try rt_mod.setNonblock(pipe_b[0]);
    try rt_mod.setNonblock(pipe_b[1]);

    g_ctx_server = .{ .rd_fd = pipe_a[0], .wr_fd = pipe_b[1], .n = n };
    g_ctx_client = .{ .rd_fd = pipe_b[0], .wr_fd = pipe_a[1], .n = n };

    var rt = try rt_mod.Runtime.init(allocator);
    defer rt.deinit();
    _ = try rt.spawn(&serverCoro);
    _ = try rt.spawn(&clientCoro);

    const start = nanosNow();
    rt.run();
    const end = nanosNow();
    return @divTrunc(end - start, n);
}

const REPS = 7;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== POC-H: tight reactor pipe RTT ===\n", .{});
    std.debug.print("Platform: Darwin-arm64, ReleaseFast, n={d}, reps={d}\n", .{ N_RTT, REPS });
    std.debug.print("IO: 1 KB write + 1 KB read per RTT, single-worker, kqueue blocking poll\n\n", .{});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N_RTT);
    for (&samples) |*s| s.* = try benchOnce(smp, N_RTT);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];

    std.debug.print("  pipe RTT (1 KB): median {d} ns/RTT (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Volt today TCP echo:  10,960 ns/RTT (1.21× Go)\n", .{});
    std.debug.print("  Go TCP echo:           9,050 ns/RTT (1.0×)\n", .{});
    std.debug.print("\nNote: POC-H is serial pipe (not TCP, not 64-parallel). Per-RTT cost should be\n", .{});
    std.debug.print("BELOW the parallel-TCP number since there's no socket overhead and no\n", .{});
    std.debug.print("multi-client contention.\n", .{});
}

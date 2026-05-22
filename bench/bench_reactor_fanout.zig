//! Bench — high-fd-pressure TCP echo for measuring reactor contention.
//!
//! Goal: pressure the reactor / scheduler interaction at scale where
//! single-reactor contention (one polling worker, global `pending`
//! counter, kqueue serialization) becomes the bottleneck.
//!
//! Shape: 512 concurrent clients × 64 RTT × 16 B payload, on a
//! NumCPU-worker runtime. Each RTT involves ~8 reactor operations
//! (waitReadable, waitWritable on both server and client). At 512
//! clients × 64 RTT × 8 ops = ~262k reactor ops per run. Long enough
//! to amortize warmup; short enough to keep variance bounded.
//!
//! Used to validate Lane 4 (per-P reactor) hypothesis. Baseline
//! number with current single-reactor design lives in CLAUDE.md;
//! a per-P implementation should beat it by ≥15 % to justify the
//! refactor (per issue #7's acceptance criteria).

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const MSG: usize = 16;
const REQS_PER_CLIENT: u32 = 64;
const N_CLIENTS: u32 = 512;

const ServerCtx = struct {
    listener: *volt.net.TcpListener,
    expected_conns: u32,
};

fn echoConn(stream: volt.net.TcpStream) void {
    var s = stream;
    defer s.close();
    var buf: [MSG]u8 = undefined;
    var i: u32 = 0;
    while (i < REQS_PER_CLIENT) : (i += 1) {
        const got = s.readFull(&buf) catch return;
        if (got < MSG) return;
        s.writeAll(buf[0..MSG]) catch return;
    }
}

fn server(ctx: *ServerCtx) void {
    var i: u32 = 0;
    while (i < ctx.expected_conns) : (i += 1) {
        const stream = ctx.listener.accept() catch return;
        const t = (volt.current.require().runtime);
        const rt: *volt.Runtime = @ptrCast(@alignCast(t));
        _ = rt.spawn(echoConn, .{stream}) catch return;
    }
}

const ClientCtx = struct {
    addr: volt.net.Address,
};

fn client(ctx: *ClientCtx) void {
    var stream = volt.net.TcpStream.connect(ctx.addr) catch return;
    defer stream.close();
    var send_buf: [MSG]u8 = undefined;
    for (&send_buf, 0..) |*b, idx| b.* = @intCast(idx & 0xff);
    var recv_buf: [MSG]u8 = undefined;
    var i: u32 = 0;
    while (i < REQS_PER_CLIENT) : (i += 1) {
        stream.writeAll(&send_buf) catch return;
        const got = stream.readFull(&recv_buf) catch return;
        if (got < MSG) return;
    }
}

const RootCtx = struct {
    wall_ns: i128 = 0,
};

fn root(ctx: *RootCtx) !void {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var server_ctx = ServerCtx{ .listener = &listener, .expected_conns = N_CLIENTS };
    const t = volt.current.require().runtime;
    const rt: *volt.Runtime = @ptrCast(@alignCast(t));
    var server_task = try rt.spawn(server, .{&server_ctx});

    var client_ctx = ClientCtx{ .addr = addr };

    const start = nanosNow();
    const clients = try rt.allocator.alloc(*volt.Task(void), N_CLIENTS);
    defer rt.allocator.free(clients);
    for (clients) |*ct| ct.* = try rt.spawn(client, .{&client_ctx});
    for (clients) |ct| ct.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
    server_task.join();
}

fn benchOnce(allocator: std.mem.Allocator) !i128 {
    var rt = try volt.Runtime.init(.{ .allocator = allocator });
    defer rt.deinit();
    var ctx = RootCtx{};
    try (try rt.run(root, .{&ctx}));
    const total_rtts: i128 = @as(i128, N_CLIENTS) * @as(i128, REQS_PER_CLIENT);
    return @divTrunc(ctx.wall_ns, total_rtts);
}

const REPS = 7;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== reactor fan-out bench ===\n", .{});
    std.debug.print("Shape: {d} clients × {d} RTT × {d} B (NumCPU workers)\n", .{ N_CLIENTS, REQS_PER_CLIENT, MSG });
    std.debug.print("Total RTTs: {d} per sample, reps={d}\n\n", .{ N_CLIENTS * REQS_PER_CLIENT, REPS });

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp);
    for (&samples) |*s| s.* = try benchOnce(smp);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];
    const aggregate_ops_per_sec = @divTrunc(1_000_000_000 * @as(i128, N_CLIENTS) * @as(i128, REQS_PER_CLIENT), median * @as(i128, N_CLIENTS) * @as(i128, REQS_PER_CLIENT));
    _ = aggregate_ops_per_sec; // unused — printout below

    // Per-RTT median + aggregate throughput.
    const throughput_per_sec = @divTrunc(@as(i128, 1_000_000_000), median);
    std.debug.print("  per-RTT: median {d} ns (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("  throughput: {d} RTT/s per-client equivalent\n", .{throughput_per_sec});
    std.debug.print("  aggregate: ~{d} RTTs total across the {d}-way fanout\n", .{
        @divTrunc(@as(i128, 1_000_000_000) * @as(i128, N_CLIENTS), median),
        N_CLIENTS,
    });
}

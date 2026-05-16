//! Bench — TCP echo, 64 concurrent clients × 16 RTT × 1 KB.
//!
//! Reference: Go's net package over loopback measures ~9,050 ns/RTT
//! on this hardware. Target: at-or-below Go.

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const MSG: usize = 1024;
const REQS_PER_CLIENT: u32 = 16;

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
        // Fire-and-forget echo connection. The task is freed by the
        // runtime via WG (no Task handle).
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
    n_clients: u32,
    wall_ns: i128 = 0,
};

fn root(ctx: *RootCtx) !void {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var server_ctx = ServerCtx{ .listener = &listener, .expected_conns = ctx.n_clients };
    const t = volt.current.require().runtime;
    const rt: *volt.Runtime = @ptrCast(@alignCast(t));
    var server_task = try rt.spawn(server, .{&server_ctx});

    var client_ctx = ClientCtx{ .addr = addr };

    const start = nanosNow();
    const clients = try rt.allocator.alloc(*volt.Task(void), ctx.n_clients);
    defer rt.allocator.free(clients);
    for (clients) |*ct| ct.* = try rt.spawn(client, .{&client_ctx});
    for (clients) |ct| ct.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
    server_task.join();
}

fn benchOnce(allocator: std.mem.Allocator, n_clients: u32) !i128 {
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    var ctx = RootCtx{ .n_clients = n_clients };
    try (try rt.run(root, .{&ctx}));
    const total_rtts: i128 = @as(i128, n_clients) * @as(i128, REQS_PER_CLIENT);
    return @divTrunc(ctx.wall_ns, total_rtts);
}

const REPS = 7;
const WARMUPS = 2;
const N_CLIENTS: u32 = 64;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== TCP echo bench (64 clients × 16 RTT × 1 KB) ===\n", .{});
    std.debug.print("Platform: ReleaseFast, reps={d}\n\n", .{REPS});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N_CLIENTS);
    for (&samples) |*s| s.* = try benchOnce(smp, N_CLIENTS);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];

    std.debug.print("  TCP echo: median {d} ns/RTT (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference:\n", .{});
    std.debug.print("  Go:          9,050 ns/RTT (1.0×)\n", .{});

    std.debug.print("\nVerdict: {s}\n", .{
        if (median <= 9500) "PASS — at or below Go" else if (median <= 11000) "CLOSE — within 1.21× of Go" else "INVESTIGATE — too much overhead",
    });
}

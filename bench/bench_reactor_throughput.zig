//! Bench — reactor throughput. Single-connection TCP ping-pong with
//! a 1-byte payload, tight loop, single worker.
//!
//! Each ping is two reactor cycles (one wake at A's read, one at B's
//! read). With a 1-byte payload the syscall + copy cost is negligible
//! so the dominant cost per ping is `register + park + kernel-deliver
//! + unpark + dispatch`. Useful for cross-platform comparison —
//! kqueue vs epoll vs io_uring vs IOCP all front the same interface
//! and this bench is the tightest receipt for "did the backend pull
//! its weight".
//!
//! Single worker because we're measuring reactor cost, not scheduler
//! parallelism. Multi-worker shapes are covered by `bench-tcp-echo`
//! and `bench-fanout-scaling`.

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const N_PINGS: u32 = 100_000;

const PeerCtx = struct {
    stream: volt.net.TcpStream,
    n: u32,
};

fn pingerPeer(ctx: *PeerCtx) void {
    var s = ctx.stream;
    defer s.close();
    var buf: [1]u8 = .{0};
    var i: u32 = 0;
    while (i < ctx.n) : (i += 1) {
        s.writeAll(&buf) catch return;
        _ = s.readFull(&buf) catch return;
    }
}

fn pongerPeer(ctx: *PeerCtx) void {
    var s = ctx.stream;
    defer s.close();
    var buf: [1]u8 = .{0};
    var i: u32 = 0;
    while (i < ctx.n) : (i += 1) {
        _ = s.readFull(&buf) catch return;
        s.writeAll(&buf) catch return;
    }
}

const RootCtx = struct {
    n: u32,
    wall_ns: i128 = 0,
};

fn root(ctx: *RootCtx) !void {
    const rt = volt.runtime();

    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    // Spawn ponger as the accept side. It blocks on accept until the
    // pinger connects below.
    const AcceptCtx = struct {
        listener: *volt.net.TcpListener,
        n: u32,
    };
    const acceptFn = struct {
        fn f(c: *AcceptCtx) void {
            const conn = c.listener.accept() catch return;
            var p = PeerCtx{ .stream = conn, .n = c.n };
            pongerPeer(&p);
        }
    }.f;
    var accept_ctx = AcceptCtx{ .listener = &listener, .n = ctx.n };
    var ponger = try rt.spawn(acceptFn, .{&accept_ctx});

    const pinger_stream = try volt.net.TcpStream.connect(addr);
    var pinger_ctx = PeerCtx{ .stream = pinger_stream, .n = ctx.n };
    var pinger = try rt.spawn(pingerPeer, .{&pinger_ctx});

    const start = nanosNow();
    pinger.join();
    ponger.join();
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, n: u32) !i128 {
    // Single worker: reactor cost only, no cross-worker noise.
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    var ctx = RootCtx{ .n = n };
    try (try rt.run(root, .{&ctx}));
    // Each ping is 2 reactor wakes (pinger's read + ponger's read).
    const total_wakes: i128 = @as(i128, n) * 2;
    return @divTrunc(ctx.wall_ns, total_wakes);
}

const REPS = 7;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== Reactor throughput bench ===\n", .{});
    std.debug.print("Single connection, 1-byte payload, 1 worker, n={d} pings × 2 wakes\n", .{N_PINGS});
    std.debug.print("Each sample = ns per (register + park + kernel deliver + unpark + dispatch)\n\n", .{});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, N_PINGS);
    for (&samples) |*s| s.* = try benchOnce(smp, N_PINGS);
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

    const median = samples[REPS / 2];
    const min = samples[0];
    const max = samples[REPS - 1];

    std.debug.print("  reactor wake: median {d} ns/wake (min {d}, max {d})\n", .{ median, min, max });
    std.debug.print("\nReference: this is the tightest cross-platform shape we measure;\n", .{});
    std.debug.print("kqueue/epoll/io_uring/IOCP numbers should track within ~2× of each other\n", .{});
    std.debug.print("on similarly-clocked hardware. Outliers point at a backend bug.\n", .{});
}

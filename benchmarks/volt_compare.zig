//! Volt vs Go comparative benchmarks (Volt side).
//!
//! Emits JSON with median over N iterations so the orchestrator can
//! compare against the Go side that runs the same workloads.
//!
//! Workloads:
//!   yield_one_way:        2-coroutine ping-pong (one-way ctx switch)
//!   spawn_join:           10k spawns + joins
//!   channel_spsc_16:      100k sends/recvs through cap-16 channel
//!   mutex_contended_8:    8 coroutines × 50k acquires on shared mutex
//!
//! Build/run (handled by compare.zig): see benchmarks/compare.zig.

const std = @import("std");
const volt = @import("volt");

const WARMUP_ITERS = 3;
const BENCH_ITERS = 11; // odd → single median sample

const bench_allocator = std.heap.smp_allocator;

// ─────────────────────────────────────────────────────────────────────
// Workload: yield ping-pong
// ─────────────────────────────────────────────────────────────────────

fn yielder(iters: u32) !void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) try volt.yield();
}

fn yieldRoot(iters: u32) !u64 {
    const t0 = volt.time.nanoTimestamp();
    var t = try volt.spawn(yielder, .{iters});
    defer volt.destroyTask(t);
    try t.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchYield(iters: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, yieldRoot, .{iters});
    // Each yield = 2 ctx swaps; divide by 2 for one-way.
    return wall / (iters * 2);
}

// ─────────────────────────────────────────────────────────────────────
// Workload: spawn + join
// ─────────────────────────────────────────────────────────────────────

fn spawnNop() void {}

fn spawnJoinRoot(n: u32) !u64 {
    const jobs = try bench_allocator.alloc(*volt.Job, n);
    defer bench_allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(spawnNop, .{});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchSpawnJoin(n: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, spawnJoinRoot, .{n});
    return wall / n;
}

// Apples-to-apples with Go's `go func + wg.Wait`: spawn N coros that
// signal done via an atomic counter, await the counter (not per-coro
// join). Removes the 10k×park/unpark cycles that `for (jobs) |j| j.join()`
// incurs and matches Go's WaitGroup pattern.

const WgCtx = struct {
    remaining: std.atomic.Value(u32),
    done_notify: *volt.sync.Notify,
};
fn wgCoro(ctx: *WgCtx) void {
    if (ctx.remaining.fetchSub(1, .acq_rel) == 1) {
        ctx.done_notify.notifyOne();
    }
}
fn spawnWaitgroupRoot(n: u32) !u64 {
    var notify = volt.sync.Notify{};
    var ctx = WgCtx{
        .remaining = std.atomic.Value(u32).init(n),
        .done_notify = &notify,
    };
    const jobs = try bench_allocator.alloc(*volt.Job, n);
    defer bench_allocator.free(jobs);

    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(wgCoro, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    try notify.wait();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}
fn benchSpawnWaitgroup(n: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, spawnWaitgroupRoot, .{n});
    return wall / n;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: channel SPSC cap=16
// ─────────────────────────────────────────────────────────────────────

const ChCtx = struct { ch: *volt.channel.Channel(u64), n: u64 };

fn chProd(ctx: *ChCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}
fn chCons(ctx: *ChCtx) !u64 {
    var sum: u64 = 0;
    while (true) {
        const v = ctx.ch.recv() catch |err| switch (err) {
            error.Closed => return sum,
            else => return err,
        };
        sum +%= v;
    }
}
fn channelRoot(n: u64) !u64 {
    var ch = try volt.channel.Channel(u64).init(bench_allocator, 16);
    defer ch.deinit();
    var ctx = ChCtx{ .ch = &ch, .n = n };
    const t0 = volt.time.nanoTimestamp();
    var prod = try volt.spawn(chProd, .{&ctx});
    defer volt.destroyTask(prod);
    var cons = try volt.spawn(chCons, .{&ctx});
    defer volt.destroyTask(cons);
    try prod.join();
    _ = try cons.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}
fn benchChannelSpsc(n: u64) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, channelRoot, .{n});
    return wall / n;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: TCP echo, N concurrent clients, K requests each.
//
// Why this workload: matches Volt's primary target — services that
// spawn many coroutines, each doing request/response I/O. NerdMeNot's
// S3 client / HTTP client / PG pool are all this shape. Spawn cost
// is amortized over the I/O; what matters is concurrent-connection
// throughput.
// ─────────────────────────────────────────────────────────────────────

const tcp_msg_size: usize = 1024;
const tcp_reqs_per_client: u32 = 16;

const TcpServerCtx = struct {
    listener: *volt.net.TcpListener,
    expected_conns: u32,
};

fn tcpEchoConn(stream: volt.net.TcpStream) void {
    var s = stream;
    defer s.close();
    var buf: [tcp_msg_size]u8 = undefined;
    var i: u32 = 0;
    while (i < tcp_reqs_per_client) : (i += 1) {
        var read: usize = 0;
        while (read < tcp_msg_size) {
            const n = s.read(buf[read..]) catch return;
            if (n == 0) return;
            read += n;
        }
        s.writeAll(buf[0..tcp_msg_size]) catch return;
    }
}

fn tcpServer(ctx: *TcpServerCtx) void {
    var i: u32 = 0;
    while (i < ctx.expected_conns) : (i += 1) {
        const stream = ctx.listener.accept() catch return;
        const j = volt.launch(tcpEchoConn, .{stream}) catch return;
        volt.destroyJob(j);
    }
}

const TcpClientCtx = struct {
    addr: volt.net.Address,
    done: *volt.sync.Notify,
    remaining: *std.atomic.Value(u32),
};

fn tcpClient(ctx: *TcpClientCtx) void {
    defer if (ctx.remaining.fetchSub(1, .acq_rel) == 1) ctx.done.notifyOne();
    var stream = volt.net.TcpStream.connect(ctx.addr) catch return;
    defer stream.close();

    var send_buf: [tcp_msg_size]u8 = undefined;
    for (&send_buf, 0..) |*b, i| b.* = @intCast(i & 0xff);
    var recv_buf: [tcp_msg_size]u8 = undefined;

    var i: u32 = 0;
    while (i < tcp_reqs_per_client) : (i += 1) {
        stream.writeAll(&send_buf) catch return;
        var read: usize = 0;
        while (read < tcp_msg_size) {
            const n = stream.read(recv_buf[read..]) catch return;
            if (n == 0) return;
            read += n;
        }
    }
}

fn tcpEchoRoot(n_clients: u32) !u64 {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var server_ctx = TcpServerCtx{ .listener = &listener, .expected_conns = n_clients };
    const server_job = try volt.launch(tcpServer, .{&server_ctx});
    defer volt.destroyJob(server_job);

    var done_notify = volt.sync.Notify{};
    var remaining = std.atomic.Value(u32).init(n_clients);
    var client_ctx = TcpClientCtx{
        .addr = addr,
        .done = &done_notify,
        .remaining = &remaining,
    };

    const t0 = volt.time.nanoTimestamp();
    const client_jobs = try bench_allocator.alloc(*volt.Job, n_clients);
    defer bench_allocator.free(client_jobs);
    for (client_jobs) |*j| j.* = try volt.launch(tcpClient, .{&client_ctx});
    defer for (client_jobs) |j| volt.destroyJob(j);
    try done_notify.wait();
    const wall = volt.time.nanoTimestamp() - t0;
    try server_job.join();
    return @intCast(wall);
}

fn benchTcpEcho(n_clients: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, tcpEchoRoot, .{n_clients});
    // ns per (request/response round trip). Each client does
    // tcp_reqs_per_client requests; total rtt count = n * reqs.
    const total_rtts = @as(u64, n_clients) * @as(u64, tcp_reqs_per_client);
    return wall / total_rtts;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: channel pipeline (producer → middle → consumer).
//
// Two channels, three coroutines, modest work between channel ops.
// More realistic than raw SPSC — closer to data-processing pipelines
// like DataFrame I/O.
// ─────────────────────────────────────────────────────────────────────

const PipelineCtx = struct {
    in: *volt.channel.Channel(u64),
    out: *volt.channel.Channel(u64),
    n: u64,
};

fn pipelineProducer(ctx: *PipelineCtx) !void {
    var i: u64 = 0;
    while (i < ctx.n) : (i += 1) try ctx.in.send(i);
    ctx.in.close();
}
fn pipelineMiddle(ctx: *PipelineCtx) !void {
    while (true) {
        const v = ctx.in.recv() catch |err| switch (err) {
            error.Closed => {
                ctx.out.close();
                return;
            },
            else => return err,
        };
        // Modest CPU work to simulate per-message processing —
        // hash-like fold that the compiler can't elide.
        var h: u64 = v;
        h ^= h >> 13;
        h *%= 0x100000001b3;
        h ^= h >> 23;
        try ctx.out.send(h);
    }
}
fn pipelineConsumer(ctx: *PipelineCtx) !u64 {
    var sum: u64 = 0;
    while (true) {
        const v = ctx.out.recv() catch |err| switch (err) {
            error.Closed => return sum,
            else => return err,
        };
        sum +%= v;
    }
}

fn channelPipelineRoot(n: u64) !u64 {
    var ch_in = try volt.channel.Channel(u64).init(bench_allocator, 64);
    defer ch_in.deinit();
    var ch_out = try volt.channel.Channel(u64).init(bench_allocator, 64);
    defer ch_out.deinit();
    var ctx = PipelineCtx{ .in = &ch_in, .out = &ch_out, .n = n };

    const t0 = volt.time.nanoTimestamp();
    var prod = try volt.spawn(pipelineProducer, .{&ctx});
    defer volt.destroyTask(prod);
    var mid = try volt.spawn(pipelineMiddle, .{&ctx});
    defer volt.destroyTask(mid);
    var cons = try volt.spawn(pipelineConsumer, .{&ctx});
    defer volt.destroyTask(cons);
    try prod.join();
    try mid.join();
    _ = try cons.join();
    const wall = volt.time.nanoTimestamp() - t0;
    return @intCast(wall);
}

fn benchChannelPipeline(n: u64) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, channelPipelineRoot, .{n});
    return wall / n;
}

// ─────────────────────────────────────────────────────────────────────
// Workload: mutex contended (8 × 50k)
// ─────────────────────────────────────────────────────────────────────

const MuCtx = struct { mu: *volt.sync.Mutex, counter: u64 = 0, iters: u32 };

fn muInc(ctx: *MuCtx) !void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        try ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn mutexRoot(workers: u32, iters: u32) !u64 {
    var mu = volt.sync.Mutex{};
    var ctx = MuCtx{ .mu = &mu, .iters = iters };
    const jobs = try bench_allocator.alloc(*volt.Job, workers);
    defer bench_allocator.free(jobs);
    const t0 = volt.time.nanoTimestamp();
    for (jobs) |*j| j.* = try volt.launch(muInc, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    const wall = volt.time.nanoTimestamp() - t0;
    std.debug.assert(ctx.counter == @as(u64, workers) * iters);
    return @intCast(wall);
}
fn benchMutex(workers: u32, iters: u32) !u64 {
    const wall = try volt.run(.{ .allocator = bench_allocator }, mutexRoot, .{ workers, iters });
    return wall / (@as(u64, workers) * iters);
}

// ─────────────────────────────────────────────────────────────────────
// Runner — median over N iterations
// ─────────────────────────────────────────────────────────────────────

fn runMedian(comptime fn_ptr: anytype, args: anytype) !u64 {
    var samples: [BENCH_ITERS]u64 = undefined;
    var w: u32 = 0;
    while (w < WARMUP_ITERS) : (w += 1) _ = try @call(.auto, fn_ptr, args);
    for (&samples) |*s| s.* = try @call(.auto, fn_ptr, args);
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    return samples[BENCH_ITERS / 2];
}

pub fn main() !void {
    // Real-workload benches first — these are the headline numbers
    // that match Volt's intended uses (S3, HTTP, PG, DataFrame I/O for
    // NerdMeNot). Synthetic microbenches follow as context.
    const tcp_echo_ns = try runMedian(benchTcpEcho, .{@as(u32, 64)});
    const pipeline_ns = try runMedian(benchChannelPipeline, .{@as(u64, 50_000)});

    const yield_ns = try runMedian(benchYield, .{@as(u32, 100_000)});
    const channel_ns = try runMedian(benchChannelSpsc, .{@as(u64, 100_000)});
    const mutex_ns = try runMedian(benchMutex, .{ @as(u32, 8), @as(u32, 50_000) });
    // spawn_* benches occasionally trip the 30s Parker watchdog under
    // sustained 10k-spawn pressure (open scheduler-race work, R
    // workstream). Re-run `zig build compare` if a run fails here.
    const spawn_ns = try runMedian(benchSpawnJoin, .{@as(u32, 10_000)});
    const spawn_wg_ns = try runMedian(benchSpawnWaitgroup, .{@as(u32, 10_000)});

    // JSON to stderr — std.debug.print is the simplest cross-version
    // stdio path in Zig 0.16. The orchestrator captures stderr.
    std.debug.print(
        "{{\n" ++
            "  \"yield_one_way_ns\": {d},\n" ++
            "  \"spawn_join_ns\": {d},\n" ++
            "  \"spawn_waitgroup_ns\": {d},\n" ++
            "  \"channel_spsc_16_ns\": {d},\n" ++
            "  \"mutex_contended_8_ns\": {d},\n" ++
            "  \"tcp_echo_rtt_ns\": {d},\n" ++
            "  \"channel_pipeline_ns\": {d}\n" ++
            "}}\n",
        .{ yield_ns, spawn_ns, spawn_wg_ns, channel_ns, mutex_ns, tcp_echo_ns, pipeline_ns },
    );
}

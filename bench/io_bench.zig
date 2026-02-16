//! Volt I/O Benchmark Suite — TCP Networking and Timer Primitives
//!
//! Benchmarks for I/O operations that involve real syscalls and timer primitives.
//!
//! TCP:
//!   - Connection establishment (bind+connect+accept cycle over loopback)
//!   - Ping-pong latency (64B message roundtrip)
//!   - Write throughput (bulk 64KB writes, read on other end)
//!
//! Timers:
//!   - Instant.now() clock read overhead
//!   - Sleep creation + tryWait check
//!   - Interval creation + tryTick
//!   - Deadline creation + check
//!   - Duration arithmetic chain
//!
//! Statistics: median of 10 iterations (5 warmup discarded).
//!
//! Run: zig build bench-io

const std = @import("std");
const volt = @import("volt");
const posix = std.posix;

const TcpListener = volt.net.TcpListener;
const TcpStream = volt.net.TcpStream;
const Address = volt.net.Address;
const Duration = volt.time.Duration;
const Instant = volt.time.Instant;
const Sleep = volt.time.Sleep;
const Interval = volt.time.Interval;
const Deadline = volt.time.Deadline;

const print = std.debug.print;

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

const ITERATIONS: usize = 10;
const WARMUP: usize = 5;

// TCP: fewer ops since each involves syscalls
const TCP_CONNECT_OPS: usize = 1_000;
const TCP_PINGPONG_OPS: usize = 10_000;
const TCP_THROUGHPUT_OPS: usize = 10_000; // 10K x 64KB = 640MB total transfer

// Timer: lightweight struct operations, can do more
const TIMER_OPS: usize = 1_000_000;

// Message sizes
const PINGPONG_MSG_SIZE: usize = 64;
const THROUGHPUT_CHUNK_SIZE: usize = 65_536; // 64KB

// ═══════════════════════════════════════════════════════════════════════════════
// Statistics — median-based (matches volt_bench.zig)
// ═══════════════════════════════════════════════════════════════════════════════

const Stats = struct {
    samples: [64]u64 = [_]u64{0} ** 64,
    count: usize = 0,

    fn add(self: *Stats, ns: u64) void {
        if (self.count < 64) {
            self.samples[self.count] = ns;
            self.count += 1;
        }
    }

    fn medianNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var sorted: [64]u64 = self.samples;
        std.mem.sort(u64, sorted[0..self.count], {}, std.sort.asc(u64));
        return sorted[self.count / 2];
    }

    fn minNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = std.math.maxInt(u64);
        for (self.samples[0..self.count]) |s| m = @min(m, s);
        return m;
    }
};

const BenchResult = struct {
    stats: Stats = .{},
    ops_per_iter: usize = 0,

    fn nsPerOp(self: BenchResult) f64 {
        const med: f64 = @floatFromInt(self.stats.medianNs());
        const ops: f64 = @floatFromInt(self.ops_per_iter);
        return if (ops > 0) med / ops else 0;
    }

    fn opsPerSec(self: BenchResult) f64 {
        const ns_per_op = self.nsPerOp();
        return if (ns_per_op > 0) 1_000_000_000.0 / ns_per_op else 0;
    }

    fn mbPerSec(self: BenchResult, bytes_per_op: usize) f64 {
        const ns_per_op = self.nsPerOp();
        if (ns_per_op == 0) return 0;
        const bytes_per_sec = @as(f64, @floatFromInt(bytes_per_op)) * 1_000_000_000.0 / ns_per_op;
        return bytes_per_sec / (1024.0 * 1024.0);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// TCP BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════

/// TCP connection establishment: bind listener, connect client, accept, close all.
/// Each op = one full connection lifecycle over loopback.
fn benchTcpConnect() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        tcpConnectIteration(TCP_CONNECT_OPS);
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        tcpConnectIteration(TCP_CONNECT_OPS);
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TCP_CONNECT_OPS };
}

fn tcpConnectIteration(ops: usize) void {
    // Bind listener once, connect/accept/close per op
    var listener = TcpListener.bind(Address.fromPort(0)) catch return;
    defer listener.close();

    const port = listener.localAddr().port();
    const addr = Address.loopbackV4(port);

    for (0..ops) |_| {
        var client = TcpStream.connect(addr) catch continue;

        // Accept with retry (non-blocking)
        var accepted = false;
        for (0..100) |_| {
            if (listener.tryAccept() catch null) |result| {
                var server = result.stream;
                server.close();
                accepted = true;
                break;
            }
        }
        if (!accepted) {
            // Fallback: just close client
        }

        client.close();
    }
}

/// TCP ping-pong: one connection, N small-message roundtrips.
/// Each op = client sends 64B, server reads 64B, server sends 64B, client reads 64B.
fn benchTcpPingPong() BenchResult {
    var stats = Stats{};

    // Set up persistent connection
    var listener = TcpListener.bind(Address.fromPort(0)) catch return .{};
    defer listener.close();

    const port = listener.localAddr().port();
    var client = TcpStream.connect(Address.loopbackV4(port)) catch return .{};
    defer client.close();

    // Accept
    var server: TcpStream = undefined;
    for (0..100) |_| {
        if (listener.tryAccept() catch null) |result| {
            server = result.stream;
            break;
        }
    } else return .{};
    defer server.close();

    var msg: [PINGPONG_MSG_SIZE]u8 = undefined;
    @memset(&msg, 'X');
    var recv_buf: [PINGPONG_MSG_SIZE]u8 = undefined;

    for (0..WARMUP) |_| {
        tcpPingPongIteration(&client, &server, &msg, &recv_buf, TCP_PINGPONG_OPS);
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        tcpPingPongIteration(&client, &server, &msg, &recv_buf, TCP_PINGPONG_OPS);
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TCP_PINGPONG_OPS };
}

fn tcpPingPongIteration(
    client: *TcpStream,
    server: *TcpStream,
    msg: []const u8,
    recv_buf: []u8,
    ops: usize,
) void {
    for (0..ops) |_| {
        // Client → Server
        writeAll(client, msg);
        readExact(server, recv_buf);

        // Server → Client
        writeAll(server, msg);
        readExact(client, recv_buf);
    }
}

/// TCP write throughput: one connection, N x 64KB bulk writes.
/// Measures raw socket write + read throughput over loopback.
fn benchTcpThroughput() BenchResult {
    var stats = Stats{};

    // Set up persistent connection
    var listener = TcpListener.bind(Address.fromPort(0)) catch return .{};
    defer listener.close();

    const port = listener.localAddr().port();
    var client = TcpStream.connect(Address.loopbackV4(port)) catch return .{};
    defer client.close();

    // Accept
    var server: TcpStream = undefined;
    for (0..100) |_| {
        if (listener.tryAccept() catch null) |result| {
            server = result.stream;
            break;
        }
    } else return .{};
    defer server.close();

    var write_buf: [THROUGHPUT_CHUNK_SIZE]u8 = undefined;
    @memset(&write_buf, 'D');
    var read_buf: [THROUGHPUT_CHUNK_SIZE]u8 = undefined;

    // Spawn reader thread to drain data
    var bytes_read = std.atomic.Value(u64).init(0);
    var done = std.atomic.Value(bool).init(false);

    for (0..WARMUP) |_| {
        tcpThroughputIteration(&client, &server, &write_buf, &read_buf, &bytes_read, &done, TCP_THROUGHPUT_OPS);
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        tcpThroughputIteration(&client, &server, &write_buf, &read_buf, &bytes_read, &done, TCP_THROUGHPUT_OPS);
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TCP_THROUGHPUT_OPS };
}

fn tcpThroughputIteration(
    client: *TcpStream,
    server: *TcpStream,
    write_buf: []const u8,
    read_buf: []u8,
    bytes_read: *std.atomic.Value(u64),
    done: *std.atomic.Value(bool),
    ops: usize,
) void {
    bytes_read.store(0, .monotonic);
    done.store(false, .monotonic);

    const total_bytes: u64 = @intCast(ops * write_buf.len);

    // Reader thread
    const reader_thread = std.Thread.spawn(.{}, struct {
        fn reader(srv: *TcpStream, buf: []u8, br: *std.atomic.Value(u64), d: *std.atomic.Value(bool), target: u64) void {
            while (br.load(.monotonic) < target and !d.load(.monotonic)) {
                if (srv.tryRead(buf) catch null) |n| {
                    if (n == 0) break; // EOF
                    _ = br.fetchAdd(@intCast(n), .monotonic);
                }
            }
        }
    }.reader, .{ server, read_buf, bytes_read, done, total_bytes }) catch return;

    // Writer: send all chunks
    for (0..ops) |_| {
        writeAll(client, write_buf);
    }

    // Wait for reader to drain
    while (bytes_read.load(.monotonic) < total_bytes) {
        std.Thread.yield() catch {};
    }
    done.store(true, .monotonic);
    reader_thread.join();
}

// ═══════════════════════════════════════════════════════════════════════════════
// TIMER BENCHMARKS
// ═══════════════════════════════════════════════════════════════════════════════

/// Instant.now() — monotonic clock read overhead.
fn benchInstantNow() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(Instant.now());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(Instant.now());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Instant.elapsed() — time measurement overhead.
fn benchInstantElapsed() BenchResult {
    var stats = Stats{};
    const base = Instant.now();

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(base.elapsed());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(base.elapsed());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Sleep creation + tryWait — measures struct init + check overhead.
fn benchSleepCreateCheck() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            var s = Sleep.init(Duration.fromSecs(60)); // far future — won't elapse
            std.mem.doNotOptimizeAway(s.tryWait());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            var s = Sleep.init(Duration.fromSecs(60));
            std.mem.doNotOptimizeAway(s.tryWait());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Sleep creation with immediate expiry — tryWait returns true.
fn benchSleepExpired() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            var s = Sleep.init(Duration.ZERO);
            std.mem.doNotOptimizeAway(s.tryWait());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            var s = Sleep.init(Duration.ZERO);
            std.mem.doNotOptimizeAway(s.tryWait());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Interval creation + tryTick — measures interval overhead.
fn benchIntervalTick() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        // Immediate interval fires on first tick
        var iv = Interval.initImmediate(Duration.fromNanos(1));
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(iv.tryTick());
        }
    }

    for (0..ITERATIONS) |_| {
        var iv = Interval.initImmediate(Duration.fromNanos(1));
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(iv.tryTick());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Deadline creation + check — measures deadline overhead.
fn benchDeadlineCheck() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            var dl = Deadline.init(Duration.fromSecs(60)); // far future
            std.mem.doNotOptimizeAway(dl.isExpired());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            var dl = Deadline.init(Duration.fromSecs(60));
            std.mem.doNotOptimizeAway(dl.isExpired());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Deadline check + remaining — measures the full check pattern.
fn benchDeadlineRemaining() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var dl = Deadline.init(Duration.fromSecs(60));
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(dl.isExpired());
            std.mem.doNotOptimizeAway(dl.remaining());
        }
    }

    for (0..ITERATIONS) |_| {
        var dl = Deadline.init(Duration.fromSecs(60));
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            std.mem.doNotOptimizeAway(dl.isExpired());
            std.mem.doNotOptimizeAway(dl.remaining());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

/// Duration arithmetic: fromNanos + add + sub + cmp chain.
fn benchDurationArithmetic() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..TIMER_OPS) |_| {
            const a = Duration.fromNanos(42);
            const b = Duration.fromMillis(1);
            const c = a.add(b);
            const d = c.sub(a);
            std.mem.doNotOptimizeAway(d.cmp(b));
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..TIMER_OPS) |_| {
            const a = Duration.fromNanos(42);
            const b = Duration.fromMillis(1);
            const c = a.add(b);
            const d = c.sub(a);
            std.mem.doNotOptimizeAway(d.cmp(b));
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = TIMER_OPS };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Socket I/O Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Write all bytes to a non-blocking TcpStream, retrying on WouldBlock.
fn writeAll(stream: *TcpStream, data: []const u8) void {
    var written: usize = 0;
    while (written < data.len) {
        if (stream.tryWrite(data[written..])) |n| {
            if (n) |bytes| {
                written += bytes;
            }
            // null = WouldBlock, retry
        } else |_| {
            return; // error
        }
    }
}

/// Read exactly buf.len bytes from a non-blocking TcpStream.
fn readExact(stream: *TcpStream, buf: []u8) void {
    var total: usize = 0;
    while (total < buf.len) {
        if (stream.tryRead(buf[total..])) |result| {
            if (result) |n| {
                if (n == 0) return; // EOF
                total += n;
            }
            // null = WouldBlock, retry
        } else |_| {
            return; // error
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// OUTPUT
// ═══════════════════════════════════════════════════════════════════════════════

fn printRow(name: []const u8, result: BenchResult) void {
    print("  {s:<36} {d:>8.1} ns/op  {d:>16.0} ops/sec\n", .{ name, result.nsPerOp(), result.opsPerSec() });
}

fn printRowWithThroughput(name: []const u8, result: BenchResult, bytes_per_op: usize) void {
    print("  {s:<36} {d:>8.1} ns/op  {d:>10.1} MB/s\n", .{ name, result.nsPerOp(), result.mbPerSec(bytes_per_op) });
}

fn printJsonEntry(name: []const u8, result: BenchResult, is_last: bool) void {
    print("    \"{s}\": {{\n", .{name});
    print("      \"ns_per_op\": {d:.2},\n", .{result.nsPerOp()});
    print("      \"ops_per_sec\": {d:.0},\n", .{result.opsPerSec()});
    print("      \"median_ns\": {d},\n", .{result.stats.medianNs()});
    print("      \"min_ns\": {d},\n", .{result.stats.minNs()});
    print("      \"ops_per_iter\": {d}\n", .{result.ops_per_iter});
    if (is_last) print("    }}\n", .{}) else print("    }},\n", .{});
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var json_mode = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--json")) json_mode = true;
    }

    // ── TCP Benchmarks ──
    print("[bench-io] Starting TCP benchmarks...\n", .{});
    const tcp_connect = benchTcpConnect();
    print("[bench-io] tcp_connect done\n", .{});
    const tcp_pingpong = benchTcpPingPong();
    print("[bench-io] tcp_pingpong done\n", .{});
    const tcp_throughput = benchTcpThroughput();
    print("[bench-io] tcp_throughput done\n", .{});

    // ── Timer Benchmarks ──
    print("[bench-io] Starting timer benchmarks...\n", .{});
    const instant_now = benchInstantNow();
    print("[bench-io] instant_now done\n", .{});
    const instant_elapsed = benchInstantElapsed();
    print("[bench-io] instant_elapsed done\n", .{});
    const sleep_check = benchSleepCreateCheck();
    print("[bench-io] sleep_check done\n", .{});
    const sleep_expired = benchSleepExpired();
    print("[bench-io] sleep_expired done\n", .{});
    const interval_tick = benchIntervalTick();
    print("[bench-io] interval_tick done\n", .{});
    const deadline_check = benchDeadlineCheck();
    print("[bench-io] deadline_check done\n", .{});
    const deadline_remaining = benchDeadlineRemaining();
    print("[bench-io] deadline_remaining done\n", .{});
    const duration_arith = benchDurationArithmetic();
    print("[bench-io] duration_arith done\n", .{});

    if (json_mode) {
        print("{{\n", .{});
        print("  \"metadata\": {{\n", .{});
        print("    \"runtime\": \"volt\",\n", .{});
        print("    \"tcp_connect_ops\": {d},\n", .{TCP_CONNECT_OPS});
        print("    \"tcp_pingpong_ops\": {d},\n", .{TCP_PINGPONG_OPS});
        print("    \"tcp_throughput_ops\": {d},\n", .{TCP_THROUGHPUT_OPS});
        print("    \"timer_ops\": {d},\n", .{TIMER_OPS});
        print("    \"iterations\": {d},\n", .{ITERATIONS});
        print("    \"warmup\": {d}\n", .{WARMUP});
        print("  }},\n", .{});
        print("  \"benchmarks\": {{\n", .{});
        printJsonEntry("tcp_connect", tcp_connect, false);
        printJsonEntry("tcp_pingpong", tcp_pingpong, false);
        printJsonEntry("tcp_throughput", tcp_throughput, false);
        printJsonEntry("instant_now", instant_now, false);
        printJsonEntry("instant_elapsed", instant_elapsed, false);
        printJsonEntry("sleep_create_check", sleep_check, false);
        printJsonEntry("sleep_expired", sleep_expired, false);
        printJsonEntry("interval_tick", interval_tick, false);
        printJsonEntry("deadline_check", deadline_check, false);
        printJsonEntry("deadline_remaining", deadline_remaining, false);
        printJsonEntry("duration_arithmetic", duration_arith, true);
        print("  }}\n", .{});
        print("}}\n", .{});
    } else {
        print("\n", .{});
        print("=" ** 74 ++ "\n", .{});
        print("  Volt I/O Benchmark Suite\n", .{});
        print("=" ** 74 ++ "\n", .{});
        print("  Config: {d} iters, {d} warmup, median-based\n\n", .{ ITERATIONS, WARMUP });

        print("  TCP NETWORKING (loopback)\n", .{});
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("Connect+accept cycle", tcp_connect);
        printRowWithThroughput("Ping-pong 64B roundtrip", tcp_pingpong, PINGPONG_MSG_SIZE * 2);
        printRowWithThroughput("Throughput 64KB writes", tcp_throughput, THROUGHPUT_CHUNK_SIZE);
        print("\n", .{});

        print("  TIMERS ({d} ops)\n", .{TIMER_OPS});
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("Instant.now()", instant_now);
        printRow("Instant.elapsed()", instant_elapsed);
        printRow("Sleep create + tryWait", sleep_check);
        printRow("Sleep expired + tryWait", sleep_expired);
        printRow("Interval tryTick (1ns period)", interval_tick);
        printRow("Deadline create + isExpired", deadline_check);
        printRow("Deadline isExpired + remaining", deadline_remaining);
        printRow("Duration arithmetic chain", duration_arith);
        print("\n", .{});

        print("=" ** 74 ++ "\n", .{});
        print("  Use --json for machine-readable output.\n", .{});
        print("=" ** 74 ++ "\n\n", .{});
    }
}

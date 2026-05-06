//! v1.1 trait-overhead bench — pipe throughput via the new
//! `BufReader`-over-`Fd`-over-pipe path. Compared head-to-head with
//! `bench_io_baseline.zig` to enforce the Risk #2 gate.
//!
//! ## Acceptance criterion
//!
//! BufReader-via-trait must read pipe data at ≤10% slower than the
//! raw `volt.io.lowlevel.read` baseline at 64 KiB chunks (ReleaseFast).
//! If a regression beyond that lands, P1 doesn't merge.
//!
//! Run side-by-side:
//!   zig build bench-io-baseline
//!   zig build bench-io-traits
//!
//! Compare the MiB/s lines. The trait number is allowed to come in
//! up to 10% under the baseline; below that, the vtable cost or the
//! BufReader fast path needs investigation.
//!
//! ## P1 status
//!
//! Inherits the same Darwin throughput-flakiness as the baseline
//! bench (kqueue ping-pong under load). Both are tracked as P1
//! follow-ups; the gate enforcement starts when both run reliably.

const std = @import("std");
const volt = @import("volt");
const posix = std.posix;

const PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;
const BUF_CAPACITY: usize = 64 * 1024;

const Pipe = struct { read_fd: posix.fd_t, write_fd: posix.fd_t };

fn openPipe() !Pipe {
    const fds = try volt.internal.syscall.pipe();
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);
    return .{ .read_fd = fds[0], .write_fd = fds[1] };
}

const ProducerArgs = struct { fd: posix.fd_t, bytes: u64 };

fn producer(args: *ProducerArgs) !void {
    const buf: [CHUNK]u8 = .{0xAB} ** CHUNK;
    var sent: u64 = 0;
    while (sent < args.bytes) {
        const remaining = args.bytes - sent;
        const want: usize = if (remaining < CHUNK) @intCast(remaining) else CHUNK;
        try volt.io.lowlevel.writeAll(args.fd, buf[0..want]);
        sent += want;
    }
    _ = posix.system.close(args.fd);
}

const ConsumerArgs = struct {
    fd: posix.fd_t,
    bytes: u64,
    allocator: std.mem.Allocator,
};

fn consumer(args: *ConsumerArgs) !u64 {
    // The path under test: Fd → reader() → BufReader → read().
    // Each consumer-side `read` flows through:
    //   BufReader.read (fast path: memcpy from internal buf)
    //   → on refill: BufReader -> Fd.reader().read -> volt.io.lowlevel.read
    // The vtable dispatch only fires on refill (every BUF_CAPACITY bytes).
    var fd = volt.io.Fd.init(args.fd);
    var br = try volt.io.BufReader.init(args.allocator, fd.reader(), BUF_CAPACITY);
    defer br.deinit();

    var buf: [CHUNK]u8 = undefined;
    var got: u64 = 0;
    while (got < args.bytes) {
        const n = try br.reader().read(&buf);
        if (n == 0) break;
        got += n;
    }
    _ = posix.system.close(args.fd);
    return got;
}

fn benchRoot(payload: u64) !u64 {
    const p = try openPipe();

    var prod_args = ProducerArgs{ .fd = p.write_fd, .bytes = payload };
    const prod_job = try volt.launch(producer, .{&prod_args});
    defer volt.destroyJob(prod_job);

    var cons_args = ConsumerArgs{
        .fd = p.read_fd,
        .bytes = payload,
        .allocator = std.heap.page_allocator,
    };
    const cons_task = try volt.spawn(consumer, .{&cons_args});
    defer volt.destroyTask(cons_task);

    const t0 = volt.time.nanoTimestamp();
    const got = try cons_task.join();
    try prod_job.join();
    const wall: u64 = @intCast(volt.time.nanoTimestamp() - t0);

    if (got != payload) std.debug.print("warn: short read — got {d}/{d}\n", .{ got, payload });
    return wall;
}

pub fn main() !void {
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, benchRoot, .{PAYLOAD_BYTES});
    const ns_per_byte = if (PAYLOAD_BYTES == 0) 0 else wall / PAYLOAD_BYTES;
    const bytes_per_sec: u64 = if (wall == 0) 0 else (PAYLOAD_BYTES * std.time.ns_per_s) / wall;
    const mib_per_sec = bytes_per_sec / (1024 * 1024);
    std.debug.print(
        \\BufReader+Fd over pipe trait-path:
        \\  payload  : {d} bytes ({d} MiB)
        \\  wall     : {d} ns
        \\  per-byte : {d} ns
        \\  bandwidth: {d} MiB/s
        \\  baseline : run `zig build bench-io-baseline` for the
        \\             reference; trait number must come in within 10%.
        \\
    ,
        .{ PAYLOAD_BYTES, PAYLOAD_BYTES / (1024 * 1024), wall, ns_per_byte, mib_per_sec },
    );
}

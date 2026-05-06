//! v1.1 baseline — pipe throughput via the existing low-level
//! `volt.io.lowlevel.read` / `write` path.
//!
//! ## Risk #2 (vtable cost on hot reads) — gate
//!
//! When P1 lands the trait surface, a sibling `bench_io_traits.zig`
//! will measure the equivalent throughput via `BufReader` over a
//! `Reader` vtable wrapping the same pipe. Acceptance criterion:
//! BufReader-via-trait ≤10% slower than this baseline at 64 KiB chunks
//! (ReleaseFast). If it regresses further, P1 doesn't merge.
//!
//! Intentionally tiny so the bench itself doesn't dominate the
//! measurement: a single reader + single writer coroutine on one
//! pipe, fixed payload, reported in MiB/s and ns/byte.
//!
//! ## P0 status: skeleton only
//!
//! The skeleton compiles and reports a number on success, but a
//! cross-worker race on Darwin causes intermittent
//! `error.Unexpected` from `kevent` during high-throughput
//! producer/consumer ping-pong (issue stays in the kqueue reactor's
//! wake path; the test suite doesn't exercise this pattern). Stabilising
//! the run is folded into P1 alongside writing `bench_io_traits.zig` —
//! gate enforcement starts when both benches run reliably side-by-side.

const std = @import("std");
const volt = @import("volt");
const posix = std.posix;

// 4 MiB is enough payload to drive a stable bandwidth measurement on a
// loopback pipe (multiple kernel buffers worth) without hitting the
// payload-size flakiness at >32 MiB seen during bring-up. P1 will
// stabilise the larger sizes alongside the trait-overhead bench.
const PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;

const Pipe = struct { read_fd: posix.fd_t, write_fd: posix.fd_t };

fn openPipe() !Pipe {
    const fds = try volt.internal.syscall.pipe();
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);
    return .{ .read_fd = fds[0], .write_fd = fds[1] };
}

const ProducerArgs = struct {
    fd: posix.fd_t,
    bytes: u64,
};

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
};

fn consumer(args: *ConsumerArgs) !u64 {
    var buf: [CHUNK]u8 = undefined;
    var got: u64 = 0;
    while (got < args.bytes) {
        const n = try volt.io.lowlevel.read(args.fd, &buf);
        if (n == 0) break; // EOF before payload exhausted — producer dropped
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

    var cons_args = ConsumerArgs{ .fd = p.read_fd, .bytes = payload };
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
        \\io.lowlevel.read pipe baseline:
        \\  payload  : {d} bytes ({d} MiB)
        \\  wall     : {d} ns
        \\  per-byte : {d} ns
        \\  bandwidth: {d} MiB/s
        \\
    ,
        .{ PAYLOAD_BYTES, PAYLOAD_BYTES / (1024 * 1024), wall, ns_per_byte, mib_per_sec },
    );
}

//! Reactor conformance tests.
//!
//! Cross-cutting platform tests that don't sit naturally inside any
//! single subsystem. The contract: every test in this file uses
//! ONLY the public surface (`Runtime`, `net.Tcp*`, `net.UdpSocket`,
//! `net.Address`, `sleep`) so each test runs identically on
//! kqueue (Darwin) / epoll + io_uring (Linux) / IOCP (Windows)
//! without `if (is_*)` branches.
//!
//! What goes here:
//!  * Cross-cutting properties (lifecycle invariants across
//!    reactor + fd + timer, e.g. fd close during pending I/O).
//!  * Corner cases that surfaced as platform-specific bugs and
//!    need a minimal, permanent regression test (preferred over
//!    growing the per-subsystem file with one-off cases).
//!  * Tests for behaviour the per-subsystem files don't cover
//!    today (e.g. IPv6 paired with the existing IPv4 test).
//!
//! What does NOT go here:
//!  * Tests that fit naturally next to their subsystem (a new
//!    UDP socket option → `net/udp.zig`; a new `File` method →
//!    `fs/file.zig`). Don't duplicate.
//!  * Backend-specific behaviour (`epoll`-only / `io_uring`-only).
//!    Backend-internal tests belong inside the backend file.
//!
//! Each test should carry a one-line note in its body explaining
//! which property it pins and — if applicable — which past bug it
//! would have caught. The names exist so the suite stays a
//! readable inventory of the cross-platform contracts, not just a
//! growing pile.
//!
//! Wired into the test build via `_ = @import(...)` in `lib.zig`.

const std = @import("std");
const testing = std.testing;

const runtime = @import("runtime.zig");
const net = @import("net.zig");
const current = @import("current.zig");
const lib = @import("lib.zig");

// volt.testing.allocator is the multi-worker-safe leak detector.
// std.testing.allocator's stack-trace capture races with worker
// stack writes and causes SIGILL crashes in spawning tests —
// see src/testing.zig for the rationale.
const test_alloc = @import("testing.zig").allocator;

const Address = net.Address;
const TcpListener = net.TcpListener;
const TcpStream = net.TcpStream;
const UdpSocket = net.UdpSocket;

// ─────────────────────────────────────────────────────────────────
// UDP IPv6 loopback round-trip
//
// Pairs with `net.udp.test.UdpSocket: connected echo round-trip
// (IPv4)`. The two protocol families take slightly different
// kernel code paths on every platform; passing IPv4 doesn't
// imply IPv6 works (and vice versa). If IPv6 loopback isn't
// available on the test host (rare — typical in stripped-down
// containers without ::1), the test skips rather than fails.
// ─────────────────────────────────────────────────────────────────

const Udp6Ctx = struct {
    server: *UdpSocket,
    server_addr: Address = undefined,
    received: u8 = 0,
    client_received: u8 = 0,
    server_done: bool = false,
};

fn udp6Server(ctx: *Udp6Ctx) !void {
    var buf: [1]u8 = undefined;
    const r = try ctx.server.recvFrom(&buf);
    ctx.received = buf[0];
    // Echo it back to whoever sent it.
    _ = try ctx.server.sendTo(buf[0..r.len], r.addr);
    ctx.server_done = true;
}

fn udp6Client(ctx: *Udp6Ctx) !void {
    var client = try UdpSocket.unboundV6();
    defer client.close();
    const payload = [_]u8{0xC6};
    _ = try client.sendTo(&payload, ctx.server_addr);
    var buf: [1]u8 = undefined;
    _ = try client.recv(&buf);
    ctx.client_received = buf[0];
}

fn udp6Root(ctx: *Udp6Ctx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var srv = try rt.spawn(udp6Server, .{ctx});
    var cli = try rt.spawn(udp6Client, .{ctx});
    _ = cli.join() catch |e| return e;
    _ = srv.join() catch |e| return e;
}

test "conformance: UDP IPv6 loopback round-trip" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();

    // Some constrained environments (network namespaces, stripped
    // CI images) disable IPv6 loopback entirely. Distinguish
    // "feature unavailable" from a real reactor bug.
    var server = UdpSocket.bind(Address.loopback6(0)) catch |err| switch (err) {
        error.SystemResources, error.OutOfDescriptors, error.Unexpected => return error.SkipZigTest,
        else => return err,
    };
    defer server.close();

    var ctx = Udp6Ctx{ .server = &server };
    ctx.server_addr = try server.localAddress();
    try (try rt.run(udp6Root, .{&ctx}));
    try testing.expect(ctx.server_done);
    try testing.expectEqual(@as(u8, 0xC6), ctx.received);
    try testing.expectEqual(@as(u8, 0xC6), ctx.client_received);
}

// ─────────────────────────────────────────────────────────────────
// TCP half-close: peer closes write half → reader observes EOF
//
// The contract: when one side of a TCP connection closes its
// write half (here, by closing the whole socket), the peer's
// next read returns 0 bytes (clean EOF), not an error and not a
// hang. Readiness-based reactors (kqueue / epoll) historically
// got this wrong by signaling "readable" once, draining the
// buffer, then never re-signaling — leaving the reader parked.
// Completion-based reactors (IOCP / io_uring) have a different
// hazard: the post-close completion may carry zero bytes
// without a distinct "EOF" flag, and the caller has to treat
// "completed read of length 0" as EOF rather than retry.
// ─────────────────────────────────────────────────────────────────

const HalfCloseCtx = struct {
    listener: *TcpListener,
    addr: Address = undefined,
    reader_saw_eof: bool = false,
};

fn halfCloseReader(ctx: *HalfCloseCtx) !void {
    var conn = try ctx.listener.accept();
    defer conn.close();
    // The closer side will close without writing anything. We
    // expect read() to return 0 (EOF), not block forever and not
    // return an error.
    var buf: [4]u8 = undefined;
    const n = try conn.read(&buf);
    try testing.expectEqual(@as(usize, 0), n);
    ctx.reader_saw_eof = true;
}

fn halfCloseCloser(ctx: *HalfCloseCtx) !void {
    var conn = try TcpStream.connect(ctx.addr);
    // Immediate close. Don't write anything — the property is
    // about "EOF on a connection that never carried data."
    conn.close();
}

fn halfCloseRoot(ctx: *HalfCloseCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var reader = try rt.spawn(halfCloseReader, .{ctx});
    var closer = try rt.spawn(halfCloseCloser, .{ctx});
    _ = closer.join() catch |e| return e;
    _ = reader.join() catch |e| return e;
}

test "conformance: TCP half-close — reader observes clean EOF" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = HalfCloseCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(halfCloseRoot, .{&ctx}));
    try testing.expect(ctx.reader_saw_eof);
}

// ─────────────────────────────────────────────────────────────────
// sleep(0) returns immediately without arming a timer
//
// Corner case for timer wheels. A naive implementation that
// always inserts into the wheel before checking the deadline
// will pay a syscall on every `sleep(0)`; a buggy one may park
// the coroutine indefinitely if the wheel collapses 0 to
// "never". The contract: `sleep(0)` is a yield-like no-op.
// ─────────────────────────────────────────────────────────────────

fn sleepZeroRoot() !void {
    try lib.sleepNanos(0);
}

test "conformance: sleep(0) returns immediately" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(sleepZeroRoot, .{}));
}

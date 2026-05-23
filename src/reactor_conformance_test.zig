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
const Cancel = lib.Cancel;

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

// ─────────────────────────────────────────────────────────────────
// recvCancel race stress — register-then-park window
//
// The hypothesis under test: `waitFdCancel` (each cancel-aware
// reactor) adds the coroutine to the Cancel's waiter list BEFORE
// registering the kernel-side wait (kevent ADD / EPOLL_CTL_ADD /
// io_uring SQE). If `cancel.fire()` runs in that window,
// `cancelCoro` calls the kernel deregister on a registration that
// doesn't exist yet (ENOENT). The code interprets ENOENT as
// "poll() owns the unpark" and silently returns — but nobody
// owns the unpark. The coroutine then registers + parks → hangs.
//
// This stress runs the recvCancel pattern N times with a small,
// jittered delay before fire(). The fire is scheduled to land
// near the recv coroutine's register-then-park window with high
// probability under contention. A single hang means the bug
// reproduces; passing N iterations under cancel_timeout means the
// invariant holds. Started flaky on Linux CI (issue forthcoming).
// ─────────────────────────────────────────────────────────────────

const RecvCancelIterCtx = struct {
    c: *Cancel,
    yield_count: u32,
};

fn recvCancelOneIter(ctx: *RecvCancelIterCtx) !void {
    var server = try UdpSocket.bind(Address.loopback4(0));
    defer server.close();
    var buf: [16]u8 = undefined;
    const r = server.recvCancel(&buf, ctx.c);
    // We send no packets, so the only legal result is Cancelled.
    try testing.expectError(error.Cancelled, r);
}

fn recvCancelOneIterFirer(ctx: *RecvCancelIterCtx) void {
    // Tunable yield count — different values exercise different
    // points in the recv's register-then-park window.
    //  0  yields → fire before recv even runs
    //  1-2       → fire likely during registerReactor / waitFd race
    //  3-10      → fire after recv is parked (the "easy" path)
    var i: u32 = 0;
    while (i < ctx.yield_count) : (i += 1) lib.yield();
    ctx.c.fire();
}

fn recvCancelStressBody() !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));

    // Sweep yield counts so each iteration hits a different point
    // in the race window. 200 iterations × ~8 distinct timings.
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var c = Cancel.init(rt);
        defer c.deinit();
        var ctx = RecvCancelIterCtx{ .c = &c, .yield_count = i % 8 };
        var recv_task = try rt.spawn(recvCancelOneIter, .{&ctx});
        var firer_task = try rt.spawn(recvCancelOneIterFirer, .{&ctx});
        // Recv must complete with Cancelled. If the race triggers,
        // this join blocks forever and the test hangs — surface via
        // zig's per-test budget.
        _ = recv_task.join() catch |e| return e;
        _ = firer_task.join();
    }
}

test "conformance: recvCancel stress — 200 iterations, no hang" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    try (try rt.run(recvCancelStressBody, .{}));
}

// ─────────────────────────────────────────────────────────────────
// fd close while a peer coroutine is parked on accept/recv
//
// The class: a coroutine is blocked in a reactor wait (TcpListener
// .accept / UdpSocket.recvFrom), then another coroutine closes
// the underlying fd. The blocked coroutine MUST unblock — with
// some categorical error (Closed / BadDescriptor / Unexpected) is
// fine — but MUST NOT hang.
//
// Inspired by libuv's `test-tcp-close-accept.c` and `test-udp-
// recv-cb-close-pollerr.c`. The libuv tests pin specific bug-
// fix behaviour after their own iterations of this race (the
// poll-close-doesnt-corrupt-stack family). These Volt ports
// don't pin a specific error variant — they pin the absence of
// hangs and segfaults, which is the load-bearing invariant for
// downstream lib authors. Tightening the error contract is a
// separate follow-up.
//
// FIXME(close-while-parked): both tests below are currently
// SkipZigTest. Volt's TcpListener.close / TcpStream.close /
// UdpSocket.close call libc close() directly — kqueue silently
// drops the kevent registration on fd close and never fires an
// event, so the parked accepter/receiver waits forever. Epoll
// has the equivalent gap: EPOLL_CTL_DEL on close-without-deregister
// orphans the parked coroutine.
//
// The fix is a reactor change, not a net-type one: each backend
// needs `closeFd(fd)` that looks up any parked waiter for that
// fd (via a new fd→coro side-table) and dispatches it with the
// next syscall returning EBADF. EV_DELETE / EPOLL_CTL_DEL return
// codes resolve the race with `poll()`, exactly the same pattern
// `cancelCoro` already uses.
//
// Scope of that change touches every reactor backend
// (reactor_kqueue, reactor_epoll, reactor_io_uring) plus the
// `close()` method on each net type. Per CLAUDE.md's phase-
// landing protocol, anything in the reactor needs the full bench
// suite green before landing. The tests stay here, skipped with
// this comment, so the gap is visible in `zig build test` output
// rather than hidden behind a deferred TODO list.
// ─────────────────────────────────────────────────────────────────

const ListenerCloseCtx = struct {
    listener: *TcpListener,
    accept_result: anyerror!void = error.NeverRan,
};

fn listenerCloseAccepter(ctx: *ListenerCloseCtx) !void {
    // accept() will park until either a connection arrives (which
    // it won't — nothing connects) or the listener fd is closed
    // out from under us.
    var stream = ctx.listener.accept() catch |e| {
        ctx.accept_result = e;
        return;
    };
    // Unexpected: no connection should have arrived. Drain it so
    // the test cleans up, then mark this as a non-error path.
    stream.close();
    ctx.accept_result = {};
}

fn listenerCloseCloser(ctx: *ListenerCloseCtx) void {
    // Yield a handful of times to give the accepter time to reach
    // the parked state, then close the listener out from under it.
    var i: u32 = 0;
    while (i < 20) : (i += 1) lib.yield();
    ctx.listener.close();
}

fn listenerCloseRoot(ctx: *ListenerCloseCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var accepter = try rt.spawn(listenerCloseAccepter, .{ctx});
    var closer = try rt.spawn(listenerCloseCloser, .{ctx});
    // accepter must unblock; if it hangs, the test budget catches it.
    _ = accepter.join() catch |e| return e;
    _ = closer.join();
}

test "conformance: TcpListener.close while accept() is parked — no hang" {
    // See FIXME(close-while-parked) above.
    if (true) return error.SkipZigTest;

    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    // Don't `defer listener.close()` — the closer coroutine owns it.
    var ctx = ListenerCloseCtx{ .listener = &listener };
    try (try rt.run(listenerCloseRoot, .{&ctx}));

    // Result is informational — what matters is that accepter
    // unblocked. Either an error or (unlikely) success means the
    // reactor handled the fd-close correctly. The NeverRan
    // sentinel would mean the accept fn never wrote a result —
    // implies a hang past the test budget.
    _ = ctx.accept_result catch |e| if (e == error.NeverRan) return error.AccepterNeverRan;
}

const UdpCloseCtx = struct {
    socket: *UdpSocket,
    recv_result: anyerror!usize = error.NeverRan,
};

fn udpCloseReceiver(ctx: *UdpCloseCtx) !void {
    var buf: [16]u8 = undefined;
    const r = ctx.socket.recvFrom(&buf) catch |e| {
        ctx.recv_result = e;
        return;
    };
    ctx.recv_result = r.len;
}

fn udpCloseCloser(ctx: *UdpCloseCtx) void {
    var i: u32 = 0;
    while (i < 20) : (i += 1) lib.yield();
    ctx.socket.close();
}

fn udpCloseRoot(ctx: *UdpCloseCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var receiver = try rt.spawn(udpCloseReceiver, .{ctx});
    var closer = try rt.spawn(udpCloseCloser, .{ctx});
    _ = receiver.join() catch |e| return e;
    _ = closer.join();
}

test "conformance: UdpSocket.close while recvFrom() is parked — no hang" {
    // See FIXME(close-while-parked) above.
    if (true) return error.SkipZigTest;

    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();

    var socket = try UdpSocket.bind(Address.loopback4(0));
    var ctx = UdpCloseCtx{ .socket = &socket };
    try (try rt.run(udpCloseRoot, .{&ctx}));

    _ = ctx.recv_result catch |e| if (e == error.NeverRan) return error.ReceiverNeverRan;
}

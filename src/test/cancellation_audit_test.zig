//! Cancellation correctness audit.
//!
//! Verifies that `Job.cancel()` propagates cleanly across every
//! parked-coroutine primitive Volt exposes — channel send/recv,
//! sleep, Mutex.lock — and that channel / mutex invariants stay
//! intact after a parked waiter is cancelled out.
//!
//! Plus a 100-coroutine torture test exercising the
//! "spawn N coros parked on TCP reads, cancel them all, every
//! one exits with `error.Cancelled`" pattern that real
//! consumers (RPC fanout, request demux, supervisor trees) rely
//! on.
//!
//! Companion bench: `bench/bench_sleep_reset.zig` measures the
//! cancel-and-new-sleep cost relevant to hot-path resettable
//! timeout patterns.

const std = @import("std");
const volt = @import("../lib.zig");

// ─────────────────────────────────────────────────────────────────────
// 1. Cancel a parked channel SENDER
// ─────────────────────────────────────────────────────────────────────

const SendCancelCtx = struct {
    ch: *volt.channel.Channel(u32),
    saw_cancelled: bool = false,
    saw_ok: bool = false,
    saw_other_error: ?anyerror = null,
};

fn senderCoro(ctx: *SendCancelCtx) void {
    // Channel is full (capacity 1, one item already inside) — this
    // send parks until either (a) someone recv's, freeing a slot, or
    // (b) cancellation wakes us. Expect (b).
    if (ctx.ch.send(99)) |_| {
        ctx.saw_ok = true;
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true else {
            ctx.saw_other_error = err;
        }
    }
}

fn sendCancelRoot() !SendCancelCtx {
    const Channel = volt.channel.Channel(u32);
    // Channel.init floors to capacity 2 internally (power-of-2 ring).
    // Send 2 items to actually fill it.
    var ch = try Channel.init(std.testing.allocator, 2);
    defer ch.deinit();

    try ch.send(1);
    try ch.send(2);

    var ctx = SendCancelCtx{ .ch = &ch };
    const sender = try volt.launch(senderCoro, .{&ctx});
    defer volt.destroyJob(sender);

    // Yield so sender registers and parks.
    var i: u32 = 0;
    while (i < 5) : (i += 1) try volt.yield();

    sender.cancel();
    sender.join() catch {};

    // Channel invariants: original items still recv'able in order.
    const a = try ch.recv();
    const b = try ch.recv();
    try std.testing.expectEqual(@as(u32, 1), a);
    try std.testing.expectEqual(@as(u32, 2), b);
    return ctx;
}

test "cancel-audit:cancel parked channel sender → error.Cancelled, channel intact" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, sendCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
}

// ─────────────────────────────────────────────────────────────────────
// 2. Cancel a parked channel RECEIVER
// ─────────────────────────────────────────────────────────────────────

const RecvCancelCtx = struct {
    ch: *volt.channel.Channel(u32),
    saw_cancelled: bool = false,
};

fn receiverCoro(ctx: *RecvCancelCtx) void {
    if (ctx.ch.recv()) |_| {
        // unexpected
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn recvCancelRoot() !RecvCancelCtx {
    const Channel = volt.channel.Channel(u32);
    var ch = try Channel.init(std.testing.allocator, 4);
    defer ch.deinit();

    var ctx = RecvCancelCtx{ .ch = &ch };
    const receiver = try volt.launch(receiverCoro, .{&ctx});
    defer volt.destroyJob(receiver);

    var i: u32 = 0;
    while (i < 5) : (i += 1) try volt.yield();

    receiver.cancel();
    receiver.join() catch {};

    // Channel invariants: still works for fresh send/recv.
    try ch.send(42);
    const got = try ch.recv();
    try std.testing.expectEqual(@as(u32, 42), got);
    return ctx;
}

test "cancel-audit:cancel parked channel receiver → error.Cancelled, channel intact" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, recvCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
}

// ─────────────────────────────────────────────────────────────────────
// 3. Cancel a coroutine asleep
// ─────────────────────────────────────────────────────────────────────

const SleepCancelCtx = struct {
    saw_cancelled: bool = false,
};

fn sleeperCoro(ctx: *SleepCancelCtx) void {
    // 1 hour — would never fire in a test; cancel must surface.
    if (volt.sleep(volt.Duration.fromSecs(3600))) |_| {
        // unexpected
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn sleepCancelRoot() !SleepCancelCtx {
    var ctx = SleepCancelCtx{};
    const sleeper = try volt.launch(sleeperCoro, .{&ctx});
    defer volt.destroyJob(sleeper);

    var i: u32 = 0;
    while (i < 5) : (i += 1) try volt.yield();

    sleeper.cancel();
    sleeper.join() catch {};
    return ctx;
}

test "cancel-audit:cancel sleeping coroutine → error.Cancelled (no firing)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, sleepCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
}

// ─────────────────────────────────────────────────────────────────────
// 4. Cancel a coroutine waiting on Mutex.lock
// ─────────────────────────────────────────────────────────────────────

const MutexCancelCtx = struct {
    mu: *volt.sync.Mutex,
    saw_cancelled: bool = false,
};

fn mutexWaiterCoro(ctx: *MutexCancelCtx) void {
    if (ctx.mu.lock()) |_| {
        // Either the unlocker handed it to us before cancel landed, or
        // we beat the cancel — release immediately.
        ctx.mu.unlock();
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn mutexCancelRoot() !MutexCancelCtx {
    var mu = volt.sync.Mutex{};
    try mu.lock(); // root holds it

    var ctx = MutexCancelCtx{ .mu = &mu };
    const waiter = try volt.launch(mutexWaiterCoro, .{&ctx});
    defer volt.destroyJob(waiter);

    // Yield so the waiter parks on the mutex.
    var i: u32 = 0;
    while (i < 5) : (i += 1) try volt.yield();

    waiter.cancel();
    waiter.join() catch {};

    // The waiter list invariants must be intact: root unlocks, and
    // a fresh lock attempt succeeds.
    mu.unlock();
    try mu.lock();
    mu.unlock();
    return ctx;
}

test "cancel-audit:cancel parked Mutex.lock → error.Cancelled, waiter list intact" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, mutexCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
}

// ─────────────────────────────────────────────────────────────────────
// 5. 100-coroutine TCP-park torture test — scope-cancel, all exit
// ─────────────────────────────────────────────────────────────────────

const TortureCtx = struct {
    listener: *volt.net.TcpListener,
    addr: volt.net.Address,
    cancelled_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn tortureWorker(ctx: *TortureCtx) void {
    var stream = volt.net.TcpStream.connect(ctx.addr) catch return;
    defer stream.close();

    // Park forever on a read — the server side never writes.
    var buf: [16]u8 = undefined;
    if (stream.read(&buf)) |_| {
        // unexpected
    } else |err| {
        if (err == error.Cancelled) {
            _ = ctx.cancelled_count.fetchAdd(1, .monotonic);
        }
    }
}

fn tortureRoot() !u32 {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var ctx = TortureCtx{ .listener = &listener, .addr = addr };

    // Spawn 100 client connections, each parked on read.
    const N: u32 = 100;
    var workers: [N]*volt.Job = undefined;
    for (&workers) |*j| j.* = try volt.launch(tortureWorker, .{&ctx});
    defer for (workers) |j| volt.destroyJob(j);

    // Accept all 100 server-side fds and let them sit (no writes).
    // We never read from these, just hold them so the connect
    // succeeds and the client read parks.
    var server_streams: [N]volt.net.TcpStream = undefined;
    for (&server_streams) |*s| s.* = try listener.accept();
    defer for (&server_streams) |*s| s.close();

    // Yield enough that all 100 are parked on read.
    var y: u32 = 0;
    while (y < 50) : (y += 1) try volt.yield();

    // Cancel all 100.
    for (workers) |j| j.cancel();
    for (workers) |j| j.join() catch {};

    return ctx.cancelled_count.load(.acquire);
}

test "cancel-audit:100 coroutines parked on TCP read, all exit on cancel" {
    const cancelled = try volt.run(.{ .allocator = std.testing.allocator }, tortureRoot, .{});
    try std.testing.expectEqual(@as(u32, 100), cancelled);
}

// ─────────────────────────────────────────────────────────────────────
// 6. Monotonic clock — verify nanoTimestamp is monotonic across yields
//    (which can move the coroutine between worker threads)
// ─────────────────────────────────────────────────────────────────────

fn monotonicWorker(samples: *std.array_list.Managed(i128)) !void {
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        const t = volt.time.nanoTimestamp();
        try samples.append(t);
        try volt.yield(); // potential cross-worker move
    }
}

fn monotonicRoot() !std.array_list.Managed(i128) {
    var samples = std.array_list.Managed(i128).init(std.testing.allocator);
    errdefer samples.deinit();

    // Spawn 4 coroutines each taking 50 timestamps with yields in
    // between. The yields cause the scheduler to potentially move
    // them between workers; we then verify monotonicity per-coro
    // (not across coros — that's not guaranteed for concurrent reads).
    const N: u32 = 4;
    var jobs: [N]*volt.Job = undefined;
    var per_coro_samples: [N]std.array_list.Managed(i128) = undefined;
    for (&per_coro_samples) |*s| s.* = std.array_list.Managed(i128).init(std.testing.allocator);
    defer for (&per_coro_samples) |*s| s.deinit();

    for (&jobs, 0..) |*j, idx| j.* = try volt.launch(monotonicWorker, .{&per_coro_samples[idx]});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    // Verify monotonicity within each coroutine's sequence.
    for (per_coro_samples) |coro_samples| {
        var k: usize = 1;
        while (k < coro_samples.items.len) : (k += 1) {
            try std.testing.expect(coro_samples.items[k] >= coro_samples.items[k - 1]);
        }
        // Push representative samples to the merged list for the
        // caller's "did we collect data" assertion.
        if (coro_samples.items.len > 0) try samples.append(coro_samples.items[0]);
    }
    return samples;
}

test "cancel-audit:nanoTimestamp monotonic per-coroutine across yields" {
    var samples = try volt.run(.{ .allocator = std.testing.allocator }, monotonicRoot, .{});
    defer samples.deinit();
    try std.testing.expectEqual(@as(usize, 4), samples.items.len);
}

// ─────────────────────────────────────────────────────────────────────
// 7. Cancel a coroutine parked in spawnBlocking — must wait for the
//    pool thread to finish writing the closure before returning, or
//    the closure (on the calling stack) is freed mid-write → UAF.
// ─────────────────────────────────────────────────────────────────────

const SpawnBlockingCancelCtx = struct {
    saw_cancelled: bool = false,
    pool_thread_finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn slowBlockingWork(ctx: *SpawnBlockingCancelCtx) u32 {
    // ~10 ms of work — long enough that we're parked when cancel lands.
    @import("../internal/thread.zig").sleep(10 * std.time.ns_per_ms);
    ctx.pool_thread_finished.store(true, .release);
    return 42;
}

fn spawnBlockingCancelCoro(ctx: *SpawnBlockingCancelCtx) void {
    if (volt.spawnBlocking(slowBlockingWork, .{ctx})) |_| {
        // unexpected — we cancelled
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn spawnBlockingCancelRoot() !SpawnBlockingCancelCtx {
    var ctx = SpawnBlockingCancelCtx{};
    const job = try volt.launch(spawnBlockingCancelCoro, .{&ctx});
    defer volt.destroyJob(job);

    // Yield enough that the child has dispatched to the blocking pool
    // and parked.
    var i: u32 = 0;
    while (i < 5) : (i += 1) try volt.yield();

    job.cancel();
    job.join() catch {};
    return ctx;
}

test "cancel-audit:cancel parked spawnBlocking → waits for pool thread (no UAF)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, spawnBlockingCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
    // The pool thread MUST have finished its work before our coroutine
    // returned — if it hadn't, the closure on the stack would have
    // been freed mid-write. We verify by checking the flag the pool
    // thread sets on completion.
    try std.testing.expect(ctx.pool_thread_finished.load(.acquire));
}

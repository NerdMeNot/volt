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
const helpers = @import("helpers.zig");

const PARK_TIMEOUT_NS: u64 = 1 * std.time.ns_per_s;

// ─────────────────────────────────────────────────────────────────────
// 1. Cancel a parked channel SENDER
// ─────────────────────────────────────────────────────────────────────

const SendCancelCtx = struct {
    ch: *volt.channel.Channel(u32),
    wg: *helpers.WaitGroup,
    saw_cancelled: bool = false,
    saw_ok: bool = false,
    saw_other_error: ?anyerror = null,
};

fn senderCoro(ctx: *SendCancelCtx) void {
    // Signal "I'm about to park" before the channel send. Tiny race
    // between done() and ch.send() reaching its park; cancel-from-
    // anywhere handles both states (running and parked) correctly.
    ctx.wg.done();
    // Channel is full — this send parks until either (a) someone
    // recv's, freeing a slot, or (b) cancellation wakes us. Expect (b).
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

    var wg = helpers.WaitGroup.init(1);
    var ctx = SendCancelCtx{ .ch = &ch, .wg = &wg };
    const sender = try volt.launch(senderCoro, .{&ctx});
    defer volt.destroyJob(sender);

    // Deterministic wait: returns once the sender has called done()
    // — i.e., it's about to park (or already parked).
    try wg.wait(PARK_TIMEOUT_NS);

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
    wg: *helpers.WaitGroup,
    saw_cancelled: bool = false,
};

fn receiverCoro(ctx: *RecvCancelCtx) void {
    ctx.wg.done();
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

    var wg = helpers.WaitGroup.init(1);
    var ctx = RecvCancelCtx{ .ch = &ch, .wg = &wg };
    const receiver = try volt.launch(receiverCoro, .{&ctx});
    defer volt.destroyJob(receiver);

    try wg.wait(PARK_TIMEOUT_NS);

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
    wg: *helpers.WaitGroup,
    saw_cancelled: bool = false,
};

fn sleeperCoro(ctx: *SleepCancelCtx) void {
    ctx.wg.done();
    // 1 hour — would never fire in a test; cancel must surface.
    if (volt.sleep(volt.Duration.fromSecs(3600))) |_| {
        // unexpected
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn sleepCancelRoot() !SleepCancelCtx {
    var wg = helpers.WaitGroup.init(1);
    var ctx = SleepCancelCtx{ .wg = &wg };
    const sleeper = try volt.launch(sleeperCoro, .{&ctx});
    defer volt.destroyJob(sleeper);

    try wg.wait(PARK_TIMEOUT_NS);

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
    wg: *helpers.WaitGroup,
    saw_cancelled: bool = false,
};

fn mutexWaiterCoro(ctx: *MutexCancelCtx) void {
    ctx.wg.done();
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

    var wg = helpers.WaitGroup.init(1);
    var ctx = MutexCancelCtx{ .mu = &mu, .wg = &wg };
    const waiter = try volt.launch(mutexWaiterCoro, .{&ctx});
    defer volt.destroyJob(waiter);

    try wg.wait(PARK_TIMEOUT_NS);

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
    wg: *helpers.WaitGroup,
    cancelled_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn tortureWorker(ctx: *TortureCtx) void {
    var stream = volt.net.TcpStream.connect(ctx.addr) catch {
        ctx.wg.done(); // signal even on connect failure so parent doesn't wedge
        return;
    };
    defer stream.close();

    // Signal "I'm about to park on read" so the parent's wg.wait()
    // returns once all 100 children have reached this point. The
    // race between done() and the read entering its park is fine:
    // cancel-from-anywhere works whether the coroutine is running
    // or parked.
    ctx.wg.done();

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

    const N: u32 = 100;
    var wg = helpers.WaitGroup.init(N);
    var ctx = TortureCtx{ .listener = &listener, .addr = addr, .wg = &wg };

    // Spawn 100 client connections, each parked on read.
    var workers: [N]*volt.Job = undefined;
    for (&workers) |*j| j.* = try volt.launch(tortureWorker, .{&ctx});
    defer for (workers) |j| volt.destroyJob(j);

    // Accept all 100 server-side fds and let them sit (no writes).
    // We never read from these, just hold them so the connect
    // succeeds and the client read parks.
    var server_streams: [N]volt.net.TcpStream = undefined;
    for (&server_streams) |*s| s.* = try listener.accept();
    defer for (&server_streams) |*s| s.close();

    // Deterministic wait: returns once all 100 workers have reached
    // their pre-read signal. 5s budget is generous; a healthy
    // runtime hits this in <100ms.
    try wg.wait(5 * std.time.ns_per_s);

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
    pool_started: *helpers.Latch,
    saw_cancelled: bool = false,
    pool_thread_finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn slowBlockingWork(ctx: *SpawnBlockingCancelCtx) u32 {
    // Signal "the pool thread is now running" — this is the
    // synchronization point the test depends on. Cancel must land
    // AFTER this so the UAF guard (Futex.wait on closure.tag in
    // spawn_blocking.zig) actually has work to do. Done from
    // inside the pool thread, not from the dispatching coroutine,
    // because the cancel-vs-dispatch race is what we're testing.
    ctx.pool_started.signal();

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
    var pool_started = helpers.Latch{};
    var ctx = SpawnBlockingCancelCtx{ .pool_started = &pool_started };
    const job = try volt.launch(spawnBlockingCancelCoro, .{&ctx});
    defer volt.destroyJob(job);

    // Deterministic wait: only cancel once the pool thread is
    // actually running its work. Without this, cancel-before-
    // dispatch returns Cancelled immediately and the pool thread
    // never runs (pool_thread_finished stays false → test fails).
    try pool_started.wait(1 * std.time.ns_per_s);

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

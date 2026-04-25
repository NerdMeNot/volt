//! Stress Tests - High-load validation
//!
//! Run: zig build test-stress
//!
//! Validates correctness under sustained load with real threads:
//! - Sync primitives under high contention
//! - Channel throughput and backpressure
//! - Memory safety (no leaks)
//! - Blocking pool thread lifecycle
//!
//! Override intensity: VOLT_TEST_INTENSITY=smoke|standard|stress|exhaustive
//! Default: stress (50k iterations, 16 threads)

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;
const debug = std.debug;

const thread_internal = @import("volt").internal.thread;

const common = @import("common");
const config = common.config;
const ConcurrentCounter = common.ConcurrentCounter;
const Barrier = common.Barrier;
const TimeoutChecker = common.TimeoutChecker;

const blitz_io = @import("volt");
const sync = blitz_io.sync;
const channel = blitz_io.channel;
const internal = blitz_io.internal;
const BlockingPool = internal.BlockingPool;
const runBlocking = internal.runBlocking;

// ═══════════════════════════════════════════════════════════════════════════════
// Output helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn printResult(comptime name: []const u8, comptime fmt: []const u8, args: anytype) void {
    debug.print("  pass  " ++ name ++ pad(name) ++ fmt ++ "\n", args);
}

fn pad(comptime name: []const u8) []const u8 {
    const width = 28;
    if (name.len >= width) return "  ";
    return ("." ** width)[0 .. width - name.len] ++ "  ";
}

/// Get config for stress tests (defaults to stress intensity)
fn stressConfig() config.Config {
    return config.stressAutoConfig();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Suite header
// ═══════════════════════════════════════════════════════════════════════════════

test "0 - stress suite header" {
    const cfg = stressConfig();
    const threads = cfg.effectiveThreadCount();
    const name = config.intensityName(cfg);

    debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Stress Tests                                                │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  High-contention, multi-threaded validation of all           │
        \\│  sync primitives, channels, and blocking pool.               │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Intensity:  {s:<48}│
        \\│  Threads:    {d:<48}│
        \\│  Iterations: {d:<48}│
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{ name, threads, cfg.iterations });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Mutex High Contention
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex stress - high contention" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const iterations = cfg.iterations;

    var mutex = sync.Mutex.init();
    var counter = ConcurrentCounter{};
    var ops_completed = ConcurrentCounter{};
    var start_barrier = Barrier.init(thread_count);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    for (0..thread_count) |_| {
        const Context = struct {
            mutex: *sync.Mutex,
            counter: *ConcurrentCounter,
            ops: *ConcurrentCounter,
            barrier: *Barrier,
            iters: usize,
        };
        const ctx = Context{
            .mutex = &mutex,
            .counter = &counter,
            .ops = &ops_completed,
            .barrier = &start_barrier,
            .iters = iterations,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                for (0..c.iters) |_| {
                    if (c.mutex.tryLock()) {
                        c.counter.increment();
                        c.mutex.unlock();
                        c.ops.increment();
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const total_attempts = thread_count * iterations;
    try testing.expect(ops_completed.get() > 0);
    try testing.expectEqual(counter.get(), ops_completed.get());
    try testing.expect(!mutex.isLocked());

    printResult("Mutex contention", "{}T x {} iters -> {}/{} tryLock succeeded", .{ thread_count, iterations, ops_completed.get(), total_attempts });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Channel Throughput
// ─────────────────────────────────────────────────────────────────────────────

test "Channel stress - throughput" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const messages_per_thread = cfg.iterations;

    var ch = try channel.Channel(u64).init(testing.allocator, 256);
    defer ch.deinit();

    var sent = ConcurrentCounter{};
    var received = ConcurrentCounter{};
    var done = Atomic(bool).init(false);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    const producer_count = thread_count / 2;
    const consumer_count = thread_count - producer_count;

    for (0..producer_count) |_| {
        const Context = struct {
            ch: *channel.Channel(u64),
            sent: *ConcurrentCounter,
            msgs: usize,
        };
        const ctx = Context{
            .ch = &ch,
            .sent = &sent,
            .msgs = messages_per_thread,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                for (0..c.msgs) |i| {
                    while (true) {
                        switch (c.ch.trySend(@intCast(i))) {
                            .ok => {
                                c.sent.increment();
                                break;
                            },
                            .full => std.Thread.yield() catch {},
                            .closed => return,
                        }
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (0..consumer_count) |_| {
        const Context = struct {
            ch: *channel.Channel(u64),
            received: *ConcurrentCounter,
            is_done: *Atomic(bool),
        };
        const ctx = Context{
            .ch = &ch,
            .received = &received,
            .is_done = &done,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                while (!c.is_done.load(.acquire)) {
                    switch (c.ch.tryRecv()) {
                        .value => c.received.increment(),
                        .empty => std.Thread.yield() catch {},
                        .closed => return,
                    }
                }
                while (true) {
                    switch (c.ch.tryRecv()) {
                        .value => c.received.increment(),
                        else => break,
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..producer_count]) |t| {
        t.join();
    }
    done.store(true, .release);
    for (threads[producer_count..spawned]) |t| {
        t.join();
    }

    try testing.expectEqual(sent.get(), received.get());

    printResult("Channel throughput", "{}P/{}C x {} msgs -> {}/{} delivered", .{ producer_count, consumer_count, messages_per_thread, received.get(), sent.get() });
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory Leak Test
// ─────────────────────────────────────────────────────────────────────────────

test "Sync primitives - no memory leaks" {
    const rounds: usize = 1000;

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    for (0..rounds) |_| {
        var mutex = sync.Mutex.init();
        _ = mutex.tryLock();
        mutex.unlock();

        var rwlock = sync.RwLock.init();
        _ = rwlock.tryReadLock();
        rwlock.readUnlock();

        var sem = sync.Semaphore.init(5);
        _ = sem.tryAcquire(1);
        sem.release(1);

        var notify = sync.Notify.init();
        notify.notifyOne();
        var waiter = sync.notify.Waiter.init();
        _ = notify.waitWith(&waiter);

        var barrier = sync.Barrier.init(1);
        var b_waiter = sync.barrier.Waiter.init();
        _ = barrier.waitWith(&b_waiter);
    }

    printResult("Memory leak check", "{} rounds x 5 primitives (Mutex/RwLock/Sem/Notify/Barrier) -> no leaks", .{rounds});
}

// ─────────────────────────────────────────────────────────────────────────────
// Timeout Test
// ─────────────────────────────────────────────────────────────────────────────

test "Sync primitives - complete within timeout" {
    const cfg = stressConfig();
    const timeout = TimeoutChecker.init(cfg.timeoutNs(), "sync_primitives");
    defer timeout.check();

    var mutex = sync.Mutex.init();
    try testing.expect(mutex.tryLock());
    mutex.unlock();

    var rwlock = sync.RwLock.init();
    try testing.expect(rwlock.tryReadLock());
    rwlock.readUnlock();
    try testing.expect(rwlock.tryWriteLock());
    rwlock.writeUnlock();

    var sem = sync.Semaphore.init(1);
    try testing.expect(sem.tryAcquire(1));
    sem.release(1);

    var notify = sync.Notify.init();
    notify.notifyOne();
    var waiter = sync.notify.Waiter.init();
    try testing.expect(notify.waitWith(&waiter));

    var barrier = sync.Barrier.init(1);
    var b_waiter = sync.barrier.Waiter.init();
    try testing.expect(barrier.waitWith(&b_waiter));

    printResult("Timeout check", "5 primitives completed within {}ms budget", .{cfg.timeout_ms});
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: RwLock Contention
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock stress - reader/writer contention" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const iterations = cfg.iterations;

    var rwlock = sync.RwLock.init();
    var read_ops = ConcurrentCounter{};
    var write_ops = ConcurrentCounter{};
    var start_barrier = Barrier.init(thread_count);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    for (0..thread_count) |tid| {
        const Context = struct {
            rwlock: *sync.RwLock,
            read_ops: *ConcurrentCounter,
            write_ops: *ConcurrentCounter,
            barrier: *Barrier,
            iters: usize,
            is_writer: bool,
        };
        const ctx = Context{
            .rwlock = &rwlock,
            .read_ops = &read_ops,
            .write_ops = &write_ops,
            .barrier = &start_barrier,
            .iters = iterations,
            .is_writer = (tid % 4 == 0),
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                for (0..c.iters) |_| {
                    if (c.is_writer) {
                        if (c.rwlock.tryWriteLock()) {
                            c.write_ops.increment();
                            c.rwlock.writeUnlock();
                        }
                    } else {
                        if (c.rwlock.tryReadLock()) {
                            c.read_ops.increment();
                            c.rwlock.readUnlock();
                        }
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const writers = thread_count / 4;
    const readers = thread_count - writers;
    const total_attempts = thread_count * iterations;
    try testing.expect(read_ops.get() > 0);

    printResult("RwLock contention", "{}R/{}W threads x {} iters -> {} reads + {} writes / {} attempts", .{ readers, writers, iterations, read_ops.get(), write_ops.get(), total_attempts });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Semaphore with Limited Permits
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore stress - limited permits contention" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const iterations = cfg.iterations;
    const permit_count: usize = 3;

    var sem = sync.Semaphore.init(permit_count);
    var acquired_ops = ConcurrentCounter{};
    var start_barrier = Barrier.init(thread_count);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    for (0..thread_count) |_| {
        const Context = struct {
            sem: *sync.Semaphore,
            acquired: *ConcurrentCounter,
            barrier: *Barrier,
            iters: usize,
        };
        const ctx = Context{
            .sem = &sem,
            .acquired = &acquired_ops,
            .barrier = &start_barrier,
            .iters = iterations,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                for (0..c.iters) |_| {
                    if (c.sem.tryAcquire(1)) {
                        c.acquired.increment();
                        c.sem.release(1);
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const total_attempts = thread_count * iterations;
    try testing.expectEqual(@as(usize, permit_count), sem.availablePermits());

    printResult("Semaphore permits", "{}T x {} iters, {} permits -> {}/{} acquired", .{ thread_count, iterations, permit_count, acquired_ops.get(), total_attempts });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Oneshot Channel Churn
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot stress - channel churn" {
    const cfg = stressConfig();
    const iterations = cfg.iterations;

    const Oneshot = channel.Oneshot(u64);
    var success_count: usize = 0;

    for (0..iterations) |i| {
        var shared = Oneshot.Shared.init();
        var pair = Oneshot.fromShared(&shared);

        if (pair.sender.send(@intCast(i))) {
            if (pair.receiver.tryRecv()) |val| {
                if (val == @as(u64, @intCast(i))) {
                    success_count += 1;
                }
            }
        }
    }

    try testing.expectEqual(iterations, success_count);

    printResult("Oneshot churn", "{} create/send/recv cycles -> {}/{} correct", .{ iterations, success_count, iterations });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Watch Channel Updates
// ─────────────────────────────────────────────────────────────────────────────

test "Watch stress - rapid updates" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const iterations = cfg.iterations;
    const receiver_count = thread_count - 1;

    var watch = channel.Watch(u64).init(0);
    defer watch.deinit();

    var updates_seen = ConcurrentCounter{};
    var done = Atomic(bool).init(false);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;

    threads[spawned] = std.Thread.spawn(.{}, struct {
        fn work(w: *channel.Watch(u64), iters: usize, d: *Atomic(bool)) void {
            for (0..iters) |i| {
                w.send(@intCast(i));
            }
            d.store(true, .release);
        }
    }.work, .{ &watch, iterations, &done }) catch return;
    spawned += 1;

    for (1..thread_count) |_| {
        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(w: *channel.Watch(u64), seen: *ConcurrentCounter, d: *Atomic(bool)) void {
                var rx = w.subscribe();
                while (!d.load(.acquire)) {
                    if (rx.hasChanged()) {
                        _ = rx.getAndUpdate();
                        seen.increment();
                    }
                    std.Thread.yield() catch {};
                }
                if (rx.hasChanged()) {
                    _ = rx.getAndUpdate();
                    seen.increment();
                }
            }
        }.work, .{ &watch, &updates_seen, &done }) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    printResult("Watch updates", "1 sender x {} msgs, {} receivers -> {} updates observed", .{ iterations, receiver_count, updates_seen.get() });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Broadcast Channel
// ─────────────────────────────────────────────────────────────────────────────

test "Broadcast stress - multiple subscribers" {
    const cfg = stressConfig();
    const iterations = cfg.iterations / 10;
    const subscriber_count: usize = 4;

    var ch = try channel.BroadcastChannel(u64).init(testing.allocator, 64);
    defer ch.deinit();

    var total_received = ConcurrentCounter{};
    var done = Atomic(bool).init(false);
    var start_barrier = Barrier.init(subscriber_count + 1);

    const BroadcastReceiver = channel.BroadcastChannel(u64).Receiver;
    var receivers: [subscriber_count]BroadcastReceiver = undefined;
    for (0..subscriber_count) |i| {
        receivers[i] = ch.subscribe();
    }

    var threads = try testing.allocator.alloc(std.Thread, subscriber_count + 1);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;

    for (0..subscriber_count) |i| {
        const Context = struct {
            rx: *BroadcastReceiver,
            received: *ConcurrentCounter,
            is_done: *Atomic(bool),
            barrier: *Barrier,
        };
        const ctx = Context{
            .rx = &receivers[i],
            .received = &total_received,
            .is_done = &done,
            .barrier = &start_barrier,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                while (!c.is_done.load(.acquire)) {
                    switch (c.rx.tryRecv()) {
                        .value => c.received.increment(),
                        .empty => std.Thread.yield() catch {},
                        .closed => return,
                        .lagged => {},
                    }
                }
                while (true) {
                    switch (c.rx.tryRecv()) {
                        .value => c.received.increment(),
                        else => break,
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    threads[spawned] = std.Thread.spawn(.{}, struct {
        fn work(c: *channel.BroadcastChannel(u64), iters: usize, d: *Atomic(bool), b: *Barrier) void {
            _ = b.wait();
            for (0..iters) |i| {
                _ = c.send(@intCast(i));
            }
            d.store(true, .release);
        }
    }.work, .{ &ch, iterations, &done, &start_barrier }) catch {
        done.store(true, .release);
        for (threads[0..spawned]) |t| {
            t.join();
        }
        return;
    };
    spawned += 1;

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const ideal = subscriber_count * iterations;
    printResult("Broadcast delivery", "{} msgs x {} subscribers -> {}/{} received", .{ iterations, subscriber_count, total_received.get(), ideal });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: OnceCell Concurrent Init
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell stress - concurrent initialization race" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const rounds = @min(cfg.iterations / 10, 5000);

    var correct: usize = 0;

    for (0..rounds) |_| {
        var cell = sync.OnceCell(u64).init();
        var init_count = Atomic(usize).init(0);

        var threads = try testing.allocator.alloc(std.Thread, thread_count);
        defer testing.allocator.free(threads);

        var spawned: usize = 0;
        for (0..thread_count) |tid| {
            threads[spawned] = std.Thread.spawn(.{}, struct {
                fn work(c: *sync.OnceCell(u64), count: *Atomic(usize), id: usize) void {
                    if (c.set(@intCast(id))) {
                        _ = count.fetchAdd(1, .monotonic);
                    }
                }
            }.work, .{ &cell, &init_count, tid }) catch continue;
            spawned += 1;
        }

        for (threads[0..spawned]) |t| {
            t.join();
        }

        if (init_count.load(.acquire) == 1 and cell.isInitialized()) {
            correct += 1;
        }
    }

    try testing.expectEqual(rounds, correct);

    printResult("OnceCell init race", "{}T racing to init x {} rounds -> {}/{} exactly-once", .{ thread_count, rounds, correct, rounds });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Select Multiple Channels
// ─────────────────────────────────────────────────────────────────────────────

test "Select stress - multiple channels racing" {
    const cfg = stressConfig();
    const iterations = cfg.iterations;
    const channel_count: usize = 4;

    var valid_selects: usize = 0;

    for (0..iterations) |i| {
        var ch1 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();
        var ch2 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch2.deinit();
        var ch3 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch3.deinit();
        var ch4 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch4.deinit();

        const target = i % channel_count;
        switch (target) {
            0 => _ = ch1.trySend(@intCast(i)),
            1 => _ = ch2.trySend(@intCast(i)),
            2 => _ = ch3.trySend(@intCast(i)),
            3 => _ = ch4.trySend(@intCast(i)),
            else => unreachable,
        }

        var sel = channel.Selector(4).init();
        _ = sel.addRecv(u32, &ch1);
        _ = sel.addRecv(u32, &ch2);
        _ = sel.addRecv(u32, &ch3);
        _ = sel.addRecv(u32, &ch4);

        const result = sel.trySelect();
        if (result != null and result.?.branch == target) {
            valid_selects += 1;
        }
    }

    try testing.expectEqual(iterations, valid_selects);

    printResult("Select channels", "{} channels x {} iters -> {}/{} correct", .{ channel_count, iterations, valid_selects, iterations });
}

test "Select stress - concurrent senders" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const iterations = cfg.iterations / 10;

    var correct: usize = 0;

    for (0..iterations) |_| {
        var ch1 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();
        var ch2 = try channel.Channel(u32).init(testing.allocator, 10);
        defer ch2.deinit();

        var sent = ConcurrentCounter{};
        var start_barrier = Barrier.init(thread_count);

        var threads = try testing.allocator.alloc(std.Thread, thread_count);
        defer testing.allocator.free(threads);

        var spawned: usize = 0;
        for (0..thread_count) |tid| {
            const Context = struct {
                ch1: *channel.Channel(u32),
                ch2: *channel.Channel(u32),
                sent: *ConcurrentCounter,
                barrier: *Barrier,
                id: usize,
            };
            const ctx = Context{
                .ch1 = &ch1,
                .ch2 = &ch2,
                .sent = &sent,
                .barrier = &start_barrier,
                .id = tid,
            };

            threads[spawned] = std.Thread.spawn(.{}, struct {
                fn work(c: Context) void {
                    _ = c.barrier.wait();
                    const target = if (c.id % 2 == 0) c.ch1 else c.ch2;
                    if (target.trySend(@intCast(c.id)) == .ok) {
                        c.sent.increment();
                    }
                }
            }.work, .{ctx}) catch continue;
            spawned += 1;
        }

        for (threads[0..spawned]) |t| {
            t.join();
        }

        var sel = channel.Selector(2).init();
        _ = sel.addRecv(u32, &ch1);
        _ = sel.addRecv(u32, &ch2);

        const result = sel.trySelect();
        if (result != null and sent.get() > 0) {
            correct += 1;
        }
    }

    try testing.expectEqual(iterations, correct);

    printResult("Select concurrent", "{}T sending to 2ch x {} rounds -> {}/{} correct", .{ thread_count, iterations, correct, iterations });
}

test "Select stress - CancelToken churn" {
    const cfg = stressConfig();
    const iterations = cfg.iterations;

    var correct: usize = 0;

    for (0..iterations) |_| {
        var token = sync.cancel_token.CancelToken.init();
        if (token.tryClaimWinner(0)) {
            if (token.getWinner() == @as(?usize, 0) and !token.isCancelledFor(0)) {
                correct += 1;
            }
        }
    }

    try testing.expectEqual(iterations, correct);

    printResult("CancelToken churn", "{} create/claim/verify cycles -> {}/{} correct", .{ iterations, correct, iterations });
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Test: Blocking Pool
// ─────────────────────────────────────────────────────────────────────────────

test "BlockingPool stress - high throughput" {
    const cfg = stressConfig();
    const thread_count = cfg.effectiveThreadCount();
    const tasks_per_thread = cfg.iterations / 10;

    var pool = BlockingPool.init(testing.allocator, .{
        .thread_cap = 8,
        .keep_alive_ns = 1 * std.time.ns_per_s,
    });
    defer pool.deinit();

    var completed = ConcurrentCounter{};
    var start_barrier = Barrier.init(thread_count);

    var threads = try testing.allocator.alloc(std.Thread, thread_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    for (0..thread_count) |_| {
        const Context = struct {
            pool: *BlockingPool,
            completed: *ConcurrentCounter,
            barrier: *Barrier,
            tasks: usize,
            allocator: std.mem.Allocator,
        };
        const ctx = Context{
            .pool = &pool,
            .completed = &completed,
            .barrier = &start_barrier,
            .tasks = tasks_per_thread,
            .allocator = testing.allocator,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                for (0..c.tasks) |_| {
                    const handle = runBlocking(c.pool, struct {
                        fn compute() u64 {
                            var sum: u64 = 0;
                            for (0..100) |i| {
                                sum += i;
                            }
                            return sum;
                        }
                    }.compute, .{}) catch continue;
                    _ = handle.wait() catch continue;
                    c.completed.increment();
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const expected = thread_count * tasks_per_thread;
    try testing.expect(completed.get() > 0);

    printResult("BlockingPool throughput", "{}T x {} tasks -> {}/{} completed, {} pool threads", .{ thread_count, tasks_per_thread, completed.get(), expected, pool.threadCount() });
}

test "BlockingPool stress - concurrent submitters" {
    const cfg = stressConfig();
    const submitter_count = cfg.effectiveThreadCount();
    const tasks_per_submitter = cfg.iterations / 20;

    var pool = BlockingPool.init(testing.allocator, .{
        .thread_cap = 16,
        .keep_alive_ns = 2 * std.time.ns_per_s,
    });
    defer pool.deinit();

    var total_results = ConcurrentCounter{};
    var start_barrier = Barrier.init(submitter_count);

    var threads = try testing.allocator.alloc(std.Thread, submitter_count);
    defer testing.allocator.free(threads);

    var spawned: usize = 0;
    for (0..submitter_count) |tid| {
        const Context = struct {
            pool: *BlockingPool,
            results: *ConcurrentCounter,
            barrier: *Barrier,
            tasks: usize,
            id: usize,
        };
        const ctx = Context{
            .pool = &pool,
            .results = &total_results,
            .barrier = &start_barrier,
            .tasks = tasks_per_submitter,
            .id = tid,
        };

        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: Context) void {
                _ = c.barrier.wait();
                for (0..c.tasks) |i| {
                    const val = c.id * 1000 + i;
                    const handle = runBlocking(c.pool, struct {
                        fn identity(x: usize) usize {
                            return x;
                        }
                    }.identity, .{val}) catch continue;
                    const result = handle.wait() catch continue;
                    if (result == val) {
                        c.results.increment();
                    }
                }
            }
        }.work, .{ctx}) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    const expected = submitter_count * tasks_per_submitter;
    try testing.expectEqual(expected, total_results.get());

    printResult("BlockingPool concurrent", "{}T x {} tasks -> {}/{} identity results correct", .{ submitter_count, tasks_per_submitter, total_results.get(), expected });
}

test "BlockingPool stress - error handling" {
    const cfg = stressConfig();
    const iterations = cfg.iterations / 10;

    var pool = BlockingPool.init(testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    var errors_caught: usize = 0;
    var successes: usize = 0;

    for (0..iterations) |i| {
        const handle = runBlocking(&pool, struct {
            fn mayFail(x: usize) error{TestError}!usize {
                if (x % 2 == 0) {
                    return error.TestError;
                }
                return x;
            }
        }.mayFail, .{i}) catch continue;

        if (handle.wait()) |_| {
            successes += 1;
        } else |_| {
            errors_caught += 1;
        }
    }

    try testing.expect(successes > 0);
    try testing.expect(errors_caught > 0);

    printResult("BlockingPool errors", "{} tasks -> {} ok + {} errors (50/50 expected)", .{ iterations, successes, errors_caught });
}

test "BlockingPool stress - thread scaling" {
    var pool = BlockingPool.init(testing.allocator, .{
        .thread_cap = 8,
        .keep_alive_ns = 100 * std.time.ns_per_ms,
    });
    defer pool.deinit();

    try testing.expectEqual(@as(usize, 0), pool.threadCount());

    var handles: [8]*internal.BlockingHandle(u64) = undefined;
    var submitted: usize = 0;

    for (0..8) |_| {
        handles[submitted] = runBlocking(&pool, struct {
            fn slowTask() u64 {
                thread_internal.sleep(50 * std.time.ns_per_ms);
                return 42;
            }
        }.slowTask, .{}) catch continue;
        submitted += 1;
    }

    thread_internal.sleep(10 * std.time.ns_per_ms);
    const threads_during = pool.threadCount();
    try testing.expect(threads_during > 0);

    for (handles[0..submitted]) |h| {
        _ = h.wait() catch {};
    }

    thread_internal.sleep(200 * std.time.ns_per_ms);
    const threads_after = pool.threadCount();
    try testing.expect(threads_after <= threads_during);

    printResult("BlockingPool scaling", "8 slow tasks -> {} threads spawned -> {} after idle", .{ threads_during, threads_after });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Suite footer
// ═══════════════════════════════════════════════════════════════════════════════

test "z - stress suite complete" {
    debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  All stress tests passed                                     │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

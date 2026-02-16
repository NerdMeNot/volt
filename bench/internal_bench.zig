//! Volt Internal Primitives Benchmark Suite
//!
//! Benchmarks for the internal (non-Future-based) sync primitives.
//! These are the low-level primitives that power the higher-level API.
//!
//! Statistics: median of 10 iterations (3 warmup discarded).
//!
//! Run with: zig build bench-internal
//! JSON output: zig build bench-internal -- --json

const std = @import("std");
const Atomic = std.atomic.Value;

// Internal sync primitives
const Mutex = @import("sync").Mutex;
const RwLock = @import("sync").RwLock;
const Semaphore = @import("sync").Semaphore;
const Notify = @import("sync").Notify;
const NotifyWaiter = @import("sync").NotifyWaiter;
const Barrier = @import("sync").Barrier;
const BarrierWaiter = @import("sync").BarrierWaiter;
const OnceCell = @import("sync").OnceCell;

// Channel primitives
const Channel = @import("channel").Channel;
const Oneshot = @import("channel").Oneshot;
const BroadcastChannel = @import("channel").BroadcastChannel;
const Watch = @import("channel").Watch;

// Blocking pool
const blocking = @import("sync").blocking;
const BlockingPool = blocking.BlockingPool;
const runBlocking = blocking.runBlocking;

const print = std.debug.print;
const Allocator = std.mem.Allocator;

// ============================================================================
// Configuration
// ============================================================================

const ITERATIONS = 10;
const WARMUP_ITERATIONS = 3;
const OPS_PER_ITERATION = 100_000;
const CHANNEL_SIZE = 1000;

// ============================================================================
// Statistics helpers
// ============================================================================

pub const Stats = struct {
    samples: [64]u64 = [_]u64{0} ** 64,
    count: usize = 0,

    pub fn init() Stats {
        return .{};
    }

    pub fn add(self: *Stats, ns: u64) void {
        if (self.count < 64) {
            self.samples[self.count] = ns;
            self.count += 1;
        }
    }

    pub fn medianNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var sorted: [64]u64 = self.samples;
        std.mem.sort(u64, sorted[0..self.count], {}, std.sort.asc(u64));
        return sorted[self.count / 2];
    }

    pub fn minNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = std.math.maxInt(u64);
        for (self.samples[0..self.count]) |s| m = @min(m, s);
        return m;
    }

    pub fn maxNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = 0;
        for (self.samples[0..self.count]) |s| m = @max(m, s);
        return m;
    }

    pub fn nsPerOp(self: Stats, ops: usize) f64 {
        return @as(f64, @floatFromInt(self.medianNs())) / @as(f64, @floatFromInt(ops));
    }

    pub fn opsPerSec(self: Stats, ops: usize) f64 {
        const median_ns = self.medianNs();
        if (median_ns == 0) return 0;
        return @as(f64, @floatFromInt(ops)) * 1_000_000_000.0 / @as(f64, @floatFromInt(median_ns));
    }
};

// ============================================================================
// Benchmark Runner
// ============================================================================

fn runBench(comptime _: []const u8, comptime benchFn: fn () u64) Stats {
    var stats = Stats.init();

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        _ = benchFn();
    }

    // Measured runs
    for (0..ITERATIONS) |_| {
        const elapsed = benchFn();
        stats.add(elapsed);
    }

    return stats;
}

fn printResult(name: []const u8, stats: Stats, ops: usize) void {
    print("  {s:<24} {d:>8.1}ns/op    {d:>12.0} ops/sec\n", .{
        name,
        stats.nsPerOp(ops),
        stats.opsPerSec(ops),
    });
}

// ============================================================================
// Mutex Benchmarks
// ============================================================================

fn benchMutexTryLock() u64 {
    var mutex = Mutex.init();
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        if (mutex.tryLock()) {
            mutex.unlock();
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchMutexContended() u64 {
    var mutex = Mutex.init();
    var counter = Atomic(usize).init(0);
    const thread_count: usize = 4;
    const ops_per_thread = OPS_PER_ITERATION / thread_count;

    const start = std.time.nanoTimestamp();

    var threads: [thread_count]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..thread_count) |_| {
        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(m: *Mutex, c: *Atomic(usize), ops: usize) void {
                for (0..ops) |_| {
                    if (m.tryLock()) {
                        _ = c.fetchAdd(1, .monotonic);
                        m.unlock();
                    }
                }
            }
        }.work, .{ &mutex, &counter, ops_per_thread }) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// RwLock Benchmarks
// ============================================================================

fn benchRwLockRead() u64 {
    var rwlock = RwLock.init();
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        if (rwlock.tryReadLock()) {
            rwlock.readUnlock();
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchRwLockWrite() u64 {
    var rwlock = RwLock.init();
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        if (rwlock.tryWriteLock()) {
            rwlock.writeUnlock();
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchRwLockMixed() u64 {
    var rwlock = RwLock.init();
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |i| {
        if (i % 10 == 0) {
            // 10% writes
            if (rwlock.tryWriteLock()) {
                rwlock.writeUnlock();
            }
        } else {
            // 90% reads
            if (rwlock.tryReadLock()) {
                rwlock.readUnlock();
            }
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// Semaphore Benchmarks
// ============================================================================

fn benchSemaphoreAcquireRelease() u64 {
    var sem = Semaphore.init(OPS_PER_ITERATION);
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        if (sem.tryAcquire(1)) {
            sem.release(1);
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchSemaphoreMultiPermit() u64 {
    var sem = Semaphore.init(100);
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        if (sem.tryAcquire(5)) {
            sem.release(5);
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// Notify Benchmarks
// ============================================================================

fn benchNotifyOne() u64 {
    var notify = Notify.init();
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        notify.notifyOne();
        var waiter = NotifyWaiter.init();
        _ = notify.waitWith(&waiter);
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// Channel Benchmarks
// ============================================================================

fn benchChannelSendRecv(allocator: Allocator) u64 {
    var ch = Channel(u64).init(allocator, CHANNEL_SIZE) catch return 0;
    defer ch.deinit();

    const start = std.time.nanoTimestamp();

    // Fill and drain cycles
    for (0..OPS_PER_ITERATION / CHANNEL_SIZE) |_| {
        // Fill
        for (0..CHANNEL_SIZE) |i| {
            _ = ch.trySend(@intCast(i));
        }
        // Drain
        for (0..CHANNEL_SIZE) |_| {
            _ = ch.tryRecv();
        }
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchChannelRoundtrip(allocator: Allocator) u64 {
    var ch = Channel(u64).init(allocator, 1) catch return 0;
    defer ch.deinit();

    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |i| {
        _ = ch.trySend(@intCast(i));
        _ = ch.tryRecv();
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// Oneshot Benchmarks
// ============================================================================

fn benchOneshot() u64 {
    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |i| {
        const OneshotU64 = Oneshot(u64);
        var shared = OneshotU64.Shared.init();
        var pair = OneshotU64.fromShared(&shared);
        _ = pair.sender.send(@intCast(i));
        _ = pair.receiver.tryRecv();
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// OnceCell Benchmarks
// ============================================================================

fn benchOnceCellGet() u64 {
    var cell = OnceCell(u64).initWith(42);

    const start = std.time.nanoTimestamp();

    for (0..OPS_PER_ITERATION) |_| {
        std.mem.doNotOptimizeAway(cell.get());
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchOnceCellInitRace() u64 {
    const thread_count: usize = 4;
    var cell = OnceCell(u64).init();

    const start = std.time.nanoTimestamp();

    var threads: [thread_count]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..thread_count) |tid| {
        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn work(c: *OnceCell(u64), id: usize) void {
                for (0..OPS_PER_ITERATION / thread_count) |_| {
                    // Race to set value
                    _ = c.set(@as(u64, id));
                    // Read value
                    std.mem.doNotOptimizeAway(c.get());
                }
            }
        }.work, .{ &cell, tid }) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// Blocking Pool Benchmarks
// ============================================================================

const BLOCKING_OPS = 10_000; // Fewer ops since blocking tasks have overhead

fn benchBlockingPoolSubmit(allocator: Allocator) u64 {
    var pool = BlockingPool.init(allocator, .{
        .thread_cap = 4,
        .keep_alive_ns = 1 * std.time.ns_per_s,
    });
    defer pool.deinit();

    // Simple task that just increments a counter
    var counter = std.atomic.Value(usize).init(0);

    const Context = struct {
        counter: *std.atomic.Value(usize),
        task: blocking.Task,

        fn run(task: *blocking.Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            _ = self.counter.fetchAdd(1, .acq_rel);
        }
    };

    var contexts: [BLOCKING_OPS]Context = undefined;
    for (0..BLOCKING_OPS) |i| {
        contexts[i] = .{
            .counter = &counter,
            .task = .{ .func = Context.run },
        };
    }

    const start = std.time.nanoTimestamp();

    // Submit all tasks
    for (0..BLOCKING_OPS) |i| {
        pool.submit(&contexts[i].task) catch {};
    }

    // Wait for all to complete
    while (counter.load(.acquire) < BLOCKING_OPS) {
        std.Thread.yield() catch {};
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchBlockingPoolRoundtrip(allocator: Allocator) u64 {
    var pool = BlockingPool.init(allocator, .{
        .thread_cap = 4,
        .keep_alive_ns = 1 * std.time.ns_per_s,
    });
    defer pool.deinit();

    const start = std.time.nanoTimestamp();

    // Submit task and wait for result, repeat
    for (0..BLOCKING_OPS) |i| {
        const handle = runBlocking(&pool, struct {
            fn compute(x: usize) usize {
                return x * 2;
            }
        }.compute, .{i}) catch continue;

        _ = handle.wait() catch {};
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

fn benchBlockingPoolContended(allocator: Allocator) u64 {
    var pool = BlockingPool.init(allocator, .{
        .thread_cap = 8,
        .keep_alive_ns = 1 * std.time.ns_per_s,
    });
    defer pool.deinit();

    const thread_count: usize = 4;
    const ops_per_thread = BLOCKING_OPS / thread_count;
    var completed = std.atomic.Value(usize).init(0);

    const start = std.time.nanoTimestamp();

    var threads: [thread_count]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..thread_count) |_| {
        threads[spawned] = std.Thread.spawn(.{}, struct {
            fn submitter(p: *BlockingPool, ops: usize, done: *std.atomic.Value(usize)) void {
                for (0..ops) |i| {
                    const handle = runBlocking(p, struct {
                        fn work(x: usize) usize {
                            return x + 1;
                        }
                    }.work, .{i}) catch continue;

                    _ = handle.wait() catch {};
                    _ = done.fetchAdd(1, .acq_rel);
                }
            }
        }.submitter, .{ &pool, ops_per_thread, &completed }) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |t| {
        t.join();
    }

    return @intCast(std.time.nanoTimestamp() - start);
}

// ============================================================================
// JSON Output
// ============================================================================

const JsonOutput = struct {
    mutex_trylock_ns: f64 = 0,
    mutex_contended_ns: f64 = 0,
    rwlock_read_ns: f64 = 0,
    rwlock_write_ns: f64 = 0,
    rwlock_mixed_ns: f64 = 0,
    semaphore_ns: f64 = 0,
    semaphore_multi_ns: f64 = 0,
    notify_ns: f64 = 0,
    channel_batch_ns: f64 = 0,
    channel_roundtrip_ns: f64 = 0,
    oneshot_ns: f64 = 0,
    oncecell_get_ns: f64 = 0,
    oncecell_race_ns: f64 = 0,
    blocking_submit_ns: f64 = 0,
    blocking_roundtrip_ns: f64 = 0,
    blocking_contended_ns: f64 = 0,
    ops_per_iteration: u64 = OPS_PER_ITERATION,
    blocking_ops: u64 = BLOCKING_OPS,
    iterations: u64 = ITERATIONS,

    pub fn print_json(self: JsonOutput) void {
        std.debug.print(
            \\{{
            \\  "mutex_trylock_ns": {d:.2},
            \\  "mutex_contended_ns": {d:.2},
            \\  "rwlock_read_ns": {d:.2},
            \\  "rwlock_write_ns": {d:.2},
            \\  "rwlock_mixed_ns": {d:.2},
            \\  "semaphore_ns": {d:.2},
            \\  "semaphore_multi_ns": {d:.2},
            \\  "notify_ns": {d:.2},
            \\  "channel_batch_ns": {d:.2},
            \\  "channel_roundtrip_ns": {d:.2},
            \\  "oneshot_ns": {d:.2},
            \\  "oncecell_get_ns": {d:.2},
            \\  "oncecell_race_ns": {d:.2},
            \\  "blocking_submit_ns": {d:.2},
            \\  "blocking_roundtrip_ns": {d:.2},
            \\  "blocking_contended_ns": {d:.2},
            \\  "ops_per_iteration": {d},
            \\  "blocking_ops": {d},
            \\  "iterations": {d}
            \\}}
            \\
        , .{
            self.mutex_trylock_ns,
            self.mutex_contended_ns,
            self.rwlock_read_ns,
            self.rwlock_write_ns,
            self.rwlock_mixed_ns,
            self.semaphore_ns,
            self.semaphore_multi_ns,
            self.notify_ns,
            self.channel_batch_ns,
            self.channel_roundtrip_ns,
            self.oneshot_ns,
            self.oncecell_get_ns,
            self.oncecell_race_ns,
            self.blocking_submit_ns,
            self.blocking_roundtrip_ns,
            self.blocking_contended_ns,
            self.ops_per_iteration,
            self.blocking_ops,
            self.iterations,
        });
    }
};

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var json_mode = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--json")) {
            json_mode = true;
        }
    }

    if (json_mode) {
        var output = JsonOutput{};

        output.mutex_trylock_ns = runBench("mutex_trylock", benchMutexTryLock).nsPerOp(OPS_PER_ITERATION);
        output.mutex_contended_ns = runBench("mutex_contended", benchMutexContended).nsPerOp(OPS_PER_ITERATION);
        output.rwlock_read_ns = runBench("rwlock_read", benchRwLockRead).nsPerOp(OPS_PER_ITERATION);
        output.rwlock_write_ns = runBench("rwlock_write", benchRwLockWrite).nsPerOp(OPS_PER_ITERATION);
        output.rwlock_mixed_ns = runBench("rwlock_mixed", benchRwLockMixed).nsPerOp(OPS_PER_ITERATION);
        output.semaphore_ns = runBench("semaphore", benchSemaphoreAcquireRelease).nsPerOp(OPS_PER_ITERATION);
        output.semaphore_multi_ns = runBench("semaphore_multi", benchSemaphoreMultiPermit).nsPerOp(OPS_PER_ITERATION);
        output.notify_ns = runBench("notify", benchNotifyOne).nsPerOp(OPS_PER_ITERATION);

        // Channel benchmarks need allocator - run inline
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchChannelSendRecv(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchChannelSendRecv(allocator));
            }
            output.channel_batch_ns = stats.nsPerOp(OPS_PER_ITERATION);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchChannelRoundtrip(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchChannelRoundtrip(allocator));
            }
            output.channel_roundtrip_ns = stats.nsPerOp(OPS_PER_ITERATION);
        }

        output.oneshot_ns = runBench("oneshot", benchOneshot).nsPerOp(OPS_PER_ITERATION);
        output.oncecell_get_ns = runBench("oncecell_get", benchOnceCellGet).nsPerOp(OPS_PER_ITERATION);
        output.oncecell_race_ns = runBench("oncecell_race", benchOnceCellInitRace).nsPerOp(OPS_PER_ITERATION);

        // Blocking pool benchmarks
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolSubmit(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolSubmit(allocator));
            }
            output.blocking_submit_ns = stats.nsPerOp(BLOCKING_OPS);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolRoundtrip(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolRoundtrip(allocator));
            }
            output.blocking_roundtrip_ns = stats.nsPerOp(BLOCKING_OPS);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolContended(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolContended(allocator));
            }
            output.blocking_contended_ns = stats.nsPerOp(BLOCKING_OPS);
        }

        output.print_json();
    } else {
        print("\n", .{});
        print("=" ** 70 ++ "\n", .{});
        print("           Volt Internal Primitives Benchmark\n", .{});
        print("=" ** 70 ++ "\n", .{});
        print("Iterations: {d} (+ {d} warmup)   Ops/iter: {d}   Stats: median\n", .{ ITERATIONS, WARMUP_ITERATIONS, OPS_PER_ITERATION });
        print("Platform: {s}\n\n", .{@tagName(@import("builtin").cpu.arch)});

        // Mutex
        print("MUTEX\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("tryLock/unlock", runBench("mutex_trylock", benchMutexTryLock), OPS_PER_ITERATION);
        printResult("contended (4 threads)", runBench("mutex_contended", benchMutexContended), OPS_PER_ITERATION);
        print("\n", .{});

        // RwLock
        print("RWLOCK\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("read lock/unlock", runBench("rwlock_read", benchRwLockRead), OPS_PER_ITERATION);
        printResult("write lock/unlock", runBench("rwlock_write", benchRwLockWrite), OPS_PER_ITERATION);
        printResult("mixed (90% read)", runBench("rwlock_mixed", benchRwLockMixed), OPS_PER_ITERATION);
        print("\n", .{});

        // Semaphore
        print("SEMAPHORE\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("acquire/release", runBench("semaphore", benchSemaphoreAcquireRelease), OPS_PER_ITERATION);
        printResult("multi-permit (5)", runBench("semaphore_multi", benchSemaphoreMultiPermit), OPS_PER_ITERATION);
        print("\n", .{});

        // Notify
        print("NOTIFY\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("notify/wait cycle", runBench("notify", benchNotifyOne), OPS_PER_ITERATION);
        print("\n", .{});

        // Channel
        print("CHANNEL\n", .{});
        print("-" ** 50 ++ "\n", .{});
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchChannelSendRecv(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchChannelSendRecv(allocator));
            }
            printResult("batch send/recv", stats, OPS_PER_ITERATION);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchChannelRoundtrip(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchChannelRoundtrip(allocator));
            }
            printResult("roundtrip", stats, OPS_PER_ITERATION);
        }
        print("\n", .{});

        // Oneshot
        print("ONESHOT\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("send/recv cycle", runBench("oneshot", benchOneshot), OPS_PER_ITERATION);
        print("\n", .{});

        // OnceCell
        print("ONCECELL\n", .{});
        print("-" ** 50 ++ "\n", .{});
        printResult("get (initialized)", runBench("oncecell_get", benchOnceCellGet), OPS_PER_ITERATION);
        printResult("init race (4 threads)", runBench("oncecell_race", benchOnceCellInitRace), OPS_PER_ITERATION);
        print("\n", .{});

        // Blocking Pool
        print("BLOCKING POOL (Tokio-style)\n", .{});
        print("-" ** 50 ++ "\n", .{});
        print("(Using {d} ops per iteration for blocking tests)\n", .{BLOCKING_OPS});
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolSubmit(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolSubmit(allocator));
            }
            printResult("submit (fire & forget)", stats, BLOCKING_OPS);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolRoundtrip(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolRoundtrip(allocator));
            }
            printResult("roundtrip (submit+wait)", stats, BLOCKING_OPS);
        }
        {
            var stats = Stats.init();
            for (0..WARMUP_ITERATIONS) |_| {
                _ = benchBlockingPoolContended(allocator);
            }
            for (0..ITERATIONS) |_| {
                stats.add(benchBlockingPoolContended(allocator));
            }
            printResult("contended (4 threads)", stats, BLOCKING_OPS);
        }
        print("\n", .{});

        print("=" ** 70 ++ "\n", .{});
        print("Benchmark complete.\n", .{});
        print("=" ** 70 ++ "\n\n", .{});
    }
}

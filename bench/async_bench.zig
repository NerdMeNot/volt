//! Blocking Pool Benchmark
//!
//! This benchmark tests the BLOCKING pool (io.concurrent):
//! - Runtime with blocking thread pool
//! - Blocking work offloading
//! - Measures throughput of blocking operations
//!
//! NOTE: This is NOT a true async benchmark. For async benchmarks using
//! Futures and proper yielding, see true_async_test.zig.
//!
//! Run with: zig build async-bench

const std = @import("std");
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;

// Combined test exports module
const volt = @import("volt");

// Runtime handle
const Io = volt.Io;
// BlockingHandle is the return type of io.concurrentBlocking()
const BlockingHandle = volt.internal.BlockingHandle;

// Sync primitives
const Mutex = volt.sync.Mutex;
const RwLock = volt.sync.RwLock;
const Semaphore = volt.sync.Semaphore;

const print = std.debug.print;

// ============================================================================
// Configuration
// ============================================================================

/// Number of measurement iterations
const ITERATIONS = 10;

/// Warmup iterations (not measured)
const WARMUP_ITERATIONS = 3;

/// Number of async tasks to spawn per measurement
const TASKS_PER_ITERATION = 50;

/// Operations per task (amortize task spawn overhead)
const OPS_PER_TASK = 100;

/// Total operations per iteration
const OPS_PER_ITERATION = TASKS_PER_ITERATION * OPS_PER_TASK;

/// Number of worker threads
const NUM_WORKERS = 4;

// ============================================================================
// Statistics
// ============================================================================

const Stats = struct {
    samples: [ITERATIONS]u64 = undefined,
    count: usize = 0,

    fn add(self: *Stats, ns: u64) void {
        if (self.count < ITERATIONS) {
            self.samples[self.count] = ns;
            self.count += 1;
        }
    }

    fn median(self: *const Stats) u64 {
        if (self.count == 0) return 0;
        var sorted: [ITERATIONS]u64 = undefined;
        @memcpy(sorted[0..self.count], self.samples[0..self.count]);
        std.mem.sort(u64, sorted[0..self.count], {}, std.sort.asc(u64));
        return sorted[self.count / 2];
    }

    fn min(self: *const Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = std.math.maxInt(u64);
        for (self.samples[0..self.count]) |s| {
            m = @min(m, s);
        }
        return m;
    }

    fn max(self: *const Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = 0;
        for (self.samples[0..self.count]) |s| {
            m = @max(m, s);
        }
        return m;
    }
};

const BenchResult = struct {
    stats: Stats,
    ops: usize,

    fn nsPerOp(self: BenchResult) f64 {
        const median_ns = self.stats.median();
        return @as(f64, @floatFromInt(median_ns)) / @as(f64, @floatFromInt(self.ops));
    }

    fn opsPerSec(self: BenchResult) f64 {
        const median_ns = self.stats.median();
        if (median_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.ops)) * 1_000_000_000.0 / @as(f64, @floatFromInt(median_ns));
    }

    fn minNsPerOp(self: BenchResult) f64 {
        return @as(f64, @floatFromInt(self.stats.min())) / @as(f64, @floatFromInt(self.ops));
    }

    fn maxNsPerOp(self: BenchResult) f64 {
        return @as(f64, @floatFromInt(self.stats.max())) / @as(f64, @floatFromInt(self.ops));
    }
};

// ============================================================================
// Mutex Async Benchmark
// ============================================================================

fn benchMutexAsync(allocator: Allocator) !BenchResult {
    var stats = Stats{};

    // Create runtime ONCE
    var io = try Runtime.init(allocator, .{ .num_workers = NUM_WORKERS });
    defer io.deinit();

    // Shared mutex for contention
    var mutex = Mutex.init();

    // Shared counter to verify correctness
    var counter = Atomic(u64).init(0);

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        try runMutexIteration(io, &mutex, &counter);
        counter.store(0, .seq_cst);
    }

    // Measured iterations
    for (0..ITERATIONS) |_| {
        counter.store(0, .seq_cst);

        const start = std.time.nanoTimestamp();
        try runMutexIteration(io, &mutex, &counter);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        stats.add(elapsed);

        // Verify correctness
        const final_count = counter.load(.seq_cst);
        if (final_count != TASKS_PER_ITERATION * OPS_PER_TASK) {
            print("WARNING: Counter mismatch! Expected {}, got {}\n", .{ TASKS_PER_ITERATION * OPS_PER_TASK, final_count });
        }
    }

    return .{ .stats = stats, .ops = OPS_PER_ITERATION };
}

fn runMutexIteration(io: Io, mutex: *Mutex, counter: *Atomic(u64)) !void {
    // Spawn blocking tasks
    var handles: [TASKS_PER_ITERATION]*BlockingHandle(void) = undefined;

    for (&handles) |*h| {
        h.* = try io.concurrentBlocking(mutexTask, .{ mutex, counter });
    }

    // Wait for all tasks
    for (handles) |h| {
        try h.wait();
    }
}

fn mutexTask(mutex: *Mutex, counter: *Atomic(u64)) void {
    for (0..OPS_PER_TASK) |_| {
        // Blocking lock - uses tryLock + yield
        while (!mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
        _ = counter.fetchAdd(1, .monotonic);
        mutex.unlock();
    }
}

// ============================================================================
// Semaphore Async Benchmark
// ============================================================================

fn benchSemaphoreAsync(allocator: Allocator) !BenchResult {
    var stats = Stats{};

    var io = try Runtime.init(allocator, .{ .num_workers = NUM_WORKERS });
    defer io.deinit();

    // Limited permits = high contention
    var semaphore = Semaphore.init(2);
    var counter = Atomic(u64).init(0);

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        try runSemaphoreIteration(io, &semaphore, &counter);
        counter.store(0, .seq_cst);
    }

    // Measured
    for (0..ITERATIONS) |_| {
        counter.store(0, .seq_cst);

        const start = std.time.nanoTimestamp();
        try runSemaphoreIteration(io, &semaphore, &counter);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        stats.add(elapsed);
    }

    return .{ .stats = stats, .ops = OPS_PER_ITERATION };
}

fn runSemaphoreIteration(io: Io, semaphore: *Semaphore, counter: *Atomic(u64)) !void {
    var handles: [TASKS_PER_ITERATION]*BlockingHandle(void) = undefined;

    for (&handles) |*h| {
        h.* = try io.concurrentBlocking(semaphoreTask, .{ semaphore, counter });
    }

    for (handles) |h| {
        try h.wait();
    }
}

fn semaphoreTask(semaphore: *Semaphore, counter: *Atomic(u64)) void {
    for (0..OPS_PER_TASK) |_| {
        while (!semaphore.tryAcquire(1)) {
            std.Thread.yield() catch {};
        }
        _ = counter.fetchAdd(1, .monotonic);
        semaphore.release(1);
    }
}

// ============================================================================
// RwLock Async Benchmark (Mixed Read/Write)
// ============================================================================

fn benchRwLockAsync(allocator: Allocator) !BenchResult {
    var stats = Stats{};

    var io = try Runtime.init(allocator, .{ .num_workers = NUM_WORKERS });
    defer io.deinit();

    var rwlock = RwLock.init();
    var read_counter = Atomic(u64).init(0);
    var write_counter = Atomic(u64).init(0);

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        try runRwLockIteration(io, &rwlock, &read_counter, &write_counter);
        read_counter.store(0, .seq_cst);
        write_counter.store(0, .seq_cst);
    }

    // Measured
    for (0..ITERATIONS) |_| {
        read_counter.store(0, .seq_cst);
        write_counter.store(0, .seq_cst);

        const start = std.time.nanoTimestamp();
        try runRwLockIteration(io, &rwlock, &read_counter, &write_counter);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        stats.add(elapsed);
    }

    return .{ .stats = stats, .ops = OPS_PER_ITERATION };
}

fn runRwLockIteration(io: Io, rwlock: *RwLock, read_counter: *Atomic(u64), write_counter: *Atomic(u64)) !void {
    var handles: [TASKS_PER_ITERATION]*BlockingHandle(void) = undefined;

    // 80% readers, 20% writers
    const num_readers = TASKS_PER_ITERATION * 8 / 10;

    for (handles[0..num_readers]) |*h| {
        h.* = try io.concurrentBlocking(readerTask, .{ rwlock, read_counter });
    }
    for (handles[num_readers..]) |*h| {
        h.* = try io.concurrentBlocking(writerTask, .{ rwlock, write_counter });
    }

    for (handles) |h| {
        try h.wait();
    }
}

fn readerTask(rwlock: *RwLock, counter: *Atomic(u64)) void {
    for (0..OPS_PER_TASK) |_| {
        while (!rwlock.tryReadLock()) {
            std.Thread.yield() catch {};
        }
        _ = counter.fetchAdd(1, .monotonic);
        rwlock.readUnlock();
    }
}

fn writerTask(rwlock: *RwLock, counter: *Atomic(u64)) void {
    for (0..OPS_PER_TASK) |_| {
        while (!rwlock.tryWriteLock()) {
            std.Thread.yield() catch {};
        }
        _ = counter.fetchAdd(1, .monotonic);
        rwlock.writeUnlock();
    }
}

// High contention benchmark removed - use simpler benchmarks first

// ============================================================================
// Output
// ============================================================================

fn printResult(name: []const u8, result: BenchResult) void {
    print("  {s:<30} {d:>8.1}ns/op  [{d:.1}-{d:.1}]  {d:>12.0} ops/sec\n", .{
        name,
        result.nsPerOp(),
        result.minNsPerOp(),
        result.maxNsPerOp(),
        result.opsPerSec(),
    });
}

fn printJsonEntry(name: []const u8, result: BenchResult, is_last: bool) void {
    print("    \"{s}\": {{\n", .{name});
    print("      \"ns_per_op\": {d:.2},\n", .{result.nsPerOp()});
    print("      \"ops_per_sec\": {d:.0},\n", .{result.opsPerSec()});
    print("      \"min_ns_per_op\": {d:.2},\n", .{result.minNsPerOp()});
    print("      \"max_ns_per_op\": {d:.2}\n", .{result.maxNsPerOp()});
    if (is_last) {
        print("    }}\n", .{});
    } else {
        print("    }},\n", .{});
    }
}

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

    print("\n", .{});
    print("=" ** 80 ++ "\n", .{});
    print("                    VOLT BLOCKING POOL BENCHMARK\n", .{});
    print("=" ** 80 ++ "\n", .{});
    print("\n", .{});
    print("This benchmark tests the BLOCKING pool (io.concurrent).\n", .{});
    print("Tasks run on dedicated blocking threads (not the async scheduler).\n", .{});
    print("\n", .{});
    print("Config:\n", .{});
    print("  Workers:            {d}\n", .{NUM_WORKERS});
    print("  Tasks/iteration:    {d}\n", .{TASKS_PER_ITERATION});
    print("  Ops/task:           {d}\n", .{OPS_PER_TASK});
    print("  Total ops/iter:     {d}\n", .{OPS_PER_ITERATION});
    print("  Iterations:         {d} (after {d} warmup)   Stats: median\n", .{ ITERATIONS, WARMUP_ITERATIONS });
    print("\n", .{});

    // Run benchmarks
    print("Running Mutex blocking benchmark...\n", .{});
    const mutex_result = try benchMutexAsync(allocator);

    print("Running Semaphore blocking benchmark...\n", .{});
    const semaphore_result = try benchSemaphoreAsync(allocator);

    print("Running RwLock blocking benchmark...\n", .{});
    const rwlock_result = try benchRwLockAsync(allocator);

    print("\n", .{});

    if (json_mode) {
        print("{{\n", .{});
        print("  \"metadata\": {{\n", .{});
        print("    \"num_workers\": {d},\n", .{NUM_WORKERS});
        print("    \"tasks_per_iteration\": {d},\n", .{TASKS_PER_ITERATION});
        print("    \"ops_per_task\": {d},\n", .{OPS_PER_TASK});
        print("    \"iterations\": {d},\n", .{ITERATIONS});
        print("    \"warmup_iterations\": {d}\n", .{WARMUP_ITERATIONS});
        print("  }},\n", .{});
        print("  \"benchmarks\": {{\n", .{});
        printJsonEntry("mutex_blocking", mutex_result, false);
        printJsonEntry("semaphore_blocking", semaphore_result, false);
        printJsonEntry("rwlock_blocking", rwlock_result, true);
        print("  }}\n", .{});
        print("}}\n", .{});
    } else {
        print("=" ** 80 ++ "\n", .{});
        print("                              RESULTS\n", .{});
        print("=" ** 80 ++ "\n", .{});
        print("\n", .{});
        print("BLOCKING POOL (io.concurrent)\n", .{});
        print("-" ** 80 ++ "\n", .{});
        printResult("Mutex (50 tasks)", mutex_result);
        printResult("Semaphore (2 permits)", semaphore_result);
        printResult("RwLock (80R/20W)", rwlock_result);
        print("\n", .{});

        print("=" ** 80 ++ "\n", .{});
        print("Note: These benchmarks test the blocking pool (io.concurrent).\n", .{});
        print("For true async benchmarks using Futures, see true_async_test.zig.\n", .{});
        print("=" ** 80 ++ "\n\n", .{});
    }
}

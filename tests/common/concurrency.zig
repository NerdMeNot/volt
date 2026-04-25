//! Concurrency Test Utilities
//!
//! Provides thread coordination primitives and helpers for testing
//! concurrent code. Inspired by Tokio's loom testing infrastructure.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const Atomic = std.atomic.Value;

/// Monotonic-clock timestamp in nanoseconds. Replaces std.time.nanoTimestamp
/// (removed in 0.16; time queries moved to std.Io which requires an Io handle).
fn nanoTimestamp() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

/// Thread-safe concurrent counter
pub const ConcurrentCounter = struct {
    value: Atomic(usize) = Atomic(usize).init(0),

    pub fn increment(self: *ConcurrentCounter) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    pub fn add(self: *ConcurrentCounter, n: usize) void {
        _ = self.value.fetchAdd(n, .monotonic);
    }

    pub fn decrement(self: *ConcurrentCounter) void {
        _ = self.value.fetchSub(1, .monotonic);
    }

    pub fn get(self: *const ConcurrentCounter) usize {
        return self.value.load(.acquire);
    }

    pub fn reset(self: *ConcurrentCounter) void {
        self.value.store(0, .release);
    }
};

/// Simple countdown latch for synchronization
pub const Latch = struct {
    count: Atomic(usize),
    event: ResetEvent = .{},

    pub fn init(count: usize) Latch {
        return .{
            .count = Atomic(usize).init(count),
        };
    }

    /// Decrement count; signal when reaching zero
    pub fn countDown(self: *Latch) void {
        const prev = self.count.fetchSub(1, .acq_rel);
        if (prev == 1) {
            self.event.set();
        }
    }

    /// Wait until count reaches zero
    pub fn wait(self: *Latch) void {
        while (self.count.load(.acquire) > 0) {
            self.event.wait();
        }
    }

    /// Wait with timeout
    pub fn timedWait(self: *Latch, timeout_ns: u64) bool {
        const deadline = nanoTimestamp() + @as(i128, timeout_ns);
        while (self.count.load(.acquire) > 0) {
            const now = nanoTimestamp();
            if (now >= deadline) return false;
            const remaining: u64 = @intCast(deadline - now);
            self.event.timedWait(remaining) catch {};
        }
        return true;
    }
};

// std.Thread.ResetEvent moved to std.Io.Event (requires Io) in Zig 0.16.
// `common` is a test-utility module without a volt dependency, so we roll
// a minimal portable replacement here. Mutex+Condition would be cleaner but
// also needs replacements; this atomic-only spin-then-yield variant is
// adequate for test infrastructure.
const ResetEvent = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn set(self: *ResetEvent) void {
        self.state.store(1, .release);
    }

    pub fn wait(self: *ResetEvent) void {
        while (self.state.load(.acquire) == 0) {
            std.Thread.yield() catch {};
        }
    }

    pub fn timedWait(self: *ResetEvent, timeout_ns: u64) error{Timeout}!void {
        var elapsed: u64 = 0;
        const slice_ns: u64 = 1_000_000; // 1ms slice
        while (self.state.load(.acquire) == 0) {
            if (elapsed >= timeout_ns) return error.Timeout;
            std.Thread.yield() catch {};
            elapsed += slice_ns;
        }
    }

    pub fn reset(self: *ResetEvent) void {
        self.state.store(0, .monotonic);
    }
};

/// Barrier for thread synchronization
pub const Barrier = struct {
    count: Atomic(usize),
    generation: Atomic(usize) = Atomic(usize).init(0),
    total: usize,
    event: ResetEvent = .{},

    pub fn init(count: usize) Barrier {
        return .{
            .count = Atomic(usize).init(count),
            .total = count,
        };
    }

    /// Wait at the barrier; returns true for the leader (last to arrive)
    pub fn wait(self: *Barrier) bool {
        const gen = self.generation.load(.acquire);
        const remaining = self.count.fetchSub(1, .acq_rel);

        if (remaining == 1) {
            // Last to arrive - reset and wake all
            self.count.store(self.total, .release);
            _ = self.generation.fetchAdd(1, .release);
            self.event.set();
            return true;
        } else {
            // Wait for generation to change
            while (self.generation.load(.acquire) == gen) {
                self.event.wait();
            }
            return false;
        }
    }

    /// Reset the barrier for reuse
    pub fn reset(self: *Barrier) void {
        self.count.store(self.total, .release);
        _ = self.generation.fetchAdd(1, .release);
        self.event.reset();
    }
};

/// Thread-local random number generator
pub const ThreadRng = struct {
    state: u64,

    pub fn init(seed: u64) ThreadRng {
        // Mix in thread ID for uniqueness
        const tid = std.Thread.getCurrentId();
        return .{
            .state = seed ^ (@as(u64, @intCast(tid)) *% 0x9E3779B97F4A7C15),
        };
    }

    pub fn initFromTime() ThreadRng {
        const timestamp: u64 = @intCast(nanoTimestamp() & 0xFFFFFFFFFFFFFFFF);
        return init(timestamp);
    }

    /// Generate random u64
    pub fn next(self: *ThreadRng) u64 {
        // xorshift64*
        var x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        return x *% 0x2545F4914F6CDD1D;
    }

    /// Random value in range [0, max)
    pub fn range(self: *ThreadRng, max: usize) usize {
        if (max == 0) return 0;
        return @intCast(self.next() % max);
    }

    /// Random bool with given probability (0.0 to 1.0)
    pub fn chance(self: *ThreadRng, probability: f64) bool {
        const val: f64 = @as(f64, @floatFromInt(self.next())) / @as(f64, @floatFromInt(std.math.maxInt(u64)));
        return val < probability;
    }

    /// Random delay in nanoseconds
    pub fn delayNs(self: *ThreadRng, max_ns: u64) u64 {
        if (max_ns == 0) return 0;
        return self.next() % max_ns;
    }
};

/// Run a function concurrently from multiple threads
pub fn runConcurrent(
    comptime thread_count: usize,
    context: anytype,
    comptime func: fn (@TypeOf(context), usize) void,
) void {
    var threads: [thread_count]std.Thread = undefined;
    var spawned: usize = 0;

    // Spawn threads
    for (0..thread_count) |i| {
        threads[i] = std.Thread.spawn(.{}, struct {
            fn run(ctx: @TypeOf(context), thread_id: usize) void {
                func(ctx, thread_id);
            }
        }.run, .{ context, i }) catch continue;
        spawned += 1;
    }

    // Join all spawned threads
    for (threads[0..spawned]) |thread| {
        thread.join();
    }
}

/// Run concurrent test with dynamic thread count
pub fn runConcurrentDynamic(
    allocator: std.mem.Allocator,
    thread_count: usize,
    context: anytype,
    comptime func: fn (@TypeOf(context), usize) void,
) !void {
    var threads = try allocator.alloc(std.Thread, thread_count);
    defer allocator.free(threads);
    var spawned: usize = 0;

    // Spawn threads
    for (0..thread_count) |i| {
        threads[i] = std.Thread.spawn(.{}, struct {
            fn run(ctx: @TypeOf(context), thread_id: usize) void {
                func(ctx, thread_id);
            }
        }.run, .{ context, i }) catch continue;
        spawned += 1;
    }

    // Join all spawned threads
    for (threads[0..spawned]) |thread| {
        thread.join();
    }
}

/// Track contention statistics
pub const ContentionTracker = struct {
    attempts: Atomic(usize) = Atomic(usize).init(0),
    successes: Atomic(usize) = Atomic(usize).init(0),
    retries: Atomic(usize) = Atomic(usize).init(0),
    max_retries: Atomic(usize) = Atomic(usize).init(0),

    pub fn recordAttempt(self: *ContentionTracker) void {
        _ = self.attempts.fetchAdd(1, .monotonic);
    }

    pub fn recordSuccess(self: *ContentionTracker, retry_count: usize) void {
        _ = self.successes.fetchAdd(1, .monotonic);
        _ = self.retries.fetchAdd(retry_count, .monotonic);

        // Update max retries
        var current_max = self.max_retries.load(.monotonic);
        while (retry_count > current_max) {
            const result = self.max_retries.cmpxchgWeak(
                current_max,
                retry_count,
                .monotonic,
                .monotonic,
            );
            if (result) |v| {
                current_max = v;
            } else {
                break;
            }
        }
    }

    pub fn getStats(self: *const ContentionTracker) struct {
        attempts: usize,
        successes: usize,
        total_retries: usize,
        max_retries: usize,
        avg_retries: f64,
    } {
        const attempts = self.attempts.load(.acquire);
        const successes = self.successes.load(.acquire);
        const retries = self.retries.load(.acquire);
        const max = self.max_retries.load(.acquire);

        return .{
            .attempts = attempts,
            .successes = successes,
            .total_retries = retries,
            .max_retries = max,
            .avg_retries = if (successes > 0)
                @as(f64, @floatFromInt(retries)) / @as(f64, @floatFromInt(successes))
            else
                0.0,
        };
    }
};

/// Yield in a busy-wait loop with cross-platform reliability.
///
/// On Windows, `std.Thread.yield()` maps to `SwitchToThread()`, which only
/// yields to threads on the **same processor**. On multi-core systems, the
/// target thread may be running on a different core, so yield returns
/// immediately and the loop burns CPU without making progress.
/// A short sleep forces the OS scheduler to consider all threads.
pub fn busyWaitYield() void {
    if (builtin.os.tag == .windows) {
        std.Thread.sleep(1_000_000); // 1ms — triggers Windows scheduler rebalancing
    } else {
        std.Thread.yield() catch {};
    }
}

/// Yield control to other threads
pub fn yieldThread() void {
    std.Thread.yield() catch {
        // Fallback: brief sleep
        std.Thread.sleep(1);
    };
}

/// Spin for a short duration to create contention opportunities
pub fn spinBriefly(rng: *ThreadRng) void {
    const iterations = rng.range(100) + 1;
    var x: u64 = rng.state;
    for (0..iterations) |_| {
        x = x *% 0x5851F42D4C957F2D +% 1;
    }
    // Prevent optimization
    std.mem.doNotOptimizeAway(&x);
}

test "ConcurrentCounter basic" {
    var counter = ConcurrentCounter{};

    counter.increment();
    counter.increment();
    counter.add(3);

    try testing.expectEqual(@as(usize, 5), counter.get());

    counter.decrement();
    try testing.expectEqual(@as(usize, 4), counter.get());
}

test "Latch countdown" {
    var latch = Latch.init(3);

    latch.countDown();
    latch.countDown();
    latch.countDown();

    latch.wait();
    try testing.expectEqual(@as(usize, 0), latch.count.load(.acquire));
}

test "Barrier synchronization" {
    var barrier = Barrier.init(2);

    var leader_count = std.atomic.Value(usize).init(0);

    const thread = std.Thread.spawn(.{}, struct {
        fn run(b: *Barrier, lc: *std.atomic.Value(usize)) void {
            if (b.wait()) {
                _ = lc.fetchAdd(1, .monotonic);
            }
        }
    }.run, .{ &barrier, &leader_count }) catch return;

    if (barrier.wait()) {
        _ = leader_count.fetchAdd(1, .monotonic);
    }

    thread.join();

    // Exactly one leader
    try testing.expectEqual(@as(usize, 1), leader_count.load(.acquire));
}

test "ThreadRng distribution" {
    var rng = ThreadRng.initFromTime();

    var counts = [_]usize{0} ** 10;
    for (0..1000) |_| {
        const bucket = rng.range(10);
        counts[bucket] += 1;
    }

    // Each bucket should have at least some hits (rough check)
    for (counts) |count| {
        try testing.expect(count > 50);
    }
}

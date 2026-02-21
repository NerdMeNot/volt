//! Volt Benchmark Suite — Matches Tokio reference (rust_bench)
//!
//! Methodology:
//!
//! TIER 1 — SYNC FAST PATH (1M ops):
//!   Single-thread, no runtime. Both sides use try_* synchronous APIs.
//!   Measures raw data structure cost: CAS, atomics, memory barriers.
//!
//! TIER 2 — CHANNEL FAST PATH (100K ops):
//!   Single-thread, no runtime. Both sides use trySend/tryRecv.
//!   Measures channel buffer operations without scheduling overhead.
//!
//! TIER 3 — ASYNC MULTI-TASK (10K ops):
//!   Both sides use their async runtimes (volt / Tokio).
//!   Measures real-world contention: scheduling, waking, backpressure.
//!   NO spin-wait loops — proper async futures throughout.
//!
//! Statistics: median of 10 iterations (5 warmup discarded).
//! ns/op = median_ns / ops_per_iter (single division, no double-counting).
//!
//! Run:     zig build bench
//! JSON:    zig build bench -- --json
//! Compare: zig build compare

const std = @import("std");
const Atomic = std.atomic.Value;
const Allocator = std.mem.Allocator;
const volt = @import("volt");

// Runtime (for async benchmarks)
const Io = volt.Io;

// Sync primitives
const Mutex = volt.sync.Mutex;
const RwLock = volt.sync.RwLock;
const Semaphore = volt.sync.Semaphore;
const Notify = volt.sync.Notify;
const Barrier = volt.sync.Barrier;
const OnceCell = volt.sync.OnceCell;

// Waiter types (for sync Notify/Barrier — Zig has sync API unlike Tokio)
const NotifyWaiter = volt.sync.notify.Waiter;
const BarrierWaiter = volt.sync.barrier.Waiter;

// Future infrastructure (for async tier)
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// Channels
const Channel = volt.channel.Channel;
const SendFuture = volt.channel.channel_mod.SendFuture;
const RecvFuture = volt.channel.channel_mod.RecvFuture;
const Oneshot = volt.channel.Oneshot;
const BroadcastChannel = volt.channel.BroadcastChannel;
const Watch = volt.channel.Watch;

fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const output = std.fmt.bufPrint(&buf, fmt, args) catch return;
    std.fs.File.stdout().writeAll(output) catch return;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Allocation Tracking
// ═══════════════════════════════════════════════════════════════════════════════

const Alignment = std.mem.Alignment;

/// Wraps any allocator and tracks total bytes allocated + allocation count.
/// Thread-safe (uses atomics) for Tier 3 async benchmarks.
const CountingAllocator = struct {
    inner: Allocator,
    bytes_allocated: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    alloc_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn init(inner: Allocator) CountingAllocator {
        return .{ .inner = inner };
    }

    fn reset(self: *CountingAllocator) void {
        self.bytes_allocated.store(0, .monotonic);
        self.alloc_count.store(0, .monotonic);
    }

    fn getAllocBytes(self: *CountingAllocator) usize {
        return self.bytes_allocated.load(.monotonic);
    }

    fn getAllocCount(self: *CountingAllocator) usize {
        return self.alloc_count.load(.monotonic);
    }

    fn allocator(self: *CountingAllocator) Allocator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(ctx: *anyopaque, len: usize, alignment: Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.inner.vtable.alloc(self.inner.ptr, len, alignment, ret_addr);
        if (result != null) {
            _ = self.bytes_allocated.fetchAdd(len, .monotonic);
            _ = self.alloc_count.fetchAdd(1, .monotonic);
        }
        return result;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.inner.vtable.resize(self.inner.ptr, memory, alignment, new_len, ret_addr);
        if (result and new_len > memory.len) {
            _ = self.bytes_allocated.fetchAdd(new_len - memory.len, .monotonic);
        }
        return result;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const result = self.inner.vtable.remap(self.inner.ptr, memory, alignment, new_len, ret_addr);
        if (result != null and new_len > memory.len) {
            _ = self.bytes_allocated.fetchAdd(new_len - memory.len, .monotonic);
        }
        return result;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.inner.vtable.free(self.inner.ptr, memory, alignment, ret_addr);
    }
};

/// File-scope allocation tracker — initialized in main()
var alloc_counter: CountingAllocator = undefined;

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration — MUST MATCH rust_bench/src/main.rs EXACTLY
// ═══════════════════════════════════════════════════════════════════════════════

// Tier 1: Sync fast path (very fast ops, need high count for measurable time)
const SYNC_OPS: usize = 1_000_000;

// Tier 2: Channel fast path (moderate cost, buffer operations)
const CHANNEL_OPS: usize = 100_000;

// Tier 3: Async multi-task (expensive, involves scheduling + contention)
const ASYNC_OPS: usize = 10_000;

// Shared config
const ITERATIONS: usize = 10;
const WARMUP: usize = 5;
const NUM_WORKERS: usize = 4;

// MPMC config
const MPMC_PRODUCERS: usize = 4;
const MPMC_CONSUMERS: usize = 4;
const MPMC_BUFFER: usize = 1_024; // Power-of-2 for optimal bitmask indexing

// Contended config
const CONTENDED_MUTEX_TASKS: usize = 4;
const CONTENDED_RWLOCK_READERS: usize = 4;
const CONTENDED_RWLOCK_WRITERS: usize = 2;
const CONTENDED_SEM_TASKS: usize = 8;
const CONTENDED_SEM_PERMITS: usize = 2;

// Batch config (for task_spawn_batch)
const BATCH_SIZE: usize = 100;

// ═══════════════════════════════════════════════════════════════════════════════
// Statistics — median-based (robust against outliers)
// ═══════════════════════════════════════════════════════════════════════════════

const Stats = struct {
    samples: [64]u64 = [_]u64{0} ** 64,
    count: usize = 0,

    fn add(self: *Stats, ns: u64) void {
        if (self.count < 64) {
            self.samples[self.count] = ns;
            self.count += 1;
        }
    }

    fn medianNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var sorted: [64]u64 = self.samples;
        std.mem.sort(u64, sorted[0..self.count], {}, std.sort.asc(u64));
        return sorted[self.count / 2];
    }

    fn minNs(self: Stats) u64 {
        if (self.count == 0) return 0;
        var m: u64 = std.math.maxInt(u64);
        for (self.samples[0..self.count]) |s| m = @min(m, s);
        return m;
    }
};

const BenchResult = struct {
    stats: Stats = .{},
    ops_per_iter: usize = 0, // ops done in ONE iteration — ns/op = median_ns / this
    total_bytes: usize = 0, // bytes allocated in last timed iteration
    total_allocs: usize = 0, // allocation calls in last timed iteration

    fn nsPerOp(self: BenchResult) f64 {
        const med: f64 = @floatFromInt(self.stats.medianNs());
        const ops: f64 = @floatFromInt(self.ops_per_iter);
        return if (ops > 0) med / ops else 0;
    }

    fn opsPerSec(self: BenchResult) f64 {
        const ns_per_op = self.nsPerOp();
        return if (ns_per_op > 0) 1_000_000_000.0 / ns_per_op else 0;
    }

    fn bytesPerOp(self: BenchResult) f64 {
        if (self.ops_per_iter == 0) return 0;
        return @as(f64, @floatFromInt(self.total_bytes)) / @as(f64, @floatFromInt(self.ops_per_iter));
    }

    fn allocsPerOp(self: BenchResult) f64 {
        if (self.ops_per_iter == 0) return 0;
        return @as(f64, @floatFromInt(self.total_allocs)) / @as(f64, @floatFromInt(self.ops_per_iter));
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// TIER 1: SYNC FAST PATH (1M ops, single-thread, try_* APIs)
// ═══════════════════════════════════════════════════════════════════════════════

fn benchMutex() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var mutex = Mutex.init();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(mutex.tryLock());
            mutex.unlock();
        }
    }

    for (0..ITERATIONS) |_| {
        var mutex = Mutex.init();
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(mutex.tryLock());
            mutex.unlock();
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

fn benchRwLockRead() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var rwlock = RwLock.init();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(rwlock.tryReadLock());
            rwlock.readUnlock();
        }
    }

    for (0..ITERATIONS) |_| {
        var rwlock = RwLock.init();
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(rwlock.tryReadLock());
            rwlock.readUnlock();
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

fn benchRwLockWrite() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var rwlock = RwLock.init();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(rwlock.tryWriteLock());
            rwlock.writeUnlock();
        }
    }

    for (0..ITERATIONS) |_| {
        var rwlock = RwLock.init();
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(rwlock.tryWriteLock());
            rwlock.writeUnlock();
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

fn benchSemaphore() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var sem = Semaphore.init(10);
        for (0..SYNC_OPS) |_| {
            std.debug.assert(sem.tryAcquire(1));
            sem.release(1);
        }
    }

    for (0..ITERATIONS) |_| {
        var sem = Semaphore.init(10);
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            std.debug.assert(sem.tryAcquire(1));
            sem.release(1);
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

fn benchOnceCellGet() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var cell = OnceCell(u64).initWith(42);
        for (0..SYNC_OPS) |_| std.mem.doNotOptimizeAway(cell.get());
    }

    for (0..ITERATIONS) |_| {
        var cell = OnceCell(u64).initWith(42);
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| std.mem.doNotOptimizeAway(cell.get());
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

fn benchOnceCellSet() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..SYNC_OPS) |i| {
            var cell = OnceCell(u64).init();
            std.mem.doNotOptimizeAway(cell.set(@intCast(i)));
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |i| {
            var cell = OnceCell(u64).init();
            std.mem.doNotOptimizeAway(cell.set(@intCast(i)));
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

// Notify: Zig has sync waitWith API (Tokio requires .notified().await).
// Both measure: create + signal + consume permit. Equivalent work.
fn benchNotify() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..SYNC_OPS) |_| {
            var notify = Notify.init();
            notify.notifyOne();
            var waiter = NotifyWaiter.init();
            std.mem.doNotOptimizeAway(notify.waitWith(&waiter));
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            var notify = Notify.init();
            notify.notifyOne();
            var waiter = NotifyWaiter.init();
            std.mem.doNotOptimizeAway(notify.waitWith(&waiter));
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

// Barrier: Zig has sync waitWith API (Tokio requires .wait().await).
// Both measure: create barrier(1) + wait (completes immediately).
fn benchBarrier() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..SYNC_OPS) |_| {
            var barrier = Barrier.init(1);
            var waiter = BarrierWaiter.init();
            std.mem.doNotOptimizeAway(barrier.waitWith(&waiter));
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..SYNC_OPS) |_| {
            var barrier = Barrier.init(1);
            var waiter = BarrierWaiter.init();
            std.mem.doNotOptimizeAway(barrier.waitWith(&waiter));
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = SYNC_OPS };
}

// ═══════════════════════════════════════════════════════════════════════════════
// TIER 2: CHANNEL FAST PATH (100K ops, single-thread, trySend/tryRecv)
// ═══════════════════════════════════════════════════════════════════════════════

fn benchChannelSend() BenchResult {
    var stats = Stats{};
    const allocator = alloc_counter.allocator();

    for (0..WARMUP) |_| {
        var ch = Channel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer ch.deinit();
        for (0..CHANNEL_OPS) |i| _ = ch.trySend(@intCast(i));
        for (0..CHANNEL_OPS) |_| _ = ch.tryRecv();
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        var ch = Channel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer ch.deinit();
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |i| _ = ch.trySend(@intCast(i));
        stats.add(@intCast(std.time.nanoTimestamp() - start));
        for (0..CHANNEL_OPS) |_| _ = ch.tryRecv(); // drain
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchChannelRecv() BenchResult {
    var stats = Stats{};
    const allocator = alloc_counter.allocator();

    for (0..WARMUP) |_| {
        var ch = Channel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer ch.deinit();
        for (0..CHANNEL_OPS) |i| _ = ch.trySend(@intCast(i));
        for (0..CHANNEL_OPS) |_| _ = ch.tryRecv();
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        var ch = Channel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer ch.deinit();
        for (0..CHANNEL_OPS) |i| _ = ch.trySend(@intCast(i)); // pre-fill
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |_| _ = ch.tryRecv();
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchChannelRoundtrip() BenchResult {
    var stats = Stats{};
    const allocator = alloc_counter.allocator();

    for (0..WARMUP) |_| {
        var ch = Channel(u64).init(allocator, 1) catch continue;
        defer ch.deinit();
        for (0..CHANNEL_OPS) |i| {
            _ = ch.trySend(@intCast(i));
            _ = ch.tryRecv();
        }
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        var ch = Channel(u64).init(allocator, 1) catch continue;
        defer ch.deinit();
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |i| {
            _ = ch.trySend(@intCast(i));
            _ = ch.tryRecv();
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchOneshot() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..CHANNEL_OPS) |i| {
            var shared = Oneshot(u64).Shared.init();
            var pair = Oneshot(u64).fromShared(&shared);
            _ = pair.sender.send(@intCast(i));
            std.mem.doNotOptimizeAway(pair.receiver.tryRecv());
        }
    }

    for (0..ITERATIONS) |_| {
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |i| {
            var shared = Oneshot(u64).Shared.init();
            var pair = Oneshot(u64).fromShared(&shared);
            _ = pair.sender.send(@intCast(i));
            std.mem.doNotOptimizeAway(pair.receiver.tryRecv());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS };
}

fn benchBroadcast() BenchResult {
    var stats = Stats{};
    const num_receivers: usize = 4;
    const allocator = alloc_counter.allocator();

    for (0..WARMUP) |_| {
        var bc = BroadcastChannel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer bc.deinit();
        var receivers: [4]BroadcastChannel(u64).Receiver = undefined;
        for (0..num_receivers) |i| receivers[i] = bc.subscribe();
        for (0..CHANNEL_OPS) |i| _ = bc.send(@intCast(i));
        for (0..num_receivers) |r| {
            for (0..CHANNEL_OPS) |_| _ = receivers[r].tryRecv();
        }
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        var bc = BroadcastChannel(u64).init(allocator, CHANNEL_OPS) catch continue;
        defer bc.deinit();
        var receivers: [4]BroadcastChannel(u64).Receiver = undefined;
        for (0..num_receivers) |i| receivers[i] = bc.subscribe();
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |i| _ = bc.send(@intCast(i));
        for (0..num_receivers) |r| {
            for (0..CHANNEL_OPS) |_| _ = receivers[r].tryRecv();
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchWatch() BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        var watch = Watch(u64).init(0);
        defer watch.deinit();
        for (0..CHANNEL_OPS) |i| {
            watch.send(@intCast(i));
            std.mem.doNotOptimizeAway(watch.borrow());
        }
    }

    for (0..ITERATIONS) |_| {
        var watch = Watch(u64).init(0);
        defer watch.deinit();
        const start = std.time.nanoTimestamp();
        for (0..CHANNEL_OPS) |i| {
            watch.send(@intCast(i));
            std.mem.doNotOptimizeAway(watch.borrow());
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = CHANNEL_OPS };
}

// ═══════════════════════════════════════════════════════════════════════════════
// TIER 3: ASYNC MULTI-TASK (10K ops, runtime required, NO spin-wait)
//
// Custom multi-poll Futures (Output + poll) are spawned via io.awaitFuture()
// and awaited with volt.Future.@"await"(io) — the public async API.
// ═══════════════════════════════════════════════════════════════════════════════

/// MPMC producer: sends msgs_count values to channel.
/// Equivalent to Rust: for i in 0..n { tx.send(i).await.unwrap(); }
///
/// Calls trySend() directly (matching Rust's zero-cost async/await).
/// Falls back to SendFuture with waiter when queue is full.
/// Yields after COOP_BUDGET sends to match Tokio's cooperative scheduling
/// (each .await in Rust is a yield point; Tokio's budget is 128 polls/task).
fn MpmcSendFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;

        /// Cooperative budget: yield after this many sends to let
        /// consumers run. Matches Tokio's 128-poll cooperative budget.
        const COOP_BUDGET: usize = 128;

        channel: *Channel(T),
        msgs_remaining: usize,
        next_value: T,
        /// Only created when queue is full
        send_future: ?SendFuture(T) = null,

        pub fn init(channel: *Channel(T), msgs_count: usize, start_value: T) Self {
            return .{ .channel = channel, .msgs_remaining = msgs_count, .next_value = start_value };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            // Resume pending send future if we were waiting on a full queue
            if (self.send_future != null) {
                switch (self.send_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => {
                        self.send_future = null;
                        self.next_value += 1;
                        self.msgs_remaining -= 1;
                    },
                }
            }

            // Hot path: direct trySend loop with cooperative yielding
            var budget: usize = COOP_BUDGET;
            while (self.msgs_remaining > 0) {
                if (budget == 0) {
                    ctx.getWaker().wakeByRef();
                    return .pending;
                }
                switch (self.channel.trySend(self.next_value)) {
                    .ok => {
                        self.next_value += 1;
                        self.msgs_remaining -= 1;
                        budget -= 1;
                    },
                    .closed => return .{ .ready = {} },
                    .full => {
                        // Cold path: queue full, use SendFuture with waiter
                        self.send_future = self.channel.sendFuture(self.next_value);
                        switch (self.send_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => {
                                self.send_future = null;
                                self.next_value += 1;
                                self.msgs_remaining -= 1;
                            },
                        }
                    },
                }
            }
            return .{ .ready = {} };
        }
    };
}

/// MPMC consumer: receives until channel closed.
/// Equivalent to Rust: while rx.recv().await.is_ok() {}
///
/// Same optimization: tryRecv() in tight loop, RecvFuture when empty.
/// Cooperative yielding after COOP_BUDGET receives.
fn MpmcRecvFuture(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = void;

        const COOP_BUDGET: usize = 128;

        channel: *Channel(T),
        /// Only created when queue is empty
        recv_future: ?RecvFuture(T) = null,

        pub fn init(channel: *Channel(T)) Self {
            return .{ .channel = channel };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
            // Resume pending recv future if we were waiting on an empty queue
            if (self.recv_future != null) {
                switch (self.recv_future.?.poll(ctx)) {
                    .pending => return .pending,
                    .ready => |val| {
                        self.recv_future = null;
                        if (val == null) return .{ .ready = {} };
                    },
                }
            }

            // Hot path: direct tryRecv loop with cooperative yielding
            var budget: usize = COOP_BUDGET;
            while (true) {
                if (budget == 0) {
                    ctx.getWaker().wakeByRef();
                    return .pending;
                }
                switch (self.channel.tryRecv()) {
                    .value => {
                        budget -= 1;
                    },
                    .closed => return .{ .ready = {} },
                    .empty => {
                        // Cold path: queue empty, use RecvFuture with waiter
                        self.recv_future = self.channel.recvFuture();
                        switch (self.recv_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => |val| {
                                self.recv_future = null;
                                if (val == null) return .{ .ready = {} };
                            },
                        }
                    },
                }
            }
        }
    };
}

/// Multi-op mutex future for contended benchmarks.
/// Equivalent to Tokio: for _ in 0..ops { let _g = mutex.lock().await; counter += 1; }
const MultiMutexFuture = struct {
    mutex: *Mutex,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.mutex.LockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(mutex: *Mutex, counter: *Atomic(usize), ops: usize) MultiMutexFuture {
        return .{ .mutex = mutex, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiMutexFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.mutex.lockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.mutex.unlock();
                            self.ops_remaining -= 1;
                            self.state = .start;
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

/// Multi-op RwLock read future.
const MultiRwLockReadFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.rwlock.ReadLockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(rwlock: *RwLock, counter: *Atomic(usize), ops: usize) MultiRwLockReadFuture {
        return .{ .rwlock = rwlock, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiRwLockReadFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.rwlock.readLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.readUnlock();
                            self.ops_remaining -= 1;
                            self.state = .start;
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

/// Multi-op RwLock write future.
const MultiRwLockWriteFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.rwlock.WriteLockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(rwlock: *RwLock, counter: *Atomic(usize), ops: usize) MultiRwLockWriteFuture {
        return .{ .rwlock = rwlock, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiRwLockWriteFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.rwlock.writeLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.writeUnlock();
                            self.ops_remaining -= 1;
                            self.state = .start;
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

/// Multi-op semaphore future.
const MultiSemaphoreFuture = struct {
    semaphore: *Semaphore,
    counter: *Atomic(usize),
    ops_remaining: usize,
    acquire_future: ?volt.sync.semaphore.AcquireFuture = null,
    state: State = .start,

    const State = enum { start, acquiring, done };
    pub const Output = void;

    pub fn init(semaphore: *Semaphore, counter: *Atomic(usize), ops: usize) MultiSemaphoreFuture {
        return .{ .semaphore = semaphore, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiSemaphoreFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    // Fast path: try lock-free acquire before creating AcquireFuture.
                    // In steady state, permits are often immediately available after
                    // the previous task's release — skip future/waker overhead.
                    if (self.semaphore.tryAcquire(1)) {
                        _ = self.counter.fetchAdd(1, .monotonic);
                        self.semaphore.release(1);
                        self.ops_remaining -= 1;
                        continue; // stay in .start
                    }
                    self.acquire_future = self.semaphore.acquireFuture(1);
                    self.state = .acquiring;
                },
                .acquiring => {
                    switch (self.acquire_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.semaphore.release(1);
                            self.ops_remaining -= 1;
                            self.state = .start;
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

fn benchChannelMPMC() BenchResult {
    var stats = Stats{};
    const msgs_per_producer = ASYNC_OPS / MPMC_PRODUCERS;
    const allocator = alloc_counter.allocator();

    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    for (0..WARMUP) |_| {
        runMPMCAsync(allocator, io, msgs_per_producer);
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        const start = std.time.nanoTimestamp();
        runMPMCAsync(allocator, io, msgs_per_producer);
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ASYNC_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn runMPMCAsync(allocator: Allocator, io: Io, msgs_per_producer: usize) void {
    var ch = Channel(u64).init(allocator, MPMC_BUFFER) catch return;

    // Spawn producers
    var prod_handles: [MPMC_PRODUCERS]volt.Future(void) = undefined;
    var prod_count: usize = 0;
    for (0..MPMC_PRODUCERS) |p| {
        const start_val: u64 = @intCast(p * msgs_per_producer);
        prod_handles[prod_count] = io.awaitFuture(
            MpmcSendFuture(u64).init(&ch, msgs_per_producer, start_val),
        ) catch continue;
        prod_count += 1;
    }

    // Spawn consumers
    var cons_handles: [MPMC_CONSUMERS]volt.Future(void) = undefined;
    var cons_count: usize = 0;
    for (0..MPMC_CONSUMERS) |_| {
        cons_handles[cons_count] = io.awaitFuture(
            MpmcRecvFuture(u64).init(&ch),
        ) catch continue;
        cons_count += 1;
    }

    // Wait for producers to finish sending
    for (prod_handles[0..prod_count]) |*h| _ = h.await(io);

    // Close channel so consumers see .closed and exit
    ch.close();

    // Wait for consumers
    for (cons_handles[0..cons_count]) |*h| _ = h.await(io);

    ch.deinit();
}

fn benchMutexContended() BenchResult {
    var stats = Stats{};
    const ops_per_task = ASYNC_OPS / CONTENDED_MUTEX_TASKS;
    const allocator = alloc_counter.allocator();

    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    var mutex = Mutex.init();
    var counter = Atomic(usize).init(0);

    for (0..WARMUP) |_| {
        counter.store(0, .seq_cst);
        var handles: [CONTENDED_MUTEX_TASKS]volt.Future(void) = undefined;
        for (0..CONTENDED_MUTEX_TASKS) |i| {
            handles[i] = io.awaitFuture(MultiMutexFuture.init(&mutex, &counter, ops_per_task)) catch continue;
        }
        for (&handles) |*h| _ = h.await(io);
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        counter.store(0, .seq_cst);
        const start = std.time.nanoTimestamp();

        var handles: [CONTENDED_MUTEX_TASKS]volt.Future(void) = undefined;
        for (0..CONTENDED_MUTEX_TASKS) |i| {
            handles[i] = io.awaitFuture(MultiMutexFuture.init(&mutex, &counter, ops_per_task)) catch continue;
        }
        for (&handles) |*h| _ = h.await(io);

        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ops_per_task * CONTENDED_MUTEX_TASKS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchRwLockContended() BenchResult {
    var stats = Stats{};
    const total_tasks = CONTENDED_RWLOCK_READERS + CONTENDED_RWLOCK_WRITERS;
    const ops_per_task = ASYNC_OPS / total_tasks;
    const allocator = alloc_counter.allocator();

    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    var rwlock = RwLock.init();
    var counter = Atomic(usize).init(0);

    for (0..WARMUP) |_| {
        counter.store(0, .seq_cst);
        var handles: [6]volt.Future(void) = undefined;
        var spawned: usize = 0;
        for (0..CONTENDED_RWLOCK_READERS) |_| {
            handles[spawned] = io.awaitFuture(MultiRwLockReadFuture.init(&rwlock, &counter, ops_per_task)) catch continue;
            spawned += 1;
        }
        for (0..CONTENDED_RWLOCK_WRITERS) |_| {
            handles[spawned] = io.awaitFuture(MultiRwLockWriteFuture.init(&rwlock, &counter, ops_per_task)) catch continue;
            spawned += 1;
        }
        for (handles[0..spawned]) |*h| _ = h.await(io);
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        counter.store(0, .seq_cst);
        const start = std.time.nanoTimestamp();

        var handles: [6]volt.Future(void) = undefined;
        var spawned: usize = 0;
        for (0..CONTENDED_RWLOCK_READERS) |_| {
            handles[spawned] = io.awaitFuture(MultiRwLockReadFuture.init(&rwlock, &counter, ops_per_task)) catch continue;
            spawned += 1;
        }
        for (0..CONTENDED_RWLOCK_WRITERS) |_| {
            handles[spawned] = io.awaitFuture(MultiRwLockWriteFuture.init(&rwlock, &counter, ops_per_task)) catch continue;
            spawned += 1;
        }
        for (handles[0..spawned]) |*h| _ = h.await(io);
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ops_per_task * total_tasks, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

fn benchSemaphoreContended() BenchResult {
    var stats = Stats{};
    const ops_per_task = ASYNC_OPS / CONTENDED_SEM_TASKS;
    const allocator = alloc_counter.allocator();

    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    var sem = Semaphore.init(CONTENDED_SEM_PERMITS);
    var counter = Atomic(usize).init(0);

    for (0..WARMUP) |_| {
        counter.store(0, .seq_cst);
        var handles: [CONTENDED_SEM_TASKS]volt.Future(void) = undefined;
        for (0..CONTENDED_SEM_TASKS) |i| {
            handles[i] = io.awaitFuture(MultiSemaphoreFuture.init(&sem, &counter, ops_per_task)) catch continue;
        }
        for (&handles) |*h| _ = h.await(io);
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        counter.store(0, .seq_cst);
        const start = std.time.nanoTimestamp();

        var handles: [CONTENDED_SEM_TASKS]volt.Future(void) = undefined;
        for (0..CONTENDED_SEM_TASKS) |i| {
            handles[i] = io.awaitFuture(MultiSemaphoreFuture.init(&sem, &counter, ops_per_task)) catch continue;
        }
        for (&handles) |*h| _ = h.await(io);

        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ops_per_task * CONTENDED_SEM_TASKS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

// ═══════════════════════════════════════════════════════════════════════════════
// TIER 4: TASK SCHEDULING (scheduler overhead measurement)
//
// Measures io.@"async" spawn+await and io.concurrent (blocking pool) overhead.
// Tokio equivalents: tokio::spawn + .await, tokio::task::spawn_blocking.
// ═══════════════════════════════════════════════════════════════════════════════

fn noopFn() void {}

/// Inner loop for serial spawn+await, runs on a worker thread.
/// Matches Tokio: async fn bench_task_spawn() called via rt.block_on().
fn benchTaskSpawnInner(io: Io) BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..ASYNC_OPS) |_| {
            var f = io.async(noopFn, .{}) catch continue;
            _ = f.await(io);
        }
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        const start = std.time.nanoTimestamp();
        for (0..ASYNC_OPS) |_| {
            var f = io.async(noopFn, .{}) catch continue;
            _ = f.await(io);
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ASYNC_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

/// Serial spawn+await: measures full round-trip through the scheduler.
/// Equivalent to Tokio: rt.block_on(async { for _ in 0..N { spawn(async {}).await } })
fn benchTaskSpawn() BenchResult {
    const allocator = alloc_counter.allocator();

    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    // Run on a worker thread (like Tokio's block_on)
    var f = io.async(benchTaskSpawnInner, .{io}) catch return .{};
    return f.await(io) catch return .{};
}

/// Inner loop for batch spawn, runs on a worker thread.
fn benchTaskSpawnBatchInner(io: Io) BenchResult {
    var stats = Stats{};
    const batches = ASYNC_OPS / BATCH_SIZE;

    for (0..WARMUP) |_| {
        for (0..batches) |_| {
            var futures: [BATCH_SIZE]volt.Future(void) = undefined;
            for (0..BATCH_SIZE) |i| {
                futures[i] = io.async(noopFn, .{}) catch continue;
            }
            for (&futures) |*f| _ = f.await(io);
        }
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        const start = std.time.nanoTimestamp();
        for (0..batches) |_| {
            var futures: [BATCH_SIZE]volt.Future(void) = undefined;
            for (0..BATCH_SIZE) |i| {
                futures[i] = io.async(noopFn, .{}) catch continue;
            }
            for (&futures) |*f| _ = f.await(io);
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ASYNC_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

/// Batch spawn: spawn BATCH_SIZE tasks, then await all. Measures scheduler throughput.
/// Equivalent to Tokio: rt.block_on(async { spawn 100 tasks, then join all })
fn benchTaskSpawnBatch() BenchResult {
    const allocator = alloc_counter.allocator();
    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    // Run on a worker thread (like Tokio's block_on)
    var f = io.async(benchTaskSpawnBatchInner, .{io}) catch return .{};
    return f.await(io) catch return .{};
}

/// Inner loop for blocking spawn, runs on a worker thread.
/// Uses async path: concurrent() returns IoFuture, waker callback via scheduler.
fn benchBlockingSpawnInner(io: Io) BenchResult {
    var stats = Stats{};

    for (0..WARMUP) |_| {
        for (0..ASYNC_OPS) |_| {
            var f = io.concurrent(noopFn, .{}) catch continue;
            _ = f.await(io) catch {};
        }
    }

    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        const start = std.time.nanoTimestamp();
        for (0..ASYNC_OPS) |_| {
            var f = io.concurrent(noopFn, .{}) catch continue;
            _ = f.await(io) catch {};
        }
        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{ .stats = stats, .ops_per_iter = ASYNC_OPS, .total_bytes = alloc_counter.getAllocBytes(), .total_allocs = alloc_counter.getAllocCount() };
}

/// Blocking pool: spawn+wait via io.concurrent (dedicated blocking thread).
/// Equivalent to Tokio: rt.block_on(async { spawn_blocking(|| {}).await })
fn benchBlockingSpawn() BenchResult {
    const allocator = alloc_counter.allocator();
    var io = Io.init(allocator, .{ .num_workers = NUM_WORKERS }) catch return .{};
    defer io.deinit();

    // Run on a worker thread (like Tokio's block_on)
    var f = io.async(benchBlockingSpawnInner, .{io}) catch return .{};
    return f.await(io) catch return .{};
}

// ═══════════════════════════════════════════════════════════════════════════════
// OUTPUT
// ═══════════════════════════════════════════════════════════════════════════════

fn printJsonEntry(name: []const u8, result: BenchResult, is_last: bool) void {
    print("    \"{s}\": {{\n", .{name});
    print("      \"ns_per_op\": {d:.2},\n", .{result.nsPerOp()});
    print("      \"ops_per_sec\": {d:.0},\n", .{result.opsPerSec()});
    print("      \"median_ns\": {d},\n", .{result.stats.medianNs()});
    print("      \"min_ns\": {d},\n", .{result.stats.minNs()});
    print("      \"ops_per_iter\": {d},\n", .{result.ops_per_iter});
    print("      \"bytes_per_op\": {d:.2},\n", .{result.bytesPerOp()});
    print("      \"allocs_per_op\": {d:.4}\n", .{result.allocsPerOp()});
    if (is_last) print("    }}\n", .{}) else print("    }},\n", .{});
}

fn printRow(name: []const u8, result: BenchResult) void {
    print("  {s:<36} {d:>8.1} ns/op  {d:>16.0} ops/sec\n", .{ name, result.nsPerOp(), result.opsPerSec() });
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    alloc_counter = CountingAllocator.init(gpa.allocator());
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var json_mode = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--json")) json_mode = true;
    }

    // ── Tier 1: Sync fast path ──
    std.debug.print("[bench] Starting Tier 1: Sync fast path...\n", .{});
    const mutex = benchMutex();
    std.debug.print("[bench] mutex done\n", .{});
    const rwlock_read = benchRwLockRead();
    std.debug.print("[bench] rwlock_read done\n", .{});
    const rwlock_write = benchRwLockWrite();
    std.debug.print("[bench] rwlock_write done\n", .{});
    const semaphore = benchSemaphore();
    std.debug.print("[bench] semaphore done\n", .{});
    const oncecell_get = benchOnceCellGet();
    std.debug.print("[bench] oncecell_get done\n", .{});
    const oncecell_set = benchOnceCellSet();
    std.debug.print("[bench] oncecell_set done\n", .{});
    const notify = benchNotify();
    std.debug.print("[bench] notify done\n", .{});
    const barrier = benchBarrier();
    std.debug.print("[bench] barrier done\n", .{});

    // ── Tier 2: Channel fast path ──
    std.debug.print("[bench] Starting Tier 2: Channels...\n", .{});
    const channel_send = benchChannelSend();
    std.debug.print("[bench] channel_send done\n", .{});
    const channel_recv = benchChannelRecv();
    std.debug.print("[bench] channel_recv done\n", .{});
    const channel_roundtrip = benchChannelRoundtrip();
    std.debug.print("[bench] channel_roundtrip done\n", .{});
    const oneshot = benchOneshot();
    std.debug.print("[bench] oneshot done\n", .{});
    const broadcast = benchBroadcast();
    std.debug.print("[bench] broadcast done\n", .{});
    const watch = benchWatch();
    std.debug.print("[bench] watch done\n", .{});

    // ── Tier 3: Async multi-task ──
    std.debug.print("[bench] Starting Tier 3: Async...\n", .{});
    const channel_mpmc = benchChannelMPMC();
    std.debug.print("[bench] channel_mpmc done\n", .{});
    const mutex_contended = benchMutexContended();
    std.debug.print("[bench] mutex_contended done\n", .{});
    const rwlock_contended = benchRwLockContended();
    std.debug.print("[bench] rwlock_contended done\n", .{});
    const semaphore_contended = benchSemaphoreContended();
    std.debug.print("[bench] semaphore_contended done\n", .{});

    // ── Tier 4: Task scheduling ──
    std.debug.print("[bench] Starting Tier 4: Task scheduling...\n", .{});
    const task_spawn = benchTaskSpawn();
    std.debug.print("[bench] task_spawn done\n", .{});
    const task_spawn_batch = benchTaskSpawnBatch();
    std.debug.print("[bench] task_spawn_batch done\n", .{});
    const blocking_spawn = benchBlockingSpawn();
    std.debug.print("[bench] blocking_spawn done\n", .{});

    if (json_mode) {
        print("{{\n", .{});
        print("  \"metadata\": {{\n", .{});
        print("    \"runtime\": \"volt\",\n", .{});
        print("    \"sync_ops\": {d},\n", .{SYNC_OPS});
        print("    \"channel_ops\": {d},\n", .{CHANNEL_OPS});
        print("    \"async_ops\": {d},\n", .{ASYNC_OPS});
        print("    \"iterations\": {d},\n", .{ITERATIONS});
        print("    \"warmup\": {d},\n", .{WARMUP});
        print("    \"workers\": {d},\n", .{NUM_WORKERS});
        print("    \"mpmc_buffer\": {d},\n", .{MPMC_BUFFER});
        print("    \"methodology\": \"sync: try_*, channels: trySend/tryRecv, async: volt.Future + io.awaitFuture\"\n", .{});
        print("  }},\n", .{});
        print("  \"benchmarks\": {{\n", .{});

        printJsonEntry("mutex", mutex, false);
        printJsonEntry("rwlock_read", rwlock_read, false);
        printJsonEntry("rwlock_write", rwlock_write, false);
        printJsonEntry("semaphore", semaphore, false);
        printJsonEntry("oncecell_get", oncecell_get, false);
        printJsonEntry("oncecell_set", oncecell_set, false);
        printJsonEntry("notify", notify, false);
        printJsonEntry("barrier", barrier, false);
        printJsonEntry("channel_send", channel_send, false);
        printJsonEntry("channel_recv", channel_recv, false);
        printJsonEntry("channel_roundtrip", channel_roundtrip, false);
        printJsonEntry("oneshot", oneshot, false);
        printJsonEntry("broadcast", broadcast, false);
        printJsonEntry("watch", watch, false);
        printJsonEntry("channel_mpmc", channel_mpmc, false);
        printJsonEntry("mutex_contended", mutex_contended, false);
        printJsonEntry("rwlock_contended", rwlock_contended, false);
        printJsonEntry("semaphore_contended", semaphore_contended, false);
        printJsonEntry("task_spawn", task_spawn, false);
        printJsonEntry("task_spawn_batch", task_spawn_batch, false);
        printJsonEntry("blocking_spawn", blocking_spawn, true);

        print("  }}\n", .{});
        print("}}\n", .{});
    } else {
        print("\n", .{});
        print("=" ** 74 ++ "\n", .{});
        print("  Volt Benchmark Suite\n", .{});
        print("=" ** 74 ++ "\n", .{});
        print("  Tiers: sync={d}  channels={d}  async={d}\n", .{ SYNC_OPS, CHANNEL_OPS, ASYNC_OPS });
        print("  Config: {d} iters, {d} warmup, {d} workers\n", .{ ITERATIONS, WARMUP, NUM_WORKERS });
        print("  Methodology: try_* (sync), trySend/tryRecv (channels), volt.Future (async)\n\n", .{});

        print("  TIER 1: SYNC FAST PATH ({d} ops, single-thread, try_*)\n", .{SYNC_OPS});
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("Mutex tryLock/unlock", mutex);
        printRow("RwLock tryReadLock/readUnlock", rwlock_read);
        printRow("RwLock tryWriteLock/writeUnlock", rwlock_write);
        printRow("Semaphore tryAcquire/release", semaphore);
        printRow("OnceCell get (initialized)", oncecell_get);
        printRow("OnceCell set (new each time)", oncecell_set);
        printRow("Notify signal+consume*", notify);
        printRow("Barrier wait (1 participant)*", barrier);
        print("  * Zig has sync waitWith API (no async overhead)\n\n", .{});

        print("  TIER 2: CHANNELS ({d} ops, single-thread, trySend/tryRecv)\n", .{CHANNEL_OPS});
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("Channel send (fill buffer)", channel_send);
        printRow("Channel recv (drain buffer)", channel_recv);
        printRow("Channel roundtrip (cap=1)", channel_roundtrip);
        printRow("Oneshot create+send+recv", oneshot);
        printRow("Broadcast 1tx+4rx", broadcast);
        printRow("Watch send+borrow", watch);
        print("\n", .{});

        print("  TIER 3: ASYNC ({d} ops, {d} workers, scheduler)\n", .{ ASYNC_OPS, NUM_WORKERS });
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("MPMC 4P+4C (buf=1000)", channel_mpmc);
        printRow("Mutex contended (4 tasks)", mutex_contended);
        printRow("RwLock contended (4R+2W)", rwlock_contended);
        printRow("Semaphore contended (8T, 2 permits)", semaphore_contended);
        print("\n", .{});

        print("  TIER 4: TASK SCHEDULING ({d} ops, {d} workers)\n", .{ ASYNC_OPS, NUM_WORKERS });
        print("  " ++ "-" ** 72 ++ "\n", .{});
        printRow("Spawn+await (noop)", task_spawn);
        printRow("Spawn batch (100) + await all", task_spawn_batch);
        printRow("Blocking spawn+wait (noop)", blocking_spawn);
        print("\n", .{});

        print("=" ** 74 ++ "\n", .{});
        print("  Use --json for machine-readable output.\n", .{});
        print("  Use 'zig build compare' for side-by-side with Tokio.\n", .{});
        print("=" ** 74 ++ "\n\n", .{});
    }
}

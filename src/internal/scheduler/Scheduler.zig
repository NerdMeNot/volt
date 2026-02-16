//! Scheduler - Production-Grade Work-Stealing Task Scheduler
//!
//! A multi-threaded scheduler with production-grade optimizations for
//! high-throughput async task execution.
//!
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │ WORKER ARCHITECTURE                                                     │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  Each Worker owns:                                                      │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │ - Local Queue (256 slots + LIFO slot)                           │   │
//! │  │ - Global Batch Buffer (32 pre-fetched tasks)                    │   │
//! │  │ - Parking State (futex-based sleep/wake)                        │   │
//! │  │ - Cooperative Budget (128 polls per tick)                       │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │ WORK FINDING PRIORITY                                                   │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  1. Pre-fetched Global Batch  -- Already holding tasks from global     │
//! │  2. Local Queue               -- Check LIFO slot, then main queue      │
//! │  3. Global Queue (periodic)   -- Every N ticks, batch-pull tasks       │
//! │  4. Work Stealing             -- Steal from other workers' queues      │
//! │  5. Global Queue (final)      -- One more check before parking         │
//! │  6. Park                      -- Sleep with timeout until woken        │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │ SCHEDULER OPTIMIZATIONS                                                 │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  1. Cooperative Budgeting (BUDGET_PER_TICK = 128)                       │
//! │     -- Prevents single task from hogging worker indefinitely           │
//! │                                                                         │
//! │  2. LIFO Poll Caps (MAX_LIFO_POLLS_PER_TICK = 3)                        │
//! │     -- Ensures LIFO slot doesn't starve main queue tasks               │
//! │                                                                         │
//! │  3. Batch Global Queue (up to 64 tasks per fetch)                      │
//! │     -- Reduces mutex contention by amortizing lock acquisition         │
//! │                                                                         │
//! │  4. Searcher Limiting (~50% of workers)                                 │
//! │     -- Prevents thundering herd when work appears                      │
//! │                                                                         │
//! │  5. Parked Worker Bitmap (64-bit, O(1) lookup via @ctz)                │
//! │     -- Efficiently find and wake exactly one parked worker             │
//! │                                                                         │
//! │  6. Adaptive Global Queue Interval                                      │
//! │     -- Check frequency scales with task duration (EWMA)                │
//! │                                                                         │
//! │  7. Lost Wakeup Prevention (transitioning_to_park flag)                │
//! │     -- Prevents race between LIFO push and parking                     │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

// Debug flag - set to true to trace scheduler operations
const debug_scheduler = false;

const header_mod = @import("Header.zig");
const Header = header_mod.Header;
const TaskQueue = header_mod.TaskQueue;
const GlobalTaskQueue = header_mod.GlobalTaskQueue;
const WorkStealQueue = header_mod.WorkStealQueue;
const Lifecycle = header_mod.Lifecycle;
const State = header_mod.State;

const timer_mod = @import("TimerWheel.zig");
const TimerWheel = timer_mod.TimerWheel;
const TimerEntry = timer_mod.TimerEntry;

// Top-level Runtime module for setting threadlocal context on workers
const runtime_mod = @import("../../runtime.zig");

// I/O Backend - centralized here so scheduler owns the I/O driver
const backend_mod = @import("../backend.zig");
pub const Backend = backend_mod.Backend;
pub const BackendType = backend_mod.BackendType;
pub const BackendConfig = backend_mod.Config;
pub const Completion = backend_mod.Completion;
pub const Operation = backend_mod.Operation;

// ═══════════════════════════════════════════════════════════════════════════════
// Cooperative Scheduling Constants
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum number of task polls per maintenance tick.
/// Prevents a single task from hogging the worker.
pub const BUDGET_PER_TICK: u32 = 128;

/// Maximum consecutive LIFO slot polls before forcing queue check.
/// Prevents LIFO slot from starving main queue tasks.
pub const MAX_LIFO_POLLS_PER_TICK: u8 = 3;

/// Maximum tasks to pull from global queue in one batch.
pub const MAX_GLOBAL_QUEUE_BATCH_SIZE: usize = 64;

/// Minimum batch size (to amortize lock cost).
pub const MIN_GLOBAL_QUEUE_BATCH_SIZE: usize = 4;

/// Default batch size for global queue.
pub const GLOBAL_QUEUE_BATCH_SIZE: usize = 32;

/// Maximum number of workers (for bitmap tracking).
pub const MAX_WORKERS: usize = 64;

// ═══════════════════════════════════════════════════════════════════════════════
// Adaptive Global Queue Interval (EWMA-based self-tuning)
// ═══════════════════════════════════════════════════════════════════════════════

/// Target latency for checking global queue (1ms).
/// We want to check global queue at least this often to prevent starvation.
const TARGET_GLOBAL_QUEUE_LATENCY_NS: u64 = 1_000_000;

/// EWMA smoothing factor (0.1 = 10% weight to new sample).
/// Lower values = more stable, higher values = more responsive.
const EWMA_ALPHA: f64 = 0.1;

/// Minimum global queue check interval (prevent constant checking).
const MIN_GLOBAL_QUEUE_INTERVAL: u32 = 8;

/// Maximum global queue check interval (prevent starvation).
const MAX_GLOBAL_QUEUE_INTERVAL: u32 = 255;

/// Default initial estimate for task poll time (50µs).
const DEFAULT_AVG_TASK_NS: u64 = 50_000;

/// Park timeout in nanoseconds (10ms).
const PARK_TIMEOUT_NS: u64 = 10 * std.time.ns_per_ms;

// ═══════════════════════════════════════════════════════════════════════════════
// FastRand - xorshift64+ PRNG
// ═══════════════════════════════════════════════════════════════════════════════

/// Fast per-worker PRNG for randomized victim selection in work stealing.
/// Uses xorshift64+ with shift triplet [17, 7, 16].
pub const FastRand = struct {
    one: u32,
    two: u32,

    pub fn init(seed: u64) FastRand {
        const s: u32 = @truncate(seed >> 32);
        var r: u32 = @truncate(seed);
        if (r == 0) r = 1;
        return .{ .one = s, .two = r };
    }

    /// xorshift64+ with shift triplet [17, 7, 16]
    pub fn fastrand(self: *FastRand) u32 {
        var s1 = self.one;
        const s0 = self.two;
        s1 ^= s1 << 17;
        s1 = s1 ^ s0 ^ (s1 >> 7) ^ (s0 >> 16);
        self.one = s0;
        self.two = s1;
        return s0 +% s1;
    }

    /// Bias-free modulo via Lemire's multiplication trick.
    /// Returns a uniformly distributed value in [0, n).
    pub fn fastrand_n(self: *FastRand, n: u32) u32 {
        const mul: u64 = @as(u64, self.fastrand()) *% @as(u64, n);
        return @truncate(mul >> 32);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

pub const Config = struct {
    /// Number of worker threads (0 = auto-detect based on CPU count)
    num_workers: usize = 0,

    /// Maximum tasks in global queue before backpressure
    global_queue_capacity: usize = 65536,

    /// Enable work stealing
    enable_stealing: bool = true,

    /// I/O backend type (null = auto-detect best for platform)
    backend_type: ?BackendType = null,

    /// Maximum I/O completions to process per tick
    max_io_completions: u32 = 256,

    pub fn getNumWorkers(self: Config) usize {
        if (self.num_workers == 0) {
            return @max(1, std.Thread.getCpuCount() catch 1);
        }
        return self.num_workers;
    }

    /// Convert to I/O backend configuration
    pub fn toBackendConfig(self: Config) BackendConfig {
        return BackendConfig{
            .backend_type = self.backend_type orelse .auto,
            .max_completions = self.max_io_completions,
        };
    }
};

pub const default_config = Config{};

// ═══════════════════════════════════════════════════════════════════════════════
// Idle State - Worker Idle Coordination
// ═══════════════════════════════════════════════════════════════════════════════

/// Tracks searching/parked workers to prevent thundering herd.
///
/// Implements the "last searcher must notify" protocol:
///
/// 1. When a task is enqueued and num_searching == 0, ONE parked worker is woken
///    and enters the "searching" state (num_searching += 1).
/// 2. When a searching worker finds work and starts executing it, it calls
///    transitionWorkerFromSearching(). If it was the LAST searcher
///    (num_searching goes to 0), it wakes another parked worker.
/// 3. This creates a chain reaction: each worker that finds work ensures the
///    next one is awake, guaranteeing all queued tasks get processed.
/// 4. When a searching worker parks without finding work, it decrements
///    num_searching. If it was the last searcher, it does a final queue check.
pub const IdleState = struct {
    /// Number of workers currently searching for work.
    /// A searching worker will either find work (and chain-notify) or park.
    /// SeqCst ordering required to pair with the fetch_sub when
    /// transitioning out of searching.
    num_searching: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Number of workers currently parked (sleeping).
    num_parked: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Bitmap of parked workers (for selective waking).
    /// Each bit represents one worker: 1 = parked, 0 = running/searching.
    /// When waking a worker, its bit is atomically cleared so the next
    /// wakeup call targets a different worker.
    parked_bitmap: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total number of workers
    num_workers: u32,

    pub fn init(num_workers: u32) IdleState {
        return .{ .num_workers = num_workers };
    }

    /// Fast-path check: should we try to wake a worker?
    /// Returns true if no workers are searching AND some are parked.
    /// Uses SeqCst to pair with the fetch_sub in transitionWorkerFromSearching.
    fn notifyShouldWakeup(self: *const IdleState) bool {
        return self.num_searching.load(.seq_cst) == 0 and
            self.num_parked.load(.seq_cst) > 0;
    }

    /// Try to claim a parked worker for waking.
    /// Atomically clears the worker's bitmap bit and adjusts counters.
    /// Returns the worker ID if successful, null if no parked worker available.
    ///
    /// On success, increments num_searching — the woken worker enters
    /// "searching" state and is responsible for the chain notification protocol.
    pub fn claimParkedWorker(self: *IdleState) ?usize {
        // Atomically claim a worker from the bitmap.
        // Loop because another thread might clear the same bit concurrently.
        while (true) {
            const bitmap = self.parked_bitmap.load(.seq_cst);
            if (bitmap == 0) return null;

            const worker_id: usize = @ctz(bitmap);
            const mask: u64 = @as(u64, 1) << @intCast(worker_id);

            // Atomically clear this worker's bit
            const prev = self.parked_bitmap.fetchAnd(~mask, .seq_cst);
            if (prev & mask == 0) {
                // Lost race — another thread cleared this bit. Retry.
                continue;
            }

            // Successfully claimed this worker.
            // Decrement parked count and increment searching count atomically.
            _ = self.num_parked.fetchSub(1, .seq_cst);
            _ = self.num_searching.fetchAdd(1, .seq_cst);

            return worker_id;
        }
    }

    /// Called when a worker is about to park.
    /// Sets the bitmap bit and increments num_parked.
    /// If the worker was searching, decrements num_searching.
    ///
    /// Returns true if this was the LAST searching worker — caller must
    /// do a final queue check to prevent lost wakeups.
    pub fn transitionWorkerToParked(self: *IdleState, worker_id: usize, is_searching: bool) bool {
        if (worker_id >= MAX_WORKERS) return false;

        // Add to parked set
        const mask: u64 = @as(u64, 1) << @intCast(worker_id);
        _ = self.parked_bitmap.fetchOr(mask, .seq_cst);
        _ = self.num_parked.fetchAdd(1, .seq_cst);

        // Handle searching state
        if (is_searching) {
            const prev = self.num_searching.fetchSub(1, .seq_cst);
            return prev == 1; // Was the last searcher
        }
        return false;
    }

    /// Called when a searching worker finds work and starts executing.
    /// Decrements num_searching.
    ///
    /// Returns true if this was the LAST searching worker — caller MUST
    /// call wakeWorkerIfNeeded() to maintain the notification chain.
    pub fn transitionWorkerFromSearching(self: *IdleState) bool {
        const prev = self.num_searching.fetchSub(1, .seq_cst);
        return prev == 1; // Was the last searcher
    }

    /// Try to become a searcher (for work stealing).
    /// Limits searchers to ~50% of workers to prevent thundering herd.
    pub fn tryBecomeSearcher(self: *IdleState, num_workers: u32) bool {
        // Allow at most 50% of workers to search simultaneously.
        // It's OK if this is slightly over due to races — it's an optimization,
        // not a correctness requirement.
        const state = self.num_searching.load(.seq_cst);
        if (2 * state >= num_workers) return false;

        _ = self.num_searching.fetchAdd(1, .seq_cst);
        return true;
    }

    /// Check if a specific worker is parked (bitmap bit is set)
    pub fn isWorkerParked(self: *const IdleState, worker_id: usize) bool {
        if (worker_id >= MAX_WORKERS) return false;
        const mask: u64 = @as(u64, 1) << @intCast(worker_id);
        return (self.parked_bitmap.load(.seq_cst) & mask) != 0;
    }

    /// Get number of parked workers
    pub fn getParkedCount(self: *const IdleState) u32 {
        return self.num_parked.load(.seq_cst);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Park State - Futex-based parking mechanism
// ═══════════════════════════════════════════════════════════════════════════════

pub const ParkState = struct {
    /// Futex word for parking
    futex: std.atomic.Value(u32) = std.atomic.Value(u32).init(UNPARKED),

    const UNPARKED: u32 = 0;
    const PARKED: u32 = 1;
    const NOTIFIED: u32 = 2;

    /// Park the worker until notified or timeout
    pub fn park(self: *ParkState, timeout_ns: ?u64) void {
        // Try to transition to PARKED
        const result = self.futex.cmpxchgStrong(
            UNPARKED,
            PARKED,
            .acq_rel,
            .acquire,
        );

        if (result == null) {
            // Successfully set to PARKED, now wait
            if (debug_scheduler) {
                std.debug.print("[PARK] parking, will wait on futex\n", .{});
            }
            if (timeout_ns) |ns| {
                std.Thread.Futex.timedWait(&self.futex, PARKED, ns) catch {};
            } else {
                std.Thread.Futex.wait(&self.futex, PARKED);
            }
            if (debug_scheduler) {
                std.debug.print("[PARK] woke from futex wait\n", .{});
            }
        } else {
            if (debug_scheduler) {
                std.debug.print("[PARK] cmpxchg failed (state was {d}), skipping wait\n", .{result.?});
            }
        }

        // Reset state
        self.futex.store(UNPARKED, .release);
    }

    /// Unpark the worker
    pub fn unpark(self: *ParkState) void {
        const prev = self.futex.swap(NOTIFIED, .acq_rel);
        if (debug_scheduler) {
            std.debug.print("[PARK] unpark: prev={d}, will_wake={}\n", .{ prev, prev == PARKED });
        }
        if (prev == PARKED) {
            std.Thread.Futex.wake(&self.futex, 1);
        }
    }

    /// Check if in parked state
    pub fn isParked(self: *const ParkState) bool {
        return self.futex.load(.acquire) == PARKED;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Worker - Multi-Threaded Worker with Production Optimizations
// ═══════════════════════════════════════════════════════════════════════════════

pub const Worker = struct {
    const Self = @This();

    /// Worker index
    index: usize,

    /// LIFO slot - single task for immediate execution (cache-hot).
    /// The newest task always goes in LIFO; evicted task goes to run_queue.
    lifo_slot: std.atomic.Value(?*Header) = std.atomic.Value(?*Header).init(null),

    /// Lock-free work-stealing run queue (replaces old TaskQueue local_queue).
    /// Owner pushes/pops from back, stealers steal half from front.
    run_queue: WorkStealQueue = WorkStealQueue.init(),

    /// Reference to scheduler
    scheduler: *Scheduler,

    /// Worker thread handle
    thread: ?std.Thread = null,

    /// Running flag
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Parking state
    park_state: ParkState = .{},

    // ─────────────────────────────────────────────────────────────────────────
    // Cooperative Scheduling State
    // ─────────────────────────────────────────────────────────────────────────

    /// Tick counter for periodic operations
    tick: u32 = 0,

    /// Cooperative budget: remaining polls this tick
    budget: u32 = BUDGET_PER_TICK,

    /// LIFO polls this tick - limited to prevent starvation
    lifo_polls_this_tick: u8 = 0,

    /// CRITICAL: Flag indicating worker is transitioning to parked state.
    /// Set BEFORE flushing LIFO, cleared AFTER waking.
    /// Prevents lost wakeup race condition.
    transitioning_to_park: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Whether this worker is in the "searching" state.
    /// A searching worker is one that has been woken by a notification and is
    /// actively looking for work. When a searching worker finds work, it
    /// transitions out of searching -- and if it was the LAST searcher, it
    /// wakes another worker (chain notification protocol).
    ///
    /// This is a per-worker cache that mirrors the global num_searching counter.
    is_searching: bool = false,

    /// Per-worker PRNG for randomized victim selection
    rand: FastRand,

    // ─────────────────────────────────────────────────────────────────────────
    // Adaptive Global Queue Interval
    // ─────────────────────────────────────────────────────────────────────────

    /// EWMA of task poll time in nanoseconds
    avg_task_ns: u64 = DEFAULT_AVG_TASK_NS,

    /// Calculated global queue check interval (adaptive)
    global_queue_interval: u32 = 61,

    /// Timestamp of batch start for measuring poll times
    batch_start_ns: i128 = 0,

    // ─────────────────────────────────────────────────────────────────────────
    // Global Queue Batch Buffer
    // ─────────────────────────────────────────────────────────────────────────

    /// Buffer for batch global queue pulls
    global_batch_buffer: [GLOBAL_QUEUE_BATCH_SIZE]?*Header = [_]?*Header{null} ** GLOBAL_QUEUE_BATCH_SIZE,

    /// Current position in batch buffer
    global_batch_pos: usize = 0,

    /// Number of tasks in batch buffer
    global_batch_len: usize = 0,

    // ─────────────────────────────────────────────────────────────────────────
    // Statistics
    // ─────────────────────────────────────────────────────────────────────────

    stats: Stats = .{},

    pub const Stats = struct {
        tasks_polled: u64 = 0,
        tasks_stolen: u64 = 0,
        times_parked: u64 = 0,
        lifo_hits: u64 = 0,
        global_batch_fetches: u64 = 0,
    };

    /// Initialize a worker with per-worker PRNG seed
    pub fn init(index: usize, scheduler: *Scheduler) Self {
        // Seed PRNG with worker index + timestamp for uniqueness
        const ts: u128 = @bitCast(std.time.nanoTimestamp());
        const seed: u64 = @as(u64, @truncate(ts)) +% @as(u64, index) *% 0x9E3779B97F4A7C15;
        return .{
            .index = index,
            .scheduler = scheduler,
            .rand = FastRand.init(seed),
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Task Scheduling (with LIFO eviction)
    // ─────────────────────────────────────────────────────────────────────────

    /// Schedule a task on this worker's LIFO slot (preferred for locality).
    /// Returns false if worker is transitioning to park - use global queue instead.
    ///
    /// LIFO eviction: the NEW task always goes into LIFO (cache-hot).
    /// If LIFO was occupied, the OLD task is evicted to run_queue.
    pub fn tryScheduleLocal(self: *Self, task: *Header) bool {
        // CRITICAL: Check transitioning flag to prevent lost wakeup
        if (self.transitioning_to_park.load(.acquire)) {
            return false; // Caller should use global queue
        }

        // Evict old LIFO task to run_queue, put new task in LIFO
        if (self.lifo_slot.swap(task, .acq_rel)) |prev| {
            // Old task evicted → push to back of run queue
            self.run_queue.push(prev, &self.scheduler.global_queue);
        }
        self.stats.lifo_hits += 1;
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Work Finding (priority order)
    // ─────────────────────────────────────────────────────────────────────────

    /// Find work from any source using the defined priority order.
    fn findWork(self: *Self) ?*Header {
        // 1. Check buffered global queue batch first
        if (self.pollGlobalBatch()) |task| {
            if (debug_scheduler) {
                std.debug.print("[WORKER {d}] findWork: found in batch buffer\n", .{self.index});
            }
            return task;
        }

        // 2. Check local queue with LIFO limiting
        if (self.pollLocalQueue()) |task| {
            if (debug_scheduler) {
                std.debug.print("[WORKER {d}] findWork: found in local queue\n", .{self.index});
            }
            return task;
        }

        // 3. Check global queue periodically (adaptive interval)
        if (self.tick % self.global_queue_interval == 0) {
            if (self.fetchGlobalBatch()) |task| {
                if (debug_scheduler) {
                    std.debug.print("[WORKER {d}] findWork: fetched from global (periodic)\n", .{self.index});
                }
                return task;
            }
        }

        // 4. Try stealing from other workers (with searcher limiting)
        if (self.scheduler.config.enable_stealing) {
            if (self.tryStealWithLimit()) |task| {
                self.stats.tasks_stolen += 1;
                if (debug_scheduler) {
                    std.debug.print("[WORKER {d}] findWork: stole task\n", .{self.index});
                }
                return task;
            }
        }

        // 5. Final check of global queue before parking
        return self.fetchGlobalBatch();
    }

    /// Poll local queue with LIFO limiting
    fn pollLocalQueue(self: *Self) ?*Header {
        // Check LIFO slot (with per-tick limit to prevent starvation)
        if (self.lifo_polls_this_tick < MAX_LIFO_POLLS_PER_TICK) {
            if (self.lifo_slot.swap(null, .acquire)) |task| {
                self.lifo_polls_this_tick +|= 1;
                return task;
            }
        } else {
            // LIFO cap reached — flush LIFO to run_queue to prevent it
            // from becoming invisible. Without this, the worker breaks out
            // of the inner loop after only MAX_LIFO_POLLS tasks per tick
            // because both LIFO (skipped) and run_queue (empty) return null.
            if (self.lifo_slot.swap(null, .acquire)) |task| {
                self.run_queue.push(task, &self.scheduler.global_queue);
            }
        }

        // Check run queue (lock-free pop from front)
        return self.run_queue.pop();
    }

    /// Poll from pre-fetched global batch buffer
    fn pollGlobalBatch(self: *Self) ?*Header {
        if (self.global_batch_pos < self.global_batch_len) {
            const task = self.global_batch_buffer[self.global_batch_pos];
            self.global_batch_buffer[self.global_batch_pos] = null;
            self.global_batch_pos += 1;
            return task;
        }
        return null;
    }

    /// Fetch a batch of tasks from global queue in a SINGLE lock acquisition.
    fn fetchGlobalBatch(self: *Self) ?*Header {
        // Lock-free emptiness check avoids taking the lock when empty
        if (self.scheduler.global_queue.isEmptyFast()) return null;

        // Adaptive batch size based on queue depth and worker count
        const num_workers = self.scheduler.workers.len;
        const global_len = self.scheduler.global_queue.lengthFast();
        const fair_share = global_len / @max(1, num_workers);
        const batch_size = @min(MAX_GLOBAL_QUEUE_BATCH_SIZE, @max(MIN_GLOBAL_QUEUE_BATCH_SIZE, fair_share));

        // Single lock acquisition for entire batch
        var batch_buf: [MAX_GLOBAL_QUEUE_BATCH_SIZE]*Header = undefined;
        const fetched = self.scheduler.global_queue.popBatch(&batch_buf, batch_size);
        if (fetched == 0) return null;

        self.stats.global_batch_fetches += 1;
        const first = batch_buf[0];

        // Push remaining to our run_queue
        for (1..fetched) |i| {
            self.run_queue.push(batch_buf[i], &self.scheduler.global_queue);
        }

        return first;
    }

    /// Try to steal work with searcher limiting (~50% of workers).
    ///
    /// The worker transitions to searching and STAYS searching until it
    /// either finds work (handled by transitionFromSearching in executeTask)
    /// or parks (handled by parkWorker).
    fn tryStealWithLimit(self: *Self) ?*Header {
        const num_workers = self.scheduler.workers.len;
        if (num_workers <= 1) return null;

        // Transition to searching if not already.
        if (!self.is_searching) {
            if (!self.scheduler.idle_state.tryBecomeSearcher(@intCast(num_workers))) {
                return null; // Too many searchers already
            }
            self.is_searching = true;
        }

        // Try stealing from other workers (half-steal with random victim)
        if (self.trySteal()) |task| {
            return task;
        }

        // Fallback: check global queue
        return self.scheduler.nextRemoteTask();
    }

    /// Try to steal half of another worker's run_queue into our run_queue.
    /// Uses random victim selection (xorshift64+ PRNG with Lemire's modulo).
    ///
    /// Does NOT steal from LIFO -- the LIFO slot is an owner-only optimization.
    fn trySteal(self: *Self) ?*Header {
        const num_workers = self.scheduler.workers.len;

        // Randomized starting victim (bias-free via Lemire's trick)
        const start = self.rand.fastrand_n(@intCast(num_workers));

        for (0..num_workers) |i| {
            const victim_id = (@as(usize, start) + i) % num_workers;
            if (victim_id == self.index) continue;

            const victim = &self.scheduler.workers[victim_id];

            // Try to steal half of victim's run_queue into our run_queue
            if (WorkStealQueue.stealInto(&victim.run_queue, &self.run_queue)) |task| {
                return task;
            }
        }

        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Searching State Protocol ("last searcher must notify")
    // ─────────────────────────────────────────────────────────────────────────

    /// Transition this worker out of the "searching" state.
    ///
    /// Called before executing a found task. If this was the LAST searching
    /// worker, we MUST wake another worker to continue the notification chain.
    fn transitionFromSearching(self: *Self) void {
        if (!self.is_searching) return;

        self.is_searching = false;

        if (self.scheduler.idle_state.transitionWorkerFromSearching()) {
            self.scheduler.wakeWorkerIfNeeded();
        }
    }

    /// Check if this worker has local tasks (LIFO slot or run queue)
    fn hasTasks(self: *const Self) bool {
        return self.lifo_slot.load(.acquire) != null or !self.run_queue.isEmpty();
    }

    /// Execute tasks from this worker's queues while waiting for a target
    /// task to complete. Called by JoinHandle.join() on worker threads.
    /// Enables work-stealing join: instead of spinning, the worker makes
    /// progress on other tasks (or the target task itself if it's in LIFO).
    pub fn helpUntilComplete(self: *Self, target: *Header) void {
        // Save current header — we may be inside a task that called join()
        const saved_header = getCurrentHeader();

        var spins: u32 = 0;
        while (!target.isComplete()) {
            // Service I/O and timers so wakeups can fire
            _ = self.scheduler.pollIo();
            if (self.index == 0) {
                _ = self.scheduler.pollTimers();
            }

            if (self.findWork()) |task| {
                self.executeTask(task);
                spins = 0;
            } else {
                if (spins < 6) {
                    std.atomic.spinLoopHint();
                } else if (spins < 12) {
                    std.Thread.yield() catch {};
                } else {
                    std.Thread.sleep(1_000); // 1us
                }
                spins +|= 1;
            }
        }

        // Restore the header of the task that called join()
        setCurrentHeader(saved_header);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Adaptive Interval Tuning
    // ─────────────────────────────────────────────────────────────────────────

    /// Update adaptive global queue interval based on measured task poll times.
    fn updateAdaptiveInterval(self: *Self, tasks_run: u32) void {
        if (tasks_run == 0) return;

        const end_ns = std.time.nanoTimestamp();
        const batch_ns: u64 = @intCast(@max(0, end_ns - self.batch_start_ns));
        const current_avg = batch_ns / tasks_run;

        // EWMA: new_avg = (current * alpha) + (old * (1 - alpha))
        const new_avg: u64 = @intFromFloat(
            (@as(f64, @floatFromInt(current_avg)) * EWMA_ALPHA) +
                (@as(f64, @floatFromInt(self.avg_task_ns)) * (1.0 - EWMA_ALPHA)),
        );
        self.avg_task_ns = @max(1, new_avg);

        // Calculate new interval: how many tasks fit in TARGET_GLOBAL_QUEUE_LATENCY_NS?
        const tasks_per_target = TARGET_GLOBAL_QUEUE_LATENCY_NS / self.avg_task_ns;
        const clamped = @min(MAX_GLOBAL_QUEUE_INTERVAL, @max(MIN_GLOBAL_QUEUE_INTERVAL, tasks_per_target));
        self.global_queue_interval = @intCast(clamped);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Worker Loop
    // ─────────────────────────────────────────────────────────────────────────

    /// Main worker loop
    fn run(self: *Self) void {
        self.running.store(true, .release);

        // Set thread-local context
        setCurrentContext(self.scheduler, self.index);
        defer clearCurrentContext();

        // Set top-level Runtime context so scope.spawn() etc. can access it
        if (self.scheduler.runtime_ptr) |rt_ptr| {
            runtime_mod.setCurrentRuntime(rt_ptr);
        }

        // Signal ready
        _ = self.scheduler.ready_count.fetchAdd(1, .release);

        while (!self.scheduler.shutdown.load(.acquire)) {
            self.tick +%= 1;
            self.batch_start_ns = std.time.nanoTimestamp();

            // Poll timers at the start of each tick (worker 0 is primary timer driver)
            if (self.index == 0) {
                _ = self.scheduler.pollTimers();
            }

            // Poll I/O completions (all workers can try, uses tryLock)
            _ = self.scheduler.pollIo();

            // Run tasks until budget exhausted or no work
            var tasks_run: u32 = 0;
            while (self.budget > 0) {
                if (self.findWork()) |task| {
                    self.executeTask(task);
                    self.budget -|= 1;
                    tasks_run += 1;
                } else {
                    break;
                }
            }

            // Update adaptive interval
            self.updateAdaptiveInterval(tasks_run);

            // Maintenance: reset budget and counters
            self.budget = BUDGET_PER_TICK;
            self.lifo_polls_this_tick = 0;
            self.global_batch_pos = 0;
            self.global_batch_len = 0;

            if (tasks_run == 0) {
                self.parkWorker();
            }
        }

        self.running.store(false, .release);
        self.drainOnShutdown();
    }

    /// Execute a single task
    fn executeTask(self: *Self, task: *Header) void {
        // CRITICAL: Transition out of searching BEFORE executing the task.
        // This enables another idle worker to try to steal work.
        // If we were the last searcher, this wakes another worker.
        self.transitionFromSearching();

        // Transition to running
        if (!task.transitionToRunning()) {
            return; // Cancelled or wrong state
        }

        // Set current header so sleep() and other async primitives can find it
        setCurrentHeader(task);
        defer setCurrentHeader(null);

        // Poll the task
        const result = task.poll();
        self.stats.tasks_polled += 1;

        switch (result) {
            .complete => {
                task.transitionToComplete();
                if (task.unref()) {
                    task.drop();
                }
            },
            .pending => {
                // Task yielded - transition back to idle
                // CRITICAL: transitionToIdle atomically clears the notified bit.
                // We check if it was set BEFORE clearing.
                const prev = task.transitionToIdle();

                // Wakeup protocol:
                // - If prev.notified was true, the waker fired while we were RUNNING.
                //   We MUST reschedule now, or the task is orphaned forever.
                // - If prev.notified was false, the task is waiting for an external event.
                if (prev.notified) {
                    // Waker fired while running - reschedule immediately
                    if (task.transitionToScheduled()) {
                        if (!self.tryScheduleLocal(task)) {
                            self.scheduler.global_queue.push(task);
                            self.scheduler.wakeWorkerIfNeeded();
                        }
                    }
                }
            },
        }
    }

    /// Park the worker with the full parking protocol.
    ///
    /// Implements:
    /// 1. Lost wakeup prevention (transitioning_to_park flag)
    /// 2. Last-searcher-must-check-queues (from transition_to_parked)
    /// 3. Proper bitmap-based wake detection (claimed by notifier vs timeout)
    fn parkWorker(self: *Self) void {
        self.stats.times_parked += 1;

        // Workers should not park if they have local work
        if (self.hasTasks()) {
            return;
        }

        // CRITICAL: Set transitioning flag BEFORE flush
        // This tells schedulers "don't push to my LIFO, use global queue"
        self.transitioning_to_park.store(true, .release);

        // Flush LIFO slot to run queue so tasks are stealable
        if (self.lifo_slot.swap(null, .acquire)) |task| {
            self.run_queue.push(task, &self.scheduler.global_queue);
        }

        // Double-check: if new work arrived during flush, don't park
        if (!self.run_queue.isEmpty() or !self.scheduler.global_queue.isEmptyFast()) {
            self.transitioning_to_park.store(false, .release);
            self.budget = BUDGET_PER_TICK;
            return;
        }

        // Transition to parked in the idle state.
        // If we were the last searching worker, we get a signal to check queues.
        const is_last_searcher = self.scheduler.idle_state.transitionWorkerToParked(
            self.index,
            self.is_searching,
        );
        self.is_searching = false;

        if (is_last_searcher) {
            // We were the last searcher. Before sleeping, check if any work
            // materialized that nobody will find.
            self.scheduler.notifyIfWorkPending();
        }

        // Calculate park timeout: use timer expiration or default
        // Worker 0 must wake to process timers, others can sleep longer
        const park_timeout: u64 = if (self.index == 0)
            self.scheduler.nextTimerExpiration() orelse PARK_TIMEOUT_NS
        else
            PARK_TIMEOUT_NS;

        // Park with timeout
        self.park_state.park(park_timeout);

        // After waking: clear transitioning flag
        self.transitioning_to_park.store(false, .release);

        // Determine HOW we woke up:
        // - If our bitmap bit is clear → we were claimed by a notification
        // - If our bitmap bit is still set → timeout/spurious wake
        if (!self.scheduler.idle_state.isWorkerParked(self.index)) {
            // Claimed by notification — we are a searcher
            self.is_searching = true;
        } else {
            // Timeout wake — remove ourselves from parked set
            const mask: u64 = @as(u64, 1) << @intCast(self.index);
            _ = self.scheduler.idle_state.parked_bitmap.fetchAnd(~mask, .seq_cst);
            _ = self.scheduler.idle_state.num_parked.fetchSub(1, .seq_cst);
            self.is_searching = false;
        }

        // Reset budget after waking
        self.budget = BUDGET_PER_TICK;
    }

    /// Drain tasks on shutdown
    fn drainOnShutdown(self: *Self) void {
        // Cancel and drop LIFO slot
        if (self.lifo_slot.swap(null, .acquire)) |task| {
            _ = task.cancel();
            if (task.unref()) {
                task.drop();
            }
        }

        // Cancel and drop run queue
        while (self.run_queue.pop()) |task| {
            _ = task.cancel();
            if (task.unref()) {
                task.drop();
            }
        }

        // Cancel and drop batch buffer
        while (self.global_batch_pos < self.global_batch_len) {
            if (self.global_batch_buffer[self.global_batch_pos]) |task| {
                _ = task.cancel();
                if (task.unref()) {
                    task.drop();
                }
            }
            self.global_batch_pos += 1;
        }
    }

    /// Wake this worker
    pub fn wake(self: *Self) void {
        self.park_state.unpark();
    }

    /// Check if worker is parked
    pub fn isParked(self: *const Self) bool {
        return self.park_state.isParked();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Scheduler - Production-grade work-stealing scheduler
// ═══════════════════════════════════════════════════════════════════════════════

pub const Scheduler = struct {
    const Self = @This();

    /// Configuration
    config: Config,

    /// Workers array
    workers: []Worker,

    /// Global injection queue
    global_queue: GlobalTaskQueue = .{},

    /// Shutdown flag
    shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Idle coordination state
    idle_state: IdleState,

    /// Number of workers that have started and are ready
    ready_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Timer wheel for sleep/timeout operations
    timer_wheel: TimerWheel,

    /// Mutex protecting timer wheel (multiple workers may poll)
    timer_mutex: std.Thread.Mutex = .{},

    /// I/O backend (owned by scheduler for centralized management)
    backend: Backend,

    /// Completion buffer for I/O polling
    completions: []Completion,

    /// Mutex protecting I/O backend (single poller at a time)
    io_mutex: std.Thread.Mutex = .{},

    /// Allocator
    allocator: Allocator,

    /// Top-level Runtime pointer (for setting threadlocal context on workers)
    runtime_ptr: ?*anyopaque = null,

    /// Statistics
    total_spawned: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    timers_processed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    io_completions_processed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Initialize the scheduler
    pub fn init(allocator: Allocator, config: Config) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const num_workers = config.getNumWorkers();

        // Initialize I/O backend (centralized here for automatic platform selection)
        var backend = try Backend.init(allocator, config.toBackendConfig());
        errdefer backend.deinit();

        // Allocate completions buffer
        const completions = try allocator.alloc(Completion, config.max_io_completions);
        errdefer allocator.free(completions);

        self.* = .{
            .config = config,
            .workers = try allocator.alloc(Worker, num_workers),
            .idle_state = IdleState.init(@intCast(num_workers)),
            .timer_wheel = TimerWheel.init(allocator),
            .backend = backend,
            .completions = completions,
            .allocator = allocator,
        };

        // Initialize workers
        for (self.workers, 0..) |*worker, i| {
            worker.* = Worker.init(i, self);
        }

        return self;
    }

    /// Deinitialize the scheduler
    pub fn deinit(self: *Self) void {
        self.timer_wheel.deinit();
        self.backend.deinit();
        self.allocator.free(self.completions);
        self.allocator.free(self.workers);
        self.allocator.destroy(self);
    }

    /// Set the top-level Runtime pointer so workers can set threadlocal context
    pub fn setRuntimePtr(self: *Self, rt_ptr: *anyopaque) void {
        self.runtime_ptr = rt_ptr;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /// Start all worker threads
    pub fn start(self: *Self) !void {
        self.ready_count.store(0, .release);

        for (self.workers) |*worker| {
            worker.thread = try std.Thread.spawn(.{}, Worker.run, .{worker});
        }
    }

    /// Wait until all workers have started
    pub fn waitForWorkersReady(self: *Self) void {
        const target: u32 = @intCast(self.workers.len);
        while (self.ready_count.load(.acquire) < target) {
            std.Thread.yield() catch {};
        }
    }

    /// Start workers and wait until all are ready
    pub fn startAndWait(self: *Self) !void {
        try self.start();
        self.waitForWorkersReady();
    }

    /// Signal shutdown and wait for all workers
    pub fn shutdownAndWait(self: *Self) void {
        self.shutdown.store(true, .release);

        // Wake all workers
        for (self.workers) |*worker| {
            worker.wake();
        }

        // Join all threads
        for (self.workers) |*worker| {
            if (worker.thread) |thread| {
                thread.join();
                worker.thread = null;
            }
        }
    }

    /// Check if scheduler is running
    pub fn isRunning(self: *Self) bool {
        return !self.shutdown.load(.acquire);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Task Spawning
    // ─────────────────────────────────────────────────────────────────────────

    /// Spawn a new task onto the scheduler.
    /// Used for INITIAL task creation (Runtime.spawn).
    /// Handles: transition IDLE->SCHEDULED, add ref, queue, wake worker.
    pub fn spawn(self: *Self, task: *Header) void {
        if (!task.transitionToScheduled()) {
            return;
        }

        task.ref();
        _ = self.total_spawned.fetchAdd(1, .monotonic);

        // Push to global queue
        self.global_queue.push(task);

        // Wake a worker if needed (using bitmap for O(1) lookup)
        self.wakeWorkerIfNeeded();
    }

    /// Enqueue an already-scheduled task.
    /// Used by waker callbacks AFTER they've already transitioned the task.
    /// Does NOT do state transition or ref increment.
    pub fn enqueue(self: *Self, task: *Header) void {
        if (debug_scheduler) {
            std.debug.print("[SCHED] enqueue: task={x}, global_queue_len before={d}\n", .{
                @intFromPtr(task),
                self.global_queue.length(),
            });
        }
        self.global_queue.push(task);
        if (debug_scheduler) {
            std.debug.print("[SCHED] enqueue: pushed, global_queue_len after={d}, calling wakeWorkerIfNeeded\n", .{
                self.global_queue.length(),
            });
        }
        self.wakeWorkerIfNeeded();
        if (debug_scheduler) {
            std.debug.print("[SCHED] enqueue: wakeWorkerIfNeeded returned\n", .{});
        }
    }

    /// Reschedule a task that yielded with notified bit set.
    /// Used by executeTask when a task was woken while running.
    /// Does transition but no ref increment (task already has ref).
    pub fn reschedule(self: *Self, task: *Header) void {
        if (!task.transitionToScheduled()) {
            return;
        }

        // Try local queue first (LIFO optimization)
        if (getCurrentWorkerIndex()) |worker_idx| {
            if (worker_idx < self.workers.len) {
                if (self.workers[worker_idx].tryScheduleLocal(task)) {
                    return;
                }
            }
        }

        // Fallback to global queue
        self.global_queue.push(task);
        self.wakeWorkerIfNeeded();
    }

    /// Spawn from a specific worker context (use local queue)
    pub fn spawnFromWorker(self: *Self, worker_idx: usize, task: *Header) void {
        if (!task.transitionToScheduled()) {
            return;
        }

        task.ref();
        _ = self.total_spawned.fetchAdd(1, .monotonic);

        if (worker_idx < self.workers.len) {
            const worker = &self.workers[worker_idx];
            if (!worker.tryScheduleLocal(task)) {
                // Worker transitioning to park, use global
                self.global_queue.push(task);
            }
        } else {
            self.global_queue.push(task);
        }
        // Always wake a parked worker so it can steal from our local queue.
        // For serial spawn+await, the woken worker finds nothing (task already
        // consumed via LIFO). For batch spawn, workers steal and run in parallel.
        self.wakeWorkerIfNeeded();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inline Task Execution (for non-worker threads)
    // ─────────────────────────────────────────────────────────────────────────

    /// Execute a single task inline (from non-worker thread).
    /// Handles state transitions and rescheduling on notified.
    fn executeInline(self: *Self, task: *Header) void {
        if (!task.transitionToRunning()) {
            return; // Cancelled or wrong state
        }

        setCurrentHeader(task);
        defer setCurrentHeader(null);

        const result = task.poll();

        switch (result) {
            .complete => {
                task.transitionToComplete();
                if (task.unref()) {
                    task.drop();
                }
            },
            .pending => {
                const prev = task.transitionToIdle();
                if (prev.notified) {
                    if (task.transitionToScheduled()) {
                        self.global_queue.push(task);
                        self.wakeWorkerIfNeeded();
                    }
                }
            },
        }
    }

    /// Block the calling (non-worker) thread until a target task completes,
    /// executing tasks from the global queue to make progress.
    pub fn blockOnComplete(self: *Self, target: *Header) void {
        var spins: u32 = 0;
        while (!target.isComplete()) {
            // Try global queue FIRST — the target task likely just landed there
            if (self.global_queue.pop()) |task| {
                self.executeInline(task);
                spins = 0;
                continue;
            }

            // Service I/O and timers so wakeups can fire
            _ = self.pollIo();
            _ = self.pollTimers();

            if (spins < 6) {
                std.atomic.spinLoopHint();
            } else if (spins < 12) {
                std.Thread.yield() catch {};
            } else {
                std.Thread.sleep(1_000); // 1us
            }
            spins +|= 1;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Wake Management -- "Last Searcher Must Notify" Protocol
    //
    // Implements the chain notification protocol:
    // 1. When a task is enqueued and no workers are searching, wake ONE.
    // 2. When a searching worker finds work, if it was the last searcher,
    //    wake another (chain notification).
    // 3. This ensures all queued tasks eventually get processed.
    // ─────────────────────────────────────────────────────────────────────────

    /// Wake a parked worker if needed.
    ///
    /// If at least one worker is already searching, no wake is needed — the
    /// searching worker will find work and chain-notify if needed.
    /// If no workers are searching, claim one parked worker and wake it.
    ///
    /// The claimed worker enters "searching" state (num_searching += 1).
    fn wakeWorkerIfNeeded(self: *Self) void {
        // Fast path: if someone is already searching, they'll find the work
        // eventually. A searching worker will find *some* work and, when it
        // transitions out of searching (via transitionFromSearching), if it
        // was the last searcher, it will notify again.
        //
        // SeqCst ordering pairs with the fetch_sub in transitionWorkerFromSearching.
        if (!self.idle_state.notifyShouldWakeup()) {
            if (debug_scheduler) {
                std.debug.print("[SCHED] wakeWorkerIfNeeded: notifyShouldWakeup=false, returning\n", .{});
            }
            return;
        }

        // Claim a parked worker. This atomically:
        // 1. Clears the worker's bitmap bit (so next call targets a different worker)
        // 2. Decrements num_parked
        // 3. Increments num_searching (worker enters searching state)
        if (self.idle_state.claimParkedWorker()) |worker_id| {
            if (debug_scheduler) {
                std.debug.print("[SCHED] wakeWorkerIfNeeded: claimed and waking worker {d}\n", .{worker_id});
            }
            if (worker_id < self.workers.len) {
                self.workers[worker_id].wake();
            }
        } else if (debug_scheduler) {
            std.debug.print("[SCHED] wakeWorkerIfNeeded: no parked worker to claim\n", .{});
        }
    }

    /// Check all queues and notify if work is pending.
    /// Called when the LAST searching worker parks -- ensures no stranded tasks.
    fn notifyIfWorkPending(self: *Self) void {
        // Check if any worker has stealable work
        for (self.workers) |*worker| {
            if (worker.lifo_slot.load(.acquire) != null or !worker.run_queue.isEmpty()) {
                self.wakeWorkerIfNeeded();
                return;
            }
        }

        // Check global queue (lock-free check)
        if (!self.global_queue.isEmptyFast()) {
            self.wakeWorkerIfNeeded();
        }
    }

    /// Pop a single task from the global injection queue.
    /// Used as fallback when work stealing finds nothing.
    fn nextRemoteTask(self: *Self) ?*Header {
        return self.global_queue.pop();
    }

    /// Wake all workers (for shutdown)
    fn wakeAll(self: *Self) void {
        for (self.workers) |*worker| {
            worker.wake();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Statistics
    // ─────────────────────────────────────────────────────────────────────────

    pub const Stats = struct {
        total_spawned: u64,
        total_polled: u64,
        total_stolen: u64,
        total_parked: u64,
        num_workers: usize,
    };

    pub fn getStats(self: *const Self) Stats {
        var total_polled: u64 = 0;
        var total_stolen: u64 = 0;
        var total_parked: u64 = 0;

        for (self.workers) |*worker| {
            total_polled += worker.stats.tasks_polled;
            total_stolen += worker.stats.tasks_stolen;
            total_parked += worker.stats.times_parked;
        }

        return .{
            .total_spawned = self.total_spawned.load(.acquire),
            .total_polled = total_polled,
            .total_stolen = total_stolen,
            .total_parked = total_parked,
            .num_workers = self.workers.len,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Timer Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Poll and process expired timers (thread-safe)
    /// Returns number of timers processed
    pub fn pollTimers(self: *Self) usize {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();

        const expired = self.timer_wheel.poll();
        const count = timer_mod.processExpiredWithScheduler(&self.timer_wheel, expired, self);

        if (count > 0) {
            _ = self.timers_processed.fetchAdd(count, .monotonic);
        }

        return count;
    }

    /// Get time until next timer expires (for park timeout)
    pub fn nextTimerExpiration(self: *Self) ?u64 {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return self.timer_wheel.nextExpiration();
    }

    /// Schedule a sleep for a task (returns timer entry for cancellation)
    /// The task will be woken when the timer expires
    pub fn scheduleSleep(self: *Self, duration_ns: u64, task: *Header) !*TimerEntry {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return timer_mod.sleep(&self.timer_wheel, duration_ns, task);
    }

    /// Schedule a deadline for a task
    pub fn scheduleDeadline(self: *Self, deadline_ns: u64, task: *Header) !*TimerEntry {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return timer_mod.deadline(&self.timer_wheel, deadline_ns, task);
    }

    /// Schedule a timer with a custom waker callback
    pub fn scheduleTimerWithWaker(
        self: *Self,
        duration_ns: u64,
        ctx: *anyopaque,
        waker: timer_mod.WakerFn,
    ) !*TimerEntry {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return timer_mod.sleepWithWaker(&self.timer_wheel, duration_ns, ctx, waker);
    }

    /// Cancel a timer
    pub fn cancelTimer(self: *Self, entry: *TimerEntry) void {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        self.timer_wheel.remove(entry);
    }

    /// Get current monotonic time from timer wheel
    pub fn now(self: *Self) u64 {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return self.timer_wheel.now();
    }

    /// Get number of active timers
    pub fn activeTimers(self: *Self) usize {
        self.timer_mutex.lock();
        defer self.timer_mutex.unlock();
        return self.timer_wheel.len();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // I/O Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Poll I/O completions (thread-safe, non-blocking)
    /// Returns number of completions processed
    pub fn pollIo(self: *Self) usize {
        // Try to acquire the I/O mutex (non-blocking)
        if (!self.io_mutex.tryLock()) {
            return 0; // Another worker is polling
        }
        defer self.io_mutex.unlock();

        // Poll for completions (non-blocking, timeout = 0)
        const count = self.backend.wait(self.completions, 0) catch return 0;

        // Process completions
        for (self.completions[0..count]) |completion| {
            self.processCompletion(completion);
        }

        if (count > 0) {
            _ = self.io_completions_processed.fetchAdd(count, .monotonic);
        }

        return count;
    }

    /// Poll I/O completions with timeout (for parking)
    /// Only call this when worker is parking (will block)
    pub fn pollIoBlocking(self: *Self, timeout_ns: u64) usize {
        self.io_mutex.lock();
        defer self.io_mutex.unlock();

        const timeout_ms: u32 = @intCast(@min(timeout_ns / std.time.ns_per_ms, std.math.maxInt(u32)));
        const count = self.backend.wait(self.completions, timeout_ms) catch return 0;

        // Process completions
        for (self.completions[0..count]) |completion| {
            self.processCompletion(completion);
        }

        if (count > 0) {
            _ = self.io_completions_processed.fetchAdd(count, .monotonic);
        }

        return count;
    }

    /// Process a single I/O completion (wake associated task)
    fn processCompletion(self: *Self, completion: Completion) void {
        // User data contains the task header pointer
        if (completion.user_data != 0) {
            const task: *Header = @ptrFromInt(completion.user_data);

            // Store the I/O result in the task for later retrieval
            // (This assumes tasks have a way to receive I/O results)

            // Wake the task by scheduling it
            if (task.transitionToScheduled()) {
                task.ref();
                self.global_queue.push(task);
                self.wakeWorkerIfNeeded();
            }
        }
    }

    /// Submit an I/O operation (returns submission ID for tracking)
    pub fn submitIo(self: *Self, op: Operation) !backend_mod.SubmissionId {
        self.io_mutex.lock();
        defer self.io_mutex.unlock();
        return self.backend.submit(op);
    }

    /// Submit multiple I/O operations
    pub fn submitIoBatch(self: *Self, ops: []const Operation) !void {
        self.io_mutex.lock();
        defer self.io_mutex.unlock();
        for (ops) |op| {
            _ = try self.backend.submit(op);
        }
    }

    /// Get active I/O backend type (for diagnostics)
    pub fn getBackendType(self: *const Self) BackendType {
        return @as(BackendType, self.backend);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Thread-local Context
// ═══════════════════════════════════════════════════════════════════════════════

threadlocal var current_worker_idx: ?usize = null;
threadlocal var current_scheduler: ?*Scheduler = null;
threadlocal var current_header: ?*Header = null;

pub fn getCurrentWorkerIndex() ?usize {
    return current_worker_idx;
}

pub fn getCurrentScheduler() ?*Scheduler {
    return current_scheduler;
}

/// Get the currently executing task header (if running inside a task).
/// This is used by sleep() and other async primitives to register with
/// the timer wheel and set the waiting flag.
pub fn getCurrentHeader() ?*Header {
    return current_header;
}

pub fn setCurrentContext(scheduler: *Scheduler, worker_idx: usize) void {
    current_scheduler = scheduler;
    current_worker_idx = worker_idx;
}

pub fn setCurrentHeader(header: ?*Header) void {
    current_header = header;
}

pub fn clearCurrentContext() void {
    current_scheduler = null;
    current_worker_idx = null;
    current_header = null;
}

/// Try work-stealing join. Returns true if on a worker thread (helped),
/// false otherwise (caller should fall back to spin-wait).
pub fn helpUntilComplete(target: *Header) bool {
    const worker_idx = getCurrentWorkerIndex() orelse return false;
    const scheduler = getCurrentScheduler() orelse return false;
    if (worker_idx >= scheduler.workers.len) return false;
    scheduler.workers[worker_idx].helpUntilComplete(target);
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Scheduler - create and destroy" {
    const allocator = std.testing.allocator;

    const sched = try Scheduler.init(allocator, .{ .num_workers = 2 });
    defer sched.deinit();

    try std.testing.expectEqual(@as(usize, 2), sched.workers.len);
}

test "Scheduler - spawn task" {
    const allocator = std.testing.allocator;

    const sched = try Scheduler.init(allocator, .{ .num_workers = 1 });
    defer sched.deinit();

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .complete;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var header = Header.init(&vtable);
    sched.spawn(&header);

    try std.testing.expectEqual(@as(u64, 1), sched.total_spawned.load(.acquire));
}

test "Worker - LIFO slot with transition check" {
    const allocator = std.testing.allocator;

    const sched = try Scheduler.init(allocator, .{ .num_workers = 1 });
    defer sched.deinit();

    var worker = &sched.workers[0];

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .complete;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var h1 = Header.init(&vtable);

    // Should succeed when not transitioning
    try std.testing.expect(worker.tryScheduleLocal(&h1));
    // New task always goes to LIFO (eviction pattern)
    try std.testing.expectEqual(&h1, worker.lifo_slot.load(.acquire));

    var h2 = Header.init(&vtable);

    // Second push should evict h1 to run_queue, h2 goes to LIFO
    try std.testing.expect(worker.tryScheduleLocal(&h2));
    try std.testing.expectEqual(&h2, worker.lifo_slot.load(.acquire));
    // h1 should now be in the run_queue
    try std.testing.expect(!worker.run_queue.isEmpty());

    // Set transitioning flag
    worker.transitioning_to_park.store(true, .release);

    var h3 = Header.init(&vtable);

    // Should fail when transitioning
    try std.testing.expect(!worker.tryScheduleLocal(&h3));

    worker.transitioning_to_park.store(false, .release);
}

test "Worker - LIFO eviction pattern" {
    const allocator = std.testing.allocator;

    const sched = try Scheduler.init(allocator, .{ .num_workers = 1 });
    defer sched.deinit();

    var worker = &sched.workers[0];

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .complete;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var h1 = Header.init(&vtable);
    var h2 = Header.init(&vtable);
    var h3 = Header.init(&vtable);

    // Push h1 -> LIFO = h1
    try std.testing.expect(worker.tryScheduleLocal(&h1));
    try std.testing.expectEqual(&h1, worker.lifo_slot.load(.acquire));

    // Push h2 -> evicts h1 to run_queue, LIFO = h2
    try std.testing.expect(worker.tryScheduleLocal(&h2));
    try std.testing.expectEqual(&h2, worker.lifo_slot.load(.acquire));

    // Push h3 -> evicts h2 to run_queue, LIFO = h3
    try std.testing.expect(worker.tryScheduleLocal(&h3));
    try std.testing.expectEqual(&h3, worker.lifo_slot.load(.acquire));

    // LIFO pop gets h3 (newest/cache-hot)
    try std.testing.expectEqual(&h3, worker.lifo_slot.swap(null, .acquire));
}

test "FastRand - produces values" {
    var rng = FastRand.init(12345);
    const v1 = rng.fastrand();
    const v2 = rng.fastrand();
    // Values should differ (not stuck)
    try std.testing.expect(v1 != v2);
}

test "FastRand - fastrand_n bounds" {
    var rng = FastRand.init(67890);
    for (0..100) |_| {
        const val = rng.fastrand_n(10);
        try std.testing.expect(val < 10);
    }
}

test "Scheduler - start and shutdown" {
    const allocator = std.testing.allocator;

    const sched = try Scheduler.init(allocator, .{ .num_workers = 2 });
    defer sched.deinit();

    try sched.startAndWait();

    for (sched.workers) |*worker| {
        try std.testing.expect(worker.running.load(.acquire));
    }

    sched.shutdownAndWait();

    for (sched.workers) |*worker| {
        try std.testing.expect(!worker.running.load(.acquire));
    }
}

test "IdleState - bitmap operations" {
    var idle = IdleState.init(4);

    // Initially no workers parked
    try std.testing.expectEqual(@as(u32, 0), idle.getParkedCount());

    // Park worker 2 (simulates transitionWorkerToParked)
    _ = idle.transitionWorkerToParked(2, false);
    try std.testing.expectEqual(@as(u32, 1), idle.getParkedCount());
    try std.testing.expect(idle.isWorkerParked(2));

    // Park worker 0 (lower ID - should be claimed first due to @ctz)
    _ = idle.transitionWorkerToParked(0, false);
    try std.testing.expectEqual(@as(u32, 2), idle.getParkedCount());

    // Claim a worker — should get worker 0 (lowest bit)
    const claimed = idle.claimParkedWorker();
    try std.testing.expectEqual(@as(usize, 0), claimed.?);
    try std.testing.expectEqual(@as(u32, 1), idle.getParkedCount());
    try std.testing.expectEqual(@as(u32, 1), idle.num_searching.load(.seq_cst));
    try std.testing.expect(!idle.isWorkerParked(0));
    try std.testing.expect(idle.isWorkerParked(2));

    // Claim another — should get worker 2
    const claimed2 = idle.claimParkedWorker();
    try std.testing.expectEqual(@as(usize, 2), claimed2.?);
    try std.testing.expectEqual(@as(u32, 0), idle.getParkedCount());
    try std.testing.expectEqual(@as(u32, 2), idle.num_searching.load(.seq_cst));

    // No more parked workers
    try std.testing.expect(idle.claimParkedWorker() == null);
}

test "IdleState - last searcher protocol" {
    var idle = IdleState.init(4);

    // Park two workers
    _ = idle.transitionWorkerToParked(0, false);
    _ = idle.transitionWorkerToParked(1, false);

    // Claim worker 0 (enters searching: num_searching=1)
    _ = idle.claimParkedWorker();
    try std.testing.expectEqual(@as(u32, 1), idle.num_searching.load(.seq_cst));

    // Worker 0 finds work → transition from searching
    // NOT the last searcher yet (because there are no other searchers... wait)
    // Actually with num_searching=1, this IS the last searcher
    const was_last = idle.transitionWorkerFromSearching();
    try std.testing.expect(was_last); // Was the last searcher
    try std.testing.expectEqual(@as(u32, 0), idle.num_searching.load(.seq_cst));
}

test "IdleState - parking with searching state" {
    var idle = IdleState.init(4);

    // Worker parks while searching — should decrement num_searching
    _ = idle.num_searching.fetchAdd(1, .seq_cst); // Simulate being a searcher
    const is_last = idle.transitionWorkerToParked(0, true);
    try std.testing.expect(is_last); // Was the last searcher
    try std.testing.expectEqual(@as(u32, 0), idle.num_searching.load(.seq_cst));
    try std.testing.expectEqual(@as(u32, 1), idle.getParkedCount());
}

test "IdleState - searcher limiting" {
    var idle = IdleState.init(4);

    // Max ~50% searchers (2 out of 4)
    try std.testing.expect(idle.tryBecomeSearcher(4));
    try std.testing.expect(idle.tryBecomeSearcher(4));
    try std.testing.expect(!idle.tryBecomeSearcher(4)); // 2*2 >= 4, should fail

    // Transition one from searching
    _ = idle.transitionWorkerFromSearching();
    try std.testing.expect(idle.tryBecomeSearcher(4)); // Should work now
}

test "Config - auto workers" {
    const config = Config{};
    const num = config.getNumWorkers();
    try std.testing.expect(num >= 1);
}

test "Cooperative budget constants" {
    try std.testing.expectEqual(@as(u32, 128), BUDGET_PER_TICK);
    try std.testing.expectEqual(@as(u8, 3), MAX_LIFO_POLLS_PER_TICK);
    try std.testing.expectEqual(@as(usize, 64), MAX_GLOBAL_QUEUE_BATCH_SIZE);
}

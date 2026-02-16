//! Executor Module
//!
//! The executor is responsible for running async tasks to completion.
//! It manages:
//! - Worker threads for parallel task execution
//! - Task queues (local and global)
//! - Work stealing for load balancing
//! - Timer wheel for sleep/timeout operations
//! - Integration with I/O backends
//!
//! This module re-exports types from the scheduler subsystem, which provides
//! production-grade optimizations:
//! - Cooperative budgeting (128 polls per tick)
//! - LIFO poll caps (prevents starvation)
//! - Batch global queue operations (amortized lock cost)
//! - Searcher limiting (~50% of workers)
//! - Parked worker bitmap (O(1) wake via @ctz)
//! - Adaptive global queue interval (EWMA-tuned)
//! - Hierarchical timer wheel (O(1) insert/expire)
//! - Shield-based cancellation (deferred cancel in critical sections)
//!
//! ## Quick Start
//!
//! ```zig
//! const blitz_io = @import("volt");
//!
//! pub fn main() !void {
//!     var runtime = try blitz_io.Runtime.init(allocator, .{});
//!     defer runtime.deinit();
//!
//!     _ = try runtime.run(myAsyncMain, .{});
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

// ─────────────────────────────────────────────────────────────────────────────
// Re-exports from Coroutine System
// ─────────────────────────────────────────────────────────────────────────────

const coro_scheduler = @import("scheduler/Scheduler.zig");
const coro_header = @import("scheduler/Header.zig");
const coro_timer = @import("scheduler/TimerWheel.zig");
const coro_pool = @import("util/pool.zig");

const builtin = @import("builtin");

/// Task header - the core task type visible to the scheduler
pub const Header = coro_header.Header;

/// Task state machine
pub const State = coro_header.State;

/// Waker - handle to wake a task when I/O is ready.
/// This is a lightweight handle that can be cloned and stored.
/// IMPORTANT: Wakers are single-use. Calling wake() twice is a bug.
pub const Waker = struct {
    /// Pointer to the task header
    header: *Header,

    /// Debug flag to detect double-use (only in debug builds)
    consumed: if (builtin.mode == .Debug) bool else void,

    /// Create a waker for a task
    pub fn init(header: *Header) Waker {
        header.ref(); // Waker holds a reference
        return .{
            .header = header,
            .consumed = if (builtin.mode == .Debug) false else {},
        };
    }

    /// Clone the waker (increments ref count)
    pub fn clone(self: Waker) Waker {
        if (builtin.mode == .Debug) {
            std.debug.assert(!self.consumed); // Cannot clone consumed waker
        }
        self.header.ref();
        return .{
            .header = self.header,
            .consumed = if (builtin.mode == .Debug) false else {},
        };
    }

    /// Wake the task (schedule it for polling)
    /// Consumes the waker - do not use after calling this
    pub fn wake(self: *Waker) void {
        if (builtin.mode == .Debug) {
            std.debug.assert(!self.consumed); // Double wake detected!
            self.consumed = true;
        }

        if (self.header.transitionToScheduled()) {
            self.header.schedule();
        }
        // Drop our reference
        _ = self.header.unref();
        self.header = undefined;
    }

    /// Wake the task by ref (for use with optional pointers)
    /// Consumes the waker
    pub fn wakeByRef(self: *Waker) void {
        self.wake();
    }

    /// Drop the waker without waking
    /// Use this if you no longer need the waker
    pub fn drop(self: *Waker) void {
        if (builtin.mode == .Debug) {
            std.debug.assert(!self.consumed); // Double drop detected!
            self.consumed = true;
        }
        _ = self.header.unref();
        self.header = undefined;
    }

    /// Check if waker is still valid (not consumed)
    pub fn isValid(self: *const Waker) bool {
        if (builtin.mode == .Debug) {
            return !self.consumed;
        }
        return true;
    }
};

/// Lifecycle states
pub const Lifecycle = coro_header.Lifecycle;

/// Task queue (intrusive linked list)
pub const TaskQueue = coro_header.TaskQueue;

/// Global task queue
pub const GlobalTaskQueue = coro_header.GlobalTaskQueue;

/// Lock-free work-stealing queue (256-slot ring buffer)
pub const WorkStealQueue = coro_header.WorkStealQueue;

/// Scheduler configuration
pub const SchedulerConfig = coro_scheduler.Config;

/// The main scheduler
pub const Scheduler = coro_scheduler.Scheduler;

/// Worker thread
pub const Worker = coro_scheduler.Worker;

/// Idle state coordination
pub const IdleState = coro_scheduler.IdleState;

/// Timer wheel for efficient timeout handling
pub const TimerWheel = coro_timer.TimerWheel;

/// Timer entry
pub const TimerEntry = coro_timer.TimerEntry;

/// Lock-free object pool for O(1) allocation
pub const ObjectPool = coro_pool.ObjectPool;

// I/O Backend types (re-exported from scheduler)
pub const Backend = coro_scheduler.Backend;
pub const BackendType = coro_scheduler.BackendType;
pub const BackendConfig = coro_scheduler.BackendConfig;
pub const Completion = coro_scheduler.Completion;
pub const Operation = coro_scheduler.Operation;

// Backend detection
const backend_mod = @import("backend.zig");
pub const detectBestBackend = backend_mod.detectBestBackend;

// Thread-local context
pub const getCurrentWorkerIndex = coro_scheduler.getCurrentWorkerIndex;
pub const getCurrentScheduler = coro_scheduler.getCurrentScheduler;

// Timer operations
pub const timer = coro_timer;

// ─────────────────────────────────────────────────────────────────────────────
// Executor - High-Level Interface (Backward Compatibility)
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the executor
pub const Config = struct {
    /// Number of worker threads (null = auto-detect based on CPU cores)
    num_workers: ?usize = null,

    /// Enable work stealing
    enable_stealing: bool = true,

    /// I/O backend type (null = auto-detect)
    backend_type: ?BackendType = null,

    /// Maximum I/O completions per tick
    max_io_completions: u32 = 256,
};

/// The main executor - wraps the coroutine scheduler
pub const Executor = struct {
    const Self = @This();

    /// Memory allocator
    allocator: Allocator,

    /// Coroutine scheduler
    scheduler: *Scheduler,

    /// Configuration
    config: Config,

    /// Create a new executor
    pub fn init(allocator: Allocator, config: Config) !Self {
        const sched_config = SchedulerConfig{
            .num_workers = config.num_workers orelse 0,
            .enable_stealing = config.enable_stealing,
            .backend_type = config.backend_type,
            .max_io_completions = config.max_io_completions,
        };

        const sched = try Scheduler.init(allocator, sched_config);

        return Self{
            .allocator = allocator,
            .scheduler = sched,
            .config = config,
        };
    }

    /// Clean up executor resources
    pub fn deinit(self: *Self) void {
        self.scheduler.deinit();
    }

    /// Start the executor
    pub fn start(self: *Self) !void {
        try self.scheduler.start();
    }

    /// Wait for workers to be ready
    pub fn waitForReady(self: *Self) void {
        self.scheduler.waitForWorkersReady();
    }

    /// Start and wait for workers to be ready
    pub fn startAndWait(self: *Self) !void {
        try self.scheduler.startAndWait();
    }

    /// Signal shutdown and wait for completion
    pub fn shutdown(self: *Self) void {
        self.scheduler.shutdownAndWait();
    }

    /// Spawn a task
    pub fn spawn(self: *Self, task_header: *Header) void {
        self.scheduler.spawn(task_header);
    }

    /// Get the number of worker threads
    pub fn numWorkers(self: *const Self) usize {
        return self.scheduler.workers.len;
    }

    /// Get executor metrics
    pub fn getMetrics(self: *const Self) Metrics {
        const sched_stats = self.scheduler.getStats();
        return Metrics{
            .num_workers = sched_stats.num_workers,
            .total_spawned = sched_stats.total_spawned,
            .total_polled = sched_stats.total_polled,
            .total_stolen = sched_stats.total_stolen,
            .total_parked = sched_stats.total_parked,
            .active_timers = self.scheduler.activeTimers(),
        };
    }

    pub const Metrics = struct {
        num_workers: usize,
        total_spawned: u64,
        total_polled: u64,
        total_stolen: u64,
        total_parked: u64,
        active_timers: usize,
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Legacy type aliases
// ─────────────────────────────────────────────────────────────────────────────

/// PollResult from the future system
pub const PollResult = @import("../future/Poll.zig").PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "executor - new coroutine types compile" {
    _ = Scheduler;
    _ = Worker;
    _ = Header;
    _ = State;
    _ = TaskQueue;
    _ = GlobalTaskQueue;
    _ = TimerWheel;
    _ = TimerEntry;
    _ = ObjectPool;
    _ = IdleState;
}

test "executor - Executor init and deinit" {
    var exec = try Executor.init(std.testing.allocator, .{
        .num_workers = 1,
    });
    defer exec.deinit();

    try std.testing.expectEqual(@as(usize, 1), exec.numWorkers());
}

test "executor - coroutine types compile" {
    // Verify all coroutine-based types compile
    _ = PollResult;
}

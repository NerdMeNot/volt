//! # Blitz-IO Runtime
//!
//! The core runtime that combines:
//! - Stackless task scheduler (work-stealing with production optimizations)
//! - I/O driver (io_uring, kqueue, epoll, IOCP)
//! - Blocking pool (for CPU-intensive work)
//! - Hierarchical timer wheel
//!
//! ## Stackless Architecture
//!
//! This runtime uses stackless futures (~256-512 bytes per task) instead of
//! stackful coroutines (16-64KB per task). This enables millions of concurrent
//! tasks instead of thousands.
//!
//! ## Usage
//!
//! ```zig
//! var rt = try Runtime.init(allocator, .{});
//! defer rt.deinit();
//!
//! // Spawn a Future-based task
//! var handle = try rt.spawn(MyFuture, my_future);
//! _ = handle.join();  // Wait from outside async context
//! ```

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

// Internal scheduler system (stackless futures)
const sched = struct {
    const runtime_mod = @import("internal/scheduler/Runtime.zig");
    pub const SchedulerRuntime = runtime_mod.Runtime;
    pub const SchedulerConfig = runtime_mod.Config;
    pub const SchedulerJoinHandle = runtime_mod.JoinHandle;
    pub const Backend = runtime_mod.Backend;
    pub const BackendType = runtime_mod.BackendType;

    pub const Scheduler = @import("internal/scheduler/Scheduler.zig").Scheduler;
    pub const Header = @import("internal/scheduler/Header.zig").Header;
};

// Blocking pool for CPU-intensive work
const blocking_mod = @import("internal/blocking.zig");
const BlockingPool = blocking_mod.BlockingPool;

// Time types
const time_mod = @import("time.zig");
pub const Duration = time_mod.Duration;
pub const Instant = time_mod.Instant;

// FnFuture for run() method
const fn_future_mod = @import("future/FnFuture.zig");
const FnFuture = fn_future_mod.FnFuture;
const FnPayload = fn_future_mod.FnPayload;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime configuration.
pub const Config = struct {
    /// Number of I/O worker threads (0 = auto based on CPU count).
    num_workers: usize = 0,

    /// Maximum blocking pool threads (default: 512).
    max_blocking_threads: usize = 512,

    /// Blocking thread idle timeout in nanoseconds (default: 10 seconds).
    blocking_keep_alive_ns: u64 = 10 * std.time.ns_per_s,

    /// I/O backend type (null = auto-detect).
    backend: ?sched.BackendType = null,
};

// ─────────────────────────────────────────────────────────────────────────────
// Runtime
// ─────────────────────────────────────────────────────────────────────────────

/// The async I/O runtime.
/// Powered by a stackless scheduler with production-grade optimizations.
pub const Runtime = struct {
    const Self = @This();

    allocator: Allocator,
    sched_runtime: *sched.SchedulerRuntime,
    blocking_pool: BlockingPool,
    shutdown: std.atomic.Value(bool),

    /// Initialize the runtime.
    pub fn init(allocator: Allocator, config: Config) !Self {
        // Initialize scheduler runtime
        const sched_config = sched.SchedulerConfig{
            .num_workers = config.num_workers,
            .enable_stealing = true,
            .backend_type = config.backend,
        };
        const sched_runtime = try sched.SchedulerRuntime.init(allocator, sched_config);
        errdefer sched_runtime.deinit();

        // Initialize blocking pool (threads spawned on demand)
        const blocking_pool = BlockingPool.init(allocator, .{
            .thread_cap = config.max_blocking_threads,
            .keep_alive_ns = config.blocking_keep_alive_ns,
        });

        return Self{
            .allocator = allocator,
            .sched_runtime = sched_runtime,
            .blocking_pool = blocking_pool,
            .shutdown = std.atomic.Value(bool).init(false),
        };
    }

    /// Clean up all resources.
    pub fn deinit(self: *Self) void {
        self.shutdown.store(true, .release);
        self.blocking_pool.deinit();
        self.sched_runtime.deinit();
    }

    /// Spawn a Future onto the async runtime.
    ///
    /// This is the core async primitive. The future will be polled by the
    /// work-stealing scheduler until completion.
    ///
    /// Example:
    /// ```zig
    /// const handle = try rt.spawn(LockFuture, mutex.lock());
    /// _ = handle.join();
    /// defer mutex.unlock();
    /// ```
    pub fn spawn(
        self: *Self,
        comptime F: type,
        future: F,
    ) !sched.SchedulerJoinHandle(F.Output) {
        return self.sched_runtime.spawn(F, future);
    }

    /// Run a function on the runtime, blocking until complete.
    ///
    /// The function receives `volt.Io` as its first parameter, providing
    /// explicit access to the runtime for spawning tasks.
    ///
    /// Prefer using `Io.init` / `io.run()` directly — this method exists
    /// for backward compatibility and internal use.
    ///
    /// Example:
    /// ```zig
    /// const volt = @import("volt");
    /// var io = try volt.Io.init(allocator, .{});
    /// defer io.deinit();
    /// try io.run(myServer);
    /// ```
    pub fn run(
        self: *Self,
        comptime func: anytype,
    ) anyerror!FnPayload(@TypeOf(func)) {
        const IoType = @import("Io.zig");
        const io_handle = IoType{ .runtime = self };
        return self.runWithIo(io_handle, func);
    }

    /// Run with a pre-created Io handle. Used by `Io.run()`.
    pub fn runWithIo(
        self: *Self,
        io_handle: @import("Io.zig"),
        comptime func: anytype,
    ) anyerror!FnPayload(@TypeOf(func)) {
        const io_args = .{io_handle};
        const FnFut = FnFuture(func, @TypeOf(io_args));
        var handle = try self.sched_runtime.spawn(FnFut, FnFut.init(io_args));
        const result = handle.join();
        if (comptime FnFut.Output == void) {
            return;
        } else {
            return result;
        }
    }

    /// Spawn a blocking function on the blocking thread pool.
    ///
    /// Use this for CPU-intensive or blocking I/O work that would starve
    /// the async I/O workers. Returns a handle that can be waited on.
    ///
    /// Example:
    /// ```zig
    /// const handle = try rt.spawnBlocking(computeHash, .{data});
    /// const hash = try handle.wait();
    /// ```
    pub fn spawnBlocking(
        self: *Self,
        comptime func: anytype,
        args: anytype,
    ) !*blocking_mod.BlockingHandle(blocking_mod.ResultType(@TypeOf(func))) {
        return blocking_mod.runBlocking(&self.blocking_pool, func, args);
    }

    /// Get the blocking pool for CPU-intensive work.
    pub fn getBlockingPool(self: *Self) *BlockingPool {
        return &self.blocking_pool;
    }

    /// Get the underlying scheduler.
    pub fn getScheduler(self: *Self) *sched.Scheduler {
        return self.sched_runtime.scheduler;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Re-exports for convenience
// ─────────────────────────────────────────────────────────────────────────────

/// JoinHandle for spawned futures.
pub const JoinHandle = sched.SchedulerJoinHandle;

// ─────────────────────────────────────────────────────────────────────────────
// Thread-Local Runtime Access
// ─────────────────────────────────────────────────────────────────────────────

/// Thread-local runtime reference.
threadlocal var current_runtime: ?*Runtime = null;

/// Get the current runtime (if running inside one).
pub fn getRuntime() ?*Runtime {
    return current_runtime;
}

/// Get the current runtime or panic.
pub fn runtime() *Runtime {
    return current_runtime orelse @panic("Not running inside a volt runtime");
}

/// Set the current runtime for this thread.
pub fn setCurrentRuntime(rt_ptr: *anyopaque) void {
    current_runtime = @ptrCast(@alignCast(rt_ptr));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Runtime - init and deinit" {
    var rt = try Runtime.init(std.testing.allocator, .{
        .num_workers = 1,
    });
    defer rt.deinit();

    // Blocking pool threads start lazily (spawned on first use)
    try std.testing.expectEqual(@as(usize, 0), rt.getBlockingPool().threadCount());
}

test "Runtime - getScheduler returns scheduler" {
    var rt = try Runtime.init(std.testing.allocator, .{
        .num_workers = 1,
    });
    defer rt.deinit();

    const scheduler = rt.getScheduler();
    try std.testing.expect(@intFromPtr(scheduler) != 0);
}

test "Runtime - getRuntime outside runtime returns null" {
    try std.testing.expectEqual(@as(?*Runtime, null), getRuntime());
}

test "Runtime - Config defaults" {
    const config = Config{};

    try std.testing.expectEqual(@as(usize, 0), config.num_workers);
    try std.testing.expectEqual(@as(usize, 512), config.max_blocking_threads);
    try std.testing.expectEqual(@as(?sched.BackendType, null), config.backend);
}

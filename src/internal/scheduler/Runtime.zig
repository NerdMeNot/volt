//! Runtime - High-Level Async Runtime
//!
//! The Runtime is the top-level entry point for volt. It manages:
//! - The scheduler with worker threads
//! - I/O driver integration
//! - Timer management
//! - Blocking pool for CPU-intensive work
//!
//! ## Async-First Design
//!
//! This runtime is async by default. The primary way to spawn work is via
//! `spawn()` which takes a Future and properly polls it, yielding when pending.
//!
//! For blocking/CPU-intensive work, use `spawnBlocking()` which offloads to
//! a dedicated thread pool.
//!
//! ## Stackless Architecture
//!
//! This runtime uses stackless futures (~256-512 bytes per task) instead of
//! stackful coroutines (16-64KB per task). This enables millions of concurrent
//! tasks instead of thousands.
//!
//! Usage:
//! ```zig
//! const rt = try Runtime.init(allocator, .{});
//! defer rt.deinit();
//!
//! // ASYNC (default): Spawn a Future-based task
//! var handle = try rt.spawn(LockFuture, mutex.lock());
//! // JoinHandle IS a Future - poll it to await completion
//! while (handle.poll(&ctx).isPending()) { ... }
//! const result = handle.poll(&ctx).ready;
//!
//! // BLOCKING (explicit, from outside runtime only):
//! const result = handle.join();
//!
//! // BLOCKING POOL: CPU-intensive work
//! var blocking_handle = try rt.spawnBlocking(heavyComputation, .{data});
//! const result = try blocking_handle.wait();
//! ```
//!

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const scheduler_mod = @import("Scheduler.zig");
const Scheduler = scheduler_mod.Scheduler;
const Worker = scheduler_mod.Worker;

// Re-export I/O types for convenience
pub const Backend = scheduler_mod.Backend;
pub const BackendType = scheduler_mod.BackendType;
pub const BackendConfig = scheduler_mod.BackendConfig;
pub const Operation = scheduler_mod.Operation;
pub const Completion = scheduler_mod.Completion;

const header_mod = @import("Header.zig");
const Header = header_mod.Header;
const State = header_mod.State;

// Blocking pool for CPU-intensive work
const blocking_mod = @import("../blocking.zig");
pub const BlockingPool = blocking_mod.BlockingPool;
pub const BlockingHandle = blocking_mod.BlockingHandle;

// Future types for async operations
const future_mod = @import("../../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

pub const Config = struct {
    /// Number of worker threads (0 = auto based on CPU count)
    num_workers: usize = 0,

    /// Enable work stealing between workers
    enable_stealing: bool = true,

    /// I/O backend type (null = auto-detect best for platform)
    backend_type: ?scheduler_mod.BackendType = null,

    /// Maximum I/O completions to process per tick
    max_io_completions: u32 = 256,

    /// Maximum threads in the blocking pool (default: 512)
    max_blocking_threads: usize = 512,

    /// Idle timeout for blocking threads in nanoseconds (default: 10s)
    blocking_keep_alive_ns: u64 = 10 * std.time.ns_per_s,

    pub fn toSchedulerConfig(self: Config) scheduler_mod.Config {
        return .{
            .num_workers = self.num_workers,
            .enable_stealing = self.enable_stealing,
            .backend_type = self.backend_type,
            .max_io_completions = self.max_io_completions,
        };
    }

    pub fn toBlockingConfig(self: Config) blocking_mod.Config {
        return .{
            .thread_cap = self.max_blocking_threads,
            .keep_alive_ns = self.blocking_keep_alive_ns,
        };
    }
};

pub const default_config = Config{};

// ═══════════════════════════════════════════════════════════════════════════════
// JoinHandle - Handle to a spawned async task
// ═══════════════════════════════════════════════════════════════════════════════

/// Handle to a spawned async task.
///
/// JoinHandle IS a Future - poll it to await the result asynchronously.
/// This is the async-first approach: compose Futures, don't block.
///
/// Example:
/// ```zig
/// var handle = try rt.spawn(MyFuture, my_future);
/// // Poll the handle like any Future
/// switch (handle.poll(&ctx)) {
///     .ready => |result| // task completed
///     .pending => // still running
/// }
/// ```
pub fn JoinHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Output type for Future trait.
        /// If T is already an error union (E!Payload), merges TaskCancelled
        /// into the error set to avoid nested error unions.
        pub const Output = blk: {
            if (T == void) break :blk void;
            if (@typeInfo(T) == .error_union) {
                const eu = @typeInfo(T).error_union;
                break :blk (eu.error_set || error{TaskCancelled})!eu.payload;
            }
            break :blk error{TaskCancelled}!T;
        };

        header: *Header,
        runtime: *Runtime,
        /// Pointer to the result storage (inside the FutureTask struct)
        result_ptr: *?T,
        /// Stored waker for notification
        stored_waker: ?Waker = null,

        // ═══════════════════════════════════════════════════════════════════
        // Future Implementation (Async - the default path)
        // ═══════════════════════════════════════════════════════════════════

        /// Poll for completion - implements Future trait.
        /// This is the async way to await a spawned task.
        pub fn poll(self: *Self, ctx: *Context) PollResult(Output) {
            if (self.header.isComplete()) {
                // Clean up waker
                if (self.stored_waker) |*w| {
                    w.deinit();
                    self.stored_waker = null;
                }

                // Get result
                if (T == void) {
                    if (self.header.unref()) {
                        self.header.drop();
                    }
                    return .{ .ready = {} };
                } else if (comptime @typeInfo(T) == .error_union) {
                    // T is E!Payload — unwrap optional, coerce error set
                    const result: ?T = self.result_ptr.*;
                    if (self.header.unref()) {
                        self.header.drop();
                    }
                    const r: T = result orelse return .{ .ready = error.TaskCancelled };
                    return .{ .ready = r };
                } else {
                    const result = self.result_ptr.*;
                    if (self.header.unref()) {
                        self.header.drop();
                    }
                    if (result) |r| {
                        return .{ .ready = r };
                    } else {
                        return .{ .ready = error.TaskCancelled };
                    }
                }
            }

            // Not complete - store/update waker
            // TODO: Register waker with Header for proper notification
            // For now, the scheduler will re-poll periodically
            const new_waker = ctx.getWaker();
            if (self.stored_waker) |*old| {
                if (!old.willWakeSame(new_waker)) {
                    old.deinit();
                    self.stored_waker = new_waker.clone();
                }
            } else {
                self.stored_waker = new_waker.clone();
            }

            return .pending;
        }

        // ═══════════════════════════════════════════════════════════════════
        // Non-blocking queries
        // ═══════════════════════════════════════════════════════════════════

        /// Check if the task is complete (non-blocking).
        pub fn isDone(self: *const Self) bool {
            return self.header.isComplete();
        }

        /// Request cancellation of the task.
        pub fn cancel(self: *Self) void {
            _ = self.header.cancel();
        }

        /// Detach the handle (task continues running, result is discarded).
        pub fn detach(self: *Self) void {
            if (self.stored_waker) |*w| {
                w.deinit();
                self.stored_waker = null;
            }
            if (self.header.unref()) {
                self.header.drop();
            }
            self.* = undefined;
        }

        /// Clean up resources
        pub fn deinit(self: *Self) void {
            if (self.stored_waker) |*w| {
                w.deinit();
                self.stored_waker = null;
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Blocking API (Explicit opt-in for sync contexts)
        // ═══════════════════════════════════════════════════════════════════

        /// Block the calling thread until the task completes.
        ///
        /// WARNING: This is a BLOCKING operation. Only use this from outside
        /// the async runtime (e.g., from main()). Inside the runtime, use
        /// poll() or compose with other Futures.
        pub fn join(self: *Self) Output {
            if (!self.header.isComplete()) {
                // If on a worker thread, help execute tasks while waiting.
                // The spawned task is typically in our LIFO slot — we execute
                // it directly instead of spinning.
                if (!scheduler_mod.helpUntilComplete(self.header)) {
                    // Not on a worker thread — help from the global queue
                    self.runtime.scheduler.blockOnComplete(self.header);
                }
            }

            // Get result
            if (T == void) {
                if (self.header.unref()) {
                    self.header.drop();
                }
                return {};
            } else if (comptime @typeInfo(T) == .error_union) {
                // T is E!Payload — unwrap optional, coerce error set
                const result: ?T = self.result_ptr.*;
                if (self.header.unref()) {
                    self.header.drop();
                }
                const r: T = result orelse return error.TaskCancelled;
                return r;
            } else {
                const result = self.result_ptr.*;
                if (self.header.unref()) {
                    self.header.drop();
                }
                if (result) |r| {
                    return r;
                } else {
                    return error.TaskCancelled;
                }
            }
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Runtime
// ═══════════════════════════════════════════════════════════════════════════════

/// The main async runtime.
///
/// Async-first design:
/// - `spawn()` is the default: spawns a Future-based async task
/// - `spawnBlocking()` is explicit: offloads blocking work to thread pool
pub const Runtime = struct {
    const Self = @This();

    /// Configuration
    config: Config,

    /// The scheduler (for async tasks)
    scheduler: *Scheduler,

    /// Blocking pool (for CPU-intensive/blocking work)
    blocking_pool: BlockingPool,

    /// Whether the runtime has been started
    started: bool = false,

    /// Allocator for runtime resources
    allocator: Allocator,

    /// Initialize a new runtime
    pub fn init(allocator: Allocator, config: Config) !*Self {
        const scheduler = try Scheduler.init(allocator, config.toSchedulerConfig());
        errdefer scheduler.deinit();

        const self = try allocator.create(Self);
        self.* = .{
            .config = config,
            .scheduler = scheduler,
            .blocking_pool = BlockingPool.init(allocator, config.toBlockingConfig()),
            .allocator = allocator,
        };

        return self;
    }

    /// Initialize with default configuration
    pub fn initDefault(allocator: Allocator) !*Self {
        return init(allocator, default_config);
    }

    /// Deinitialize the runtime
    pub fn deinit(self: *Self) void {
        // Shutdown blocking pool first
        self.blocking_pool.deinit();

        // Then shutdown scheduler
        if (self.started) {
            self.scheduler.shutdownAndWait();
        }
        self.scheduler.deinit();
        self.allocator.destroy(self);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /// Start the runtime (spawns worker threads)
    pub fn start(self: *Self) !void {
        if (self.started) return;
        try self.scheduler.start();
        self.started = true;
    }

    /// Shutdown the runtime and wait for all tasks to complete
    pub fn shutdown(self: *Self) void {
        if (!self.started) return;
        self.scheduler.shutdownAndWait();
        self.started = false;
    }

    /// Check if runtime is running
    pub fn isRunning(self: *const Self) bool {
        return self.started and self.scheduler.isRunning();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ASYNC Task Spawning (Default Path - Stay in Event Loop)
    // ─────────────────────────────────────────────────────────────────────────

    /// Spawn an async task (Future-based).
    ///
    /// This is THE way to spawn work in the runtime. The Future is polled,
    /// yielding when pending and resuming when its waker is called.
    ///
    /// Example:
    /// ```zig
    /// var handle = try rt.spawn(LockFuture, mutex.lock());
    /// try handle.join();
    /// defer mutex.unlock();
    /// ```
    pub fn spawn(
        self: *Self,
        comptime F: type,
        future: F,
    ) !JoinHandle(F.Output) {
        const FutureTaskType = header_mod.FutureTask(F);

        // Ensure runtime is started
        if (!self.started) {
            try self.start();
        }

        // Create the future task
        const task = try FutureTaskType.create(self.allocator, future);

        // Set up scheduler callbacks
        task.setScheduler(self.scheduler, scheduleCallback, rescheduleCallback);

        // Get pointer to result storage
        const result_ptr: *?F.Output = &task.result;

        // Spawn onto scheduler — use local queue when on a worker thread
        // for LIFO slot placement (enables work-stealing join optimization)
        if (scheduler_mod.getCurrentWorkerIndex()) |worker_idx| {
            self.scheduler.spawnFromWorker(worker_idx, &task.header);
        } else {
            self.scheduler.spawn(&task.header);
        }

        return .{
            .header = &task.header,
            .runtime = self,
            .result_ptr = result_ptr,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BLOCKING Task Spawning (Explicit Escape Hatch - Leave Event Loop)
    // ─────────────────────────────────────────────────────────────────────────

    /// Run blocking or CPU-intensive work on the blocking pool.
    ///
    /// Use this for:
    /// - CPU-intensive computation
    /// - FFI calls that may block
    /// - Legacy synchronous code
    /// - File system operations (until async fs is ready)
    ///
    /// Example:
    /// ```zig
    /// var handle = try rt.spawnBlocking(computeHash, .{large_data});
    /// const hash = try handle.wait();
    /// ```
    pub fn spawnBlocking(
        self: *Self,
        comptime func: anytype,
        args: anytype,
    ) !*BlockingHandle(ResultType(@TypeOf(func))) {
        return blocking_mod.runBlocking(&self.blocking_pool, func, args);
    }

    // Callback for schedule - called AFTER FutureTask.scheduleImpl does the transition.
    // Just enqueues the task - ref already incremented by scheduleImpl.
    //
    // LIFO optimization: if the waker fires on a worker thread, push to
    // that worker's LIFO slot for immediate execution. This avoids global queue
    // mutex contention and preserves cache locality (the woken task runs on the
    // same core that just released the resource it was waiting on).
    fn scheduleCallback(sched_ptr: *anyopaque, header: *Header) void {
        const sched: *Scheduler = @ptrCast(@alignCast(sched_ptr));
        if (scheduler_mod.getCurrentWorkerIndex()) |worker_idx| {
            if (worker_idx < sched.workers.len) {
                if (sched.workers[worker_idx].tryScheduleLocal(header)) {
                    return;
                }
            }
        }
        sched.enqueue(header);
    }

    // Callback for reschedule - called AFTER FutureTask.rescheduleImpl does the transition.
    // Just enqueues the task - no ref change needed.
    // Same LIFO optimization as scheduleCallback.
    fn rescheduleCallback(sched_ptr: *anyopaque, header: *Header) void {
        const sched: *Scheduler = @ptrCast(@alignCast(sched_ptr));
        if (scheduler_mod.getCurrentWorkerIndex()) |worker_idx| {
            if (worker_idx < sched.workers.len) {
                if (sched.workers[worker_idx].tryScheduleLocal(header)) {
                    return;
                }
            }
        }
        sched.enqueue(header);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // I/O Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Submit an I/O operation to the backend
    pub fn submitIo(self: *Self, op: Operation) !@import("../backend.zig").SubmissionId {
        return self.scheduler.submitIo(op);
    }

    /// Submit multiple I/O operations
    pub fn submitIoBatch(self: *Self, ops: []const Operation) !void {
        return self.scheduler.submitIoBatch(ops);
    }

    /// Get the active I/O backend type
    pub fn getBackendType(self: *const Self) BackendType {
        return self.scheduler.getBackendType();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Statistics
    // ─────────────────────────────────────────────────────────────────────────

    pub const Stats = struct {
        total_spawned: u64,
        num_workers: usize,
        io_completions: u64,
        timers_processed: u64,
        is_running: bool,
        blocking_threads: usize,
        blocking_idle: usize,
        blocking_pending: usize,
    };

    pub fn getStats(self: *const Self) Stats {
        return .{
            .total_spawned = self.scheduler.total_spawned.load(.acquire),
            .num_workers = self.scheduler.workers.len,
            .io_completions = self.scheduler.io_completions_processed.load(.acquire),
            .timers_processed = self.scheduler.timers_processed.load(.acquire),
            .is_running = self.isRunning(),
            .blocking_threads = self.blocking_pool.threadCount(),
            .blocking_idle = self.blocking_pool.idleCount(),
            .blocking_pending = self.blocking_pool.pendingTasks(),
        };
    }
};

/// Helper to get return type, unwrapping error unions.
fn ResultType(comptime Func: type) type {
    const info = @typeInfo(Func);
    if (info == .pointer) {
        return ResultType(info.pointer.child);
    }
    const ret = info.@"fn".return_type.?;
    if (@typeInfo(ret) == .error_union) {
        return @typeInfo(ret).error_union.payload;
    }
    return ret;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Convenience Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Thread-local runtime pointer
threadlocal var current_runtime: ?*Runtime = null;

/// Get the current runtime (if in a task context)
pub fn current() ?*Runtime {
    return current_runtime;
}

/// Set the current runtime (internal use)
pub fn setCurrent(rt: ?*Runtime) void {
    current_runtime = rt;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Runtime - create and destroy" {
    const allocator = std.testing.allocator;

    const rt = try Runtime.init(allocator, .{ .num_workers = 1 });
    defer rt.deinit();

    try std.testing.expect(!rt.started);
}

test "Runtime - start and shutdown" {
    const allocator = std.testing.allocator;

    const rt = try Runtime.init(allocator, .{ .num_workers = 2 });
    defer rt.deinit();

    try rt.start();
    // Wait for workers to be ready (proper synchronization, no arbitrary sleep)
    rt.scheduler.waitForWorkersReady();

    try std.testing.expect(rt.isRunning());

    rt.shutdown();
    try std.testing.expect(!rt.isRunning());
}

test "Runtime - spawnBlocking simple task" {
    const allocator = std.testing.allocator;

    const rt = try Runtime.init(allocator, .{ .num_workers = 1 });
    defer rt.deinit();

    const TestFn = struct {
        fn run() void {
            // Do nothing
        }
    };

    const handle = try rt.spawnBlocking(TestFn.run, .{});

    // Wait for completion
    try handle.wait();
}

test "Runtime - spawnBlocking with return value" {
    const allocator = std.testing.allocator;

    const rt = try Runtime.init(allocator, .{ .num_workers = 1 });
    defer rt.deinit();

    const TestFn = struct {
        fn compute(a: i32, b: i32) i32 {
            return a + b;
        }
    };

    const handle = try rt.spawnBlocking(TestFn.compute, .{ 10, 32 });
    const result = try handle.wait();

    try std.testing.expectEqual(@as(i32, 42), result);
}

test "Runtime - spawnBlocking multiple tasks" {
    const allocator = std.testing.allocator;

    const rt = try Runtime.init(allocator, .{ .num_workers = 2 });
    defer rt.deinit();

    var counter = std.atomic.Value(usize).init(0);

    const TestFn = struct {
        fn run(ctr: *std.atomic.Value(usize)) void {
            _ = ctr.fetchAdd(1, .acq_rel);
        }
    };

    const num_tasks = 10;
    var handles: [num_tasks]*BlockingHandle(void) = undefined;
    for (&handles) |*h| {
        h.* = try rt.spawnBlocking(TestFn.run, .{&counter});
    }

    for (handles) |h| {
        try h.wait();
    }

    try std.testing.expectEqual(@as(usize, num_tasks), counter.load(.acquire));
}

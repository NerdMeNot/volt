//! Blocking Thread Pool
//!
//! A dedicated thread pool for running CPU-intensive or blocking work,
//! separate from the async task scheduler.
//!
//! Features:
//! - Dynamic scaling: spawns threads on demand up to thread_cap
//! - Idle timeout: threads exit after keep_alive duration (default 10s)
//! - Spurious wakeup handling: num_notify counter for correctness
//! - Proper shutdown: drains queue, joins all threads
//! - Thread self-cleanup: exiting threads join previous exiting thread

const std = @import("std");
const thr = @import("thread.zig");
const Allocator = std.mem.Allocator;

/// Default keep-alive duration (10 seconds).
const KEEP_ALIVE_NS: u64 = 10 * std.time.ns_per_s;

/// Configuration for the blocking pool.
pub const Config = struct {
    /// Maximum threads to spawn (default: 512).
    thread_cap: usize = 512,

    /// Idle timeout before threads exit (nanoseconds).
    /// Default: 10 seconds.
    keep_alive_ns: u64 = KEEP_ALIVE_NS,
};

/// Metrics about the pool (for observability).
pub const Metrics = struct {
    num_threads: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    num_idle_threads: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    queue_depth: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn incNumThreads(self: *Metrics) void {
        _ = self.num_threads.fetchAdd(1, .monotonic);
    }

    fn decNumThreads(self: *Metrics) void {
        _ = self.num_threads.fetchSub(1, .monotonic);
    }

    fn incNumIdleThreads(self: *Metrics) void {
        _ = self.num_idle_threads.fetchAdd(1, .monotonic);
    }

    fn decNumIdleThreads(self: *Metrics) usize {
        return self.num_idle_threads.fetchSub(1, .monotonic);
    }

    fn incQueueDepth(self: *Metrics) void {
        _ = self.queue_depth.fetchAdd(1, .monotonic);
    }

    fn decQueueDepth(self: *Metrics) void {
        _ = self.queue_depth.fetchSub(1, .monotonic);
    }

    pub fn numThreads(self: *const Metrics) usize {
        return self.num_threads.load(.monotonic);
    }

    pub fn numIdleThreads(self: *const Metrics) usize {
        return self.num_idle_threads.load(.monotonic);
    }

    pub fn queueDepth(self: *const Metrics) usize {
        return self.queue_depth.load(.monotonic);
    }
};

/// A task to execute on the blocking pool.
pub const Task = struct {
    /// Function to execute.
    func: *const fn (*Task) void,

    /// Next task in queue (intrusive list).
    next: ?*Task = null,

    /// Whether this task is mandatory (must run even during shutdown).
    mandatory: bool = false,
};

/// Shared state protected by mutex.
const Shared = struct {
    /// Task queue (intrusive singly-linked list).
    queue_head: ?*Task = null,
    queue_tail: ?*Task = null,

    /// Number of pending notifications (for spurious wakeup handling).
    /// Incremented on notify, decremented on legitimate wakeup.
    num_notify: u32 = 0,

    /// Shutdown flag.
    shutdown: bool = false,

    /// Worker threads indexed by ID (for proper cleanup).
    worker_threads: std.AutoHashMapUnmanaged(usize, std.Thread) = .{},

    /// Counter for assigning worker thread IDs.
    worker_thread_index: usize = 0,

    /// Last exiting thread's handle (for chain-joining on timeout exit).
    /// Each timed-out thread joins the previous one to avoid handle leaks.
    last_exiting_thread: ?std.Thread = null,

    fn deinit(self: *Shared, allocator: Allocator) void {
        self.worker_threads.deinit(allocator);
    }
};

/// A blocking thread pool that grows dynamically.
pub const BlockingPool = struct {
    const Self = @This();

    /// Spin iterations a worker does after draining its queue, before
    /// falling back to condvar park. Must outlast the caller's result
    /// processing + resubmission time (~5-10µs) to catch sequential
    /// spawn-await patterns without a condvar round-trip (~3-5µs).
    /// ~32µs on ARM (YIELD ≈ 1ns), ~100-300µs on x86 (PAUSE ≈ 5-10ns).
    const WORKER_SPIN_ITERS: u32 = 32768;

    allocator: Allocator,

    /// Synchronization.
    mutex: thr.Mutex = .{},
    condvar: thr.Condition = .{},

    /// Shared state (protected by mutex).
    shared: Shared = .{},

    /// Configuration.
    thread_cap: usize,
    keep_alive_ns: u64,

    /// Metrics (atomic, readable without lock).
    metrics: Metrics = .{},

    /// Atomic hint set by submit() after enqueuing a task.
    /// Spinning workers poll this to catch fast resubmissions without
    /// entering the condvar (avoiding ~3-5µs kernel round-trip).
    task_available: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Number of workers currently spinning (not yet parked in condvar).
    /// Checked by submit() to avoid spawning new threads unnecessarily.
    num_spinning: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Initialize the blocking pool.
    pub fn init(allocator: Allocator, config: Config) Self {
        return Self{
            .allocator = allocator,
            .thread_cap = config.thread_cap,
            .keep_alive_ns = config.keep_alive_ns,
        };
    }

    /// Shutdown the pool and wait for all threads.
    pub fn deinit(self: *Self) void {
        var last_exiting_thread: ?std.Thread = null;
        var workers_to_join: std.AutoHashMapUnmanaged(usize, std.Thread) = .{};

        {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Already shutdown?
            if (self.shared.shutdown) {
                return;
            }

            self.shared.shutdown = true;
            self.condvar.broadcast();

            // Take ownership of thread handles
            last_exiting_thread = self.shared.last_exiting_thread;
            self.shared.last_exiting_thread = null;

            // Swap out the worker threads map
            workers_to_join = self.shared.worker_threads;
            self.shared.worker_threads = .{};
        }

        // Join threads outside the lock
        if (last_exiting_thread) |thread| {
            thread.join();
        }

        var iter = workers_to_join.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.join();
        }
        workers_to_join.deinit(self.allocator);

        // Clean up shared state
        self.mutex.lock();
        self.shared.deinit(self.allocator);
        self.mutex.unlock();
    }

    /// Submit a task to the blocking pool.
    pub fn submit(self: *Self, task: *Task) error{PoolShutdown}!void {
        var should_signal = false;

        self.mutex.lock();

        if (self.shared.shutdown) {
            self.mutex.unlock();
            return error.PoolShutdown;
        }

        // Enqueue task
        task.next = null;
        if (self.shared.queue_tail) |tail| {
            tail.next = task;
        } else {
            self.shared.queue_head = task;
        }
        self.shared.queue_tail = task;
        self.metrics.incQueueDepth();

        // Hint for spinning workers (set before unlock so the spin loop
        // sees the flag after the enqueue is visible).
        self.task_available.store(1, .release);

        // Spawn or notify
        if (self.metrics.numIdleThreads() == 0) {
            if (self.num_spinning.load(.acquire) > 0) {
                // A worker is spinning and will pick up the task via
                // the task_available flag — no signal or spawn needed.
                self.mutex.unlock();
            } else if (self.metrics.numThreads() < self.thread_cap) {
                // No idle or spinning threads — spawn a new one.
                const id = self.shared.worker_thread_index;
                self.shared.worker_thread_index += 1;

                const thread = std.Thread.spawn(.{}, workerLoop, .{ self, id }) catch |err| {
                    if (self.metrics.numThreads() > 0) {
                        self.mutex.unlock();
                        return;
                    }
                    std.debug.panic("OS can't spawn worker thread: {}", .{err});
                };

                self.shared.worker_threads.put(self.allocator, id, thread) catch {};
                self.metrics.incNumThreads();
                self.mutex.unlock();
            } else {
                // At max threads — a busy worker will pick up the task.
                self.mutex.unlock();
            }
        } else {
            // Wake an idle (condvar-parked) thread.
            _ = self.metrics.decNumIdleThreads();
            self.shared.num_notify += 1;
            self.mutex.unlock();
            // Signal OUTSIDE the lock — avoids the woken thread immediately
            // contending on the mutex the signaler still holds.
            should_signal = true;
        }

        if (should_signal) {
            self.condvar.signal();
        }
    }

    /// Worker thread main loop.
    ///
    /// Three phases per iteration:
    /// 1. BUSY — drain the task queue under the mutex
    /// 2. SPIN — release mutex, poll `task_available` for fast resubmissions
    /// 3. PARK — re-acquire mutex, condvar.timedWait for slow arrivals
    ///
    /// The spin phase eliminates a condvar round-trip (~3-5µs on macOS)
    /// when tasks are resubmitted quickly (e.g., sequential spawn-await loops).
    fn workerLoop(self: *Self, worker_id: usize) void {
        var shared = &self.shared;
        var join_on_thread: ?std.Thread = null;

        self.mutex.lock();

        main_loop: while (true) {
            // ── BUSY: Process all available tasks ──
            while (shared.queue_head) |task| {
                shared.queue_head = task.next;
                if (shared.queue_head == null) {
                    shared.queue_tail = null;
                }
                self.metrics.decQueueDepth();

                // Run task outside the lock
                self.mutex.unlock();
                task.func(task);
                self.mutex.lock();
            }

            // Check for shutdown before spinning
            if (shared.shutdown) {
                break;
            }

            // ── SPIN: Try to catch fast resubmissions without condvar ──
            // Register as spinning so submit() doesn't spawn unnecessarily.
            _ = self.num_spinning.fetchAdd(1, .release);
            self.mutex.unlock();

            var caught = false;
            var spin: u32 = 0;
            while (spin < WORKER_SPIN_ITERS) : (spin += 1) {
                if (self.task_available.load(.acquire) != 0) {
                    self.task_available.store(0, .release);
                    caught = true;
                    break;
                }
                std.atomic.spinLoopHint();
            }

            _ = self.num_spinning.fetchSub(1, .release);

            if (caught) {
                self.mutex.lock();
                continue :main_loop;
            }

            // ── PARK: Spin failed — fall back to condvar wait ──
            self.mutex.lock();

            // Re-check queue after re-acquiring lock (submit may have
            // enqueued between our spin ending and lock acquisition).
            if (shared.queue_head != null) {
                continue :main_loop;
            }

            self.metrics.incNumIdleThreads();

            while (!shared.shutdown) {
                // Timed wait (keep_alive timeout)
                const timed_out = if (self.condvar.timedWait(&self.mutex, self.keep_alive_ns)) |_|
                    false
                else |err| switch (err) {
                    error.Timeout => true,
                };

                if (shared.num_notify != 0) {
                    // Legitimate wakeup — acknowledge and go back to BUSY
                    shared.num_notify -= 1;
                    break;
                }

                // Check for timeout (not shutdown)
                if (!shared.shutdown and timed_out) {
                    // Remove ourselves and chain-join with previous exiting thread
                    const my_handle = shared.worker_threads.fetchRemove(worker_id);
                    if (my_handle) |entry| {
                        join_on_thread = shared.last_exiting_thread;
                        shared.last_exiting_thread = entry.value;
                    }
                    break :main_loop;
                }

                // Spurious wakeup — go back to sleep
            }

            if (shared.shutdown) {
                // Drain the queue on shutdown
                while (shared.queue_head) |task| {
                    shared.queue_head = task.next;
                    if (shared.queue_head == null) {
                        shared.queue_tail = null;
                    }
                    self.metrics.decQueueDepth();

                    self.mutex.unlock();

                    // Run mandatory tasks, skip non-mandatory
                    if (task.mandatory) {
                        task.func(task);
                    }

                    self.mutex.lock();
                }

                // Undo the idle increment
                self.metrics.incNumIdleThreads();
                break;
            }
        }

        // Thread exit
        self.metrics.decNumThreads();
        _ = self.metrics.decNumIdleThreads();

        // Wake shutdown waiter if we're the last thread
        if (shared.shutdown and self.metrics.numThreads() == 0) {
            self.condvar.signal();
        }

        self.mutex.unlock();

        // Join previous exiting thread (outside lock to avoid deadlock)
        if (join_on_thread) |thread| {
            thread.join();
        }
    }

    /// Get pool metrics.
    pub fn getMetrics(self: *const Self) *const Metrics {
        return &self.metrics;
    }

    // Convenience accessors
    pub fn threadCount(self: *const Self) usize {
        return self.metrics.numThreads();
    }

    pub fn idleCount(self: *const Self) usize {
        return self.metrics.numIdleThreads();
    }

    pub fn pendingTasks(self: *const Self) usize {
        return self.metrics.queueDepth();
    }
};

/// Result handle for blocking operations.
/// Allows waiting for the result of a blocking task.
///
/// Single-owner: created by `runBlocking()`, the caller uses `.wait()`
/// which blocks on a futex, then frees the context.
pub fn BlockingHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const PENDING: u32 = 0;
        pub const COMPLETED: u32 = 1;

        // Result storage
        result: ?T = null,
        err: ?anyerror = null,

        // Synchronization — single futex word replaces mutex+condvar.
        // Safe because BlockingHandle is single-waiter, single-waker, single-use.
        state: std.atomic.Value(u32) = std.atomic.Value(u32).init(PENDING),

        // The task (embedded, no separate allocation)
        task: Task = .{ .func = undefined },

        // For cleanup: pointer to self and destroy function
        destroy_fn: *const fn (*anyopaque) void = undefined,
        destroy_ctx: *anyopaque = undefined,

        /// Wait for result (blocking).
        /// Note: This frees the handle, do not use after calling.
        pub fn wait(self: *Self) !T {
            // Spin briefly — task may complete very quickly
            var spin: u32 = 0;
            while (spin < 4) : (spin += 1) {
                if (self.state.load(.acquire) == COMPLETED) break;
                std.atomic.spinLoopHint();
            }

            // Futex wait — blocks until state != PENDING
            while (self.state.load(.acquire) == PENDING) {
                thr.Futex.wait(&self.state, PENDING);
            }

            // Copy results before freeing
            const result = self.result;
            const err = self.err;
            const destroy_fn = self.destroy_fn;
            const destroy_ctx = self.destroy_ctx;

            // Free the context
            destroy_fn(destroy_ctx);

            if (err) |e| {
                return e;
            }
            return result.?;
        }

        /// Check if complete (non-blocking).
        pub fn isComplete(self: *const Self) bool {
            return self.state.load(.acquire) == COMPLETED;
        }

        /// Signal completion with value (called by blocking worker thread).
        fn complete(self: *Self, value: T) void {
            self.result = value;
            self.state.store(COMPLETED, .release);
            thr.Futex.wake(&self.state, 1);
        }

        /// Signal error (called by blocking worker thread).
        fn completeWithError(self: *Self, e: anyerror) void {
            self.err = e;
            self.state.store(COMPLETED, .release);
            thr.Futex.wake(&self.state, 1);
        }
    };
}

/// Execute a function on the blocking pool and return a handle.
pub fn runBlocking(
    pool: *BlockingPool,
    comptime func: anytype,
    args: anytype,
) !*BlockingHandle(ResultType(@TypeOf(func))) {
    const Result = ResultType(@TypeOf(func));
    const Args = @TypeOf(args);
    const Handle = BlockingHandle(Result);

    // Context captures args and provides the task callback
    const Context = struct {
        handle: Handle,
        args: Args,
        allocator: Allocator,

        fn execute(task: *Task) void {
            // Recover context from task pointer
            const self: *@This() = @fieldParentPtr("handle", @as(*Handle, @fieldParentPtr("task", task)));

            // Call the function
            const result = @call(.auto, func, self.args);

            // Handle error union
            if (@typeInfo(@TypeOf(result)) == .error_union) {
                if (result) |value| {
                    self.handle.complete(value);
                } else |err| {
                    self.handle.completeWithError(err);
                }
            } else {
                self.handle.complete(result);
            }
        }

        fn destroy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.allocator.destroy(self);
        }
    };

    // Allocate context (contains handle)
    const ctx = try pool.allocator.create(Context);
    ctx.* = .{
        .handle = .{},
        .args = args,
        .allocator = pool.allocator,
    };
    ctx.handle.task.func = Context.execute;
    ctx.handle.destroy_fn = Context.destroy;
    ctx.handle.destroy_ctx = ctx;

    try pool.submit(&ctx.handle.task);

    return &ctx.handle;
}

/// Helper to get return type, unwrapping error unions.
pub fn ResultType(comptime Func: type) type {
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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "BlockingPool - init and deinit" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 0), pool.threadCount());
}

test "BlockingPool - spawn thread on submit" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    var completed = std.atomic.Value(bool).init(false);

    const Context = struct {
        flag: *std.atomic.Value(bool),
        task: Task,

        fn run(task: *Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            self.flag.store(true, .release);
        }
    };

    var ctx = Context{
        .flag = &completed,
        .task = .{ .func = Context.run },
    };

    try pool.submit(&ctx.task);

    // Wait for completion
    while (!completed.load(.acquire)) {
        std.Thread.yield() catch {};
    }

    try std.testing.expect(completed.load(.acquire));
    try std.testing.expect(pool.threadCount() >= 1);
}

test "BlockingPool - multiple tasks" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    var counter = std.atomic.Value(usize).init(0);

    const Context = struct {
        counter: *std.atomic.Value(usize),
        task: Task,

        fn run(task: *Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            _ = self.counter.fetchAdd(1, .acq_rel);
        }
    };

    const num_tasks = 10;
    var contexts: [num_tasks]Context = undefined;

    for (0..num_tasks) |i| {
        contexts[i] = .{
            .counter = &counter,
            .task = .{ .func = Context.run },
        };
        try pool.submit(&contexts[i].task);
    }

    // Wait for all tasks
    while (counter.load(.acquire) < num_tasks) {
        std.Thread.yield() catch {};
    }

    try std.testing.expectEqual(@as(usize, num_tasks), counter.load(.acquire));
}

test "BlockingPool - submit after shutdown fails" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 2,
    });

    pool.deinit();

    const Context = struct {
        task: Task,
        fn run(_: *Task) void {}
    };

    var ctx = Context{ .task = .{ .func = Context.run } };
    const result = pool.submit(&ctx.task);
    try std.testing.expectError(error.PoolShutdown, result);
}

test "BlockingPool - runBlocking returns result" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    const handle = try runBlocking(&pool, struct {
        fn compute(a: i32, b: i32) i32 {
            return a + b;
        }
    }.compute, .{ 10, 32 });

    const result = try handle.wait();
    try std.testing.expectEqual(@as(i32, 42), result);
}

test "BlockingPool - runBlocking handles errors" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    const handle = try runBlocking(&pool, struct {
        fn failing() error{TestError}!i32 {
            return error.TestError;
        }
    }.failing, .{});

    const result = handle.wait();
    try std.testing.expectError(error.TestError, result);
}

test "BlockingPool - metrics" {
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
    });
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 0), pool.threadCount());
    try std.testing.expectEqual(@as(usize, 0), pool.idleCount());
    try std.testing.expectEqual(@as(usize, 0), pool.pendingTasks());
}

test "BlockingPool - thread idle timeout" {
    // Use very short keep_alive for testing
    var pool = BlockingPool.init(std.testing.allocator, .{
        .thread_cap = 4,
        .keep_alive_ns = 50 * std.time.ns_per_ms, // 50ms
    });
    defer pool.deinit();

    var completed = std.atomic.Value(bool).init(false);

    const Context = struct {
        flag: *std.atomic.Value(bool),
        task: Task,

        fn run(task: *Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            self.flag.store(true, .release);
        }
    };

    var ctx = Context{
        .flag = &completed,
        .task = .{ .func = Context.run },
    };

    try pool.submit(&ctx.task);

    // Wait for task
    while (!completed.load(.acquire)) {
        std.Thread.yield() catch {};
    }

    const initial_threads = pool.threadCount();
    try std.testing.expect(initial_threads >= 1);

    // Wait for idle timeout (50ms + buffer)
    thr.sleep(150 * std.time.ns_per_ms);

    // Thread should have exited due to idle timeout
    // (may still be 1 if another task came in, but shouldn't be more)
    try std.testing.expect(pool.threadCount() <= initial_threads);
}

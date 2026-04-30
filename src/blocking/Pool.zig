//! BlockingPool — dedicated OS-thread pool for sync/CPU-bound work.
//!
//! Each `volt.spawnBlocking(fn, args)` submission is enqueued here and
//! picked up by one of `n_threads` dedicated OS threads. The submitting
//! coroutine parks; the pool thread runs the fn and unparks it when done.
//!
//! Why: stackful coroutines mean we can park the calling coroutine
//! while a *separate OS thread* does the blocking work, so the worker
//! threads stay free to run other coroutines. This is the Tokio
//! `spawn_blocking` analogue, simpler because there's no Pin/Future
//! polling — the user function is just a regular synchronous fn.
//!
//! ## Thread → coroutine wake protocol
//!
//! Pool threads aren't worker threads — they don't have a worker TLS
//! set. They DO have the runtime TLS set on startup, so any
//! `Park.unpark` they trigger routes correctly:
//!   `unpark → scheduleCoro → currentRuntime() → rt.schedule(coro)`
//!   `→ injectGlobal` (cross-thread injection queue)
//!   `→ notifyOneWorker` (wakes a runtime worker to dispatch).

const std = @import("std");
const thread = @import("../internal/thread.zig");
const tls = @import("../scheduler/tls.zig");

pub const Job = struct {
    run_fn: *const fn (*anyopaque) void,
    ctx: *anyopaque,
    next: ?*Job = null,
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    runtime: *anyopaque, // *Runtime, kept opaque to avoid an import cycle

    threads: []std.Thread,

    /// Mutex-protected work queue + shutdown flag.
    queue_mutex: thread.Mutex = .{},
    queue_cond: thread.Condition = .{},
    head: ?*Job = null,
    tail: ?*Job = null,
    shutting_down: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        runtime_ptr: *anyopaque,
        n_threads: usize,
    ) !*Pool {
        const self = try allocator.create(Pool);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .runtime = runtime_ptr,
            .threads = try allocator.alloc(std.Thread, n_threads),
        };
        errdefer allocator.free(self.threads);

        var started: usize = 0;
        errdefer {
            self.queue_mutex.lock();
            self.shutting_down = true;
            self.queue_cond.broadcast();
            self.queue_mutex.unlock();
            for (self.threads[0..started]) |t| t.join();
        }
        for (self.threads) |*t| {
            t.* = try std.Thread.spawn(.{}, threadEntry, .{self});
            started += 1;
        }
        return self;
    }

    pub fn deinit(self: *Pool) void {
        self.queue_mutex.lock();
        self.shutting_down = true;
        self.queue_cond.broadcast();
        self.queue_mutex.unlock();

        for (self.threads) |t| t.join();

        self.allocator.free(self.threads);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Submit a job. The pool will run `run_fn(ctx)` on one of its
    /// threads as soon as one is available.
    pub fn submit(self: *Pool, job: *Job) void {
        job.next = null;
        self.queue_mutex.lock();
        if (self.tail) |t| {
            t.next = job;
        } else {
            self.head = job;
        }
        self.tail = job;
        self.queue_cond.signal();
        self.queue_mutex.unlock();
    }

    fn threadEntry(self: *Pool) void {
        // Set runtime TLS so Park.unpark from this thread routes via
        // currentRuntime() → injectGlobal → notifyOneWorker.
        tls.setRuntime(self.runtime);
        defer tls.clearRuntime();

        while (true) {
            self.queue_mutex.lock();
            while (self.head == null and !self.shutting_down) {
                self.queue_cond.wait(&self.queue_mutex);
            }
            if (self.shutting_down and self.head == null) {
                self.queue_mutex.unlock();
                return;
            }
            const job = self.head.?;
            self.head = job.next;
            if (self.head == null) self.tail = null;
            self.queue_mutex.unlock();

            job.run_fn(job.ctx);
        }
    }
};

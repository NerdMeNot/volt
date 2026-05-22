//! OS-thread pool for `volt.spawnBlocking` — bridges synchronous /
//! CPU-bound code into the coroutine runtime.
//!
//! ## Why
//!
//! Volt is cooperative: a coroutine that doesn't yield pins its
//! worker until it returns. Long sync work (a 5-second database
//! call, gzip on a megabyte, anything that doesn't have an async
//! variant) breaks the runtime's scheduling. `spawnBlocking` hands
//! the work to a pool of OS threads instead and parks the coroutine
//! on a completion signal. Same shape as Tokio's `spawn_blocking`.
//!
//! ## Design
//!
//! * **Lazy growth.** Zero OS threads at init. First submit spawns
//!   one. Pool grows up to `config.max_threads` on demand; subsequent
//!   submits reuse idle threads via an intrusive idle-list.
//! * **Unbounded queue.** Jobs land in a singly-linked list under a
//!   TTAS spinlock. If every pool thread is busy (queue depth N
//!   while `spawned == max_threads`), new submits queue rather than
//!   error. Backpressure is the caller's responsibility — wrap in
//!   a `Semaphore.acquire` at the boundary where you understand the
//!   workload.
//! * **Per-thread Parker.** Each pool thread owns a `Parker`. Idle
//!   threads link themselves into a LIFO idle-list and park. Submit
//!   pops one from the idle-list and unparks it — single futex wake
//!   per submit, no thundering herd.
//! * **Stable callsite addresses.** The Job and the result/done
//!   closure live on the calling coroutine's stack, which sits at a
//!   stable VA for the coroutine's lifetime. Pool threads hold the
//!   pointer across the work and parking-lot wake.
//! * **Shutdown waits for in-flight.** `deinit` sets the shutdown
//!   flag, unparks every pool thread, and joins. If a thread is
//!   mid-work-fn it runs to completion first — we can't safely
//!   cancel an OS thread inside a syscall. Same caller contract as
//!   `reactor.deinit`'s `pending == 0`: drain before you tear down.
//!
//! ## What v1 doesn't do
//!
//! * **No idle-thread teardown.** `keep_alive_ms` is in Config for
//!   forward-compatibility but ignored — threads stick around until
//!   shutdown. With `max_threads = 32` default that's at most 32
//!   OS-thread stacks (~MB each, guard-paged so ~tens of KB
//!   resident). Future addition: timed `Parker.parkUntil(ns)`.
//! * **No max_queue_depth.** Queue grows unbounded; runaway sync
//!   producers eventually OOM. YAGNI'd until someone hits it.

const std = @import("std");
const builtin = @import("builtin");
const parker_mod = @import("parker.zig");

pub const Config = struct {
    /// Upper bound on concurrent OS threads in the pool. Lazy — the
    /// pool only spawns up to this many when there's actually
    /// concurrent work for that many; idle = zero threads. The cap
    /// caps the *peak*, not the steady-state cost. 128 covers
    /// realistic services (e.g. a few hundred concurrent HTTP
    /// requests each making a sync DB call); raise to 512 or more
    /// for sync-IO-heavy workloads, lower if you want the pool to
    /// surface contention as queueing earlier.
    max_threads: u32 = 128,
    /// **v1: ignored.** Eventual idle timeout before a pool thread
    /// terminates. Present in Config so future versions can tune
    /// without an API break.
    keep_alive_ms: u64 = 10_000,
};

/// One unit of work for the pool. Caller stack-allocates and submits.
/// `run_fn` is a comptime-generated trampoline that knows how to cast
/// `arg_ptr` back to the concrete closure type, call the user
/// function, store the result, and signal completion.
pub const Job = struct {
    run_fn: *const fn (*anyopaque) void,
    arg_ptr: *anyopaque,
    /// Intrusive next-pointer for the pool's queue. Owned by the
    /// pool's spinlock for the duration of the job's queue residency.
    next: ?*Job = null,
};

/// Per-pool-thread state. The thread loops: pop a Job → run it; if
/// the queue is empty, link self into the idle-list and park.
const PoolThread = struct {
    parker: parker_mod.Parker = .{},
    /// LIFO intrusive next-ptr for the pool's idle-list.
    next_idle: ?*PoolThread = null,
    /// Set to true when the thread is in the idle-list. Read under
    /// the pool lock; written under the pool lock.
    is_idle: bool = false,
    pool: *Pool,
    thread: std.Thread = undefined,
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    config: Config,

    /// TTAS spinlock protecting `job_head`, `job_tail`, `idle_head`,
    /// and `spawned`. Critical sections are list-push / list-pop —
    /// no syscalls held under the lock (parker.unpark fires after
    /// release).
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    job_head: ?*Job = null,
    job_tail: ?*Job = null,

    /// LIFO of currently-idle PoolThreads. Submit pops from head;
    /// pool thread pushes to head when going idle.
    idle_head: ?*PoolThread = null,

    /// Pre-allocated array of `max_threads` `PoolThread` slots.
    /// Slots `[0, spawned)` correspond to live OS threads; slots
    /// `[spawned, max_threads)` are unspawned.
    threads: []PoolThread,

    /// Count of OS threads currently spawned. Grows on submit when
    /// every existing thread is busy; never shrinks until deinit.
    spawned: u32 = 0,

    /// Set by `deinit`. Pool threads observe it on wake and exit.
    shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Pool {
        std.debug.assert(config.max_threads > 0);
        const threads = try allocator.alloc(PoolThread, config.max_threads);
        // Zero each slot so the parker / fields start in a known
        // state before any thread spawns into them.
        for (threads) |*t| t.* = .{ .pool = undefined };
        return .{
            .allocator = allocator,
            .config = config,
            .threads = threads,
        };
    }

    pub fn deinit(self: *Pool) void {
        // Signal exit. Pool threads check this after every park-wake.
        self.shutdown.store(true, .release);
        // Unpark everyone — idle threads park forever otherwise.
        var i: u32 = 0;
        while (i < self.spawned) : (i += 1) {
            self.threads[i].parker.unpark();
        }
        i = 0;
        while (i < self.spawned) : (i += 1) {
            self.threads[i].thread.join();
        }
        // Job queue should be drained by now — if not, the caller
        // shut down with work outstanding. Pool threads run jobs to
        // completion before exiting, so a nonempty queue at this
        // point means a submit raced with shutdown.
        std.debug.assert(self.job_head == null);
        self.allocator.free(self.threads);
    }

    /// Submit a job. The job + its closure must live until completion
    /// (typically: both on the calling coroutine's stack, which is
    /// stable for the coroutine's lifetime). `spawnBlocking` in
    /// `lib.zig` is the canonical caller.
    ///
    /// Returns `error.PoolShuttingDown` if `deinit` has already been
    /// called. Returns `error.SystemResources` if the pool needed to
    /// spawn a new thread and `std.Thread.spawn` failed.
    pub fn submit(self: *Pool, job: *Job) !void {
        if (self.shutdown.load(.acquire)) return error.PoolShuttingDown;

        self.acquire();

        // Push job to tail of queue.
        job.next = null;
        if (self.job_tail) |t| {
            t.next = job;
            self.job_tail = job;
        } else {
            self.job_head = job;
            self.job_tail = job;
        }

        // If an idle thread is waiting, wake it.
        if (self.idle_head) |t| {
            self.idle_head = t.next_idle;
            t.next_idle = null;
            t.is_idle = false;
            self.release();
            t.parker.unpark();
            return;
        }

        // No idle threads. If we have headroom, spawn one.
        if (self.spawned < self.config.max_threads) {
            const idx = self.spawned;
            const t = &self.threads[idx];
            t.* = .{ .pool = self };
            self.spawned += 1;
            self.release();
            t.thread = std.Thread.spawn(.{}, workerLoop, .{t}) catch |err| {
                // Roll back the spawn count under the lock. The job
                // we just queued stays — another existing thread (or
                // a future spawn that succeeds) will eventually pick
                // it up. If no such thread exists, the caller's coro
                // will park forever — but that's the
                // "pool exhausted" reality regardless.
                self.acquire();
                defer self.release();
                self.spawned -= 1;
                return switch (err) {
                    error.SystemResources, error.ThreadQuotaExceeded => error.SystemResources,
                    else => err,
                };
            };
            return;
        }

        // All threads busy + at max. Job stays in the queue;
        // whichever thread finishes next will pick it up.
        self.release();
    }

    fn workerLoop(t: *PoolThread) void {
        const self = t.pool;
        while (true) {
            self.acquire();
            if (self.job_head) |job| {
                self.job_head = job.next;
                if (self.job_head == null) self.job_tail = null;
                self.release();
                job.run_fn(job.arg_ptr);
                continue;
            }
            // Queue empty. Check shutdown before parking — without
            // this, a deinit racing with our pop-empty would unpark
            // us before we parked, then we'd park forever.
            if (self.shutdown.load(.acquire)) {
                self.release();
                return;
            }
            // Link into idle-list and park. The submit path pops us
            // and unparks; deinit unparks unconditionally.
            t.next_idle = self.idle_head;
            self.idle_head = t;
            t.is_idle = true;
            self.release();

            t.parker.park();

            // On wake: someone has either (a) queued a job and popped
            // us off the idle-list, or (b) set shutdown. The next
            // loop iteration handles both — we re-acquire and either
            // pop a job or observe shutdown.
        }
    }

    inline fn acquire(self: *Pool) void {
        while (true) {
            while (self.lock.load(.monotonic) != 0) std.atomic.spinLoopHint();
            if (self.lock.cmpxchgWeak(0, 1, .acquire, .monotonic) == null) return;
        }
    }

    inline fn release(self: *Pool) void {
        self.lock.store(0, .release);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing_alloc = @import("testing.zig").allocator;

test "Pool: submit + run single job" {
    var pool = try Pool.init(testing_alloc, .{ .max_threads = 2 });
    defer pool.deinit();

    const Closure = struct {
        ran: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        fn run(opaque_ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(opaque_ptr));
            self.ran.store(1, .release);
        }
    };
    var c = Closure{};
    var job = Job{ .run_fn = &Closure.run, .arg_ptr = &c };
    try pool.submit(&job);

    // Spin until the pool thread reports completion. No bound — if
    // the pool is broken, the test framework's timeout catches it.
    while (c.ran.load(.acquire) == 0) std.atomic.spinLoopHint();
}

test "Pool: submit N jobs, all complete" {
    var pool = try Pool.init(testing_alloc, .{ .max_threads = 4 });
    defer pool.deinit();

    const N = 32;
    const Closure = struct {
        counter: *std.atomic.Value(u32),
        fn run(opaque_ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(opaque_ptr));
            _ = self.counter.fetchAdd(1, .acq_rel);
        }
    };

    var counter = std.atomic.Value(u32).init(0);
    var closures: [N]Closure = undefined;
    var jobs: [N]Job = undefined;
    var i: usize = 0;
    while (i < N) : (i += 1) {
        closures[i] = .{ .counter = &counter };
        jobs[i] = .{ .run_fn = &Closure.run, .arg_ptr = &closures[i] };
        try pool.submit(&jobs[i]);
    }

    while (counter.load(.acquire) < N) std.atomic.spinLoopHint();
}

test "Pool: queue grows past max_threads — extra jobs eventually run" {
    // max_threads = 1 so the 2nd+ submits land in the queue while
    // the first job runs.
    var pool = try Pool.init(testing_alloc, .{ .max_threads = 1 });
    defer pool.deinit();

    const Closure = struct {
        counter: *std.atomic.Value(u32),
        fn run(opaque_ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(opaque_ptr));
            _ = self.counter.fetchAdd(1, .acq_rel);
        }
    };

    var counter = std.atomic.Value(u32).init(0);
    var closures: [16]Closure = undefined;
    var jobs: [16]Job = undefined;
    for (&closures, 0..) |*c, idx| {
        c.* = .{ .counter = &counter };
        jobs[idx] = .{ .run_fn = &Closure.run, .arg_ptr = c };
        try pool.submit(&jobs[idx]);
    }

    while (counter.load(.acquire) < 16) std.atomic.spinLoopHint();
}

//! Worker — one OS thread executing coroutines.
//!
//! Each worker owns a Chase-Lev deque (`run_queue`). The owner pushes/pops
//! at the bottom (LIFO — cache-warm); siblings steal from the top (FIFO).
//!
//! Find-work order, in priority:
//!   1. Local deque (LIFO pop)
//!   2. Global injection queue (cross-thread spawns / wakes)
//!   3. Steal from a random sibling worker
//! When all three turn up empty, the worker decides between two parking
//! modes:
//!   A. If the reactor has pending I/O waits AND no other worker is polling,
//!      claim the reactor lock and do a blocking `poll` with a short
//!      timeout. Events delivered are pushed onto the local deque and
//!      surfaced on the next iteration.
//!   B. Otherwise sleep on the worker's parker; wake when another thread
//!      schedules work or signals shutdown.
//!
//! Worker 0 is the bootstrap thread (the thread that called `volt.run`).
//! It exits the loop when the root coroutine reaches `.done` and signals
//! the other workers to shut down.

const std = @import("std");
const builtin = @import("builtin");

const ctx = @import("../coroutine/context_arm64.zig");
const co = @import("../coroutine/coroutine.zig");
const Coroutine = co.Coroutine;
const Deque = @import("deque.zig").Deque;
const tls = @import("tls.zig");
const thread = @import("../internal/thread.zig");

const runtime_mod = @import("../runtime.zig");

const INITIAL_DEQUE_CAPACITY: usize = 256;

/// How long a worker blocks on the reactor before re-checking shutdown / new
/// work elsewhere. 100ms keeps shutdown latency reasonable without burning
/// CPU. EV_USER (kqueue) / eventfd (epoll) for instant wake is a v1.0 polish.
const REACTOR_POLL_TIMEOUT_NS: u64 = 100 * std.time.ns_per_ms;

/// Per-worker idle parker. Mutex + condvar; wake-up coordination via two
/// flags (`parked` for the fast-path "nobody to wake" check, `unpark_pending`
/// to swallow a missed-wake race when unpark arrives just before park).
const Parker = struct {
    mutex: thread.Mutex = .{},
    condition: thread.Condition = .{},
    parked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    unpark_pending: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn park(self: *Parker) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.unpark_pending.swap(false, .acq_rel)) return;

        self.parked.store(true, .release);
        defer self.parked.store(false, .release);
        while (!self.unpark_pending.swap(false, .acq_rel)) {
            self.condition.wait(&self.mutex);
        }
    }

    fn unpark(self: *Parker) void {
        // Stash the unpark intention even if the thread isn't blocked yet —
        // the next park() drains it before sleeping.
        self.unpark_pending.store(true, .release);
        if (!self.parked.load(.acquire)) return;
        self.mutex.lock();
        self.condition.signal();
        self.mutex.unlock();
    }
};

pub const Worker = struct {
    id: usize,
    runtime: *runtime_mod.Runtime,
    run_queue: Deque(*Coroutine),
    /// Coroutines this worker created. Worker frees them at deinit. Workers
    /// don't deinit until ALL workers have exited their loops, so by deinit
    /// time nothing is executing on any thread.
    spawned: std.array_list.Managed(*Coroutine),
    /// Local PRNG for steal-victim selection. Per-worker so we don't share
    /// state with siblings on every steal attempt.
    rng: std.Random.DefaultPrng,
    /// Worker's own context — coroutines yield back here.
    main_ctx: ctx.Context = .{},
    /// Sleep state. The worker blocks here when no work is found.
    parker: Parker = .{},
    /// Thread handle. Worker 0 is the bootstrap thread (no handle); workers
    /// 1..N-1 are spawned by Runtime.start.
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        id: usize,
        runtime: *runtime_mod.Runtime,
        rng_seed: u64,
    ) !Worker {
        return .{
            .id = id,
            .runtime = runtime,
            .run_queue = try Deque(*Coroutine).init(allocator, INITIAL_DEQUE_CAPACITY),
            .spawned = std.array_list.Managed(*Coroutine).init(allocator),
            .rng = std.Random.DefaultPrng.init(rng_seed),
        };
    }

    pub fn deinit(self: *Worker, allocator: std.mem.Allocator) void {
        for (self.spawned.items) |coro| {
            coro.destroy_extras_fn(allocator, coro);
            allocator.free(coro.stack);
            allocator.destroy(coro);
        }
        self.spawned.deinit();
        self.run_queue.deinit();
    }

    /// Track ownership of a freshly-created coroutine and push it onto the
    /// local run queue. Owner-only.
    pub fn pushOwned(self: *Worker, coro: *Coroutine) !void {
        try self.spawned.append(coro);
        coro.home_worker = @ptrCast(self);
        self.run_queue.push(coro);
    }

    /// Push a runnable coro onto this worker's run queue. Owner-only.
    /// Used by the worker's own dispatch loop and reactor wake callback.
    pub fn pushLocal(self: *Worker, coro: *Coroutine) void {
        self.run_queue.push(coro);
    }

    pub fn unpark(self: *Worker) void {
        self.parker.unpark();
    }

    /// Cheap probe of whether this worker is currently sleeping on its
    /// parker. Used by the runtime to avoid waking already-running workers.
    pub fn isParked(self: *const Worker) bool {
        return self.parker.parked.load(.acquire);
    }

    /// Main loop. If `until_done` is non-null, exits when that coroutine
    /// reaches `.done` (the bootstrap path on worker 0). Otherwise loops
    /// until the runtime's shutdown flag is set.
    pub fn run(self: *Worker, until_done: ?*Coroutine) void {
        tls.setRuntime(@ptrCast(self.runtime));
        tls.setCurrentWorker(@ptrCast(self));
        defer tls.clearCurrentWorker();
        defer tls.clearRuntime();

        while (true) {
            if (self.shouldStop(until_done)) return;

            if (self.findWork()) |coro| {
                self.dispatch(coro);
                continue;
            }

            // No work in any queue. Decide between blocking on the reactor
            // (if there's pending I/O and no one else is polling) or
            // sleeping on the parker.
            self.idleStep();

            if (self.shouldStop(until_done)) return;
        }
    }

    inline fn shouldStop(self: *Worker, until_done: ?*Coroutine) bool {
        if (until_done) |target| {
            if (target.loadState() == .done) return true;
        }
        return self.runtime.shutdownRequested();
    }

    fn findWork(self: *Worker) ?*Coroutine {
        if (self.run_queue.pop()) |c| return c;
        if (self.runtime.injection.tryPop()) |c| return c;
        if (self.tryStealFromSibling()) |c| return c;
        return null;
    }

    fn tryStealFromSibling(self: *Worker) ?*Coroutine {
        const workers = self.runtime.workers;
        if (workers.len <= 1) return null;

        const start: usize = @intCast(self.rng.random().uintLessThan(usize, workers.len));
        var attempts: usize = 0;
        while (attempts < workers.len) : (attempts += 1) {
            const idx = (start + attempts) % workers.len;
            if (idx == self.id) continue;
            if (workers[idx].run_queue.stealLoop()) |c| return c;
        }
        return null;
    }

    /// Idle path: try to do useful work via the reactor; otherwise sleep.
    fn idleStep(self: *Worker) void {
        const rt = self.runtime;

        // If reactor has pending I/O AND we can claim the poll, block on it
        // briefly. Wake events go to our local deque — surface on next loop.
        if (rt.reactor.pendingCount() > 0 and rt.tryClaimReactorPoll()) {
            defer rt.releaseReactorPoll();
            const Wake = struct {
                worker: *Worker,
                fn wake(opaque_self: *anyopaque, coro: *Coroutine) anyerror!void {
                    const w: *@This() = @ptrCast(@alignCast(opaque_self));
                    if (coro.casState(.parked, .runnable) != null) return;
                    w.worker.run_queue.push(coro);
                    // Wake one sibling in case the I/O burst is bigger than
                    // we can dispatch alone.
                    w.worker.runtime.notifyOneWorker();
                }
            };
            var wake_ctx: Wake = .{ .worker = self };
            _ = rt.reactor.poll(REACTOR_POLL_TIMEOUT_NS, &wake_ctx, &Wake.wake) catch |err| {
                std.debug.panic("worker {d}: reactor.poll failed: {}", .{ self.id, err });
            };
            return;
        }

        // No reactor work or already being polled — sleep on the parker.
        self.parker.park();
    }

    fn dispatch(self: *Worker, coro: *Coroutine) void {
        tls.setCurrent(coro, @ptrCast(self.runtime));

        coro.scheduler_ctx = &self.main_ctx;
        coro.storeState(.running);

        ctx.swap(&self.main_ctx, &coro.ctx);
        // Coroutine yielded, parked, or finished — read state with acquire.
        const observed = coro.loadState();

        tls.clearCurrent();

        switch (observed) {
            .running => {
                // Yielded — back onto local run queue.
                coro.storeState(.runnable);
                self.run_queue.push(coro);
            },
            .done => {
                // CAS-detach the waiter (Job.join attaches via CAS too).
                const old_waiter = coro.waiter.swap(null, .acq_rel);
                if (old_waiter) |waiter| {
                    if (waiter.casState(.parked, .runnable) == null) {
                        self.runtime.scheduleRunnable(waiter);
                    }
                }
                self.runtime.notifyOneWorker();
            },
            .parked => {
                // Suspended — owner re-enqueues on wake.
            },
            .runnable => unreachable,
        }
    }
};

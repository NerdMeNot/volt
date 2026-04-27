//! Runtime: top-level handle to the Volt scheduler + reactor.
//!
//! Owns the worker pool, the global injection queue, the I/O reactor, and
//! the shutdown signal. Users construct a Runtime via `volt.run(allocator,
//! root_fn, args)` — they don't usually touch this type directly.
//!
//! v0.3 multi-worker model:
//!   - N OS threads. Worker 0 is the bootstrap thread (the caller of
//!     `volt.run`). Workers 1..N-1 are dedicated threads spawned by `start`.
//!   - Each worker owns a Chase-Lev work-stealing deque (LIFO push/pop for
//!     the owner, FIFO steal for thieves).
//!   - Cross-thread spawns and reactor wakes go through the global injection
//!     queue; workers check it after their local deque.
//!   - Shared reactor with single-poller-at-a-time claim. Whichever worker
//!     idles first claims the lock and polls; events are pushed to that
//!     worker's local deque.
//!
//! Multi-worker work-stealing is the v0.3+ baseline. Per-worker io_uring
//! arrives at v0.9.

const std = @import("std");
const ctx = @import("coroutine/context_arm64.zig");
const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("coroutine/spawn.zig");
const stack_mod = @import("coroutine/stack.zig");
const tls = @import("scheduler/tls.zig");
const reactor_mod = @import("io/reactor.zig");
const Worker = @import("scheduler/worker.zig").Worker;
const Injection = @import("scheduler/injection.zig").Injection;
const time_mod = @import("time.zig");

pub const Config = struct {
    /// Default stack size for spawned coroutines.
    default_stack_size: usize = stack_mod.default_size,
    /// Number of worker threads. `null` = use the CPU count (with a floor
    /// of 1). Multi-worker is the default — set to 1 only for deterministic
    /// tests / debugging.
    workers: ?usize = null,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: Config,
    workers: []Worker,
    injection: Injection,
    reactor: reactor_mod.Reactor,

    /// Single-poller-at-a-time claim flag. Workers `cmpxchg false → true`
    /// before calling `reactor.poll`, store false after. Atomic, no mutex.
    poll_claim: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Set once when the bootstrap thread is ready to tear down. Workers
    /// observe this in their main loop and exit.
    shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Round-robin counter for spreading wake-ups across workers (used by
    /// notifyOneWorker to absorb the about-to-park race).
    notify_rr: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Runtime {
        const num_workers = blk: {
            if (config.workers) |n| {
                if (n == 0) return error.InvalidWorkerCount;
                break :blk n;
            }
            break :blk @max(1, std.Thread.getCpuCount() catch 1);
        };

        var rt: Runtime = .{
            .allocator = allocator,
            .config = config,
            .workers = try allocator.alloc(Worker, num_workers),
            .injection = Injection.init(allocator),
            .reactor = try reactor_mod.Reactor.init(allocator),
        };
        errdefer allocator.free(rt.workers);
        errdefer rt.injection.deinit();
        errdefer rt.reactor.deinit();

        // Initialize each worker. Workers point back at the runtime; we
        // pass the owning Runtime pointer up to its caller (volt.run) so
        // it can hand out a stable address before workers start running.
        var initialized: usize = 0;
        errdefer for (rt.workers[0..initialized]) |*w| w.deinit(allocator);
        const base_seed: i128 = time_mod.nanoTimestamp();
        for (rt.workers, 0..) |*w, i| {
            const seed: u64 = @as(u64, @bitCast(@as(i64, @truncate(base_seed ^ @as(i128, @intCast(i))))));
            w.* = try Worker.init(allocator, i, undefined, seed);
            initialized += 1;
        }

        return rt;
    }

    /// Patch each worker's `runtime` pointer to point at our final stable
    /// location. Called by `volt.run` after the Runtime value has been
    /// stored at its long-lived address (typically a stack slot in `run`).
    pub fn bindWorkers(self: *Runtime) void {
        for (self.workers) |*w| w.runtime = self;
    }

    pub fn deinit(self: *Runtime) void {
        for (self.workers) |*w| w.deinit(self.allocator);
        self.allocator.free(self.workers);
        self.injection.deinit();
        self.reactor.deinit();
    }

    pub fn shutdownRequested(self: *const Runtime) bool {
        return self.shutdown_flag.load(.acquire);
    }

    pub fn requestShutdown(self: *Runtime) void {
        self.shutdown_flag.store(true, .release);
        self.notifyAllWorkers();
    }

    pub fn tryClaimReactorPoll(self: *Runtime) bool {
        return self.poll_claim.cmpxchgStrong(false, true, .acq_rel, .monotonic) == null;
    }

    pub fn releaseReactorPoll(self: *Runtime) void {
        self.poll_claim.store(false, .release);
    }

    /// Wake one parked worker — used when new work appears on the injection
    /// queue or a coroutine completes (its waiter may now be runnable).
    /// Wake one worker. Two races to absorb:
    ///   1. A worker is between findWork and parker.park (parked flag not
    ///      yet set). We must not lose the wake — `unpark` sets the parker's
    ///      `unpark_pending` flag which the worker drains before sleeping.
    ///   2. The reactor poller is blocked in `kevent`. EV_USER tickle
    ///      returns it immediately.
    ///
    /// Strategy: scan for an actually-parked worker first (avoids waking a
    /// running one — no thundering herd in the common case). If none
    /// observed parked, call `unpark` on a round-robin victim — that sets
    /// `unpark_pending` and absorbs the about-to-park race.
    pub fn notifyOneWorker(self: *Runtime) void {
        if (self.poll_claim.load(.acquire)) self.reactor.tickle();

        const rr = self.notify_rr.fetchAdd(1, .monotonic);
        var i: usize = 0;
        while (i < self.workers.len) : (i += 1) {
            const idx = (rr + i) % self.workers.len;
            if (self.workers[idx].isParked()) {
                self.workers[idx].unpark();
                return;
            }
        }
        // No worker observed as parked — set unpark_pending on a round-
        // robin victim to absorb the about-to-park race.
        self.workers[rr % self.workers.len].unpark();
    }

    pub fn notifyAllWorkers(self: *Runtime) void {
        if (self.poll_claim.load(.acquire)) self.reactor.tickle();
        for (self.workers) |*w| w.unpark();
    }

    /// Place a runnable coroutine on a worker.
    ///
    /// Strategy: if we're on a worker thread, push to the calling worker's
    /// own deque (cache-warm; safe under Chase-Lev because owner-only
    /// pushes are legal). Otherwise inject globally and wake a worker.
    ///
    /// This is the single chokepoint for "I have a runnable coroutine,
    /// please run it." Park.unpark, the bootstrap, and any future cross-
    /// thread wake all funnel through here.
    pub fn schedule(self: *Runtime, coro: *Coroutine) void {
        if (currentWorker()) |me| {
            me.run_queue.push(coro);
            me.unpark();
            return;
        }
        self.injectGlobal(coro);
    }

    /// Push a coroutine onto the global injection queue and wake a worker.
    /// Called from non-worker threads, or as a fallback when local deque
    /// access isn't possible.
    pub fn injectGlobal(self: *Runtime, coro: *Coroutine) void {
        if (self.injection.push(coro)) {
            self.notifyOneWorker();
            return;
        } else |err| switch (err) {
            error.QueueFull => self.injectBlocking(coro),
            error.OutOfMemory => @panic("injection.push: OOM"),
        }
    }

    /// Backpressure path when injection is full. Spins-then-yields until
    /// the queue drains. Should be vanishingly rare — default cap is 16K.
    fn injectBlocking(self: *Runtime, coro: *Coroutine) void {
        var attempts: usize = 0;
        while (true) : (attempts += 1) {
            if (self.injection.push(coro)) {
                self.notifyOneWorker();
                return;
            } else |err| switch (err) {
                error.QueueFull => {
                    if (attempts > 64) std.Thread.yield() catch {};
                    std.atomic.spinLoopHint();
                },
                error.OutOfMemory => @panic("injection.push: OOM"),
            }
        }
    }

    /// Spawn a coroutine for `user_fn(args)` on the calling worker's deque.
    /// MUST be called from a worker thread.
    pub fn createCoroutine(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        const created = try spawn_mod.create(
            self.allocator,
            self.config.default_stack_size,
            user_fn,
            args,
        );
        const w = currentWorker() orelse @panic(
            "Runtime.createCoroutine called outside a worker thread. " ++
                "Use Runtime.spawnRoot from the bootstrap thread instead.",
        );
        try w.pushOwned(created.coro);
        w.unpark();
        return created;
    }

    /// Bootstrap-only: create the root coroutine and place it on worker 0's
    /// deque. Called from `volt.run` BEFORE any worker thread has been
    /// spawned, so concurrent access is impossible.
    pub fn spawnRoot(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        const created = try spawn_mod.create(
            self.allocator,
            self.config.default_stack_size,
            user_fn,
            args,
        );
        try self.workers[0].pushOwned(created.coro);
        return created;
    }

    /// Start workers 1..N-1 as dedicated threads. Worker 0 is the caller
    /// (bootstrap) thread. Returns once the threads have been spawned.
    pub fn start(self: *Runtime) !void {
        var started: usize = 1;
        errdefer {
            // Best-effort tear-down: signal shutdown, join what started.
            self.shutdown_flag.store(true, .release);
            for (self.workers[1..started]) |*w| w.unpark();
            for (self.workers[1..started]) |*w| {
                if (w.thread) |t| {
                    t.join();
                    w.thread = null;
                }
            }
        }
        for (self.workers[1..]) |*w| {
            w.thread = try std.Thread.spawn(.{}, workerThreadEntry, .{w});
            started += 1;
        }
    }

    /// Drive worker 0 (this thread) until `until_done` reaches `.done`.
    /// Then signal shutdown and join the other workers.
    pub fn runUntilDone(self: *Runtime, until_done: *Coroutine) void {
        self.workers[0].run(until_done);

        // Root finished — signal shutdown and reap the other workers.
        self.requestShutdown();
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| {
                t.join();
                w.thread = null;
            }
        }
    }
};

fn workerThreadEntry(w: *Worker) void {
    w.run(null);
}

/// Recover a *Runtime from the type-erased TLS slot.
pub fn currentRuntime() ?*Runtime {
    const raw = tls.currentRuntimeRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

pub fn currentWorker() ?*Worker {
    const raw = tls.currentWorkerRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

test "runtime: init/deinit cycle" {
    var rt = try Runtime.init(std.testing.allocator, .{ .workers = 2 });
    defer rt.deinit();
    try std.testing.expectEqual(stack_mod.default_size, rt.config.default_stack_size);
    try std.testing.expectEqual(@as(usize, 2), rt.workers.len);
}

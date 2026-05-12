//! v2 Runtime — multi-worker stackful scheduler.
//!
//! N OS threads each run a Worker dispatch loop. Coroutines:
//!   * `volt2.run(fn, args, .{})` is the bootstrap. It spawns N
//!     worker threads, queues the root coroutine, blocks the caller
//!     until the root completes, tears down the workers, returns
//!     the root's result.
//!   * `volt2.spawn(fn, args)` inside a coroutine pushes to that
//!     worker's local FIFO queue. From a non-worker thread (rare —
//!     mostly the driver before/after run), pushes to the global
//!     injection queue.
//!
//! Architecture (POC-validated):
//!   * Per-worker FIFO local queue (mutex-protected for steal access)
//!   * Lock-free Treiber stack as global injection queue
//!   * Worker dispatch priority: local → injection → steal → reactor poll → park
//!   * pthread_cond-based Parker (POC-D; ulock/futex upgrade later)
//!   * Cross-thread wake via parked-workers bitmap

const std = @import("std");
const context = @import("context_arm64.zig");
const coroutine = @import("coroutine.zig");
const wait_group = @import("wait_group.zig");
const task_mod = @import("task.zig");
const current = @import("current.zig");
const reactor_mod = @import("reactor_kqueue.zig");
const worker_mod = @import("worker.zig");
const parker_mod = @import("parker.zig");

pub const Coroutine = coroutine.Coroutine;
pub const Frame = coroutine.Frame;
pub const PendingKind = coroutine.PendingKind;
pub const WaitGroup = wait_group.WaitGroup;
pub const Task = task_mod.Task;
pub const Reactor = reactor_mod.Reactor;
pub const Worker = worker_mod.Worker;
pub const Parker = parker_mod.Parker;
pub const STACK_SIZE: usize = 16 * 1024;
pub const MAX_WORKERS: usize = 64; // bitmap is u64

pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Worker thread count. null = std.Thread.getCpuCount.
    workers: ?usize = null,
};

/// Cooperative yield. Re-queues the current coroutine onto the
/// running worker's local queue (tail, FIFO — yields don't bounce
/// to the front via lifo_slot).
pub fn yield() void {
    const c = current.require();
    c.pending = .yield;
    context.swap(&c.ctx, c.main_ctx);
}

/// Suspend the current coroutine until `unpark(coro)` is called.
/// Worker observes `.park` after swap-back; does NOT re-queue.
pub fn park() void {
    const c = current.require();
    c.pending = .park;
    context.swap(&c.ctx, c.main_ctx);
}

/// Re-queue a parked coroutine. Pushes to global injection so any
/// worker — including parked siblings — can pick it up. Wakes one
/// parked worker if any.
pub fn unpark(c: *Coroutine) void {
    const rt: *Runtime = @ptrCast(@alignCast(c.runtime));
    rt.injection.push(c);
    rt.wakeOneParked();
}

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    injection: worker_mod.InjectionQueue = .{},
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    reactor: Reactor = .{},
    shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Bit i set ⇔ workers[i] is parked. Worker 0 is the driver
    /// thread that called Runtime.run; workers 1..N-1 are spawned
    /// pthreads. Worker 0 participates in the same parked-bitmap
    /// machinery as the rest — uniform model, no special case.
    parked_workers: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// CAS-claim "I am the current reactor poller" — only one worker
    /// polls kqueue at a time.
    reactor_poller_taken: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(cfg: Config) !*Runtime {
        const n = cfg.workers orelse @max(1, try std.Thread.getCpuCount());
        if (n > MAX_WORKERS) return error.TooManyWorkers;

        const rt = try cfg.allocator.create(Runtime);
        errdefer cfg.allocator.destroy(rt);

        rt.* = .{
            .allocator = cfg.allocator,
            .workers = try cfg.allocator.alloc(Worker, n),
            .reactor = try Reactor.init(),
        };
        for (rt.workers, 0..) |*w, i| w.init(i, rt);

        // Spawn pthread workers 1..N-1. Worker 0 is the driver thread
        // (the thread that called Runtime.init / will call Runtime.run);
        // it joins the pool in `run()`. This makes the calling thread
        // a first-class worker rather than a special parker-on-WG case.
        for (rt.workers[1..]) |*w| {
            w.thread = try std.Thread.spawn(.{}, workerThreadEntry, .{ rt, w });
        }
        return rt;
    }

    pub fn deinit(self: *Runtime) void {
        self.shutdown.store(true, .release);
        // Wake every spawned worker so it observes shutdown.
        // Worker 0 is the driver thread — by this point it has
        // already returned from run() and is on the deinit path,
        // so it doesn't need an unpark.
        for (self.workers[1..]) |*w| w.parker.unpark();
        for (self.workers[1..]) |*w| w.thread.join();
        for (self.workers) |*w| w.deinit();
        self.allocator.free(self.workers);
        self.reactor.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Comptime-typed spawn. Allocates Frame + Coroutine + stack + Task,
    /// pushes coro to the current worker's local queue (if on a worker)
    /// or the global injection queue. Wakes a parked worker.
    pub fn spawn(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !*Task(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
        const F = Frame(user_fn, @TypeOf(args));
        const T = F.Result;

        const frame = try self.allocator.create(F);
        errdefer self.allocator.destroy(frame);

        const c = try self.allocator.create(Coroutine);
        errdefer self.allocator.destroy(c);

        const stack = try self.allocator.alignedAlloc(u8, .@"16", STACK_SIZE);
        errdefer self.allocator.free(stack);

        const task = try self.allocator.create(Task(T));
        errdefer self.allocator.destroy(task);

        frame.* = .{ .args = args, .coro = c };
        c.* = .{
            .stack = stack,
            .frame_ptr = frame,
            .frame_destroy = &F.destroy,
            // main_ctx is set by the dispatching worker; placeholder here.
            .main_ctx = undefined,
            .runtime = self,
            .has_task = true,
            .wg = null,
        };
        const stack_top: [*]u8 = stack.ptr + STACK_SIZE;
        context.initContext(&c.ctx, stack_top, frame);

        task.* = .{
            .coro = c,
            .result_ptr = &frame.result,
            .frame_ptr = frame,
            .frame_destroy = &F.destroy,
            .allocator = self.allocator,
            .wg = WaitGroup.init(1),
        };
        c.wg = @ptrCast(&task.wg);

        _ = self.pending.fetchAdd(1, .acq_rel);
        self.pushNew(c);
        return task;
    }

    /// Push a freshly-spawned coroutine. If called from inside a
    /// coroutine on a worker, uses that worker's `pushLifo` so the
    /// just-spawned continuation has the lowest dispatch latency
    /// (single-slot LIFO cache, with the evicted slot landing in
    /// the queue tail for fairness). Otherwise (driver thread or
    /// any non-worker context) injects globally.
    /// Wakes a parked worker in either case.
    fn pushNew(self: *Runtime, c: *Coroutine) void {
        if (worker_mod.currentWorker()) |w_raw| {
            const w: *Worker = @ptrCast(@alignCast(w_raw));
            w.pushLifo(c, &self.injection);
        } else {
            self.injection.push(c);
        }
        self.wakeOneParked();
    }

    /// Find a parked worker via the bitmap, atomically claim it
    /// (clear its bit), unpark its Parker. Safe to call from any
    /// thread; no-op if no worker is parked.
    pub fn wakeOneParked(self: *Runtime) void {
        var bitmap = self.parked_workers.load(.acquire);
        while (bitmap != 0) {
            const idx = @ctz(bitmap);
            const bit: u64 = @as(u64, 1) << @intCast(idx);
            if (self.parked_workers.cmpxchgWeak(bitmap, bitmap & ~bit, .acq_rel, .acquire)) |observed| {
                bitmap = observed;
                continue;
            }
            // We cleared the bit for `idx`. Unpark that worker.
            self.workers[idx].parker.unpark();
            return;
        }
    }

    fn markParked(self: *Runtime, w: *Worker) void {
        const bit: u64 = @as(u64, 1) << @intCast(w.id);
        _ = self.parked_workers.fetchOr(bit, .acq_rel);
    }

    fn unmarkParked(self: *Runtime, w: *Worker) void {
        const bit: u64 = @as(u64, 1) << @intCast(w.id);
        _ = self.parked_workers.fetchAnd(~bit, .acq_rel);
    }

    fn tryClaimPoller(self: *Runtime) bool {
        return self.reactor_poller_taken.cmpxchgStrong(false, true, .acq_rel, .acquire) == null;
    }

    fn releasePoller(self: *Runtime) void {
        self.reactor_poller_taken.store(false, .release);
    }

    /// Run `user_fn(args)` as the root coroutine. The calling thread
    /// participates as worker 0 — it runs the dispatch loop alongside
    /// the spawned pthread workers, exiting only when the root
    /// coroutine's WaitGroup hits 0.
    ///
    /// No special driver path: this is the same workerLoop the
    /// spawned threads run, with one additional exit condition.
    pub fn run(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !@typeInfo(@TypeOf(user_fn)).@"fn".return_type.? {
        var task = try self.spawn(user_fn, args);
        // Calling thread becomes worker 0 for the duration of run().
        // After this returns, deinit will shut down the other workers.
        workerLoopUntilTaskDone(self, &self.workers[0], &task.wg);
        return task.join();
    }
};

/// Entry point for pthread-spawned workers (workers 1..N-1).
/// Runs the dispatch loop until shutdown.
fn workerThreadEntry(rt: *Runtime, w: *Worker) void {
    worker_mod.currentWorkerSet(@ptrCast(w));
    defer worker_mod.currentWorkerSet(null);
    workerLoopUntilShutdown(rt, w);
}

/// Worker 0 (driver thread) loop variant: same dispatch as the spawned
/// workers, but exits when the target WaitGroup hits 0 (root coro done)
/// rather than waiting for shutdown.
fn workerLoopUntilTaskDone(rt: *Runtime, w: *Worker, target_wg: *WaitGroup) void {
    worker_mod.currentWorkerSet(@ptrCast(w));
    defer worker_mod.currentWorkerSet(null);
    while (target_wg.count() != 0) {
        if (!tryOneDispatchCycle(rt, w)) {
            // No work found anywhere. Park if target still pending.
            if (target_wg.count() == 0) return;
            parkWorker(rt, w);
        }
    }
}

/// Spawned-worker loop variant: runs until shutdown.
fn workerLoopUntilShutdown(rt: *Runtime, w: *Worker) void {
    while (!rt.shutdown.load(.acquire)) {
        if (!tryOneDispatchCycle(rt, w)) {
            if (rt.shutdown.load(.acquire)) return;
            parkWorker(rt, w);
        }
    }
}

/// Try one full dispatch cycle: local → injection → steal → reactor.
/// Returns true if a coroutine was dispatched OR the reactor was
/// polled (work may have arrived). Returns false if nothing was
/// found anywhere — caller should park.
fn tryOneDispatchCycle(rt: *Runtime, w: *Worker) bool {
    // 1. LIFO slot + local FIFO queue.
    if (w.popLocal()) |c| {
        dispatch(rt, w, c);
        return true;
    }
    // 2. Global injection (cross-thread spawns, reactor unparks, overflow).
    if (rt.injection.pop()) |c| {
        dispatch(rt, w, c);
        return true;
    }
    // 3. Steal from sibling. stealInto moves half of victim's queue
    //    into ours, returning the first stolen task.
    if (stealFromSiblings(rt, w)) |c| {
        dispatch(rt, w, c);
        return true;
    }
    // 4. Reactor poll. If any worker has in-flight IO AND we can
    //    claim the poller role, block on kevent. Events deliver via
    //    unpark → injection → next iteration picks them up.
    if (rt.reactor.pending > 0 and rt.tryClaimPoller()) {
        _ = rt.reactor.poll(true);
        rt.releasePoller();
        return true;
    }
    return false;
}

/// Park this worker. Adds self to parked_workers bitmap, rechecks
/// queues + reactor for race-arrived work, then blocks on the
/// parker. Cross-thread spawns / unparks wake via wakeOneParked.
fn parkWorker(rt: *Runtime, w: *Worker) void {
    rt.markParked(w);
    defer rt.unmarkParked(w);
    // Recheck — work may have arrived between findWork and mark.
    if (w.lifo_slot.load(.acquire) != null) return;
    if (!w.local.isEmpty() or !rt.injection.isEmpty()) return;
    if (rt.reactor.pending > 0) return;
    if (rt.shutdown.load(.acquire)) return;
    w.parker.park();
}

fn stealFromSiblings(rt: *Runtime, self: *Worker) ?*Coroutine {
    if (rt.workers.len <= 1) return null;
    const start: usize = self.rng.random().uintLessThan(usize, rt.workers.len);
    var attempts: usize = 0;
    while (attempts < rt.workers.len) : (attempts += 1) {
        const idx = (start + attempts) % rt.workers.len;
        if (idx == self.id) continue;
        if (rt.workers[idx].local.stealInto(&self.local)) |c| return c;
    }
    return null;
}

fn dispatch(rt: *Runtime, w: *Worker, c: *Coroutine) void {
    c.pending = .done;
    c.main_ctx = &w.main_ctx;
    current.set(c);
    context.swap(&w.main_ctx, &c.ctx);
    current.clear();
    switch (c.pending) {
        .yield => w.pushQueue(c, &rt.injection),
        .park => {}, // some external path will re-queue
        .done => {
            if (c.wg) |wg_atomic| {
                const wg_typed: *WaitGroup = @ptrCast(@alignCast(wg_atomic));
                wg_typed.done();
            }
            if (!c.has_task) {
                if (c.frame_destroy) |destroy_fn| destroy_fn(c.frame_ptr, rt.allocator);
            }
            rt.allocator.free(c.stack);
            rt.allocator.destroy(c);
            const prev = rt.pending.fetchSub(1, .acq_rel);
            if (prev == 1) {
                // Last coroutine just completed — wake worker 0
                // (the driver thread) specifically so it can observe
                // its target_wg = 0 and exit run(). If worker 0 isn't
                // parked, this is a no-op stored notification.
                rt.workers[0].parker.unpark();
            }
        },
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;

fn returnInt(x: u32) u32 {
    return x * 2;
}

fn returnString() []const u8 {
    return "hello from coro";
}

fn noReturn(counter: *std.atomic.Value(u32)) void {
    _ = counter.fetchAdd(1, .acq_rel);
}

test "runtime: spawn typed fn returning u32" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    const result = try rt.run(returnInt, .{@as(u32, 21)});
    try std.testing.expectEqual(@as(u32, 42), result);
}

test "runtime: spawn typed fn returning slice" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    const result = try rt.run(returnString, .{});
    try std.testing.expectEqualStrings("hello from coro", result);
}

fn fanOutRoot(counter: *std.atomic.Value(u32)) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var tasks: [100]*Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(noReturn, .{counter});
    for (&tasks) |t| t.join();
}

test "runtime: 100 coros fanned out across workers" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 4 });
    defer rt.deinit();
    var counter = std.atomic.Value(u32).init(0);
    try rt.run(fanOutRoot, .{&counter});
    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
}

fn yieldingWorker(counter: *std.atomic.Value(u32), n: u32) void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        _ = counter.fetchAdd(1, .acq_rel);
        yield();
    }
}

fn yieldRoot(counter: *std.atomic.Value(u32)) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var t1 = try rt.spawn(yieldingWorker, .{ counter, @as(u32, 10) });
    var t2 = try rt.spawn(yieldingWorker, .{ counter, @as(u32, 10) });
    t1.join();
    t2.join();
}

test "runtime: two yielding coroutines interleave across workers" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    var counter = std.atomic.Value(u32).init(0);
    try rt.run(yieldRoot, .{&counter});
    try std.testing.expectEqual(@as(u32, 20), counter.load(.acquire));
}

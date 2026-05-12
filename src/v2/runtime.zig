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
/// running worker's local queue.
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

/// Re-queue a parked coroutine. Always pushes to the global injection
/// queue so that ANY worker — including parked siblings — can pick it
/// up. Wakes one parked worker if any.
///
/// We could push to the caller's local queue for locality, but that
/// would leave the coro stranded if the caller-worker is busy and
/// parked workers exist that could run it sooner. Injection-then-wake
/// is the safer default; locality optimization is a follow-up.
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
    /// Bit i set ⇔ workers[i] is parked.
    parked_workers: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// CAS-claim "I am the current reactor poller" — only one worker
    /// polls kqueue at a time.
    reactor_poller_taken: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Driver thread parks here while waiting for `pending` to hit 0.
    driver_parker: Parker = .{},

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
        rt.driver_parker.init();
        for (rt.workers, 0..) |*w, i| try w.init(i, rt, cfg.allocator);

        // Spawn worker pthreads.
        for (rt.workers) |*w| {
            w.thread = try std.Thread.spawn(.{}, workerLoop, .{ rt, w });
        }
        return rt;
    }

    pub fn deinit(self: *Runtime) void {
        self.shutdown.store(true, .release);
        // Wake every worker so they observe shutdown.
        for (self.workers) |*w| w.parker.unpark();
        for (self.workers) |*w| w.thread.join();
        for (self.workers) |*w| w.deinit();
        self.allocator.free(self.workers);
        self.driver_parker.deinit();
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

    /// Push a coroutine onto the local queue if called from a worker,
    /// else onto the global injection queue. Wakes a parked worker.
    fn pushNew(self: *Runtime, c: *Coroutine) void {
        if (worker_mod.currentWorker()) |w_raw| {
            const w: *Worker = @ptrCast(@alignCast(w_raw));
            w.local.push(c);
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

    /// Run `user_fn(args)` as the root coroutine and block this
    /// thread until it completes. Spawns workers internally.
    pub fn run(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !@typeInfo(@TypeOf(user_fn)).@"fn".return_type.? {
        var task = try self.spawn(user_fn, args);
        // Wait on the root completing. We can't park ourselves on
        // the WaitGroup the same way coroutines do (we're a regular
        // OS thread), so spin-poll on a Parker. The last `done()` on
        // the root's wg won't fire driver_parker — we instead poll.
        // Simpler: spin-wait on task.wg.count() via the parker.
        // Use the `pending == 0` signal: every coro completion does
        // pending.fetchSub; when it hits 0, all coros are done.
        while (self.pending.load(.acquire) > 0) {
            self.driver_parker.park();
        }
        return task.join();
    }
};

/// Worker thread entry. Sets TLS, runs dispatch loop until shutdown.
fn workerLoop(rt: *Runtime, w: *Worker) void {
    worker_mod.currentWorkerSet(@ptrCast(w));
    defer worker_mod.currentWorkerSet(null);

    while (!rt.shutdown.load(.acquire)) {
        // 1. Local queue (FIFO, owner-favored).
        if (w.local.pop()) |c| {
            dispatch(rt, w, c);
            continue;
        }
        // 2. Global injection (cross-thread spawns, reactor unparks
        //    when called from non-worker context).
        if (rt.injection.pop()) |c| {
            dispatch(rt, w, c);
            continue;
        }
        // 3. Steal from random sibling.
        if (stealFromSiblings(rt, w)) |c| {
            dispatch(rt, w, c);
            continue;
        }
        // 4. Park strategy.
        //
        // If the reactor has in-flight registrations AND we can
        // claim the poller role, do a BLOCKING reactor poll. This
        // is the "park-on-reactor" path: kqueue.kevent blocks until
        // any registered fd fires, then unparks the coroutines.
        //
        // Otherwise (no IO pending or another worker is polling),
        // park on the parker. Cross-thread spawns / unparks wake us
        // via the parked_workers bitmap.
        if (rt.reactor.pending > 0 and rt.tryClaimPoller()) {
            _ = rt.reactor.poll(true);
            rt.releasePoller();
            continue;
        }
        rt.markParked(w);
        defer rt.unmarkParked(w);
        // Recheck — work may have arrived between findWork and mark.
        if (!w.local.isEmpty() or !rt.injection.isEmpty() or rt.shutdown.load(.acquire)) continue;
        // Also recheck whether reactor work appeared.
        if (rt.reactor.pending > 0) continue;
        w.parker.park();
    }
}

fn stealFromSiblings(rt: *Runtime, self: *Worker) ?*Coroutine {
    if (rt.workers.len <= 1) return null;
    const start: usize = self.rng.random().uintLessThan(usize, rt.workers.len);
    var attempts: usize = 0;
    while (attempts < rt.workers.len) : (attempts += 1) {
        const idx = (start + attempts) % rt.workers.len;
        if (idx == self.id) continue;
        const r = rt.workers[idx].local.steal();
        switch (r.result) {
            .success => return r.item,
            .empty, .retry => continue, // try next sibling
        }
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
        .yield => w.local.push(c),
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
            if (prev == 1) rt.driver_parker.unpark();
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

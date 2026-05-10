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

const ctx = @import("../coroutine/context.zig");
const co = @import("../coroutine/coroutine.zig");
const Coroutine = co.Coroutine;
const stack_mod = @import("../coroutine/stack.zig");
const stack_pool_mod = @import("../coroutine/stack_pool.zig");
const stack_overflow = @import("../coroutine/stack_overflow.zig");
const event_source = @import("../coroutine/event_source.zig");
const Deque = @import("deque.zig").Deque;
const current = @import("current.zig");
const thread = @import("../internal/thread.zig");

const runtime_mod = @import("../runtime.zig");

const INITIAL_DEQUE_CAPACITY: usize = 256;

/// Pin the current OS thread to CPU `core_idx % cpu_count`. Linux
/// only for v1.0; Darwin no-ops (its affinity APIs are coarser-grained
/// and require Mach API plumbing — v1.x add-on).
fn pinToCore(core_idx: usize) !void {
    if (comptime builtin.os.tag != .linux) return;
    const cpus = std.Thread.getCpuCount() catch 1;
    const core: usize = core_idx % @max(cpus, 1);
    var cpuset: std.os.linux.cpu_set_t = std.mem.zeroes(std.os.linux.cpu_set_t);
    const idx_bits: usize = core / @bitSizeOf(usize);
    const bit_in: u6 = @intCast(core % @bitSizeOf(usize));
    cpuset[idx_bits] |= @as(usize, 1) << bit_in;
    const tid: i32 = @intCast(std.os.linux.gettid());
    std.os.linux.sched_setaffinity(tid, &cpuset) catch return error.AffinityFailed;
}

/// Maximum reactor poll block time. With EV_USER tickle wired up via
/// `Runtime.notify*`, the polling worker returns immediately on new work
/// or shutdown — this timeout is effectively a watchdog for stuck conditions.
const REACTOR_POLL_TIMEOUT_NS: u64 = 5 * std.time.ns_per_s;

/// Watchdog deadline for `Parker.park`. The parking protocol is provably
/// lost-wake-free under seq_cst: unpark's `swap(NOTIFIED, seq_cst)` and
/// park's `cmpxchg(EMPTY → WAITING, seq_cst)` linearize through the same
/// modification order, and either ordering is handled (see proof in
/// `docs/internals/architecture.md` §"Parker protocol"). This timeout
/// only fires if that proof is wrong — which is a real bug, not something
/// to silently rescue. We panic with diagnostics so the bug surfaces
/// immediately instead of degrading silently.
///
/// Generous deadline: 30s. A healthy system unparks a parked worker in
/// microseconds; if 30s pass with no notify, something is genuinely
/// wedged (a lost wake, a `notifyAllWorkers` that didn't reach this
/// worker, or a shutdown that didn't signal).
const PARK_WATCHDOG_NS: u64 = 30 * std.time.ns_per_s;

/// Per-worker idle parker. Single-atomic state machine — one modification
/// order means no cross-atomic IRIW race.
///
/// States: `empty` | `notified` | `waiting`.
/// All transitions linearize through `state` with seq_cst on the slow
/// path (see comment in `park`).
const Parker = struct {
    mutex: thread.Mutex = .{},
    condition: thread.Condition = .{},
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(EMPTY),

    /// Optional registration into the runtime's parked-workers bitmap.
    /// Set by `Worker.bindBitmap`. Toggled inside `park` so the bit is
    /// set iff this worker is actually inside `cond.wait`.
    bitmap: ?*std.atomic.Value(u64) = null,
    bit: u64 = 0,

    const EMPTY: u8 = 0;
    const NOTIFIED: u8 = 1;
    const WAITING: u8 = 2;

    fn park(self: *Parker) void {
        // Fast path: a notification was stored before we entered park —
        // consume it and return without taking the mutex.
        if (self.state.cmpxchgStrong(NOTIFIED, EMPTY, .acquire, .monotonic) == null) {
            return;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Register in the parked-workers bitmap BEFORE the WAITING
        // transition so a concurrent waker can find us via O(1) bitmap
        // lookup. Cleared on exit.
        if (self.bitmap) |bm| _ = bm.fetchOr(self.bit, .release);
        defer {
            if (self.bitmap) |bm| _ = bm.fetchAnd(~self.bit, .release);
        }

        // Transition empty → waiting. seq_cst so this RMW linearizes
        // with unpark's seq_cst swap below.
        if (self.state.cmpxchgStrong(EMPTY, WAITING, .seq_cst, .acquire)) |observed| {
            std.debug.assert(observed == NOTIFIED);
            self.state.store(EMPTY, .release);
            return;
        }

        while (self.state.load(.acquire) == WAITING) {
            // Hard panic-with-diagnostics watchdog. A 30s wait with no
            // unpark means either a lost wake (proof in architecture.md
            // says impossible — would be a real bug) or a stuck state
            // we shouldn't try to recover from. Surface it loudly
            // instead of silently re-entering the run loop.
            self.condition.timedWait(&self.mutex, PARK_WATCHDOG_NS) catch {
                std.debug.panic(
                    "Parker.park: 30s watchdog fired (no unpark within deadline). " ++
                        "state={d}; this should be impossible under the seq_cst " ++
                        "protocol — likely a lost wake or a missed notifyAllWorkers. " ++
                        "See docs/internals/architecture.md.",
                    .{self.state.load(.acquire)},
                );
            };
        }
        self.state.store(EMPTY, .release);
    }

    fn unpark(self: *Parker) void {
        const old = self.state.swap(NOTIFIED, .seq_cst);
        if (old == WAITING) {
            self.mutex.lock();
            self.condition.signal();
            self.mutex.unlock();
        }
    }

    fn isParked(self: *const Parker) bool {
        return self.state.load(.acquire) == WAITING;
    }
};

pub const Worker = struct {
    id: usize,
    runtime: *runtime_mod.Runtime,
    run_queue: Deque(*Coroutine),

    /// Per-worker stack pool. Owner-only access (no mutex). The fast path
    /// for both spawn-side `acquire` and complete-side `release` runs
    /// here — global pool is a cold-path fallback for cross-worker
    /// frees. Pre-warmed at init: mmap N stacks upfront so first-wave
    /// spawns are syscall-free. See `stack_pool.LocalPool`.
    local_stack_pool: stack_pool_mod.LocalPool,

    /// Per-worker monotonic counters. Lock-free atomics. Written by
    /// the worker (and one other thread for `steals_from_me`); read
    /// by `volt.observability.metrics(rt)` for diagnostic dashboards.
    /// All counters monotonically increase over the worker's lifetime.
    dispatch_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    park_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    steal_success_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    steal_attempt_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// LIFO slot — single-coroutine cache-warm priority for `Park.unpark`
    /// continuations (request/response chains). Owner-only; thieves see
    /// only the deque. Tokio-style.
    lifo_slot: ?*Coroutine = null,
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
            .local_stack_pool = try stack_pool_mod.LocalPool.init(
                allocator,
                id,
                stack_pool_mod.LocalPool.DEFAULT_PREWARM_PER_WORKER,
                stack_pool_mod.LocalPool.DEFAULT_CAP_PER_WORKER,
            ),
            .rng = std.Random.DefaultPrng.init(rng_seed),
        };
    }

    pub fn deinit(self: *Worker, allocator: std.mem.Allocator) void {
        _ = allocator;
        // Coroutine struct + stack are freed via the lifecycle
        // rendezvous in `coroutine.lifecycleRelease` (Done.subscribe ↔
        // destroyJob/destroyTask), or `lifecycleReleaseRoot` for the
        // root from `volt.run`. Worker no longer tracks per-spawn
        // ownership — that pattern accumulated forever for long-
        // running services. See P1.
        self.local_stack_pool.deinit();
        self.run_queue.deinit();
    }

    /// Place a freshly-created coroutine on the deque (NOT the LIFO
    /// slot). Owner-only.
    ///
    /// Fresh spawns go to the deque so siblings can steal them. Routing
    /// fresh spawns through the LIFO slot would hide them from thieves
    /// and bottleneck burst spawns on a single worker.
    pub fn pushOwned(self: *Worker, coro: *Coroutine) !void {
        self.run_queue.push(coro);
    }

    /// Cache-warm enqueue: install in the LIFO slot, evicting any
    /// existing occupant to the deque. Used by the schedule path
    /// (`Runtime.schedule` for Park.unpark continuations) — the
    /// request/response chain stays hot in L1 on the same core.
    pub fn enqueueLocal(self: *Worker, coro: *Coroutine) void {
        if (self.lifo_slot) |evicted| {
            self.run_queue.push(evicted);
        }
        self.lifo_slot = coro;
    }

    /// Plain deque push, bypassing the LIFO slot. Used by `volt.yield`
    /// so a yielder doesn't displace a continuation. Owner-only.
    pub fn pushLocal(self: *Worker, coro: *Coroutine) void {
        self.run_queue.push(coro);
    }

    pub fn unpark(self: *Worker) void {
        self.parker.unpark();
    }

    /// Cheap probe of whether this worker is currently sleeping on its
    /// parker. Used by the runtime to avoid waking already-running workers.
    pub fn isParked(self: *const Worker) bool {
        return self.parker.isParked();
    }

    /// Register this worker into the runtime's parked-workers bitmap.
    /// Called once by `Runtime.bindWorkers` before workers start.
    pub fn bindBitmap(self: *Worker, word: *std.atomic.Value(u64), bit: u64) void {
        self.parker.bitmap = word;
        self.parker.bit = bit;
    }

    /// Main loop. If `until_done` is non-null, exits when that coroutine
    /// reaches `.done` (the bootstrap path on worker 0). Otherwise loops
    /// until the runtime's shutdown flag is set.
    pub fn run(self: *Worker, until_done: ?*Coroutine) void {
        // Quiescence ack — `runUntilDone` waits on `quiesced_count`
        // after thread.join to formally observe shutdown completion.
        // Increment exactly once per `run` call, regardless of which
        // exit path we take.
        defer _ = self.runtime.quiesced_count.fetchAdd(1, .release);

        current.setRuntime(@ptrCast(self.runtime));
        current.setCurrentWorker(@ptrCast(self));
        defer current.clearCurrentWorker();
        defer current.clearRuntime();

        // Install the SIGSEGV handler + per-thread sigaltstack so guard-
        // page hits inside coroutines turn into either a stack-grow or
        // a graceful `error.StackOverflow` instead of a process crash.
        // Idempotent — the handler install is one-shot process-wide,
        // the alt stack is per-thread.
        stack_overflow.installPerThread() catch |err| {
            std.debug.panic("worker {d}: stack_overflow.installPerThread failed: {}", .{ self.id, err });
        };

        // Optional CPU pinning (Config.pin_workers).
        if (self.runtime.config.pin_workers) {
            pinToCore(self.id) catch {}; // best-effort; no fatal on failure
        }

        while (true) {
            if (self.shouldStop(until_done)) return;

            if (self.findWork()) |coro| {
                self.dispatch(coro);
                continue;
            }

            self.idleStep();
        }
    }

    inline fn shouldStop(self: *Worker, until_done: ?*Coroutine) bool {
        if (until_done) |target| {
            if (target.isDone()) return true;
        }
        return self.runtime.shutdownRequested();
    }

    fn findWork(self: *Worker) ?*Coroutine {
        // 1. LIFO slot — wake-driven continuations, hottest priority.
        if (self.lifo_slot) |c| {
            self.lifo_slot = null;
            return c;
        }
        // 2. Local deque — owner-LIFO pop.
        if (self.run_queue.pop()) |c| return c;
        // 3. Injection — cross-thread spawns/wakes.
        if (self.runtime.injection.tryPop()) |c| return c;
        // 4. Steal from siblings.
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
            _ = self.steal_attempt_count.fetchAdd(1, .monotonic);
            if (workers[idx].run_queue.stealLoop()) |c| {
                _ = self.steal_success_count.fetchAdd(1, .monotonic);
                return c;
            }
        }
        return null;
    }

    /// Idle path: try to do useful work via the reactor; otherwise sleep.
    fn idleStep(self: *Worker) void {
        const rt = self.runtime;

        // If reactor has pending I/O AND we can claim the poll, block on it.
        // The reactor's wake target is a *Park (registered by I/O code in
        // `io/wait.zig`). `park.unpark()` runs the canonical race-free wake
        // protocol — atomically takes the parked coro and routes it via
        // `Runtime.schedule`.
        if (rt.reactor.pendingCount() > 0 and rt.tryClaimReactorPoll()) {
            defer rt.releaseReactorPoll();
            const Park = @import("park.zig").Park;
            const Wake = struct {
                fn wake(_: *anyopaque, target: *anyopaque) anyerror!void {
                    const park: *Park = @ptrCast(@alignCast(target));
                    park.unpark();
                }
            };
            var dummy: u8 = 0;
            _ = rt.reactor.poll(REACTOR_POLL_TIMEOUT_NS, @ptrCast(&dummy), &Wake.wake) catch |err| {
                std.debug.panic("worker {d}: reactor.poll failed: {}", .{ self.id, err });
            };
            return;
        }

        // No reactor work or already being polled — sleep on the parker.
        _ = self.park_count.fetchAdd(1, .monotonic);
        self.parker.park();
    }

    fn dispatch(self: *Worker, coro: *Coroutine) void {
        _ = self.dispatch_count.fetchAdd(1, .monotonic);
        current.setCurrent(coro, @ptrCast(self.runtime));
        coro.scheduler_ctx = &self.main_ctx;

        // Stack-overflow recovery checkpoint. If the coroutine writes
        // past the floor of its 1 MiB reservation, the SIGSEGV handler
        // sets `overflow_flag` and `siglongjmp`s back here; we route
        // through Done with `error.StackOverflow`. Sub-floor faults
        // are absorbed transparently by `stack.tryGrow` (commit one
        // more page, retry the instruction).
        var dispatch_cp: stack_overflow.DispatchCheckpoint = .{};
        stack_overflow.beginDispatch(&dispatch_cp);
        defer stack_overflow.endDispatch();

        if (stack_overflow.setjmpDispatch(&dispatch_cp)) {
            // Coroutine's stack is unsafe to re-enter. Route through Done.
            current.clearCurrent();
            const done_es = &event_source.done_singleton;
            done_es.subscribe_fn(@ptrCast(@constCast(done_es)), coro);
            return;
        }

        ctx.swap(&self.main_ctx, &coro.ctx);
        // Coroutine yielded, parked, or finished. It set `pending_event`
        // before the swap (either to Yield/Park via yieldWith, or to Done
        // via the trampoline on completion). Hand off ownership.
        const es = coro.pending_event;

        current.clearCurrent();

        es.subscribe_fn(@ptrCast(@constCast(es)), coro);
    }
};

//! Runtime: top-level handle to the Volt scheduler + reactor.
//!
//! Owns the worker pool, the global injection queue, the I/O reactor, and
//! the shutdown signal. Users construct a Runtime via `volt.run(config,
//! root_fn, args)` — they don't usually touch this type directly.
//!
//! ## Scheduler model
//!
//!   - N OS threads. Worker 0 is the bootstrap thread (the caller of
//!     `volt.run`). Workers 1..N-1 are dedicated threads spawned by `start`.
//!   - Each worker owns a Chase-Lev work-stealing deque (LIFO push/pop for
//!     the owner, FIFO steal for thieves).
//!   - Cross-thread spawns and reactor wakes go through the global injection
//!     queue; workers check it after their local deque.
//!   - Shared reactor with single-poller-at-a-time claim. Whichever worker
//!     idles first claims the lock and polls; events are pushed to that
//!     worker's local deque.

const std = @import("std");
const ctx = @import("coroutine/context.zig");
const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("coroutine/spawn.zig");
const stack_mod = @import("coroutine/stack.zig");
// Import the functions directly (not the module as `current`) because
// this file uses `current` as a local variable name in CAS loops.
const sched_current = @import("scheduler/current.zig");
const currentCoroutine = sched_current.currentCoroutine;
const currentRuntimeRaw = sched_current.currentRuntimeRaw;
const currentWorkerRaw = sched_current.currentWorkerRaw;
const setCurrent = sched_current.setCurrent;
const clearCurrent = sched_current.clearCurrent;
const setCurrentWorker = sched_current.setCurrentWorker;
const clearCurrentWorker = sched_current.clearCurrentWorker;
const setRuntime = sched_current.setRuntime;
const clearRuntime = sched_current.clearRuntime;
const reactor_mod = @import("io/reactor.zig");
const Worker = @import("scheduler/worker.zig").Worker;
const Injection = @import("scheduler/injection.zig").Injection;
const time_mod = @import("time.zig");
const thread = @import("internal/thread.zig");

/// Maximum supported worker count. Sharded across `BITMAP_WORDS` 64-bit
/// atomics in `parked_workers`. Raise both constants together.
pub const MAX_WORKERS: usize = 256;
pub const BITMAP_WORDS: usize = (MAX_WORKERS + 63) / 64;

/// Number of shards in the live-coroutine registry. Hash distributes
/// coros across shards to reduce contention on register/unregister.
/// With 16 shards and 8 workers, contention is ~1/16th of single-mutex
/// design. Must be a power of 2 (we use bitmask, not modulo).
pub const LIVE_SHARDS: usize = 16;

/// One bucket of the sharded live-coroutine registry.
pub const LiveShard = struct {
    mutex: thread.Mutex = .{},
    head: ?*Coroutine = null,
};

/// Hash a coroutine pointer to a shard index. Mixes high bits so
/// consecutive allocations from a slab pool don't all land in shard 0.
inline fn shardFor(coro: *const Coroutine) usize {
    const p = @intFromPtr(coro);
    // Drop low bits (alignment), then mask. xor in high bits for
    // better distribution under slab allocators that hand out
    // sequential addresses.
    return ((p >> 6) ^ (p >> 12)) & (LIVE_SHARDS - 1);
}

/// Volt's per-coroutine stack model is opinionated and not configurable
/// — like Go's 2 KiB initial stack. Each coroutine gets:
///   - **1 MiB reserved** virtual address space (cheap on 64-bit).
///   - **1 page committed** at the top (4 KiB on Linux/Intel, 16 KiB
///     on Apple Silicon — the smallest unit `mprotect` operates on,
///     and equivalent in spirit to Go's 2 KiB minimum).
///   - **Grow-on-demand** via the SIGSEGV handler (`coroutine/stack.zig`),
///     transparent to user code, no compiler help required.
///   - **Graceful failure** when the 1 MiB ceiling is hit — joiners
///     receive `error.StackOverflow`; the runtime keeps running.
pub const Config = struct {
    /// Allocator used for the worker pool, the injection queue, the
    /// reactor, and per-coroutine stacks. Required.
    allocator: std.mem.Allocator,

    /// Number of worker threads. `null` = use the CPU count (with a
    /// floor of 1). Multi-worker is the default — set to 1 only for
    /// deterministic tests / debugging.
    workers: ?usize = null,

    /// Deterministic mode: single worker, fixed RNG seed (`steal-rng`
    /// becomes pure-pseudo), no time-based jitter. Pairs with the
    /// loom-style model checker. Forces `workers = 1` if set; the
    /// `workers` field is ignored in this mode.
    ///
    /// NB: this doesn't fully eliminate non-determinism (the OS thread
    /// scheduler still nudges futex wakes, the syscall layer can
    /// re-order). But it removes Volt's own contributions, which is
    /// enough for "same seed → same scheduler trace" for most tests.
    deterministic: bool = false,

    /// Pin each worker thread to a CPU core (worker `i` → core
    /// `i % cpu_count`). Reduces cross-core cache traffic on
    /// latency-sensitive workloads. Linux: `pthread_setaffinity_np`.
    /// Darwin: a no-op (Darwin's QoS-class APIs aren't a clean fit;
    /// Future releases may add opt-in support via `thread_policy_set`).
    pin_workers: bool = false,
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

    /// Quiescence counter — workers `fetchAdd(1)` exactly once when their
    /// `run()` loop returns. `runUntilDone` waits on this counter after
    /// `thread.join` to formally observe shutdown completion (rather than
    /// relying purely on join's implicit serialization). Provides explicit
    /// telemetry: a hung worker shows up as `quiesced_count < workers.len`,
    /// not as a silent join hang. See `docs/internals/architecture.md`.
    quiesced_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Round-robin counter for spreading wake-ups across workers (used by
    /// notifyOneWorker to absorb the about-to-park race).
    notify_rr: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Bitmap of currently parked workers, sharded across `BITMAP_WORDS`
    /// 64-bit atomics. Bit `i` (= `1 << (i % 64)` in word `i / 64`) is set
    /// iff worker `i` is inside `cond.wait`. Each Parker maintains its
    /// own bit.
    ///
    /// `MAX_WORKERS = 256` covers EPYC/Threadripper (192 cores) with room.
    /// To extend further, raise `BITMAP_WORDS`. O(1) wake selection still
    /// holds: at most `BITMAP_WORDS` word-loads + a CAS to claim a bit.
    parked_workers: [BITMAP_WORDS]std.atomic.Value(u64) =
        .{std.atomic.Value(u64).init(0)} ** BITMAP_WORDS,

    /// Lazily-initialized blocking-thread pool for `spawnBlocking`.
    /// First `spawnBlocking` call creates it; runtime deinit shuts it
    /// down. Pointer-not-null is the "initialized" check (CAS-based
    /// install for thread safety).
    blocking_pool: std.atomic.Value(?*@import("blocking/Pool.zig").Pool) =
        std.atomic.Value(?*@import("blocking/Pool.zig").Pool).init(null),

    /// Recycled coroutine stacks. `Done.subscribe` pushes here; new
    /// `createCoroutine` calls pop from here before falling back to
    /// `mmap`. Drastically reduces spawn-cost syscalls under steady-
    /// state spawn-and-complete workloads.
    stack_pool: @import("coroutine/stack_pool.zig").StackPool,

    /// Sharded live-coroutine registry. Used by
    /// `volt.observability.snapshot` to walk every live coroutine.
    /// `Runtime.createCoroutine` / `spawnRoot` insert; `lifecycleRelease`
    /// removes.
    ///
    /// Sharded (16 buckets) to reduce contention under concurrent
    /// spawn: each spawn locks ONE shard, not the global registry.
    /// With 8 workers and 16 shards, contention is ~1/16th of the
    /// single-mutex design. Each shard is otherwise the same intrusive
    /// doubly-linked list as before.
    ///
    /// Backwards compat aliases: `live_coroutines` and `live_mutex`
    /// remain pointing at shard 0 for any code (snapshot) that walked
    /// them directly — but those readers now use `iterateLive` which
    /// walks all shards.
    live_shards: [LIVE_SHARDS]LiveShard = .{LiveShard{}} ** LIVE_SHARDS,

    pub fn init(config: Config) !Runtime {
        // Reset the diagnostic reactor trace so each runtime gets a
        // clean ring (no events from prior test runs contaminating the
        // dump if THIS runtime leaks). No-op when -Dreactor-trace=false.
        @import("io/reactor_trace.zig").reset();

        const allocator = config.allocator;
        const num_workers = blk: {
            // Determinism mode: force single worker, ignore `workers`.
            if (config.deterministic) break :blk @as(usize, 1);
            if (config.workers) |n| {
                if (n == 0) return error.InvalidWorkerCount;
                if (n > MAX_WORKERS) return error.TooManyWorkers;
                break :blk n;
            }
            const detected = @max(1, std.Thread.getCpuCount() catch 1);
            // Cap at MAX_WORKERS so we always fit in the parked-workers
            // bitmap. Future systems above this — extend `BITMAP_WORDS`.
            break :blk @min(detected, MAX_WORKERS);
        };

        var rt: Runtime = .{
            .allocator = allocator,
            .config = config,
            .workers = try allocator.alloc(Worker, num_workers),
            .injection = Injection.init(allocator),
            .reactor = try reactor_mod.Reactor.init(allocator),
            .stack_pool = @import("coroutine/stack_pool.zig").StackPool.init(allocator),
        };
        errdefer allocator.free(rt.workers);
        errdefer rt.injection.deinit();
        errdefer rt.reactor.deinit();

        // Initialize each worker. Workers point back at the runtime; we
        // pass the owning Runtime pointer up to its caller (volt.run) so
        // it can hand out a stable address before workers start running.
        var initialized: usize = 0;
        errdefer for (rt.workers[0..initialized]) |*w| w.deinit(allocator);
        // Deterministic mode uses a fixed seed; otherwise a time-based
        // one for steal-victim variation across runs.
        const base_seed: i128 = if (config.deterministic) 0xC0FFEE else time_mod.nanoTimestamp();
        for (rt.workers, 0..) |*w, i| {
            const seed: u64 = @as(u64, @bitCast(@as(i64, @truncate(base_seed ^ @as(i128, @intCast(i))))));
            w.* = try Worker.init(allocator, i, undefined, seed);
            initialized += 1;
        }

        return rt;
    }

    /// Patch each worker's `runtime` pointer to point at our final stable
    /// location, and register each into the parked-workers bitmap. Called
    /// by `volt.run` after the Runtime value has been stored at its
    /// long-lived address (typically a stack slot in `run`).
    pub fn bindWorkers(self: *Runtime) void {
        for (self.workers) |*w| {
            w.runtime = self;
            const word_idx = w.id / 64;
            const bit_in_word = w.id % 64;
            w.bindBitmap(&self.parked_workers[word_idx], @as(u64, 1) << @intCast(bit_in_word));
        }
    }

    pub fn deinit(self: *Runtime) void {
        // Cleanliness invariants. If these fire, the runtime is leaking
        // kernel registrations or has live coroutines we'll about to
        // abandon — both classes of bug we want to catch loudly.
        //
        //   - reactor.pendingCount() == 0: every register* must have a
        //     paired unregister* by shutdown time. The Phase 1 audit
        //     scripts (`audit_park_sites.sh`, `audit_registrations.sh`)
        //     enforce the source-level pairing; this assert is the
        //     runtime gate.
        //
        // We `assert` (not panic) so ReleaseFast strips the check —
        // the cost is zero in production but the gate fires loud in
        // test/Debug builds, which is exactly when leaks should
        // surface.
        // Cleanliness invariant: every register* must be paired with
        // exactly one decrement (by unregisterWait or by poll consuming
        // the kernel event) before the runtime tears down. Closed in R2
        // by fixing the registerWait race: waiters.put now happens
        // BEFORE kevent ADD, so kqueue-fired events never miss the
        // waiters lookup. See docs/internals/cancellation-contract.md.
        //
        // Re-enabled as a hard assert — under -Dreactor-trace, the
        // print + dump still fire first so a regression's evidence is
        // preserved.
        const pc = self.reactor.pendingCount();
        if (pc != 0) {
            std.debug.print(
                "[runtime.deinit] reactor leak: pendingCount = {d}\n",
                .{pc},
            );
            const trace = @import("io/reactor_trace.zig");
            if (comptime trace.enabled) {
                std.debug.print("[runtime.deinit] dumping reactor trace (all events)\n", .{});
                trace.dumpAll();
            }
        }
        std.debug.assert(pc == 0);

        // Sanity bound on the quiescence counter. The actual correctness
        // gate is `waitForQuiescence` inside `runUntilDone`; this assert
        // just catches a counter that has somehow exceeded the worker
        // count (would indicate a double-fetchAdd bug).
        std.debug.assert(self.quiesced_count.load(.acquire) <= self.workers.len);

        if (self.blocking_pool.load(.acquire)) |pool| pool.deinit();

        // Defense-in-depth leak gate. With P1's rendezvous, every
        // coroutine should be reclaimed before deinit (Done.subscribe ↔
        // destroyJob/destroyTask). Anything still in `live_coroutines`
        // at deinit time is a user-visible leak — they forgot to call
        // destroyJob/destroyTask on a Job/Task they spawned. Drain the
        // list so we don't leak the structs / extras / stacks; print
        // a warning so the leak is observable.
        {
            var leaked: usize = 0;
            // Walk all shards. By deinit time, all workers have quiesced
            // (waitForQuiescence guarantees), so no concurrent register/
            // unregister can race with this drain. Still take per-shard
            // locks for hygiene.
            for (&self.live_shards) |*shard| {
                shard.mutex.lock();
                var cur = shard.head;
                while (cur) |coro| {
                    const next = coro.next_alive;
                    coro.prev_alive = null;
                    coro.next_alive = null;
                    coro.destroy_extras_fn(self.allocator, coro);
                    if (!coro.stack_pooled.load(.acquire)) {
                        stack_mod.free(self.allocator, coro.stack);
                    }
                    // No separate allocator.destroy(coro) — destroy_extras_fn
                    // frees the whole Frame (which includes coro).
                    leaked += 1;
                    cur = next;
                }
                shard.head = null;
                shard.mutex.unlock();
            }
            if (leaked > 0) {
                std.debug.print(
                    "[runtime.deinit] {d} coroutine(s) still alive at " ++
                        "shutdown — caller forgot destroyJob/destroyTask. " ++
                        "Reclaimed defensively.\n",
                    .{leaked},
                );
            }
        }

        for (self.workers) |*w| w.deinit(self.allocator);
        self.allocator.free(self.workers);
        self.stack_pool.deinit();
        self.injection.deinit();
        self.reactor.deinit();
    }

    /// Default thread count for the blocking pool (Tokio default — 512
    /// is enough for most workloads without exhausting OS thread limits).
    pub const DEFAULT_BLOCKING_THREADS: usize = 512;

    /// Get (or lazily create) the blocking-thread pool.
    pub fn blockingPool(self: *Runtime) !*@import("blocking/Pool.zig").Pool {
        if (self.blocking_pool.load(.acquire)) |p| return p;
        // Race-safe lazy init: build the pool, CAS-install. If we lose
        // the race, free our copy and use the winner's.
        const Pool = @import("blocking/Pool.zig").Pool;
        const new_pool = try Pool.init(self.allocator, @ptrCast(self), DEFAULT_BLOCKING_THREADS);
        if (self.blocking_pool.cmpxchgStrong(null, new_pool, .acq_rel, .acquire)) |existing| {
            new_pool.deinit();
            return existing.?;
        }
        return new_pool;
    }

    pub fn shutdownRequested(self: *const Runtime) bool {
        return self.shutdown_flag.load(.acquire);
    }

    pub fn requestShutdown(self: *Runtime) void {
        self.shutdown_flag.store(true, .release);
        self.notifyAllWorkers();
    }

    /// Maximum time `waitForQuiescence` polls before panicking with a
    /// diagnostic dump. 10s is generous: a healthy shutdown completes in
    /// microseconds (workers observe `shutdown_flag` on their next
    /// `findWork`/wake cycle). If this fires, a worker is genuinely stuck
    /// — almost always a lost wake or a coroutine that won't yield to a
    /// cancellation. We want a loud failure, not a silent hang.
    pub const SHUTDOWN_DEADLINE_NS: u64 = 10 * std.time.ns_per_s;

    /// Block until every worker's `run()` has returned (observable via
    /// `quiesced_count == workers.len`). Returns `error.Timeout` if the
    /// deadline elapses — `runUntilDone` translates that into a panic
    /// with diagnostics.
    pub fn waitForQuiescence(self: *Runtime, timeout_ns: u64) error{Timeout}!void {
        const start_ns = time_mod.nanoTimestamp();
        const target = self.workers.len;
        while (self.quiesced_count.load(.acquire) < target) {
            const elapsed_ns: u64 = @intCast(time_mod.nanoTimestamp() - start_ns);
            if (elapsed_ns > timeout_ns) return error.Timeout;
            // Yield CPU briefly. The poll resolution doesn't matter — this
            // path runs at most once per shutdown, on the bootstrap thread,
            // and the workers all unblock together once `shutdown_flag` is
            // visible.
            thread.sleep(1 * std.time.ns_per_ms);
        }
    }

    pub fn tryClaimReactorPoll(self: *Runtime) bool {
        return self.poll_claim.cmpxchgStrong(false, true, .acq_rel, .monotonic) == null;
    }

    pub fn releaseReactorPoll(self: *Runtime) void {
        self.poll_claim.store(false, .release);
    }

    /// Wake one parked worker — used when new work appears on the injection
    /// queue or shutdown is signaled.
    ///
    /// Anti-herd invariant: skip the wake entirely if a worker is already
    /// "searching" (woken, scanning, hasn't found work yet) — the searcher
    /// will discover the new push on its next iteration. Otherwise pick a
    /// parked worker via the O(1) bitmap and atomically reserve a search
    /// slot before unparking.
    ///
    /// Two paths to absorb:
    ///   1. The reactor poller is blocked in `kevent`. `EV_USER` tickle
    ///      returns it immediately.
    ///   2. A worker is in `cond.wait` — find it via `parked_workers` and
    ///      atomically claim it.
    ///
    /// If nothing is parked AND nothing is searching, buffer the wake on a
    /// round-robin victim (its `Parker.unpark` stores NOTIFIED, drained by
    /// the next park). This absorbs the worker-between-findWork-and-park
    /// race that the bitmap can't see.
    pub fn notifyOneWorker(self: *Runtime) void {
        if (self.poll_claim.load(.acquire)) self.reactor.tickle();
        // No exclusion — any parked worker is fair game.
        if (self.takeOneFromBitmap(no_exclude_word, 0)) |idx| {
            self.workers[idx].unpark();
            return;
        }
        const rr = self.notify_rr.fetchAdd(1, .monotonic);
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
    /// pushes are legal) AND nudge one parked sibling so it has a chance
    /// to steal. Without the sibling nudge, parked workers stay parked
    /// until a `Done` event fires — which is after the work has run, by
    /// which time the deque is empty. That collapses parallelism: small
    /// bursts of spawns end up running serially on one worker.
    ///
    /// If we're not on a worker thread (cross-thread wake), inject
    /// globally and wake one parked worker via `notifyOneWorker`.
    ///
    /// This is the single chokepoint for "I have a runnable coroutine,
    /// please run it." Park.unpark, the bootstrap, and any future cross-
    /// thread wake all funnel through here.
    pub fn schedule(self: *Runtime, coro: *Coroutine) void {
        if (currentWorker()) |me| {
            // Schedule path = "I just made this runnable; run it next on
            // my core." Goes through the LIFO slot for cache locality.
            me.enqueueLocal(coro);
            self.wakeOneSibling(me.id);
            return;
        }
        self.injectGlobal(coro);
    }

    /// Nudge one parked sibling so it can steal from the current worker.
    ///
    /// History (M1): an earlier anti-herd check (`if num_searching != 0
    /// skip`) modelled after Tokio/Go was removed during early debugging
    /// of a re-entrant mutex bug in Channel's blocking-path recheck;
    /// restoring it is gated on the model-checker stress harness so the
    /// missed-wake corner cases can be flushed out properly. For now we
    /// wake unconditionally — a few extra futex-wakes per scheduling
    /// event, but obviously correct.
    ///
    /// Race absorption: if `takeOneFromBitmap` returns null (no worker
    /// is observably in the bitmap right now), we fall back to a
    /// round-robin unpark. This buffers a NOTIFIED on the target
    /// parker so a worker that's mid-park (between findWork and
    /// bitmap.fetchOr) consumes it on its next state CAS instead of
    /// missing the wake. Same shape `notifyOneWorker` uses for cross-
    /// thread injection.
    pub fn wakeOneSibling(self: *Runtime, self_id: usize) void {
        if (self.workers.len <= 1) return;
        const exclude_word = self_id / 64;
        const exclude_bit = @as(u64, 1) << @intCast(self_id % 64);
        if (self.takeOneFromBitmap(exclude_word, exclude_bit)) |idx| {
            self.workers[idx].unpark();
        }
        // No fallback. The previous design unconditionally fired a
        // round-robin unpark when no sibling was observably parked,
        // intended to handle the mid-park race (worker between
        // findWork-empty and bitmap.fetchOr). That fallback fires on
        // EVERY spawn under burst load, hammering a busy sibling's
        // parker.state cache line with cross-core atomic CAS — pure
        // overhead because the target was already running.
        //
        // We accept the rare mid-park race: a worker that just decided
        // to park (but hasn't set its bit yet) might miss this wake.
        // The next spawn / schedule fires wakeOneSibling again and
        // catches them. For burst workloads (the perf case), the
        // bounded delay is microseconds. For lone spawns, the spawner
        // owns the work — siblings being parked doesn't lose anything.
    }

    /// Sentinel for `takeOneFromBitmap` — out-of-range word index meaning
    /// "no worker is excluded."
    const no_exclude_word: usize = std.math.maxInt(usize);

    /// Atomically pick and clear one parked-worker bit, optionally
    /// excluding the specific (word, bit) of the calling worker. Returns
    /// the worker index if claimed, null if no parked worker exists
    /// (excluding the exclusion).
    ///
    /// Walks the sharded bitmap starting from a round-robin position,
    /// preferring bits at or above the start (within the first word) and
    /// continuing into subsequent words on wrap-around. The RR rotation
    /// spreads wakes across workers rather than hammering the lowest-
    /// indexed parked sibling.
    ///
    /// O(BITMAP_WORDS) word loads in the worst case (typical: 1). The
    /// CAS-loop within a word converges in 1–2 iterations under typical
    /// contention.
    fn takeOneFromBitmap(self: *Runtime, exclude_word: usize, exclude_bit: u64) ?usize {
        const rr = self.notify_rr.fetchAdd(1, .monotonic);
        const start_global: usize = rr % @max(@as(usize, 1), self.workers.len);
        const start_word: usize = start_global / 64;
        const start_bit_in_word: u6 = @intCast(start_global % 64);

        // Pass 1: from `start_word` forward (wrapping), prefer at-or-above
        // start_bit_in_word in the first word.
        var i: usize = 0;
        while (i < BITMAP_WORDS) : (i += 1) {
            const word_idx = (start_word + i) % BITMAP_WORDS;
            if (self.tryClaimInWord(word_idx, exclude_word, exclude_bit, if (i == 0) start_bit_in_word else 0)) |idx| {
                return idx;
            }
        }
        // Pass 2 (wrap): the first word's bits BELOW start_bit_in_word
        // weren't checked. Try them now.
        if (start_bit_in_word != 0) {
            if (self.tryClaimBelow(start_word, exclude_word, exclude_bit, start_bit_in_word)) |idx| {
                return idx;
            }
        }
        return null;
    }

    /// Try to claim a bit in `word_idx` at or above `min_bit`. Returns the
    /// worker index on success.
    fn tryClaimInWord(
        self: *Runtime,
        word_idx: usize,
        exclude_word: usize,
        exclude_bit: u64,
        min_bit: u6,
    ) ?usize {
        var current = self.parked_workers[word_idx].load(.acquire);
        while (true) {
            var candidates = current;
            if (word_idx == exclude_word) candidates &= ~exclude_bit;
            // Restrict to bits at or above min_bit.
            candidates &= ~@as(u64, 0) << min_bit;
            if (candidates == 0) return null;

            const target_bit = candidates & (~candidates +% 1);
            const next = current & ~target_bit;
            if (self.parked_workers[word_idx].cmpxchgWeak(current, next, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            const bit_in_word: u6 = @intCast(@ctz(target_bit));
            return word_idx * 64 + bit_in_word;
        }
    }

    /// Try to claim a bit in `word_idx` strictly below `max_bit`.
    fn tryClaimBelow(
        self: *Runtime,
        word_idx: usize,
        exclude_word: usize,
        exclude_bit: u64,
        max_bit: u6,
    ) ?usize {
        var current = self.parked_workers[word_idx].load(.acquire);
        while (true) {
            var candidates = current;
            if (word_idx == exclude_word) candidates &= ~exclude_bit;
            // Restrict to bits below max_bit.
            const max_bit_wide: u7 = max_bit;
            const below_mask: u64 = (@as(u64, 1) << @intCast(max_bit_wide)) - 1;
            candidates &= below_mask;
            if (candidates == 0) return null;

            const target_bit = candidates & (~candidates +% 1);
            const next = current & ~target_bit;
            if (self.parked_workers[word_idx].cmpxchgWeak(current, next, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            const bit_in_word: u6 = @intCast(@ctz(target_bit));
            return word_idx * 64 + bit_in_word;
        }
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

    /// Variant of `createCoroutine` that allocates the spawn Frame from
    /// the given allocator instead of `self.allocator`. Used by
    /// `volt.scope` to allocate children's Frames from a scope-local
    /// arena. See `api/launch.zig:launchWith`.
    pub fn createCoroutineWith(
        self: *Runtime,
        allocator: std.mem.Allocator,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        return self.createCoroutineImpl(allocator, user_fn, args);
    }

    /// Spawn a coroutine for `user_fn(args)` on the calling worker's deque.
    /// MUST be called from a worker thread.
    pub fn createCoroutine(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        return self.createCoroutineImpl(self.allocator, user_fn, args);
    }

    fn createCoroutineImpl(
        self: *Runtime,
        frame_allocator: std.mem.Allocator,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        // Resolve the calling worker first — we need it for the local
        // stack pool below, and it's fast (one TLS read).
        const w = currentWorker() orelse @panic(
            "Runtime.createCoroutine called outside a worker thread. " ++
                "Use Runtime.spawnRoot from the bootstrap thread instead.",
        );

        // Acquire a stack — three-tier search:
        //   1. Worker's local pool (owner-only, no mutex). Hot path.
        //   2. Runtime's global pool (mutex; cross-worker fallback).
        //   3. Fresh `mmap` via spawn_mod.create when both pools empty.
        // With pre-warmed local pools, steady-state hits tier 1 and
        // does ZERO syscalls in the spawn hot loop.
        var stage: enum { initial, has_local_recycled, has_global_recycled, has_created } = .initial;
        const recycled = blk: {
            if (w.local_stack_pool.acquire(stack_mod.default_reserved)) |gs| {
                stage = .has_local_recycled;
                break :blk @as(?stack_mod.GrowableStack, gs);
            }
            if (self.stack_pool.acquire(stack_mod.default_reserved)) |gs| {
                stage = .has_global_recycled;
                break :blk @as(?stack_mod.GrowableStack, gs);
            }
            break :blk null;
        };

        var created: spawn_mod.Created(@TypeOf(user_fn)) = undefined;
        errdefer switch (stage) {
            .initial => {},
            .has_local_recycled => if (recycled) |gs| w.local_stack_pool.release(gs),
            .has_global_recycled => if (recycled) |gs| self.stack_pool.release(gs),
            .has_created => {
                if (!created.coro.stack_pooled.load(.acquire)) {
                    stack_mod.free(self.allocator, created.coro.stack);
                }
                // destroy_extras_fn frees the whole frame (Coroutine
                // included) — no separate allocator.destroy needed.
                // Frame uses frame_allocator (may differ from self.allocator
                // when called via createCoroutineWith for scope-inline).
                created.coro.destroy_extras_fn(frame_allocator, created.coro);
            },
        };

        // Route Frame allocation through the calling worker's slab
        // FramePool first (fast path: ~5ns bump+freelist pop). Falls
        // back to `frame_allocator` only on pool exhaustion or for
        // Frame types too large to fit any size class. The pool is
        // safe to use cross-thread on free (lock-free single-link
        // push); alloc is owner-only here so no contention.
        const frame_source = spawn_mod.FrameSource{
            .pool = &w.frame_pool,
            .pool_worker_id = @intCast(w.id),
        };
        created = try spawn_mod.createWithFrameSource(
            frame_allocator,
            frame_source,
            .{ .reserved = stack_mod.default_reserved },
            recycled,
            user_fn,
            args,
        );
        // Ownership of the recycled stack is now inside `created`.
        // Bump stage so the errdefer no longer tries to return it to
        // the pool (would double-account vs the `has_created` branch).
        stage = .has_created;

        // Capture the parent for async-backtrace ancestry.
        if (currentCoroutine()) |parent| {
            created.coro.parent_id = @intFromPtr(parent);
        }

        // Push to deque BEFORE registering live: if pushOwned fails
        // (OOM growing the deque), the errdefer cleans `created` —
        // unwinding a registerLive would also race with snapshot
        // iterators that hold `live_mutex`.
        try w.pushOwned(created.coro);
        self.registerLive(created.coro);
        // Wake a parked sibling so it has a chance to steal. Same reasoning
        // as `schedule`: without this, parallelism collapses on bursty
        // spawn patterns.
        self.wakeOneSibling(w.id);
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
            .{ .reserved = stack_mod.default_reserved },
            null, // root never recycles — pool is empty at startup
            user_fn,
            args,
        );
        // Mark as the until-done watch target. Done.subscribe uses this to
        // wake worker 0 (the bootstrap watcher) on completion — without
        // having to broadcast a wake to every worker.
        created.coro.is_root = true;
        self.registerLive(created.coro);
        try self.workers[0].pushOwned(created.coro);
        return created;
    }

    /// Insert `coro` at the head of the per-runtime live registry.
    /// Called by `createCoroutine` / `spawnRoot`. Cleared by
    /// `unregisterLive` on lifecycle release. The intrusive
    /// prev/next pointers live on the Coroutine struct itself, so no
    /// per-spawn allocation.
    pub fn registerLive(self: *Runtime, coro: *Coroutine) void {
        const shard = &self.live_shards[shardFor(coro)];
        shard.mutex.lock();
        defer shard.mutex.unlock();
        coro.prev_alive = null;
        coro.next_alive = shard.head;
        if (shard.head) |old_head| old_head.prev_alive = coro;
        shard.head = coro;
    }

    /// Remove `coro` from the live registry. Called from
    /// `coroutine.lifecycleRelease` (and `lifecycleReleaseRoot`)
    /// before freeing the struct. Idempotent: a coro that was never
    /// registered (or already unregistered) is a no-op.
    pub fn unregisterLive(self: *Runtime, coro: *Coroutine) void {
        const shard = &self.live_shards[shardFor(coro)];
        shard.mutex.lock();
        defer shard.mutex.unlock();
        if (coro.prev_alive) |p| {
            p.next_alive = coro.next_alive;
        } else if (shard.head == coro) {
            shard.head = coro.next_alive;
        }
        if (coro.next_alive) |n| {
            n.prev_alive = coro.prev_alive;
        }
        coro.prev_alive = null;
        coro.next_alive = null;
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

        // Formal quiescence gate. After thread.join, every worker's
        // `run()` has returned and incremented `quiesced_count`. This
        // wait is normally a no-op (counter already at target by the
        // time we get here), but exists as a hard observability point:
        // if we ever introduce a code path that bypasses the per-worker
        // `defer fetchAdd` (e.g. a panic that escapes), the counter
        // catches it. The deadline panic surfaces the bug loudly.
        self.waitForQuiescence(SHUTDOWN_DEADLINE_NS) catch {
            std.debug.panic(
                "Runtime.runUntilDone: workers did not quiesce within {d}ms — " ++
                    "quiesced={d}/{d}. A worker's run() did not return; either " ++
                    "thread.join hung (lost wake?) or a panic bypassed the " ++
                    "fetchAdd. Stack traces should follow.",
                .{
                    SHUTDOWN_DEADLINE_NS / std.time.ns_per_ms,
                    self.quiesced_count.load(.acquire),
                    self.workers.len,
                },
            );
        };
    }
};

fn workerThreadEntry(w: *Worker) void {
    w.run(null);
}

/// Recover a *Runtime from the type-erased TLS slot.
pub fn currentRuntime() ?*Runtime {
    const raw = currentRuntimeRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

pub fn currentWorker() ?*Worker {
    const raw = currentWorkerRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

test "runtime: init/deinit cycle" {
    var rt = try Runtime.init(.{ .allocator = std.testing.allocator, .workers = 2 });
    defer rt.deinit();
    try std.testing.expectEqual(@as(usize, 2), rt.workers.len);
}

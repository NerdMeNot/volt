//! Runtime — multi-worker stackful scheduler (M:N, Phase 1).
//!
//! N OS threads (Ms) each run a dispatch loop. Each M is bound 1:1
//! to a P (processor / scheduler unit) for the lifetime of the
//! Runtime in Phase 1. Later phases will allow M ↔ P detach.
//!
//!   * `Runtime.run(fn, args)` is the bootstrap. The calling thread
//!     joins the pool as M[0]/P[0], spawns N-1 pthreads for M[1..N-1],
//!     queues the root coroutine, runs the dispatch loop until the
//!     root's `done` flag is set, returns the root's result. Other
//!     Ms stay alive until `deinit`.
//!   * `Runtime.spawn(fn, args)` inside a coroutine pushes onto the
//!     current P's lifo_slot for the warmest possible dispatch
//!     latency. From a non-coroutine thread, pushes to the global
//!     injection queue.
//!
//! Architecture:
//!   * Per-P fixed-256 lock-free work-stealing queue. Owner pops
//!     FIFO; stealers claim half. Overflow spills HALF to global
//!     injection (Tokio strategy).
//!   * Single-slot LIFO cache on each P for spawn-chain locality.
//!   * Lock-free Treiber-stack global injection queue (will become
//!     per-P mailbox in Phase 2).
//!   * P dispatch priority: lifo_slot → local → injection → steal
//!     → reactor poll → park.
//!   * Parker built on `__ulock_wait` (Darwin) / `FUTEX_WAIT` (Linux),
//!     lives on M.
//!   * Cross-thread wake via parked-Ms bitmap (acq_rel CAS claims
//!     one victim).
//!   * Anti-herd: `num_searching` lets pushers skip the wake when
//!     another M is already scanning for work.
//!   * Periodic fairness: every 61 dispatches, check injection
//!     before local — prevents starvation of injected work when a
//!     P's local queue is repeatedly fed by yields.

const std = @import("std");
const context = @import("context.zig");
const coroutine = @import("coroutine.zig");
const task_mod = @import("task.zig");
const current = @import("current.zig");
const reactor_mod = @import("reactor.zig");
const worker_mod = @import("worker.zig");
const p_mod = @import("p.zig");
const parker_mod = @import("parker.zig");
const park_mod = @import("park.zig");
const stack_mod = @import("stack.zig");
const blocking_pool_mod = @import("blocking_pool.zig");
const signal_mod = @import("signal.zig");
const fs_ring_mod = @import("fs_ring.zig");
const builtin = @import("builtin");

pub const Coroutine = coroutine.Coroutine;
pub const Frame = coroutine.Frame;
pub const PendingKind = coroutine.PendingKind;
pub const Task = task_mod.Task;
pub const Reactor = reactor_mod.Reactor;
pub const M = worker_mod.M;
pub const P = p_mod.P;
pub const Parker = parker_mod.Parker;
/// Usable per-coroutine stack size — `BODY_SIZE` from `stack.zig`.
/// Re-exported here for downstream consumers (benches, docs) that
/// historically referenced `volt.STACK_SIZE`.
pub const STACK_SIZE: usize = stack_mod.BODY_SIZE;
pub const MAX_WORKERS: usize = 64; // bitmap is u64

/// Errors `Runtime.spawn` (and `volt.spawn`) can return. Explicit
/// named set so library callers can `catch err switch` exhaustively
/// — and so `scope`/`withTimeout`/`joinFirst` can union it with
/// the body's error set instead of inheriting `anyerror`.
///
/// `OutOfMemory` comes from the per-spawn Frame+Task heap alloc and
/// the coroutine-struct pool's allocator fallback. The other three
/// come from the stack arena: `ArenaExhausted` (every slot in use),
/// `MmapFailed` / `MprotectFailed` (kernel VM-space exhaustion or
/// permission failure during lazy slot commit).
pub const SpawnError = std.mem.Allocator.Error || stack_mod.Error;

/// Minimalist runtime instrumentation hook. Set
/// `Runtime.Config.observer = &my_observer` and the runtime fires
/// these callbacks at the corresponding lifecycle points. When
/// `observer == null`, every hook site folds to a dead branch at
/// optimisation time — zero overhead.
///
/// Use this for: spawn/done counters, hung-coroutine investigators,
/// per-coroutine timing profilers, custom tracing emitters
/// (StatsD, JSON-lines, OpenTelemetry — whatever shape you want).
/// Volt deliberately doesn't ship a tracing impl in v1; this is the
/// hook layer downstream observers build on.
///
/// Async-signal-safety: callbacks run from the dispatching worker.
/// They are NOT signal-handler safe. Keep callbacks fast — they
/// fire on the hot scheduler path.
pub const Observer = struct {
    /// Fired after a new coroutine is constructed and queued by
    /// `Runtime.spawn`. The coroutine has not yet started running.
    onSpawn: ?*const fn (coro: *Coroutine) void = null,
    /// Fired when a coroutine begins parking (after the primitive
    /// has registered the waiter; just before the ctx swap).
    onPark: ?*const fn (coro: *Coroutine) void = null,
    /// Fired when `runtime.unpark` re-queues a parked coroutine.
    onUnpark: ?*const fn (coro: *Coroutine) void = null,
    /// Fired when a coroutine's trampoline returns (terminal state).
    /// The `coro` pointer is about to be recycled — read fields
    /// before returning from the callback.
    onComplete: ?*const fn (coro: *Coroutine) void = null,
};

pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Worker thread count. null = std.Thread.getCpuCount.
    workers: ?usize = null,
    /// Hard cap on concurrently-live coroutine stacks. The runtime
    /// pre-reserves `max_concurrent_stacks * stack_reservation_size`
    /// of virtual address space at init (PROT_NONE — zero RSS until
    /// used) and hands slots out from a slab arena. Spawn returns
    /// `error.ArenaExhausted` once every slot is in use.
    ///
    /// The default covers typical interactive workloads and the full
    /// bench suite. Raise for high-concurrency servers (e.g. tens of
    /// thousands of HTTP connections); lower to bound virtual address
    /// usage on tight 32-bit-style budgets.
    max_concurrent_stacks: usize = stack_mod.DEFAULT_MAX_STACKS,
    /// Per-coroutine stack VA reservation. The effective stack ceiling
    /// is `stack_reservation_size - guard_page - 0` (the top body is
    /// committed lazily; deeper pages commit on SIGSEGV up to the
    /// guard). Default 1 MiB → ~1008 KiB usable on Darwin, ~1020 KiB
    /// on Linux. Raise for deep-recursion / heavy C-library callouts;
    /// lower to pack more concurrent coroutines into the same VA
    /// budget. Must be a multiple of the page size.
    ///
    /// RSS is unaffected — every coroutine starts with the same 16
    /// KiB body commit; only this knob's *virtual* cost scales.
    stack_reservation_size: usize = stack_mod.DEFAULT_RESERVATION_SIZE,
    /// OS-thread pool used by `volt.spawnBlocking` to run sync /
    /// CPU-bound code without pinning a worker. Lazy — no threads
    /// spawn until the first `spawnBlocking` call. Default caps
    /// concurrent sync work at 128 threads; raise for sync-IO-heavy
    /// services. See `src/blocking_pool.zig`.
    blocking: blocking_pool_mod.Config = .{},
    /// Optional instrumentation hook. Set to `&my_observer` to
    /// receive callbacks on spawn / park / unpark / complete.
    /// Default `null` — every hook site folds away at optimisation
    /// time, zero runtime cost.
    observer: ?*const Observer = null,
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
///
/// `park_state` machine closes the register-then-park race:
///
/// 1. A primitive that wants to wait registers the coroutine in its
///    own waiter slot (so an unparker can find it).
/// 2. The primitive calls `runtime.park()`.
/// 3. The coroutine swaps out. Dispatch then atomically transitions
///    `park_state` RUNNING → PARKED.
/// 4. If at step 3 we observe NOTIFIED (unpark fired while we were
///    still on-CPU), dispatch immediately re-queues the coroutine
///    instead of leaving it parked.
///
/// Without this, an unpark fired between (1) and the actual
/// `context.swap` in (2) would push the coroutine onto the run
/// queue while it is *still being executed* by its owning worker —
/// a second worker would then dispatch it and two workers would
/// race on the same stack.
pub fn park() void {
    const c = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(c.runtime));
    if (rt.observer) |o| if (o.onPark) |cb| cb(c);
    c.pending = .park;
    context.swap(&c.ctx, c.main_ctx);
}

/// Re-queue a coroutine that asked (or will ask) to park.
///
/// Three cases:
///   PARKED   → CAS to RUNNING, push to injection, wake a parked worker.
///   RUNNING  → CAS to NOTIFIED. Dispatch sees NOTIFIED in its `.park`
///              branch and re-queues immediately — no double-dispatch.
///   NOTIFIED → no-op (already queued for self-wake by dispatch).
pub fn unpark(c: *Coroutine) void {
    {
        const rt: *Runtime = @ptrCast(@alignCast(c.runtime));
        if (rt.observer) |o| if (o.onUnpark) |cb| cb(c);
    }
    while (true) {
        const s = c.park_state.load(.acquire);
        switch (s) {
            ParkState.PARKED => {
                if (c.park_state.cmpxchgWeak(
                    ParkState.PARKED,
                    ParkState.RUNNING,
                    .acq_rel,
                    .acquire,
                )) |_| continue;
                const rt: *Runtime = @ptrCast(@alignCast(c.runtime));
                // Push to a P's mailbox. If we're on an M, use that
                // M's P (preserves locality); otherwise default to
                // P[0]. Stealing will distribute work if needed.
                const target_p = if (worker_mod.currentM()) |m_raw| blk: {
                    const m: *M = @ptrCast(@alignCast(m_raw));
                    break :blk m.p;
                } else &rt.ps[0];
                target_p.mailbox.push(c);
                _ = target_p.stat_unparks_to_inject.fetchAdd(1, .monotonic);
                rt.wakeOneParked();
                return;
            },
            ParkState.RUNNING => {
                if (c.park_state.cmpxchgWeak(
                    ParkState.RUNNING,
                    ParkState.NOTIFIED,
                    .acq_rel,
                    .acquire,
                )) |_| continue;
                return;
            },
            ParkState.NOTIFIED => return,
            else => unreachable,
        }
    }
}

/// Direct handoff: dispatch `target` inline on the current M, returning
/// to the calling coroutine when `target` finishes, yields, or parks.
/// Saves the park→unpark→wake round-trip that a normal `Task.join`
/// would otherwise pay.
///
/// Returns `true` if `target` ran to completion (caller can proceed);
/// `false` otherwise — either we couldn't claim `target` from the
/// lifo slot, or target yielded/parked partway and is now queued or
/// waiting for an unparker. On `false`, the caller's contract is to
/// fall through to the normal blocking path (`parkOn` etc.).
///
/// **Precondition**: must be called from inside a coroutine on a worker
/// M. `target` must be in this M's P's `lifo_slot` (we only handle that
/// case in v1; future work extends to local WSQ).
///
/// Why this is safe wrt fairness:
/// * `target` was just spawned by the caller on this M; it hasn't been
///   stolen yet (lifo_slot is owner-only — siblings only steal from
///   the local queue).
/// * If we lose the lifo CAS, `target` already moved (evicted to local
///   queue by another spawn) and the caller takes the slow path — no
///   priority inversion.
pub fn tryDispatchInline(target: *Coroutine) bool {
    const m_raw = worker_mod.currentM() orelse return false;
    const m: *M = @ptrCast(@alignCast(m_raw));
    if (!m.p.tryRemoveLifo(target)) return false;

    const caller = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(caller.runtime));

    // Route target's swap-back to the caller's ctx, not the M's
    // dispatch loop. When target completes (or yields/parks), it
    // executes `context.swap(&target.ctx, target.main_ctx)` — which
    // is the caller's ctx, so we resume here in `tryDispatchInline`.
    target.pending = .done;
    target.main_ctx = &caller.ctx;

    current.set(target);
    context.swap(&caller.ctx, &target.ctx);
    // Back from target's swap-back. Restore the threadlocal `current`
    // to the caller — we're running on the caller's stack again.
    current.set(caller);

    switch (target.pending) {
        .done => {
            // Same cleanup as the M's dispatch `.done` branch. We
            // duplicate it here rather than calling into dispatch
            // because dispatch expects an M.main_ctx swap path.
            if (rt.observer) |o| if (o.onComplete) |cb| cb(target);
            if (target.task_done) |done| {
                done.store(task_mod.DONE, .release);
                _ = park_mod.unparkOne(rt, done);
            }
            if (target.task_thread_parker) |p| p.unpark();
            if (!target.has_task) {
                if (target.frame_destroy) |destroy_fn| destroy_fn(target.frame_ptr, rt.allocator);
            }
            _ = m.p.stat_done.fetchAdd(1, .monotonic);
            m.p.freeStack(target.stack);
            m.p.freeCoroutine(target, rt.allocator);
            return true;
        },
        .yield => {
            // Target yielded partway. Re-queue to local; some M will
            // pick it up. Caller falls through to its normal wait.
            // `target.main_ctx` is stale (points to caller.ctx) but
            // gets overwritten on next dispatch entry — no leak.
            m.p.pushQueue(target);
            return false;
        },
        .park => {
            // Target is mid-parking. Mirrors dispatch's `.park` branch:
            // CAS RUNNING → PARKED. If we observe NOTIFIED instead, an
            // unpark fired between the primitive's register step and
            // this swap-back; re-queue immediately rather than leaving
            // the coro stranded.
            if (target.park_state.cmpxchgStrong(
                ParkState.RUNNING,
                ParkState.PARKED,
                .acq_rel,
                .acquire,
            )) |observed| {
                std.debug.assert(observed == ParkState.NOTIFIED);
                target.park_state.store(ParkState.RUNNING, .release);
                m.p.mailbox.push(target);
                rt.wakeOneParked();
            }
            return false;
        },
    }
}

const ParkState = coroutine.ParkState;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    /// M[i] (OS thread) and P[i] (scheduler state) are 1:1-bound in
    /// Phase 1 of the M:N restructure. Later phases will allow
    /// dynamic re-binding via P.attached_m.
    ///
    /// Each P owns a `mailbox` (per-P MPMC queue); cross-P pushes
    /// target a specific P's mailbox instead of one global queue,
    /// reducing cache-line contention.
    ms: []M,
    ps: []P,
    // No struct-level default — Linux's `Reactor` is a tagged union
    // (epoll vs io_uring) and can't default-init from `.{}`. The init
    // site below names this field explicitly via `.reactor = reactor_inst`.
    reactor: Reactor,
    shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Bit i set ⇔ ms[i] is parked. M[0] is the driver thread.
    /// Cache-line padded — every worker fetchOr/fetchAnd/cmpxchg's this
    /// on every park/unpark cycle, and used to share a line with
    /// `num_searching` (also hot on every find-work cycle). Splitting
    /// removed the worst false-sharing hotspot in the runtime.
    parked_workers: std.atomic.Value(u64) align(std.atomic.cache_line) = std.atomic.Value(u64).init(0),
    /// Count of workers currently in the find-work phase of the
    /// dispatch loop. Anti-herd: `wakeOneParked` skips when > 0.
    num_searching: std.atomic.Value(u32) align(std.atomic.cache_line) = std.atomic.Value(u32).init(0),
    /// Missed-wakeup latch. Set by `wakeOneParked` when it bails
    /// because `num_searching > 0` (the anti-herd guard). Read +
    /// cleared by the worker loop AFTER it fetchSub's `num_searching`
    /// and BEFORE it commits to `parkWorker`. If observed set, the
    /// worker re-enters find-work instead of parking — closing the
    /// race where a pusher saw `num_searching > 0` while the
    /// searcher was already mid-fetchSub, and bailed on the wake
    /// expecting that searcher to find the work, but the searcher
    /// had already passed its last `tryFindAndDispatch` and was
    /// about to park.
    ///
    /// Modelled on Go's `sched.needspinning` (`runtime/proc.go`
    /// design doc lines 79-104, set/cleared sites at proc.go:3158,
    /// 3618). Without this latch, the lock-free `wakeOneParked` /
    /// `num_searching.fetchSub` / `parkWorker` triangle has a
    /// missed-wakeup window that Go closes via the explicit
    /// `needspinning → goto top` re-becomes-spinning path and
    /// Tokio closes via mutex-mediated idle-set transition
    /// (`idle.rs::transition_worker_to_parked`).
    need_spinning: std.atomic.Value(u32) align(std.atomic.cache_line) = std.atomic.Value(u32).init(0),
    /// CAS-claim "I am the current reactor poller".
    reactor_poller_taken: std.atomic.Value(bool) align(std.atomic.cache_line) = std.atomic.Value(bool).init(false),
    /// Shared parking lot — one wait/wake mechanism for every
    /// coroutine-level sync primitive (Mutex, Notify, Semaphore,
    /// channel block paths, Task.join, etc.). See `src/park.zig`
    /// and `docs/internals/parking-lot.md`.
    parking_lot: park_mod.ParkingLot,
    /// Slab arena that backs every coroutine stack. One `mmap` at
    /// `init`, one `munmap` at `deinit`; per-P pools cache freed
    /// slots so the steady-state hot path is allocation-free.
    stack_arena: stack_mod.Arena,
    /// OS-thread pool used by `volt.spawnBlocking`. Lazy — no
    /// threads spawn until the first submit. See `src/blocking_pool.zig`.
    blocking_pool: blocking_pool_mod.Pool,
    /// Per-P io_uring instances for async file I/O — Phase 2B.
    /// One ring per P (matches the universal "ring per OS thread"
    /// pattern from TigerBeetle / Glommio / Seastar, see
    /// `docs/internals/async-fs-io.md §3.1`). Indexed by `P.id`.
    /// Populated only on Linux, and only when every per-P ring
    /// init succeeded. Left null on Darwin / Windows, on older
    /// Linux kernels without io_uring, and on Linux when seccomp
    /// or sysctl blocks io_uring_setup. The Reactor's fs methods
    /// check this slice (Phase 2C) and fall back to the
    /// spawnBlocking proxy when null.
    fs_rings: ?[]fs_ring_mod.FsRing = null,
    /// Phase 2C.2: count of in-flight fs ring SQEs (kernel-side
    /// pending, not yet drained as CQEs). The reactor's `pending`
    /// counter doesn't include fs ring eventfds (those are
    /// wake signals, not registered I/O), so workers wouldn't
    /// otherwise know to claim the reactor poller role when the
    /// only outstanding work is a parked fs coroutine.
    /// Bumped by `fsRingRead/Write/etc` after submit; decremented
    /// by `drainFsRingInto` per CQE drained. `tryFindAndDispatch`
    /// enters the reactor poll path when this is > 0 (in addition
    /// to the existing `pendingCount > 0` check); `parkWorker`'s
    /// defensive checks similarly avoid parking when > 0.
    fs_in_flight: std.atomic.Value(u32) align(std.atomic.cache_line) =
        std.atomic.Value(u32).init(0),
    /// Optional instrumentation hook — see `Observer` doc. Read by
    /// the four hook sites (spawn, park, unpark, complete). Nullable
    /// so the hot path stays branch-and-fold when no observer is set.
    observer: ?*const Observer = null,
    // Diagnostic counters live per-P now (see `P.stat_*`) — they
    // were on Runtime as shared atomics, but every spawn / done /
    // unpark hit the same cache line, costing ~40 % of multi-worker
    // throughput. Summed at `dumpState` time.

    pub fn init(cfg: Config) !*Runtime {
        const n = cfg.workers orelse @max(1, try std.Thread.getCpuCount());
        if (n > MAX_WORKERS) return error.TooManyWorkers;

        const rt = try cfg.allocator.create(Runtime);
        errdefer cfg.allocator.destroy(rt);

        const ms = try cfg.allocator.alloc(M, n);
        errdefer cfg.allocator.free(ms);
        const ps = try cfg.allocator.alloc(P, n);
        errdefer cfg.allocator.free(ps);

        // Reactor backend is fixed per platform — epoll (Linux),
        // kqueue (Darwin), IOCP (Windows). io_uring exists in tree
        // but is not user-selectable; see reactor_linux.zig for the
        // future async-file-I/O plan.
        const reactor_inst = try Reactor.init(cfg.allocator);

        rt.* = .{
            .allocator = cfg.allocator,
            .ms = ms,
            .ps = ps,
            .reactor = reactor_inst,
            .parking_lot = park_mod.ParkingLot.init(),
            .stack_arena = try stack_mod.Arena.init(cfg.allocator, cfg.max_concurrent_stacks, cfg.stack_reservation_size),
            .blocking_pool = try blocking_pool_mod.Pool.init(cfg.allocator, cfg.blocking),
            .observer = cfg.observer,
        };
        errdefer rt.stack_arena.deinit(cfg.allocator);
        errdefer rt.blocking_pool.deinit();
        // Initialize each P, then bind each M to its P (1:1 in Phase 1).
        // Each P's stack pool cap = fair share of the arena across
        // workers, so asymmetric spawn (one driver, many workers)
        // can't pin every freed slot in one worker's pool.
        const fair_share: u32 = @intCast(@max(1, cfg.max_concurrent_stacks / n));
        for (rt.ps, 0..) |*p, i| {
            p.init(i, rt);
            p.arena = &rt.stack_arena;
            p.arena_usable_offset = rt.stack_arena.usableOffset();
            p.stack_pool_cap = fair_share;
        }
        for (rt.ms, 0..) |*m, i| m.init(&rt.ps[i]);

        // Per-P io_uring rings (Linux, Phase 2B). Graceful
        // degradation: if any per-P init fails — kernel too old,
        // sysctl-disabled, seccomp-blocked, ENOMEM — undo the
        // successful inits and leave `fs_rings = null`. The
        // Reactor's fs methods (Phase 2C) read this slice and fall
        // back to the spawnBlocking proxy when null, so the
        // failure mode is "no async fs win on this host", not
        // "Runtime.init fails".
        if (builtin.os.tag == .linux) {
            tryInitFsRings(rt, cfg.allocator);
            // Phase 2C.1: if the rings were created, wire each
            // ring's eventfd into the shared reactor's epoll +
            // install the wake handler. The handler implements the
            // Signal-Only-If-Idle protocol against parked_workers.
            // Failures here are non-fatal: we tear down what was
            // registered, free the rings, and leave fs_rings = null
            // so the spawnBlocking fallback (Phase 1) kicks in.
            if (rt.fs_rings != null) {
                wireFsRingWakeups(rt) catch {
                    if (rt.fs_rings) |rings| {
                        for (rings) |*r| r.deinit();
                        cfg.allocator.free(rings);
                        rt.fs_rings = null;
                    }
                };
            }
        }

        // Spawn pthread workers 1..N-1. M[0] is the driver thread
        // (the thread that called Runtime.init / will call
        // Runtime.run); it joins the pool in `run()`. This makes
        // the calling thread a first-class M rather than a special
        // parker-on-WG case.
        for (rt.ms[1..]) |*m| {
            m.thread = try std.Thread.spawn(.{}, workerThreadEntry, .{ rt, m });
        }
        return rt;
    }

    pub fn deinit(self: *Runtime) void {
        self.shutdown.store(true, .release);
        // Wake any spawned M currently blocked inside `reactor.poll(true)`.
        // `parker.unpark` below only wakes Ms parked on their own parker;
        // an M inside a blocking kevent / epoll_wait / submit_and_wait /
        // GetQueuedCompletionStatusEx is in a syscall and won't observe
        // the shutdown flag without this nudge. Combined with the
        // `!shutdown` guard in `tryFindAndDispatch`, the woken M
        // returns from poll, loops, and exits cleanly.
        self.reactor.interrupt();
        // Wake every spawned M so it observes shutdown. M[0] is the
        // driver thread — by this point it has already returned from
        // run() and is on the deinit path, so it doesn't need an unpark.
        for (self.ms[1..]) |*m| m.parker.unpark();
        for (self.ms[1..]) |*m| m.thread.join();
        for (self.ms) |*m| m.deinit();
        // Blocking pool teardown happens BEFORE reactor + parking_lot
        // teardown: a pool thread completing a job calls
        // `park.unparkOne(rt, ...)`, which touches the parking_lot.
        // Draining the pool first guarantees no such call races the
        // deinit. The pool itself blocks on in-flight jobs to
        // complete — caller contract is "drain spawnBlocking before
        // calling Runtime.deinit", same shape as the
        // `reactor.deinit pending == 0` invariant.
        self.blocking_pool.deinit();
        // Pools are quiescent now — every M has stopped dispatching,
        // so no further spawn/done can touch them. Drain coroutine
        // pools back to the allocator and stack pools back to the
        // arena; the arena's munmap happens after all Ps are done.
        for (self.ps) |*p| p.drainPools(self.allocator);
        // Per-P io_uring rings (if init populated them).
        if (self.fs_rings) |rings| {
            for (rings) |*r| r.deinit();
            self.allocator.free(rings);
            self.fs_rings = null;
        }
        self.stack_arena.deinit(self.allocator);
        self.allocator.free(self.ms);
        self.allocator.free(self.ps);
        self.reactor.deinit();
        self.parking_lot.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Comptime-typed spawn. Allocates one Combined{Frame, Task} struct
    /// + Coroutine + stack (3 allocations total — Coroutine and stack
    /// come from per-P pools after the first spawn), pushes coro to
    /// the current worker's local queue. Wakes a parked worker.
    ///
    /// The Frame + Task combine matches Go's `g` allocation shape:
    /// the per-spawn closure and the user-visible handle live in one
    /// allocation, freed together by `Task.join`. Saves one allocator
    /// round-trip per spawn vs the previous 4-alloc layout.
    pub fn spawn(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) SpawnError!*Task(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
        const FWT = task_mod.FrameWithTask(user_fn, @TypeOf(args));

        const combined = try self.allocator.create(FWT);
        errdefer self.allocator.destroy(combined);

        const owning_p: ?*P = if (worker_mod.currentM()) |m_raw| blk: {
            const m: *M = @ptrCast(@alignCast(m_raw));
            break :blk m.p;
        } else null;

        const c = if (owning_p) |p|
            try p.allocCoroutine(self.allocator)
        else
            try self.allocator.create(Coroutine);
        errdefer if (owning_p) |p| p.freeCoroutine(c, self.allocator) else self.allocator.destroy(c);

        // Driver-thread spawn (no owning P): pull the slot directly
        // from the arena. Worker-thread spawn: route through the
        // owning P's local pool, which falls back to the arena on
        // miss. Either way, free goes back to the arena (driver) or
        // the P's local LIFO (worker).
        const stack = if (owning_p) |p|
            try p.allocStack()
        else
            try self.stack_arena.alloc();
        errdefer if (owning_p) |p| p.freeStack(stack) else self.stack_arena.free(stack);

        combined.frame = .{ .args = args, .coro = c };
        c.* = .{
            .stack = stack,
            .frame_ptr = &combined.frame,
            .frame_destroy = &FWT.destroy,
            // main_ctx is set by the dispatching worker; placeholder here.
            .main_ctx = undefined,
            .runtime = self,
            .has_task = true,
        };
        // SP grows down from the top of the body region. The guard
        // page sits below — overflow walks into PROT_NONE and SIGSEGVs.
        const stack_top: [*]u8 = stack + self.stack_arena.slotSize();
        context.initContext(&c.ctx, stack_top, &combined.frame);

        combined.task = .{
            .coro = c,
            .result_ptr = &combined.frame.result,
            .frame_ptr = &combined.frame,
            .frame_destroy = &FWT.destroy,
            .allocator = self.allocator,
            .done = std.atomic.Value(u32).init(task_mod.NOT_DONE),
        };
        c.task_done = &combined.task.done;

        if (owning_p) |p| {
            _ = p.stat_spawned.fetchAdd(1, .monotonic);
        } else {
            _ = self.ps[0].stat_spawned.fetchAdd(1, .monotonic);
        }
        if (self.observer) |o| if (o.onSpawn) |cb| cb(c);
        self.pushNew(c);
        return &combined.task;
    }

    /// Push a freshly-spawned coroutine.
    ///
    /// If called from inside a coroutine on an M, uses that M's P's
    /// `pushLifo` so the just-spawned continuation has the lowest
    /// dispatch latency (single-slot LIFO cache, with the evicted
    /// slot landing in that P's local queue / mailbox).
    ///
    /// From a non-M context (driver pre-`run()`), pushes to P[0]'s
    /// mailbox — P[0] will be the driver in a moment.
    ///
    /// Wakes a parked M in either case.
    fn pushNew(self: *Runtime, c: *Coroutine) void {
        if (worker_mod.currentM()) |m_raw| {
            const m: *M = @ptrCast(@alignCast(m_raw));
            m.p.pushLifo(c);
        } else {
            self.ps[0].mailbox.push(c);
        }
        self.wakeOneParked();
    }

    /// Find a parked M via the bitmap, atomically claim it (clear
    /// its bit), unpark its Parker. Safe to call from any thread;
    /// no-op if no M is parked OR if another M is already searching
    /// for work (anti-herd guard).
    ///
    /// ## Anti-herd guard + missed-wakeup latch (Go's `wakep`)
    ///
    /// `num_searching > 0` ⇒ don't wake — the searcher will find
    /// the work via its dispatch cycle. But this opens a race:
    /// the "searcher" may have already fetchSub'd its share of
    /// `num_searching` and be in `parkWorker`'s commit path. To
    /// close it, we set `need_spinning = 1` when bailing, and the
    /// worker-loop's post-fetchSub recheck (see
    /// `workerLoopUntilShutdown` / `workerLoopUntilTaskDone`)
    /// observes that flag and retries `tryFindAndDispatch` instead
    /// of parking.
    ///
    /// The re-load of `num_searching` AFTER `need_spinning.store`
    /// closes the symmetric race: if the searcher fetchSub'd
    /// between our first load and our flag-set, our flag is
    /// useless (the searcher already passed its check); we must
    /// fall through to the real wake.
    pub fn wakeOneParked(self: *Runtime) void {
        // Anti-herd: any searcher will find the work — unless it
        // has already fetchSub'd and is on the parkWorker commit
        // path. The need_spinning latch + re-load handles that
        // narrow window.
        if (self.num_searching.load(.acquire) > 0) {
            self.need_spinning.store(1, .release);
            if (self.num_searching.load(.acquire) > 0) return;
            // Fall through — the searcher already transitioned out
            // and may not observe our latch. Do a real wake.
        }

        var bitmap = self.parked_workers.load(.acquire);
        while (bitmap != 0) {
            const idx = @ctz(bitmap);
            const bit: u64 = @as(u64, 1) << @intCast(idx);
            if (self.parked_workers.cmpxchgWeak(bitmap, bitmap & ~bit, .acq_rel, .acquire)) |observed| {
                bitmap = observed;
                continue;
            }
            // We cleared the bit for `idx`. Unpark that M.
            self.ms[idx].parker.unpark();
            return;
        }
    }

    fn markParked(self: *Runtime, m: *M) void {
        const bit: u64 = @as(u64, 1) << @intCast(m.p.id);
        _ = self.parked_workers.fetchOr(bit, .acq_rel);
    }

    fn unmarkParked(self: *Runtime, m: *M) void {
        const bit: u64 = @as(u64, 1) << @intCast(m.p.id);
        _ = self.parked_workers.fetchAnd(~bit, .acq_rel);
    }

    fn tryClaimPoller(self: *Runtime) bool {
        return self.reactor_poller_taken.cmpxchgStrong(false, true, .acq_rel, .acquire) == null;
    }

    fn releasePoller(self: *Runtime) void {
        self.reactor_poller_taken.store(false, .release);
    }

    /// Diagnostic — dumps the scheduler's atomic state to stderr.
    /// Safe to call from any thread (reads atomics with .acquire),
    /// but the snapshot is non-coherent across the multiple loads.
    /// Intended for hang investigation only.
    pub fn dumpState(self: *Runtime) void {
        std.debug.print("\n=== Runtime.dumpState ===\n", .{});
        std.debug.print("  shutdown:           {}\n", .{self.shutdown.load(.acquire)});
        std.debug.print("  parked_workers:     0b{b}\n", .{self.parked_workers.load(.acquire)});
        std.debug.print("  num_searching:      {d}\n", .{self.num_searching.load(.acquire)});
        std.debug.print("  reactor_poller:     {}\n", .{self.reactor_poller_taken.load(.acquire)});
        std.debug.print("  reactor_pending:    {d}\n", .{self.reactor.pendingCount()});
        // Per-P stat counters summed across all P's (sharded to
        // avoid cache-line contention on the hot path).
        var spawned: u64 = 0;
        var done: u64 = 0;
        var fairness_hits: u64 = 0;
        var unparks_to_inject: u64 = 0;
        for (self.ps) |*p| {
            spawned += p.stat_spawned.load(.monotonic);
            done += p.stat_done.load(.monotonic);
            fairness_hits += p.stat_fairness_hits.load(.monotonic);
            unparks_to_inject += p.stat_unparks_to_inject.load(.monotonic);
        }
        std.debug.print("  total_spawned:      {d}\n", .{spawned});
        std.debug.print("  total_done:         {d}\n", .{done});
        std.debug.print("  spawned - done:     {d}  ← non-zero = lost coroutines\n", .{spawned -% done});
        std.debug.print("  fairness_hits:      {d}\n", .{fairness_hits});
        std.debug.print("  unparks_to_inject:  {d}\n", .{unparks_to_inject});
        for (self.ms, self.ps, 0..) |*m, *p, i| {
            const lifo_set = p.lifo_slot.load(.acquire) != null;
            const local_empty = p.local.isEmpty();
            const mailbox_empty = p.mailbox.isEmpty();
            const parker_state = m.parker.state.load(.acquire);
            std.debug.print("  m[{d}]/p[{d}]: lifo_set={}, local_empty={}, mailbox_empty={}, parker_state={d}\n", .{ i, p.id, lifo_set, local_empty, mailbox_empty, parker_state });
        }
        std.debug.print("=========================\n", .{});
    }

    /// Run `user_fn(args)` as the root coroutine. The calling thread
    /// participates as worker 0 — it runs the dispatch loop alongside
    /// the spawned pthread workers, exiting only when the root
    /// coroutine's `done` flag is set.
    ///
    /// The driver thread isn't a coroutine, so the parking-lot path
    /// doesn't apply to wake it. Instead, the root Coroutine carries
    /// `task_thread_parker = &workers[0].parker`, and dispatch's
    /// `.done` branch directly unparks it when the root completes.
    pub fn run(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !@typeInfo(@TypeOf(user_fn)).@"fn".return_type.? {
        var task = try self.spawn(user_fn, args);
        // The driver is M[0]; wake its parker when the root task
        // completes (regardless of which M dispatches the root's
        // `.done`).
        task.coro.task_thread_parker = &self.ms[0].parker;
        // Calling thread becomes M[0] for the duration of run().
        // After this returns, deinit will shut down the other Ms.
        workerLoopUntilTaskDone(self, &self.ms[0], &task.done);
        return task.join();
    }

    /// Spawn `user_fn(args)` as a root coroutine and return a
    /// `*DetachedHandle(T)` without blocking the calling thread.
    /// The runtime's spawned worker threads (M[1..N-1]) drive
    /// dispatch; the caller's thread stays free to do other work
    /// (event loops, CLI parsing, embedding scenarios).
    ///
    /// Call `handle.wait()` from any OS thread to block until the
    /// root completes and consume its result. Requires
    /// `Config.workers >= 2` because the calling thread isn't
    /// available as M[0] — at least one spawned M must exist to
    /// drive dispatch. Returns `error.NeedMultipleWorkers` if not.
    ///
    /// **Lifetime**: the returned handle's storage is owned by the
    /// Runtime's allocator. `wait()` consumes the handle (don't use
    /// it after). Do not call `Runtime.deinit` before `wait()`.
    pub fn runDetached(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) (SpawnError || error{NeedMultipleWorkers})!*DetachedHandle(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
        if (self.ms.len < 2) return error.NeedMultipleWorkers;

        const Ret = @typeInfo(@TypeOf(user_fn)).@"fn".return_type.?;
        const Handle = DetachedHandle(Ret);

        var task = try self.spawn(user_fn, args);
        // Allocate handle; on failure roll back the spawn by aborting
        // before the task ever observed — but we can't un-spawn, so
        // err out of the allocation requires the caller to .wait()
        // anyway. Simpler to error here as SpawnError.OutOfMemory.
        const handle = try self.allocator.create(Handle);
        handle.* = .{ .task = task, .parker = .{}, .allocator = self.allocator };
        // When dispatch sees the root coroutine complete, it unparks
        // this parker — same mechanism `run` uses for M[0]'s parker.
        task.coro.task_thread_parker = &handle.parker;
        return handle;
    }

    /// Run `body(*Cancel, args...)` as the root coroutine. Installs
    /// a SIGINT handler (POSIX) that fires the `*Cancel` on signal.
    /// When the handler fires, every cancel-aware blocking call in
    /// the body's coroutine tree wakes with `error.Cancelled` and
    /// the body unwinds for graceful shutdown.
    ///
    /// **POSIX-only for v1.** On Windows this falls back to plain
    /// `run` with a no-op Cancel (Wave 2.5 will wire
    /// `SetConsoleCtrlHandler`).
    ///
    /// Body shape: `fn(*Cancel, ...args) E!T`. On natural body
    /// completion the cancel fires anyway (to release the internal
    /// signal-poller); the body's return value still surfaces.
    ///
    /// Caller contract: do not call concurrently from multiple
    /// runtimes in the same process — the signal handler dispatches
    /// via a single process-global fd.
    pub fn runWithSignals(
        self: *Runtime,
        comptime body: anytype,
        args: anytype,
    ) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
        if (comptime is_windows_rt) {
            return self.runWithSignalsNoOpWindows(body, args);
        }

        var pipe_fds: [2]c_int = undefined;
        if (posix_pipe(&pipe_fds) != 0) return error.PipeCreateFailed;
        defer _ = posix_close(pipe_fds[0]);
        defer _ = posix_close(pipe_fds[1]);

        // Non-blocking on both ends. The handler's `write` is in a
        // signal context and must not block; the poller drives reads
        // through the reactor's wait-readable path.
        reactor_mod.setNonblock(pipe_fds[0]) catch {};
        reactor_mod.setNonblock(pipe_fds[1]) catch {};

        sigint_pipe_write_fd.store(pipe_fds[1], .release);
        defer sigint_pipe_write_fd.store(-1, .release);

        const SIGINT: c_int = 2;
        var old_action: signal_mod.Sigaction = undefined;
        var new_action: signal_mod.Sigaction = std.mem.zeroes(signal_mod.Sigaction);
        new_action.sa_sigaction = @ptrCast(&sigintHandler);
        new_action.sa_flags = signal_mod.SA_SIGINFO;
        _ = signal_mod.sigaction_fn(SIGINT, &new_action, &old_action);
        defer _ = signal_mod.sigaction_fn(SIGINT, &old_action, null);

        const ArgsT = @TypeOf(args);
        const Internal = struct {
            fn run(read_fd: c_int, user_args: ArgsT) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
                var c = @import("cancel.zig").Cancel.init(@ptrCast(@alignCast(current.require().runtime)));
                defer c.deinit();

                const Poller = struct {
                    fn run(cn: *@import("cancel.zig").Cancel, fd: c_int) void {
                        const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
                        // Lazy-init a PollDesc for the self-pipe.
                        // pd_handle.release tears it down before the
                        // outer scope libc-closes the pipe fds.
                        var pd_slot: @import("pd_handle.zig").Atomic = .{};
                        defer @import("pd_handle.zig").release(&pd_slot, fd);
                        const pd = @import("pd_handle.zig").ensure(&pd_slot, rt, fd) catch return;
                        defer pd.decref();
                        rt.reactor.waitFdCancel(fd, pd, .read, cn) catch return;
                        cn.fire();
                    }
                };
                var poller = try (@import("lib.zig").spawn(Poller.run, .{ &c, read_fd }));

                const body_args = .{&c} ++ user_args;
                const result = @call(.auto, body, body_args);

                // Body done — fire cancel (idempotent if already
                // fired by the signal handler) so the poller wakes
                // and exits.
                c.fire();
                _ = poller.join();
                return result;
            }
        };

        return try self.run(Internal.run, .{ pipe_fds[0], args });
    }

    /// Windows fallback for `runWithSignals` — provides the Cancel
    /// to the body but doesn't install any signal handler. The body
    /// must fire the cancel itself (e.g. via custom Win32 console
    /// control handler) for graceful shutdown.
    fn runWithSignalsNoOpWindows(
        self: *Runtime,
        comptime body: anytype,
        args: anytype,
    ) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
        const ArgsT = @TypeOf(args);
        const Internal = struct {
            fn run(user_args: ArgsT) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
                var c = @import("cancel.zig").Cancel.init(@ptrCast(@alignCast(current.require().runtime)));
                defer c.deinit();
                const body_args = .{&c} ++ user_args;
                return @call(.auto, body, body_args);
            }
        };
        return try self.run(Internal.run, .{args});
    }
};

// ─── Phase 2B: per-P io_uring (fs) ring initialisation ────────────

/// Try to populate `rt.fs_rings` with one `FsRing` per P. All-or-
/// nothing: any per-P init failure undoes the successful ones and
/// leaves `rt.fs_rings = null`. The Reactor's fs path treats null
/// as "use spawnBlocking proxy", so the fallback is automatic.
///
/// Caller must already have checked `builtin.os.tag == .linux`.
fn tryInitFsRings(rt: *Runtime, allocator: std.mem.Allocator) void {
    const rings = allocator.alloc(fs_ring_mod.FsRing, rt.ps.len) catch return;
    var inited: usize = 0;
    while (inited < rings.len) : (inited += 1) {
        rings[inited] = fs_ring_mod.FsRing.init() catch {
            // Roll back what we've built, then bail.
            var j: usize = 0;
            while (j < inited) : (j += 1) rings[j].deinit();
            allocator.free(rings);
            return;
        };
    }
    rt.fs_rings = rings;
}

// ─── Phase 2C.1: fs ring wake-up wiring ───────────────────────────

/// Module-private back-pointer used by `fsRingWakeHandler`. Set
/// once at `Runtime.init` time, never changes. The handler runs
/// on the reactor poller's thread and has no other way to reach
/// the Runtime without it. Single-runtime-per-process for now —
/// see `is_windows_rt` block below for the same assumption on the
/// signal infrastructure.
var fs_ring_wake_rt: ?*Runtime = null;

/// Phase 2C.1: register each ring's eventfd with the shared
/// reactor's epoll and install the wake handler. Called from
/// `Runtime.init` after `tryInitFsRings` succeeded.
///
/// On any registration failure, returns the error — the caller
/// (init) treats this as fatal-to-fs-rings (not fatal-to-init)
/// and tears down the rings, falling back to spawnBlocking.
fn wireFsRingWakeups(rt: *Runtime) !void {
    const rings = rt.fs_rings.?; // guaranteed non-null by caller
    // Bind the module back-pointer first so the handler is safe
    // to call the moment we install it.
    fs_ring_wake_rt = rt;
    for (rings, 0..) |*r, i| {
        try r.registerEventFd();
        try rt.reactor.addFsRingEventFd(r.eventfd, @intCast(i));
    }
    rt.reactor.setFsWakeHandler(&fsRingWakeHandler);
}

/// Reactor poller invokes this when a per-P fs ring's eventfd
/// fires. Implements the Signal-Only-If-Idle protocol from
/// `docs/internals/phase-2c-design.md §2`:
///   1. Drain the eventfd (8-byte read, mandatory for
///      level-triggered epoll).
///   2. Load `parked_workers.acquire`.
///   3. If owner's bit is set, unpark the owner M. Otherwise drop
///      — the owner is running or searching and will pick up the
///      CQE on its next find-work iteration.
fn fsRingWakeHandler(p_id: u32) void {
    const rt = fs_ring_wake_rt orelse return;
    const rings = rt.fs_rings orelse return;
    if (p_id >= rings.len) return;
    rings[p_id].drainEventFd();

    // Signal-Only-If-Idle: only wake the owner M if its bit is
    // set in parked_workers. Otherwise the owner is in find-work
    // (Running or Searching state) and will drain its own ring
    // via the Layer 1 peek on the next iteration.
    const bit: u64 = @as(u64, 1) << @intCast(p_id);
    const bm = rt.parked_workers.load(.acquire);
    if ((bm & bit) != 0) {
        rt.ms[p_id].parker.unpark();
    }
}

/// Phase 2C.1 Layer 1: drain ready CQEs on `p`'s fs ring, resolve
/// each via `user_data → *FsOp`, write the result, and unpark the
/// waiting coroutine. Returns count drained. Safe to call from
/// the owner worker only (owner-only-drain decision, design memo
/// §4).
///
/// Called from `tryFindAndDispatch` (between popMailbox-fairness
/// and popLocal) and from `parkWorker` (after `markParked` and
/// inside the spin loop). The unparked coroutines land in this
/// P's mailbox (via `unpark`'s currentM-aware target selection),
/// where the existing `mailbox.isEmpty()` defensive check picks
/// them up and aborts the park.
///
/// Uses `unpark` rather than direct `p.pushQueue` because the
/// coroutine may be in PARKED state already (CQE arrived after
/// the dispatcher's swap-back) or in RUNNING state (CQE arrived
/// before swap-back). `unpark`'s CAS handles both transitions
/// correctly; a bare push would race with the swap-back's
/// PARK_STATE transition and risk double-dispatch.
fn drainFsRingInto(rt: *Runtime, p: *P) usize {
    if (builtin.os.tag != .linux) return 0;
    const rings = rt.fs_rings orelse return 0;
    if (p.id >= rings.len) return 0;
    var cqes: [16]@import("std").os.linux.io_uring_cqe = undefined;
    const n = rings[p.id].peekBatch(&cqes) catch return 0;
    for (cqes[0..n]) |cqe| {
        const op: *fs_ring_mod.FsOp = @ptrFromInt(cqe.user_data);
        const res: isize = cqe.res;
        op.result = .{
            .value = res,
            .err = if (res < 0) @intCast(-res) else 0,
        };
        unpark(op.coro);
    }
    if (n > 0) _ = rt.fs_in_flight.fetchSub(n, .acq_rel);
    return n;
}

// ─── Phase 2C.2: fs ring routing helpers ─────────────────────────
//
// Each helper attempts to route an fs op through the per-P
// io_uring ring. Returns non-null on success, null when the ring
// path is unavailable (no fs_rings on this Runtime, not on a
// worker M, SQ full, submit failed) — caller falls back to the
// Phase 1 spawnBlocking proxy. Callers must be in a coroutine
// (the spawnBlocking fallback also requires it).
//
// The FsOp is allocated on the caller's coroutine stack. The
// stack VA is stable for the coroutine's lifetime (CLAUDE.md
// invariant 5); the coroutine waits in `park()` for its CQE
// before returning, so the FsOp outlives the kernel's in-flight
// window. See `docs/internals/phase-2c-design.md §1`.

const fs_result = @import("reactor_fs.zig").FsResult;

inline fn currentPRing(rt: *Runtime) ?*fs_ring_mod.FsRing {
    const rings = rt.fs_rings orelse return null;
    const m_raw = worker_mod.currentM() orelse return null;
    const m: *M = @ptrCast(@alignCast(m_raw));
    if (m.p.id >= rings.len) return null;
    return &rings[m.p.id];
}

pub fn fsRingRead(fd: i32, buf: []u8, offset: u64) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepRead(fd, buf, offset, @intFromPtr(&op)) catch return null;
    _ = ring.flush() catch return null;
    _ = rt.fs_in_flight.fetchAdd(1, .acq_rel);
    park();
    return op.result;
}

pub fn fsRingWrite(fd: i32, buf: []const u8, offset: u64) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepWrite(fd, buf, offset, @intFromPtr(&op)) catch return null;
    _ = ring.flush() catch return null;
    _ = rt.fs_in_flight.fetchAdd(1, .acq_rel);
    park();
    return op.result;
}

pub fn fsRingFsync(fd: i32) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepFsync(fd, @intFromPtr(&op), false) catch return null;
    _ = ring.flush() catch return null;
    _ = rt.fs_in_flight.fetchAdd(1, .acq_rel);
    park();
    return op.result;
}

pub fn fsRingFdatasync(fd: i32) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepFsync(fd, @intFromPtr(&op), true) catch return null;
    _ = ring.flush() catch return null;
    _ = rt.fs_in_flight.fetchAdd(1, .acq_rel);
    park();
    return op.result;
}

// ─── runWithSignals — graceful-shutdown infrastructure ─────────────
//
// File-scope helpers used by Runtime.runWithSignals (which lives on
// Runtime as a method, below). SIGINT (and on POSIX SIGTERM) wire to
// a Cancel the user's root owns. The mechanism is the classic POSIX
// self-pipe: signal handler writes one byte; a poller coroutine waits
// on the read end via the reactor; on wake, fires the cancel.
//
// Async-signal-safety: the handler only does `write(fd, &byte, 1)`.
// One global pipe fd — signals are process-wide, so multi-runtime
// graceful-shutdown isn't supported in v1 (it'd race on this slot).

const is_windows_rt = builtin.os.tag == .windows;
const posix_pipe = if (is_windows_rt) {} else @extern(
    *const fn (fds: *[2]c_int) callconv(.c) c_int,
    .{ .name = "pipe" },
);
const posix_close = if (is_windows_rt) {} else @extern(
    *const fn (fd: c_int) callconv(.c) c_int,
    .{ .name = "close" },
);
const posix_write = if (is_windows_rt) {} else @extern(
    *const fn (fd: c_int, buf: [*]const u8, count: usize) callconv(.c) isize,
    .{ .name = "write" },
);

var sigint_pipe_write_fd: std.atomic.Value(c_int) = std.atomic.Value(c_int).init(-1);

fn sigintHandler(sig: c_int, info: *anyopaque, ctx: ?*anyopaque) callconv(.c) void {
    _ = sig;
    _ = info;
    _ = ctx;
    const fd = sigint_pipe_write_fd.load(.acquire);
    if (fd < 0) return;
    const byte: u8 = 1;
    _ = posix_write(fd, @ptrCast(&byte), 1);
}

/// Handle returned by `Runtime.runDetached`. The OS-thread parker
/// equivalent of `Task.join`: `wait()` blocks the calling thread
/// (not a coroutine) until the root completes.
pub fn DetachedHandle(comptime T: type) type {
    return struct {
        const Self = @This();
        task: *Task(T),
        parker: parker_mod.Parker,
        allocator: std.mem.Allocator,

        /// Block the calling OS thread until the root completes,
        /// return its result, free the task and the handle. Safe to
        /// call from any OS thread (NOT a coroutine — for that, use
        /// `Task.join`). The handle is consumed by this call.
        pub fn wait(self: *Self) T {
            // Park OS thread until dispatch's `.done` branch unparks
            // our parker. Loop in case of spurious wakes.
            while (self.task.done.load(.acquire) == task_mod.NOT_DONE) {
                self.parker.park();
            }
            const result = self.task.result_ptr.*;
            const allocator = self.allocator;
            self.task.frame_destroy(self.task.frame_ptr, self.task.allocator);
            allocator.destroy(self);
            return result;
        }

        /// Non-consuming completion check. Safe to call from any
        /// thread. Useful for polling instead of blocking.
        pub fn isDone(self: *const Self) bool {
            return self.task.done.load(.acquire) == task_mod.DONE;
        }
    };
}

/// Entry point for pthread-spawned Ms (M[1..N-1]).
/// Runs the dispatch loop until shutdown.
fn workerThreadEntry(rt: *Runtime, m: *M) void {
    worker_mod.currentMSet(@ptrCast(m));
    defer worker_mod.currentMSet(null);
    workerLoopUntilShutdown(rt, m);
}

/// M[0] (driver thread) loop variant: same dispatch as the spawned
/// Ms, but exits when the target task's `done` flag is set (root
/// coro complete) rather than waiting for shutdown.
fn workerLoopUntilTaskDone(rt: *Runtime, m: *M, target_done: *std.atomic.Value(u32)) void {
    worker_mod.currentMSet(@ptrCast(m));
    defer worker_mod.currentMSet(null);
    while (target_done.load(.acquire) == task_mod.NOT_DONE) {
        _ = rt.num_searching.fetchAdd(1, .acq_rel);
        if (tryFindAndDispatch(rt, m)) continue;
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        // Drain the need_spinning latch — see Runtime.need_spinning
        // doc + wakeOneParked. A pusher that bailed on our anti-herd
        // guard while we were fetchSub'ing here set this flag, and
        // is relying on us to retry the find-work loop instead of
        // parking. The swap is acq_rel so the next fetchAdd above
        // is happens-after any pusher's mailbox push that paired
        // with the latch set.
        if (rt.need_spinning.swap(0, .acq_rel) != 0) continue;
        if (target_done.load(.acquire) == task_mod.DONE) return;
        parkWorker(rt, m);
    }
}

/// Spawned-M loop variant: runs until shutdown.
fn workerLoopUntilShutdown(rt: *Runtime, m: *M) void {
    while (!rt.shutdown.load(.acquire)) {
        _ = rt.num_searching.fetchAdd(1, .acq_rel);
        if (tryFindAndDispatch(rt, m)) continue;
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        // Drain the need_spinning latch — see workerLoopUntilTaskDone
        // for the rationale.
        if (rt.need_spinning.swap(0, .acq_rel) != 0) continue;
        if (rt.shutdown.load(.acquire)) return;
        parkWorker(rt, m);
    }
}

/// Find one piece of work and dispatch it. Returns true if anything
/// was dispatched / reactor polled. Returns false if no work was
/// found anywhere — caller should park.
///
/// IMPORTANT: this function maintains the searching/dispatching
/// invariant. On entry, the caller has fetchAdd'd num_searching
/// (worker is "in find-work phase"). When we commit to dispatching,
/// we fetchSub *before* the ctx swap so other pushers correctly see
/// that we're no longer searching. (A worker mid-dispatch can't
/// pick up new work; it must not count as searching.)
/// Every Nth dispatch, check the injection queue BEFORE the local
/// queue. Without this, a worker whose local queue is repeatedly
/// fed (e.g. a tight yield loop) never observes coroutines that
/// get unparked into the injection queue — the local-first
/// priority starves them. Mirrors Go's `schedule.checkGlobalRunq`.
const INJECTION_CHECK_INTERVAL: u32 = 61;

fn tryFindAndDispatch(rt: *Runtime, m: *M) bool {
    m.p.dispatch_count +%= 1;

    // Periodic fairness: check own mailbox before local. Prevents
    // starvation when local is fed by tight yield loops.
    if (m.p.dispatch_count % INJECTION_CHECK_INTERVAL == 0) {
        if (m.p.popMailbox()) |c| {
            _ = m.p.stat_fairness_hits.fetchAdd(1, .monotonic);
            _ = rt.num_searching.fetchSub(1, .acq_rel);
            dispatch(rt, m, c);
            return true;
        }
    }

    // Phase 2C.1 Layer 1: drain own P's fs ring CQEs into local.
    // Userspace-only (peek_cqe is two atomic loads); resumed
    // coroutines get picked up by the popLocal check below in
    // the same iteration. Sits BEFORE popLocal so a single
    // iteration covers both pre-existing local work AND
    // just-arrived CQEs.
    _ = drainFsRingInto(rt, m.p);

    if (m.p.popLocal()) |c| {
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        dispatch(rt, m, c);
        return true;
    }
    if (m.p.popMailbox()) |c| {
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        dispatch(rt, m, c);
        return true;
    }
    if (stealFromSiblings(rt, m)) |c| {
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        dispatch(rt, m, c);
        return true;
    }
    if ((rt.reactor.pendingCount() > 0 or rt.fs_in_flight.load(.acquire) > 0) and rt.tryClaimPoller()) {
        // Don't enter the blocking poll if shutdown has fired: we'd
        // immediately be woken by `reactor.interrupt()` (deinit
        // always fires one), but a worker re-checking pending after
        // shutdown could otherwise spin entering+exiting poll. Just
        // surrender the claim; caller decrements num_searching and
        // re-checks shutdown.
        if (rt.shutdown.load(.acquire)) {
            rt.releasePoller();
            return false;
        }
        // Drop searching count while inside the blocking kevent —
        // this thread can't pick up other work while in the syscall,
        // and pushers should be able to wake parked siblings.
        _ = rt.num_searching.fetchSub(1, .acq_rel);
        _ = rt.reactor.poll(true);
        rt.releasePoller();
        return true;
    }
    return false;
}

/// Park this M. Adds self to parked_workers bitmap, rechecks its P's
/// queues + reactor for race-arrived work, then blocks on the parker.
/// Cross-thread spawns / unparks wake via wakeOneParked.
///
/// Called from a non-searching state — the caller has already
/// fetchSub'd num_searching when it decided not to find more work.
/// Pre-park spin budget. When find-work returns nothing, retry the
/// cheap arrival checks this many times before committing to a real
/// `parker.park()` syscall. Saves a park+unpark roundtrip (~600 ns
/// on Darwin) when bursty spawners push work into our mailbox
/// within a few microseconds of us idling out.
///
/// Tunable per workload: high values hurt idle-CPU; low values miss
/// the burst window. 64 is the Tokio default analog and matches
/// micro-bench measurements on Apple Silicon (work arrival window
/// 200ns – 5us in spawn-heavy shapes).
const SPIN_BEFORE_PARK: u32 = 64;

fn parkWorker(rt: *Runtime, m: *M) void {
    rt.markParked(m);
    defer rt.unmarkParked(m);
    // Recheck — work may have arrived between findWork and mark.
    // We deliberately do NOT sniff sibling queues here: the
    // missed-wakeup race that a sniff used to address is now
    // closed by `Runtime.need_spinning`, drained in the worker
    // loop BEFORE we reach this point. A sniff at park time
    // would be an incomplete patch (work can sit in a sibling's
    // lifo_slot / local WSQ too, not just mailbox), and racy
    // against the sibling's owner popping concurrently.
    //
    // Phase 2C.1: drain own P's fs ring BEFORE the local-empty
    // check — `drainFsRingInto` pushes resumed coroutines onto
    // `m.p.local`, so the existing `local.isEmpty()` check
    // naturally catches them. Must sit between `markParked` and
    // the work-source checks (intent-to-park invariant from
    // design memo §2): the bit is up, so any CQE that arrives
    // after this drain still triggers the Layer 2 eventfd path
    // and signals us.
    _ = drainFsRingInto(rt, m.p);
    if (m.p.lifo_slot.load(.acquire) != null) return;
    if (!m.p.local.isEmpty()) return;
    if (!m.p.mailbox.isEmpty()) return;
    if (rt.reactor.pendingCount() > 0) return;
    if (rt.fs_in_flight.load(.acquire) > 0) return;
    if (rt.shutdown.load(.acquire)) return;

    // Spin briefly before committing to a real park. Bursty spawn
    // patterns push work into our mailbox within microseconds; the
    // park + later unpark syscall pair costs ~600 ns on Darwin,
    // dwarfing the cost of a few hundred ns of spin-checks.
    var spins: u32 = 0;
    while (spins < SPIN_BEFORE_PARK) : (spins += 1) {
        std.atomic.spinLoopHint();
        // Re-drain on each spin iteration — catches CQEs that
        // arrive during the spin window without paying the
        // eventfd → parker roundtrip.
        _ = drainFsRingInto(rt, m.p);
        if (m.p.lifo_slot.load(.acquire) != null) return;
        if (!m.p.mailbox.isEmpty()) return;
        if (!m.p.local.isEmpty()) return;
        if (rt.reactor.pendingCount() > 0) return;
        if (rt.fs_in_flight.load(.acquire) > 0) return;
        if (rt.shutdown.load(.acquire)) return;
    }

    m.parker.park();
}

/// Steal from a sibling P. Tries to steal a batch from the sibling's
/// local queue first (preserves the work-stealing protocol); if
/// that fails, peeks at the sibling's mailbox (single-pop fallback,
/// since mailbox is MPMC). Returns one coroutine to dispatch.
///
/// Attempts capped to `⌈log₂(N)⌉ + 1` siblings per call (with a
/// floor of 4 to keep small-N coverage). With a random start per
/// call, different searching workers cover different subsets, so
/// the collective coverage is still ~N siblings per few cycles.
/// Capping reduces CAS contention on hot siblings at high worker
/// counts; if work was missed, the next worker's wakeOneParked
/// (always fired on push) gives us another chance with a fresh
/// random start.
fn stealFromSiblings(rt: *Runtime, self: *M) ?*Coroutine {
    if (rt.ps.len <= 1) return null;
    const start: usize = self.p.rng.random().uintLessThan(usize, rt.ps.len);
    const log_n = std.math.log2_int_ceil(usize, rt.ps.len);
    const max_attempts = @max(@as(usize, 4), log_n + 1);
    const cap = @min(max_attempts, rt.ps.len);
    var attempts: usize = 0;
    while (attempts < cap) : (attempts += 1) {
        const idx = (start + attempts) % rt.ps.len;
        if (idx == self.p.id) continue;
        const sibling = &rt.ps[idx];
        if (sibling.local.stealInto(&self.p.local)) |c| return c;
        if (sibling.mailbox.pop()) |c| return c;
    }
    return null;
}

fn dispatch(rt: *Runtime, m: *M, c: *Coroutine) void {
    c.pending = .done;
    c.main_ctx = &m.main_ctx;
    current.set(c);
    context.swap(&m.main_ctx, &c.ctx);
    current.clear();
    switch (c.pending) {
        .yield => m.p.pushQueue(c),
        .park => {
            // Transition RUNNING → PARKED. If we observe NOTIFIED
            // instead, an unpark fired between the primitive's
            // waiter-register step and this swap-back. The
            // coroutine is logically wake-pending; re-queue
            // immediately rather than leaving it stranded.
            if (c.park_state.cmpxchgStrong(
                ParkState.RUNNING,
                ParkState.PARKED,
                .acq_rel,
                .acquire,
            )) |observed| {
                std.debug.assert(observed == ParkState.NOTIFIED);
                c.park_state.store(ParkState.RUNNING, .release);
                m.p.mailbox.push(c);
                rt.wakeOneParked();
            }
        },
        .done => {
            if (rt.observer) |o| if (o.onComplete) |cb| cb(c);
            // Signal the joiner. The parking-lot's validator-under-
            // lock guarantees no register-then-park race here: a
            // joiner that observes `done == DONE` in its validator
            // bails without parking; otherwise it's enqueued under
            // the bucket lock and our `unparkOne` pops it.
            if (c.task_done) |done| {
                done.store(task_mod.DONE, .release);
                _ = park_mod.unparkOne(rt, done);
            }
            // The driver thread (worker 0) parks via its Parker,
            // not via the parking lot. The root task carries a
            // direct Parker pointer; wake it here.
            //
            // ALSO fire reactor.interrupt() — the driver may be
            // blocked in reactor.poll(true) via tryFindAndDispatch
            // rather than in parker.park(); the parker.unpark stores
            // NOTIFIED but the driver is waiting on kqueue, not the
            // parker. Without the interrupt, the driver hangs on
            // kqueue waiting for an event that may never come.
            // See https://github.com/NerdMeNot/volt/issues/1.
            if (c.task_thread_parker) |p| {
                p.unpark();
                rt.reactor.interrupt();
            }
            if (!c.has_task) {
                if (c.frame_destroy) |destroy_fn| destroy_fn(c.frame_ptr, rt.allocator);
            }
            _ = m.p.stat_done.fetchAdd(1, .monotonic);
            // Recycle the stack + Coroutine into the current P's pools
            // (LIFO, cache-warm). Handing them to the next spawn on
            // this P avoids the allocator round-trip.
            m.p.freeStack(c.stack);
            m.p.freeCoroutine(c, rt.allocator);
        },
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// `volt.testing.allocator` — leak-detecting + multi-worker-safe.
// `std.testing.allocator`'s DebugAllocator captures stack traces via
// `debug.SelfInfo`'s module map, which the unwinder walks without
// synchronization. Multi-worker spawn corrupts that map and crashes
// with EXC_BAD_ACCESS. Our wrapper sets `stack_trace_frames = 0`
// (kills the unwinder race) and `thread_safe = true` (DebugAllocator's
// own bookkeeping mutex).
const test_allocator = @import("testing.zig").allocator;

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

// Phase 2B: verify the per-P io_uring rings are populated when the
// environment supports them. On Linux with a usable kernel + perms
// (the scripts/probe-linux.sh "READY" path) every P gets a ring.
// On non-Linux, on older kernels, or when seccomp/sysctl blocks
// io_uring, fs_rings stays null and fs ops will fall back to
// spawnBlocking once Phase 2C wires the Reactor surface.
test "runtime: fs_rings populated when io_uring is available" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 3 });
    defer rt.deinit();
    if (builtin.os.tag != .linux) {
        try std.testing.expect(rt.fs_rings == null);
        return;
    }
    // On Linux: rings should exist iff the kernel and sandbox
    // permit io_uring_setup. If they don't, fs_rings stays null
    // (graceful degradation, not an init failure). Don't hard-assert
    // either way — the probe script is the authoritative test for
    // io_uring availability.
    if (rt.fs_rings) |rings| {
        try std.testing.expectEqual(@as(usize, 3), rings.len);
        // Every ring accepted at least the bare flags=0 tier.
        for (rings) |r| try std.testing.expect(r.flags_used != 0xffff_ffff);
    }
}

// Phase 2C.1: verify the wake-up wiring is in place when fs_rings
// is populated. Each ring should have an eventfd registered (>= 0),
// the runtime's back-pointer for the wake handler should be set,
// and a manual SQE submission should both land a CQE AND mark the
// eventfd readable (proves the Layer 2 path is hot).
test "runtime: fs ring eventfd is wired into the shared reactor" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();

    const rings = rt.fs_rings orelse return error.SkipZigTest; // probe-blocked env
    for (rings) |r| try std.testing.expect(r.eventfd >= 0);
    // Module back-pointer set by `wireFsRingWakeups`.
    try std.testing.expect(fs_ring_wake_rt == rt);

    // Submit a no-op-ish fsync on stderr against ring 0, drive the
    // CQE to completion, and verify Layer 1's drainFsRingInto would
    // pick it up. We can't easily test the eventfd → handler path
    // from a unit test (it requires a worker actually being parked),
    // so this just exercises the prep + flush + drain plumbing.
    try rings[0].prepFsync(2, 0xC1_C1_C1_C1, false);
    _ = try rings[0].flush();
    var cqes: [1]@import("std").os.linux.io_uring_cqe = undefined;
    _ = try rings[0].waitOne(&cqes);
    try std.testing.expectEqual(@as(u64, 0xC1_C1_C1_C1), cqes[0].user_data);
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
    try (try rt.run(fanOutRoot, .{&counter}));
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
    try (try rt.run(yieldRoot, .{&counter}));
    try std.testing.expectEqual(@as(u32, 20), counter.load(.acquire));
}

// ─── tryDispatchInline tests ──────────────────────────────────────────

fn returnFortyTwo() u32 {
    return 42;
}

fn inlineRoot(out: *u32) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    // Spawn a task that will sit in our P's lifo_slot.
    const task = try rt.spawn(returnFortyTwo, .{});
    // Direct-handoff dispatch the spawned coro. After this returns
    // true, target.coro and its stack are freed; task struct + frame
    // still alive for join().
    const did_handoff = tryDispatchInline(task.coro);
    if (!did_handoff) @panic("expected lifo handoff to succeed on freshly-spawned task");
    // task.done should now be DONE — join returns immediately.
    out.* = task.join();
}

test "runtime: tryDispatchInline runs target inline and join returns result" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var out: u32 = 0;
    try (try rt.run(inlineRoot, .{&out}));
    try std.testing.expectEqual(@as(u32, 42), out);
}

fn inlineMissRoot(out: *u32) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    // Spawn TWO tasks. The second push evicts the first from lifo_slot
    // into the local queue. So the FIRST task is no longer in lifo,
    // and tryDispatchInline must return false for it.
    const t1 = try rt.spawn(returnFortyTwo, .{});
    const t2 = try rt.spawn(returnFortyTwo, .{});
    const got1 = tryDispatchInline(t1.coro);
    if (got1) @panic("expected lifo handoff to MISS for evicted t1");
    // The second (still in lifo) should hit.
    const got2 = tryDispatchInline(t2.coro);
    if (!got2) @panic("expected lifo handoff to hit for fresh t2");
    out.* = t1.join() + t2.join();
}

test "runtime: tryDispatchInline returns false when target is not in lifo_slot" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var out: u32 = 0;
    try (try rt.run(inlineMissRoot, .{&out}));
    try std.testing.expectEqual(@as(u32, 84), out);
}

fn yieldThenReturn() u32 {
    yield(); // pending = .yield → swap back to whoever dispatched us
    return 7;
}

fn inlineYieldRoot(out: *u32) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    // Spawn a coro that yields immediately, then call tryDispatchInline.
    // Expected: tryDispatchInline returns FALSE (target yielded, not done).
    // The target lands back on the local queue and gets dispatched by the
    // normal loop. The subsequent task.join completes normally.
    const t = try rt.spawn(yieldThenReturn, .{});
    const got = tryDispatchInline(t.coro);
    if (got) @panic("tryDispatchInline must return false when target yielded");
    out.* = t.join();
}

test "runtime: tryDispatchInline returns false when target yields, target completes via normal dispatch" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var out: u32 = 0;
    try (try rt.run(inlineYieldRoot, .{&out}));
    try std.testing.expectEqual(@as(u32, 7), out);
}

const ParkCtx = struct {
    note: *@import("sync.zig").Notify,
    flag: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn waitOnNotify(ctx: *ParkCtx) u32 {
    ctx.note.wait();
    _ = ctx.flag.fetchAdd(1, .acq_rel);
    return 99;
}

fn waker(ctx: *ParkCtx) void {
    // Yield until the waiter has had a chance to park.
    while (ctx.flag.load(.acquire) == 0) {
        // Tiny pause; spin if waiter still queued.
        yield();
        // After one yield, if the waiter parked, notify will wake it.
        ctx.note.notifyOne();
        yield();
        break;
    }
}

fn inlineParkRoot(out: *u32) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var note = @import("sync.zig").Notify.init();
    defer note.deinit();
    var pctx = ParkCtx{ .note = &note };

    // Spawn the waiter, then tryDispatchInline. The waiter will park
    // inside `note.wait()`. tryDispatchInline must return false and the
    // waiter must end up properly registered in the parking lot for the
    // subsequent waker to unpark it.
    const waiter_t = try rt.spawn(waitOnNotify, .{&pctx});
    const got = tryDispatchInline(waiter_t.coro);
    if (got) @panic("tryDispatchInline must return false when target parks");

    // Spawn the waker which fires the notify, then join everyone.
    const waker_t = try rt.spawn(waker, .{&pctx});
    waker_t.join();
    out.* = waiter_t.join();
}

test "runtime: tryDispatchInline returns false when target parks, target completes after unpark" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    var out: u32 = 0;
    try (try rt.run(inlineParkRoot, .{&out}));
    try std.testing.expectEqual(@as(u32, 99), out);
}

fn noopRoot() void {}

test "runtime: deinit unblocks a worker stuck inside reactor.poll" {
    // Provokes the race the Phase 6 follow-up fixes: a worker is
    // inside `reactor.poll(true)` when deinit is called. Before
    // the fix, `parker.unpark` doesn't reach a thread inside a
    // syscall, so deinit hangs forever on `m.thread.join()`.
    //
    // Darwin-only because Linux's `Reactor` is a tagged union over
    // {epoll, io_uring} and doesn't expose `pending` at the top
    // level. The deinit wire-up + tryFindAndDispatch guard tested
    // here run on every platform; the interrupt() mechanism each
    // backend dispatches to is covered in its respective unit test
    // (reactor_kqueue.zig has one; epoll/io_uring/iocp gated by
    // the cross-compile + their own future direct tests).
    if (comptime @import("builtin").os.tag != .macos) return error.SkipZigTest;

    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });

    // run() exits when the root coro returns; spawned Ms remain in
    // workerLoopUntilShutdown. With pending bumped artificially,
    // any worker that enters tryFindAndDispatch's "claim poller"
    // branch will enter poll(true). The dispatcher's reactor-poll
    // branch fires only on pendingCount > 0.
    try rt.run(noopRoot, .{});

    rt.reactor.pending.store(1, .release);

    // Brief spin so at least one of the two workers actually enters
    // poll. The spin doesn't need to be tight — if no worker is in
    // poll when interrupt fires, the test still passes (interrupt
    // becomes a no-op and the parker.unpark path drives shutdown).
    // The 1M iterations gives plenty of opportunity to enter kevent
    // without depending on a wall-clock sleep API.
    var i: u32 = 0;
    while (i < 1_000_000) : (i += 1) std.atomic.spinLoopHint();

    // Reset pending so reactor.deinit's `pending == 0` assertion
    // holds — we only bumped it to provoke the race, not because
    // any real coroutine is parked.
    rt.reactor.pending.store(0, .release);

    // Test passes if deinit returns; it hangs forever if the fix
    // is missing.
    rt.deinit();
}

// ─── runDetached tests ──────────────────────────────────────────────

fn detachedAdd(a: u32, b: u32) u32 {
    return a + b;
}

test "runDetached: caller thread doesn't block; wait() retrieves result" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();

    // Spawning detached returns immediately to the caller's OS thread —
    // the spawned M[1] picks up the root and runs it. The caller does
    // unrelated work (here: a brief atomic spin) before blocking on
    // wait(). Whether the task completed before, during, or after the
    // spin, wait() returns the result either way.
    const handle = try rt.runDetached(detachedAdd, .{ @as(u32, 19), @as(u32, 23) });
    var i: u32 = 0;
    while (i < 1_000_000) : (i += 1) std.atomic.spinLoopHint();
    const result = handle.wait();
    try std.testing.expectEqual(@as(u32, 42), result);
}

test "runDetached: workers=1 returns error.NeedMultipleWorkers" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    const r = rt.runDetached(detachedAdd, .{ @as(u32, 1), @as(u32, 2) });
    try std.testing.expectError(error.NeedMultipleWorkers, r);
}

// ─── Observer hook test ────────────────────────────────────────────

// Process-global counters — the Observer hooks are bare function
// pointers (no per-callback context capability in v1), so we have
// to use file-scope state to communicate from the callbacks back to
// the test.
var test_obs_spawn: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
var test_obs_complete: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

fn testObsOnSpawn(c: *Coroutine) void {
    _ = c;
    _ = test_obs_spawn.fetchAdd(1, .acq_rel);
}
fn testObsOnComplete(c: *Coroutine) void {
    _ = c;
    _ = test_obs_complete.fetchAdd(1, .acq_rel);
}

fn observerTinyTask() void {}

fn observerRoot() !void {
    // Spawn five trivial tasks; each fires onSpawn at queue time
    // and onComplete at dispatch's `.done` branch.
    var tasks: [5]*Task(void) = undefined;
    for (&tasks) |*t| t.* = try @import("lib.zig").spawn(observerTinyTask, .{});
    for (tasks) |t| _ = t.join();
}

test "Observer: onSpawn + onComplete fire once per task" {
    test_obs_spawn.store(0, .release);
    test_obs_complete.store(0, .release);
    var obs = Observer{
        .onSpawn = &testObsOnSpawn,
        .onComplete = &testObsOnComplete,
    };
    var rt = try Runtime.init(.{
        .allocator = test_allocator,
        .workers = 2,
        .observer = &obs,
    });
    defer rt.deinit();
    try (try rt.run(observerRoot, .{}));
    // 5 user tasks + 1 root = 6 onSpawn / 6 onComplete.
    try std.testing.expectEqual(@as(u32, 6), test_obs_spawn.load(.acquire));
    try std.testing.expectEqual(@as(u32, 6), test_obs_complete.load(.acquire));
}

// ─── runWithSignals tests ──────────────────────────────────────────

fn runWithSignalsNoSigBody(c: *@import("cancel.zig").Cancel) !void {
    _ = c;
    // Body returns immediately — exercises the setup/teardown path
    // without ever needing a real signal.
}

test "runWithSignals: clean setup + teardown when body returns" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    try rt.runWithSignals(runWithSignalsNoSigBody, .{});
}

const SignalCtx = struct {
    cancelled_observed: bool = false,
};

// Body that parks on a long sleep via the shared Cancel. When the
// SIGINT handler fires the cancel, sleepCancel returns
// error.Cancelled and the body sets a flag we assert on.
fn runWithSignalsCancelBody(
    c: *@import("cancel.zig").Cancel,
    ctx: *SignalCtx,
) !void {
    const lib = @import("lib.zig");
    // Spawn a side coro that raise()s SIGINT after yielding a few
    // times — that gives the poller time to install + park.
    const Raiser = struct {
        fn run() void {
            var i: u32 = 0;
            while (i < 100) : (i += 1) lib.yield();
            const raise_fn = @extern(*const fn (sig: c_int) callconv(.c) c_int, .{ .name = "raise" });
            _ = raise_fn(2); // SIGINT
        }
    };
    const raiser = lib.spawn(Raiser.run, .{}) catch return;

    // Park on sleepCancel for far longer than the test could
    // possibly need — the signal-driven cancel.fire is the only
    // way this completes within reasonable time.
    lib.sleepCancel(lib.Duration.fromSecs(60), c) catch |e| switch (e) {
        error.Cancelled => ctx.cancelled_observed = true,
        else => {},
    };

    _ = raiser.join();
}

test "runWithSignals: SIGINT fires Cancel; body unwinds gracefully" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    var ctx = SignalCtx{};
    try rt.runWithSignals(runWithSignalsCancelBody, .{&ctx});
    try std.testing.expect(ctx.cancelled_observed);
}

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
    /// **Linux only.** Chooses between epoll and io_uring backends.
    /// `.auto` probes for io_uring at init and falls back to epoll
    /// on older kernels (< 5.10) or sysctl-disabled environments.
    /// Ignored on Darwin (always kqueue) and Windows (always IOCP).
    io_backend: reactor_mod.IoBackend = .auto,
    /// OS-thread pool used by `volt.spawnBlocking` to run sync /
    /// CPU-bound code without pinning a worker. Lazy — no threads
    /// spawn until the first `spawnBlocking` call. Default caps
    /// concurrent sync work at 128 threads; raise for sync-IO-heavy
    /// services. See `src/blocking_pool.zig`.
    blocking: blocking_pool_mod.Config = .{},
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
    parked_workers: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Count of workers currently in the find-work phase of the
    /// dispatch loop. Anti-herd: `wakeOneParked` skips when > 0.
    num_searching: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// CAS-claim "I am the current reactor poller".
    reactor_poller_taken: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
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

        // Reactor init: on Linux, the io_backend config selects
        // between epoll and io_uring (tagged union dispatch in
        // reactor_linux.zig). On other platforms `io_backend` is
        // accepted but ignored — kqueue or IOCP is fixed.
        const reactor_inst = if (@import("builtin").os.tag == .linux)
            switch (cfg.io_backend) {
                .auto => try Reactor.init(),
                .epoll => try Reactor.initBackend(.epoll),
                .io_uring => try Reactor.initBackend(.io_uring),
            }
        else
            try Reactor.init();

        rt.* = .{
            .allocator = cfg.allocator,
            .ms = ms,
            .ps = ps,
            .reactor = reactor_inst,
            .parking_lot = park_mod.ParkingLot.init(),
            .stack_arena = try stack_mod.Arena.init(cfg.allocator, cfg.max_concurrent_stacks, cfg.stack_reservation_size),
            .blocking_pool = try blocking_pool_mod.Pool.init(cfg.allocator, cfg.blocking),
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
    pub fn wakeOneParked(self: *Runtime) void {
        // Anti-herd: if any M is already searching, it will find
        // the new work on its current or next dispatch cycle. No
        // need to wake a parked sibling — that's just thrashing
        // the bitmap cache line + paying ulock_wake overhead.
        if (self.num_searching.load(.acquire) > 0) return;

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
};

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
        // Enter find-work phase: count this M as searching.
        // Anti-herd: pushers see num_searching > 0 and skip the
        // bitmap CAS + ulock_wake, knowing we'll pick up their push.
        _ = rt.num_searching.fetchAdd(1, .acq_rel);
        if (tryFindAndDispatch(rt, m)) {
            // tryFindAndDispatch decrements num_searching on hit
            // before swapping into the coroutine. We're back from
            // the swap now; loop iterates and fetchAdds again.
            continue;
        }
        // No work found. Leave find-work phase and park.
        _ = rt.num_searching.fetchSub(1, .acq_rel);
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
    if (rt.reactor.pendingCount() > 0 and rt.tryClaimPoller()) {
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
fn parkWorker(rt: *Runtime, m: *M) void {
    rt.markParked(m);
    defer rt.unmarkParked(m);
    // Recheck — work may have arrived between findWork and mark.
    if (m.p.lifo_slot.load(.acquire) != null) return;
    if (!m.p.local.isEmpty()) return;
    if (!m.p.mailbox.isEmpty()) return;
    // Don't sniff sibling mailboxes here — stealing will find them
    // on the next loop iteration if we get woken.
    if (rt.reactor.pendingCount() > 0) return;
    if (rt.shutdown.load(.acquire)) return;
    m.parker.park();
}

/// Steal from a sibling P. Tries to steal a batch from the sibling's
/// local queue first (preserves the work-stealing protocol); if
/// that fails, peeks at the sibling's mailbox (single-pop fallback,
/// since mailbox is MPMC). Returns one coroutine to dispatch.
fn stealFromSiblings(rt: *Runtime, self: *M) ?*Coroutine {
    if (rt.ps.len <= 1) return null;
    const start: usize = self.p.rng.random().uintLessThan(usize, rt.ps.len);
    var attempts: usize = 0;
    while (attempts < rt.ps.len) : (attempts += 1) {
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
            if (c.task_thread_parker) |p| p.unpark();
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

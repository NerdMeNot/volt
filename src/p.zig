//! P — processor / scheduler unit (M:N scheduler).
//!
//! Owns the per-worker scheduler state: run queue, lifo_slot,
//! mailbox, dispatch counter, RNG, per-P Coroutine + stack pools.
//! In Phase 1, P is 1:1-bound to an M at startup and never detaches;
//! later phases introduce detach/attach.
//!
//! ## State
//!
//! - `local` — fixed-256 work-stealing queue. Owner FIFO-pops;
//!   sibling Ps steal half.
//! - `lifo_slot` — single-slot LIFO cache for spawn-chain locality.
//! - `mailbox` — MPMC Treiber stack. Receives cross-P pushes and
//!   local-queue overflow. Owner P pops in dispatch; sibling Ps can
//!   pop during stealing.
//! - `fairness_countdown` — periodic mailbox-fairness down-counter
//!   (see `tryFindAndDispatch` in `runtime.zig`).
//! - `coro_pool` — owner-only LIFO free list for recycled Coroutine
//!   structs. Capped at `POOL_CAP`; overflow goes back to the
//!   allocator.
//! - `stack_pool` — owner-only LIFO free list for recycled stack
//!   slots. Capped at `STACK_POOL_CAP`. Slot memory lives in
//!   `Runtime`'s slab arena; freeing a slot pushes onto the local
//!   list when there's room, otherwise pushes to the arena's free
//!   list (cheap — no `munmap`). Cap prevents one P from pinning
//!   the entire arena under asymmetric spawn patterns (e.g.
//!   single-driver-fan-out): the spawning P drains arena, worker
//!   Ps free locally, arena empties, driver fails. Cap forces
//!   workers to shed excess back to arena so the cycle balances.
//!
//! See `docs/internals/scheduler-mn.md` for the full design.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const wsq_mod = @import("work_steal_queue.zig");
const stack_mod = @import("stack.zig");

pub const Coroutine = coroutine.Coroutine;
pub const LocalQueue = wsq_mod.WorkStealQueue;
pub const StackPtr = stack_mod.StackPtr;

const Mailbox = @import("worker.zig").Mailbox;

/// Soft cap on per-P Coroutine free-list size. Matches Go's `gFree`
/// local cache shape. Larger caches hold more reusable structures but
/// pin allocator memory; 64 covers typical spawn-bursts without
/// bloating idle workers.
pub const POOL_CAP: u16 = 64;

/// Fallback per-P stack-slot cap when no arena is attached (unit
/// tests that construct bare `P`s). Production sets `stack_pool_cap`
/// at `Runtime.init` to `arena.n_slots / n_workers` — each P gets a
/// fair share of the slab. Tests without an arena have no cross-P
/// rebalance concern so the cap value is immaterial.
pub const TEST_STACK_POOL_CAP: u32 = std.math.maxInt(u32);

/// Every Nth dispatch, `tryFindAndDispatch` checks this P's mailbox
/// before its local queue, so a tight local-fed yield loop can't
/// starve coroutines unparked into the mailbox. 61 is prime (mirrors
/// Go's `schedule.checkGlobalRunq`) — chosen so the check doesn't
/// phase-lock with any power-of-two queue/batch size. Consumed via a
/// down-counter (`P.fairness_countdown`), not a modulo.
pub const INJECTION_CHECK_INTERVAL: u32 = 61;

pub const P = struct {
    id: usize,
    runtime: *anyopaque, // *Runtime; opaque to avoid cycle
    local: LocalQueue,
    /// Single-slot LIFO cache for the just-pushed continuation.
    /// Cache-line padded — owner reads/writes constantly via swap/
    /// CAS, must not share a line with `mailbox.head` (cross-P
    /// written on every unpark).
    lifo_slot: std.atomic.Value(?*Coroutine) align(std.atomic.cache_line) = std.atomic.Value(?*Coroutine).init(null),
    /// Per-P MPMC mailbox. Cross-P pushes target a specific P's
    /// mailbox instead of one shared global queue, partitioning the
    /// cache-line contention. Padded so the owner's adjacent
    /// owner-only fields (fairness_countdown, rng, coro_pool) don't
    /// share a line with the cross-P-written head pointer.
    mailbox: Mailbox align(std.atomic.cache_line) = .{},
    /// Per-dispatch down-counter for periodic mailbox/injection
    /// fairness. Reloads to `INJECTION_CHECK_INTERVAL - 1` each time it
    /// hits 0; the fairness check fires on the reload. A down-counter
    /// (decrement + compare-to-zero) avoids the `udiv`/`msub` that a
    /// `% 61` (prime, no strength-reduction) would emit on every
    /// dispatch. Initialised so the first check fires on the 61st
    /// dispatch, matching the old `count % 61 == 0` schedule.
    fairness_countdown: u32 = INJECTION_CHECK_INTERVAL - 1,
    rng: std.Random.DefaultPrng,
    /// Owner-only LIFO free list of recycled `Coroutine` structs.
    /// Threaded through `Coroutine.next` (unused while a coro is
    /// neither queued nor done).
    coro_pool: ?*Coroutine = null,
    coro_pool_count: u16 = 0,
    /// Per-P diagnostic counters. Sharded across P's to avoid the
    /// cache-line ping-pong that a single Runtime-level atomic would
    /// cause on every spawn / done / unpark — that contention was
    /// measured at ~40 % of multi-worker latency.
    ///
    /// `spawned`, `done`, `fairness_hits` are incremented by the
    /// owning M only. `unparks_to_inject` is incremented by whoever
    /// pushes to this P's mailbox (which can be a different M); the
    /// contention there is bounded by the mailbox push rate.
    ///
    /// Atomic with `.monotonic` — same machine cost as a non-atomic
    /// add on ARM64 (single LDADD), but explicitly safe for racing
    /// readers in `dumpState`.
    stat_spawned: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stat_done: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stat_fairness_hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Cross-P-written: a worker pushing to THIS P's mailbox
    /// increments. Separated from the owner-only stats so cross-P
    /// writes don't bounce the cache line that the owner is
    /// continuously updating.
    stat_unparks_to_inject: std.atomic.Value(u64) align(std.atomic.cache_line) = std.atomic.Value(u64).init(0),
    /// Owner-only LIFO free list of recycled stack slots. Capped at
    /// `stack_pool_cap` to prevent one P from pinning the arena
    /// under asymmetric spawn patterns. The intrusive next-pointer
    /// lives at offset `stack.usableOffset()` from the slot's base
    /// (the start of the committed body region).
    stack_pool: ?StackPtr = null,
    stack_pool_count: u32 = 0,
    /// Fair-share cap = `arena.n_slots / n_workers`, set by
    /// `Runtime.init`. Above the cap, `freeStack` sheds back to the
    /// arena instead of growing the local pool — so under asymmetric
    /// spawn (one driver, N workers) the arena keeps a refill supply
    /// for the driver instead of every freed slot getting pinned in
    /// a worker's pool. Tests without an arena get `TEST_STACK_POOL_CAP`.
    stack_pool_cap: u32 = TEST_STACK_POOL_CAP,
    /// Pointer to the Runtime's slab arena. Per-P pool miss / shutdown
    /// drain go through here. Opaque-typed to avoid an import cycle —
    /// resolved to `*stack_mod.Arena` at use site.
    arena: ?*stack_mod.Arena = null,
    /// Cached `arena.usableOffset()`. Hot-path optimisation: spawn
    /// reads this on every allocStack call, and the arena's slot_size
    /// is fixed for the runtime's lifetime, so we copy it here to
    /// avoid a pointer-indirection load on the spawn hot path.
    /// Set by `Runtime.init` when it assigns `arena`.
    arena_usable_offset: usize = 0,

    pub fn init(self: *P, id: usize, runtime: *anyopaque) void {
        self.* = .{
            .id = id,
            .runtime = runtime,
            .local = LocalQueue.init(),
            .rng = std.Random.DefaultPrng.init(@intCast(id +% 0xDEADBEEF)),
        };
    }

    /// Owner-side push using the lifo_slot for spawn-chain locality.
    /// Local-queue overflow lands in `self.mailbox`.
    pub fn pushLifo(self: *P, c: *Coroutine) void {
        const evicted = self.lifo_slot.swap(c, .acq_rel);
        if (evicted) |e| self.local.push(e, &self.mailbox);
    }

    /// Owner-side push that goes straight to the main queue. Used
    /// for yield (re-queue at tail for fairness).
    pub fn pushQueue(self: *P, c: *Coroutine) void {
        self.local.push(c, &self.mailbox);
    }

    /// Owner-side pop in dispatch priority: lifo_slot first, then queue.
    pub fn popLocal(self: *P) ?*Coroutine {
        if (self.lifo_slot.swap(null, .acq_rel)) |c| return c;
        return self.local.pop();
    }

    /// Pop one item from `mailbox`. Callable from any thread (MPMC).
    pub fn popMailbox(self: *P) ?*Coroutine {
        return self.mailbox.pop();
    }

    /// Owner-side: if `target` is currently in this P's `lifo_slot`,
    /// remove it and return true. Otherwise return false without
    /// touching the slot. Used by direct-handoff in `Task.join` to
    /// claim the just-spawned continuation before any other M can
    /// steal it. Single CAS, no locking.
    pub fn tryRemoveLifo(self: *P, target: *Coroutine) bool {
        const cur = self.lifo_slot.load(.acquire);
        if (cur != target) return false;
        // CAS target → null. Failure means the slot moved between
        // our load and the CAS (some other op evicted target);
        // caller falls through to the slow path.
        return self.lifo_slot.cmpxchgStrong(target, null, .acq_rel, .acquire) == null;
    }

    /// Owner-only: take a recycled Coroutine from the pool, or
    /// allocate one. The returned struct is uninitialized — caller
    /// must fully overwrite it before use.
    pub fn allocCoroutine(self: *P, allocator: std.mem.Allocator) !*Coroutine {
        if (self.coro_pool) |c| {
            self.coro_pool = c.next;
            self.coro_pool_count -= 1;
            c.next = null;
            return c;
        }
        return try allocator.create(Coroutine);
    }

    /// Owner-only: return a Coroutine to the pool. Over-cap entries
    /// are released to the allocator.
    pub fn freeCoroutine(self: *P, c: *Coroutine, allocator: std.mem.Allocator) void {
        if (self.coro_pool_count >= POOL_CAP) {
            allocator.destroy(c);
            return;
        }
        c.next = self.coro_pool;
        self.coro_pool = c;
        self.coro_pool_count += 1;
    }

    /// Owner-only: take a recycled stack slot from the local pool,
    /// or pull one from the arena. Returns `error.ArenaExhausted` if
    /// the local pool is empty and the arena is fully allocated.
    pub fn allocStack(self: *P) !StackPtr {
        if (self.stack_pool) |base| {
            // Next-pointer lives at offset `usable_offset` — start
            // of the committed body region. Cached at init so we
            // hit one local field load instead of an arena
            // pointer-indirection on every spawn.
            const next_loc: *?StackPtr = @ptrCast(@alignCast(base + self.arena_usable_offset));
            self.stack_pool = next_loc.*;
            self.stack_pool_count -= 1;
            return base;
        }
        const arena = self.arena orelse return error.ArenaExhausted;
        return arena.alloc();
    }

    /// Owner-only: return a stack slot to the local LIFO pool, or
    /// shed to the arena if the local cap is hit. Local push is a
    /// single pointer write; arena push is a spinlock acquire (rare
    /// after warm-up unless the workload is asymmetric).
    pub fn freeStack(self: *P, base: StackPtr) void {
        if (self.stack_pool_count >= self.stack_pool_cap) {
            // The pool only ever holds slots from `self.arena`; a P
            // without an arena can't have allocated a stack, so it
            // also can't be freeing one.
            self.arena.?.free(base);
            return;
        }
        const next_loc: *?StackPtr = @ptrCast(@alignCast(base + self.arena_usable_offset));
        next_loc.* = self.stack_pool;
        self.stack_pool = base;
        self.stack_pool_count += 1;
    }

    /// Release every entry held in the local pools. Coroutine structs
    /// go back to the allocator; stack slots go back to the arena
    /// (the arena owns the slab — actual `munmap` happens in
    /// `Arena.deinit`). Called from `Runtime.deinit` after all Ms
    /// have stopped.
    pub fn drainPools(self: *P, allocator: std.mem.Allocator) void {
        while (self.coro_pool) |c| {
            self.coro_pool = c.next;
            allocator.destroy(c);
        }
        self.coro_pool_count = 0;
        if (self.arena) |arena| {
            while (self.stack_pool) |base| {
                const next_loc: *?StackPtr = @ptrCast(@alignCast(base + self.arena_usable_offset));
                self.stack_pool = next_loc.*;
                arena.free(base);
            }
        } else {
            // No arena attached — only happens in unit tests that
            // construct a bare P. Stack pool should be empty here.
            std.debug.assert(self.stack_pool == null);
        }
        self.stack_pool_count = 0;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "P.tryRemoveLifo: empty slot returns false" {
    var p: P = undefined;
    p.init(0, undefined);
    var fake_coro: Coroutine = .{};
    try std.testing.expect(!p.tryRemoveLifo(&fake_coro));
}

test "P.tryRemoveLifo: target in slot returns true and clears slot" {
    var p: P = undefined;
    p.init(0, undefined);
    var fake_coro: Coroutine = .{};
    p.lifo_slot.store(&fake_coro, .release);
    try std.testing.expect(p.tryRemoveLifo(&fake_coro));
    try std.testing.expectEqual(@as(?*Coroutine, null), p.lifo_slot.load(.acquire));
}

test "P.tryRemoveLifo: different coro in slot returns false and leaves slot" {
    var p: P = undefined;
    p.init(0, undefined);
    var coro_a: Coroutine = .{};
    var coro_b: Coroutine = .{};
    p.lifo_slot.store(&coro_a, .release);
    try std.testing.expect(!p.tryRemoveLifo(&coro_b));
    // Slot is unchanged.
    try std.testing.expectEqual(@as(?*Coroutine, &coro_a), p.lifo_slot.load(.acquire));
}

test "P.coro_pool: alloc/free round-trip recycles the same struct" {
    var p: P = undefined;
    p.init(0, undefined);
    const a = std.testing.allocator;
    const c1 = try p.allocCoroutine(a);
    p.freeCoroutine(c1, a);
    // Same struct should come back from the pool.
    const c2 = try p.allocCoroutine(a);
    try std.testing.expectEqual(c1, c2);
    // Drain restores it to the underlying allocator (no leak).
    p.freeCoroutine(c2, a);
    p.drainPools(a);
}

test "P.coro_pool: over-cap entries go to allocator (no overflow)" {
    var p: P = undefined;
    p.init(0, undefined);
    const a = std.testing.allocator;
    // Fill the pool up to POOL_CAP. After this, additional free's
    // must allocator.destroy without growing pool_count past cap.
    var i: u16 = 0;
    while (i < POOL_CAP + 5) : (i += 1) {
        const c = try a.create(Coroutine);
        p.freeCoroutine(c, a);
    }
    try std.testing.expect(p.coro_pool_count == POOL_CAP);
    p.drainPools(a);
    try std.testing.expect(p.coro_pool_count == 0);
}

test "P.stack_pool: alloc/free round-trip recycles the same memory" {
    var p: P = undefined;
    p.init(0, undefined);
    const a = std.testing.allocator;
    var arena = try stack_mod.Arena.init(a, 4, stack_mod.DEFAULT_RESERVATION_SIZE);
    defer arena.deinit(a);
    p.arena = &arena;
    p.arena_usable_offset = arena.usableOffset();

    const s1 = try p.allocStack();
    p.freeStack(s1);
    const s2 = try p.allocStack();
    try std.testing.expectEqual(s1, s2);
    p.freeStack(s2);
    p.drainPools(a);
}

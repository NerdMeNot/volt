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
//! - `dispatch_count` — fairness counter (see `runtime.zig`).
//! - `coro_pool` / `stack_pool` — owner-only LIFO free lists for
//!   recycled Coroutine structs and stacks. See `POOL_CAP`.
//!
//! See `docs/internals/scheduler-mn.md` for the full design.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const wsq_mod = @import("work_steal_queue.zig");

pub const Coroutine = coroutine.Coroutine;
pub const LocalQueue = wsq_mod.WorkStealQueue;

const Mailbox = @import("worker.zig").Mailbox;

/// Soft cap on per-P free-list size. Matches Go's `gFree` local cache
/// shape. Larger caches hold more reusable structures but pin RSS;
/// 64 covers typical spawn-bursts (most ping/pong + scatter/gather
/// fan-outs are well under this) without bloating idle workers.
pub const POOL_CAP: u16 = 64;

pub const P = struct {
    id: usize,
    runtime: *anyopaque, // *Runtime; opaque to avoid cycle
    local: LocalQueue,
    /// Single-slot LIFO cache for the just-pushed continuation.
    lifo_slot: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),
    /// Per-P MPMC mailbox. Cross-P pushes target a specific P's
    /// mailbox instead of one shared global queue, partitioning the
    /// cache-line contention.
    mailbox: Mailbox = .{},
    /// Per-dispatch counter for periodic mailbox/injection fairness.
    dispatch_count: u32 = 0,
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
    stat_unparks_to_inject: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Owner-only LIFO free list of recycled stacks. The next-pointer
    /// is stored in the first 8 bytes of the freed stack itself (Go's
    /// gFree shape) — saves the per-entry node allocation.
    stack_pool: ?[*]align(16) u8 = null,
    stack_pool_count: u16 = 0,

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

    /// Owner-only: take a recycled stack from the pool, or allocate
    /// a fresh one.
    pub fn allocStack(self: *P, allocator: std.mem.Allocator, stack_size: usize) ![]align(16) u8 {
        if (self.stack_pool) |ptr| {
            // First 8 bytes of the freed stack hold the next pointer.
            const next_loc: *?[*]align(16) u8 = @ptrCast(@alignCast(ptr));
            self.stack_pool = next_loc.*;
            self.stack_pool_count -= 1;
            return ptr[0..stack_size];
        }
        return try allocator.alignedAlloc(u8, .@"16", stack_size);
    }

    /// Owner-only: return a stack to the pool. Pool only accepts
    /// default-sized stacks (mixed sizes break the intrusive list);
    /// other sizes go straight to the allocator. Pool cap eviction
    /// also goes to the allocator.
    pub fn freeStack(
        self: *P,
        stack: []align(16) u8,
        allocator: std.mem.Allocator,
        expected_size: usize,
    ) void {
        if (stack.len != expected_size or self.stack_pool_count >= POOL_CAP) {
            allocator.free(stack);
            return;
        }
        const next_loc: *?[*]align(16) u8 = @ptrCast(@alignCast(stack.ptr));
        next_loc.* = self.stack_pool;
        self.stack_pool = stack.ptr;
        self.stack_pool_count += 1;
    }

    /// Release every entry held in the pools. Called from
    /// `Runtime.deinit` after all Ms have stopped.
    pub fn drainPools(self: *P, allocator: std.mem.Allocator, stack_size: usize) void {
        while (self.coro_pool) |c| {
            self.coro_pool = c.next;
            allocator.destroy(c);
        }
        self.coro_pool_count = 0;
        while (self.stack_pool) |ptr| {
            const next_loc: *?[*]align(16) u8 = @ptrCast(@alignCast(ptr));
            self.stack_pool = next_loc.*;
            allocator.free(ptr[0..stack_size]);
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
    p.drainPools(a, 16 * 1024);
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
    p.drainPools(a, 16 * 1024);
    try std.testing.expect(p.coro_pool_count == 0);
}

test "P.stack_pool: alloc/free round-trip recycles the same memory" {
    var p: P = undefined;
    p.init(0, undefined);
    const a = std.testing.allocator;
    const SIZE = 16 * 1024;
    const s1 = try p.allocStack(a, SIZE);
    p.freeStack(s1, a, SIZE);
    const s2 = try p.allocStack(a, SIZE);
    try std.testing.expectEqual(s1.ptr, s2.ptr);
    p.freeStack(s2, a, SIZE);
    p.drainPools(a, SIZE);
}

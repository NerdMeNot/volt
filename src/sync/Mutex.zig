//! Mutex — async mutex for coroutines.
//!
//! Fair (strict FIFO), zero-allocation, RAII via `defer mu.unlock()`.
//! Built on a lock-free MCS-style queue with `*Coroutine` nodes
//! intrusive in the Coroutine struct. Pure Zig idiom: no allocator
//! anywhere in lock/unlock; no internal `std.Thread.Mutex` for the
//! waiter list.
//!
//! ## Usage
//!
//! ```zig
//! var mu = Mutex{};
//! try mu.lock();
//! defer mu.unlock();
//! // critical section
//! ```
//!
//! ## Design
//!
//! - **`tail: ?*Coroutine`** atomic. `null` = unlocked. Non-null =
//!   most recent waiter in chain. Earlier waiters are reachable via
//!   `mwait_next` pointers; the lock holder is implicit (the head of
//!   the chain).
//! - **Acquire** (`lock`): brief spin → `tail.swap(me)`. If the
//!   previous tail was null, we're the head and we hold the lock.
//!   Otherwise we link our predecessor's `mwait_next = me` and park.
//! - **Release** (`unlock`): walk `mwait_next` to find the live
//!   successor; skip any tombstoned (cancelled) waiters; hand the
//!   lock to the live successor by setting their `mwait_granted` and
//!   `unpark()`. If no successor exists, CAS `tail` from `me` to
//!   `null`.
//!
//! ## Why MCS
//!
//! The earlier design used a `std.Thread.Mutex` to protect a flat
//! waiter list. Under 8-way contention, that internal mutex was hot —
//! ~200 ns/cycle of overhead. MCS replaces it with two atomic ops
//! per cycle (the swap and the next-pointer store), eliminating the
//! internal mutex entirely. FIFO fairness is preserved by construction
//! (`tail.swap` is a total order).
//!
//! ## Cancellation
//!
//! A waiter cancelled before its grant arrives sets a tombstone
//! (`mwait_cancelled = true`) and exits with `error.Cancelled` without
//! touching the chain. The predecessor's eventual handoff sees the
//! tombstone and skips past — searching forward for a live successor.
//! If the cancelled waiter has no successor, the predecessor releases
//! the lock cleanly via the standard tail CAS.
//!
//! Lock is NOT held on `error.Cancelled` return.
//!
//! ## Future work (v1.x)
//!
//! - **Same-worker continuation handoff**: when the live successor is
//!   parked on the same worker as the unlocker, ctx-swap directly to
//!   them (bypassing the scheduler dispatch). Saves ~300 ns per cycle
//!   on the same-worker case. Requires worker-id tracking on the
//!   parked waiter and changes to `Worker.dispatch` to handle the
//!   "swap returned from a different coro than dispatched" path —
//!   non-trivial, deferred to v1.x.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const current = @import("../scheduler/current.zig");
const Park = @import("../scheduler/park.zig").Park;

/// Deadlock detection — only compiled into Debug builds. Each Mutex
/// remembers its current holder; each Coroutine remembers the Mutex
/// it's currently waiting to acquire (`waiting_on_mutex`). Before
/// committing to park, we walk the holder→waiting chain looking for
/// a cycle that includes us. If found, panic with the cycle printed
/// so the bug surfaces immediately instead of hanging.
///
/// Catches the most common deadlock class: two coroutines acquiring
/// two mutexes in opposite orders. Doesn't catch every deadlock
/// (channels, notifies, semaphore queues, custom park primitives) —
/// future v1.x extends to a generic wait-for graph at Park level.
///
/// Zero release-mode cost: the holder field is `if (debug) ... else void`,
/// the cycle walk lives behind `if (comptime debug)`, so ReleaseFast
/// emits no extra atomic ops, no extra struct bytes.
const debug_deadlock = builtin.mode == .Debug;

/// Userspace spin attempts before committing to a park. Each attempt is
/// `SPIN_HINTS_PER_ITER` PAUSE-equivalent hints (~1 ns each on x86 with
/// the `pause` instruction; ARM emits `yield`). Total spin budget on
/// contention: SPIN_ITERS × SPIN_HINTS_PER_ITER ≈ 120 ns.
///
/// Why spin: a coroutine ctx-swap costs ~30–60 ns minimum (worker dispatch
/// + scheduler logic). If the lock is going to be released within ~100 ns
/// (the common case for short critical sections), spinning is cheaper than
/// parking + waking. If the hold is longer, the spin completes quickly
/// and we fall through to the park — no perf regression vs no-spin.
///
/// Comptime-gated: zero in Debug builds so contention bugs (e.g. holding
/// a Mutex across an `await`) surface as immediate parking instead of
/// being masked by a tight spin. ReleaseFast/ReleaseSafe spin normally.
///
/// Inspired by Go's `sync.Mutex` (active_spin=4, active_spin_cnt=30).
/// We match Go's iteration count; per-iter hint count is 12 (vs Go's 30)
/// — coroutines have lower base ctx-swap cost than goroutines, so the
/// crossover where parking wins comes sooner. Tunable via Runtime.Config.
const spin_iters: u32 = if (builtin.mode == .Debug) 0 else 4;
const spin_hints_per_iter: u32 = if (builtin.mode == .Debug) 0 else 12;

pub const Mutex = struct {
    /// Tail of the MCS-style lock-free queue.
    ///
    /// - `null` → unlocked, no waiters.
    /// - non-null → the lock is held; this pointer is the LATEST waiter
    ///   in the FIFO chain. Earlier waiters can be walked via
    ///   `mwait_next` pointers. The HEAD of the chain (current holder)
    ///   is implicit — it's the coroutine that did the original swap
    ///   with `pred=null`.
    ///
    /// FIFO fairness preserved: `tail.swap(me)` is a total order, so
    /// the chain is in arrival order. No `waiter_mutex` needed.
    tail: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    /// Debug-only: current lock holder (or null). Comptime-elided in
    /// release builds — `?*Coroutine` becomes `void`, no struct bytes,
    /// no atomics. See `debug_deadlock`.
    holder: if (debug_deadlock) std.atomic.Value(usize) else void = if (debug_deadlock)
        std.atomic.Value(usize).init(0)
    else {},

    /// Try to acquire the lock without blocking. Returns true on success.
    /// Requires a coroutine context — Volt's Mutex is coroutine-only;
    /// use `std.Thread.Mutex` for OS-thread synchronization.
    pub fn tryLock(self: *Mutex) bool {
        const me = current.currentCoroutine() orelse
            @panic("volt.sync.Mutex.tryLock requires a coroutine context");
        const got = self.tail.cmpxchgStrong(null, me, .acquire, .monotonic) == null;
        if (got) {
            // Initialize my chain slot. No predecessor to set my.next, so
            // I do it myself.
            me.mwait_next.store(null, .monotonic);
            if (comptime debug_deadlock) {
                self.holder.store(@intFromPtr(me), .release);
            }
        }
        return got;
    }

    /// Acquire the lock, parking the coroutine if it's held.
    /// Returns `error.Cancelled` if the coroutine is cancelled while
    /// queued.
    ///
    /// Cancellation model:
    /// - If we're cancelled BEFORE the grant arrives, we set our
    ///   `mwait_cancelled` tombstone and exit with `error.Cancelled`.
    ///   The lock is NOT held. We don't touch tail or our successor's
    ///   chain link — the predecessor's eventual handoff sees the
    ///   tombstone and skips past us to our successor (or releases
    ///   the lock if we had none).
    /// - If we get the grant and THEN observe cancellation, we hand
    ///   off to our successor and exit with `error.Cancelled`. Symmetric
    ///   to the cancellation-while-holding-the-lock case.
    pub fn lock(self: *Mutex) error{Cancelled}!void {
        // Fast path: uncontended.
        if (self.tryLock()) return;

        // Spin path: brief userspace spin before committing to park.
        if (comptime spin_iters > 0) {
            var iter: u32 = 0;
            while (iter < spin_iters) : (iter += 1) {
                var p: u32 = 0;
                while (p < spin_hints_per_iter) : (p += 1) std.atomic.spinLoopHint();
                if (self.tryLock()) return;
            }
        }

        const me = current.currentCoroutine() orelse
            @panic("volt.sync.Mutex.lock requires a coroutine context");

        // Debug deadlock check.
        if (comptime debug_deadlock) {
            checkCycle(me, self);
        }

        // MCS enqueue.
        me.mwait_next.store(null, .monotonic);
        me.mwait_granted.store(false, .monotonic);
        me.mwait_cancelled.store(false, .monotonic);

        const pred_opt = self.tail.swap(me, .acq_rel);
        if (pred_opt) |pred| {
            pred.mwait_next.store(me, .release);

            if (comptime debug_deadlock) {
                me.waiting_on_mutex.store(@intFromPtr(self), .release);
            }

            // Park until grant arrives OR cancel fires. parkUncancellable
            // registers current_park so cancel can wake us (but doesn't
            // return error.Cancelled — we decide). The loop re-parks if
            // we wake spuriously without grant.
            while (!me.mwait_granted.load(.acquire)) {
                if (me.cancel_flag.load(.acquire)) {
                    // Cancelled before grant. Tombstone & exit. Predecessor's
                    // handoff will skip past us via mwait_cancelled check.
                    me.mwait_cancelled.store(true, .release);
                    if (comptime debug_deadlock) {
                        me.waiting_on_mutex.store(0, .release);
                    }
                    return error.Cancelled;
                }
                me.mwait_park.parkUncancellable();
            }
        }
        // else: no predecessor — I have the lock immediately.

        // ── I hold the lock ──
        if (comptime debug_deadlock) {
            me.waiting_on_mutex.store(0, .release);
            self.holder.store(@intFromPtr(me), .release);
        }

        // Cancellation that arrived AFTER grant: I have the lock but
        // shouldn't keep it. Hand off and exit Cancelled.
        if (me.cancel_flag.load(.acquire)) {
            self.handoffNext(me);
            return error.Cancelled;
        }
    }

    /// Release the lock. Must only be called by the holder.
    pub fn unlock(self: *Mutex) void {
        const me = current.currentCoroutine() orelse
            @panic("volt.sync.Mutex.unlock requires a coroutine context");

        if (comptime debug_deadlock) self.holder.store(0, .release);

        self.handoffNext(me);
    }

    /// Common handoff path: find a live successor (skipping any
    /// tombstoned/cancelled waiters) and hand them the lock; otherwise
    /// CAS tail back to null. Used by both unlock() and the
    /// cancellation-after-grant path in lock().
    fn handoffNext(self: *Mutex, holder: *Coroutine) void {
        // Walk the chain, skipping cancelled tombstones. `me` is the
        // current node we're trying to hand off PAST. Initially the
        // holder; if next is tombstoned, advance `me = next` and try
        // again.
        var me = holder;
        while (true) {
            var next_opt = me.mwait_next.load(.acquire);

            if (next_opt == null) {
                // No successor visible yet. Try to release the lock by
                // CAS'ing tail from me to null.
                if (self.tail.cmpxchgStrong(me, null, .acq_rel, .acquire) == null) {
                    return; // released
                }
                // CAS failed: someone did `tail.swap(them)` after our
                // load. They WILL store `me.mwait_next = them` shortly.
                while (true) {
                    next_opt = me.mwait_next.load(.acquire);
                    if (next_opt != null) break;
                    std.atomic.spinLoopHint();
                }
            }

            const next = next_opt.?;

            if (next.mwait_cancelled.load(.acquire)) {
                // Tombstoned waiter — skip past. Continue searching from
                // `next` (it's still in the chain logically; its own
                // mwait_next is what we now consult).
                me = next;
                continue;
            }

            // Live successor — hand off the grant.
            // unparkLocal (not unpark) skips wakeOneSibling: the mutex
            // chain runs serially on whichever worker first dispatched
            // the holder, no point waking siblings for work that's
            // already strictly local. Saves ~50 ns per cycle.
            next.mwait_granted.store(true, .release);
            next.mwait_park.unparkLocal();
            return;
        }
    }

    /// Walk the holder→waiting chain looking for a cycle that includes
    /// `me`. Compiled only in Debug builds. Panics with the cycle path
    /// printed if a deadlock is detected.
    fn checkCycle(me: *Coroutine, target: *Mutex) void {
        if (comptime !debug_deadlock) return;
        // Bound the walk so a cycle-free chain never hangs the check.
        // 64 hops is far more than any realistic application's lock
        // depth (typical: 2-4).
        const MAX_HOPS: u32 = 64;
        var hops: u32 = 0;
        var mu: *Mutex = target;
        while (hops < MAX_HOPS) : (hops += 1) {
            const holder_ptr = mu.holder.load(.acquire);
            if (holder_ptr == 0) return; // not held — no cycle possible
            const holder: *Coroutine = @ptrFromInt(holder_ptr);
            if (holder == me) {
                std.debug.panic(
                    "Volt deadlock detected: coroutine attempting to acquire " ++
                        "Mutex@0x{x} which is held by itself, or a cycle leads back here. " ++
                        "Most common cause: two coroutines acquire two mutexes in opposite orders.",
                    .{@intFromPtr(target)},
                );
            }
            const next_mu_ptr = holder.waiting_on_mutex.load(.acquire);
            if (next_mu_ptr == 0) return; // holder isn't waiting — no cycle
            mu = @ptrFromInt(next_mu_ptr);
            if (mu == target) {
                std.debug.panic(
                    "Volt deadlock detected: cycle of mutex acquisitions back to Mutex@0x{x}. " ++
                        "Most common cause: lock ordering inconsistency between coroutines.",
                    .{@intFromPtr(target)},
                );
            }
        }
        // Bound exceeded — pathological chain length, surface as deadlock.
        std.debug.panic(
            "Volt deadlock detection: holder→waiting chain exceeded {d} hops " ++
                "without resolution. Probably a cycle.",
            .{MAX_HOPS},
        );
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn tryLockTestRoot() !void {
    var mu = Mutex{};
    try std.testing.expect(mu.tryLock());
    try std.testing.expect(!mu.tryLock());
    mu.unlock();
    try std.testing.expect(mu.tryLock());
    mu.unlock();
}

test "mutex: tryLock uncontended succeeds, second fails" {
    // Mutex now requires a coroutine context (MCS chain uses *Coroutine
    // nodes). The pure-thread tryLock test wraps in volt.run.
    try volt.run(.{ .allocator = std.testing.allocator }, tryLockTestRoot, .{});
}

const CounterCtx = struct {
    mu: *Mutex,
    counter: u64 = 0,
};

fn incrementer(ctx: *CounterCtx, iters: u32) !void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) {
        try ctx.mu.lock();
        ctx.counter += 1;
        ctx.mu.unlock();
    }
}

fn counterRoot(workers: u32, iters_per: u32) !u64 {
    var mu = Mutex{};
    var ctx = CounterCtx{ .mu = &mu };

    const allocator = std.testing.allocator;
    const jobs = try allocator.alloc(*volt.Job, workers);
    defer allocator.free(jobs);

    for (jobs) |*j| j.* = try volt.launch(incrementer, .{ &ctx, iters_per });
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    return ctx.counter;
}

test "mutex stress: 8 coroutines × 200 increments, counter == 1600" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, counterRoot, .{ @as(u32, 8), @as(u32, 200) });
    try std.testing.expectEqual(@as(u64, 1600), total);
}

test "mutex stress: 16 × 500 = 8000" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, counterRoot, .{ @as(u32, 16), @as(u32, 500) });
    try std.testing.expectEqual(@as(u64, 8000), total);
}

const FifoCtx = struct {
    mu: *Mutex,
    order: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    finishes: [4]std.atomic.Value(u32) = .{
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
    },
};

fn fifoLocker(ctx: *FifoCtx, slot: usize) !void {
    try ctx.mu.lock();
    const order = ctx.order.fetchAdd(1, .monotonic);
    ctx.finishes[slot].store(order + 1, .release);
    // Spin a bit while holding the lock so subsequent lockers enqueue.
    var spin: u32 = 0;
    while (spin < 1000) : (spin += 1) std.atomic.spinLoopHint();
    ctx.mu.unlock();
}

fn fifoRoot() !FifoCtx {
    var mu = Mutex{};
    var ctx = FifoCtx{ .mu = &mu };

    // Hold the mutex from the root, spawn 4 lockers in order, release.
    try mu.lock();

    var lockers: [4]*volt.Job = undefined;
    for (&lockers, 0..) |*j, i| {
        j.* = try volt.launch(fifoLocker, .{ &ctx, i });
        // Yield so each locker enqueues before the next is launched.
        try volt.yield();
    }
    defer for (lockers) |j| volt.destroyJob(j);

    // Yield more to give all 4 a chance to park on the waiter list.
    var k: u32 = 0;
    while (k < 8) : (k += 1) try volt.yield();

    mu.unlock();
    for (lockers) |j| try j.join();
    return ctx;
}

test "mutex: contended unlock wakes every waiter — no lost wakes, no double-grants" {
    // What we actually test: under contention, every waiter eventually
    // acquires the mutex exactly once, and the total number of acquires
    // equals the number of waiters. Catches lost wakes (waiter parks
    // forever) and double-grants (two waiters resume on one unlock).
    //
    // What we DELIBERATELY don't assert: the order of acquisition.
    // Volt's WaiterList is a 5-line FIFO linked list (pushBack/popFront)
    // — algorithmically self-evident. Asserting end-to-end FIFO would
    // require deterministic launch→park→unlock ordering, which the
    // multi-worker scheduler doesn't provide (and shouldn't have to).
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, fifoRoot, .{});

    // Each slot should hold a distinct value in [1, 4]. Verify the set,
    // not the order.
    var seen = [_]bool{ false, false, false, false };
    inline for (0..4) |i| {
        const v = ctx.finishes[i].load(.acquire);
        try std.testing.expect(v >= 1 and v <= 4);
        try std.testing.expect(!seen[v - 1]); // no duplicate
        seen[v - 1] = true;
    }
    for (seen) |s| try std.testing.expect(s);
}

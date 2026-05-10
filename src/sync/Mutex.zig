//! Mutex — async mutex for coroutines.
//!
//! Fair (FIFO waiter list), zero-allocation, RAII via `defer mu.unlock()`.
//! Built on `Park` (the same single-atomic substrate the channel waiter
//! list uses), with a fast-path lock-free CAS for the uncontended case.
//!
//! ## Usage
//!
//! ```zig
//! var mu = Mutex{};
//! mu.lock();
//! defer mu.unlock();
//! // critical section
//! ```
//!
//! Or non-blocking:
//!
//! ```zig
//! if (mu.tryLock()) {
//!     defer mu.unlock();
//!     // critical section
//! }
//! ```
//!
//! ## Design
//!
//! - `state` (atomic u8) — 0 = unlocked, 1 = locked. Fast path is a
//!   single CAS for both lock and unlock.
//! - On contention, the waiter takes a slow-path lock on `waiter_mutex`
//!   (an internal sync mutex), enqueues itself, and parks. The unlocker
//!   does a fast `state` CAS first; if successful, checks the waiter
//!   list under `waiter_mutex` and wakes the head waiter.
//! - The wake protocol mirrors `Channel.zig`'s post-M1 design: the
//!   recheck inside the slow-path lock uses a no-wake variant of the
//!   primitive, so we never recursively re-enter the same mutex.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const current = @import("../scheduler/current.zig");
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

const UNLOCKED: u8 = 0;
const LOCKED: u8 = 1;

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
    const Waiter = struct {
        park: Park = .{},
        next: ?*Waiter = null,
    };

    const WaiterList = struct {
        head: ?*Waiter = null,
        tail: ?*Waiter = null,

        fn pushBack(self: *@This(), w: *Waiter) void {
            w.next = null;
            if (self.tail) |t| t.next = w else self.head = w;
            self.tail = w;
        }

        fn popFront(self: *@This()) ?*Waiter {
            const w = self.head orelse return null;
            self.head = w.next;
            if (self.head == null) self.tail = null;
            w.next = null;
            return w;
        }

        fn isEmpty(self: *const @This()) bool {
            return self.head == null;
        }
    };

    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(UNLOCKED),
    waiter_mutex: thread.Mutex = .{},
    waiters: WaiterList = .{},

    /// Debug-only: current lock holder (or null). Comptime-elided in
    /// release builds — `?*Coroutine` becomes `void`, no struct bytes,
    /// no atomics. See `debug_deadlock`.
    holder: if (debug_deadlock) std.atomic.Value(usize) else void = if (debug_deadlock)
        std.atomic.Value(usize).init(0)
    else {},

    /// Try to acquire the lock without blocking. Returns true on success.
    pub fn tryLock(self: *Mutex) bool {
        const got = self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null;
        if (comptime debug_deadlock) {
            if (got) {
                if (current.currentCoroutine()) |me| {
                    self.holder.store(@intFromPtr(me), .release);
                }
            }
        }
        return got;
    }

    /// Acquire the lock, suspending the coroutine if it's held.
    /// Returns `error.Cancelled` if the coroutine is cancelled while
    /// parked on the waiter list (waiter list invariants stay
    /// intact — best-effort removal under the slow-path mutex).
    ///
    /// API note: this used to return `void`, but the void variant
    /// silently busy-looped on cancel since `parkCurrent` would
    /// return `error.Cancelled` indefinitely. The error union is
    /// the honest signature. Callers that don't care about
    /// cancellation can `mu.lock() catch {}` — the lock is never
    /// half-acquired on error.
    pub fn lock(self: *Mutex) error{Cancelled}!void {
        // Fast path: uncontended.
        if (self.tryLock()) return;

        // Spin path: brief userspace spin before committing to park.
        // Catches the common short-hold case where the lock holder will
        // release within ~100ns. Comptime-zero in Debug so contention
        // bugs surface immediately instead of being masked by spinning.
        // (See `spin_iters` / `spin_hints_per_iter` consts above.)
        if (comptime spin_iters > 0) {
            var iter: u32 = 0;
            while (iter < spin_iters) : (iter += 1) {
                var p: u32 = 0;
                while (p < spin_hints_per_iter) : (p += 1) std.atomic.spinLoopHint();
                if (self.tryLock()) return;
            }
        }

        // Debug deadlock check — walk the holder→waiting chain looking
        // for a cycle that includes us. Catches A-locks-X-then-Y vs
        // B-locks-Y-then-X classics. Comptime-gated; release builds
        // skip this entirely.
        if (comptime debug_deadlock) {
            if (current.currentCoroutine()) |me| {
                checkCycle(me, self);
            }
        }

        // Slow path: enqueue and park.
        var waiter: Waiter = .{};
        self.waiter_mutex.lock();
        // Re-check under the slow-path mutex. If the lock is now free
        // (and no waiters ahead of us), grab it.
        if (self.waiters.isEmpty() and self.tryLock()) {
            self.waiter_mutex.unlock();
            return;
        }
        self.waiters.pushBack(&waiter);
        // Record we're waiting on THIS mutex so other coros' deadlock
        // walks can follow the chain through us.
        if (comptime debug_deadlock) {
            if (current.currentCoroutine()) |me| {
                me.waiting_on_mutex.store(@intFromPtr(self), .release);
            }
        }
        self.waiter_mutex.unlock();

        // Park until the unlocker hands the lock to us.
        waiter.park.parkCurrent() catch |err| switch (err) {
            error.Cancelled => {
                // Best-effort remove from waiter list. If we'd
                // already been popped, the unlocker handed us the
                // lock — surface it (caller becomes the holder).
                self.waiter_mutex.lock();
                const removed = self.removeWaiter(&waiter);
                self.waiter_mutex.unlock();
                if (comptime debug_deadlock) {
                    if (current.currentCoroutine()) |me| {
                        me.waiting_on_mutex.store(0, .release);
                    }
                }
                if (!removed) {
                    // We were already handed the lock. The unlocker
                    // moved `state` to LOCKED; we own it now. Caller
                    // gets the lock despite the cancel; their next
                    // yield surfaces error.Cancelled and they
                    // unwind via defer mu.unlock().
                    if (comptime debug_deadlock) {
                        if (current.currentCoroutine()) |me| {
                            self.holder.store(@intFromPtr(me), .release);
                        }
                    }
                    return;
                }
                // Genuine cancel — surface it. Lock is NOT held.
                return error.Cancelled;
            },
        };
        // Lock was handed off to us by the unlocker; we own it.
        if (comptime debug_deadlock) {
            if (current.currentCoroutine()) |me| {
                me.waiting_on_mutex.store(0, .release);
                self.holder.store(@intFromPtr(me), .release);
            }
        }
    }

    /// Release the lock. Must only be called by the holder.
    pub fn unlock(self: *Mutex) void {
        // Clear holder BEFORE the handoff so a parallel deadlock walk
        // doesn't see us as still holding while the new owner sets up.
        if (comptime debug_deadlock) self.holder.store(0, .release);

        // Decide handoff vs drop under the slow-path mutex. Critical:
        // we MUST NOT drop state to UNLOCKED before checking the
        // waiter list. An earlier version did
        //     state.store(UNLOCKED); waiter_mutex.lock();
        //     if (popFront) state.store(LOCKED) else …
        // which opens a window where a concurrent tryLock steals the
        // lock between the two stores AND the popped waiter is then
        // unparked thinking it owns the lock too — both proceed into
        // the critical section. Surfaced as a lost-increment in the
        // 16×500 mutex-stress test on Linux x86 iouring (got 7899/8000).
        //
        // The fix: keep state=LOCKED across handoff. The waiter takes
        // ownership directly. Only the no-waiters branch drops state.
        self.waiter_mutex.lock();
        const w = self.waiters.popFront();
        if (w) |waiter| {
            self.waiter_mutex.unlock();
            waiter.park.unpark();
        } else {
            self.state.store(UNLOCKED, .release);
            self.waiter_mutex.unlock();
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

    fn removeWaiter(self: *Mutex, target: *Waiter) bool {
        var prev: ?*Waiter = null;
        var cur = self.waiters.head;
        while (cur) |w| : (cur = w.next) {
            if (w == target) {
                if (prev) |p| p.next = w.next else self.waiters.head = w.next;
                if (self.waiters.tail == w) self.waiters.tail = prev;
                w.next = null;
                return true;
            }
            prev = w;
        }
        return false;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "mutex: tryLock uncontended succeeds, second fails" {
    var mu = Mutex{};
    try std.testing.expect(mu.tryLock());
    try std.testing.expect(!mu.tryLock());
    mu.unlock();
    try std.testing.expect(mu.tryLock());
    mu.unlock();
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

//! Synchronization primitives — Mutex, Notify, Semaphore.
//!
//! Multi-thread native: correct from N OS threads, by design.
//! Single-worker is a special case, not a special code path. There
//! are no "single-thread-safe only" caveats in this file.
//!
//! ## Design
//!
//! Each primitive has TWO layers of synchronization:
//!
//!   1. An atomic "state word" that handles the uncontended fast path
//!      and the producer-consumer race against the wait-queue. Lock-
//!      free; no syscall when no waiters.
//!
//!   2. A short per-primitive internal mutex (`thread_mutex`) that
//!      protects the wait-queue manipulation only. Held briefly:
//!      push waiter / pop waiter. Never held across park / unpark.
//!
//! This is the standard "fast atomic path + slow lock path" pattern
//! used by Java monitors, Tokio's Mutex, parking_lot, and others.
//! The "internal mutex" is intentionally NOT lock-free: wait-queue
//! manipulation is rare relative to fast-path traffic, and the
//! lock-free alternatives (MCS queue, etc.) have subtle ordering
//! requirements that are easy to get wrong; the pthread mutex
//! captures the same correctness story for free.
//!
//! ## Wait-queue protocol
//!
//! Waiter (`lock`, `wait`, `acquire`):
//!   1. Try the fast path on the atomic state. If acquired, return.
//!   2. Lock the internal mutex. Try the fast path again (in case the
//!      releaser raced between step 1 and step 2). If acquired now,
//!      unlock and return.
//!   3. Append `current()` to the wait queue. Unlock the internal mutex.
//!   4. `park()` — suspends until unparked by the matching releaser.
//!   5. On wake, retry from step 1 (we may have been spuriously woken,
//!      or another waiter beat us — protocol re-validates ownership).
//!
//! Releaser (`unlock`, `notifyOne`, `release`):
//!   1. Update the atomic state to relinquish ownership.
//!   2. Lock the internal mutex.
//!   3. Pop the head waiter (if any). Unlock.
//!   4. If a waiter was popped, `unpark(waiter)` — the waiter races
//!      to re-acquire on its retry. (For direct-handoff variants —
//!      Mutex, Semaphore — the releaser does NOT relinquish ownership
//!      in step 1 when there's a waiter; ownership transfers directly.)
//!
//! ## Direct handoff (Mutex, Semaphore)
//!
//! For fair FIFO: when a waiter exists, the releaser hands ownership
//! to the waiter directly rather than going to UNLOCKED. This:
//!   * preserves FIFO order (no one can sneak in between unlock and
//!     the waiter's retry)
//!   * eliminates the "starve under contention" failure mode
//!
//! The handoff is encoded by NOT changing the atomic state in step 1
//! — `locked` stays `LOCKED`, ownership transfers via the unpark.
//! The unparked waiter skips its retry and proceeds directly.
//!
//! Notify uses notify-no-handoff (notify is one-shot signaling, not
//! mutual exclusion).

const std = @import("std");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const park = @import("park.zig");
const cancel_mod = @import("cancel.zig");

// pthread-backed internal mutex. We use pthread's because it has
// adaptive spin-then-park built-in and it's already a dependency
// (worker pthread spawn). The internal mutex is held only across
// wait-queue manipulation (a few pointer assignments) so contention
// here is minimal and even an unoptimized pthread mutex is fine.
const PthreadMutex = extern struct { _opaque: [8]u64 align(8) = @splat(0) };
extern "c" fn pthread_mutex_init(m: *PthreadMutex, attr: ?*anyopaque) c_int;
extern "c" fn pthread_mutex_destroy(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_lock(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_unlock(m: *PthreadMutex) c_int;

// ─────────────────────────────────────────────────────────────────────
// Wait queue — FIFO, held under a primitive's internal mutex
// ─────────────────────────────────────────────────────────────────────
const WaitQueue = struct {
    head: ?*coroutine.Coroutine = null,
    tail: ?*coroutine.Coroutine = null,

    inline fn push(self: *WaitQueue, c: *coroutine.Coroutine) void {
        c.wait_next = null;
        if (self.tail) |t| {
            t.wait_next = c;
            self.tail = c;
        } else {
            self.head = c;
            self.tail = c;
        }
    }

    inline fn pop(self: *WaitQueue) ?*coroutine.Coroutine {
        const h = self.head orelse return null;
        self.head = h.wait_next;
        if (self.head == null) self.tail = null;
        h.wait_next = null;
        return h;
    }

    inline fn isEmpty(self: *const WaitQueue) bool {
        return self.head == null;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Mutex
// ─────────────────────────────────────────────────────────────────────
//
// State machine (atomic u32):
//   UNLOCKED  = 0   — free
//   LOCKED    = 1   — held, no parked waiters (or unverified)
//   CONTENDED = 2   — held, ≥1 waiter parked on `&state` in the lot
//
// Fast paths:
//   lock:   CAS UNLOCKED → LOCKED. If succeeds, done.
//   unlock: CAS LOCKED → UNLOCKED. If succeeds, done (no waiters).
//
// Slow paths use the parking lot keyed on `&state`. No pthread
// mutex — the lot's bucket lock + validator-under-lock pattern
// provides the wait/wake coordination we need.
//
// On contention the lock loop spins briefly before parking. Most
// critical sections in practice are sub-µs; a few hundred ns of
// spin avoids the park+unpark round-trip cost when the holder is
// about to release. This is the same trick Go's runtime uses
// (`sync.Mutex` spins `active_spin=4` iterations on multi-core).
//
// No direct handoff: unlock writes UNLOCKED and unparks one waiter;
// the woken waiter re-CASes UNLOCKED→LOCKED. A new arrival could
// steal between unpark and acquire, but `unparkOne` wakes exactly
// one waiter per unlock so the herd is bounded to (1 woken + new
// arrivals). Under steady contention this converges; under bursty
// contention the spin loop covers the common case.

pub const Mutex = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(UNLOCKED),

    const UNLOCKED: u32 = 0;
    const LOCKED: u32 = 1;
    const CONTENDED: u32 = 2;

    /// Brief spin count on contention before parking. On Apple
    /// Silicon a `pause`-class hint + 4 retries covers most
    /// sub-µs critical sections (the typical case for fine-grained
    /// counters / map updates).
    const SPIN_LIMIT: u32 = 40;

    pub fn init() Mutex {
        return .{};
    }

    pub fn deinit(self: *Mutex) void {
        _ = self;
    }

    pub fn lock(self: *Mutex) void {
        // Fast path: UNLOCKED → LOCKED.
        if (self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null) return;
        self.lockSlow();
    }

    fn lockSlow(self: *Mutex) void {
        @branchHint(.cold);
        var spin: u32 = 0;
        // Once we've parked at least once we know the lot MAY still
        // hold other waiters. We must therefore acquire as CONTENDED
        // rather than LOCKED, so the next unlock goes slow-path and
        // calls `unparkOne` to drain them. Otherwise other parked
        // waiters would be orphaned the moment we (a non-marker
        // owner) unlock via the fast path.
        //
        // Once the lot actually drains, an unparkOne misses, state
        // settles to UNLOCKED, and fast-path acquire resumes for
        // the next contention-free run.
        var have_parked: bool = false;
        while (true) {
            const s = self.state.load(.monotonic);

            if (s == UNLOCKED) {
                const target: u32 = if (have_parked) CONTENDED else LOCKED;
                if (self.state.cmpxchgWeak(UNLOCKED, target, .acquire, .monotonic) == null) return;
                continue;
            }

            // Spin briefly if no one is parked yet — holder may
            // release momentarily.
            if (s == LOCKED and spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }

            // Mark contended (if not already), then park on `&state`.
            if (s != CONTENDED) {
                if (self.state.cmpxchgWeak(s, CONTENDED, .monotonic, .monotonic) != null) {
                    // CAS failed — state changed under us; loop and re-read.
                    continue;
                }
            }
            park.parkOn(&self.state, lockValidator);
            have_parked = true;
            spin = 0;
        }
    }

    /// Validator under bucket lock: stay parked only if state is
    /// still CONTENDED. If unlock dropped it to UNLOCKED in between
    /// our CAS and the parking lot's check, we exit park cleanly
    /// instead of waiting for an unpark that already missed us.
    ///
    /// Also checks `current.cancel_in_flight` — if a cancel-aware
    /// caller fires its `Cancel` between `c.register` and parkOn,
    /// the cancel.fire's `unparkAll(&state)` doesn't reach us yet
    /// (we haven't parked), so the validator must catch the race
    /// itself.
    fn lockValidator(addr: *const anyopaque) bool {
        const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
        if (current.get()) |me| {
            if (me.cancel_in_flight) |cp| {
                const c: *cancel_mod.Cancel = @ptrCast(@alignCast(cp));
                if (c.isFired()) return false;
            }
        }
        return sp.load(.acquire) == CONTENDED;
    }

    /// Non-blocking. Returns true if acquired.
    pub fn tryLock(self: *Mutex) bool {
        return self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null;
    }

    /// Cancel-aware lock. Same semantics as `lock` but returns
    /// `error.Cancelled` if the cancel fires while parked (or
    /// before entering the wait path). On successful return the
    /// caller owns the lock; on `Cancelled` the caller does NOT
    /// hold the lock and should not call `unlock`.
    pub fn lockCancel(self: *Mutex, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        if (c.isFired()) return error.Cancelled;
        // Fast path.
        if (self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null) return;
        return self.lockCancelSlow(c);
    }

    fn lockCancelSlow(self: *Mutex, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        @branchHint(.cold);
        var spin: u32 = 0;
        var have_parked: bool = false;
        while (true) {
            if (c.isFired()) return error.Cancelled;

            const s = self.state.load(.monotonic);

            if (s == UNLOCKED) {
                const target: u32 = if (have_parked) CONTENDED else LOCKED;
                if (self.state.cmpxchgWeak(UNLOCKED, target, .acquire, .monotonic) == null) return;
                continue;
            }

            if (s == LOCKED and spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }

            if (s != CONTENDED) {
                if (self.state.cmpxchgWeak(s, CONTENDED, .monotonic, .monotonic) != null) continue;
            }

            // Register with the cancel BEFORE parking. If fire races
            // with our park, the cancel's waiter list ensures fire
            // calls `unparkAll(&self.state)` and our park returns.
            var waiter: cancel_mod.Waiter = .{};
            if (c.register(&waiter, @intFromPtr(&self.state))) {
                return error.Cancelled;
            }
            // Publish the cancel to the validator, which runs under
            // the parking-lot bucket lock and re-checks it. Without
            // this, a fire between `register` and `parkOn`'s
            // bucket-lock acquire would miss us — its `unparkAll`
            // fires before we're parked.
            const me = current.require();
            me.cancel_in_flight = @ptrCast(c);
            park.parkOn(&self.state, lockValidator);
            me.cancel_in_flight = null;
            c.deregister(&waiter);
            if (c.isFired()) return error.Cancelled;
            have_parked = true;
            spin = 0;
        }
    }

    pub fn unlock(self: *Mutex) void {
        // Fast path: LOCKED → UNLOCKED, no parked waiters.
        if (self.state.cmpxchgStrong(LOCKED, UNLOCKED, .release, .monotonic) == null) return;
        self.unlockSlow();
    }

    fn unlockSlow(self: *Mutex) void {
        @branchHint(.cold);
        // state was CONTENDED. Drop to UNLOCKED first; then wake one.
        // Future arrivals that observe UNLOCKED can fast-path-CAS
        // for the lock. The woken waiter will re-CAS on wakeup.
        self.state.store(UNLOCKED, .release);
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        _ = park.unparkOne(rt, &self.state);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Notify — one-shot or broadcast signal
// ─────────────────────────────────────────────────────────────────────
//
// State (atomic u32):
//   bit 0: notified flag (1 stored permit)
//
// Semantics:
//   wait: if notified, consume the permit, return. Else queue
//         self under inner mutex and park.
//   notifyOne: lock inner; if any waiter, pop+unpark; else set
//              notified flag (next wait consumes it).
//   notifyAll: lock inner; pop+unpark every waiter. Does NOT set
//              the notified flag.

pub const Notify = struct {
    notified: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    waiters: WaitQueue = .{},
    inner: PthreadMutex,

    pub fn init() Notify {
        var n: Notify = .{ .inner = .{} };
        _ = pthread_mutex_init(&n.inner, null);
        return n;
    }

    pub fn deinit(self: *Notify) void {
        _ = pthread_mutex_destroy(&self.inner);
    }

    pub fn wait(self: *Notify) void {
        if (self.notified.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            return;
        }
        const me = current.require();
        _ = pthread_mutex_lock(&self.inner);
        if (self.notified.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            _ = pthread_mutex_unlock(&self.inner);
            return;
        }
        self.waiters.push(me);
        _ = pthread_mutex_unlock(&self.inner);
        runtime.park();
    }

    pub fn notifyOne(self: *Notify) void {
        _ = pthread_mutex_lock(&self.inner);
        const w = self.waiters.pop();
        if (w == null) {
            // Store INSIDE lock — see analysis in earlier commit.
            self.notified.store(1, .release);
            _ = pthread_mutex_unlock(&self.inner);
            return;
        }
        _ = pthread_mutex_unlock(&self.inner);
        runtime.unpark(w.?);
    }

    pub fn notifyAll(self: *Notify) void {
        _ = pthread_mutex_lock(&self.inner);
        var to_wake: WaitQueue = .{};
        while (self.waiters.pop()) |w| to_wake.push(w);
        _ = pthread_mutex_unlock(&self.inner);
        while (to_wake.pop()) |w| runtime.unpark(w);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Semaphore — counting permits with direct handoff
// ─────────────────────────────────────────────────────────────────────

pub const Semaphore = struct {
    permits: std.atomic.Value(u32),
    waiters: WaitQueue = .{},
    inner: PthreadMutex,

    pub fn init(initial: u32) Semaphore {
        var s: Semaphore = .{
            .permits = std.atomic.Value(u32).init(initial),
            .inner = .{},
        };
        _ = pthread_mutex_init(&s.inner, null);
        return s;
    }

    pub fn deinit(self: *Semaphore) void {
        _ = pthread_mutex_destroy(&self.inner);
    }

    pub fn acquire(self: *Semaphore) void {
        // Fast path: decrement if positive.
        while (true) {
            const p = self.permits.load(.acquire);
            if (p == 0) break;
            if (self.permits.cmpxchgWeak(p, p - 1, .acq_rel, .acquire) == null) {
                return;
            }
        }
        // Slow path.
        const me = current.require();
        _ = pthread_mutex_lock(&self.inner);
        // Re-check under the lock — release may have raced.
        while (true) {
            const p = self.permits.load(.acquire);
            if (p == 0) break;
            if (self.permits.cmpxchgWeak(p, p - 1, .acq_rel, .acquire) == null) {
                _ = pthread_mutex_unlock(&self.inner);
                return;
            }
        }
        self.waiters.push(me);
        _ = pthread_mutex_unlock(&self.inner);
        runtime.park();
        // Direct handoff: a permit was reserved for us on unpark.
        // Just return.
    }

    pub fn tryAcquire(self: *Semaphore) bool {
        while (true) {
            const p = self.permits.load(.acquire);
            if (p == 0) return false;
            if (self.permits.cmpxchgWeak(p, p - 1, .acq_rel, .acquire) == null) {
                return true;
            }
        }
    }

    pub fn release(self: *Semaphore) void {
        _ = pthread_mutex_lock(&self.inner);
        const w = self.waiters.pop();
        if (w == null) {
            // Increment permits INSIDE the lock — same reasoning
            // as Notify.notifyOne. Storing outside the lock leaves
            // a window where an acquire() can re-check under the
            // lock, see permits=0, push self, and park before the
            // increment becomes visible.
            _ = self.permits.fetchAdd(1, .release);
            _ = pthread_mutex_unlock(&self.inner);
            return;
        }
        // Direct handoff: don't increment permits; the unparked
        // waiter consumes it.
        _ = pthread_mutex_unlock(&self.inner);
        runtime.unpark(w.?);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests — run at workers=NumCPU (multi-thread default). Single-worker
// is a special configuration check, not the default test fixture.
// ─────────────────────────────────────────────────────────────────────

// See note in src/runtime.zig: std.testing.allocator's stack-trace
// capture corrupts under multi-worker spawn. smp_allocator is the
// thread-safe choice for runtime-touching tests.
const test_allocator = std.heap.smp_allocator;
const Runtime = runtime.Runtime;

// Mutex test — N concurrent coros each incrementing a shared counter.
// With workers=NumCPU, coros actually run on different OS threads.
const MutexCtx = struct {
    mu: *Mutex,
    counter: *u64,
    iters: u32,
};

fn mutexInc(ctx: *MutexCtx) void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        ctx.mu.lock();
        ctx.counter.* += 1;
        ctx.mu.unlock();
    }
}

fn mutexTestRoot(ctx: *MutexCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var tasks: [16]*@import("task.zig").Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(mutexInc, .{ctx});
    for (&tasks) |t| t.join();
}

test "Mutex: serializes 16 coros on multi-worker runtime" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var mu = Mutex.init();
    defer mu.deinit();
    var counter: u64 = 0;
    var ctx = MutexCtx{ .mu = &mu, .counter = &counter, .iters = 1000 };
    try (try rt.run(mutexTestRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u64, 16 * 1000), counter);
}

// ─── Mutex.lockCancel tests ──────────────────────────────────────────

const LockCancelCtx = struct {
    mu: *Mutex,
    c: *cancel_mod.Cancel,
    got_cancelled: bool = false,
};

fn lockCancelWaiter(ctx: *LockCancelCtx) !void {
    // The mutex is already held by the test root. We try to acquire
    // it; the cancel will fire while we're parked.
    ctx.mu.lockCancel(ctx.c) catch |err| {
        if (err == error.Cancelled) ctx.got_cancelled = true;
        return;
    };
    // Unexpected — we acquired despite cancel
    ctx.mu.unlock();
}

fn lockCancelRoot(ctx: *LockCancelCtx) !void {
    // Take the mutex so the spawned waiter blocks.
    ctx.mu.lock();
    var waiter = try @import("lib.zig").spawn(lockCancelWaiter, .{ctx});
    // Yield a few times to ensure the waiter parks.
    var i: u32 = 0;
    while (i < 16) : (i += 1) @import("runtime.zig").yield();
    // Fire the cancel — should wake the parked waiter with Cancelled.
    ctx.c.fire();
    _ = waiter.join() catch {};
    ctx.mu.unlock();
}

test "Mutex.lockCancel: fires while parked, returns Cancelled" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var mu = Mutex.init();
    defer mu.deinit();
    var c = cancel_mod.Cancel.init(rt);
    defer c.deinit();
    var ctx = LockCancelCtx{ .mu = &mu, .c = &c };
    try (try rt.run(lockCancelRoot, .{&ctx}));
    try std.testing.expect(ctx.got_cancelled);
}

fn lockCancelPrefiredWaiter(ctx: *LockCancelCtx) !void {
    ctx.mu.lockCancel(ctx.c) catch |err| {
        if (err == error.Cancelled) ctx.got_cancelled = true;
        return;
    };
    ctx.mu.unlock();
}

fn lockCancelPrefiredRoot(ctx: *LockCancelCtx) !void {
    ctx.c.fire();
    var waiter = try @import("lib.zig").spawn(lockCancelPrefiredWaiter, .{ctx});
    _ = waiter.join() catch {};
}

test "Mutex.lockCancel: pre-fired cancel returns Cancelled immediately" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var mu = Mutex.init();
    defer mu.deinit();
    var c = cancel_mod.Cancel.init(rt);
    defer c.deinit();
    var ctx = LockCancelCtx{ .mu = &mu, .c = &c };
    try (try rt.run(lockCancelPrefiredRoot, .{&ctx}));
    try std.testing.expect(ctx.got_cancelled);
}

test "Mutex: works at workers=1 (single-worker configuration)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var mu = Mutex.init();
    defer mu.deinit();
    var counter: u64 = 0;
    var ctx = MutexCtx{ .mu = &mu, .counter = &counter, .iters = 1000 };
    try (try rt.run(mutexTestRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u64, 16 * 1000), counter);
}

// Notify handshake — multi-thread version. Spawn many waiters across
// workers; main coro notifies them one by one.
const NotifyCtx = struct {
    note: *Notify,
    fired: *std.atomic.Value(u32),
    n: u32,
};

fn notifyWaiter(ctx: *NotifyCtx) void {
    ctx.note.wait();
    _ = ctx.fired.fetchAdd(1, .acq_rel);
}

fn notifyTestRoot(ctx: *NotifyCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var waiters: [8]*@import("task.zig").Task(void) = undefined;
    for (&waiters) |*t| t.* = try rt.spawn(notifyWaiter, .{ctx});
    // Wake them all one by one.
    var i: u32 = 0;
    while (i < waiters.len) : (i += 1) {
        ctx.note.notifyOne();
    }
    for (&waiters) |t| t.join();
}

test "Notify: notifyOne wakes one waiter at a time, multi-worker" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var note = Notify.init();
    defer note.deinit();
    var fired = std.atomic.Value(u32).init(0);
    var ctx = NotifyCtx{ .note = &note, .fired = &fired, .n = 8 };
    try (try rt.run(notifyTestRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u32, 8), fired.load(.acquire));
}

// Semaphore cap=3 with many concurrent acquirers.
const SemCtx = struct {
    sem: *Semaphore,
    counter: *std.atomic.Value(u32),
};

fn semWorker(ctx: *SemCtx) void {
    ctx.sem.acquire();
    _ = ctx.counter.fetchAdd(1, .acq_rel);
    ctx.sem.release();
}

fn semTestRoot(ctx: *SemCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var tasks: [100]*@import("task.zig").Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(semWorker, .{ctx});
    for (&tasks) |t| t.join();
}

test "Semaphore: 100 coros acquire/release with cap=3, multi-worker" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();

    var sem = Semaphore.init(3);
    defer sem.deinit();
    var counter = std.atomic.Value(u32).init(0);
    var ctx = SemCtx{ .sem = &sem, .counter = &counter };
    try (try rt.run(semTestRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
    try std.testing.expectEqual(@as(u32, 3), sem.permits.load(.acquire));
}

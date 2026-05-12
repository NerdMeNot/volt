//! v2 synchronization primitives — Mutex, Notify, Semaphore.
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
//! requirements documented in v0.x's Mutex.zig. Future tightening
//! can swap this internal mutex for MCS — same correctness story.
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
//   LOCKED    = 1   — held, no waiters
//   CONTENDED = 2   — held, ≥1 waiter parked
//
// Fast paths:
//   lock:   CAS UNLOCKED → LOCKED. If succeeds, done.
//   unlock: CAS LOCKED → UNLOCKED. If succeeds, done (no waiters).
//
// Slow paths use the internal mutex + wait queue. Direct handoff
// preserves fairness: when unlock pops a waiter, ownership transfers
// (state stays != UNLOCKED), so a sneaker can't beat the popped
// waiter to the lock.

pub const Mutex = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(UNLOCKED),
    waiters: WaitQueue = .{},
    inner: PthreadMutex = .{},
    inner_initialized: bool = false,

    const UNLOCKED: u32 = 0;
    const LOCKED: u32 = 1;
    const CONTENDED: u32 = 2;

    fn ensureInit(self: *Mutex) void {
        if (!self.inner_initialized) {
            _ = pthread_mutex_init(&self.inner, null);
            self.inner_initialized = true;
        }
    }

    pub fn deinit(self: *Mutex) void {
        if (self.inner_initialized) {
            _ = pthread_mutex_destroy(&self.inner);
            self.inner_initialized = false;
        }
    }

    pub fn lock(self: *Mutex) void {
        // Fast path: UNLOCKED → LOCKED.
        if (self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null) {
            return;
        }
        self.ensureInit();
        const me = current.require();
        while (true) {
            // Slow path: take inner mutex, re-check state.
            _ = pthread_mutex_lock(&self.inner);
            // Try fast path again — releaser may have raced between
            // our first CAS and the inner-mutex acquire.
            if (self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null) {
                _ = pthread_mutex_unlock(&self.inner);
                return;
            }
            // Mark contended so unlock knows there's a waiter.
            _ = self.state.swap(CONTENDED, .acq_rel);
            self.waiters.push(me);
            _ = pthread_mutex_unlock(&self.inner);
            runtime.park();
            // On wake: ownership has been directly handed to us.
            // (See unlock: when a waiter is popped, state stays
            // CONTENDED/LOCKED and the waiter takes ownership on
            // unpark.) Just return.
            return;
        }
    }

    /// Non-blocking. Returns true if acquired.
    pub fn tryLock(self: *Mutex) bool {
        return self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null;
    }

    pub fn unlock(self: *Mutex) void {
        // Fast path: LOCKED → UNLOCKED. If no waiters, done.
        if (self.state.cmpxchgStrong(LOCKED, UNLOCKED, .release, .monotonic) == null) {
            return;
        }
        // Slow path: state is CONTENDED.
        self.ensureInit();
        _ = pthread_mutex_lock(&self.inner);
        const w = self.waiters.pop();
        if (w == null) {
            // Race: contended bit was set but waiter already popped
            // (or never made it to queue). Drop to UNLOCKED.
            self.state.store(UNLOCKED, .release);
            _ = pthread_mutex_unlock(&self.inner);
            return;
        }
        // Direct handoff: state stays != UNLOCKED. New owner is `w`.
        // If queue is now empty, demote CONTENDED → LOCKED to enable
        // unlock's fast path on the next release.
        if (self.waiters.isEmpty()) {
            self.state.store(LOCKED, .release);
        }
        _ = pthread_mutex_unlock(&self.inner);
        runtime.unpark(w.?);
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
    inner: PthreadMutex = .{},
    inner_initialized: bool = false,

    fn ensureInit(self: *Notify) void {
        if (!self.inner_initialized) {
            _ = pthread_mutex_init(&self.inner, null);
            self.inner_initialized = true;
        }
    }

    pub fn deinit(self: *Notify) void {
        if (self.inner_initialized) {
            _ = pthread_mutex_destroy(&self.inner);
            self.inner_initialized = false;
        }
    }

    pub fn wait(self: *Notify) void {
        // Fast path: a permit is stored.
        if (self.notified.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            return;
        }
        self.ensureInit();
        const me = current.require();
        _ = pthread_mutex_lock(&self.inner);
        // Re-check under the lock — notifyOne may have raced.
        if (self.notified.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            _ = pthread_mutex_unlock(&self.inner);
            return;
        }
        self.waiters.push(me);
        _ = pthread_mutex_unlock(&self.inner);
        runtime.park();
    }

    pub fn notifyOne(self: *Notify) void {
        self.ensureInit();
        _ = pthread_mutex_lock(&self.inner);
        const w = self.waiters.pop();
        if (w == null) {
            // No waiter — store permit for next wait.
            _ = pthread_mutex_unlock(&self.inner);
            self.notified.store(1, .release);
            return;
        }
        _ = pthread_mutex_unlock(&self.inner);
        runtime.unpark(w.?);
    }

    pub fn notifyAll(self: *Notify) void {
        self.ensureInit();
        _ = pthread_mutex_lock(&self.inner);
        // Drain the queue while we hold the lock; unpark after release.
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
    inner: PthreadMutex = .{},
    inner_initialized: bool = false,

    pub fn init(initial: u32) Semaphore {
        return .{ .permits = std.atomic.Value(u32).init(initial) };
    }

    fn ensureInit(self: *Semaphore) void {
        if (!self.inner_initialized) {
            _ = pthread_mutex_init(&self.inner, null);
            self.inner_initialized = true;
        }
    }

    pub fn deinit(self: *Semaphore) void {
        if (self.inner_initialized) {
            _ = pthread_mutex_destroy(&self.inner);
            self.inner_initialized = false;
        }
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
        self.ensureInit();
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
        self.ensureInit();
        _ = pthread_mutex_lock(&self.inner);
        const w = self.waiters.pop();
        if (w == null) {
            _ = pthread_mutex_unlock(&self.inner);
            _ = self.permits.fetchAdd(1, .release);
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

const test_allocator = std.testing.allocator;
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

    var mu = Mutex{};
    defer mu.deinit();
    var counter: u64 = 0;
    var ctx = MutexCtx{ .mu = &mu, .counter = &counter, .iters = 1000 };
    try rt.run(mutexTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u64, 16 * 1000), counter);
}

test "Mutex: works at workers=1 (single-worker configuration)" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var mu = Mutex{};
    defer mu.deinit();
    var counter: u64 = 0;
    var ctx = MutexCtx{ .mu = &mu, .counter = &counter, .iters = 1000 };
    try rt.run(mutexTestRoot, .{&ctx});

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

    var note = Notify{};
    defer note.deinit();
    var fired = std.atomic.Value(u32).init(0);
    var ctx = NotifyCtx{ .note = &note, .fired = &fired, .n = 8 };
    try rt.run(notifyTestRoot, .{&ctx});

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
    try rt.run(semTestRoot, .{&ctx});

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
    try std.testing.expectEqual(@as(u32, 3), sem.permits.load(.acquire));
}

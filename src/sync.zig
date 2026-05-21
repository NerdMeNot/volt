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
// RwLock — multiple-readers OR single-writer
// ─────────────────────────────────────────────────────────────────────
//
// State word (atomic u32):
//   bits 0-29 (30 bits): active reader count (~1 billion max)
//   bit 30: WRITER_WAITING — ≥1 writer parked / in slow path
//   bit 31: WRITER_HELD — a writer holds the lock exclusively
//
// Parking key: `&self.state` (single key; readers + writers park
// here, validators sort out who can proceed). Wakes use
// `unparkAll` so woken readers can rush in together; woken writers
// re-CAS for WRITER_HELD. This trades a little wake-thrash under
// contention for a very small state-machine — the parking-lot
// queue stays the only data structure that holds waiters.

pub const RwLock = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    const WRITER_HELD: u32 = 1 << 31;
    const WRITER_WAITING: u32 = 1 << 30;
    const READER_MASK: u32 = (1 << 30) - 1;
    const SPIN_LIMIT: u32 = 40;

    pub fn init() RwLock {
        return .{};
    }

    pub fn deinit(self: *RwLock) void {
        _ = self;
    }

    /// Acquire shared (read) access. Blocks while a writer holds
    /// or is waiting.
    pub fn readLock(self: *RwLock) void {
        if (self.tryReadLockFast()) return;
        self.readLockSlow();
    }

    inline fn tryReadLockFast(self: *RwLock) bool {
        const s = self.state.load(.monotonic);
        if (s & (WRITER_HELD | WRITER_WAITING) != 0) return false;
        return self.state.cmpxchgWeak(s, s + 1, .acquire, .monotonic) == null;
    }

    fn readLockSlow(self: *RwLock) void {
        @branchHint(.cold);
        var spin: u32 = 0;
        while (true) {
            const s = self.state.load(.monotonic);
            if (s & (WRITER_HELD | WRITER_WAITING) == 0) {
                if (self.state.cmpxchgWeak(s, s + 1, .acquire, .monotonic) == null) return;
                continue;
            }
            if (spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }
            park.parkOn(&self.state, readValidator);
            spin = 0;
        }
    }

    /// Try to acquire a read lock without blocking. Returns true
    /// on success.
    pub fn tryReadLock(self: *RwLock) bool {
        return self.tryReadLockFast();
    }

    /// Cancel-aware read lock. Returns `error.Cancelled` if the
    /// cancel fires while parked.
    pub fn readLockCancel(self: *RwLock, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        if (c.isFired()) return error.Cancelled;
        if (self.tryReadLockFast()) return;
        return self.readLockCancelSlow(c);
    }

    fn readLockCancelSlow(self: *RwLock, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        @branchHint(.cold);
        var spin: u32 = 0;
        while (true) {
            if (c.isFired()) return error.Cancelled;
            const s = self.state.load(.monotonic);
            if (s & (WRITER_HELD | WRITER_WAITING) == 0) {
                if (self.state.cmpxchgWeak(s, s + 1, .acquire, .monotonic) == null) return;
                continue;
            }
            if (spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }

            var waiter: cancel_mod.Waiter = .{};
            if (c.register(&waiter, @intFromPtr(&self.state))) return error.Cancelled;
            const me = current.require();
            me.cancel_in_flight = @ptrCast(c);
            park.parkOn(&self.state, readValidator);
            me.cancel_in_flight = null;
            c.deregister(&waiter);
            if (c.isFired()) return error.Cancelled;
            spin = 0;
        }
    }

    /// Release a previously-acquired read lock. Last reader out
    /// with a writer waiting wakes the wait queue.
    pub fn readUnlock(self: *RwLock) void {
        const prev = self.state.fetchSub(1, .release);
        // If we were the last reader and a writer is waiting, wake.
        // `unparkAll` (vs One) is intentional — parked readers
        // re-check via their validator and re-park if needed; the
        // writer wakes and races for WRITER_HELD.
        if ((prev & READER_MASK) == 1 and (prev & WRITER_WAITING) != 0) {
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            _ = park.unparkAll(rt, &self.state);
        }
    }

    /// Acquire exclusive (write) access. Blocks while any reader or
    /// another writer holds.
    pub fn writeLock(self: *RwLock) void {
        if (self.state.cmpxchgStrong(0, WRITER_HELD, .acquire, .monotonic) == null) return;
        self.writeLockSlow();
    }

    fn writeLockSlow(self: *RwLock) void {
        @branchHint(.cold);
        var spin: u32 = 0;
        while (true) {
            const s = self.state.load(.monotonic);
            // Free / only-other-writers-waiting → try to claim.
            if (s == 0) {
                if (self.state.cmpxchgWeak(0, WRITER_HELD, .acquire, .monotonic) == null) return;
                continue;
            }
            if (s == WRITER_WAITING) {
                if (self.state.cmpxchgWeak(WRITER_WAITING, WRITER_HELD | WRITER_WAITING, .acquire, .monotonic) == null) return;
                continue;
            }
            // Otherwise: holders present. Ensure WRITER_WAITING is set
            // so future readers park.
            if (s & WRITER_WAITING == 0) {
                if (self.state.cmpxchgWeak(s, s | WRITER_WAITING, .monotonic, .monotonic) != null) continue;
            }
            if (spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }
            park.parkOn(&self.state, writeValidator);
            spin = 0;
        }
    }

    /// Try to acquire a write lock without blocking.
    pub fn tryWriteLock(self: *RwLock) bool {
        return self.state.cmpxchgStrong(0, WRITER_HELD, .acquire, .monotonic) == null;
    }

    /// Cancel-aware write lock.
    pub fn writeLockCancel(self: *RwLock, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        if (c.isFired()) return error.Cancelled;
        if (self.state.cmpxchgStrong(0, WRITER_HELD, .acquire, .monotonic) == null) return;
        return self.writeLockCancelSlow(c);
    }

    fn writeLockCancelSlow(self: *RwLock, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        @branchHint(.cold);
        var spin: u32 = 0;
        while (true) {
            if (c.isFired()) return error.Cancelled;
            const s = self.state.load(.monotonic);
            if (s == 0) {
                if (self.state.cmpxchgWeak(0, WRITER_HELD, .acquire, .monotonic) == null) return;
                continue;
            }
            if (s == WRITER_WAITING) {
                if (self.state.cmpxchgWeak(WRITER_WAITING, WRITER_HELD | WRITER_WAITING, .acquire, .monotonic) == null) return;
                continue;
            }
            if (s & WRITER_WAITING == 0) {
                if (self.state.cmpxchgWeak(s, s | WRITER_WAITING, .monotonic, .monotonic) != null) continue;
            }
            if (spin < SPIN_LIMIT) {
                std.atomic.spinLoopHint();
                spin += 1;
                continue;
            }

            var waiter: cancel_mod.Waiter = .{};
            if (c.register(&waiter, @intFromPtr(&self.state))) return error.Cancelled;
            const me = current.require();
            me.cancel_in_flight = @ptrCast(c);
            park.parkOn(&self.state, writeValidator);
            me.cancel_in_flight = null;
            c.deregister(&waiter);
            if (c.isFired()) return error.Cancelled;
            spin = 0;
        }
    }

    /// Release a write lock. Wakes everyone parked on the state
    /// word; readers and the next writer race for entry via their
    /// validators + CAS.
    pub fn writeUnlock(self: *RwLock) void {
        // Common case: no waiters → just clear and done.
        if (self.state.cmpxchgStrong(WRITER_HELD, 0, .release, .monotonic) == null) return;
        self.writeUnlockSlow();
    }

    fn writeUnlockSlow(self: *RwLock) void {
        @branchHint(.cold);
        // state was WRITER_HELD | WRITER_WAITING. Drop WRITER_HELD,
        // keep WRITER_WAITING (the queued writers re-claim it
        // themselves on the next CAS). All parked waiters re-evaluate.
        _ = self.state.fetchAnd(~WRITER_HELD, .release);
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        _ = park.unparkAll(rt, &self.state);
    }

    fn readValidator(addr: *const anyopaque) bool {
        if (currentCancelFired()) return false;
        const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
        const s = sp.load(.acquire);
        // Keep parking iff a writer holds OR a writer is waiting.
        return (s & (WRITER_HELD | WRITER_WAITING)) != 0;
    }

    fn writeValidator(addr: *const anyopaque) bool {
        if (currentCancelFired()) return false;
        const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
        const s = sp.load(.acquire);
        // Keep parking iff there's a holder. State == 0 or
        // WRITER_WAITING-only means we can race to acquire.
        return !(s == 0 or s == WRITER_WAITING);
    }
};

/// Shared helper for sync primitives' validators — checks
/// whether the currently-running coroutine has a `Cancel`
/// in flight that has fired.
inline fn currentCancelFired() bool {
    if (current.get()) |me| {
        if (me.cancel_in_flight) |cp| {
            const c: *cancel_mod.Cancel = @ptrCast(@alignCast(cp));
            return c.isFired();
        }
    }
    return false;
}

// ─────────────────────────────────────────────────────────────────────
// OnceCell(T) — lazy single-init
// ─────────────────────────────────────────────────────────────────────
//
// State word:
//   0 = EMPTY   (no init started)
//   1 = INIT_IN_FLIGHT (some coroutine is inside `init_fn`)
//   2 = INITIALIZED (value is published)
//
// Init is one-shot: the first `get` caller CAS-claims the slot,
// runs `init_fn`, stores the value, and unparks any followers.
// The followers see INIT_IN_FLIGHT and park on `&self.state`
// until the initial caller publishes.
//
// Cancel applies only to the *parking* side — `init_fn` itself
// runs to completion, matching Tokio's semantics: interrupting
// the initializer mid-flight would leave the cell wedged with
// no clean recovery.

pub fn OnceCell(comptime T: type) type {
    return struct {
        const Self = @This();
        const EMPTY: u32 = 0;
        const INIT_IN_FLIGHT: u32 = 1;
        const INITIALIZED: u32 = 2;

        state: std.atomic.Value(u32) = std.atomic.Value(u32).init(EMPTY),
        value: T = undefined,

        pub fn init() Self {
            return .{};
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }

        /// Returns true if the cell has been initialized — useful
        /// for cheap non-blocking checks.
        pub inline fn isInitialized(self: *const Self) bool {
            return self.state.load(.acquire) == INITIALIZED;
        }

        /// Returns the cell's value, calling `init_fn(init_ctx)`
        /// to compute it on the first call (across all callers).
        /// Concurrent callers block on `&self.state` until the
        /// first caller's `init_fn` returns.
        pub fn get(self: *Self, init_ctx: anytype, comptime init_fn: anytype) T {
            // Fast path: already initialized.
            if (self.state.load(.acquire) == INITIALIZED) return self.value;
            return self.getSlow(init_ctx, init_fn);
        }

        fn getSlow(self: *Self, init_ctx: anytype, comptime init_fn: anytype) T {
            @branchHint(.cold);
            while (true) {
                const s = self.state.load(.acquire);
                if (s == INITIALIZED) return self.value;
                if (s == EMPTY) {
                    if (self.state.cmpxchgStrong(EMPTY, INIT_IN_FLIGHT, .acquire, .acquire) == null) {
                        // We own the init.
                        self.value = init_fn(init_ctx);
                        self.state.store(INITIALIZED, .release);
                        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
                        _ = park.unparkAll(rt, &self.state);
                        return self.value;
                    }
                    continue;
                }
                // INIT_IN_FLIGHT — park.
                park.parkOn(&self.state, onceValidator);
            }
        }

        /// Cancel-aware variant. Only affects waiters parked on a
        /// concurrent initializer — never interrupts `init_fn` if
        /// the calling coro is the one doing the init.
        pub fn getCancel(self: *Self, init_ctx: anytype, comptime init_fn: anytype, c: *cancel_mod.Cancel) cancel_mod.Error!T {
            if (self.state.load(.acquire) == INITIALIZED) return self.value;
            return self.getCancelSlow(init_ctx, init_fn, c);
        }

        fn getCancelSlow(self: *Self, init_ctx: anytype, comptime init_fn: anytype, c: *cancel_mod.Cancel) cancel_mod.Error!T {
            @branchHint(.cold);
            while (true) {
                if (c.isFired()) return error.Cancelled;
                const s = self.state.load(.acquire);
                if (s == INITIALIZED) return self.value;
                if (s == EMPTY) {
                    if (self.state.cmpxchgStrong(EMPTY, INIT_IN_FLIGHT, .acquire, .acquire) == null) {
                        // We own the init — runs to completion
                        // even if cancel fires.
                        self.value = init_fn(init_ctx);
                        self.state.store(INITIALIZED, .release);
                        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
                        _ = park.unparkAll(rt, &self.state);
                        return self.value;
                    }
                    continue;
                }

                var waiter: cancel_mod.Waiter = .{};
                if (c.register(&waiter, @intFromPtr(&self.state))) return error.Cancelled;
                const me = current.require();
                me.cancel_in_flight = @ptrCast(c);
                park.parkOn(&self.state, onceValidator);
                me.cancel_in_flight = null;
                c.deregister(&waiter);
                if (c.isFired()) return error.Cancelled;
            }
        }

        fn onceValidator(addr: *const anyopaque) bool {
            if (currentCancelFired()) return false;
            const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
            return sp.load(.acquire) == INIT_IN_FLIGHT;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Barrier — N-way cyclic rendezvous
// ─────────────────────────────────────────────────────────────────────
//
// Each `arrive` decrements `remaining`. When `remaining` reaches
// 0, all parked siblings wake (and `remaining` resets to N for
// the next cycle — Barrier is cyclic, matching the C/POSIX
// pthread_barrier_t shape).
//
// Each cycle bumps `generation`. Parked coroutines remember the
// generation they entered on; the validator keeps them parked
// only while `remaining > 0 AND generation == entry_gen`. This
// avoids the classic "early arrivers from cycle N+1 race with
// late wake of cycle N" hazard.

pub const Barrier = struct {
    /// Packed state — high 16 bits: generation counter (wraps);
    /// low 16 bits: `remaining` arrivals for the current cycle.
    /// Atomic so multi-worker `arrive`s race correctly.
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    n: u16,

    const REMAINING_MASK: u32 = 0xFFFF;
    const GEN_SHIFT: u5 = 16;

    pub fn init(n: u32) Barrier {
        std.debug.assert(n > 0 and n <= std.math.maxInt(u16));
        return .{ .n = @intCast(n), .state = std.atomic.Value(u32).init(n) };
    }

    pub fn deinit(self: *Barrier) void {
        _ = self;
    }

    /// Block until N-1 other coroutines have also arrived. The
    /// last arrival wakes all and rolls the generation.
    pub fn arrive(self: *Barrier) void {
        // Snapshot generation under which we entered.
        const entry_state = self.state.load(.acquire);
        const entry_gen = entry_state >> GEN_SHIFT;

        // Decrement remaining.
        const prev = self.state.fetchSub(1, .acq_rel);
        const prev_remaining = prev & REMAINING_MASK;
        if (prev_remaining == 1) {
            // We're the last arrival — reset remaining to n,
            // bump generation, wake everyone.
            const new_gen = ((prev >> GEN_SHIFT) + 1) & 0xFFFF;
            self.state.store((new_gen << GEN_SHIFT) | @as(u32, self.n), .release);
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            _ = park.unparkAll(rt, &self.state);
            return;
        }

        // Not last — park until generation rolls.
        var ctx = BarrierWaitCtx{ .entry_gen = entry_gen };
        const me = current.require();
        // Stash the entry_gen so the validator can compare. We
        // reuse `cancel_in_flight` as a generic scratch slot —
        // safe because validators only fire on the parked coro
        // and we restore null on exit.
        const saved = me.cancel_in_flight;
        me.cancel_in_flight = @ptrCast(&ctx);
        park.parkOn(&self.state, barrierValidator);
        me.cancel_in_flight = saved;
    }

    /// Cancel-aware variant. If the cancel fires while parked,
    /// the coro returns `error.Cancelled` and the barrier
    /// counter is rolled back so the remaining N-1 cohort can
    /// still rendezvous.
    pub fn arriveCancel(self: *Barrier, c: *cancel_mod.Cancel) cancel_mod.Error!void {
        if (c.isFired()) return error.Cancelled;

        const entry_state = self.state.load(.acquire);
        const entry_gen = entry_state >> GEN_SHIFT;
        const prev = self.state.fetchSub(1, .acq_rel);
        const prev_remaining = prev & REMAINING_MASK;
        if (prev_remaining == 1) {
            const new_gen = ((prev >> GEN_SHIFT) + 1) & 0xFFFF;
            self.state.store((new_gen << GEN_SHIFT) | @as(u32, self.n), .release);
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            _ = park.unparkAll(rt, &self.state);
            return;
        }

        var waiter: cancel_mod.Waiter = .{};
        if (c.register(&waiter, @intFromPtr(&self.state))) {
            // Cancel already fired — roll back our arrival so
            // the remaining cohort still rendezvouses.
            _ = self.state.fetchAdd(1, .acq_rel);
            return error.Cancelled;
        }

        const me = current.require();
        const saved = me.cancel_in_flight;
        var ctx = BarrierCancelWaitCtx{ .entry_gen = entry_gen, .cancel = c };
        me.cancel_in_flight = @ptrCast(&ctx);
        park.parkOn(&self.state, barrierCancelValidator);
        me.cancel_in_flight = saved;
        c.deregister(&waiter);

        if (c.isFired()) {
            // Cancel fired while parked. Roll back our decrement
            // so the barrier doesn't release the cohort short.
            _ = self.state.fetchAdd(1, .acq_rel);
            return error.Cancelled;
        }
    }
};

const BarrierWaitCtx = struct {
    entry_gen: u32,
};

const BarrierCancelWaitCtx = struct {
    entry_gen: u32,
    cancel: *cancel_mod.Cancel,
};

fn barrierValidator(addr: *const anyopaque) bool {
    const me = current.get() orelse return false;
    const ctx_ptr = me.cancel_in_flight orelse return false;
    const ctx: *const BarrierWaitCtx = @ptrCast(@alignCast(ctx_ptr));
    const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    const s = sp.load(.acquire);
    // Keep parking iff generation hasn't rolled forward.
    return (s >> Barrier.GEN_SHIFT) == ctx.entry_gen;
}

fn barrierCancelValidator(addr: *const anyopaque) bool {
    const me = current.get() orelse return false;
    const ctx_ptr = me.cancel_in_flight orelse return false;
    const ctx: *const BarrierCancelWaitCtx = @ptrCast(@alignCast(ctx_ptr));
    if (ctx.cancel.isFired()) return false;
    const sp: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    const s = sp.load(.acquire);
    return (s >> Barrier.GEN_SHIFT) == ctx.entry_gen;
}

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

// `volt.testing.allocator` — leak-detecting + multi-worker-safe.
// See src/testing.zig for why std.testing.allocator doesn't fit.
const test_allocator = @import("testing.zig").allocator;
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
    entered: *std.atomic.Value(u32),
    n: u32,
};

fn notifyWaiter(ctx: *NotifyCtx) void {
    _ = ctx.entered.fetchAdd(1, .acq_rel);
    ctx.note.wait();
    _ = ctx.fired.fetchAdd(1, .acq_rel);
}

fn notifyTestRoot(ctx: *NotifyCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var waiters: [8]*@import("task.zig").Task(void) = undefined;
    for (&waiters) |*t| t.* = try rt.spawn(notifyWaiter, .{ctx});
    // Notify's `notifyOne`-with-no-waiter stores a single permit
    // (Tokio-style; multiple unmatched notifies coalesce to one).
    // If R fires its 8 notifies before any waiter has parked, 7
    // waiters then park forever. We must wait for all waiters to
    // reach `wait()` and then give the dispatcher a beat to swap
    // them out — same pattern as `bench/bench_rss.zig`.
    while (ctx.entered.load(.acquire) < ctx.n) runtime.yield();
    var y: u32 = 0;
    while (y < 100) : (y += 1) runtime.yield();
    var i: u32 = 0;
    while (i < waiters.len) : (i += 1) {
        ctx.note.notifyOne();
    }
    for (&waiters) |t| t.join();
}

test "Notify: notifyOne wakes one waiter at a time, multi-worker" {
    // `workers = 2` deliberately forces the race density that exposes
    // the "notify before waiter park" misuse pattern. At higher worker
    // counts the dispatch latency hides this race on most hardware; at
    // 2 workers it surfaces deterministically — so this test stays a
    // regression check for the `notifyTestRoot` synchronization.
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();

    var note = Notify.init();
    defer note.deinit();
    var fired = std.atomic.Value(u32).init(0);
    var entered = std.atomic.Value(u32).init(0);
    var ctx = NotifyCtx{ .note = &note, .fired = &fired, .entered = &entered, .n = 8 };
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

// ─────────────────────────────────────────────────────────────────────
// RwLock tests
// ─────────────────────────────────────────────────────────────────────

const RwCtx = struct {
    rw: *RwLock,
    counter: *u64,
    reads_done: *std.atomic.Value(u32),
    writes_done: *std.atomic.Value(u32),
};

fn rwReader(ctx: *RwCtx) void {
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        ctx.rw.readLock();
        _ = ctx.counter.*;
        ctx.rw.readUnlock();
    }
    _ = ctx.reads_done.fetchAdd(1, .acq_rel);
}

fn rwWriter(ctx: *RwCtx) void {
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        ctx.rw.writeLock();
        ctx.counter.* += 1;
        ctx.rw.writeUnlock();
    }
    _ = ctx.writes_done.fetchAdd(1, .acq_rel);
}

fn rwTestRoot(ctx: *RwCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var readers: [4]*@import("task.zig").Task(void) = undefined;
    var writers: [4]*@import("task.zig").Task(void) = undefined;
    for (&readers) |*t| t.* = try rt.spawn(rwReader, .{ctx});
    for (&writers) |*t| t.* = try rt.spawn(rwWriter, .{ctx});
    for (readers) |t| t.join();
    for (writers) |t| t.join();
}

test "RwLock: 4 readers x 4 writers, multi-worker counter sanity" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var rw = RwLock.init();
    defer rw.deinit();
    var counter: u64 = 0;
    var reads_done = std.atomic.Value(u32).init(0);
    var writes_done = std.atomic.Value(u32).init(0);
    var ctx = RwCtx{
        .rw = &rw,
        .counter = &counter,
        .reads_done = &reads_done,
        .writes_done = &writes_done,
    };
    try (try rt.run(rwTestRoot, .{&ctx}));
    // Each writer adds 10; 4 writers × 10 increments → counter == 40.
    try std.testing.expectEqual(@as(u64, 40), counter);
    try std.testing.expectEqual(@as(u32, 4), reads_done.load(.acquire));
    try std.testing.expectEqual(@as(u32, 4), writes_done.load(.acquire));
}

const RwCancelCtx = struct {
    rw: *RwLock,
    cancel: *cancel_mod.Cancel,
    got: ?anyerror = null,
};

fn rwCancelReader(ctx: *RwCancelCtx) void {
    const r = ctx.rw.readLockCancel(ctx.cancel);
    ctx.got = if (r) |_| null else |e| e;
}

fn rwCancelRoot(ctx: *RwCancelCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    // Writer holds — reader can't get in.
    ctx.rw.writeLock();
    var reader = try rt.spawn(rwCancelReader, .{ctx});
    // Yield enough that the reader parks.
    var i: u32 = 0;
    while (i < 50) : (i += 1) runtime.yield();
    ctx.cancel.fire();
    _ = reader.join();
    ctx.rw.writeUnlock();
}

test "RwLock.readLockCancel: fires while parked, returns Cancelled" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var rw = RwLock.init();
    defer rw.deinit();
    var c = cancel_mod.Cancel.init(rt);
    defer c.deinit();
    var ctx = RwCancelCtx{ .rw = &rw, .cancel = &c };
    try (try rt.run(rwCancelRoot, .{&ctx}));
    try std.testing.expectEqual(@as(?anyerror, error.Cancelled), ctx.got);
}

// ─────────────────────────────────────────────────────────────────────
// OnceCell + Barrier tests
// ─────────────────────────────────────────────────────────────────────

const OnceInitCount = struct { calls: std.atomic.Value(u32) = std.atomic.Value(u32).init(0) };
fn onceInitFn(ctx: *OnceInitCount) u32 {
    _ = ctx.calls.fetchAdd(1, .acq_rel);
    return 0x42;
}

const OnceCtx = struct {
    cell: *OnceCell(u32),
    init_ctx: *OnceInitCount,
    results: [8]u32 = .{0} ** 8,
};

fn onceGetter(ctx: *OnceCtx, idx: u32) void {
    ctx.results[idx] = ctx.cell.get(ctx.init_ctx, onceInitFn);
}

fn onceTestRoot(ctx: *OnceCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var tasks: [8]*@import("task.zig").Task(void) = undefined;
    for (&tasks, 0..) |*t, i| t.* = try rt.spawn(onceGetter, .{ ctx, @as(u32, @intCast(i)) });
    for (tasks) |t| t.join();
}

test "OnceCell: init_fn runs exactly once under contention" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var cell = OnceCell(u32){};
    var init_ctx = OnceInitCount{};
    var ctx = OnceCtx{ .cell = &cell, .init_ctx = &init_ctx };
    try (try rt.run(onceTestRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u32, 1), init_ctx.calls.load(.acquire));
    for (ctx.results) |r| try std.testing.expectEqual(@as(u32, 0x42), r);
}

const BarrierCtx = struct {
    bar: *Barrier,
    arrived: *std.atomic.Value(u32),
    passed: *std.atomic.Value(u32),
};

fn barrierArrival(ctx: *BarrierCtx) void {
    _ = ctx.arrived.fetchAdd(1, .acq_rel);
    ctx.bar.arrive();
    _ = ctx.passed.fetchAdd(1, .acq_rel);
}

fn barrierTestRoot(ctx: *BarrierCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var tasks: [4]*@import("task.zig").Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(barrierArrival, .{ctx});
    for (tasks) |t| t.join();
}

test "Barrier: 4-way rendezvous releases all arrivals together" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var bar = Barrier.init(4);
    defer bar.deinit();
    var arrived = std.atomic.Value(u32).init(0);
    var passed = std.atomic.Value(u32).init(0);
    var ctx = BarrierCtx{ .bar = &bar, .arrived = &arrived, .passed = &passed };
    try (try rt.run(barrierTestRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u32, 4), arrived.load(.acquire));
    try std.testing.expectEqual(@as(u32, 4), passed.load(.acquire));
}

//! Semaphore - Async Counting Semaphore
//!
//! A counting semaphore that limits concurrent access to a resource.
//! Tasks can acquire permits (decrement) and release permits (increment).
//! For blocking semaphore, use std.Thread.Semaphore from the standard library.
//!
//! ## Usage
//!
//! ```zig
//! var sem = Semaphore.init(3);  // Allow 3 concurrent accessors
//!
//! // Try to acquire without waiting (non-blocking)
//! if (sem.tryAcquire(1)) {
//!     defer sem.release(1);
//!     // Got permit
//! }
//!
//! // Async acquire with waiter
//! var waiter = Semaphore.Waiter.init(1);
//! if (!sem.acquireWait(&waiter)) {
//!     // Yield to scheduler, wait for notification
//! }
//! defer sem.release(1);
//! ```
//!
//! ## Design
//!
//! Batch semaphore algorithm with direct-handoff to prevent starvation:
//!
//! - tryAcquire: lock-free CAS loop (no mutex)
//! - release: ALWAYS takes the mutex, serves waiters directly from the
//!   release amount, only surplus goes to the atomic. No separate
//!   waiter_count -- queue emptiness checked under lock.
//! - acquireWait: lock-free CAS fast path for full acquisition. If
//!   insufficient permits, locks the mutex BEFORE the CAS that drains
//!   remaining permits. This eliminates the release-before-queue race.
//!
//! Key invariant: permits NEVER float in the atomic when waiters are
//! queued. release() serves waiters directly, surplus to atomic.
//! This prevents starvation and missed wakeups.

const std = @import("std");

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;
const WakeList = @import("../internal/util/wake_list.zig").DefaultWakeList;

// Future system imports
const future_mod = @import("../future.zig");
const FutureWaker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// A waiter for semaphore permits.
///
/// The `state` field tracks remaining permits needed (starts at requested,
/// decrements to 0). Completion is when state == 0.
pub const Waiter = struct {
    /// Remaining permits needed. Starts at `permits_requested`, decremented
    /// by assignPermits(). When 0, the waiter is satisfied.
    state: std.atomic.Value(usize),

    /// Original number of permits requested (for cancellation math)
    permits_requested: usize,

    /// Waker to invoke when permits are granted
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Intrusive list pointers
    pointers: Pointers(Waiter) = .{},

    /// Debug-mode invocation tracking
    invocation: InvocationId = .{},

    const Self = @This();

    pub fn init(permits: usize) Self {
        return .{
            .state = std.atomic.Value(usize).init(permits),
            .permits_requested = permits,
        };
    }

    /// Create a waiter with waker already configured.
    pub fn initWithWaker(permits: usize, ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{
            .state = std.atomic.Value(usize).init(permits),
            .permits_requested = permits,
            .waker_ctx = ctx,
            .waker = wake_fn,
        };
    }

    /// Set the waker for this waiter
    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    /// Check if the waiter is ready (acquisition is complete).
    /// Completion = state == 0 (all requested permits granted).
    pub fn isReady(self: *const Self) bool {
        return self.state.load(.acquire) == 0;
    }

    /// Check if acquisition is complete (alias for isReady).
    pub const isComplete = isReady;

    /// Get invocation token (for debug tracking)
    pub fn token(self: *const Self) InvocationId.Id {
        return self.invocation.get();
    }

    /// Verify invocation token matches (debug mode)
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.invocation.verify(tok);
    }

    /// Reset for reuse (generates new invocation ID)
    pub fn reset(self: *Self) void {
        self.state.store(self.permits_requested, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }

    /// Assign permits to this waiter. Decrements state by min(state, *n),
    /// decrements *n by the same amount. Returns true if waiter is now
    /// satisfied (state == 0).
    ///
    /// MUST be called under the semaphore's mutex. Since only the mutex
    /// holder modifies waiter.state, a plain store is used (no CAS needed).
    /// The .release ordering ensures the polling task's .acquire load in
    /// isComplete() sees the update.
    fn assignPermits(self: *Waiter, n: *usize) bool {
        const curr = self.state.load(.monotonic);
        if (curr == 0 or n.* == 0) return curr == 0;
        const assign = @min(curr, n.*);
        self.state.store(curr - assign, .release);
        n.* -= assign;
        return (curr - assign) == 0;
    }
};

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Intrusive list of waiters
const WaiterList = LinkedList(Waiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Semaphore
// ─────────────────────────────────────────────────────────────────────────────

/// A counting semaphore.
///
/// Batch semaphore with direct-handoff:
/// - tryAcquire: lock-free CAS (no mutex)
/// - release: lock-free fast_waiter slot for single-waiter case, mutex slow path
/// - acquireWait: lock-free fast path, mutex-before-CAS slow path
pub const Semaphore = struct {
    /// Currently available permits (only modified under mutex OR via CAS).
    /// Cache-line aligned to avoid false sharing with mutex/waiters under
    /// contention — permits is CAS'd by every tryAcquire on the fast path.
    permits: std.atomic.Value(usize) align(std.atomic.cache_line),

    /// Atomic single-waiter fast slot for lock-free release.
    /// Value: 0 = empty, non-zero = pointer to Waiter.
    /// Registration: CAS 0→ptr under mutex (in acquireWaitDirect).
    /// Wake: swap to 0 in release() before taking mutex (lock-free).
    ///
    /// NOT the same as the has_waiters flag that deadlocked (see MEMORY.md).
    /// That checked a flag to skip mutex — permits floated in the atomic
    /// while a waiter was queued. fast_waiter atomically CONSUMES a waiter
    /// pointer via swap, handing permits directly without the atomic counter.
    fast_waiter: std.atomic.Value(usize),

    /// Mutex protecting the waiters list. Taken by release() slow path and
    /// acquireWait()'s slow path. NOT taken by tryAcquire().
    mutex: std.Thread.Mutex,

    /// Waiters list: pushFront for new waiters, serve from back (FIFO)
    waiters: WaiterList,

    const Self = @This();

    /// Create a semaphore with the given number of permits.
    pub fn init(permits: usize) Self {
        return .{
            .permits = std.atomic.Value(usize).init(permits),
            .fast_waiter = std.atomic.Value(usize).init(0),
            .mutex = .{},
            .waiters = .{},
        };
    }

    /// Try to acquire permits without waiting (lock-free CAS loop).
    /// Returns true if permits were acquired, false if not enough available.
    pub fn tryAcquire(self: *Self, num: usize) bool {
        var current = self.permits.load(.acquire);
        while (true) {
            if (current < num) {
                return false;
            }

            if (self.permits.cmpxchgWeak(
                current,
                current - num,
                .acq_rel,
                .acquire,
            )) |new_val| {
                current = new_val;
                continue;
            }

            return true;
        }
    }

    /// Low-level: Acquire permits with explicit waiter.
    /// Returns true if permits were acquired immediately.
    /// Returns false if the waiter was added to the queue (task should yield).
    ///
    /// The waiter must be kept alive until complete or cancelled.
    ///
    /// Algorithm:
    /// 1. Lock-free CAS for full acquisition (fast path, no mutex)
    /// 2. If insufficient: lock mutex BEFORE CAS that drains remaining permits
    /// 3. Under lock: drain what we can, queue waiter if still not enough
    /// No post-queue re-check needed — release() also takes the mutex.
    pub fn acquireWait(self: *Self, waiter: *Waiter) bool {
        const needed = waiter.state.load(.acquire);

        // Fast path: try lock-free CAS for full acquisition
        var curr = self.permits.load(.acquire);
        while (curr >= needed) {
            if (self.permits.cmpxchgWeak(curr, curr - needed, .acq_rel, .acquire)) |updated| {
                curr = updated;
                continue;
            }
            // Got all permits lock-free
            waiter.state.store(0, .release);
            return true;
        }

        // Slow path: lock BEFORE CAS to close the race window.
        // This ensures no release() can add permits between our CAS and
        // queue insertion, because release() also needs this mutex.
        self.mutex.lock();

        // Under lock: CAS to drain available permits.
        // tryAcquire() is lock-free, so permits can change concurrently.
        var acquired: usize = 0;
        curr = self.permits.load(.acquire);
        while (acquired < needed and curr > 0) {
            const take = @min(curr, needed - acquired);
            if (self.permits.cmpxchgWeak(curr, curr - take, .acq_rel, .acquire)) |updated| {
                curr = updated;
                continue;
            }
            acquired += take;
            curr = self.permits.load(.acquire);
        }

        if (acquired >= needed) {
            // Got all permits under lock
            waiter.state.store(0, .release);
            self.mutex.unlock();
            return true;
        }

        // Partially fulfilled — update waiter state
        if (acquired > 0) {
            var rem = acquired;
            _ = waiter.assignPermits(&rem);
        }

        // Add to front of list (FIFO: serve from back)
        self.waiters.pushFront(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Like acquireWait() but skips the lock-free CAS fast path.
    /// Used by AcquireFuture.poll(.init) which already called tryAcquire()
    /// and failed — repeating the CAS is wasteful since it almost never
    /// succeeds nanoseconds after tryAcquire() just failed.
    ///
    /// Goes straight to mutex + CAS-under-lock + queue.
    fn acquireWaitDirect(self: *Self, waiter: *Waiter) bool {
        const needed = waiter.state.load(.acquire);

        self.mutex.lock();

        // Under lock: CAS to drain available permits.
        // tryAcquire() is lock-free, so permits can change concurrently.
        var acquired: usize = 0;
        var curr = self.permits.load(.acquire);
        while (acquired < needed and curr > 0) {
            const take = @min(curr, needed - acquired);
            if (self.permits.cmpxchgWeak(curr, curr - take, .acq_rel, .acquire)) |updated| {
                curr = updated;
                continue;
            }
            acquired += take;
            curr = self.permits.load(.acquire);
        }

        if (acquired >= needed) {
            waiter.state.store(0, .release);
            self.mutex.unlock();
            return true;
        }

        // Partially fulfilled — update waiter state
        if (acquired > 0) {
            var rem = acquired;
            _ = waiter.assignPermits(&rem);
        }

        // Try single-waiter fast slot (avoids mutex on wake path)
        if (self.fast_waiter.cmpxchgStrong(0, @intFromPtr(waiter), .release, .monotonic) == null) {
            self.mutex.unlock();
            return false;
        }

        // Fast slot occupied — fall back to linked list
        self.waiters.pushFront(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Release permits back to the semaphore.
    ///
    /// Fast path: try to serve the fast_waiter slot lock-free (no mutex).
    /// Slow path: takes mutex, serves linked-list waiters directly.
    pub fn release(self: *Self, num: usize) void {
        if (num == 0) return;

        // Fast path: try to serve fast_waiter without mutex.
        // In the common case (8 tasks, 2 permits), most releases find
        // exactly 1 waiter — serve it entirely through the atomic slot.
        const fast_ptr = self.fast_waiter.swap(0, .acq_rel);
        if (fast_ptr != 0) {
            const w: *Waiter = @ptrFromInt(fast_ptr);
            // CRITICAL: Copy waker info BEFORE modifying state to avoid
            // use-after-free (AcquireFuture may deinit after isComplete).
            const waker_fn = w.waker;
            const waker_ctx = w.waker_ctx;
            const remaining = w.state.load(.monotonic);

            if (num >= remaining) {
                // Fully satisfy the waiter
                w.state.store(0, .release);
                const surplus = num - remaining;
                if (surplus > 0) {
                    // Surplus permits: check for more waiters under mutex
                    self.releaseSurplus(surplus);
                }
                // Wake AFTER state is visible
                if (waker_fn) |wf| if (waker_ctx) |ctx| wf(ctx);
                return;
            }

            // Partial: assign what we can, put waiter back
            w.state.store(remaining - num, .release);
            // Try fast slot first (common: slot is still empty since we just swapped)
            if (self.fast_waiter.cmpxchgStrong(0, @intFromPtr(w), .release, .monotonic) == null) {
                return; // Back in fast slot, no wake needed (still waiting)
            }
            // Fast slot taken by new waiter — push to list under mutex
            self.mutex.lock();
            self.waiters.pushFront(w);
            self.mutex.unlock();
            return;
        }

        // Slow path: no fast_waiter, take mutex
        self.mutex.lock();

        // Check for list waiters under lock
        if (self.waiters.back() == null) {
            _ = self.permits.fetchAdd(num, .release);
            self.mutex.unlock();
            return;
        }

        // Serve queued waiters from the linked list
        self.releaseToWaiters(num);
    }

    /// Release surplus permits after serving the fast_waiter.
    /// Checks for more waiters under mutex, or adds to atomic.
    fn releaseSurplus(self: *Self, surplus: usize) void {
        self.mutex.lock();
        if (self.waiters.back() == null) {
            _ = self.permits.fetchAdd(surplus, .release);
            self.mutex.unlock();
            return;
        }
        self.releaseToWaiters(surplus);
    }

    /// Slow path for release(): serve permits to queued waiters.
    /// Called with mutex held. Unlocks mutex and wakes waiters on return.
    fn releaseToWaiters(self: *Self, num: usize) void {
        var wake_list: WakeList = .{};
        var rem = num;

        // Serve waiters from the back (oldest first = FIFO)
        while (rem > 0) {
            const waiter = self.waiters.back() orelse break;

            // Under lock — only we modify waiter.state
            const remaining = waiter.state.load(.monotonic);
            if (remaining == 0) {
                // Already satisfied — remove and collect waker
                _ = self.waiters.popBack();
                if (waiter.waker) |wf| {
                    if (waiter.waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
                continue;
            }

            const assign = @min(rem, remaining);
            // Plain store — we hold the mutex, no concurrent writers.
            // .release ensures polling task's .acquire load sees the update.
            waiter.state.store(remaining - assign, .release);
            rem -= assign;

            if (remaining - assign == 0) {
                // Waiter fully satisfied — remove from queue
                _ = self.waiters.popBack();
                if (waiter.waker) |wf| {
                    if (waiter.waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            } else {
                // Waiter partially satisfied — can't help further
                break;
            }
        }

        // Only surplus permits go to the atomic
        if (rem > 0) {
            _ = self.permits.fetchAdd(rem, .release);
        }

        self.mutex.unlock();

        // Wake AFTER releasing the lock
        wake_list.wakeAll();
    }

    /// Cancel an acquisition.
    /// Returns any partially acquired permits back to the semaphore.
    pub fn cancelAcquire(self: *Self, waiter: *Waiter) void {
        // Quick check if already complete
        if (waiter.isComplete()) {
            return;
        }

        // Try to cancel from fast slot (lock-free)
        if (self.fast_waiter.cmpxchgStrong(@intFromPtr(waiter), 0, .acq_rel, .monotonic) == null) {
            // Removed from fast slot — return partial permits
            const remaining = waiter.state.load(.monotonic);
            const acquired = waiter.permits_requested - remaining;
            if (acquired > 0) {
                self.release(acquired);
            }
            return;
        }

        self.mutex.lock();

        // Check again under lock
        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        // Remove from list if linked
        if (WaiterList.isLinked(waiter) or self.waiters.front() == waiter or self.waiters.back() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        // Calculate how many permits were partially acquired
        const remaining = waiter.state.load(.monotonic); // under lock
        const acquired = waiter.permits_requested - remaining;

        self.mutex.unlock();

        // Return partial permits (may wake other waiters)
        if (acquired > 0) {
            self.release(acquired);
        }
    }

    /// Get available permits (for debugging).
    pub fn availablePermits(self: *const Self) usize {
        return self.permits.load(.acquire);
    }

    /// Get number of waiters (for debugging/testing — O(n) list walk).
    /// Includes the fast_waiter slot if occupied.
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        if (self.fast_waiter.load(.acquire) != 0) count += 1;
        var it = self.waiters.iterator();
        while (it.next()) |_| count += 1;
        return count;
    }

    /// Walk the waiter queue and count waiters matching a predicate.
    /// Must be called under mutex or when no concurrent access.
    pub fn countWaitersMatching(self: *Self, comptime pred: fn (usize) bool) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        var it = self.waiters.iterator();
        while (it.next()) |w| {
            if (pred(w.permits_requested)) {
                count += 1;
            }
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Permit Convenience Methods
    // ═══════════════════════════════════════════════════════════════════════

    /// Try to acquire permits and return a permit guard if successful (non-blocking).
    pub fn tryAcquirePermit(self: *Self, num: usize) ?SemaphorePermit {
        if (self.tryAcquire(num)) {
            return SemaphorePermit.init(self, num);
        }
        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Async API
    // ═══════════════════════════════════════════════════════════════════════

    /// Acquire permits, blocking the current task until acquired.
    ///
    /// NOTE: Returns without acquiring if the runtime cannot spawn the
    /// internal task (e.g., out of memory or shutting down). Use
    /// `acquireFuture(n)` for explicit error handling in production code.
    pub fn acquire(self: *Self, io: @import("../Io.zig"), num: usize) void {
        var f = io.awaitFuture(self.acquireFuture(num)) catch return;
        _ = f.await(io);
    }

    /// Acquire permits. Returns an `AcquireFuture` that resolves when acquired.
    pub fn acquireFuture(self: *Self, num: usize) AcquireFuture {
        return AcquireFuture.init(self, num);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// SemaphorePermit - RAII guard
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that releases permits when dropped.
pub const SemaphorePermit = struct {
    semaphore: *Semaphore,
    permits: usize,

    const Self = @This();

    pub fn init(semaphore: *Semaphore, permits: usize) Self {
        return .{
            .semaphore = semaphore,
            .permits = permits,
        };
    }

    /// Release permits and invalidate this guard.
    pub fn release(self: *Self) void {
        if (self.permits > 0) {
            self.semaphore.release(self.permits);
            self.permits = 0;
        }
    }

    /// Forget this permit (don't release on drop).
    pub fn forget(self: *Self) void {
        self.permits = 0;
    }

    /// Deinit (releases permits).
    pub fn deinit(self: *Self) void {
        self.release();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// AcquireFuture - Async permit acquisition
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when semaphore permits are acquired.
pub const AcquireFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = void;

    /// Reference to the semaphore
    semaphore: *Semaphore,

    /// Number of permits to acquire
    num_permits: usize,

    /// Our waiter node (embedded to avoid allocation)
    waiter: Waiter,

    /// State machine for the future
    state: State,

    /// Stored waker for when we're woken by release()
    stored_waker: ?FutureWaker,

    const State = enum {
        /// Haven't tried to acquire yet
        init,
        /// Waiting for permits
        waiting,
        /// Permits acquired
        acquired,
    };

    /// Initialize a new acquire future
    pub fn init(semaphore: *Semaphore, num_permits: usize) Self {
        return .{
            .semaphore = semaphore,
            .num_permits = num_permits,
            .waiter = Waiter.init(num_permits),
            .state = .init,
            .stored_waker = null,
        };
    }

    /// Poll the future - implements Future trait
    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        switch (self.state) {
            .init => {
                // Cooperative budgeting: yield if this task has consumed its budget
                if (!ctx.pollProceed()) return .pending;

                // Fast path: try lock-free acquire before waker/waiter setup.
                // Skips 2 atomic ops (waker clone+deinit) when permits are available.
                if (self.semaphore.tryAcquire(self.num_permits)) {
                    self.state = .acquired;
                    return .{ .ready = {} };
                }

                // Slow path: no permits available, set up waker and wait
                self.stored_waker = ctx.getWaker().clone();
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                if (self.semaphore.acquireWaitDirect(&self.waiter)) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                } else {
                    self.state = .waiting;
                    return .pending;
                }
            },

            .waiting => {
                // Check if we've been granted the permits
                if (self.waiter.isComplete()) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                }

                // Not yet - update waker in case it changed (task migration)
                const new_waker = ctx.getWaker();
                if (self.stored_waker) |*old| {
                    if (!old.willWakeSame(new_waker)) {
                        old.deinit();
                        self.stored_waker = new_waker.clone();
                    }
                } else {
                    self.stored_waker = new_waker.clone();
                }

                return .pending;
            },

            .acquired => {
                return .{ .ready = {} };
            },
        }
    }

    /// Cancel the acquisition
    pub fn cancel(self: *Self) void {
        if (self.state == .waiting) {
            self.semaphore.cancelAcquire(&self.waiter);
        }
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    /// Deinit the future
    pub fn deinit(self: *Self) void {
        self.cancel();
    }

    /// Callback invoked by Semaphore.release() when permits are granted
    fn wakeCallback(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.stored_waker) |*w| {
            w.wakeByRef();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Semaphore - tryAcquire success" {
    var sem = Semaphore.init(3);

    try std.testing.expect(sem.tryAcquire(1));
    try std.testing.expectEqual(@as(usize, 2), sem.availablePermits());

    try std.testing.expect(sem.tryAcquire(2));
    try std.testing.expectEqual(@as(usize, 0), sem.availablePermits());
}

test "Semaphore - tryAcquire failure" {
    var sem = Semaphore.init(2);

    try std.testing.expect(!sem.tryAcquire(3));
    try std.testing.expectEqual(@as(usize, 2), sem.availablePermits());

    try std.testing.expect(sem.tryAcquire(2));
    try std.testing.expect(!sem.tryAcquire(1));
}

test "Semaphore - acquire and release with waiter" {
    var sem = Semaphore.init(1);
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // First acquire succeeds immediately
    var waiter1 = Waiter.init(1);
    try std.testing.expect(sem.acquireWait(&waiter1));
    try std.testing.expect(waiter1.isComplete());

    // Second acquire should wait
    var waiter2 = Waiter.init(1);
    waiter2.setWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!sem.acquireWait(&waiter2));
    try std.testing.expect(!waiter2.isComplete());
    try std.testing.expectEqual(@as(usize, 1), sem.waiterCount());

    // Release wakes waiter2
    sem.release(1);
    try std.testing.expect(waiter2.isComplete());
    try std.testing.expect(woken);
}

test "Semaphore - FIFO ordering" {
    var sem = Semaphore.init(0);

    // Add waiters in order
    var waiters: [3]Waiter = undefined;
    for (&waiters) |*w| {
        w.* = Waiter.init(1);
        _ = sem.acquireWait(w);
    }

    try std.testing.expectEqual(@as(usize, 3), sem.waiterCount());

    // Release one at a time - should wake in FIFO order
    sem.release(1);
    try std.testing.expect(waiters[0].isComplete());
    try std.testing.expect(!waiters[1].isComplete());
    try std.testing.expect(!waiters[2].isComplete());

    sem.release(1);
    try std.testing.expect(waiters[1].isComplete());
    try std.testing.expect(!waiters[2].isComplete());

    sem.release(1);
    try std.testing.expect(waiters[2].isComplete());
}

test "Semaphore - cancel returns permits" {
    var sem = Semaphore.init(2);

    // Acquire all permits
    try std.testing.expect(sem.tryAcquire(2));
    try std.testing.expectEqual(@as(usize, 0), sem.availablePermits());

    // Start waiting for more
    var waiter = Waiter.init(2);
    try std.testing.expect(!sem.acquireWait(&waiter));

    // Cancel - should return any partial acquisition
    sem.cancelAcquire(&waiter);
    try std.testing.expectEqual(@as(usize, 0), sem.waiterCount());
}

test "Semaphore - tryAcquirePermit" {
    var sem = Semaphore.init(2);

    if (sem.tryAcquirePermit(1)) |p| {
        var permit = p;
        defer permit.deinit();
        try std.testing.expectEqual(@as(usize, 1), sem.availablePermits());
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expectEqual(@as(usize, 2), sem.availablePermits());
}

test "Semaphore - permit forget" {
    var sem = Semaphore.init(2);

    {
        var permit = sem.tryAcquirePermit(1).?;
        try std.testing.expectEqual(@as(usize, 1), sem.availablePermits());

        // Forget the permit (don't release on deinit)
        permit.forget();
        permit.deinit();
    }

    // Permit was not released
    try std.testing.expectEqual(@as(usize, 1), sem.availablePermits());

    // Clean up
    sem.release(1);
}

test "Semaphore.Waiter - initWithWaker convenience" {
    var sem = Semaphore.init(0); // No permits
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Create waiter with initWithWaker (fewer steps than init + setWaker)
    var waiter = Waiter.initWithWaker(1, @ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!sem.acquireWait(&waiter));
    try std.testing.expect(!waiter.isReady()); // Using isReady (unified API)

    // Release permits
    sem.release(1);
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(woken);
}

test "Semaphore.Waiter - isReady is alias for isComplete" {
    var sem = Semaphore.init(1);
    var waiter = Waiter.init(1);

    // Both should return false initially
    try std.testing.expect(!waiter.isReady());
    try std.testing.expect(!waiter.isComplete());

    // Acquire should succeed immediately
    try std.testing.expect(sem.acquireWait(&waiter));

    // Both should return true
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(waiter.isComplete());

    sem.release(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// AcquireFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "AcquireFuture - immediate acquisition" {
    var sem = Semaphore.init(3);

    // Create future and poll - should acquire immediately
    var future = sem.acquireFuture(2);
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(usize, 1), sem.availablePermits());

    sem.release(2);
    try std.testing.expectEqual(@as(usize, 3), sem.availablePermits());
}

test "AcquireFuture - waits when not enough permits" {
    var sem = Semaphore.init(1);
    var waker_called = false;

    // Create a test waker that tracks calls
    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) FutureWaker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Acquire the only permit first
    try std.testing.expect(sem.tryAcquire(1));

    // Create future for 1 permit - first poll should return pending
    var future = sem.acquireFuture(1);
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);
    try std.testing.expectEqual(@as(usize, 1), sem.waiterCount());

    // Release - should wake the future
    sem.release(1);
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());

    sem.release(1);
}

test "AcquireFuture - cancel removes from queue" {
    var sem = Semaphore.init(0); // No permits initially

    // Create future and poll to add to queue
    var future = sem.acquireFuture(2);
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());
    try std.testing.expectEqual(@as(usize, 1), sem.waiterCount());

    // Cancel
    future.cancel();
    try std.testing.expectEqual(@as(usize, 0), sem.waiterCount());

    // Clean up
    future.deinit();
}

test "AcquireFuture - multiple waiters FIFO" {
    var sem = Semaphore.init(0); // No permits initially

    // Create multiple futures
    var future1 = sem.acquireFuture(1);
    var future2 = sem.acquireFuture(1);
    var future3 = sem.acquireFuture(1);

    var ctx = Context{ .waker = &future_mod.noop_waker };

    // All should return pending
    try std.testing.expect(future1.poll(&ctx).isPending());
    try std.testing.expect(future2.poll(&ctx).isPending());
    try std.testing.expect(future3.poll(&ctx).isPending());

    try std.testing.expectEqual(@as(usize, 3), sem.waiterCount());

    // Release one permit - future1 should be ready
    sem.release(1);
    try std.testing.expect(future1.poll(&ctx).isReady());
    try std.testing.expect(future2.poll(&ctx).isPending());
    try std.testing.expect(future3.poll(&ctx).isPending());

    // Release another - future2 should be ready
    sem.release(1);
    try std.testing.expect(future2.poll(&ctx).isReady());
    try std.testing.expect(future3.poll(&ctx).isPending());

    // Release another - future3 should be ready
    sem.release(1);
    try std.testing.expect(future3.poll(&ctx).isReady());

    future1.deinit();
    future2.deinit();
    future3.deinit();
}

test "AcquireFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(AcquireFuture));
    try std.testing.expect(AcquireFuture.Output == void);
}

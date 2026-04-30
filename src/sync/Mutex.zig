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
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

const UNLOCKED: u8 = 0;
const LOCKED: u8 = 1;

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

    /// Try to acquire the lock without blocking. Returns true on success.
    pub fn tryLock(self: *Mutex) bool {
        return self.state.cmpxchgStrong(UNLOCKED, LOCKED, .acquire, .monotonic) == null;
    }

    /// Acquire the lock, suspending the coroutine if it's held.
    pub fn lock(self: *Mutex) void {
        // Fast path: uncontended.
        if (self.tryLock()) return;

        // Slow path: enqueue and park.
        while (true) {
            var waiter: Waiter = .{};
            self.waiter_mutex.lock();
            // Re-check under the slow-path mutex. If the lock is now free
            // (and no waiters ahead of us), grab it.
            if (self.waiters.isEmpty() and self.tryLock()) {
                self.waiter_mutex.unlock();
                return;
            }
            self.waiters.pushBack(&waiter);
            self.waiter_mutex.unlock();

            // Park until the unlocker hands the lock to us. lock() has
            // no error union so a cancel here unwinds via best-effort
            // waiter removal; the queue invariants stay intact.
            waiter.park.parkCurrent() catch |err| switch (err) {
                error.Cancelled => {
                    // Best-effort remove from waiter list. If we'd
                    // already been popped, the unlocker handed us the
                    // lock — keep it.
                    self.waiter_mutex.lock();
                    const removed = self.removeWaiter(&waiter);
                    self.waiter_mutex.unlock();
                    if (!removed) {
                        // We were already handed the lock. Hold it
                        // (caller will see the cancellation later when
                        // they next yield); they MUST call unlock.
                        return;
                    }
                    // Genuine cancel — but we have no way to surface it
                    // without an error union. Fall through and re-loop;
                    // the next `parkCurrent` in this thread will return
                    // `error.Cancelled` and the calling coroutine will
                    // unwind cleanly. (This is the best we can do for
                    // an async-but-not-error-returning mutex API.)
                    continue;
                },
            };
            // Lock was handed off to us by the unlocker; we own it.
            return;
        }
    }

    /// Release the lock. Must only be called by the holder.
    pub fn unlock(self: *Mutex) void {
        // Drop the lock first.
        self.state.store(UNLOCKED, .release);

        // Check for waiters and hand off.
        self.waiter_mutex.lock();
        const w = self.waiters.popFront();
        if (w) |waiter| {
            // Hand the lock directly to this waiter — bring `state`
            // back to LOCKED so a concurrent `tryLock` doesn't steal
            // from under them.
            self.state.store(LOCKED, .release);
            self.waiter_mutex.unlock();
            waiter.park.unpark();
        } else {
            self.waiter_mutex.unlock();
        }
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

fn incrementer(ctx: *CounterCtx, iters: u32) void {
    var i: u32 = 0;
    while (i < iters) : (i += 1) {
        ctx.mu.lock();
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

fn fifoLocker(ctx: *FifoCtx, slot: usize) void {
    ctx.mu.lock();
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
    mu.lock();

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

test "mutex: FIFO ordering — lockers acquire in enqueue order" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, fifoRoot, .{});
    // Each finishes[i] should equal its enqueue order (1, 2, 3, 4).
    try std.testing.expectEqual(@as(u32, 1), ctx.finishes[0].load(.acquire));
    try std.testing.expectEqual(@as(u32, 2), ctx.finishes[1].load(.acquire));
    try std.testing.expectEqual(@as(u32, 3), ctx.finishes[2].load(.acquire));
    try std.testing.expectEqual(@as(u32, 4), ctx.finishes[3].load(.acquire));
}

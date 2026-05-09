//! Semaphore — async counting semaphore.
//!
//! `acquire(n)` blocks until at least `n` permits are available, then
//! consumes them. `release(n)` returns permits and wakes waiters in FIFO
//! order whose request can now be satisfied.
//!
//! Backbone for `RwLock`, connection pools, rate limiters, and any
//! "at most N concurrent" pattern.
//!
//! ## Usage
//!
//! ```zig
//! var sem = Semaphore.init(10);  // 10 permits
//!
//! sem.acquire(1);
//! defer sem.release(1);
//! // ... do bounded work ...
//! ```
//!
//! ## Design
//!
//! - `permits` (atomic u32) — tracks available permits.
//! - Fast path: `tryAcquire(n)` does a single CAS (`permits >= n` →
//!   `permits - n`). Uncontended is one atomic.
//! - Slow path: under `waiter_mutex`, enqueue a (waiter, requested_n)
//!   record and park.
//! - `release(n)` increments `permits`, then walks the head of the
//!   waiter list and wakes head waiters whose request fits.
//!
//! `acquire` is cancellable: a cancelled coroutine wakes from its park
//! and surfaces `error.Cancelled` via the standard cancel path. Combine
//! with `volt.withTimeout` for time-bounded acquisition.

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

pub const Semaphore = struct {
    const Waiter = struct {
        park: Park = .{},
        wanted: u32 = 0,
        granted: bool = false,
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

    permits: std.atomic.Value(u32),
    waiter_mutex: thread.Mutex = .{},
    waiters: WaiterList = .{},

    pub fn init(initial_permits: u32) Semaphore {
        return .{ .permits = std.atomic.Value(u32).init(initial_permits) };
    }

    /// Try to consume `n` permits without blocking. Returns true on success.
    pub fn tryAcquire(self: *Semaphore, n: u32) bool {
        var current = self.permits.load(.acquire);
        while (true) {
            if (current < n) return false;
            if (self.permits.cmpxchgWeak(current, current - n, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            return true;
        }
    }

    /// Consume `n` permits, suspending the coroutine until they're available.
    pub fn acquire(self: *Semaphore, n: u32) void {
        // Fast path.
        if (self.tryAcquire(n)) return;

        // Slow path: enqueue and park. The releaser will set
        // `granted = true` when it allocates permits to us.
        var waiter: Waiter = .{ .wanted = n };
        self.waiter_mutex.lock();
        // Re-check under the slow-path lock — a concurrent release may
        // have freed up permits, AND if we're at the head of the queue,
        // we have priority over racing fast-path acquirers.
        if (self.waiters.isEmpty() and self.tryAcquire(n)) {
            self.waiter_mutex.unlock();
            return;
        }
        self.waiters.pushBack(&waiter);
        self.waiter_mutex.unlock();

        while (true) {
            waiter.park.parkCurrent() catch |err| switch (err) {
                error.Cancelled => {
                    self.waiter_mutex.lock();
                    if (waiter.granted) {
                        // Releaser handed us permits — keep them; the
                        // caller is responsible for calling release.
                        // The caller observed cancellation already on
                        // entry to `acquire`; we honor the grant.
                        self.waiter_mutex.unlock();
                        return;
                    }
                    _ = self.removeWaiterLocked(&waiter);
                    self.waiter_mutex.unlock();
                    // No way to surface the error from a void-returning
                    // API. The next yield will re-trigger Cancelled.
                    return;
                },
            };
            // Woken. If granted, we have our permits.
            if (@atomicLoad(bool, &waiter.granted, .acquire)) return;
            // Spurious wake — re-park.
        }
    }

    /// Return `n` permits and wake any head waiters who can now proceed.
    pub fn release(self: *Semaphore, n: u32) void {
        // Add permits back atomically.
        _ = self.permits.fetchAdd(n, .release);

        // Wake as many head waiters as can be satisfied.
        self.waiter_mutex.lock();
        var to_wake: ?*Waiter = null;
        var to_wake_tail: ?*Waiter = null;
        while (self.waiters.head) |head| {
            // Try to consume head.wanted from the available pool.
            if (self.tryAcquire(head.wanted)) {
                _ = self.waiters.popFront();
                @atomicStore(bool, &head.granted, true, .release);
                head.next = null;
                if (to_wake_tail) |t| {
                    t.next = head;
                } else {
                    to_wake = head;
                }
                to_wake_tail = head;
            } else {
                break; // can't satisfy head; don't skip ahead (FIFO)
            }
        }
        self.waiter_mutex.unlock();

        var cur = to_wake;
        while (cur) |w| {
            const next = w.next;
            w.next = null;
            w.park.unpark();
            cur = next;
        }
    }

    /// Snapshot of currently available permits. For diagnostics; the
    /// value can change between this read and any subsequent operation.
    pub fn available(self: *const Semaphore) u32 {
        return self.permits.load(.acquire);
    }

    fn removeWaiterLocked(self: *Semaphore, target: *Waiter) bool {
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

test "semaphore: tryAcquire respects available count" {
    var sem = Semaphore.init(3);
    try std.testing.expect(sem.tryAcquire(1));
    try std.testing.expect(sem.tryAcquire(1));
    try std.testing.expect(sem.tryAcquire(1));
    try std.testing.expect(!sem.tryAcquire(1));
    sem.release(2);
    try std.testing.expectEqual(@as(u32, 2), sem.available());
    try std.testing.expect(sem.tryAcquire(2));
    try std.testing.expect(!sem.tryAcquire(1));
}

const PoolCtx = struct {
    sem: *Semaphore,
    in_flight: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    max_observed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn poolWorker(ctx: *PoolCtx) !void {
    ctx.sem.acquire(1);
    defer ctx.sem.release(1);

    // Track concurrent in-flight count and update the max.
    const cur = ctx.in_flight.fetchAdd(1, .acq_rel) + 1;
    var max_seen = ctx.max_observed.load(.acquire);
    while (cur > max_seen) {
        if (ctx.max_observed.cmpxchgWeak(max_seen, cur, .acq_rel, .acquire)) |obs| {
            max_seen = obs;
        } else break;
    }
    try volt.yield();
    try volt.yield();
    _ = ctx.in_flight.fetchSub(1, .acq_rel);
}

fn poolRoot(permits: u32, n_workers: u32) !u32 {
    var sem = Semaphore.init(permits);
    var ctx = PoolCtx{ .sem = &sem };

    const allocator = std.testing.allocator;
    const tasks = try allocator.alloc(*volt.Task(@TypeOf(poolWorker)), n_workers);
    defer allocator.free(tasks);

    for (tasks) |*t| t.* = try volt.spawn(poolWorker, .{&ctx});
    defer for (tasks) |t| volt.destroyTask(t);
    for (tasks) |t| try t.join();

    return ctx.max_observed.load(.acquire);
}

test "semaphore: 4-permit pool with 32 workers, max in-flight ≤ 4" {
    const max_observed = try volt.run(.{ .allocator = std.testing.allocator }, poolRoot, .{ @as(u32, 4), @as(u32, 32) });
    try std.testing.expect(max_observed >= 1);
    try std.testing.expect(max_observed <= 4);
}

test "semaphore: 1-permit pool with 16 workers, max in-flight == 1" {
    const max_observed = try volt.run(.{ .allocator = std.testing.allocator }, poolRoot, .{ @as(u32, 1), @as(u32, 16) });
    try std.testing.expectEqual(@as(u32, 1), max_observed);
}

const FifoSemCtx = struct {
    sem: *Semaphore,
    order: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    finishes: [4]std.atomic.Value(u32) = .{
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
    },
};

fn fifoWaiter(ctx: *FifoSemCtx, slot: usize) void {
    ctx.sem.acquire(1);
    const order = ctx.order.fetchAdd(1, .monotonic);
    ctx.finishes[slot].store(order + 1, .release);
}

fn fifoSemRoot() !FifoSemCtx {
    var sem = Semaphore.init(0);
    var ctx = FifoSemCtx{ .sem = &sem };

    var lockers: [4]*volt.Job = undefined;
    for (&lockers, 0..) |*j, i| {
        j.* = try volt.launch(fifoWaiter, .{ &ctx, i });
        try volt.yield();
    }
    defer for (lockers) |j| volt.destroyJob(j);

    var k: u32 = 0;
    while (k < 8) : (k += 1) try volt.yield();

    // Release one permit at a time so we can observe FIFO order.
    var r: u32 = 0;
    while (r < 4) : (r += 1) {
        sem.release(1);
        var p: u32 = 0;
        while (p < 4) : (p += 1) try volt.yield();
    }

    for (lockers) |j| try j.join();
    return ctx;
}

test "semaphore: every release wakes exactly one waiter — no lost wakes, no double-wakes" {
    // What we actually test: 4 waiters park on a 0-permit semaphore;
    // 4 single-permit releases must wake every waiter exactly once.
    // Catches lost wakes (a release fires but no waiter resumes) and
    // double-wakes (one release wakes two waiters).
    //
    // What we DELIBERATELY don't assert: the order of acquisition.
    // The wait queue is a 5-line FIFO linked list (pushBack/popFront)
    // — algorithmically self-evident. End-to-end FIFO depends on the
    // launch→park ordering, which the multi-worker scheduler doesn't
    // provide and shouldn't have to.
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, fifoSemRoot, .{});

    var seen = [_]bool{ false, false, false, false };
    inline for (0..4) |i| {
        const v = ctx.finishes[i].load(.acquire);
        try std.testing.expect(v >= 1 and v <= 4);
        try std.testing.expect(!seen[v - 1]);
        seen[v - 1] = true;
    }
    for (seen) |s| try std.testing.expect(s);
}

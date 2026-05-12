//! v2 synchronization primitives — Mutex, Notify, Semaphore.
//!
//! All single-worker (no atomics on internal state; the runtime's
//! cooperative scheduling provides serialization). Multi-worker
//! (Phase 3.3) will redesign these around shared atomics + parker
//! bitmap, similar to v0.x's MCS Mutex.
//!
//! Park/unpark via the runtime's existing primitives. Wait queues
//! are FIFO linked lists threaded through `Coroutine.wait_next`.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");

// ─────────────────────────────────────────────────────────────────────
// Wait queue helper — FIFO via Coroutine.wait_next
// ─────────────────────────────────────────────────────────────────────
const WaitQueue = struct {
    head: ?*coroutine.Coroutine = null,
    tail: ?*coroutine.Coroutine = null,

    fn push(self: *WaitQueue, c: *coroutine.Coroutine) void {
        c.wait_next = null;
        if (self.tail) |t| {
            t.wait_next = c;
            self.tail = c;
        } else {
            self.head = c;
            self.tail = c;
        }
    }

    fn pop(self: *WaitQueue) ?*coroutine.Coroutine {
        const h = self.head orelse return null;
        self.head = h.wait_next;
        if (self.head == null) self.tail = null;
        h.wait_next = null;
        return h;
    }

    fn isEmpty(self: *const WaitQueue) bool {
        return self.head == null;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Mutex
// ─────────────────────────────────────────────────────────────────────
pub const Mutex = struct {
    locked: bool = false,
    waiters: WaitQueue = .{},

    pub fn lock(self: *Mutex) void {
        if (!self.locked) {
            self.locked = true;
            return;
        }
        const me = current.require();
        self.waiters.push(me);
        runtime.park();
        // When unparked, we ARE the new owner — `unlock` did the
        // direct lock handoff to keep it fair (no re-CAS).
    }

    /// Non-blocking. Returns true if lock acquired.
    pub fn tryLock(self: *Mutex) bool {
        if (self.locked) return false;
        self.locked = true;
        return true;
    }

    pub fn unlock(self: *Mutex) void {
        std.debug.assert(self.locked);
        if (self.waiters.pop()) |next| {
            // Direct handoff — lock stays held by the new owner.
            runtime.unpark(next);
            return;
        }
        self.locked = false;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Notify — single permit + waiter queue, async-await-style
// ─────────────────────────────────────────────────────────────────────
pub const Notify = struct {
    notified: bool = false,
    waiters: WaitQueue = .{},

    /// Block until a notification is delivered. If a permit was
    /// already stored, consume it and return immediately.
    pub fn wait(self: *Notify) void {
        if (self.notified) {
            self.notified = false;
            return;
        }
        const me = current.require();
        self.waiters.push(me);
        runtime.park();
    }

    /// Wake one waiter. If no waiters, store the permit (next wait
    /// returns immediately).
    pub fn notifyOne(self: *Notify) void {
        if (self.waiters.pop()) |w| {
            runtime.unpark(w);
            return;
        }
        self.notified = true;
    }

    /// Wake every current waiter. Does NOT store a permit — future
    /// waits will block until the next notifyOne / notifyAll.
    pub fn notifyAll(self: *Notify) void {
        while (self.waiters.pop()) |w| runtime.unpark(w);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Semaphore — counting permits
// ─────────────────────────────────────────────────────────────────────
pub const Semaphore = struct {
    permits: u32,
    waiters: WaitQueue = .{},

    pub fn init(initial_permits: u32) Semaphore {
        return .{ .permits = initial_permits };
    }

    pub fn acquire(self: *Semaphore) void {
        if (self.permits > 0) {
            self.permits -= 1;
            return;
        }
        const me = current.require();
        self.waiters.push(me);
        runtime.park();
        // On unpark, the releaser handed us the permit directly.
    }

    pub fn tryAcquire(self: *Semaphore) bool {
        if (self.permits == 0) return false;
        self.permits -= 1;
        return true;
    }

    pub fn release(self: *Semaphore) void {
        if (self.waiters.pop()) |w| {
            // Direct handoff — don't increment permits.
            runtime.unpark(w);
            return;
        }
        self.permits += 1;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;
const Runtime = runtime.Runtime;

const MutexCtx = struct {
    mu: *Mutex,
    counter: *u32,
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

test "Mutex: serializes 8 coros each doing 1k increments" {
    var rt = try Runtime.init(test_allocator);
    defer rt.deinit();

    var mu = Mutex{};
    var counter: u32 = 0;
    var ctx = MutexCtx{ .mu = &mu, .counter = &counter, .iters = 1000 };

    var tasks: [8]*@import("task.zig").Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(mutexInc, .{&ctx});
    rt.run();
    for (&tasks) |t| t.join();

    try std.testing.expectEqual(@as(u32, 8 * 1000), counter);
}

const NotifyCtx = struct {
    note: *Notify,
    fired: *bool,
};

fn notifyWaiter(ctx: *NotifyCtx) void {
    ctx.note.wait();
    ctx.fired.* = true;
}

fn notifySender(ctx: *NotifyCtx) void {
    ctx.note.notifyOne();
}

test "Notify: wait + notifyOne handshake" {
    var rt = try Runtime.init(test_allocator);
    defer rt.deinit();

    var note = Notify{};
    var fired = false;
    var ctx = NotifyCtx{ .note = &note, .fired = &fired };

    var waiter = try rt.spawn(notifyWaiter, .{&ctx});
    var sender = try rt.spawn(notifySender, .{&ctx});
    rt.run();
    waiter.join();
    sender.join();

    try std.testing.expect(fired);
}

const SemCtx = struct {
    sem: *Semaphore,
    counter: *u32,
};

fn semWorker(ctx: *SemCtx) void {
    ctx.sem.acquire();
    ctx.counter.* += 1;
    ctx.sem.release();
}

test "Semaphore: 100 coros acquire/release with cap=3" {
    var rt = try Runtime.init(test_allocator);
    defer rt.deinit();

    var sem = Semaphore.init(3);
    var counter: u32 = 0;
    var ctx = SemCtx{ .sem = &sem, .counter = &counter };

    var tasks: [100]*@import("task.zig").Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(semWorker, .{&ctx});
    rt.run();
    for (&tasks) |t| t.join();

    try std.testing.expectEqual(@as(u32, 100), counter);
    try std.testing.expectEqual(@as(u32, 3), sem.permits);
}

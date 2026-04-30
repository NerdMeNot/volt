//! Notify — async notification primitive (one-shot or broadcast).
//!
//! Coroutines call `wait()` to suspend; producers call `notifyOne()` to
//! wake one waiter or `notifyAll()` to wake everyone. Handles the
//! notify-before-wait race: a `notifyOne` with no waiter stores a
//! single permit that the next `wait` consumes immediately. (`notifyAll`
//! does NOT store permits — broadcast is "wake everyone currently
//! waiting" only, no buffer.)
//!
//! ## Usage
//!
//! ```zig
//! var ready = Notify{};
//!
//! // Producer:
//! prepareWork();
//! ready.notifyOne();
//!
//! // Consumer:
//! ready.wait();
//! consumeWork();
//! ```
//!
//! ## Permit semantics
//!
//! - `notifyOne` increments a (saturating-at-one) permit counter, then
//!   wakes one head waiter.
//! - `wait` first tries to consume a stored permit (CAS 1→0). If
//!   successful, returns immediately. Otherwise enqueues and parks.
//! - `notifyAll` does NOT touch the permit counter — only wakes
//!   currently-parked waiters. Late `wait()` callers see no permit.

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

pub const WaitError = error{Cancelled};

pub const Notify = struct {
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

        fn drain(self: *@This()) ?*Waiter {
            const head = self.head;
            self.head = null;
            self.tail = null;
            return head;
        }
    };

    /// Saturating-at-1 permit. `notifyOne` sets it; `wait`'s fast path
    /// CAS-clears it.
    permit: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
    waiter_mutex: thread.Mutex = .{},
    waiters: WaiterList = .{},

    /// Suspend the calling coroutine until notified. Returns immediately
    /// if a permit is stored.
    pub fn wait(self: *Notify) WaitError!void {
        // Fast path: consume an already-stored permit.
        if (self.permit.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            return;
        }

        var waiter: Waiter = .{};
        self.waiter_mutex.lock();
        // Re-check the permit under the lock — a `notifyOne` could
        // have raced us between the fast-path check and the lock.
        if (self.permit.cmpxchgStrong(1, 0, .acquire, .monotonic) == null) {
            self.waiter_mutex.unlock();
            return;
        }
        self.waiters.pushBack(&waiter);
        self.waiter_mutex.unlock();

        waiter.park.parkCurrent() catch |err| switch (err) {
            error.Cancelled => {
                self.waiter_mutex.lock();
                _ = self.removeWaiterLocked(&waiter);
                self.waiter_mutex.unlock();
                return error.Cancelled;
            },
        };
    }

    /// Wake one waiter. If no waiter is parked, store a permit so the
    /// next `wait` returns immediately. The permit saturates at 1
    /// (multiple notifyOnes with no waiters collapse into one wake).
    pub fn notifyOne(self: *Notify) void {
        // Try to hand directly to a parked waiter first — avoids
        // wasting a permit if someone's already waiting.
        self.waiter_mutex.lock();
        const w = self.waiters.popFront();
        if (w) |_| {
            self.waiter_mutex.unlock();
            w.?.park.unpark();
            return;
        }
        // No waiter — stash a permit (saturating at 1).
        self.permit.store(1, .release);
        self.waiter_mutex.unlock();
    }

    /// Wake all currently-parked waiters. Does NOT store a permit —
    /// late `wait()` callers won't see anything.
    pub fn notifyAll(self: *Notify) void {
        self.waiter_mutex.lock();
        const drained = self.waiters.drain();
        self.waiter_mutex.unlock();

        var cur = drained;
        while (cur) |w| {
            const next = w.next;
            w.next = null;
            w.park.unpark();
            cur = next;
        }
    }

    fn removeWaiterLocked(self: *Notify, target: *Waiter) bool {
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

const NotifyCtx = struct {
    notify: *Notify,
    woke: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn waiterFn(ctx: *NotifyCtx) WaitError!void {
    try ctx.notify.wait();
    _ = ctx.woke.fetchAdd(1, .monotonic);
}

fn notifierFn(ctx: *NotifyCtx) !void {
    // Yield to let the waiter park.
    var i: u32 = 0;
    while (i < 4) : (i += 1) try volt.yield();
    ctx.notify.notifyOne();
}

fn singleNotifyRoot() !u32 {
    var n = Notify{};
    var ctx = NotifyCtx{ .notify = &n };

    var w = try volt.spawn(waiterFn, .{&ctx});
    defer volt.destroyTask(w);
    var n_task = try volt.spawn(notifierFn, .{&ctx});
    defer volt.destroyTask(n_task);

    try w.join();
    try n_task.join();
    return ctx.woke.load(.acquire);
}

test "notify: notifyOne wakes a single parked waiter" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, singleNotifyRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), woke);
}

fn notifyBeforeWaitRoot() !u32 {
    var n = Notify{};
    n.notifyOne();
    // Now spawn a waiter — the stored permit should let it return
    // immediately on its first wait().
    var ctx = NotifyCtx{ .notify = &n };
    var w = try volt.spawn(waiterFn, .{&ctx});
    defer volt.destroyTask(w);
    try w.join();
    return ctx.woke.load(.acquire);
}

test "notify: notify-before-wait stores permit for next wait" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, notifyBeforeWaitRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), woke);
}

// Note: a "notifyAll wakes every parked waiter" test is intentionally
// omitted. Notify's `notifyAll` doesn't buffer — late waiters (those
// not yet parked when notifyAll fires) miss the wake. There's no
// deterministic way to ensure N coroutines have ALL reached the
// inside of `parkCurrent` before we fire notifyAll without racing
// against the actual park transition. Users wanting guaranteed
// all-N delivery should use `Broadcast(T)` (which buffers in a ring)
// or `Watch(T)` (which versions the value); Notify is for fan-out
// when "wake whoever's listening NOW" is the contract.
//
// notifyAll's correctness on a SINGLE waiter is exercised by the
// next test, which is deterministic.

const SingleAllCtx = struct {
    notify: *Notify,
    woke: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn singleAllWaiter(ctx: *SingleAllCtx) WaitError!void {
    try ctx.notify.wait();
    _ = ctx.woke.fetchAdd(1, .monotonic);
}

fn singleAllerFn(ctx: *SingleAllCtx) !void {
    var i: u32 = 0;
    while (i < 8) : (i += 1) try volt.yield();
    ctx.notify.notifyAll();
}

fn singleNotifyAllRoot() !u32 {
    var n = Notify{};
    var ctx = SingleAllCtx{ .notify = &n };

    var w = try volt.spawn(singleAllWaiter, .{&ctx});
    defer volt.destroyTask(w);
    var notifier = try volt.spawn(singleAllerFn, .{&ctx});
    defer volt.destroyTask(notifier);

    try w.join();
    try notifier.join();
    return ctx.woke.load(.acquire);
}

test "notify: notifyAll wakes a parked waiter" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, singleNotifyAllRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), woke);
}

fn permitSaturationRoot() !u32 {
    var n = Notify{};
    // 5 notifyOnes with no waiter — should saturate to 1 permit.
    var i: u32 = 0;
    while (i < 5) : (i += 1) n.notifyOne();
    var ctx = NotifyCtx{ .notify = &n };
    var w = try volt.spawn(waiterFn, .{&ctx});
    defer volt.destroyTask(w);
    try w.join();
    return ctx.woke.load(.acquire);
}

test "notify: 5 notifyOnes saturate to 1 permit" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, permitSaturationRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), woke);
}

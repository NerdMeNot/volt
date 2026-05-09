//! CancellationToken — explicit cancellation handle.
//!
//! Decouples "request to cancel" from "the coroutine being cancelled."
//! Useful when:
//! - You want to cancel a *group* of unrelated tasks together
//!   (build a CancellationToken, hand it to each task, call `cancel()`).
//! - You want hierarchy — a child token cancels when its parent cancels
//!   (`linkParent`). Cascade is synchronous and deterministic.
//! - You want to react to cancellation via the embedded `cancelled`
//!   Notify (e.g. `try token.cancelled.wait()`).
//!
//! ## Usage
//!
//! ```zig
//! var token = CancellationToken.init();
//! defer token.deinit();
//!
//! var t1 = try volt.spawn(workerWithToken, .{ &token, ctx });
//! var t2 = try volt.spawn(workerWithToken, .{ &token, ctx });
//!
//! // Later, from anywhere:
//! token.cancel();   // cascades synchronously to any linkParent'd children
//! ```
//!
//! ## Design
//!
//! Cancellation cascade is implemented via an intrusive singly-linked
//! list of "linked children" rooted on each parent token. `linkParent`
//! pushes the child onto the parent's list (under the parent's mutex),
//! then re-checks the parent's flag in case the parent cancelled
//! between the pre-check and the push. `cancel` flips the flag, drains
//! the linked-children list, and recursively cancels each.
//!
//! No coroutine is spawned for the link — the cascade is synchronous,
//! so there's no parent-cancel-before-child-parks race.

const std = @import("std");
const Notify = @import("Notify.zig").Notify;
const thread = @import("../internal/thread.zig");

pub const CancellationToken = struct {
    flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Wakes when the token transitions to cancelled. Use
    /// `token.cancelled.wait()` from a coroutine to suspend until then.
    cancelled: Notify = .{},

    /// Intrusive list head of linked children to cascade-cancel.
    /// Protected by `link_mutex`. `next` is the per-node link.
    link_mutex: thread.Mutex = .{},
    linked_children_head: ?*CancellationToken = null,
    /// Used by `linkParent` to chain into the parent's children list.
    /// Only meaningful while we're a member of some parent's list.
    next_linked: ?*CancellationToken = null,

    pub fn init() CancellationToken {
        return .{};
    }

    pub fn deinit(_: *CancellationToken) void {
        // Notify drains via notifyAll on cancel; nothing else to free.
    }

    pub fn isCancelled(self: *const CancellationToken) bool {
        return self.flag.load(.acquire);
    }

    /// Cancel the token. Idempotent — additional `cancel` calls are
    /// no-ops. Wakes everyone parked on `cancelled.wait()` AND closes
    /// the Notify so any late-arrival `cancelled.wait()` returns
    /// immediately. Cascades to every child that called
    /// `linkParent(self)`.
    pub fn cancel(self: *CancellationToken) void {
        if (self.flag.cmpxchgStrong(false, true, .acq_rel, .monotonic) != null) {
            return; // already cancelled
        }
        // notifyAllAndClose, not bare notifyAll: cancellation is
        // one-shot and any future `cancelled.wait()` must observe it
        // even if called AFTER cancel fires (the broadcast-race that
        // bare notifyAll leaves open).
        self.cancelled.notifyAllAndClose();

        // Drain the linked-children list and cascade. Recursive but
        // bounded by the depth of the link chain (typically 1–2).
        self.link_mutex.lock();
        var head = self.linked_children_head;
        self.linked_children_head = null;
        self.link_mutex.unlock();

        while (head) |child| {
            const next = child.next_linked;
            child.next_linked = null;
            child.cancel();
            head = next;
        }
    }

    /// Link `self` to `parent`: when `parent` cancels (or is already
    /// cancelled at the time of this call), `self` cancels too.
    /// Synchronous cascade — no coroutine spawned, no wake-race.
    pub fn linkParent(self: *CancellationToken, parent: *CancellationToken) void {
        // Fast path: parent already cancelled.
        if (parent.isCancelled()) {
            self.cancel();
            return;
        }
        // Push onto parent's children list.
        parent.link_mutex.lock();
        // Re-check under the lock — `parent.cancel` takes the same
        // mutex when draining, so if it had started draining we'd
        // either see a cancelled flag here OR we get added to the
        // (now-empty) list AFTER cancel finished. Handle both.
        if (parent.isCancelled()) {
            parent.link_mutex.unlock();
            self.cancel();
            return;
        }
        self.next_linked = parent.linked_children_head;
        parent.linked_children_head = self;
        parent.link_mutex.unlock();
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "token: init clear, cancel sets flag, idempotent" {
    var t = CancellationToken.init();
    defer t.deinit();
    try std.testing.expect(!t.isCancelled());
    t.cancel();
    try std.testing.expect(t.isCancelled());
    t.cancel(); // no-op
    try std.testing.expect(t.isCancelled());
}

const WaitCtx = struct {
    token: *CancellationToken,
    woke_at: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn cancelWaiter(ctx: *WaitCtx) !void {
    try ctx.token.cancelled.wait();
    _ = ctx.woke_at.fetchAdd(1, .monotonic);
}

fn cancelRoot() !u32 {
    var t = CancellationToken.init();
    defer t.deinit();
    var ctx = WaitCtx{ .token = &t };

    var w = try volt.spawn(cancelWaiter, .{&ctx});
    defer volt.destroyTask(w);

    // Yield so the waiter parks on `cancelled`.
    var i: u32 = 0;
    while (i < 8) : (i += 1) try volt.yield();

    t.cancel();
    try w.join();
    return ctx.woke_at.load(.acquire);
}

test "token: cancel wakes waiters parked on cancelled.wait()" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, cancelRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), woke);
}

test "token: linkParent — parent cancel cascades to child synchronously" {
    var p = CancellationToken.init();
    defer p.deinit();
    var c = CancellationToken.init();
    defer c.deinit();

    c.linkParent(&p);
    try std.testing.expect(!c.isCancelled());

    p.cancel();
    try std.testing.expect(c.isCancelled());
}

test "token: linkParent on already-cancelled parent cancels child immediately" {
    var p = CancellationToken.init();
    defer p.deinit();
    var c = CancellationToken.init();
    defer c.deinit();

    p.cancel();
    c.linkParent(&p);
    try std.testing.expect(c.isCancelled());
}

test "token: deep cascade (grandparent → parent → child)" {
    var gp = CancellationToken.init();
    defer gp.deinit();
    var p = CancellationToken.init();
    defer p.deinit();
    var c = CancellationToken.init();
    defer c.deinit();

    p.linkParent(&gp);
    c.linkParent(&p);

    gp.cancel();
    try std.testing.expect(p.isCancelled());
    try std.testing.expect(c.isCancelled());
}

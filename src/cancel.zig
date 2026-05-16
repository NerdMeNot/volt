//! Go-style cancellation. A `Cancel` is an opaque handle backed by
//! an atomic flag + a list of waiters that want to be woken when
//! the flag fires. Caller-owned lifetime; passed explicitly into
//! every fn that can park.
//!
//! ```zig
//! var c = volt.Cancel.init(rt);
//! defer c.deinit();
//! _ = try rt.spawn(work, .{&c, ...});
//! // From any coro / thread that can reach `c`:
//! c.fire();
//! ```
//!
//! Cancel-aware blocking ops (`Mutex.lockCancel`, `Spsc.recvCancel`,
//! etc.):
//!
//!   1. Check `c.isFired()` cheaply.
//!   2. Register a `Waiter` on the cancel's list, recording the
//!      parking-lot key the op is about to park on.
//!   3. Park on the primitive's key. The primitive's normal wake
//!      path wakes the coro on success.
//!   4. On wake: deregister, then re-check `c.isFired()`. If fired,
//!      return `error.Cancelled`; otherwise the op succeeded.
//!
//! `fire()` flips the flag and walks the waiter list, calling
//! `parking_lot.unparkAll` on each recorded park-addr. The "All"
//! flavour is deliberate — primitives may have multiple coros parked
//! on the same key (only some of which are cancel-aware); waking
//! everyone and letting each validator re-evaluate is simpler and
//! still correct. Spurious wakes loop back to park.
//!
//! ## Memory model
//!
//! Waiters live on the calling coro's stack. The list is protected
//! by a one-byte spinlock — `register` / `deregister` / fire's
//! drain all take the lock briefly. Spinlock contention is bounded
//! by the number of cancel-aware ops pending on this Cancel, which
//! is small in practice.
//!
//! `fire()` snapshots the list under the lock, releases, then
//! unparks each entry. That keeps `unparkAll` (which can be a
//! visible amount of work) outside the spinlock.

const std = @import("std");
const park = @import("park.zig");
const runtime = @import("runtime.zig");

pub const Error = error{Cancelled};

pub const Waiter = struct {
    park_addr: usize = 0,
    next: ?*Waiter = null,
    prev: ?*Waiter = null,
    in_list: bool = false,
};

pub const Cancel = struct {
    rt: *runtime.Runtime,
    fired_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lock: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
    head: ?*Waiter = null,

    pub fn init(rt: *runtime.Runtime) Cancel {
        return .{ .rt = rt };
    }

    pub fn deinit(self: *Cancel) void {
        // Nothing dynamic to free. The list is empty by API contract
        // at deinit time — any outstanding waiters mean a coro is
        // still parked, which is a use-after-free in flight.
        std.debug.assert(self.head == null);
    }

    /// Cheap synchronous check. Use at code-paths between blocking
    /// calls to bail early on cancel.
    pub inline fn isFired(self: *const Cancel) bool {
        return self.fired_flag.load(.acquire);
    }

    /// Returns `error.Cancelled` if the cancel has fired, else void.
    pub inline fn checkpoint(self: *const Cancel) Error!void {
        if (self.isFired()) return error.Cancelled;
    }

    fn acquireLock(self: *Cancel) void {
        while (self.lock.swap(1, .acquire) != 0) std.atomic.spinLoopHint();
    }

    fn releaseLock(self: *Cancel) void {
        self.lock.store(0, .release);
    }

    /// Register `w` to be woken on fire. `park_addr` is the
    /// parking-lot key the caller is about to park on; `fire()`
    /// uses it to call `unparkAll` and wake this coro.
    ///
    /// Returns true if the cancel has already fired (caller MUST
    /// NOT park; nothing to deregister). Returns false otherwise;
    /// caller MUST call `deregister(w)` after wake.
    pub fn register(self: *Cancel, w: *Waiter, park_addr: usize) bool {
        // Fast pre-check (avoids the lock if already fired).
        if (self.fired_flag.load(.acquire)) return true;
        self.acquireLock();
        if (self.fired_flag.load(.acquire)) {
            self.releaseLock();
            return true;
        }
        w.park_addr = park_addr;
        w.next = self.head;
        w.prev = null;
        w.in_list = true;
        if (self.head) |h| h.prev = w;
        self.head = w;
        self.releaseLock();
        return false;
    }

    /// Remove `w` from the waiter list. Idempotent — safe to call
    /// even if `fire()` already drained `w`.
    pub fn deregister(self: *Cancel, w: *Waiter) void {
        self.acquireLock();
        if (w.in_list) {
            if (w.prev) |p| {
                p.next = w.next;
            } else {
                self.head = w.next;
            }
            if (w.next) |n| n.prev = w.prev;
            w.in_list = false;
        }
        self.releaseLock();
    }

    /// Set the fired flag and wake every registered waiter via the
    /// parking lot. Idempotent — second call is a no-op.
    pub fn fire(self: *Cancel) void {
        if (self.fired_flag.swap(true, .acq_rel)) return;
        // Drain the list under the lock; mark each waiter as removed
        // so any concurrent `deregister` is a no-op.
        self.acquireLock();
        const drained = self.head;
        var cur = drained;
        while (cur) |w| {
            w.in_list = false;
            cur = w.next;
        }
        self.head = null;
        self.releaseLock();
        // Wake each parked op (outside the lock — unparkAll can be
        // a non-trivial amount of work).
        var w = drained;
        while (w) |it| {
            w = it.next;
            _ = park.unparkAll(self.rt, @ptrFromInt(it.park_addr));
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// Use smp_allocator (not std.testing.allocator) — see comment in
// src/runtime.zig.
const test_allocator = std.heap.smp_allocator;

test "Cancel: checkpoint returns Cancelled after fire" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    try std.testing.expect(!c.isFired());
    try c.checkpoint();
    c.fire();
    try std.testing.expect(c.isFired());
    try std.testing.expectError(error.Cancelled, c.checkpoint());
    // Idempotent — second fire is a no-op.
    c.fire();
    try std.testing.expect(c.isFired());
}

test "Cancel: register+deregister round-trip with no fire" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    var w: Waiter = .{};
    try std.testing.expect(!c.register(&w, 0xCAFE));
    try std.testing.expect(w.in_list);
    c.deregister(&w);
    try std.testing.expect(!w.in_list);
}

test "Cancel: register after fire returns true immediately" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    c.fire();
    var w: Waiter = .{};
    try std.testing.expect(c.register(&w, 0xBEEF));
    try std.testing.expect(!w.in_list);
}

test "Cancel: deregister on already-drained waiter is a no-op" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    var w: Waiter = .{};
    try std.testing.expect(!c.register(&w, 0xDEAD));
    c.fire();
    // fire() drained w. deregister should be safe and idempotent.
    try std.testing.expect(!w.in_list);
    c.deregister(&w);
    try std.testing.expect(!w.in_list);
}

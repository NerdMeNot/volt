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
const coroutine = @import("coroutine.zig");

pub const Error = error{Cancelled};

/// A `Waiter` represents a single registered "fire me on cancel"
/// hook. The `Kind` variant decides how `fire()` wakes the
/// owner:
///
///   * `.park_addr` — owner is parked in the parking lot keyed on
///     that address. Fire calls `parking_lot.unparkAll` on the key
///     and the validator-under-lock pattern re-checks `isFired`.
///     Used by `Mutex.lockCancel`, `Spsc.recvCancel`, etc.
///   * `.reactor_coro` — owner is parked in the kernel's event
///     queue (kqueue/epoll/io_uring/IOCP). Fire calls
///     `runtime.reactor.cancelCoro(coro)`, which performs the
///     per-platform deregistration syscall and unparks. The
///     reactor's deregister returns "did we actually remove the
///     pending op", arbitrating the race against a concurrent
///     poll() to avoid double-unpark / double-decrement.
pub const Waiter = struct {
    kind: Kind = .{ .park_addr = 0 },
    next: ?*Waiter = null,
    prev: ?*Waiter = null,
    in_list: bool = false,

    pub const Kind = union(enum) {
        park_addr: usize,
        reactor_coro: *coroutine.Coroutine,
    };
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

    /// Register `w` to be woken on fire via the parking lot.
    /// `park_addr` is the parking-lot key the caller is about to
    /// park on; `fire()` uses it to call `unparkAll` and wake this
    /// coro.
    ///
    /// Returns true if the cancel has already fired (caller MUST
    /// NOT park; nothing to deregister). Returns false otherwise;
    /// caller MUST call `deregister(w)` after wake.
    pub fn register(self: *Cancel, w: *Waiter, park_addr: usize) bool {
        w.kind = .{ .park_addr = park_addr };
        return self.registerCommon(w);
    }

    /// Register `w` to be woken on fire via the reactor's
    /// per-platform `cancelCoro` syscall. Used by cancel-aware
    /// fd-wait variants (`waitReadableCancel` etc.) whose parked
    /// owner lives in the kernel's event queue, not the parking
    /// lot.
    pub fn registerReactor(self: *Cancel, w: *Waiter, coro: *coroutine.Coroutine) bool {
        w.kind = .{ .reactor_coro = coro };
        return self.registerCommon(w);
    }

    fn registerCommon(self: *Cancel, w: *Waiter) bool {
        // Fast pre-check (avoids the lock if already fired).
        if (self.fired_flag.load(.acquire)) return true;
        self.acquireLock();
        if (self.fired_flag.load(.acquire)) {
            self.releaseLock();
            return true;
        }
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

    /// Set the fired flag and wake every registered waiter.
    /// Dispatches per-`Waiter.kind`: parking-lot waiters go through
    /// `unparkAll`; reactor waiters go through the reactor's
    /// `cancelCoro` syscall path. Idempotent — second call is a
    /// no-op.
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
        // Wake each parked op (outside the lock — both paths can be
        // a non-trivial amount of work).
        var w = drained;
        while (w) |it| {
            w = it.next;
            switch (it.kind) {
                .park_addr => |addr| _ = park.unparkAll(self.rt, @ptrFromInt(addr)),
                .reactor_coro => |c| self.rt.reactor.cancelCoro(c),
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// `volt.testing.allocator` — leak-detecting + multi-worker-safe.
// See src/testing.zig for why std.testing.allocator doesn't fit.
const test_allocator = @import("testing.zig").allocator;

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

// ─── sleepCancel: timer-side cancel-aware reactor parking ────────

const lib = @import("lib.zig");

const SleepCancelCtx = struct {
    cancel: *Cancel,
    got: ?anyerror = null,
};

fn sleepCancelSleeper(ctx: *SleepCancelCtx) void {
    // A 10s sleep — way longer than the test's wall-clock budget,
    // so if cancel doesn't fire eagerly the test times out and
    // we know the wiring is broken.
    const result = lib.sleepCancel(lib.Duration.fromSecs(10), ctx.cancel);
    ctx.got = if (result) |_| null else |e| e;
}

fn sleepCancelFirer(ctx: *SleepCancelCtx) void {
    // Yield enough that the sleeper has parked before we fire.
    var i: u32 = 0;
    while (i < 50) : (i += 1) @import("runtime.zig").yield();
    ctx.cancel.fire();
}

fn sleepCancelRoot(ctx: *SleepCancelCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(@import("current.zig").require().runtime));
    var sleeper = try rt.spawn(sleepCancelSleeper, .{ctx});
    var firer = try rt.spawn(sleepCancelFirer, .{ctx});
    _ = sleeper.join();
    _ = firer.join();
}

test "Cancel: sleepCancel wakes a sleeping coro on Cancel.fire" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    var ctx = SleepCancelCtx{ .cancel = &c };
    try (try rt.run(sleepCancelRoot, .{&ctx}));
    try std.testing.expectEqual(@as(?anyerror, error.Cancelled), ctx.got);
}

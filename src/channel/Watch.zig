//! Watch(T) — single-value broadcast with change notification.
//!
//! One sender, many receivers. Each receiver has its own "seen version"
//! counter and gets notified when the value changes (not for every
//! intermediate value — receivers only ever observe the LATEST). Useful
//! for config hot-reload, leader-election state, slow-consumer-friendly
//! status broadcast, etc.
//!
//! ## Usage
//!
//! ```zig
//! var watch = Watch(Config).init(initial_config);
//! defer watch.deinit();
//!
//! // Producer (some coroutine):
//! watch.send(new_config);
//!
//! // Receiver (each consumer holds its own *Receiver):
//! var rx = watch.subscribe();
//! while (true) {
//!     try rx.changed();           // suspend until value changes
//!     const cfg = rx.current();    // snapshot the current value
//!     applyConfig(cfg);
//! }
//! ```
//!
//! ## Semantics
//!
//! - `send` always overwrites — slow receivers see only the latest value.
//! - `current` returns a snapshot copy of T (no shared-reference contract,
//!   so receivers can't observe a torn write).
//! - `changed` returns when the version has advanced since the last call
//!   to `changed`/`markSeen`/`subscribe`. Returns `error.Closed` if the
//!   channel was closed.
//! - On `subscribe`, the receiver's `seen_version` is set to the current
//!   version, so it only fires on FUTURE changes.

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

const errors = @import("errors.zig");
pub const ChangedError = errors.RecvError;

pub fn Watch(comptime T: type) type {
    return struct {
        const Self = @This();

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

            fn drain(self: *@This()) ?*Waiter {
                const head = self.head;
                self.head = null;
                self.tail = null;
                return head;
            }
        };

        mutex: thread.Mutex = .{},
        value: T,
        version: u64 = 1,
        closed: bool = false,
        waiters: WaiterList = .{},

        /// Construct with an initial value. Receivers created via
        /// `subscribe` start at the current version, so they only fire
        /// on FUTURE updates.
        pub fn init(initial: T) Self {
            return .{ .value = initial };
        }

        pub fn deinit(self: *Self) void {
            // Defensive — callers should ensure no live waiters.
            assert(self.waiters.head == null);
            self.* = undefined;
        }

        /// Update the value and notify all parked receivers. Slow receivers
        /// won't see the previous value; they'll snapshot the latest on
        /// their next `current()`.
        pub fn send(self: *Self, val: T) void {
            self.mutex.lock();
            if (self.closed) {
                self.mutex.unlock();
                return;
            }
            self.value = val;
            self.version +%= 1;
            const drained = self.waiters.drain();
            self.mutex.unlock();
            unparkAll(drained);
        }

        /// Mark the channel closed. Wakes every parked `changed()` so it
        /// returns `error.Closed`.
        pub fn close(self: *Self) void {
            self.mutex.lock();
            self.closed = true;
            const drained = self.waiters.drain();
            self.mutex.unlock();
            unparkAll(drained);
        }

        /// Create a Receiver synced to the current version. The Receiver's
        /// `changed()` will fire on the NEXT update, not the current one.
        pub fn subscribe(self: *Self) Receiver {
            self.mutex.lock();
            defer self.mutex.unlock();
            return .{ .watch = self, .seen_version = self.version };
        }

        pub const Receiver = struct {
            watch: *Self,
            seen_version: u64,

            /// Snapshot the current value. Returns by value to avoid
            /// any shared-reference contract; callers always see a
            /// coherent T.
            pub fn current(self: *Receiver) T {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                return self.watch.value;
            }

            /// True iff the value has been updated since the last
            /// `changed()` / `markSeen()` / `subscribe()`.
            pub fn hasChanged(self: *const Receiver) bool {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                return self.watch.version != self.seen_version;
            }

            /// Mark the current version as seen, without waiting.
            pub fn markSeen(self: *Receiver) void {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                self.seen_version = self.watch.version;
            }

            /// Suspend until the value is updated, returning `void` on
            /// the next `send`. Returns `error.Closed` if the channel is
            /// closed (immediately if already closed).
            pub fn changed(self: *Receiver) ChangedError!void {
                while (true) {
                    var waiter: Waiter = .{};
                    self.watch.mutex.lock();
                    if (self.watch.version != self.seen_version) {
                        self.seen_version = self.watch.version;
                        self.watch.mutex.unlock();
                        return;
                    }
                    if (self.watch.closed) {
                        self.watch.mutex.unlock();
                        return error.Closed;
                    }
                    self.watch.waiters.pushBack(&waiter);
                    self.watch.mutex.unlock();

                    waiter.park.parkCurrent() catch |err| switch (err) {
                        error.Cancelled => {
                            self.watch.removeWaiter(&waiter);
                            return error.Cancelled;
                        },
                    };
                    // Loop to recheck. Send drains the whole list, so a
                    // wake here normally means we'll see the new version
                    // on the next iteration's check.
                }
            }
        };

        fn removeWaiter(self: *Self, target: *Waiter) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            var prev: ?*Waiter = null;
            var cur = self.waiters.head;
            while (cur) |w| : (cur = w.next) {
                if (w == target) {
                    if (prev) |p| p.next = w.next else self.waiters.head = w.next;
                    if (self.waiters.tail == w) self.waiters.tail = prev;
                    w.next = null;
                    return;
                }
                prev = w;
            }
            // Already popped by `send`/`close`. Fine.
        }

        fn unparkAll(head: ?*Waiter) void {
            var cur = head;
            while (cur) |w| {
                const next = w.next;
                w.next = null;
                w.park.unpark();
                cur = next;
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "watch: initial subscribe + current returns initial value" {
    var w = Watch(u32).init(7);
    defer w.deinit();
    var rx = w.subscribe();
    try std.testing.expectEqual(@as(u32, 7), rx.current());
    try std.testing.expect(!rx.hasChanged());
}

test "watch: send bumps version, hasChanged reports it" {
    var w = Watch(u32).init(7);
    defer w.deinit();
    var rx = w.subscribe();
    w.send(8);
    try std.testing.expect(rx.hasChanged());
    rx.markSeen();
    try std.testing.expect(!rx.hasChanged());
    try std.testing.expectEqual(@as(u32, 8), rx.current());
}

const Barrier = @import("../sync/Barrier.zig").Barrier;
const Notify = @import("../sync/Notify.zig").Notify;

const ChangedCtx = struct {
    w: *Watch(u32),
    /// Signaled by the watcher AFTER it subscribes, so the sender
    /// can't start before the watcher's seen_version is pinned.
    subscribed: *Notify,
    /// Signaled by the watcher AFTER it consumes each value, so the
    /// sender can pace the next send (otherwise back-to-back sends
    /// would collapse into a single .changed event for the watcher).
    consumed: *Notify,
    received: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn watcherFn(ctx: *ChangedCtx) !void {
    var rx = ctx.w.subscribe();
    ctx.subscribed.notifyOne();
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try rx.changed();
        _ = ctx.received.fetchAdd(rx.current(), .monotonic);
        ctx.consumed.notifyOne();
    }
}

fn senderFn(ctx: *ChangedCtx) !void {
    try ctx.subscribed.wait();
    var i: u32 = 1;
    while (i <= 3) : (i += 1) {
        ctx.w.send(i);
        try ctx.consumed.wait();
    }
}

fn changedRoot() !u32 {
    var w = Watch(u32).init(0);
    defer w.deinit();
    var subscribed = Notify{};
    var consumed = Notify{};
    var ctx = ChangedCtx{
        .w = &w,
        .subscribed = &subscribed,
        .consumed = &consumed,
    };

    var watcher = try volt.spawn(watcherFn, .{&ctx});
    defer volt.destroyTask(watcher);
    var sender = try volt.spawn(senderFn, .{&ctx});
    defer volt.destroyTask(sender);

    try sender.join();
    try watcher.join();
    return ctx.received.load(.acquire);
}

test "watch: receiver sees each send via changed() (Notify-paced)" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, changedRoot, .{});
    try std.testing.expectEqual(@as(u32, 6), sum); // 1+2+3
}

const FanOutCtx = struct {
    w: *Watch(u32),
    /// 32 watchers + 1 root all sync here before send.
    barrier: *Barrier,
    woke: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn fanOutWatcher(ctx: *FanOutCtx) !void {
    var rx = ctx.w.subscribe();
    _ = ctx.barrier.wait();
    try rx.changed();
    _ = ctx.woke.fetchAdd(1, .monotonic);
}

fn fanOutRoot() !u32 {
    var w = Watch(u32).init(0);
    defer w.deinit();
    const N: u32 = 32;
    var barrier = Barrier.init(N + 1);
    var ctx = FanOutCtx{ .w = &w, .barrier = &barrier };

    var watchers: [N]*volt.Task(@TypeOf(fanOutWatcher)) = undefined;
    for (&watchers) |*t| t.* = try volt.spawn(fanOutWatcher, .{&ctx});
    defer for (watchers) |t| volt.destroyTask(t);

    // Trip the barrier — by definition all 32 watchers have already
    // subscribed by the time we trip (subscribe is sequenced before
    // their barrier.wait()).
    _ = barrier.wait();

    w.send(99);

    for (watchers) |t| try t.join();
    return ctx.woke.load(.acquire);
}

test "watch: single send wakes all parked receivers (fan-out)" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, fanOutRoot, .{});
    try std.testing.expectEqual(@as(u32, 32), woke);
}

const CloseCtx2 = struct {
    w: *Watch(u32),
    subscribed: *Notify,
};

fn closeWaitFn(ctx: *CloseCtx2) ChangedError!void {
    var rx = ctx.w.subscribe();
    ctx.subscribed.notifyOne();
    try rx.changed();
}

fn closeWatchRoot() !void {
    var w = Watch(u32).init(0);
    defer w.deinit();
    var subscribed = Notify{};
    var ctx = CloseCtx2{ .w = &w, .subscribed = &subscribed };

    var waiter = try volt.spawn(closeWaitFn, .{&ctx});
    defer volt.destroyTask(waiter);

    try subscribed.wait();
    w.close();

    const result = waiter.join();
    try std.testing.expectError(error.Closed, result);
}

test "watch: close wakes parked receivers with error.Closed" {
    try volt.run(.{ .allocator = std.testing.allocator }, closeWatchRoot, .{});
}

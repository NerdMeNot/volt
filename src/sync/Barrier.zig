//! Barrier — N-task sync point with auto-reset.
//!
//! All N tasks call `wait()`. The first N-1 to arrive park; the Nth
//! "trips" the barrier — it returns `.leader` and wakes the
//! N-1 parked tasks, who each return `.follower`. The
//! barrier auto-resets for the next round.
//!
//! ## Usage
//!
//! ```zig
//! var barrier = Barrier.init(4);
//!
//! // Each of 4 coroutines:
//! switch (barrier.wait()) {
//!     .leader => {}, // exactly one of the 4 sees this
//!     .follower => {},
//! }
//! ```

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

pub const Barrier = struct {
    /// Outcome of `Barrier.wait`. Exactly one waiter per generation
    /// returns `.leader`; the rest get `.follower`.
    pub const WaitOutcome = enum { leader, follower };

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

    n: u32,
    mutex: thread.Mutex = .{},
    arrived: u32 = 0,
    /// Generation counter — bumped each time the barrier trips. Allows
    /// a parked waiter to distinguish "my round" from a stale residual
    /// wake (in case it spuriously wakes from anywhere).
    generation: u64 = 0,
    waiters: WaiterList = .{},

    pub fn init(n: u32) Barrier {
        assert(n >= 1);
        return .{ .n = n };
    }

    /// Block until all `n` tasks have arrived. Returns `.leader` to
    /// exactly one task per generation; the rest get `.follower`.
    pub fn wait(self: *Barrier) WaitOutcome {
        self.mutex.lock();
        const my_gen = self.generation;
        self.arrived += 1;
        if (self.arrived == self.n) {
            // We're the leader — trip the barrier.
            self.arrived = 0;
            self.generation +%= 1;
            const drained = self.waiters.drain();
            self.mutex.unlock();

            // Wake all the followers.
            var cur = drained;
            while (cur) |w| {
                const next = w.next;
                w.next = null;
                w.park.unpark();
                cur = next;
            }
            return .leader;
        }

        // Follower path — enqueue + park, then re-check generation on wake.
        var waiter: Waiter = .{};
        self.waiters.pushBack(&waiter);
        self.mutex.unlock();

        while (true) {
            // On cancel, leave the waiter list and surface as a follower —
            // the leader bit goes to whoever was the actual N-th arrival.
            waiter.park.parkCurrent() catch {
                self.mutex.lock();
                _ = self.removeWaiterLocked(&waiter);
                self.mutex.unlock();
                return .follower;
            };
            // Genuine wake from leader: my_gen has now been bumped.
            self.mutex.lock();
            const advanced = self.generation != my_gen;
            self.mutex.unlock();
            if (advanced) return .follower;
            // Spurious — re-park.
        }
    }

    fn removeWaiterLocked(self: *Barrier, target: *Waiter) bool {
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

const BarrierCtx = struct {
    b: *Barrier,
    leaders: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    finishes: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn barrierWaiter(ctx: *BarrierCtx) void {
    if (ctx.b.wait() == .leader) _ = ctx.leaders.fetchAdd(1, .monotonic);
    _ = ctx.finishes.fetchAdd(1, .monotonic);
}

fn singleRoundRoot(n: u32) !BarrierCtx {
    var b = Barrier.init(n);
    var ctx = BarrierCtx{ .b = &b };

    const allocator = std.testing.allocator;
    const jobs = try allocator.alloc(*volt.Job, n);
    defer allocator.free(jobs);

    for (jobs) |*j| j.* = try volt.launch(barrierWaiter, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    return ctx;
}

test "barrier: exactly one leader per round (n=8)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, singleRoundRoot, .{@as(u32, 8)});
    try std.testing.expectEqual(@as(u32, 1), ctx.leaders.load(.acquire));
    try std.testing.expectEqual(@as(u32, 8), ctx.finishes.load(.acquire));
}

test "barrier: exactly one leader per round (n=2)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, singleRoundRoot, .{@as(u32, 2)});
    try std.testing.expectEqual(@as(u32, 1), ctx.leaders.load(.acquire));
    try std.testing.expectEqual(@as(u32, 2), ctx.finishes.load(.acquire));
}

test "barrier: n=1 trips immediately, the lone task is leader" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, singleRoundRoot, .{@as(u32, 1)});
    try std.testing.expectEqual(@as(u32, 1), ctx.leaders.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.finishes.load(.acquire));
}

const MultiRoundCtx = struct {
    b: *Barrier,
    rounds: u32,
    leaders: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn multiRoundWaiter(ctx: *MultiRoundCtx) void {
    var i: u32 = 0;
    while (i < ctx.rounds) : (i += 1) {
        if (ctx.b.wait() == .leader) _ = ctx.leaders.fetchAdd(1, .monotonic);
    }
}

fn multiRoundRoot() !u32 {
    var b = Barrier.init(4);
    var ctx = MultiRoundCtx{ .b = &b, .rounds = 5 };
    var jobs: [4]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(multiRoundWaiter, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return ctx.leaders.load(.acquire);
}

test "barrier: 4 tasks × 5 rounds, exactly 5 leaders total" {
    const leaders = try volt.run(.{ .allocator = std.testing.allocator }, multiRoundRoot, .{});
    try std.testing.expectEqual(@as(u32, 5), leaders);
}

//! Async Barrier + Notify Integration Tests
//!
//! First async coverage for Barrier.waitFuture() and Notify.waitFuture().
//! These primitives had ZERO async test coverage — only sync spin-loop
//! stress tests existed.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Io = volt.Io;
const Barrier = volt.sync.Barrier;
const Notify = volt.sync.Notify;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Barrier Future — waits at barrier N generations, counts leaders
// ─────────────────────────────────────────────────────────────────────────────

const BarrierTaskFuture = struct {
    barrier: *Barrier,
    leader_count: *Atomic(usize),
    generations_remaining: usize,
    current_wait: ?volt.sync.barrier.WaitFuture = null,
    state: State = .start,

    const State = enum { start, waiting, done };
    pub const Output = void;

    pub fn init(barrier: *Barrier, leader_count: *Atomic(usize), generations: usize) BarrierTaskFuture {
        return .{ .barrier = barrier, .leader_count = leader_count, .generations_remaining = generations };
    }

    pub fn poll(self: *BarrierTaskFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.generations_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.current_wait = self.barrier.waitFuture();
                    self.state = .waiting;
                },
                .waiting => {
                    switch (self.current_wait.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => |result| {
                            if (result.is_leader) {
                                _ = self.leader_count.fetchAdd(1, .monotonic);
                            }
                            self.generations_remaining -= 1;
                            self.current_wait = null;
                            self.state = .start;
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Notify Futures — waiter + notifier
// ─────────────────────────────────────────────────────────────────────────────

const NotifyWaiterFuture = struct {
    notify: *Notify,
    done: *Atomic(usize),
    current_wait: ?volt.sync.notify.WaitFuture = null,
    state: State = .start,

    const State = enum { start, waiting, done };
    pub const Output = void;

    pub fn init(notify: *Notify, done: *Atomic(usize)) NotifyWaiterFuture {
        return .{ .notify = notify, .done = done };
    }

    pub fn poll(self: *NotifyWaiterFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    self.current_wait = self.notify.waitFuture();
                    self.state = .waiting;
                },
                .waiting => {
                    switch (self.current_wait.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.done.fetchAdd(1, .monotonic);
                            self.state = .done;
                            return .{ .ready = {} };
                        },
                    }
                },
                .done => return .{ .ready = {} },
            }
        }
    }
};

/// Notifier that calls notifyOne() N times, yielding cooperatively between each.
const NotifierFuture = struct {
    notify: *Notify,
    remaining: usize,

    pub const Output = void;

    pub fn init(notify: *Notify, count: usize) NotifierFuture {
        return .{ .notify = notify, .remaining = count };
    }

    pub fn poll(self: *NotifierFuture, ctx: *Context) PollResult(void) {
        if (self.remaining == 0) {
            return .{ .ready = {} };
        }
        self.notify.notifyOne();
        self.remaining -= 1;
        if (self.remaining > 0) {
            // Yield to let waiter process notification before sending next
            ctx.getWaker().wakeByRef();
            return .pending;
        }
        return .{ .ready = {} };
    }
};

/// Notifier that calls notifyAll() once.
const NotifyAllFuture = struct {
    notify: *Notify,
    called: bool = false,

    pub const Output = void;

    pub fn init(notify: *Notify) NotifyAllFuture {
        return .{ .notify = notify };
    }

    pub fn poll(self: *NotifyAllFuture, _: *Context) PollResult(void) {
        if (!self.called) {
            self.notify.notifyAll();
            self.called = true;
        }
        return .{ .ready = {} };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Async Barrier - 4 tasks x 10 generations" {
    const num_tasks = 4;
    const generations = 10;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var barrier = Barrier.init(num_tasks);
    var leader_count = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(BarrierTaskFuture.init(&barrier, &leader_count, generations));
    }
    for (&handles) |*h| _ = h.await(io);

    // Exactly one leader per generation
    try testing.expectEqual(@as(usize, generations), leader_count.load(.acquire));
}

test "Async Barrier - 8 tasks x 5 generations" {
    const num_tasks = 8;
    const generations = 5;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var barrier = Barrier.init(num_tasks);
    var leader_count = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(BarrierTaskFuture.init(&barrier, &leader_count, generations));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, generations), leader_count.load(.acquire));
}

test "Async Notify - 1 notifier 4 waiters" {
    const num_waiters = 4;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var notify = Notify.init();
    var done = Atomic(usize).init(0);

    // Spawn waiters first
    var waiter_handles: [num_waiters]volt.Future(void) = undefined;
    for (0..num_waiters) |i| {
        waiter_handles[i] = try io.awaitFuture(NotifyWaiterFuture.init(&notify, &done));
    }

    // Spawn notifier that sends one notification at a time
    var notifier_handle = try io.awaitFuture(NotifierFuture.init(&notify, num_waiters));

    _ = notifier_handle.await(io);
    for (&waiter_handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_waiters), done.load(.acquire));
}

test "Async Notify - notifyAll" {
    const num_waiters = 4;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var notify = Notify.init();
    var done = Atomic(usize).init(0);

    // Spawn waiters first
    var waiter_handles: [num_waiters]volt.Future(void) = undefined;
    for (0..num_waiters) |i| {
        waiter_handles[i] = try io.awaitFuture(NotifyWaiterFuture.init(&notify, &done));
    }

    // Spawn notifyAll
    var notifier_handle = try io.awaitFuture(NotifyAllFuture.init(&notify));

    _ = notifier_handle.await(io);
    for (&waiter_handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_waiters), done.load(.acquire));
}

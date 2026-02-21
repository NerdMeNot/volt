//! Async Semaphore Integration Tests
//!
//! Exercises AcquireFuture through the real scheduler under contention.
//! Catches the semaphore starvation deadlock where release() skipped
//! the mutex when no waiters appeared to be queued.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Io = volt.Io;
const Semaphore = volt.sync.Semaphore;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Future state machine (adapted from bench/volt_bench.zig)
// ─────────────────────────────────────────────────────────────────────────────

const MultiSemaphoreFuture = struct {
    semaphore: *Semaphore,
    counter: *Atomic(usize),
    ops_remaining: usize,
    acquire_future: ?volt.sync.semaphore.AcquireFuture = null,
    state: State = .start,

    const State = enum { start, acquiring, done };
    pub const Output = void;

    pub fn init(semaphore: *Semaphore, counter: *Atomic(usize), ops: usize) MultiSemaphoreFuture {
        return .{ .semaphore = semaphore, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiSemaphoreFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    // Fast path: try lock-free acquire
                    if (self.semaphore.tryAcquire(1)) {
                        _ = self.counter.fetchAdd(1, .monotonic);
                        self.semaphore.release(1);
                        self.ops_remaining -= 1;
                        continue;
                    }
                    self.acquire_future = self.semaphore.acquireFuture(1);
                    self.state = .acquiring;
                },
                .acquiring => {
                    switch (self.acquire_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.semaphore.release(1);
                            self.ops_remaining -= 1;
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
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Async Semaphore - 8 tasks 2 permits" {
    const num_tasks = 8;
    const num_permits = 2;
    const ops_per_task = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var semaphore = Semaphore.init(num_permits);
    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(MultiSemaphoreFuture.init(&semaphore, &counter, ops_per_task));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks * ops_per_task), counter.load(.acquire));
}

test "Async Semaphore - 16 tasks 1 permit" {
    const num_tasks = 16;
    const num_permits = 1;
    const ops_per_task = 50;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var semaphore = Semaphore.init(num_permits);
    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(MultiSemaphoreFuture.init(&semaphore, &counter, ops_per_task));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks * ops_per_task), counter.load(.acquire));
}

test "Async Semaphore - no contention" {
    const num_tasks = 4;
    const num_permits = 4;
    const ops_per_task = 200;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var semaphore = Semaphore.init(num_permits);
    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(MultiSemaphoreFuture.init(&semaphore, &counter, ops_per_task));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks * ops_per_task), counter.load(.acquire));
}

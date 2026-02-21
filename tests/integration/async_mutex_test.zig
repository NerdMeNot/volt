//! Async Mutex + RwLock Integration Tests
//!
//! Exercises LockFuture / ReadLockFuture / WriteLockFuture through the real
//! scheduler under contention. Catches the lost-wakeup bug (Feb 2026) where
//! tasks hung when 100+ contended on the same mutex.

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Io = volt.Io;
const Mutex = volt.sync.Mutex;
const RwLock = volt.sync.RwLock;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Future state machines (adapted from bench/volt_bench.zig)
// ─────────────────────────────────────────────────────────────────────────────

const MultiMutexFuture = struct {
    mutex: *Mutex,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.mutex.LockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(mutex: *Mutex, counter: *Atomic(usize), ops: usize) MultiMutexFuture {
        return .{ .mutex = mutex, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiMutexFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.mutex.lockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.mutex.unlock();
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

const MultiRwLockReadFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.rwlock.ReadLockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(rwlock: *RwLock, counter: *Atomic(usize), ops: usize) MultiRwLockReadFuture {
        return .{ .rwlock = rwlock, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiRwLockReadFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.rwlock.readLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.readUnlock();
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

const MultiRwLockWriteFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    ops_remaining: usize,
    lock_future: ?volt.sync.rwlock.WriteLockFuture = null,
    state: State = .start,

    const State = enum { start, locking, done };
    pub const Output = void;

    pub fn init(rwlock: *RwLock, counter: *Atomic(usize), ops: usize) MultiRwLockWriteFuture {
        return .{ .rwlock = rwlock, .counter = counter, .ops_remaining = ops };
    }

    pub fn poll(self: *MultiRwLockWriteFuture, ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .start => {
                    if (self.ops_remaining == 0) {
                        self.state = .done;
                        return .{ .ready = {} };
                    }
                    self.lock_future = self.rwlock.writeLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.writeUnlock();
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

test "Async Mutex - 8 tasks contended" {
    const num_tasks = 8;
    const ops_per_task = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var mutex = Mutex.init();
    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(MultiMutexFuture.init(&mutex, &counter, ops_per_task));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks * ops_per_task), counter.load(.acquire));
}

test "Async Mutex - 64 tasks contended" {
    const num_tasks = 64;
    const ops_per_task = 50;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var mutex = Mutex.init();
    var counter = Atomic(usize).init(0);

    var handles: [num_tasks]volt.Future(void) = undefined;
    for (0..num_tasks) |i| {
        handles[i] = try io.awaitFuture(MultiMutexFuture.init(&mutex, &counter, ops_per_task));
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, num_tasks * ops_per_task), counter.load(.acquire));
}

test "Async RwLock - 4R + 2W" {
    const num_readers = 4;
    const num_writers = 2;
    const total_tasks = num_readers + num_writers;
    const ops_per_task = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var rwlock = RwLock.init();
    var counter = Atomic(usize).init(0);

    var handles: [total_tasks]volt.Future(void) = undefined;
    var idx: usize = 0;
    for (0..num_readers) |_| {
        handles[idx] = try io.awaitFuture(MultiRwLockReadFuture.init(&rwlock, &counter, ops_per_task));
        idx += 1;
    }
    for (0..num_writers) |_| {
        handles[idx] = try io.awaitFuture(MultiRwLockWriteFuture.init(&rwlock, &counter, ops_per_task));
        idx += 1;
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, total_tasks * ops_per_task), counter.load(.acquire));
}

test "Async RwLock - writer-heavy 2R + 4W" {
    const num_readers = 2;
    const num_writers = 4;
    const total_tasks = num_readers + num_writers;
    const ops_per_task = 100;

    var io = try Io.init(testing.allocator, .{ .num_workers = 4 });
    defer io.deinit();

    var rwlock = RwLock.init();
    var counter = Atomic(usize).init(0);

    var handles: [total_tasks]volt.Future(void) = undefined;
    var idx: usize = 0;
    for (0..num_readers) |_| {
        handles[idx] = try io.awaitFuture(MultiRwLockReadFuture.init(&rwlock, &counter, ops_per_task));
        idx += 1;
    }
    for (0..num_writers) |_| {
        handles[idx] = try io.awaitFuture(MultiRwLockWriteFuture.init(&rwlock, &counter, ops_per_task));
        idx += 1;
    }
    for (&handles) |*h| _ = h.await(io);

    try testing.expectEqual(@as(usize, total_tasks * ops_per_task), counter.load(.acquire));
}

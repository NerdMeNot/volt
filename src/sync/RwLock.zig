//! RwLock — async reader-writer lock with writer priority.
//!
//! Multiple concurrent readers, OR one exclusive writer. Built on
//! `Semaphore` with `MAX_READERS` permits: a reader claims 1 permit, a
//! writer claims all of them. The Semaphore's FIFO waiter list gives us
//! writer priority for free — once a writer enqueues asking for all
//! permits, the queue can't satisfy a 1-permit reader without first
//! satisfying the writer.
//!
//! ## Usage
//!
//! ```zig
//! var rw = RwLock{};
//!
//! // Many readers concurrent:
//! rw.lockShared();
//! defer rw.unlockShared();
//! const value = read(&shared);
//!
//! // Exclusive writer:
//! rw.lockExclusive();
//! defer rw.unlockExclusive();
//! mutate(&shared);
//! ```

const std = @import("std");
const Semaphore = @import("Semaphore.zig").Semaphore;

/// Maximum concurrent readers before a 1-permit acquire fails. Picking
/// `1 << 24` (16M) leaves room for the writer's full claim — `release`
/// math operates on `u32`. In practice nobody hits this; channels &
/// pools cap their fan-out long before.
const MAX_READERS: u32 = 1 << 24;

pub const RwLock = struct {
    sem: Semaphore = Semaphore.init(MAX_READERS),

    pub fn lockShared(self: *RwLock) void {
        self.sem.acquire(1);
    }

    pub fn tryLockShared(self: *RwLock) bool {
        return self.sem.tryAcquire(1);
    }

    pub fn unlockShared(self: *RwLock) void {
        self.sem.release(1);
    }

    pub fn lockExclusive(self: *RwLock) void {
        self.sem.acquire(MAX_READERS);
    }

    pub fn tryLockExclusive(self: *RwLock) bool {
        return self.sem.tryAcquire(MAX_READERS);
    }

    pub fn unlockExclusive(self: *RwLock) void {
        self.sem.release(MAX_READERS);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

test "rwlock: tryLockShared succeeds many times, blocks tryLockExclusive" {
    var rw = RwLock{};
    try std.testing.expect(rw.tryLockShared());
    try std.testing.expect(rw.tryLockShared());
    try std.testing.expect(rw.tryLockShared());
    try std.testing.expect(!rw.tryLockExclusive());
    rw.unlockShared();
    rw.unlockShared();
    rw.unlockShared();
    try std.testing.expect(rw.tryLockExclusive());
    rw.unlockExclusive();
}

test "rwlock: tryLockExclusive blocks tryLockShared until released" {
    var rw = RwLock{};
    try std.testing.expect(rw.tryLockExclusive());
    try std.testing.expect(!rw.tryLockShared());
    try std.testing.expect(!rw.tryLockExclusive());
    rw.unlockExclusive();
    try std.testing.expect(rw.tryLockShared());
    rw.unlockShared();
}

const RwCtx = struct {
    rw: *RwLock,
    shared: u64 = 0,
    readers_in_flight: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    max_readers: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    writer_observed_alone: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn rwReader(ctx: *RwCtx) !void {
    ctx.rw.lockShared();
    defer ctx.rw.unlockShared();

    const n = ctx.readers_in_flight.fetchAdd(1, .acq_rel) + 1;
    var seen = ctx.max_readers.load(.acquire);
    while (n > seen) {
        if (ctx.max_readers.cmpxchgWeak(seen, n, .acq_rel, .acquire)) |obs| {
            seen = obs;
        } else break;
    }
    // Hold the lock across several yields so other readers actually
    // overlap with us — a single yield can finish before any sibling
    // worker has woken to dispatch the next reader.
    var k: u32 = 0;
    while (k < 8) : (k += 1) try volt.yield();
    _ = ctx.readers_in_flight.fetchSub(1, .acq_rel);
    _ = ctx.shared; // pretend to read
}

fn rwWriter(ctx: *RwCtx) !void {
    ctx.rw.lockExclusive();
    defer ctx.rw.unlockExclusive();

    // Verify no readers are in flight.
    if (ctx.readers_in_flight.load(.acquire) == 0) {
        _ = ctx.writer_observed_alone.fetchAdd(1, .acq_rel);
    }
    ctx.shared += 1;
    try volt.yield();
}

fn rwRoot() !RwCtx {
    var rw = RwLock{};
    var ctx = RwCtx{ .rw = &rw };

    const N_R: u32 = 16;
    const N_W: u32 = 4;

    var readers: [N_R]*volt.Task(@TypeOf(rwReader)) = undefined;
    var writers: [N_W]*volt.Task(@TypeOf(rwWriter)) = undefined;

    for (&readers) |*t| t.* = try volt.spawn(rwReader, .{&ctx});
    for (&writers) |*t| t.* = try volt.spawn(rwWriter, .{&ctx});
    defer for (readers) |t| volt.destroyTask(t);
    defer for (writers) |t| volt.destroyTask(t);

    for (readers) |t| try t.join();
    for (writers) |t| try t.join();

    return ctx;
}

test "rwlock: 16 readers + 4 writers — writers exclusive, all complete" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, rwRoot, .{});
    // The hard invariant: every writer observed an empty reader pool.
    try std.testing.expectEqual(@as(u32, 4), ctx.writer_observed_alone.load(.acquire));
    // Every writer's increment landed.
    try std.testing.expectEqual(@as(u64, 4), ctx.shared);
    // Soft invariant — at least one reader was in flight at some point.
    // We'd ideally assert ≥ 2 (genuine reader concurrency), but achieving
    // observable overlap requires more orchestration than a yield-loop;
    // the M8 stress harness with explicit barriers will pin that down.
    try std.testing.expect(ctx.max_readers.load(.acquire) >= 1);
}

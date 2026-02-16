//! RwLock - Async Read-Write Lock
//!
//! A read-write lock that allows multiple concurrent readers OR a single writer.
//! Writers have priority to prevent writer starvation.
//! For blocking rwlock, use std.Thread.RwLock from the standard library.
//!
//! ## Usage
//!
//! ```zig
//! var rwlock = RwLock.init();
//!
//! // Multiple readers allowed concurrently (non-blocking)
//! if (rwlock.tryReadLock()) {
//!     defer rwlock.readUnlock();
//!     // Read shared data
//! }
//!
//! // Only one writer, excludes all readers (non-blocking)
//! if (rwlock.tryWriteLock()) {
//!     defer rwlock.writeUnlock();
//!     // Modify shared data
//! }
//! ```
//!
//! ## Design
//!
//! Built on Semaphore(MAX_READS):
//! - Read lock = acquire 1 permit
//! - Write lock = acquire MAX_READS permits (drains all)
//!
//! Writer priority emerges naturally: a queued writer's partial acquisition
//! drains permits to 0, so new readers' tryAcquire(1) fails. FIFO queue
//! serves the writer before any reader queued behind it.
//!
//! Diagnostics (isWriteLocked, getReaderCount) are derived from semaphore
//! permit state — zero extra atomic operations on the lock/unlock hot path.
//!

const std = @import("std");

const semaphore_mod = @import("Semaphore.zig");
const Semaphore = semaphore_mod.Semaphore;
const SemaphoreWaiter = semaphore_mod.Waiter;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;

// Future system imports
const future_mod = @import("../future.zig");
const FutureWaker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiters
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for read lock acquisition (1 permit).
pub const ReadWaiter = struct {
    inner: SemaphoreWaiter,
    /// User's waker (set by caller, forwarded to semaphore waiter)
    user_waker: ?WakerFn = null,
    user_waker_ctx: ?*anyopaque = null,

    const Self = @This();

    pub fn init() Self {
        return .{ .inner = SemaphoreWaiter.init(1) };
    }

    /// Create a waiter with waker already configured.
    pub fn initWithWaker(ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{
            .inner = SemaphoreWaiter.init(1),
            .user_waker = wake_fn,
            .user_waker_ctx = ctx,
        };
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.user_waker = wake_fn;
        self.user_waker_ctx = ctx;
    }

    /// Check if the waiter is ready (lock was acquired).
    pub fn isReady(self: *const Self) bool {
        return self.inner.isReady();
    }

    /// Check if lock was acquired (alias for isReady).
    pub const isAcquired = isReady;

    pub fn token(self: *const Self) InvocationId.Id {
        return self.inner.token();
    }

    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.inner.verifyToken(tok);
    }

    pub fn reset(self: *Self) void {
        self.inner.reset();
        self.user_waker = null;
        self.user_waker_ctx = null;
    }
};

/// Waiter for write lock acquisition (MAX_READS permits).
pub const WriteWaiter = struct {
    inner: SemaphoreWaiter,
    /// User's waker (set by caller, forwarded to semaphore waiter)
    user_waker: ?WakerFn = null,
    user_waker_ctx: ?*anyopaque = null,

    const Self = @This();

    pub fn init() Self {
        return .{ .inner = SemaphoreWaiter.init(RwLock.MAX_READS) };
    }

    /// Create a waiter with waker already configured.
    pub fn initWithWaker(ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{
            .inner = SemaphoreWaiter.init(RwLock.MAX_READS),
            .user_waker = wake_fn,
            .user_waker_ctx = ctx,
        };
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.user_waker = wake_fn;
        self.user_waker_ctx = ctx;
    }

    /// Check if the waiter is ready (lock was acquired).
    pub fn isReady(self: *const Self) bool {
        return self.inner.isReady();
    }

    /// Check if lock was acquired (alias for isReady).
    pub const isAcquired = isReady;

    pub fn token(self: *const Self) InvocationId.Id {
        return self.inner.token();
    }

    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.inner.verifyToken(tok);
    }

    pub fn reset(self: *Self) void {
        self.inner.reset();
        self.user_waker = null;
        self.user_waker_ctx = null;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// RwLock
// ─────────────────────────────────────────────────────────────────────────────

/// An async-aware read-write lock. Built on Semaphore(MAX_READS).
///
/// Diagnostics (isWriteLocked, getReaderCount) are derived from semaphore
/// permit state — zero extra atomics on the lock/unlock hot path.
pub const RwLock = struct {
    /// Underlying semaphore: MAX_READS permits total
    sem: Semaphore,

    const Self = @This();

    /// Maximum concurrent readers. Writer needs all of them.
    pub const MAX_READS: usize = std.math.maxInt(u32) >> 3; // ~536M

    /// Create a new unlocked RwLock.
    pub fn init() Self {
        return .{
            .sem = Semaphore.init(MAX_READS),
        };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Read Lock
    // ═══════════════════════════════════════════════════════════════════════

    /// Try to acquire read lock without waiting.
    pub fn tryReadLock(self: *Self) bool {
        return self.sem.tryAcquire(1);
    }

    /// Low-level: Acquire read lock with explicit waiter.
    /// Sets user's waker directly on the semaphore waiter (no wrapper needed).
    pub fn readLockWait(self: *Self, waiter: *ReadWaiter) bool {
        // Forward user's waker directly to semaphore waiter
        if (waiter.user_waker) |wf| {
            if (waiter.user_waker_ctx) |ctx| {
                waiter.inner.setWaker(ctx, wf);
            }
        }

        return self.sem.acquireWait(&waiter.inner);
    }

    /// Release read lock.
    pub fn readUnlock(self: *Self) void {
        self.sem.release(1);
    }

    /// Cancel a pending read lock.
    pub fn cancelReadLock(self: *Self, waiter: *ReadWaiter) void {
        self.sem.cancelAcquire(&waiter.inner);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Write Lock
    // ═══════════════════════════════════════════════════════════════════════

    /// Try to acquire write lock without waiting.
    pub fn tryWriteLock(self: *Self) bool {
        return self.sem.tryAcquire(MAX_READS);
    }

    /// Low-level: Acquire write lock with explicit waiter.
    /// Sets user's waker directly on the semaphore waiter (no wrapper needed).
    pub fn writeLockWait(self: *Self, waiter: *WriteWaiter) bool {
        // Forward user's waker directly to semaphore waiter
        if (waiter.user_waker) |wf| {
            if (waiter.user_waker_ctx) |ctx| {
                waiter.inner.setWaker(ctx, wf);
            }
        }

        return self.sem.acquireWait(&waiter.inner);
    }

    /// Release write lock.
    pub fn writeUnlock(self: *Self) void {
        self.sem.release(MAX_READS);
    }

    /// Cancel a pending write lock.
    pub fn cancelWriteLock(self: *Self, waiter: *WriteWaiter) void {
        self.sem.cancelAcquire(&waiter.inner);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Diagnostics
    // ═══════════════════════════════════════════════════════════════════════

    /// Check if write-locked (derived from semaphore permits).
    /// Returns true when all MAX_READS permits are consumed (i.e. a writer holds them).
    pub fn isWriteLocked(self: *Self) bool {
        return self.sem.availablePermits() == 0;
    }

    /// Get reader count (derived from semaphore permits).
    /// Returns MAX_READS - available_permits when readers hold the lock.
    /// Returns 0 when write-locked or unlocked.
    pub fn getReaderCount(self: *Self) usize {
        const avail = self.sem.availablePermits();
        if (avail >= MAX_READS) return 0; // Unlocked
        if (avail == 0) return 0; // Write-locked
        return MAX_READS - avail;
    }

    /// Get waiting reader count (walks waiter queue, O(n)).
    pub fn waitingReaders(self: *Self) usize {
        return self.sem.countWaitersMatching(isReaderWaiter);
    }

    /// Get waiting writer count (walks waiter queue, O(n)).
    pub fn waitingWriters(self: *Self) usize {
        return self.sem.countWaitersMatching(isWriterWaiter);
    }

    fn isReaderWaiter(permits_requested: usize) bool {
        return permits_requested == 1;
    }

    fn isWriterWaiter(permits_requested: usize) bool {
        return permits_requested == MAX_READS;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Guard Convenience Methods
    // ═══════════════════════════════════════════════════════════════════════

    /// Try to read lock and return a guard if successful (non-blocking).
    pub fn tryReadLockGuard(self: *Self) ?ReadGuard {
        if (self.tryReadLock()) {
            return ReadGuard.init(self);
        }
        return null;
    }

    /// Try to write lock and return a guard if successful (non-blocking).
    pub fn tryWriteLockGuard(self: *Self) ?WriteGuard {
        if (self.tryWriteLock()) {
            return WriteGuard.init(self);
        }
        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Async API
    // ═══════════════════════════════════════════════════════════════════════

    /// Acquire read lock, blocking the current task until acquired.
    pub fn readLock(self: *Self, io: @import("../Io.zig")) void {
        var f = io.awaitFuture(self.readLockFuture()) catch return;
        _ = f.await(io);
    }

    /// Acquire read lock. Returns a `ReadLockFuture`.
    pub fn readLockFuture(self: *Self) ReadLockFuture {
        return ReadLockFuture.init(self);
    }

    /// Acquire write lock, blocking the current task until acquired.
    pub fn writeLock(self: *Self, io: @import("../Io.zig")) void {
        var f = io.awaitFuture(self.writeLockFuture()) catch return;
        _ = f.await(io);
    }

    /// Acquire write lock. Returns a `WriteLockFuture`.
    pub fn writeLockFuture(self: *Self) WriteLockFuture {
        return WriteLockFuture.init(self);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// RAII Guards
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard for read lock.
pub const ReadGuard = struct {
    rwlock: *RwLock,
    active: bool,

    const Self = @This();

    pub fn init(rwlock: *RwLock) Self {
        return .{ .rwlock = rwlock, .active = true };
    }

    pub fn unlock(self: *Self) void {
        if (self.active) {
            self.rwlock.readUnlock();
            self.active = false;
        }
    }

    pub fn deinit(self: *Self) void {
        self.unlock();
    }
};

/// RAII guard for write lock.
pub const WriteGuard = struct {
    rwlock: *RwLock,
    active: bool,

    const Self = @This();

    pub fn init(rwlock: *RwLock) Self {
        return .{ .rwlock = rwlock, .active = true };
    }

    pub fn unlock(self: *Self) void {
        if (self.active) {
            self.rwlock.writeUnlock();
            self.active = false;
        }
    }

    pub fn deinit(self: *Self) void {
        self.unlock();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// ReadLockFuture - Async read lock acquisition
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when a read lock is acquired.
pub const ReadLockFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = void;

    /// Reference to the RwLock
    rwlock: *RwLock,

    /// Read waiter (wraps semaphore waiter + diagnostic counter update)
    waiter: ReadWaiter,

    /// State machine for the future
    state: State,

    /// Stored waker for when we're woken by unlock
    stored_waker: ?FutureWaker,

    const State = enum {
        init,
        waiting,
        acquired,
    };

    pub fn init(rwlock: *RwLock) Self {
        return .{
            .rwlock = rwlock,
            .waiter = ReadWaiter.init(),
            .state = .init,
            .stored_waker = null,
        };
    }

    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        switch (self.state) {
            .init => {
                self.stored_waker = ctx.getWaker().clone();

                // Set the user waker on the ReadWaiter (will be wrapped by readLockWait)
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                if (self.rwlock.readLockWait(&self.waiter)) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                } else {
                    self.state = .waiting;
                    return .pending;
                }
            },

            .waiting => {
                if (self.waiter.isAcquired()) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                }

                const new_waker = ctx.getWaker();
                if (self.stored_waker) |*old| {
                    if (!old.willWakeSame(new_waker)) {
                        old.deinit();
                        self.stored_waker = new_waker.clone();
                    }
                } else {
                    self.stored_waker = new_waker.clone();
                }

                return .pending;
            },

            .acquired => {
                return .{ .ready = {} };
            },
        }
    }

    pub fn cancel(self: *Self) void {
        if (self.state == .waiting) {
            self.rwlock.cancelReadLock(&self.waiter);
        }
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    pub fn deinit(self: *Self) void {
        self.cancel();
    }

    fn wakeCallback(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.stored_waker) |*w| {
            w.wakeByRef();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// WriteLockFuture - Async write lock acquisition
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when a write lock is acquired.
pub const WriteLockFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = void;

    /// Reference to the RwLock
    rwlock: *RwLock,

    /// Write waiter (wraps semaphore waiter + diagnostic flag update)
    waiter: WriteWaiter,

    /// State machine for the future
    state: State,

    /// Stored waker for when we're woken by unlock
    stored_waker: ?FutureWaker,

    const State = enum {
        init,
        waiting,
        acquired,
    };

    pub fn init(rwlock: *RwLock) Self {
        return .{
            .rwlock = rwlock,
            .waiter = WriteWaiter.init(),
            .state = .init,
            .stored_waker = null,
        };
    }

    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        switch (self.state) {
            .init => {
                self.stored_waker = ctx.getWaker().clone();

                // Set the user waker on the WriteWaiter (will be wrapped by writeLockWait)
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                if (self.rwlock.writeLockWait(&self.waiter)) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                } else {
                    self.state = .waiting;
                    return .pending;
                }
            },

            .waiting => {
                if (self.waiter.isAcquired()) {
                    self.state = .acquired;
                    if (self.stored_waker) |*w| {
                        w.deinit();
                        self.stored_waker = null;
                    }
                    return .{ .ready = {} };
                }

                const new_waker = ctx.getWaker();
                if (self.stored_waker) |*old| {
                    if (!old.willWakeSame(new_waker)) {
                        old.deinit();
                        self.stored_waker = new_waker.clone();
                    }
                } else {
                    self.stored_waker = new_waker.clone();
                }

                return .pending;
            },

            .acquired => {
                return .{ .ready = {} };
            },
        }
    }

    pub fn cancel(self: *Self) void {
        if (self.state == .waiting) {
            self.rwlock.cancelWriteLock(&self.waiter);
        }
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    pub fn deinit(self: *Self) void {
        self.cancel();
    }

    fn wakeCallback(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.stored_waker) |*w| {
            w.wakeByRef();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "RwLock - multiple readers" {
    var rwlock = RwLock.init();

    // Multiple readers can acquire
    try std.testing.expect(rwlock.tryReadLock());
    try std.testing.expect(rwlock.tryReadLock());
    try std.testing.expect(rwlock.tryReadLock());

    try std.testing.expectEqual(@as(usize, 3), rwlock.getReaderCount());

    // Writer cannot acquire while readers hold
    try std.testing.expect(!rwlock.tryWriteLock());

    rwlock.readUnlock();
    rwlock.readUnlock();
    rwlock.readUnlock();

    try std.testing.expectEqual(@as(usize, 0), rwlock.getReaderCount());
}

test "RwLock - exclusive writer" {
    var rwlock = RwLock.init();

    try std.testing.expect(rwlock.tryWriteLock());
    try std.testing.expect(rwlock.isWriteLocked());

    // No one else can acquire
    try std.testing.expect(!rwlock.tryReadLock());
    try std.testing.expect(!rwlock.tryWriteLock());

    rwlock.writeUnlock();
    try std.testing.expect(!rwlock.isWriteLocked());
}

test "RwLock - writer priority" {
    var rwlock = RwLock.init();

    // Reader holds lock
    try std.testing.expect(rwlock.tryReadLock());

    // Writer waits — this drains semaphore permits toward 0
    var write_waiter = WriteWaiter.init();
    try std.testing.expect(!rwlock.writeLockWait(&write_waiter));
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingWriters());

    // New reader should wait (writer has drained most permits)
    try std.testing.expect(!rwlock.tryReadLock());

    // Release reader - writer should get lock
    rwlock.readUnlock();
    try std.testing.expect(write_waiter.isAcquired());
    try std.testing.expect(rwlock.isWriteLocked());

    rwlock.writeUnlock();
}

test "RwLock - wake all readers after writer" {
    var rwlock = RwLock.init();
    var woken: [3]bool = .{ false, false, false };

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Writer holds lock
    try std.testing.expect(rwlock.tryWriteLock());

    // Readers wait
    var read_waiters: [3]ReadWaiter = undefined;
    for (&read_waiters, 0..) |*w, i| {
        w.* = ReadWaiter.init();
        w.setWaker(@ptrCast(&woken[i]), TestWaker.wake);
        _ = rwlock.readLockWait(w);
    }

    try std.testing.expectEqual(@as(usize, 3), rwlock.waitingReaders());

    // Writer releases - all readers should wake
    rwlock.writeUnlock();

    for (woken) |w| {
        try std.testing.expect(w);
    }
    try std.testing.expectEqual(@as(usize, 3), rwlock.getReaderCount());

    // Clean up
    rwlock.readUnlock();
    rwlock.readUnlock();
    rwlock.readUnlock();
}

test "RwLock - cancel read lock" {
    var rwlock = RwLock.init();

    // Writer holds lock
    try std.testing.expect(rwlock.tryWriteLock());

    // Reader waits
    var read_waiter = ReadWaiter.init();
    try std.testing.expect(!rwlock.readLockWait(&read_waiter));
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingReaders());

    // Cancel the read lock
    rwlock.cancelReadLock(&read_waiter);
    try std.testing.expectEqual(@as(usize, 0), rwlock.waitingReaders());
    try std.testing.expect(!read_waiter.isAcquired());

    rwlock.writeUnlock();
}

test "RwLock - cancel write lock" {
    var rwlock = RwLock.init();

    // Reader holds lock
    try std.testing.expect(rwlock.tryReadLock());

    // Writer waits
    var write_waiter = WriteWaiter.init();
    try std.testing.expect(!rwlock.writeLockWait(&write_waiter));
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingWriters());

    // Cancel the write lock
    rwlock.cancelWriteLock(&write_waiter);
    try std.testing.expectEqual(@as(usize, 0), rwlock.waitingWriters());
    try std.testing.expect(!write_waiter.isAcquired());

    // New reader should be able to acquire (no waiting writer)
    try std.testing.expect(rwlock.tryReadLock());

    rwlock.readUnlock();
    rwlock.readUnlock();
}

test "RwLock - tryReadLockGuard" {
    var rwlock = RwLock.init();

    if (rwlock.tryReadLockGuard()) |g| {
        var guard = g;
        defer guard.deinit();
        try std.testing.expectEqual(@as(usize, 1), rwlock.getReaderCount());
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expectEqual(@as(usize, 0), rwlock.getReaderCount());
}

test "RwLock - tryWriteLockGuard" {
    var rwlock = RwLock.init();

    if (rwlock.tryWriteLockGuard()) |g| {
        var guard = g;
        defer guard.deinit();
        try std.testing.expect(rwlock.isWriteLocked());
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(!rwlock.isWriteLocked());
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadLockFuture / WriteLockFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "ReadLockFuture - immediate acquisition" {
    var rwlock = RwLock.init();

    // Create future and poll - should acquire immediately
    var future = rwlock.readLockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(usize, 1), rwlock.getReaderCount());

    rwlock.readUnlock();
    try std.testing.expectEqual(@as(usize, 0), rwlock.getReaderCount());
}

test "ReadLockFuture - waits when writer holds lock" {
    var rwlock = RwLock.init();
    var waker_called = false;

    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) FutureWaker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Writer holds lock
    try std.testing.expect(rwlock.tryWriteLock());

    // Create future - first poll should return pending
    var future = rwlock.readLockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingReaders());

    // Writer releases - should wake the future
    rwlock.writeUnlock();
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expectEqual(@as(usize, 1), rwlock.getReaderCount());

    rwlock.readUnlock();
}

test "WriteLockFuture - immediate acquisition" {
    var rwlock = RwLock.init();

    // Create future and poll - should acquire immediately
    var future = rwlock.writeLockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(rwlock.isWriteLocked());

    rwlock.writeUnlock();
    try std.testing.expect(!rwlock.isWriteLocked());
}

test "WriteLockFuture - waits when readers hold lock" {
    var rwlock = RwLock.init();
    var waker_called = false;

    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) FutureWaker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Reader holds lock
    try std.testing.expect(rwlock.tryReadLock());

    // Create write future - first poll should return pending
    var future = rwlock.writeLockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingWriters());

    // Reader releases - should wake the writer
    rwlock.readUnlock();
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(rwlock.isWriteLocked());

    rwlock.writeUnlock();
}

test "ReadLockFuture - cancel removes from queue" {
    var rwlock = RwLock.init();

    // Writer holds lock
    try std.testing.expect(rwlock.tryWriteLock());

    // Create read future and poll to add to queue
    var future = rwlock.readLockFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingReaders());

    // Cancel
    future.cancel();
    try std.testing.expectEqual(@as(usize, 0), rwlock.waitingReaders());

    future.deinit();
    rwlock.writeUnlock();
}

test "WriteLockFuture - cancel removes from queue" {
    var rwlock = RwLock.init();

    // Reader holds lock
    try std.testing.expect(rwlock.tryReadLock());

    // Create write future and poll to add to queue
    var future = rwlock.writeLockFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());
    try std.testing.expectEqual(@as(usize, 1), rwlock.waitingWriters());

    // Cancel
    future.cancel();
    try std.testing.expectEqual(@as(usize, 0), rwlock.waitingWriters());

    future.deinit();
    rwlock.readUnlock();
}

test "ReadLockFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(ReadLockFuture));
    try std.testing.expect(ReadLockFuture.Output == void);
}

test "WriteLockFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(WriteLockFuture));
    try std.testing.expect(WriteLockFuture.Output == void);
}

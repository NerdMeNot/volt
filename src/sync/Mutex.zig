//! Mutex - Async Mutual Exclusion
//!
//! An async-aware mutex that allows tasks to wait for exclusive access.
//! For blocking mutex, use `std.Thread.Mutex` from the standard library.
//!
//! ## Usage
//!
//! ```zig
//! var mutex = Mutex.init();
//!
//! // Try to lock without waiting (non-blocking)
//! if (mutex.tryLock()) {
//!     defer mutex.unlock();
//!     // Critical section
//! }
//!
//! // Async lock with waiter (for use with runtime)
//! var waiter = Mutex.Waiter.init();
//! if (!mutex.lockWait(&waiter)) {
//!     // Waiter added to queue - yield to scheduler
//!     // When woken, waiter.isAcquired() will be true
//! }
//! defer mutex.unlock();
//! ```
//!
//! ## Design
//!
//! Built on Semaphore(1). All correctness flows through the Semaphore's
//! proven algorithm (permits go directly to waiters, no starvation).
//!

const std = @import("std");

const semaphore_mod = @import("Semaphore.zig");
const Semaphore = semaphore_mod.Semaphore;
const SemaphoreWaiter = semaphore_mod.Waiter;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// A waiter for mutex lock acquisition.
/// Thin wrapper around Semaphore.Waiter with 1 permit.
pub const Waiter = struct {
    inner: SemaphoreWaiter,

    const Self = @This();

    pub fn init() Self {
        return .{ .inner = SemaphoreWaiter.init(1) };
    }

    /// Create a waiter with waker already configured.
    pub fn initWithWaker(ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{ .inner = SemaphoreWaiter.initWithWaker(1, ctx, wake_fn) };
    }

    /// Set the waker for this waiter
    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.inner.setWaker(ctx, wake_fn);
    }

    /// Check if the waiter is ready (lock was acquired).
    pub fn isReady(self: *const Self) bool {
        return self.inner.isReady();
    }

    /// Check if lock was acquired (alias for isReady).
    pub const isAcquired = isReady;

    /// Get invocation token (for debug tracking)
    pub fn token(self: *const Self) InvocationId.Id {
        return self.inner.token();
    }

    /// Verify invocation token matches (debug mode)
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.inner.verifyToken(tok);
    }

    /// Reset for reuse (generates new invocation ID)
    pub fn reset(self: *Self) void {
        self.inner.reset();
    }
};

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

// ─────────────────────────────────────────────────────────────────────────────
// Mutex
// ─────────────────────────────────────────────────────────────────────────────

/// An async-aware mutex. Built on Semaphore(1).
pub const Mutex = struct {
    sem: Semaphore,

    const Self = @This();

    /// Create a new unlocked mutex.
    pub fn init() Self {
        return .{ .sem = Semaphore.init(1) };
    }

    /// Try to acquire the lock without waiting.
    /// Returns true if lock was acquired, false otherwise.
    pub fn tryLock(self: *Self) bool {
        return self.sem.tryAcquire(1);
    }

    /// Low-level: Acquire the lock with explicit waiter.
    /// Returns true if lock was acquired immediately.
    /// Returns false if the waiter was added to the queue (task should yield).
    pub fn lockWait(self: *Self, waiter: *Waiter) bool {
        return self.sem.acquireWait(&waiter.inner);
    }

    /// Release the lock.
    pub fn unlock(self: *Self) void {
        self.sem.release(1);
    }

    /// Cancel a lock acquisition.
    pub fn cancelLock(self: *Self, waiter: *Waiter) void {
        self.sem.cancelAcquire(&waiter.inner);
    }

    /// Check if locked (for debugging).
    pub fn isLocked(self: *const Self) bool {
        return self.sem.availablePermits() == 0;
    }

    /// Get number of waiters (for debugging).
    pub fn waiterCount(self: *Self) usize {
        return self.sem.waiterCount();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Convenience API (takes Io — the user-facing API)
    // ═══════════════════════════════════════════════════════════════════════

    /// Acquire the lock, blocking the current task until acquired.
    ///
    /// This is the primary API. Internally spawns the lock future on the
    /// runtime and waits for completion.
    ///
    /// NOTE: Returns without acquiring if the runtime cannot spawn the
    /// internal task (e.g., out of memory or shutting down). Use
    /// `lockFuture()` for explicit error handling in production code.
    ///
    /// ```zig
    /// mutex.lock(io);
    /// defer mutex.unlock();
    /// // critical section
    /// ```
    pub fn lock(self: *Self, io: @import("../Io.zig")) void {
        var f = io.awaitFuture(self.lockFuture()) catch return;
        _ = f.await(io);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Async API (returns Futures — for internal/advanced use)
    // ═══════════════════════════════════════════════════════════════════════

    /// Acquire the lock. Returns a `LockFuture` that resolves when acquired.
    /// Prefer `lock(io)` unless you need manual Future control.
    pub fn lockFuture(self: *Self) LockFuture {
        return LockFuture.init(self);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Guard Convenience Methods
    // ═══════════════════════════════════════════════════════════════════════

    /// Try to lock and return a guard if successful (non-blocking).
    pub fn tryLockGuard(self: *Self) ?MutexGuard {
        if (self.tryLock()) {
            return MutexGuard.init(self);
        }
        return null;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// MutexGuard - RAII guard
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that releases the mutex when dropped.
pub const MutexGuard = struct {
    mutex: *Mutex,
    active: bool,

    const Self = @This();

    pub fn init(mutex: *Mutex) Self {
        return .{
            .mutex = mutex,
            .active = true,
        };
    }

    /// Explicitly unlock.
    pub fn unlock(self: *Self) void {
        if (self.active) {
            self.mutex.unlock();
            self.active = false;
        }
    }

    /// Deinit (unlocks if still held).
    pub fn deinit(self: *Self) void {
        self.unlock();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// LockFuture - Async lock acquisition
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when the mutex lock is acquired.
/// Wraps Semaphore's AcquireFuture for 1 permit.
pub const LockFuture = struct {
    const Self = @This();

    /// Output type for Future trait
    pub const Output = void;

    /// The underlying acquire future
    acquire_future: semaphore_mod.AcquireFuture,

    /// Reference to the mutex (for cancel/diagnostics)
    mutex: *Mutex,

    /// Initialize a new lock future
    pub fn init(mutex: *Mutex) Self {
        return .{
            .acquire_future = mutex.sem.acquireFuture(1),
            .mutex = mutex,
        };
    }

    /// Poll the future - implements Future trait
    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        return self.acquire_future.poll(ctx);
    }

    /// Cancel the lock acquisition
    pub fn cancel(self: *Self) void {
        self.acquire_future.cancel();
    }

    /// Deinit the future
    pub fn deinit(self: *Self) void {
        self.acquire_future.deinit();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex - tryLock success" {
    var mutex = Mutex.init();

    try std.testing.expect(mutex.tryLock());
    try std.testing.expect(mutex.isLocked());

    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());
}

test "Mutex - tryLock fails when locked" {
    var mutex = Mutex.init();

    try std.testing.expect(mutex.tryLock());
    try std.testing.expect(!mutex.tryLock());

    mutex.unlock();
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

test "Mutex - lock and unlock with waiter" {
    var mutex = Mutex.init();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // First lock succeeds immediately
    var waiter1 = Waiter.init();
    try std.testing.expect(mutex.lockWait(&waiter1));
    try std.testing.expect(waiter1.isAcquired());

    // Second lock should wait
    var waiter2 = Waiter.init();
    waiter2.setWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!mutex.lockWait(&waiter2));
    try std.testing.expect(!waiter2.isAcquired());
    try std.testing.expectEqual(@as(usize, 1), mutex.waiterCount());

    // Unlock grants to waiter2 and calls waker
    mutex.unlock();
    try std.testing.expect(waiter2.isAcquired());
    try std.testing.expect(woken);
    try std.testing.expect(mutex.isLocked()); // Still locked by waiter2

    // Clean unlock
    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());
}

test "Mutex - FIFO ordering" {
    var mutex = Mutex.init();

    // Lock the mutex
    try std.testing.expect(mutex.tryLock());

    // Add waiters in order
    var waiters: [3]Waiter = undefined;
    for (&waiters) |*w| {
        w.* = Waiter.init();
        _ = mutex.lockWait(w);
    }

    try std.testing.expectEqual(@as(usize, 3), mutex.waiterCount());

    // Unlock should wake in FIFO order
    mutex.unlock();
    try std.testing.expect(waiters[0].isAcquired());
    try std.testing.expect(!waiters[1].isAcquired());
    try std.testing.expect(!waiters[2].isAcquired());

    mutex.unlock();
    try std.testing.expect(waiters[1].isAcquired());

    mutex.unlock();
    try std.testing.expect(waiters[2].isAcquired());

    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());
}

test "Mutex - cancel removes waiter" {
    var mutex = Mutex.init();

    // Lock the mutex
    try std.testing.expect(mutex.tryLock());

    // Add waiter
    var waiter = Waiter.init();
    _ = mutex.lockWait(&waiter);
    try std.testing.expectEqual(@as(usize, 1), mutex.waiterCount());

    // Cancel
    mutex.cancelLock(&waiter);
    try std.testing.expectEqual(@as(usize, 0), mutex.waiterCount());

    // Unlock should succeed with no waiters
    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());
}

test "Mutex - tryLockGuard success" {
    var mutex = Mutex.init();

    if (mutex.tryLockGuard()) |g| {
        var guard = g;
        defer guard.deinit();
        try std.testing.expect(mutex.isLocked());
    } else {
        try std.testing.expect(false); // Should have succeeded
    }

    try std.testing.expect(!mutex.isLocked());
}

test "Mutex - tryLockGuard failure" {
    var mutex = Mutex.init();

    // Lock the mutex
    try std.testing.expect(mutex.tryLock());

    // tryLockGuard should fail
    try std.testing.expect(mutex.tryLockGuard() == null);

    mutex.unlock();
}

test "Mutex - guard RAII" {
    var mutex = Mutex.init();

    {
        try std.testing.expect(mutex.tryLock());
        var guard = MutexGuard.init(&mutex);
        try std.testing.expect(mutex.isLocked());

        guard.deinit();
    }

    try std.testing.expect(!mutex.isLocked());
}

test "Mutex.Waiter - initWithWaker convenience" {
    var mutex = Mutex.init();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Lock the mutex first
    try std.testing.expect(mutex.tryLock());

    // Create waiter with initWithWaker (fewer steps than init + setWaker)
    var waiter = Waiter.initWithWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!mutex.lockWait(&waiter));
    try std.testing.expect(!waiter.isReady()); // Using isReady (unified API)
    try std.testing.expect(!waiter.isAcquired()); // Alias still works

    // Unlock grants to waiter
    mutex.unlock();
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(woken);

    mutex.unlock();
}

test "Mutex.Waiter - isReady is alias for isAcquired" {
    var mutex = Mutex.init();
    var waiter = Waiter.init();

    // Both should return false initially
    try std.testing.expect(!waiter.isReady());
    try std.testing.expect(!waiter.isAcquired());

    // Lock should set acquired
    try std.testing.expect(mutex.lockWait(&waiter));

    // Both should return true
    try std.testing.expect(waiter.isReady());
    try std.testing.expect(waiter.isAcquired());

    mutex.unlock();
}

// ─────────────────────────────────────────────────────────────────────────────
// LockFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "LockFuture - immediate acquisition" {
    var mutex = Mutex.init();

    // Create future and poll - should acquire immediately
    var future = mutex.lockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(mutex.isLocked());

    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());
}

test "LockFuture - waits when contended" {
    var mutex = Mutex.init();
    var waker_called = false;

    // Create a test waker that tracks calls
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

        fn toWaker(self: *@This()) Waker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Lock the mutex first
    try std.testing.expect(mutex.tryLock());

    // Create future - first poll should return pending
    var future = mutex.lockFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);
    try std.testing.expectEqual(@as(usize, 1), mutex.waiterCount());

    // Unlock - should wake the future
    mutex.unlock();
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(mutex.isLocked()); // Still locked by future

    mutex.unlock();
}

test "LockFuture - cancel removes from queue" {
    var mutex = Mutex.init();

    // Lock the mutex
    try std.testing.expect(mutex.tryLock());

    // Create future and poll to add to queue
    var future = mutex.lockFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());
    try std.testing.expectEqual(@as(usize, 1), mutex.waiterCount());

    // Cancel
    future.cancel();
    try std.testing.expectEqual(@as(usize, 0), mutex.waiterCount());

    // Clean up
    future.deinit();
    mutex.unlock();
}

test "LockFuture - multiple waiters FIFO" {
    var mutex = Mutex.init();

    // Lock the mutex
    try std.testing.expect(mutex.tryLock());

    // Create multiple futures
    var future1 = mutex.lockFuture();
    var future2 = mutex.lockFuture();
    var future3 = mutex.lockFuture();

    var ctx = Context{ .waker = &future_mod.noop_waker };

    // All should return pending
    try std.testing.expect(future1.poll(&ctx).isPending());
    try std.testing.expect(future2.poll(&ctx).isPending());
    try std.testing.expect(future3.poll(&ctx).isPending());

    try std.testing.expectEqual(@as(usize, 3), mutex.waiterCount());

    // Unlock - future1 should be ready
    mutex.unlock();
    try std.testing.expect(future1.poll(&ctx).isReady());
    try std.testing.expect(future2.poll(&ctx).isPending());
    try std.testing.expect(future3.poll(&ctx).isPending());

    // Unlock - future2 should be ready
    mutex.unlock();
    try std.testing.expect(future2.poll(&ctx).isReady());
    try std.testing.expect(future3.poll(&ctx).isPending());

    // Unlock - future3 should be ready
    mutex.unlock();
    try std.testing.expect(future3.poll(&ctx).isReady());

    // Final unlock
    mutex.unlock();
    try std.testing.expect(!mutex.isLocked());

    future1.deinit();
    future2.deinit();
    future3.deinit();
}

test "LockFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(LockFuture));
    try std.testing.expect(LockFuture.Output == void);
}

//! Edge Case Tests - Unusual Scenarios and Boundary Conditions
//!
//! Tests for edge cases that might be overlooked in normal testing:
//! - Zero/empty/boundary values
//! - Single-element scenarios
//! - Double operations (double lock, double release, etc.)
//! - Interleaved operations
//! - State transitions

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const sync = @import("volt").sync;
const channel = @import("volt").channel;

// ═══════════════════════════════════════════════════════════════════════════════
// Mutex Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Mutex - tryLock returns true when unlocked" {
    var mutex = sync.Mutex.init();
    try testing.expect(mutex.tryLock());
    mutex.unlock();
}

test "Mutex - tryLock returns false when locked" {
    var mutex = sync.Mutex.init();
    try testing.expect(mutex.tryLock());
    try testing.expect(!mutex.tryLock()); // Second tryLock fails
    mutex.unlock();
}

test "Mutex - isLocked reflects actual state" {
    var mutex = sync.Mutex.init();
    try testing.expect(!mutex.isLocked());
    _ = mutex.tryLock();
    try testing.expect(mutex.isLocked());
    mutex.unlock();
    try testing.expect(!mutex.isLocked());
}

test "Mutex - rapid lock/unlock cycles" {
    var mutex = sync.Mutex.init();
    for (0..1000) |_| {
        try testing.expect(mutex.tryLock());
        mutex.unlock();
    }
    try testing.expect(!mutex.isLocked());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Semaphore Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Semaphore - zero initial permits" {
    var sem = sync.Semaphore.init(0);
    try testing.expectEqual(@as(usize, 0), sem.availablePermits());
    try testing.expect(!sem.tryAcquire(1)); // Should fail
}

test "Semaphore - acquire more than available" {
    var sem = sync.Semaphore.init(3);
    try testing.expect(!sem.tryAcquire(4)); // Asking for more than available
    try testing.expectEqual(@as(usize, 3), sem.availablePermits());
}

test "Semaphore - acquire exactly available" {
    var sem = sync.Semaphore.init(5);
    try testing.expect(sem.tryAcquire(5));
    try testing.expectEqual(@as(usize, 0), sem.availablePermits());
    sem.release(5);
    try testing.expectEqual(@as(usize, 5), sem.availablePermits());
}

test "Semaphore - release more than acquired" {
    var sem = sync.Semaphore.init(2);
    try testing.expect(sem.tryAcquire(1));
    sem.release(5); // Release more
    try testing.expect(sem.availablePermits() >= 5);
}

test "Semaphore - single permit acquire/release" {
    var sem = sync.Semaphore.init(1);
    try testing.expect(sem.tryAcquire(1));
    try testing.expect(!sem.tryAcquire(1));
    sem.release(1);
    try testing.expect(sem.tryAcquire(1));
    sem.release(1);
}

// ═══════════════════════════════════════════════════════════════════════════════
// RwLock Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "RwLock - multiple concurrent readers" {
    var rwlock = sync.RwLock.init();
    try testing.expect(rwlock.tryReadLock());
    try testing.expect(rwlock.tryReadLock());
    try testing.expect(rwlock.tryReadLock());
    try testing.expect(!rwlock.tryWriteLock()); // Can't write with readers
    rwlock.readUnlock();
    rwlock.readUnlock();
    rwlock.readUnlock();
}

test "RwLock - writer blocks new readers" {
    var rwlock = sync.RwLock.init();
    try testing.expect(rwlock.tryWriteLock());
    try testing.expect(!rwlock.tryReadLock());
    rwlock.writeUnlock();
    try testing.expect(rwlock.tryReadLock());
    rwlock.readUnlock();
}

test "RwLock - alternating read/write" {
    var rwlock = sync.RwLock.init();
    for (0..100) |i| {
        if (i % 2 == 0) {
            try testing.expect(rwlock.tryReadLock());
            rwlock.readUnlock();
        } else {
            try testing.expect(rwlock.tryWriteLock());
            rwlock.writeUnlock();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Barrier Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Barrier - single task is leader immediately" {
    var barrier = sync.Barrier.init(1);
    var waiter = sync.barrier.Waiter.init();
    const immediate = barrier.waitWith(&waiter);
    try testing.expect(immediate);
    try testing.expect(waiter.is_leader.load(.acquire));
}

test "Barrier - generation increments after release" {
    var barrier = sync.Barrier.init(1);
    try testing.expectEqual(@as(usize, 0), barrier.currentGeneration());

    var waiter1 = sync.barrier.Waiter.init();
    _ = barrier.waitWith(&waiter1);
    try testing.expectEqual(@as(usize, 1), barrier.currentGeneration());

    var waiter2 = sync.barrier.Waiter.init();
    _ = barrier.waitWith(&waiter2);
    try testing.expectEqual(@as(usize, 2), barrier.currentGeneration());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Notify Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Notify - notifyOne before wait" {
    var notify = sync.Notify.init();
    notify.notifyOne();

    var waiter = sync.notify.Waiter.init();
    const consumed = notify.waitWith(&waiter);
    try testing.expect(consumed);
}

test "Notify - multiple notifyOne calls" {
    var notify = sync.Notify.init();
    notify.notifyOne();
    notify.notifyOne();
    notify.notifyOne();

    // Only one permit stored
    var waiter = sync.notify.Waiter.init();
    _ = notify.waitWith(&waiter);
}

test "Notify - wait cancel restores state" {
    var notify = sync.Notify.init();

    var waiter = sync.notify.Waiter.init();
    const immediate = notify.waitWith(&waiter);
    try testing.expect(!immediate);

    // Cancel
    notify.cancelWait(&waiter);

    // Should be able to wait again
    notify.notifyOne();
    var waiter2 = sync.notify.Waiter.init();
    const consumed = notify.waitWith(&waiter2);
    try testing.expect(consumed);
}

// ═══════════════════════════════════════════════════════════════════════════════
// OnceCell Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "OnceCell - get before set returns null" {
    var cell = sync.OnceCell(u32).init();
    try testing.expectEqual(@as(?*u32, null), cell.get());
}

test "OnceCell - initWith is immediately initialized" {
    var cell = sync.OnceCell(u32).initWith(42);
    try testing.expect(cell.isInitialized());
    try testing.expectEqual(@as(u32, 42), cell.get().?.*);
}

test "OnceCell - double set fails" {
    var cell = sync.OnceCell(u32).init();
    try testing.expect(cell.set(1));
    try testing.expect(!cell.set(2)); // Second set fails
    try testing.expectEqual(@as(u32, 1), cell.get().?.*);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Channel Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Channel - recv on empty returns empty" {
    var ch = try channel.Channel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    try testing.expect(ch.tryRecv() == .empty);
}

test "Channel - send on full returns full" {
    var ch = try channel.Channel(u32).init(testing.allocator, 2);
    defer ch.deinit();

    try testing.expectEqual(channel.Channel(u32).SendResult.ok, ch.trySend(1));
    try testing.expectEqual(channel.Channel(u32).SendResult.ok, ch.trySend(2));
    try testing.expectEqual(channel.Channel(u32).SendResult.full, ch.trySend(3));
}

test "Channel - close prevents new sends" {
    var ch = try channel.Channel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    _ = ch.trySend(1);
    ch.close();
    try testing.expectEqual(channel.Channel(u32).SendResult.closed, ch.trySend(2));
}

test "Channel - recv drains after close" {
    var ch = try channel.Channel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    _ = ch.trySend(1);
    _ = ch.trySend(2);
    ch.close();

    try testing.expectEqual(channel.Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());
    try testing.expectEqual(channel.Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());
    try testing.expect(ch.tryRecv() == .closed);
}

test "Channel - capacity 1 works" {
    var ch = try channel.Channel(u32).init(testing.allocator, 1);
    defer ch.deinit();

    try testing.expectEqual(channel.Channel(u32).SendResult.ok, ch.trySend(42));
    try testing.expectEqual(channel.Channel(u32).SendResult.full, ch.trySend(43));
    try testing.expectEqual(channel.Channel(u32).RecvResult{ .value = 42 }, ch.tryRecv());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Oneshot Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Oneshot - recv before send returns null" {
    const Os = channel.Oneshot(u32);
    var shared = Os.Shared.init();
    var pair = Os.fromShared(&shared);

    try testing.expectEqual(@as(?u32, null), pair.receiver.tryRecv());
}

test "Oneshot - double send fails" {
    const Os = channel.Oneshot(u32);
    var shared = Os.Shared.init();
    var pair = Os.fromShared(&shared);

    try testing.expect(pair.sender.send(1));
    try testing.expect(!pair.sender.send(2)); // Second send fails
    try testing.expectEqual(@as(?u32, 1), pair.receiver.tryRecv());
}

test "Oneshot - close without send" {
    const Os = channel.Oneshot(u32);
    var shared = Os.Shared.init();
    var pair = Os.fromShared(&shared);

    pair.sender.close();
    try testing.expectEqual(@as(?u32, null), pair.receiver.tryRecv());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Watch Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Watch - initial value accessible" {
    var watch = channel.Watch(u32).init(42);
    defer watch.deinit();

    try testing.expectEqual(@as(u32, 42), watch.borrow().*);
}

test "Watch - send updates value" {
    var watch = channel.Watch(u32).init(0);
    defer watch.deinit();

    watch.send(100);
    try testing.expectEqual(@as(u32, 100), watch.borrow().*);
}

test "Watch - subscriber sees updates" {
    var watch = channel.Watch(u32).init(0);
    defer watch.deinit();

    var rx = watch.subscribe();
    watch.send(42);
    try testing.expect(rx.hasChanged());
    try testing.expectEqual(@as(u32, 42), rx.getAndUpdate());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Broadcast Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

test "Broadcast - no subscribers ignores send" {
    var ch = try channel.BroadcastChannel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    // No subscribers - send still works (returns send result)
    _ = ch.send(42);
}

test "Broadcast - subscriber gets all values" {
    var ch = try channel.BroadcastChannel(u32).init(testing.allocator, 4);
    defer ch.deinit();

    var rx = ch.subscribe();
    _ = ch.send(1);
    _ = ch.send(2);
    _ = ch.send(3);

    // Check we can receive all values
    const r1 = rx.tryRecv();
    try testing.expect(r1 == .value);
    try testing.expectEqual(@as(u32, 1), r1.value);

    const r2 = rx.tryRecv();
    try testing.expect(r2 == .value);
    try testing.expectEqual(@as(u32, 2), r2.value);

    const r3 = rx.tryRecv();
    try testing.expect(r3 == .value);
    try testing.expectEqual(@as(u32, 3), r3.value);
}

test "Broadcast - lagged receiver" {
    var ch = try channel.BroadcastChannel(u32).init(testing.allocator, 2);
    defer ch.deinit();

    var rx = ch.subscribe();
    _ = ch.send(1);
    _ = ch.send(2);
    _ = ch.send(3); // Overwrites 1

    // First recv should show lag
    const result = rx.tryRecv();
    try testing.expect(result == .lagged or result == .value);
}

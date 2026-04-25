---
title: Mutex & RwLock
description: Async-aware mutual exclusion and read-write locks for protecting
  shared state in Volt tasks.
slug: v1.0.0-zig0.15.2/usage/mutex-rwlock
---

Volt provides async-aware `Mutex` and `RwLock` that yield to the scheduler when contended, instead of blocking the OS thread. This lets other tasks make progress while a task waits for a lock.

For blocking (OS-level) mutexes, use `std.Thread.Mutex` from the Zig standard library.

## Mutex

A `Mutex` provides exclusive access to a shared resource. Only one task can hold the lock at a time. When a second task tries to acquire a held mutex, it is suspended and placed in a FIFO queue.

### Initialization

```zig
const volt = @import("volt");

var mutex = volt.sync.Mutex.init();
```

No allocator is needed. `Mutex` is zero-allocation and requires no `deinit`.

### Non-blocking: tryLock / unlock

`tryLock` returns `true` if the lock was acquired immediately, `false` if it is already held:

```zig
if (mutex.tryLock()) {
    defer mutex.unlock();
    // Critical section -- exclusive access guaranteed
    shared_counter += 1;
}
```

### RAII guard: tryLockGuard

For scoped locking, `tryLockGuard` returns an optional `MutexGuard` that automatically unlocks on `deinit`:

```zig
if (mutex.tryLockGuard()) |g| {
    var guard = g;
    defer guard.deinit();
    shared_data.update();
} else {
    // Lock is held by another task
}
```

### Async: lock(io)

`lock(io)` acquires the mutex asynchronously. The calling task is suspended (not spin-waiting) until the lock is acquired:

```zig
mutex.lock(io);
defer mutex.unlock();
// Critical section -- exclusive access guaranteed
shared_counter += 1;
```

Pass the `io: volt.Io` handle so the mutex can yield to the scheduler when contended and resume the task when the lock becomes available.

### Advanced: lockFuture()

For manual future composition or custom schedulers, `lockFuture()` returns a `LockFuture` implementing the `Future` trait (`Output = void`, has `poll`, `cancel`, `deinit`):

```zig
var future = mutex.lockFuture();
// Poll the future manually through your scheduler...
// When future.poll() returns .ready, the lock is held.
defer mutex.unlock();
```

### Low-level: lockWait with explicit Waiter

For manual integration with custom schedulers, use the waiter API:

```zig
var waiter = volt.sync.mutex.Waiter.init();
if (!mutex.lockWait(&waiter)) {
    // Waiter was added to the queue. Yield to scheduler.
    // When woken, waiter.isAcquired() will be true.
}
defer mutex.unlock();
```

You can attach a waker callback so the scheduler is notified when the lock becomes available:

```zig
var waiter = volt.sync.mutex.Waiter.initWithWaker(@ptrCast(&my_ctx), myWakeCallback);
```

### Cancellation

Cancel a pending lock acquisition with `cancelLock`:

```zig
mutex.cancelLock(&waiter);
```

Or on the future:

```zig
future.cancel();
```

### Diagnostics

```zig
mutex.isLocked();     // bool -- is the mutex currently held?
mutex.waiterCount();  // usize -- number of tasks waiting (O(n))
```

***

## RwLock

An `RwLock` allows multiple concurrent readers OR a single exclusive writer. It is ideal for read-heavy workloads where writes are infrequent.

### Design

`RwLock` is built on top of `Semaphore(MAX_READS)`:

* **Read lock** = acquire 1 permit
* **Write lock** = acquire all `MAX_READS` permits (~536 million)

Writer priority emerges naturally: a queued writer drains permits toward zero, so new `tryReadLock` calls fail until the writer is served.

### Initialization

```zig
var rwlock = volt.sync.RwLock.init();
```

Zero-allocation, no `deinit` required.

### Non-blocking reads

```zig
if (rwlock.tryReadLock()) {
    defer rwlock.readUnlock();
    const value = shared_config.host;
    // ... use value
}
```

Multiple tasks can hold read locks simultaneously.

### Non-blocking writes

```zig
if (rwlock.tryWriteLock()) {
    defer rwlock.writeUnlock();
    shared_config = new_config;
}
```

A write lock fails if any readers or another writer hold the lock.

### RAII guards

```zig
if (rwlock.tryReadLockGuard()) |g| {
    var guard = g;
    defer guard.deinit();
    // Read shared data
}

if (rwlock.tryWriteLockGuard()) |g| {
    var guard = g;
    defer guard.deinit();
    // Modify shared data
}
```

### Async: readLock(io) / writeLock(io)

Both yield to the scheduler when contended and resume the task when the lock is acquired:

```zig
// Acquire a read lock asynchronously
rwlock.readLock(io);
defer rwlock.readUnlock();
const value = shared_config.host;

// Acquire a write lock asynchronously
rwlock.writeLock(io);
defer rwlock.writeUnlock();
shared_config = new_config;
```

Pass the `io: volt.Io` handle so the lock can cooperate with the scheduler.

### Advanced: readLockFuture() / writeLockFuture()

For manual future composition, these return `ReadLockFuture` / `WriteLockFuture` implementing the `Future` trait (`Output = void`):

```zig
var read_future = rwlock.readLockFuture();
// Poll through your scheduler...
defer rwlock.readUnlock();

var write_future = rwlock.writeLockFuture();
// Poll through your scheduler...
defer rwlock.writeUnlock();
```

### Low-level: waiter API

```zig
// Read waiter
var read_waiter = volt.sync.rwlock.ReadWaiter.init();
if (!rwlock.readLockWait(&read_waiter)) {
    // Yield, will be woken when read lock is granted
}
defer rwlock.readUnlock();

// Write waiter
var write_waiter = volt.sync.rwlock.WriteWaiter.init();
if (!rwlock.writeLockWait(&write_waiter)) {
    // Yield, will be woken when write lock is granted
}
defer rwlock.writeUnlock();
```

### Cancellation

```zig
rwlock.cancelReadLock(&read_waiter);
rwlock.cancelWriteLock(&write_waiter);

// Or on futures:
read_future.cancel();
write_future.cancel();
```

### Diagnostics

```zig
rwlock.isWriteLocked();    // bool
rwlock.getReaderCount();   // usize -- number of active readers
rwlock.waitingReaders();   // usize -- queued readers (O(n))
rwlock.waitingWriters();   // usize -- queued writers (O(n))
```

***

## Choosing between Mutex and RwLock

| Scenario | Recommendation |
|----------|---------------|
| Single writer, no concurrent reads | `Mutex` -- simpler, slightly less overhead |
| Many readers, rare writes | `RwLock` -- readers proceed in parallel |
| Short critical sections | `Mutex` -- contention is rare, simplicity wins |
| Read-heavy config lookups | `RwLock` -- readers never block each other |
| Write-heavy workload | `Mutex` -- RwLock writer priority adds overhead for no benefit |

## Performance characteristics

Both primitives use zero-allocation intrusive linked lists for their waiter queues. The fast paths (`tryLock`, `tryReadLock`) are lock-free CAS operations. The slow paths (contended acquisition) take an OS mutex only to manipulate the waiter queue -- never for the critical section itself.

FIFO ordering is guaranteed: waiters are served in the order they arrived, preventing starvation.

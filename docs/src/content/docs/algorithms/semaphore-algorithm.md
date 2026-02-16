---
title: Counting Semaphore Algorithm
description: The batch semaphore algorithm with direct-handoff used in Volt, designed to prevent permit starvation under contention.
---

Volt's `Semaphore` is a counting semaphore that limits concurrent access to a resource. Tasks can acquire N permits (decrement) and release N permits (increment). When permits are unavailable, tasks yield to the scheduler rather than blocking the OS thread. The Mutex and RwLock primitives are built on top of the Semaphore.

The algorithm was carefully designed to prevent **permit starvation** -- a subtle bug where permits "slip through" between a failed CAS and waiter queue insertion, causing waiters to starve indefinitely.

## The Starvation Problem

Consider a naive semaphore implementation:

```
Thread A (acquirer):                Thread B (releaser):
1. CAS: try subtract N permits
   -> fails, not enough
2.                                   3. release(N): add N permits to atomic
4. lock mutex, add to waiter queue
5. unlock mutex, sleep
```

Between steps 1 and 4, Thread B releases permits (step 3). Those permits go to the atomic counter. Thread A does not see them because it already decided to wait. The permits are now "floating" in the atomic while a waiter sits in the queue. If all subsequent `release()` calls follow the same pattern (check waiters, find none yet queued, add to atomic), the waiter starves.

## The Volt Solution: Lock-Before-CAS

Volt eliminates the race by acquiring the mutex **before** the CAS that drains remaining permits in the slow path. Since `release()` also always takes the mutex, there is no window where permits can slip through.

```
                   acquireWait()                       release()
               +-----------------------+          +------------------+
Fast path:     | CAS for full amount   |          |                  |
               | (lock-free, no mutex) |          |                  |
               +-----------+-----------+          |                  |
                           |                      |                  |
                     (insufficient)               |                  |
                           |                      |                  |
               +-----------v-----------+          |                  |
Slow path:     | Lock mutex            |          | Lock mutex       |
               | CAS to drain what's   |          | If waiters:      |
               |   available           |          |   serve directly |
               | If still not enough:  |          | If no waiters:   |
               |   add waiter to queue |          |   add to atomic  |
               | Unlock mutex          |          | Unlock mutex     |
               +-----------------------+          +------------------+
```

**Key invariant:** Permits NEVER float in the atomic counter when waiters are queued. `release()` serves waiters directly; only surplus permits go to the atomic.

## Data Structures

The semaphore uses three components:

```zig
pub const Semaphore = struct {
    /// Available permits (modified under mutex OR via CAS)
    permits: std.atomic.Value(usize),

    /// Protects the waiter list. Taken by release() and acquireWait()'s slow path.
    mutex: std.Thread.Mutex,

    /// FIFO waiter queue (push front, serve from back)
    waiters: WaiterList,
};
```

Each waiter tracks how many permits it still needs:

```zig
pub const Waiter = struct {
    /// Remaining permits needed (starts at requested, decrements to 0)
    state: std.atomic.Value(usize),

    /// Original request (for cancellation math)
    permits_requested: usize,

    /// Wake callback
    waker: ?WakerFn,
    waker_ctx: ?*anyopaque,

    /// Intrusive list pointers (zero allocation)
    pointers: Pointers(Waiter),
};
```

Waiters are intrusive -- they are embedded in the `AcquireFuture` struct that the caller owns. No heap allocation is needed per wait operation.

## The tryAcquire Fast Path

For the uncontended case, `tryAcquire` is a simple lock-free CAS loop with no mutex involvement:

```zig
pub fn tryAcquire(self: *Self, num: usize) bool {
    var current = self.permits.load(.acquire);
    while (true) {
        if (current < num) return false;

        if (self.permits.cmpxchgWeak(current, current - num, .acq_rel, .acquire)) |new_val| {
            current = new_val;
            continue;
        }
        return true;
    }
}
```

This is the common case for an uncontended semaphore and compiles down to a tight CAS loop with no function calls.

## The acquireWait Algorithm

When `tryAcquire` fails and the caller is willing to wait:

```zig
pub fn acquireWait(self: *Self, waiter: *Waiter) bool {
    const needed = waiter.state.load(.acquire);

    // === Fast path: lock-free CAS for full acquisition ===
    var curr = self.permits.load(.acquire);
    while (curr >= needed) {
        if (self.permits.cmpxchgWeak(curr, curr - needed, .acq_rel, .acquire)) |updated| {
            curr = updated;
            continue;
        }
        waiter.state.store(0, .release);
        return true;  // Got all permits, no mutex needed
    }

    // === Slow path: lock BEFORE CAS ===
    self.mutex.lock();

    // Under lock: drain available permits via CAS
    var acquired: usize = 0;
    curr = self.permits.load(.acquire);
    while (acquired < needed and curr > 0) {
        const take = @min(curr, needed - acquired);
        if (self.permits.cmpxchgWeak(curr, curr - take, .acq_rel, .acquire)) |updated| {
            curr = updated;
            continue;
        }
        acquired += take;
        curr = self.permits.load(.acquire);
    }

    if (acquired >= needed) {
        waiter.state.store(0, .release);
        self.mutex.unlock();
        return true;  // Got all permits under lock
    }

    // Partially fulfilled -- update waiter state and queue
    if (acquired > 0) {
        var rem = acquired;
        _ = waiter.assignPermits(&rem);
    }

    self.waiters.pushFront(waiter);  // FIFO: push front, serve from back
    self.mutex.unlock();
    return false;  // Caller must yield
}
```

The CAS in the slow path is still needed because `tryAcquire()` can concurrently succeed (it is lock-free). But the critical property is that we hold the mutex when we decide to queue the waiter. Since `release()` also holds the mutex when checking the waiter queue, the race window is closed.

## The release Algorithm

`release()` **always** takes the mutex, even when there are no waiters. This is the key to starvation prevention:

```zig
pub fn release(self: *Self, num: usize) void {
    if (num == 0) return;

    self.mutex.lock();

    if (self.waiters.back() == null) {
        // No waiters -- surplus goes to atomic
        _ = self.permits.fetchAdd(num, .release);
        self.mutex.unlock();
        return;
    }

    // Serve queued waiters directly
    self.releaseToWaiters(num);
}
```

When waiters are present, permits are handed directly to them rather than going through the atomic counter:

```zig
fn releaseToWaiters(self: *Self, num: usize) void {
    var wake_list: WakeList = .{};
    var rem = num;

    // Serve waiters from the back (oldest first = FIFO)
    while (rem > 0) {
        const waiter = self.waiters.back() orelse break;

        const remaining = waiter.state.load(.monotonic);  // Under lock
        if (remaining == 0) {
            _ = self.waiters.popBack();
            // Collect waker for batch wake after unlock
            if (waiter.waker) |wf| {
                if (waiter.waker_ctx) |ctx| {
                    wake_list.push(.{ .context = ctx, .wake_fn = wf });
                }
            }
            continue;
        }

        const assign = @min(rem, remaining);
        waiter.state.store(remaining - assign, .release);
        rem -= assign;

        if (remaining - assign == 0) {
            _ = self.waiters.popBack();
            // Collect waker
            if (waiter.waker) |wf| {
                if (waiter.waker_ctx) |ctx| {
                    wake_list.push(.{ .context = ctx, .wake_fn = wf });
                }
            }
        } else {
            break;  // Waiter partially satisfied, no more permits
        }
    }

    // Only surplus permits go to atomic
    if (rem > 0) {
        _ = self.permits.fetchAdd(rem, .release);
    }

    self.mutex.unlock();

    // Wake AFTER releasing the lock
    wake_list.wakeAll();
}
```

Important details:

1. **FIFO ordering**: Waiters are pushed to the front and served from the back, ensuring fairness.
2. **Batch waking**: Wakers are collected into a `WakeList` and invoked after the mutex is released. This prevents holding the lock during potentially expensive wake operations.
3. **Partial fulfillment**: A waiter requesting N permits may receive them incrementally across multiple `release()` calls. The `waiter.state` tracks remaining needed permits.
4. **Direct handoff**: Permits go directly from the releaser to the waiter without passing through the atomic counter. This is what prevents starvation.

## Cancellation

When a future is cancelled (e.g., via `select` or timeout), any partially acquired permits must be returned:

```zig
pub fn cancelAcquire(self: *Self, waiter: *Waiter) void {
    if (waiter.isComplete()) return;

    self.mutex.lock();
    if (waiter.isComplete()) { self.mutex.unlock(); return; }

    // Remove from list
    self.waiters.remove(waiter);
    waiter.pointers.reset();

    const remaining = waiter.state.load(.monotonic);
    const acquired = waiter.permits_requested - remaining;
    self.mutex.unlock();

    // Return partial permits (may wake other waiters!)
    if (acquired > 0) {
        self.release(acquired);
    }
}
```

The partial-permit return calls `release()`, which may wake other waiters that were blocked. This ensures cancellation does not leak permits.

## The Mutex as Semaphore(1)

The Volt Mutex is implemented as a single-permit semaphore:

```zig
pub const Mutex = struct {
    sem: Semaphore,

    pub fn init() Mutex {
        return .{ .sem = Semaphore.init(1) };
    }

    pub fn tryLock(self: *Self) bool {
        return self.sem.tryAcquire(1);
    }

    pub fn unlock(self: *Self) void {
        self.sem.release(1);
    }
};
```

This means the Mutex inherits all of the Semaphore's correctness guarantees: starvation prevention, FIFO ordering, safe cancellation, and zero-allocation waiters.

## Performance Characteristics

| Operation | Uncontended | Contended |
|-----------|-------------|-----------|
| tryAcquire | Single CAS (~5-10ns) | CAS retry loop |
| acquireWait (fast path) | Single CAS (~5-10ns) | Falls to slow path |
| acquireWait (slow path) | Mutex + CAS (~50ns) | Mutex + queue (~100ns) |
| release (no waiters) | Mutex + fetchAdd (~30ns) | Same |
| release (with waiters) | Mutex + direct handoff (~50ns) | Mutex + wake (~100ns) |

The "always lock in release" design adds ~20-30ns to the uncontended release path compared to a lock-free release. This tradeoff was chosen deliberately: correctness under contention is more important than saving a few nanoseconds in the uncontended case. The mutex is a simple `std.Thread.Mutex` (typically a futex on Linux), which is extremely fast when uncontended.

### Source Files

- Semaphore: `src/sync/Semaphore.zig`
- Mutex (built on Semaphore): `src/sync/Mutex.zig`

## References

- Edsger W. Dijkstra, "Cooperating Sequential Processes," *Programming Languages*, NATO Advanced Study Institute, 1968. [https://www.cs.utexas.edu/~EWD/transcriptions/EWD01xx/EWD123.html](https://www.cs.utexas.edu/~EWD/transcriptions/EWD01xx/EWD123.html)
- Tokio semaphore implementation: [`tokio/src/sync/batch_semaphore.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/batch_semaphore.rs)
- Maurice Herlihy and Nir Shavit, *The Art of Multiprocessor Programming*, Morgan Kaufmann, 2012 — Chapter 8 (monitors and blocking synchronization).

---
title: Semaphore (FIFO with N-permit fairness)
description: How volt.sync.Semaphore handles multi-permit acquires while preserving FIFO fairness.
---

A counting semaphore is conceptually simple — `acquire(n)`
decrements a counter by `n` if there's enough; otherwise blocks.
The interesting part is what "fair" means when N-permit waiters
can race with 1-permit waiters.

Volt's semaphore is **strict FIFO with N-permit fairness**: the
head of the waiter list goes first, even if it asks for more
permits than a later waiter would.

## The state

```zig
permits: AtomicU32                 // current available permits
waiter_mutex: Mutex                // guards the waiter list
waiters: WaiterList                // FIFO list of (waiter, requested_n)
```

A `Waiter` is a small struct on the requesting coroutine's stack:

```zig
const Waiter = struct {
    park: Park = .{},
    wanted: u32 = 0,
    granted: bool = false,
    next: ?*Waiter = null,
};
```

## Fast path: tryAcquire

Lock-free CAS on `permits`:

```zig
pub fn tryAcquire(self: *Semaphore, n: u32) bool {
    var current = self.permits.load(.acquire);
    while (true) {
        if (current < n) return false;
        if (self.permits.cmpxchgWeak(current, current - n, .acq_rel, .acquire)) |obs| {
            current = obs;
            continue;
        }
        return true;
    }
}
```

Single atomic in the uncontended case. The CAS loop handles
concurrent acquirers — one of them succeeds, the others retry.

## Slow path: acquire

`acquire(n)` calls `tryAcquire(n)` first. On false, it goes to
the slow path:

```zig
pub fn acquire(self: *Semaphore, n: u32) void {
    if (self.tryAcquire(n)) return;

    var waiter: Waiter = .{ .wanted = n };
    self.waiter_mutex.lock();

    // Re-check under the slow-path mutex. If permits became
    // available AND no waiters are ahead of us, grab them now —
    // we have priority over racing fast-path acquirers because
    // we're under the slow-path mutex.
    if (self.waiters.isEmpty() and self.tryAcquire(n)) {
        self.waiter_mutex.unlock();
        return;
    }

    self.waiters.pushBack(&waiter);
    self.waiter_mutex.unlock();

    while (true) {
        waiter.park.parkCurrent() catch {
            // Cancelled — remove from list, return.
            self.waiter_mutex.lock();
            if (waiter.granted) {
                // Releaser already handed us permits; honor the grant.
                self.waiter_mutex.unlock();
                return;
            }
            _ = self.removeWaiterLocked(&waiter);
            self.waiter_mutex.unlock();
            return;
        };
        if (@atomicLoad(bool, &waiter.granted, .acquire)) return;
        // Spurious wake — re-park.
    }
}
```

The waiter struct is on the calling coroutine's stack. It lives
until `acquire` returns. The releaser will set
`waiter.granted = true` and unpark when permits become available
to this specific waiter.

## Release: serve the head FIFO

```zig
pub fn release(self: *Semaphore, n: u32) void {
    _ = self.permits.fetchAdd(n, .release);

    self.waiter_mutex.lock();
    var to_wake: ?*Waiter = null;
    var to_wake_tail: ?*Waiter = null;

    // Wake as many head waiters as can be satisfied.
    while (self.waiters.head) |head| {
        if (self.tryAcquire(head.wanted)) {
            _ = self.waiters.popFront();
            @atomicStore(bool, &head.granted, true, .release);
            // Build a separate list to wake AFTER releasing the mutex.
            head.next = null;
            if (to_wake_tail) |t| t.next = head else to_wake = head;
            to_wake_tail = head;
        } else {
            // Head can't be satisfied. Don't skip ahead — that would
            // violate FIFO and let smaller-N waiters jump the queue.
            break;
        }
    }
    self.waiter_mutex.unlock();

    // Unpark outside the lock to minimize hold time.
    var cur = to_wake;
    while (cur) |w| {
        const next = w.next;
        w.next = null;
        w.park.unpark();
        cur = next;
    }
}
```

The key line is `break` when the head can't be satisfied. A
naive implementation might skip ahead to the next waiter that
*could* fit — but that violates FIFO. Strict FIFO means: if the
head asks for 4 permits and only 3 are available, every waiter
behind it must wait, even waiters that only need 1 permit.

## Why strict FIFO?

The alternative is "best-fit FIFO" — wake whoever fits, in
arrival order. That increases throughput when permits are
fragmented, but it allows starvation: a 4-permit waiter could
wait forever if the queue keeps producing 1-permit waiters who
slip in past it.

Strict FIFO is what users typically expect from a Semaphore. The
parking_lot in Rust does the same. Throughput-tuned alternatives
(Tokio's tokio::sync::Semaphore is best-fit) make different
tradeoffs.

## What happens on cancel mid-wait

A cancelled coroutine wakes with `error.Cancelled`. The acquire
path catches that:

```zig
waiter.park.parkCurrent() catch {
    self.waiter_mutex.lock();
    if (waiter.granted) {
        // Releaser already gave us permits — keep them; the caller
        // observed cancellation already on entry.
        self.waiter_mutex.unlock();
        return;
    }
    _ = self.removeWaiterLocked(&waiter);
    self.waiter_mutex.unlock();
    return;
};
```

If the releaser hadn't gotten to us yet, we remove ourselves from
the queue cleanly. If the releaser HAD just granted permits to us,
we honor the grant — those permits are ours, the caller is
responsible for `release`ing them. The `acquire` API has no error
union, so the caller can't directly observe a cancel-during-wait;
the next `volt.yield()` or suspending call will surface it.

## Why no separate "fairness mode" config

`Semaphore.init(n)` is one way: strict FIFO. If you want
unfair-but-fast, you can build it with:

```zig
var permits: std.atomic.Value(u32) = .{ .raw = 8 };
// trySemaphoreLike-acquire/release with bare CAS, no waiter list,
// busy-yield on contention.
```

That's ~10 lines and you lose blocking, which is fine for some
workloads. Volt's Semaphore is the well-defined "fair, blocks on
contention" tool; if you need something else, build it.

## File

`src/sync/Semaphore.zig`. ~200 lines including tests.

The wake protocol mirrors `Mutex` and `RwLock` — same shape:
fast-path CAS + slow-path waiter mutex + park.

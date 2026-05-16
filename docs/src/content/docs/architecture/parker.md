---
title: Park
description: The single-atomic suspension primitive that every blocking operation in Volt is built on.
---

`Park` is Volt's universal "suspend until told otherwise"
primitive. Every blocking operation in the runtime — channel
recv, mutex lock, sleep, I/O wait, join — uses Park internally.
The runtime is small partly because there's only one suspension
mechanism to get right.

## The state

```zig
pub const Park = struct {
    state: AtomicUsize = .{ .raw = 0 },
};
```

A single atomic word. Three meaningful values:

- `0` — idle. No waiter, no notify.
- `pointer` — a coroutine pointer, low bits clear (alignment > 4).
  Means: "this coroutine is parked here; wake it on notify."
- `NOTIFIED` (= `1`) — a notify arrived before any waiter parked.
  The next `parkCurrent` returns immediately.

The state machine:

```
                          ┌─── parkCurrent ───┐
                          │                   │
                          ▼                   │
       ┌────────────────────────┐              │
       │   IDLE (state == 0)    │              │
       └────────┬───────┬───────┘              │
                │       │                       │
        unpark  │       │   parkCurrent        │
                │       │                       │
                ▼       ▼                       │
       ┌──────────┐  ┌──────────────────┐      │
       │ NOTIFIED │  │ PARKED           │      │
       │ (== 1)   │  │ (state == coro*) │──────┘
       └────┬─────┘  └────────┬─────────┘  (resume,
            │                 │             return ok)
   parkCurrent              unpark
   (consume,                  │
    return ok)                ▼
            │         ┌──────────────────┐
            └────────►│   schedule(coro) │
                      └──────────────────┘
```

The high bit of the pointer encoding doubles as the NOTIFIED bit
(distinguishable because real coroutine pointers are aligned and
never equal `1`).

## parkCurrent

```zig
pub fn parkCurrent(self: *Park) error{Cancelled}!void {
    const coro = currentCoroutine();
    if (coro.isCancelled()) return error.Cancelled;

    const ptr = @intFromPtr(coro);

    // Try to install our pointer.
    const observed = self.state.cmpxchgStrong(0, ptr, .acq_rel, .acquire);

    if (observed == NOTIFIED) {
        // Someone notified before we could park. Reset and return.
        self.state.store(0, .release);
        return;
    }

    // Park: register the Park as our current_park (for cancel
    // propagation), then suspend.
    coro.current_park.store(@intFromPtr(self), .release);
    swapToScheduler(coro);

    // Resumed.
    coro.current_park.store(0, .release);
    if (coro.isCancelled()) return error.Cancelled;
    return;
}
```

The `current_park` registration is what makes Park
*cancellable from anywhere*. When you call `Job.cancel()`:

```zig
pub fn cancel(self: *Coroutine) void {
    self.cancel_flag.store(true, .release);
    const park_addr = self.current_park.load(.acquire);
    if (park_addr != 0) {
        const park: *Park = @ptrFromInt(park_addr);
        park.unpark();
    }
}
```

Cancel sets the flag AND unparks whatever Park the coroutine is
currently parked on. The Park-cancel routes through the same
suspension machinery as a normal wake; the resumed coroutine
checks `isCancelled()` post-resume and surfaces `error.Cancelled`.

This is how `volt.withTimeout` works through arbitrary nested
suspensions. The watcher cancels the child; the child's current
Park unparks; the cancel surfaces from whatever blocking call it
was in (sleep, I/O wait, channel, mutex…). One mechanism, applies
uniformly.

## unpark

```zig
pub fn unpark(self: *Park) void {
    const observed = self.state.swap(NOTIFIED, .acq_rel);

    // Three cases:
    if (observed == 0) return;                     // no waiter; flag set
    if (observed == NOTIFIED) return;              // already notified; idempotent

    // observed is a coroutine pointer; schedule it.
    const coro: *Coroutine = @ptrFromInt(observed);
    scheduleCoro(coro);
}
```

`unpark` is idempotent — calling it twice with no `parkCurrent`
between just leaves the NOTIFIED flag set. The next
`parkCurrent` consumes it and returns immediately.

This is the "notify before wait" race: producer calls `unpark`
before consumer has reached `parkCurrent`. Park handles it
correctly without any special logic in the consumer — the
consumer just observes NOTIFIED and returns.

## Why a single atomic?

Park's value comes from being **the one suspension primitive**.
If channels had their own park-with-waiter-list and mutexes had
their own and sleep had its own, cancel would need to know about
each of them and unpark them appropriately. With one Park type,
cancel just unparks the current Park. The complexity disappears.

## Building blocking primitives on Park

Most Volt primitives use Park as their suspension mechanism. The
recipe:

```
1. Try fast path (CAS, lock-free check).
2. On fast-path failure, prepare to park:
   - Take a slow-path mutex.
   - Re-check fast path (in case state changed during the lock).
   - Enqueue a waiter struct (containing a Park) into the
     primitive's waiter list.
   - Release the slow-path mutex.
3. parkCurrent. Suspend.
4. On resume, check what woke us:
   - error.Cancelled? Remove from waiter list, return.
   - Spurious wake? Re-check the primitive's state and re-park
     if still blocked.
   - Genuine wake from a notifier? Return.
```

Notifier side:

```
1. Update primitive's state (free a slot, drop a lock, fire a
   value, etc.).
2. Take the slow-path mutex.
3. Pop a waiter (or all waiters) from the list.
4. Release the mutex.
5. Call park.unpark() on each popped waiter.
```

The mutex protects the waiter list; the Park itself doesn't need
external synchronization (it's lock-free internally).

## Example: Notify

`volt.sync.Notify` is almost the simplest possible Park-based
primitive:

```zig
pub const Notify = struct {
    park: Park = .{},
    notified_count: AtomicU32 = .{ .raw = 0 },

    pub fn wait(self: *Notify) error{Cancelled}!void {
        // Fast path: a notify is already pending.
        if (self.notified_count.load(.acquire) > 0) {
            _ = self.notified_count.fetchSub(1, .acq_rel);
            return;
        }
        // Park.
        try self.park.parkCurrent();
    }

    pub fn notifyOne(self: *Notify) void {
        _ = self.notified_count.fetchAdd(1, .release);
        self.park.unpark();
    }
};
```

(The real implementation has a waiter list for `notifyAll`; this
is a simplification.)

## Why not std.Thread.Condition?

`std.Thread.Condition` blocks the OS thread, which would block a
Volt worker. Volt's Park blocks only the coroutine — the worker
keeps running other coroutines. The whole point.

For the underlying OS-level blocking that Park resolves to (when
all workers idle), Volt uses `std.Thread.Condition` indirectly
via its internal `thread.Mutex` + `thread.Condition`. The worker
parks, the OS thread sleeps, the kernel notifies on signal.

## File

`src/scheduler/park.zig`. ~150 lines. Read it; it's the most
load-bearing file in the runtime.

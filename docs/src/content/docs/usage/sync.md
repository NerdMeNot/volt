---
title: Sync Primitives
description: Mutex, RwLock, Semaphore, Notify, Barrier, OnceCell — the locks and signals under volt.sync.*.
---

All sync primitives in `volt.sync.*` are zero-allocation, built on
the single `Park` substrate, and cancellable from any park (cancel
propagates into `error.Cancelled` at the next suspension point).

## Mutex — fair FIFO

```zig
var mu: volt.sync.Mutex = .{};

mu.lock();
defer mu.unlock();
// critical section
```

Or non-blocking:

```zig
if (mu.tryLock()) {
    defer mu.unlock();
    // critical section
} else {
    // contended; do something else
}
```

Fair FIFO waiter list — the longest-waiting coroutine is the first
to acquire the lock when the holder unlocks. This prevents
starvation under contention but does mean lock acquisition is
roughly the cost of one Park per contended call.

### Pattern: hold across suspension

You can hold a Mutex across a suspending call, but only do it
intentionally:

```zig
mu.lock();
defer mu.unlock();
const v = try ch.recv();   // suspends WHILE HOLDING the lock!
```

This is sometimes correct (you genuinely need the lock held while
you're waiting on a channel), but usually a bug. The pattern that
usually wants:

```zig
const v = try ch.recv();
mu.lock();
defer mu.unlock();
applyValue(v);
```

## RwLock — reader-writer, writer-priority

```zig
var rw: volt.sync.RwLock = .{};

// Many readers, none excluded:
rw.lockShared();
defer rw.unlockShared();
const v = state.someField;

// One writer, all readers excluded:
rw.lockExclusive();
defer rw.unlockExclusive();
state.someField = newValue;
```

`tryLockShared` / `tryLockExclusive` return `bool` for non-blocking
attempts.

Implementation note: the RwLock is built on a `Semaphore` with
writer-priority — when a writer is waiting, no new readers are
admitted. This prevents the classic "writer never gets a chance"
starvation pattern under heavy read load.

Use `RwLock` when:

- Reads are far more frequent than writes (>10:1 ratio is the rough
  rule).
- The critical section is large enough that the bookkeeping cost is
  amortized.

For small critical sections (a few field reads), a plain `Mutex` is
typically faster — RwLock's bookkeeping costs more than a contended
Mutex acquire.

## Semaphore — counting permits

```zig
var sem = volt.sync.Semaphore.init(8);   // 8 permits

sem.acquire(1);
defer sem.release(1);
// ... work bounded to 8 concurrent ...
```

Common patterns:

- **Connection pool**: `Semaphore.init(pool_size)`; each connection
  acquire/release pair gates pool checkout.
- **Rate limiter**: `Semaphore.init(burst)`; a separate timer
  releases `rate` permits per interval.
- **Bounded concurrency**: `Semaphore.init(max_concurrent)` around
  any expensive operation.

Non-blocking and multi-permit forms:

```zig
if (sem.tryAcquire(2)) {
    defer sem.release(2);
    // ...
}

const n: u32 = sem.available();   // permits currently free (snapshot)
```

The waiter list is FIFO and respects the requested-N: if the head
waiter wants 4 permits and only 3 are available, no smaller waiter
behind it gets to skip ahead. This is the parking_lot fairness
contract.

## Notify — condition variable

```zig
var n: volt.sync.Notify = .{};

// Producer:
producePayload();
n.notifyOne();   // wake one waiter, if any

// Consumer:
try n.wait();    // suspend until notify; error.Cancelled if cancelled
```

`notifyAll()` wakes every parked waiter at once. `notifyOne()` is
the typical choice for single-consumer-per-event patterns.

Notify handles the notify-before-wait race correctly: if `notifyOne`
fires before the consumer parks, the next `wait` returns
immediately. Internally that's a single counter increment +
condition check.

Notify does NOT have a Mutex baked in (unlike `std.Thread.Condition`).
That's deliberate — Volt's coroutine model means you can express
"wait, holding mutex" by just locking the mutex *after* `wait()`
returns. The pattern:

```zig
// Producer:
mu.lock();
buffer.push(item);
mu.unlock();
n.notifyOne();

// Consumer:
try n.wait();
mu.lock();
const item = buffer.pop();
mu.unlock();
```

If you need the cond-var semantic where wait+unlock+relock is
atomic with the wait, that's a Volt-shape that doesn't exist —
because Volt doesn't have the spurious-wakeup problem
condition-variables exist to solve. Park doesn't spuriously wake.

## Barrier — N-task sync point

```zig
var b = volt.sync.Barrier.init(4);

// Each of 4 coroutines:
switch (b.wait()) {
    .leader => {
        // exactly one of the 4 sees this per round
    },
    .follower => {},
}
```

`Barrier.wait()` blocks until N coroutines have called it. The
last to arrive is the leader; the rest are followers. The barrier
auto-resets for the next round.

Use when:

- N parallel pipelines need to sync at a checkpoint between phases.
- One special coroutine should do "leader" work (e.g., aggregate
  partial results) while siblings wait.

`Barrier.wait` is non-cancellable: once you've entered the sync
point, breaking out via cancel would desynchronize peers, so cancel
returns the cancelled coroutine as `.follower` rather than
unwinding it.

## OnceCell(T) — lazy one-time init

```zig
var cell = volt.sync.OnceCell([]const u8){};

// Cheap repeated check:
if (cell.get()) |v| return v;

// First-call-wins init:
const v = try cell.getOrInit(allocator, struct {
    fn init(a: std.mem.Allocator) ![]const u8 {
        return try loadConfig(a);
    }
}.init);
```

`get()` returns `?T` — the snapshot if initialized, else `null`.
`getOrInit(alloc, init_fn)` runs `init_fn(alloc)` exactly once
across all callers and returns the result; concurrent callers
serialize on the init.

Use for:

- Lazy expensive global config / connection / cache.
- Constants computed at first use.

The internal state is `empty / initializing / initialized` — three
states, single CAS for transitions. No allocator needed unless your
init function uses one.

## Picking a primitive

| You want… | Use |
|---|---|
| Exclusive access to a struct | `Mutex` |
| Many readers, infrequent writer | `RwLock` |
| Bounded concurrency | `Semaphore` |
| Wake one waiter on event | `Notify` |
| N tasks to meet at a checkpoint | `Barrier` |
| Lazy one-time init | `OnceCell(T)` |

If you find yourself reaching for two of these for the same value,
consider whether a `Channel(T)` would be cleaner — passing values
between coroutines often beats locking shared mutable state.

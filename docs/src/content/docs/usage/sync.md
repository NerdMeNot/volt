---
title: Sync primitives
description: Mutex, Notify, Semaphore — three primitives that cover most coroutine-level synchronization. All three sit on the parking lot.
---

Three sync primitives. All three are built on the parking lot;
none of them carry their own waiter queue. The reason they're
small surfaces is the parking lot does the work — see [The parking
lot](/architecture/parking-lot/) for the substrate.

## `volt.Mutex`

```zig
var mu = volt.Mutex.init();
defer mu.deinit();

mu.lock();                       // parks on contention
defer mu.unlock();
// ... critical section ...
```

Standard mutual exclusion. State machine is `UNLOCKED ↔ LOCKED ↔
CONTENDED`:

- Uncontended `lock`: single CAS UNLOCKED → LOCKED.
- Contended `lock`: spin briefly, then CAS to CONTENDED and park on
  `&self.state`.
- `unlock` LOCKED: single CAS to UNLOCKED.
- `unlock` CONTENDED: CAS to UNLOCKED, then unparkOne on
  `&self.state`.

The contended path also handles a race-case where the spinning
slow-path's CAS-to-CONTENDED and the unlock's CAS-to-UNLOCKED
interleave; the slow-path detects the race via the swap-CONTENDED
return value and takes ownership inline. The race is the reason
unlock's fast path doesn't take an inner mutex.

```zig
// Non-blocking:
if (mu.tryLock()) {
    defer mu.unlock();
    // ... critical section ...
}

// Cancel-aware:
try mu.lockCancel(&cancel);    // error.Cancelled if Cancel fires
```

Contended-Mutex bench: 15 ns/op (Volt) vs 81 ns/op (Go). The 5.4×
gap is mostly Go paying for write-barriers on parked goroutine
pointers; we don't have GC, so we don't pay.

## `volt.Notify`

```zig
var n = volt.Notify.init();
defer n.deinit();

// One coroutine waits:
n.wait();        // consumes one permit, or parks until notifyOne

// Any coroutine wakes:
n.notifyOne();   // wakes one waiter, or stores one permit
n.notifyAll();   // wakes all waiters
```

A "permit + parker" primitive. Internal state is a small counter:

- `notifyOne` increments the permit counter (saturating at 1) or
  wakes a parked waiter.
- `wait` decrements the permit (returning immediately) or parks.

Permits don't accumulate beyond 1 — `notifyOne` called 5 times
when no one is waiting stores 1 permit, not 5. This matches the
"signal once, wait once" pattern. For counting semantics, use
`Semaphore`.

`notifyAll` wakes every parked waiter (without consuming permits).

Notify is the building block for one-shot waits (done flags,
barriers, condition variables). The runtime itself uses Notify
patterns in `Task.join`'s parking primitive.

## `volt.Semaphore`

```zig
var sem = volt.Semaphore.init(8);    // 8 permits available
defer sem.deinit();

sem.acquire();                       // parks if 0 permits
defer sem.release();
// ... do work that requires a permit ...
```

A counting semaphore. `init(N)` starts with N permits.

- `acquire` decrements (parks if 0).
- `release` increments and unparks one waiter (if any).
- `tryAcquire()` returns false instead of parking.

Use cases:

- **Concurrency limits.** Cap N coroutines running a thing at
  once: `sem = Semaphore.init(8)`, every worker `acquire`s before
  starting, `release`s after finishing.
- **Resource pools.** N database connections in a pool, semaphore
  with N permits, each `acquire` waits for an available
  connection.
- **Rate limiters (with sleep).** Token bucket: refill task
  periodically releases permits.

Direct-handoff release: when `release` finds a parked waiter, it
unparks via direct handoff (the waiter's Task is dispatched
inline on the releaser's worker via the LIFO slot). Cuts the
park/unpark round trip — same optimization as `Task.join`.

## What's NOT here

Volt does not provide:

- **RwLock / RWMutex.** Add as a library on top if you need it;
  the underlying parking lot makes a reader-writer state machine
  trivial. Not core because most workloads can use `Mutex` with
  short critical sections and not measurably suffer.
- **Condition variables.** Use `Notify` + a separate mutex if you
  want explicit signal/wait. The combination isn't packaged into
  one type because the parking lot already gives you the wait part
  for free.
- **Barrier.** Use a `Semaphore` initialised to 0, plus N
  `release` calls.
- **OnceCell / Once.** Library territory; the runtime doesn't need
  it.

## Cancellation

Every blocking sync op has a cancel-aware variant:

```zig
try mu.lockCancel(&cancel);
```

Same shape as channel cancel-aware ops — register a Cancel waiter
under the bucket lock atomically with checking the primitive
state. See [Structured Concurrency](/usage/structured-concurrency/).

Cancel-aware `Notify` and `Semaphore` variants exist for
completeness; if you have a `*Cancel` in scope, prefer them over
the non-Cancel forms.

## See also

- [The parking lot](/architecture/parking-lot/) — wait/wake substrate that all three use.
- [Structured Concurrency](/usage/structured-concurrency/) — cancel-aware patterns.
- [Choosing a primitive](/guides/choosing-primitive/) — decision tree across sync + channels.

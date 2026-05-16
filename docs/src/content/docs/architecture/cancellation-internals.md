---
title: Cancellation internals
description: How volt.Cancel wakes a coroutine parked on a Mutex / channel / sleep — without each primitive knowing about Cancel. The validator hook and the waiter list.
---

Volt's user-facing cancellation surface is small:
[`volt.Cancel`](/usage/structured-concurrency/) with `fire()`,
`checkpoint()`, `isFired()`. Underneath, "wake every blocking op
that's currently waiting" is a non-trivial problem. Different
blocking ops live on different addresses in the parking lot; the
Cancel doesn't a-priori know which.

The trick: the parking lot's **validator hook** lets every park
re-check arbitrary state under the bucket lock. The cancel-aware
variants use that hook to check the Cancel flag atomically with
the primitive's own state. Cancel.fire walks its waiter list and
unparks each on the primitive's address — the primitive's wake
path returns to the cancel-aware op, which re-checks the Cancel
and exits with `error.Cancelled`.

## Mental model

> A `Cancel` is a **wake-up doorbell** that knows everyone who's
> listening, plus an atomic "doorbell rang" flag. When `fire()` is
> called, it rings the doorbell (sets the flag) and runs through
> the listener list (the waiter list), poking each listener at the
> exact wait-point it's parked on. The listener's "you got poked"
> handler re-checks the flag, sees it set, and returns
> `error.Cancelled` from whatever blocking op was holding it.

The listener list isn't kept by every primitive. The Cancel keeps
its own list. Each cancel-aware op temporarily registers a waiter
on the Cancel that points back to "the primitive address where
this op is parked". Cancel.fire walks the list and unparks each.

## The Cancel struct

```zig
pub const Cancel = struct {
    rt: *Runtime,
    fired: std.atomic.Value(bool) = .init(false),
    lock: std.atomic.Value(u32) = .init(0),  // spinlock for waiter list
    waiters: ?*Waiter = null,                  // intrusive linked list

    const Waiter = struct {
        next: ?*Waiter,
        park_addr: *const anyopaque,  // the address this waiter is parked on
    };
};
```

Reference: `src/cancel.zig`.

The `waiters` list is intrusive: each `Waiter` lives on the
caller's stack (the cancel-aware op stack-allocates one before
parking). No heap allocation per registration.

## The cancel-aware op pattern

Every cancel-aware blocking op (`Mutex.lockCancel`,
`Spsc.recvCancel`, etc.) follows the same shape:

```zig
pub fn lockCancel(self: *Mutex, c: *Cancel) error{Cancelled}!void {
    // 1. Fast path: try to acquire without parking.
    if (self.tryLock()) return;

    // 2. Stack-allocate a Waiter; register on the Cancel.
    var w = Cancel.Waiter{ .next = null, .park_addr = &self.state };
    if (c.register(&w, &self.state)) {
        // Cancel was already fired — fast-return.
        return error.Cancelled;
    }
    defer c.deregister(&w);

    // 3. Park on the Mutex's address. The validator checks
    //    both the Mutex state AND the Cancel flag atomically
    //    under the bucket lock.
    park.parkOn(&self.state, lockValidator);

    // 4. After unpark, re-check the Cancel flag. If fired,
    //    we were unparked by Cancel.fire — return error.
    if (c.isFired()) return error.Cancelled;

    // Otherwise: Mutex was unlocked; we got the lock.
    return;
}
```

Reference: `src/sync.zig` (Mutex.lockCancel) and parallel patterns
in `src/channel.zig` (recvCancel / sendCancel).

The four steps:

1. **Fast path** — most ops succeed without parking.
2. **Register** — stack-allocate a `Waiter` and push it onto the
   Cancel's list. The register call is atomic-flag-check +
   spinlock-protected list-push.
3. **Park with validator** — `parkOn(addr, validator)` calls
   `validator` under the bucket lock to decide whether to actually
   park. The validator checks both the primitive's own state AND
   the Cancel's flag.
4. **Re-check on wake** — whether we were unparked by the
   primitive's normal wake or by Cancel.fire, we re-check
   `Cancel.isFired()` to disambiguate.

## The validator hook closes the race

Here's the race we have to close:

```
Thread A (cancel-aware op)         Thread B (Cancel.fire)
═══════════════════════           ═══════════════════════

1. tryLock fails
2. register(&w) → not fired
                                  3. fired = true
                                  4. iterate waiters, call
                                     unparkAll(w.park_addr)
                                     → no waiters yet on
                                       that address
5. parkOn(&state, validator)
                                  // A is parked forever
```

Without protection, Thread B observed an empty waiter list because
A hadn't called parkOn yet — even though A had registered with the
Cancel. The unparkAll did nothing; A is now parked and Cancel.fire
already returned.

The validator-under-lock pattern fixes it:

```zig
fn lockValidator(state: *const anyopaque) bool {
    const self: *Mutex = ...;
    const c: ?*Cancel = current_coro.cancel_in_flight;
    // (1) Re-check Mutex state under the bucket lock.
    if (self.state.load(.acquire) == UNLOCKED) return false;
    // (2) Re-check the Cancel flag under the bucket lock.
    if (c) |cancel| {
        if (cancel.isFired()) return false;
    }
    return true;
}
```

The parking lot's `parkOn` takes the bucket lock, calls the
validator, and only enqueues the waiter if it returns true. If the
validator sees `Cancel.isFired() == true`, it returns false; the
waiter never gets enqueued; `parkOn` returns immediately. The
cancel-aware op then re-checks the Cancel flag and returns
`error.Cancelled`.

Cancel.fire's unparkAll happens **outside** the bucket lock; if a
new register-then-park is mid-flight on this address, fire's
unparkAll might miss it. But the validator catches it: between
register and parkOn, if fire happened, the validator sees the
flag set and refuses to park.

This is the same validator-under-lock pattern that closes the
register-then-park race for non-cancellation cases. See
[The parking lot](/architecture/parking-lot/) for the substrate.

## Cancel.fire

```zig
pub fn fire(self: *Cancel) void {
    if (self.fired.swap(true, .acq_rel) == true) return;  // idempotent

    // Drain the waiter list. Each waiter holds a park_addr;
    // unparkAll on each address wakes every coroutine parked there.
    self.spinlockAcquire();
    var head = self.waiters;
    self.waiters = null;
    self.spinlockRelease();

    while (head) |w| {
        head = w.next;
        parking_lot.unparkAll(self.rt, w.park_addr);
    }
}
```

Reference: `src/cancel.zig`.

The fire path:

1. **Atomic flag swap** — `swap(true, .acq_rel)`. If already
   fired, return (idempotent).
2. **Drain the waiter list** under the spinlock. We take the list
   ownership (set `self.waiters = null`) so subsequent registers
   land on a fresh empty list. (They'll see `fired == true` via
   the register's fast-path check and return immediately
   anyway, but draining defends against any spinning.)
3. **For each waiter, `unparkAll` on its `park_addr`.** This wakes
   every coroutine parked on that address — not just the one that
   registered. That's fine — coroutines on the same address that
   aren't holding this Cancel will re-park because their
   validator's normal state-check still returns true.

`fire` can be called from any thread (driver or worker). The flag
is set with `.acq_rel` so the cancel-aware op's subsequent
re-check sees it with the right memory ordering.

## `Cancel.checkpoint()`

For CPU-bound code that doesn't park:

```zig
pub fn checkpoint(self: *const Cancel) error{Cancelled}!void {
    if (self.fired.load(.acquire)) return error.Cancelled;
}
```

A single atomic load + branch. Cost is in the noise (~1 ns on
arm64). Call from inside tight loops every N iterations; the
overhead is far cheaper than a syscall, and the cancellation
latency is bounded by N × inner-iteration-time.

## Tried & rejected: per-coroutine cancel slot

The first design (briefly) attached a `cancel_in_flight: *Cancel`
slot directly to the Coroutine struct. Every blocking op would
check `current.cancel_in_flight` automatically.

The problem: it makes cancellation **implicit data**. A function
that calls a blocking op has no way to know whether the caller
set up a Cancel; reading the function's signature gives you no
information. Bugs like "I forgot to set cancel_in_flight before
spawning that worker" become hard to spot.

Volt picked Go's model: `*Cancel` is **explicit data** threaded
through parameters. The cost is one extra parameter in
cancel-aware signatures; the benefit is that you can see exactly
where cancellation can flow into your code.

The `cancel_in_flight` slot still exists internally — but as the
validator's input (the validator reads it from the current
coroutine to know which Cancel to check), not as the user API.

## Tried & rejected: Tokio-style abort

Tokio's model: a `JoinHandle::abort()` causes the next time the
spawned task's executor calls `Future::poll` to return
`Poll::Ready(Err(JoinError::Cancelled))`. The task's drop runs
its destructors.

For stackful coroutines this is conceptually wrong. There's no
`Future::poll` — the coroutine has a real stack with a real
suspended call chain. "Drop the future" means tearing down a
running coroutine's stack from another thread, which requires
running its destructors, which requires resuming it. You can't
yank the rug.

Go-style: pass cancellation as data, let the coroutine observe at
its own pace, let it unwind normally. Volt does this with the
`*Cancel` plumbing and the parking-lot integration that makes
observation prompt.

## Performance

The cancel-aware ops add one extra spinlock-protected register +
deregister per blocking call. Measured cost: ~10-20 ns extra over
the non-cancel variant. The Cancel flag check itself is one atomic
load on every park+unpark, ~1 ns. Most workloads don't notice.

`bench-mutex` (contended) is **15 ns/op** — the cancel-aware
variant adds maybe 1-2 ns on the contended path. The Mutex isn't
parking on every op (uncontended fast path is one CAS), so the
amortized cancel cost is negligible.

## Further reading

- [Structured concurrency](/usage/structured-concurrency/) — the user-facing API.
- [The parking lot](/architecture/parking-lot/) — the validator-under-lock pattern.
- Go's `context.Context` — the inspiration. Same "cancellation as data" shape, less prompt at blocking ops (Go's `context` requires explicit `select { case <-ctx.Done(): }` plumbing; Volt's cancel-aware variants embed that into every blocking op).
- Tokio's `CancellationToken` (separate from `JoinHandle::abort`) — Rust equivalent of `volt.Cancel`, same waiter-list shape.

---
title: Volt memory model
description: The rules under which Volt's primitives are correct on any worker count.
---

# Volt memory model

Volt is a multi-threaded runtime by design, not by patch. Every primitive is
correct when used from N OS threads. The single-worker case (`workers = 1`)
is a *configuration*, not a *test fixture* — the same code paths run at
`workers = 1` and `workers = NumCPU`, with the same correctness guarantees.

This document specifies, for each piece of shared state in the runtime,
*who* reads it, *who* writes it, *in what order* (Acquire / Release / SeqCst),
and *what happens-before relation* is established by that ordering. If
you change a shared-state access, update this document — or your change
isn't done.

## Atomics model

Volt uses Zig's `std.atomic.Value(T)` with explicit memory orderings.
The Zig orderings map to the C++20 / LLVM model:

| Ordering | Use |
|---|---|
| `.monotonic` (relaxed) | Counters where ordering doesn't matter (telemetry) |
| `.acquire` | Read that establishes happens-before from a matching `.release` write |
| `.release` | Write that establishes happens-before for matching `.acquire` reads |
| `.acq_rel` | RMW operations (fetchAdd, fetchSub, cmpxchg) acting as both |
| `.seq_cst` | Total order required across multiple atomics (rare; document why) |

**Rule:** every atomic access has an ordering that's justified by a
specific happens-before relation. `.monotonic` is allowed only when the
value is for telemetry or when ordering is already established by a
companion atomic with stronger ordering.

## Shared state — by component

### `Runtime`

```
allocator                          // immutable after init
workers: []Worker                  // pointer + len immutable; per-worker
                                   //   state has its own model below
injection                          // shared queue (see InjectionQueue)
reactor                            // single-poller reactor (see Reactor)
shutdown: atomic bool              // see "Shutdown flag"
parked_workers: atomic u64         // see "Parker bitmap"
num_searching: atomic u32          // see "Searching counter"
reactor_poller_taken: atomic bool  // see "Reactor poller claim"
```

There is **no global `pending` task counter.** Termination of the
root coroutine is observed via the root Task's WaitGroup: `Runtime.run`
registers the driver thread's Parker as `thread_waiter` on that wg,
and the final `done()` wakes it. Per-task waiters use the wg's
coroutine `waiter` slot. See "WaitGroup" below.

#### Shutdown flag

```zig
Runtime.shutdown: std.atomic.Value(bool)
```

* **Writers**: `Runtime.deinit` (`.release`).
* **Readers**: every worker's loop top (`.acquire`).
* **Ordering**: pairs of `.release` / `.acquire`. After `deinit` sets
  the flag, any worker observing `true` is guaranteed to also see all
  prior writes by the deinit thread (in particular: all in-flight
  coroutines have completed if `pending == 0`).

#### Parker bitmap

```zig
Runtime.parked_workers: std.atomic.Value(u64)
```

* **Layout**: bit `i` is set iff `workers[i]` is currently parked.
* **Writers**:
  * Worker marking itself parked: `fetchOr(1 << i, .acq_rel)`.
  * Worker unmarking on wake: `fetchAnd(~(1 << i), .acq_rel)`.
  * `wakeOneParked` clearing a victim's bit via CAS loop on the full
    u64 (`cmpxchgWeak(_, _, .acq_rel, .acquire)`).
* **Readers**: any thread calling `wakeOneParked` (`.acquire` load of
  the bitmap as the CAS expected value).
* **Ordering** justification:
  * Worker's `markParked` (release fence) is paired with `wakeOneParked`'s
    CAS-acquire. After CAS succeeds clearing a worker's bit,
    `wakeOneParked` calls `parker.unpark` whose `.seq_cst` swap on the
    parker state forms a fence with the parked worker's `.seq_cst` CAS
    in `Parker.park`.
  * Worker's `unmarkParked` (release) happens *after* the worker has
    observed work. The next pusher's `wakeOneParked` (acquire) sees the
    cleared bit and chooses someone else (or no-op).
* **Known limitation (TODO #145)**: single cache-line CAS contention.
  Sharding the bitmap into per-cache-line atomics is the next step.

#### Searching counter

```zig
Runtime.num_searching: std.atomic.Value(u32)
```

Anti-herd guard. Counts workers currently in the *find-work phase*
of the dispatch loop — actively scanning local / injection / steal
victims for a runnable coroutine. Critically does **not** count
workers that are currently dispatching a coroutine.

* **Writers**: every worker, around its find-work phase.
  `fetchAdd(1, .acq_rel)` at the top of the dispatch iteration;
  `fetchSub(1, .acq_rel)` before committing to dispatch / before
  entering `reactor.poll` / before parking.
* **Readers**: any thread calling `wakeOneParked`
  (`load(.acquire)`). If positive, the wake is skipped — the
  searching worker is expected to pick up the just-pushed work on
  its current or next pass. The latch below covers the case where
  it has already passed its last scan.

#### Missed-wakeup latch (`need_spinning`)

```zig
Runtime.need_spinning: std.atomic.Value(u32)
```

Modelled on Go's `sched.needspinning` (`runtime/proc.go` design
doc, set/cleared at proc.go:3158, 3618). Closes the race that
`num_searching` alone leaves open:

1. Worker A is searching: `num_searching == 1`.
2. A's last scan returns empty. A `fetchSub`'s `num_searching`,
   reaching 0.
3. Pusher P pushes work + calls `wakeOneParked`. Reads
   `num_searching == 0` (visible by load-acquire / release-rmw
   pairing with A's fetchSub). Reads `parked_workers` bitmap;
   A has not yet `markParked`'d, so the bitmap is empty. P
   bails without waking anyone.
4. A reaches `parkWorker`, marks itself, rechecks queues, sees
   nothing, and parks. P's pushed work sits with no live worker.

`need_spinning` closes step 3 → step 4: when P observes
`num_searching > 0`, it sets `need_spinning = 1` AND re-loads
`num_searching`. If the second load still shows > 0, the searcher
hasn't transitioned yet — it will catch the push via its normal
scan. If the second load shows 0 (the searcher transitioned
between P's first load and the latch set), P falls through to do
a real wake.

The worker side drains the latch AFTER `fetchSub` and BEFORE
`parkWorker`:

```zig
_ = rt.num_searching.fetchSub(1, .acq_rel);
if (rt.need_spinning.swap(0, .acq_rel) != 0) continue; // re-search
parkWorker(rt, m);
```

The `swap(0, .acq_rel)` synchronises-with the pusher's
`store(1, .release)` — if the worker observes 1, all stores
sequenced before the latch set (including the pushed work) are
visible to subsequent loads in the next find-work pass.

* **Writers**: `wakeOneParked` (`store(1, .release)` when bailing
  on the anti-herd guard).
* **Readers / clearers**: worker loops
  (`workerLoopUntilShutdown`, `workerLoopUntilTaskDone`) via
  `swap(0, .acq_rel)` after the `fetchSub` of `num_searching` and
  before `parkWorker`.
* **Why this is sound (audit-anchored)**: the previous "wakeOneParked
  will see the bit and unpark" invariant relied on `markParked`
  happening before any pusher's check; this was racy when the
  pusher loaded `num_searching` before the searcher's `fetchSub`
  but loaded `parked_workers` after — both reads can see "no one
  to wake". The latch is the explicit handshake the previous design
  was missing. Tokio achieves the same closure via mutex-mediated
  idle transition (`tokio::runtime::scheduler::multi_thread::idle`);
  the latch is the lock-free variant.
* **Residual gap closed by a park-time full scan (2026-05-31)**: the
  latch fixes the searcher-*transition* race, but a *coverage* gap
  remained. `stealFromSiblings` is capped + randomized (bounds CAS
  contention at high worker counts), so a searching worker can fail
  to sample the one sibling mailbox holding work. Cross-thread
  unparks from blocking-pool threads (which have no current M) all
  push to `ps[0].mailbox`; combined with `wakeOneParked`'s anti-herd
  bail, the coro could be stranded in `ps[0].mailbox` with every
  worker parked. Fix: `parkWorker`, after `markParked` (intent-to-park
  established) and the spin loop, does ONE uncapped scan of every
  sibling mailbox (`stealAnyMailboxInto`) before the `park()` syscall.
  Invariant: **no worker commits to parking while any mailbox is
  non-empty**, so the all-parked state cannot coexist with queued
  work. Future pushes are still covered by the bit being set.

#### `spawnBlocking` completion handshake (`done` + `released`)

```zig
// closure on the calling coroutine's stack (lib.zig)
done: std.atomic.Value(u32)      // set by pool thread when result ready
released: std.atomic.Value(u32)  // set by pool thread as its LAST touch
```

The pool thread runs `result = f(args); done.store(1, .release);
unparkOne(rt, &done); released.store(1, .release)`. The coroutine
parks on `done` via the parking lot (validator reads `done`), then —
critically — spins `while (released.load(.acquire) == 0)` before
returning.

* **Why two flags**: `done` gates the *waiter's logic* (result is
  ready, stop waiting). It is NOT a lifetime barrier: the validator
  can observe `done == 1` and the coroutine can return + free its
  stack frame (where the closure lives) while the pool thread is
  still inside `unparkOne(rt, &done)`, dereferencing `rt` and
  `&done`. That was a use-after-free that woke the wrong coroutine
  and deadlocked the scheduler (pre-2026-05-31). `released` is the
  separate lifetime barrier the owner spins on, published by the
  pool thread as its final action. Same pattern as `FsCancelCtx.completed`.
* **Writers**: pool thread (`store(.release)` on both, in order).
* **Readers**: the coroutine — `done` via the parking-lot validator,
  `released` via the post-park acquire-spin.

#### Reactor poller claim

```zig
Runtime.reactor_poller_taken: std.atomic.Value(bool)
```

* Single-claim flag for "I am the worker that is currently inside
  `reactor.poll()`." Exactly one worker holds the poller role at a
  time; others either find work elsewhere or park.
* **Writers/Readers**: any worker about to poll
  (`cmpxchgStrong(false, true, .acq_rel, .acquire)`); the same worker
  on release (`.release`).
* **Ordering**: the `.acq_rel` CAS establishes mutual exclusion of the
  reactor's kqueue file descriptor. After release, the next claimer's
  acquire sees the poller's writes to `reactor.pending`.

#### Driver participation

The thread that calls `Runtime.run(fn, args)` *participates as a
worker* (worker 0). It runs the same `workerLoop` as the spawned
workers, with one extra exit condition: when the root coroutine's
`wg.count()` hits 0, it returns. Other workers continue until
`Runtime.deinit`.

The driver thread is in the parker bitmap, eligible for unpark on
new work, just like any other worker. There is no special
`driver_parker`; `workers[0].parker` plays both roles. Wake-on-root-
done is delivered through the root Task's `WaitGroup.thread_waiter`,
which `Runtime.run` sets to `&workers[0].parker`.

### `InjectionQueue` (lock-free MPMC Treiber stack)

```zig
InjectionQueue.head: std.atomic.Value(?*Coroutine)
```

* **Writers**: any thread pushing via CAS (`cmpxchgWeak(_, _, .release, .monotonic)`).
* **Readers**: any thread popping via CAS (`cmpxchgWeak(_, _, .acq_rel, .acquire)`).
* **Happens-before**: pusher's `.release` synchronizes with popper's
  `.acquire`. The pusher writes `coro.next = old_head` before the CAS;
  the popper sees that `next` link after a successful CAS.
* **Per-coroutine `next` field**: written non-atomically by the pusher
  immediately before the CAS publish. The CAS's `.release` fence
  carries the write. Popper reads `next` after acquiring the head;
  acquire fence guarantees visibility.

### `WorkStealQueue` (fixed-cap SPMC)

The full memory model is documented in
`src/work_steal_queue.zig` and follows Le et al., *"Correct and
Efficient Work-Stealing for Weak Memory Models"* (PPoPP'13). Summary:

* `tail`: written by owner only; `.release` on push, `.monotonic` reads.
* `head` (packed `(steal_head, real_head)`): all writers CAS with
  `.acq_rel`. The packed two-counter design distinguishes
  "claimed but not released" (in-flight steal) from
  "actually advanced" so producers refuse to overflow into a
  stealing region.
* `buffer[idx]`: atomic slots. Producer writes with `.release`
  before advancing tail; the owner `pop` reads with `.acquire`
  after observing head.
  * **Stealer bulk copy is `.monotonic`, not acquire/release.** In
    `stealInto`, the per-slot src loads and dst stores use
    `.monotonic` because the fences are carried by surrounding ops,
    not the per-slot access: the stealer's `src.tail.load(.acquire)`
    synchronizes-with the producer's `tail.store(.release)`, so every
    slot store below the observed tail already happens-before the
    copy (and the stealer only touches slots in
    `[real, real+to_steal) ⊆ [head, tail)`); the phase-1 claim CAS
    (`.acq_rel`) fences the claimed range against rival stealers; and
    the copied-in dst slots are published to any dst consumer solely
    by the trailing `dst.tail.store(.release)`. Per-slot
    acquire/release would be redundant. On arm64 this is LDR/STR
    instead of LDA/STL per stolen item.
* Fixed capacity → no `grow()` → no realloc race. Overflow on push
  spills half to `InjectionQueue` (Tokio strategy).

### `Worker.lifo_slot`

```zig
Worker.lifo_slot: std.atomic.Value(?*Coroutine)
```

* **Owner-only** for both reads and writes. Stealers do NOT touch
  this slot; it's a worker-private single-task cache.
* **Writes**: `pushLifo` does `swap(coro, .acq_rel)`; evicted occupant
  goes to the main queue.
* **Reads**: `popLocal` does `swap(null, .acq_rel)`.
* **Why atomic if owner-only?** Because `wakeOneParked` may race with
  owner. The parker-recheck in the dispatch loop loads `lifo_slot`
  to decide whether to park; if a concurrent pushLifo from another
  thread (cross-thread spawn that found us as current worker)
  populates the slot, the recheck-acquire sees it. **Note**: this
  case can only arise if a coroutine running on this worker calls
  spawn, which then finds `current_worker == self` and pushes.
  Same-thread access — still atomic for memory-fence consistency
  with the parker bitmap's cross-thread synchronization.

### `Coroutine`

The Coroutine struct has both single-set fields (written at spawn,
read everywhere after) and multi-set fields:

| Field | Setter | Reader | Pattern |
|---|---|---|---|
| `ctx` | dispatch / coro itself | swap asm | Single-thread per dispatch slice (only the running worker accesses it) |
| `main_ctx` | dispatch (every time) | trampoline / yield / park | Single-thread per dispatch slice |
| `stack` | spawn (once) | swap, free | Set-once; visible via release-fence on push |
| `frame_ptr` / `frame_destroy` | spawn (once) | dispatch on `.done` | Set-once |
| `pending: PendingKind` | trampoline / yield / park | dispatch | Same thread (the running worker writes it just before swap-out; the same worker reads after swap-back) |
| `next` (intrusive) | queue push | queue pop | Established by the queue's release/acquire on head |
| `wait_next` (intrusive) | sync wait queue push | sync wait queue pop | Same as `next` but for sync primitive queues |
| `wg: ?*WaitGroupAtomic` | spawn (once) | dispatch on `.done` | Set-once; visible via release on push |
| `runtime: *anyopaque` | spawn (once) | unpark | Set-once |
| `has_task` | spawn (once) | dispatch | Set-once |
| `park_state: atomic u32` | `dispatch`, `runtime.unpark` | same | See "Park state machine" |

#### Park state machine

The "register a waiter, then `runtime.park()`" pattern is racy on
multi-worker without an atomic state on the Coroutine itself: an
unpark fired between the register step and the actual `context.swap`
would enqueue the coroutine while another worker is *still running
it*, leading to two workers dispatching the same stack.

`park_state` (values defined in `coroutine.ParkState`) closes the
window:

* **RUNNING** — coroutine is executing on its worker.
* **PARKED** — coroutine has fully swapped out; safe to unpark.
* **NOTIFIED** — an unpark arrived while the coroutine was still
  RUNNING; dispatch consumes it instead of leaving the coro stranded.

Transitions:

| Site | Transition | Ordering |
|---|---|---|
| `dispatch` (sees `c.pending == .park`) | `RUNNING → PARKED` via `cmpxchgStrong` | `.acq_rel` / `.acquire` |
| `dispatch` (above CAS fails with NOTIFIED) | `NOTIFIED → RUNNING`, re-queue | `.release` store + `injection.push` |
| `runtime.unpark` (sees PARKED) | `PARKED → RUNNING`, then `injection.push` | `.acq_rel` |
| `runtime.unpark` (sees RUNNING) | `RUNNING → NOTIFIED` | `.acq_rel` |
| `runtime.unpark` (sees NOTIFIED) | no-op (idempotent) | — |

Why this is correct:

* The transition to PARKED happens *after* dispatch observes the
  swap-back from `context.swap` — so the coroutine's registers and
  stack pointer are guaranteed already saved into `c.ctx`. A
  subsequent unpark that sees PARKED can safely enqueue.
* An unpark that arrives mid-register-then-park sees RUNNING and
  stores NOTIFIED. The coroutine's own swap-back then enters
  dispatch's `.park` branch, the CAS RUNNING→PARKED fails (state
  is NOTIFIED), and dispatch re-queues the coroutine itself. No
  external worker tries to dispatch a coroutine that is still on-CPU.

**Single-set fields** (set at spawn, read after): correctness comes from
the queue's release-fence on push — anything written *before* the push
is visible after the matching pop's acquire. No need for per-field atomics.

**Multi-set fields** (`pending`, `ctx`, `main_ctx`) are accessed by
exactly one thread at a time (the worker currently running this coroutine),
across a ctx-swap boundary. The Zig `extern fn voltCtxSwap` asm acts as a
full memory barrier (`mov sp, x9` plus the function call boundary), so
writes before the swap are visible after the swap-back on the same thread.
*Different workers* can run a coroutine across its lifetime (steal +
re-dispatch), but the queue push/pop fences carry the writes between
worker handoffs.

### `WaitGroup`

```zig
WaitGroup.counter: std.atomic.Value(u32)
WaitGroup.waiter: std.atomic.Value(?*Coroutine)
WaitGroup.thread_waiter: std.atomic.Value(?*Parker)
```

Two waiter slots so the same WaitGroup can be joined by either a
coroutine (`waiter`, unparked via `runtime.unpark` onto the run
queue) or the driver thread (`thread_waiter`, unparked directly via
`Parker.unpark`). Both slots fire on the terminal `done()`; in
practice exactly one is used per wg — Task uses the coroutine slot;
`Runtime.run` sets the thread slot to wake the driver.

* **Counter**: `fetchAdd(.acq_rel)` on `add`, `fetchSub(.acq_rel)`
  on `done`, `load(.acquire)` on `count`.
* **Coroutine waiter slot**: `swap(.acq_rel)` on both wait-side
  (register) and done-side (consume).
* **Thread waiter slot**: `store(.release)` once at registration,
  `load(.acquire)` from `done()`. Set-once at registration time;
  never reset (the Parker handles the actual one-shot semantics).
* **Cross-thread protocol** for the coroutine slot:
  1. `add(n)`: counter increment. Happens-before the corresponding
     `done`s.
  2. `wait()`: load counter; if 0, return. Else swap self into
     waiter slot; *re-check counter* (to close the race with a
     `done` that completed between the load and the swap); if zero
     now, swap waiter back out and return; else park.
  3. `done()`: counter decrement; if decremented to 0, swap waiter
     slot to null; if non-null, unpark that coroutine. Then load
     `thread_waiter`; if non-null, unpark the Parker.
* **Why the re-check works**: the writes are ordered. After
  `done`'s `fetchSub` (`.acq_rel`), it does
  `waiter.swap(null, .acq_rel)`. If wait()'s `waiter.store` happened
  *before* this swap, done() sees the waiter and unparks. If
  wait()'s `waiter.store` happened *after*, then wait()'s subsequent
  counter recheck sees the decremented value (the counter was
  decremented before the waiter swap), and wait() cleans up its own
  registration.

### `Reactor` (kqueue today; epoll / io_uring next)

```zig
Reactor.kq: i32                                // immutable after init
Reactor.pending: std.atomic.Value(u32)         // in-flight registrations
```

* **Writers** of `pending`: `waitFd` (any worker, inside a coroutine
  about to park on an fd) does `fetchAdd(1, .acq_rel)` after a
  successful `kevent` register; `poll` (the worker holding the
  poller claim) does `fetchSub(count, .acq_rel)` once per drain.
* **Readers**: every dispatcher reads `pending.load(.acquire)` to
  decide whether to claim the poller and whether to keep parking
  short of poll-blocking.
* **Why atomic**: three or more threads touch this field
  concurrently (register from any worker, drain from the poller,
  read from every dispatcher). Plain `+= 1` / `-= 1` was a data race.
* **What the counter does NOT carry**: it does not guarantee fd
  readiness — that's kqueue's job. It only gates "should this
  worker bother calling kevent at all." The actual happens-before
  for the ready event lives in the kernel.

### Sync primitives — Mutex, Notify, Semaphore

Each primitive has TWO layers of synchronization:

1. **Atomic state word** that handles the uncontended fast path and
   the producer–consumer race against the wait queue. Lock-free; no
   syscall when there are no waiters.
2. **Short per-primitive internal mutex** (`inner: PthreadMutex`)
   that protects wait-queue manipulation only. Held briefly
   (push waiter / pop waiter). Never held across `park` / `unpark`.

Eager init: `Mutex.init()` / `Notify.init()` / `Semaphore.init(n)`
call `pthread_mutex_init` immediately. There is no lazy-init flag —
that pattern had a race where two threads on the first-contention
path could both call `pthread_mutex_init`.

#### Mutex

* **State word** `Mutex.state: atomic u32`. Values:
  - `UNLOCKED = 0`
  - `LOCKED = 1` (held, no waiters known)
  - `CONTENDED = 2` (held, ≥ 1 waiter parked)
* **Fast path lock**: CAS `UNLOCKED → LOCKED` (`.acquire / .monotonic`).
* **Fast path unlock**: CAS `LOCKED → UNLOCKED` (`.release / .monotonic`).
* **Slow path lock**: take `inner`, re-try fast path, swap state to
  `CONTENDED` (`.acq_rel`), push self onto wait queue, drop
  `inner`, `runtime.park()`. On wake, ownership has been directly
  handed to us — return.
* **Slow path unlock**: take `inner`, pop head waiter; if none, store
  `UNLOCKED` and release `inner`; else if queue is now empty,
  demote state to `LOCKED` (`.release`); drop `inner`,
  `runtime.unpark(waiter)`. **Direct handoff**: state stays
  `LOCKED`/`CONTENDED` across the unpark; no `tryLock` sneaker can
  intercept ownership before the unparked waiter resumes.

#### Notify

* **State word** `Notify.notified: atomic u32` (one stored permit).
* `wait`: CAS `1 → 0`; on miss, take `inner`, re-try, push self,
  drop `inner`, park.
* `notifyOne`: take `inner`, pop a waiter; if none, drop `inner`
  then `notified.store(1, .release)` (next `wait` consumes); else
  drop `inner` then `unpark(waiter)`.
* `notifyAll`: take `inner`, drain the queue into a local list,
  drop `inner`, unpark each. Does **not** set the permit.

#### Semaphore

* **State word** `Semaphore.permits: atomic u32`.
* `acquire`: spin CAS to decrement if positive; on zero, take
  `inner`, re-try, push self, drop `inner`, park. On wake, a permit
  has been reserved for us — return.
* `release`: take `inner`, pop a waiter; if none, drop `inner` and
  `permits.fetchAdd(1, .release)`; else drop `inner`, unpark.
  **Direct handoff**: don't increment permits when a waiter exists;
  the permit transfers directly via the unpark.

### `Spsc` channel

Comptime-specialized single-producer / single-consumer ring buffer.
"SPSC" is a *placement* constraint (one producer, one consumer), not
a thread constraint — the producer and consumer can run on any two
OS threads.

```zig
ring: [cap]T                                                  (producer writes)
head: atomic u64                                              (producer writer, consumer reader)
tail: atomic u64                                              (consumer writer, producer reader)
recv_waiter: atomic ?*Coroutine                               (set by recv, consumed by send)
send_waiter: atomic ?*Coroutine                               (set by send, consumed by recv)
closed: atomic bool
```

* `head` and `tail` sit on separate cache lines (`align(CACHE_LINE)`).
* Producer publishes a slot: `ring[h & MASK] = v; head.store(h+1, .release)`.
* Consumer reads after `head.load(.acquire)`; the acquire pairs with
  the producer's release, carrying the slot write.
* **Block-on-full / block-on-empty** uses the same double-check
  pattern as `WaitGroup.wait`:
  1. Register self in the waiter slot (`store(.release)`).
  2. Re-load the matching counter (`tail` for the producer,
     `head` for the consumer) with `.acquire`. If the condition
     no longer holds, swap the waiter slot back to null and retry.
  3. Otherwise `runtime.park()`. On wake, loop.
* **Wake-up** is single-slot (`swap(null, .acq_rel)`) — the matching
  side reads the slot atomically and unparks the registered
  coroutine.

### TLS (per-thread, never shared)

* `current.current` (current Coroutine pointer on this thread)
* `worker.current_worker` (current Worker pointer on this thread)

These are `threadlocal var ?*T`. No cross-thread access by construction;
no atomics needed.

## Single-worker as a special case

When `workers = 1`, **all of the above orderings still apply**. The
fences are present even when there's only one thread; the optimizer
may elide some of them on a single-core target, but the *source code*
makes the same guarantees. There are no `if (workers == 1)` fast paths
that skip atomics. Single-worker is a configuration, not a code path.

## What's NOT covered by this document

* Performance — orderings here are chosen for *correctness*. Some are
  stronger than strictly necessary on a TSO-like architecture (arm64
  acquire/release map to LDA/STL); none are weaker than required.
  Tightening to `.monotonic` requires a happens-before proof recorded
  in this document.
* Linux/Windows specifics — the model is platform-agnostic. Platform
  notes go in `docs/src/content/docs/architecture/platform-*.md`.
* The reactor's kqueue / epoll / io_uring memory semantics — those
  follow the OS contracts, documented in `reactor_kqueue.zig` etc.

## How to use this document

Adding shared state? Add a section here first. State the
*who-reads / who-writes / what-orders / what-happens-before*. If you
can't fill in that template, your design isn't ready.

Changing an ordering? Justify it here. A weaker ordering is acceptable
only with a happens-before proof against the matching access on the
other thread.

Removing a primitive's "single-thread-safe only" caveat? Update the
relevant section.

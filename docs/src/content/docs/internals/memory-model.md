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
allocator               // immutable after init
workers: []Worker       // pointer + len are immutable; per-worker
                        // state has its own model below
injection               // shared queue (see InjectionQueue)
pending: atomic u32     // *** see "Pending counter" below ***
reactor                 // single-poller (see Reactor)
shutdown: atomic bool   // see "Shutdown flag"
parked_workers: atomic u64 // see "Parker bitmap"
reactor_poller_taken: atomic bool // see "Reactor poller claim"
driver_parker           // see "Driver participation"
```

#### Pending counter

```zig
Runtime.pending: std.atomic.Value(u32)
```

* **Writers**: any worker dispatching a `.done` coroutine
  (`fetchSub(1, .acq_rel)`); `Runtime.spawn` from any thread
  (`fetchAdd(1, .acq_rel)`).
* **Readers**: the driver thread polling for `pending == 0`
  (`load(.acquire)`).
* **Ordering**: `.acq_rel` on RMW. The `fetchSub`'s acquire fence
  pairs with the spawn site's `.release` fence on push (which happens
  *before* the coro can complete), establishing happens-before from
  spawn → coro body → completion → counter decrement.
* **Driver visibility**: when the last coro completes
  (`prev == 1` in fetchSub), the worker unparks `driver_parker`. The
  driver's `.acquire` load of `pending` paired with the unparker's
  fence guarantees the driver sees the result memory and any side
  effects of the root coro.
* **Known limitation (TODO #144)**: this is a single shared atomic
  decremented by every worker. Under high spawn rate it bounces a
  cache line constantly. The philosophy-shaped fix is per-worker
  counters with sum-on-check. Until then, this is correct but
  contended.

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

The thread that calls `Runtime.run(fn, args)` *participates as a worker*
(worker 0). It runs the same `workerLoop` as the spawned workers, with
one extra exit condition: when the root coroutine's `wg.count()` hits 0,
it returns. Other workers continue until `Runtime.deinit`.

This eliminates the previous `driver_parker` special case. The driver
thread is in the parker bitmap, eligible for unpark on new work, just
like any other worker.

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
`src/v2/work_steal_queue.zig` and follows Le et al., *"Correct and
Efficient Work-Stealing for Weak Memory Models"* (PPoPP'13). Summary:

* `tail`: written by owner only; `.release` on push, `.monotonic` reads.
* `head` (packed `(steal_head, real_head)`): all writers CAS with
  `.acq_rel`. The packed two-counter design distinguishes
  "claimed but not released" (in-flight steal) from
  "actually advanced" so producers refuse to overflow into a
  stealing region.
* `buffer[idx]`: atomic slots. Producer writes with `.release`
  before advancing tail. Consumer/stealer reads with `.acquire`
  after observing tail/head.
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
```

* **Counter**: `fetchAdd(.acq_rel)` on `add`, `fetchSub(.acq_rel)` on
  `done`, `load(.acquire)` on `count`.
* **Waiter slot**: `swap(.acq_rel)` on both wait-side (register) and
  done-side (consume).
* **Cross-thread protocol**:
  1. `add(n)`: counter increment. Happens-before the corresponding
     `done`s.
  2. `wait()`: load counter; if 0, return. Else swap self into waiter
     slot; *re-check counter* (to close the race with a `done` that
     completed between the load and the swap); if zero now, swap waiter
     back out and return; else park.
  3. `done()`: counter decrement; if decremented to 0, swap waiter slot
     to null; if it was non-null, unpark that coroutine.
* **Why the re-check works**: the writes are ordered. After `done`'s
  `fetchSub` (`.acq_rel`), it does `waiter.swap(null, .acq_rel)`. If
  wait()'s `waiter.store` happened *before* this swap, done() sees the
  waiter and unparks. If wait()'s `waiter.store` happened *after*, then
  wait()'s subsequent counter recheck sees the decremented value (since
  the counter was decremented before the waiter swap), and wait()
  cleans up its own registration.

### Sync primitives (Mutex, Notify, Semaphore) — currently being rewritten

The current implementations use non-atomic state fields and unsynchronized
wait queues. This is the philosophy-shaped flaw being fixed. See TODOs
#147 and #148. After the rewrite:

* `Mutex.locked: atomic u32` (0 = free, 1 = held). CAS-acquire on lock.
* `Notify.notified: atomic bool` with the same wait/notify protocol as
  `WaitGroup`.
* `Semaphore.permits: atomic u32` with `fetchSub` acquire.
* Wait queue manipulation under a primitive-internal short mutex
  (held only for the queue op, not across park/unpark).

When these land, the memory model entries here will replace the
"currently being rewritten" placeholder.

### `Spsc` channel — currently being rewritten

The current implementation works in single-worker. Cross-thread
correctness requires the standard double-check pattern after registering
the waiter — same pattern as `WaitGroup.wait` above. See TODO #148.

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
that skip atomics in v2. Single-worker is a configuration, not a code
path.

## What's NOT covered by this document

* Performance — orderings here are chosen for *correctness*. Some are
  stronger than strictly necessary on a TSO-like architecture (arm64
  acquire/release map to LDA/STL); none are weaker than required.
  Tightening to `.monotonic` requires a happens-before proof recorded
  in this document.
* Linux/Windows specifics — the model is platform-agnostic. Platform
  notes go in `docs/src/content/docs/internals/platform-*.md`.
* The reactor's kqueue / epoll / io_uring memory semantics — those
  follow the OS contracts, documented in `reactor_kqueue.zig` etc.

## How to use this document

Adding shared state to v2? Add a section here first. State the
*who-reads / who-writes / what-orders / what-happens-before*. If you
can't fill in that template, your design isn't ready.

Changing an ordering? Justify it here. A weaker ordering is acceptable
only with a happens-before proof against the matching access on the
other thread.

Removing a primitive's "single-thread-safe only" caveat? Update the
relevant section.

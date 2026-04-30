---
title: Work Stealing
description: How Volt's scheduler distributes coroutines across worker threads — LIFO slot, local deque, global injection, steal.
---

A work-stealing scheduler distributes runnable tasks across N
worker threads such that every worker stays busy when there's
work to do, with minimal coordination overhead. Volt's scheduler
is the Tokio shape: per-worker LIFO slot + local FIFO deque +
global injection queue + cross-worker steal.

## The dispatch loop

Every Volt worker runs roughly this loop forever:

```
loop {
    1. Have something in my LIFO slot? swap-into it.
    2. Pop from my local deque. Got one? swap-into it.
    3. Drain a small batch from the global injection queue.
       Got any? push onto local deque, retry from 1.
    4. Try to steal from another worker's deque. Got one? swap-into it.
    5. Try to claim the reactor. If I own the claim:
       reactor.poll(timeout); wakes go onto local deque.
       Release the claim; retry from 1.
    6. Park: set my bit in parked_workers; condvar-wait.
}
```

The order is deliberate: local first (cheapest), inject second
(no-coordination wakes go here), steal third (cross-thread
coordination), reactor fourth (block-and-wait), park last
(no work to do).

## The four queues

```
   ┌──── per worker ────┐    ┌──── per worker ────┐
   │                    │    │                    │
   │   LIFO slot        │    │   LIFO slot        │
   │   ┌───────────┐    │    │   ┌───────────┐    │
   │   │  coro*    │    │    │   │  coro*    │    │
   │   └───────────┘    │    │   └───────────┘    │
   │                    │    │                    │
   │   Local deque      │    │   Local deque      │
   │   ┌──────────────┐ │    │   ┌──────────────┐ │
   │   │ A B C D E F  │ │ ◄──┼─► │ U V W X      │ │  ◄── steals
   │   └──────────────┘ │    │   └──────────────┘ │
   │   owner: LIFO  ▲   │    │                    │
   │   thieves: FIFO    │    │                    │
   └─────────┬──────────┘    └─────────┬──────────┘
             │                          │
             │   drain after local      │
             ▼                          ▼
        ┌─────────────────────────────────────┐
        │   Global injection queue (mutex)     │
        │   ┌─────────────────────────┐        │
        │   │ I1  I2  I3  I4  I5      │        │
        │   └─────────────────────────┘        │
        └─────────────────────────────────────┘
                          ▲
                          │ cross-thread spawn,
                          │ reactor wake
                          │
              ┌───────────┴───────────┐
              │   Reactor (kqueue/    │
              │    epoll/uring/IOCP)  │
              └───────────────────────┘
```

Each worker maintains:

- **LIFO slot**: a single coroutine pointer; "the most recent thing
  I scheduled." Hot-cache friendly; the most recently pushed
  coroutine is the most likely to still be in cache.
- **Local deque**: a Chase-Lev work-stealing deque (256 slots by
  default). Owner pushes/pops LIFO from one end; thieves steal
  FIFO from the other.

The runtime maintains:

- **Global injection queue**: a mutex-protected `ArrayList` of
  coroutine pointers. Cross-thread spawns and reactor wakes go
  here; workers drain it after the local deque.
- **Parked-worker bitmap**: 256-bit, sharded across 4 atomic u64s.
  Bit `i` = worker `i` is condvar-parked.

## Why a LIFO slot?

When you `volt.launch(fn, args)` from inside a running coroutine,
the new coroutine usually depends on data the parent just
produced (cache-hot). Running it next preserves that locality.
LIFO slot says "the next thing I dispatch is the most recently
spawned" — same intuition.

If the LIFO slot is occupied when you spawn another, the displaced
coroutine moves to the local deque (FIFO behavior from there).

## Why FIFO for steals?

A worker's local deque has the *oldest* (and probably coldest)
coroutines at the bottom. Thieves steal from the bottom; that
gives the worker its hottest coroutines back to itself, and
thieves get older ones the worker isn't likely to revisit anyway.

Empirically this gives better cache locality than stealing from
the top.

## Steal selection

A worker that needs to steal picks a victim. Volt uses a
(repeatable) per-worker random sequence that visits other
workers in pseudo-random order; the first non-empty deque it
finds yields a steal. Simple, no contention beyond the deque's
own atomics.

If no worker has anything to steal, the worker falls through to
the reactor + park path.

## Why a global injection queue?

Two cases need it:

1. **Cross-thread spawn.** `volt.launch` from a non-Volt OS thread
   (e.g., a callback in another runtime) can't go onto a worker's
   local deque — there's no such worker context. The injection
   queue accepts it, and the next worker to drain picks it up.

2. **Reactor wakes from outside the polling worker.** When the
   reactor delivers a wake event, it pushes the woken coroutine
   onto a worker's deque — usually the worker that currently
   holds the reactor claim. But sometimes the woken coroutine
   originated on a different worker; injection avoids the
   complexity of figuring out "which worker really should run
   this."

The mutex on the injection queue is held briefly — push or pop a
single pointer, release. It doesn't backpressure normal work.

## Worker waking

Idle workers don't busy-poll. They condvar-wait with their bit
set in the parked-workers bitmap.

When a coroutine becomes runnable on a worker that's currently
busy:

1. The runnable coroutine is pushed onto the *busy* worker's
   deque (or the injection queue, depending on the call path).
2. `notifyOneWorker()` runs:
   - Read the parked-workers bitmap.
   - `@ctz` to find the lowest-indexed parked worker.
   - CAS to clear that bit.
   - Signal that worker's condvar.
3. The woken worker re-enters the dispatch loop from step 1.

`@ctz` makes the wake-one selection O(1) regardless of how many
workers are parked. The 256-bit bitmap (sharded across 4 u64
atomics) covers EPYC/Threadripper-class machines.

## Single-poller-claim on the reactor

The reactor is shared across workers. Only one worker calls
`reactor.poll()` at a time, claimed via a CAS on `poll_claim`.
While that worker is in `kqueue_kevent` / `epoll_wait` /
`io_uring_enter`, other workers continue dispatching local work
or steal.

When `poll()` returns, the polling worker pushes wakes onto its
own deque (and possibly the injection queue), releases the claim,
then continues dispatching. Other workers can now claim the
reactor on their next idle iteration.

This avoids the "thundering herd of workers all calling
epoll_wait" pattern that's slow under high load.

## When coroutines move between workers

Coroutines are not pinned to workers. After a suspension, the
coroutine resumes on whichever worker dispatched it next. That
might be:

- The same worker, if the wake came from the same worker.
- A different worker, if a steal or reactor poll happened on
  another worker first.

Anything that was on the coroutine's stack still works because
the stack is virtual memory that hasn't moved (the same address
range, same physical pages, just dispatched on a different OS
thread now).

## Cooperative budgeting

The dispatch loop has an implicit budget: each coroutine runs
until it suspends. Volt does **not** preempt running coroutines
(asynchronous preemption is scaffolded but unwired in v1.0). A
CPU-bound coroutine that never yields holds its worker until it
returns.

This is fine for most workloads — `volt.io.read`, `Channel.recv`,
`Mutex.lock`, etc. all suspend, so any I/O-bound or
synchronization-bound code yields naturally. If you have a
genuinely CPU-only loop (e.g., a hot computation), add `try
volt.yield();` periodically or move it to `volt.spawnBlocking`.

## Why not work-sharing?

The alternative to work-stealing is *work-sharing*: a global
queue everyone pulls from. Simpler, but a single point of
contention. Modern runtimes universally pick work-stealing for the
same reason: better scaling under load.

## Why not centrally-planned distribution?

Some runtimes try to pre-balance work — assign tasks to workers
based on hashes or LSB bits. This works when tasks are short and
homogeneous; it falls over the moment tasks have varying
runtimes (workers with long tasks accumulate, workers with short
ones idle).

Work-stealing self-balances: a worker only takes on more when it
runs out of its own work. Bursty, heterogeneous, or unpredictable
workloads benefit most.

## Files that implement this

- `src/scheduler/worker.zig` — the dispatch loop.
- `src/scheduler/deque.zig` — Chase-Lev work-stealing deque.
- `src/scheduler/injection.zig` — global injection queue.
- `src/scheduler/park.zig` — Park primitive used for worker idle
  + every blocking primitive in Volt.
- `src/runtime.zig` — owns the worker array, the bitmap, the
  reactor claim.

The whole scheduler is ~1000 lines.

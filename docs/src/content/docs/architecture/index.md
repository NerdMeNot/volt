---
title: Architecture
description: How Volt is built. The five components, how a coroutine moves through them, and where to read next depending on what you want to learn.
---

This chapter is a mini-textbook on how a modern stackful M:N
coroutine runtime is built. It's organised top-down: this page
gives you the system shape; each subpage takes one component and
goes deep.

You can read top-to-bottom or jump in. Each page has a "Further
reading" footer pointing at adjacent topics.

## The system, in one diagram

```
                              ┌─────────────────────────────┐
                              │     volt.Runtime            │
                              │                             │
                              │  ms: []M       (OS threads) │
                              │  ps: []P       (per-worker  │
                              │                 sched state)│
                              │  reactor       (kqueue /    │
       │                 epoll /     │
       │                 io_uring /  │
       │                 IOCP)       │
                              │  parking_lot   (sharded     │
                              │                 wait/wake)  │
                              │  stack_arena   (slab,       │
                              │                 lazy        │
                              │                 mprotect)   │
                              └─────────────────────────────┘
                                         │
   ┌─────────────────────────────────────┼─────────────────────────────────────┐
   │                                     │                                     │
   ▼                                     ▼                                     ▼

┌──────────────────────┐        ┌──────────────────────┐         ┌──────────────────────┐
│      M[0]            │        │      M[1]            │   ...   │      M[N-1]          │
│   (driver thread)    │        │   (pthread worker)   │         │   (pthread worker)   │
│                      │        │                      │         │                      │
│  Parker              │        │  Parker              │         │  Parker              │
│  ▲                   │        │  ▲                   │         │  ▲                   │
│  │                   │        │  │                   │         │  │                   │
│  P[0]                │        │  P[1]                │         │  P[N-1]              │
│   ├─ WSQ (256 fixed) │        │   ├─ WSQ (256 fixed) │         │   ├─ WSQ (256 fixed) │
│   ├─ LIFO slot       │        │   ├─ LIFO slot       │         │   ├─ LIFO slot       │
│   ├─ Mailbox (MPMC)  │        │   ├─ Mailbox (MPMC)  │         │   ├─ Mailbox (MPMC)  │
│   ├─ coro pool       │        │   ├─ coro pool       │         │   ├─ coro pool       │
│   └─ stack pool      │        │   └─ stack pool      │         │   └─ stack pool      │
│                      │        │                      │         │                      │
└──────────────────────┘        └──────────────────────┘         └──────────────────────┘
   │                              │                                 │
   │                              │                                 │
   │  ┌──────────────────────────┘                                 │
   │  │  (work stealing across P's WSQs)                            │
   │  ▼                                                             │
   │  ┌────────────────────────────────────────────────────────┐    │
   │  │                  Coroutine                              │    │
   │  │   stack: → slot in stack_arena (1 MiB virtual,          │    │
   │  │                                 16 KiB committed)       │    │
   │  │   ctx:   saved registers (AAPCS64 wide-save)            │    │
   │  │   pending: yield / park / done                          │    │
   │  │   park_state: atomic (RUNNING / NOTIFIED / PARKED)      │    │
   │  └────────────────────────────────────────────────────────┘    │
   │                                                                 │
   │                                                                 │
   └─────────────────────────────────────────────────────────────────┘
```

## The five components

These are the actual moving parts. Everything else is built from
them.

### 1. The M:N scheduler

Volt has **M OS threads** running **N coroutines** where N can be
orders of magnitude larger than M. Each thread (`M` in source) is
bound 1:1 to a per-worker scheduler state (`P` in source) — the M
holds the thread, the P holds the runnable-coroutine queues. The
M's dispatch loop pops from its P's queues, runs the coroutine
until it suspends, repeats.

[Read more →](/architecture/mn-scheduler/)

### 2. Work stealing

When a P's local queue is empty, the M doesn't go to sleep
immediately. It tries to **steal** coroutines from sibling P's. A
fixed-size 256-slot deque per P, Chase-Lev-style: owner FIFO-pops
from the bottom, thieves CAS-pop from the top.

Plus a single-slot **LIFO cache** in front of the queue — the most
recently spawned coroutine sits here for spawn-chain locality, so
the immediate joiner can grab it back with a single CAS (this is
the [direct handoff](/architecture/direct-handoff/) optimization).

Plus a per-P **MPMC mailbox** for cross-thread pushes (unparks
from a different M, queue overflow). One Vyukov ring per P;
producers and consumers can be any thread.

[Read more →](/architecture/work-stealing/)

### 3. The parking lot

When a coroutine blocks — on a Mutex, channel, sleep, join —
it parks. The parking lot is a **sharded hash map from address
to FIFO waiter list**. Every blocking primitive in Volt is built
on `parkOn(addr, validator)`; the address is some atomic field
in the primitive's state.

The validator hook is the load-bearing detail: it lets the parking
lot re-check arbitrary state under the bucket lock, atomically
with queueing the waiter. This closes the register-then-park race
that bedevils ad-hoc wait/wake implementations.

[Read more →](/architecture/parking-lot/)

### 4. The reactor

I/O ops and timers park on the reactor instead of the parking lot.
One reactor per Runtime. Single-poller-claim: one M at a time CASes
the "I'm polling" flag, calls the backend's harvest syscall, and
unparks coroutines back to worker queues. The other workers run
regular work in parallel.

Registrations are one-shot and carry the coroutine pointer as
their wake identity — when the kernel delivers the event, we know
exactly which coroutine to wake. Four backends front the same
interface: kqueue (Darwin/BSD), epoll and io_uring (Linux),
IOCP (Windows, polyfilled as readiness via zero-byte WSARecv).

[Read more →](/architecture/reactor/) — and
[the per-platform deep-dive](/architecture/reactor-backends/).

### 5. The slab arena

Coroutine stacks come from a **single mmap reservation** at
runtime init. N slots of 1 MiB virtual each by default (tunable
via `Runtime.Config.stack_reservation_size`), lazy-mprotect on
first use. Per-P pools cache freed slots for cache locality; arena
is the backing store for cross-P balancing.

The arena replaced a per-spawn `mmap` design that hit a 30× cliff
when batch size exceeded the cache cap. The
[postmortem](/performance/slab-arena-postmortem/) is the receipt.

[Read more →](/architecture/slab-arena/)

## A coroutine's journey

To make the five components concrete, here's what happens when
you call `volt.spawn(fn, .{})` from inside a running coroutine:

```mermaid
sequenceDiagram
    participant Caller as Caller coroutine
    participant Spawn as volt.spawn
    participant Arena as Slab arena
    participant Pool as P's pools
    participant LIFO as P's LIFO slot
    participant Disp as P's dispatch loop
    participant Child as Child coroutine

    Caller->>Spawn: spawn(fn, args)
    Spawn->>Pool: allocCoroutine (Coroutine struct)
    Spawn->>Pool: allocStack (StackPtr)
    alt local pool hit
        Pool-->>Spawn: pop existing slot
    else local pool miss
        Pool->>Arena: arena.alloc()
        Arena-->>Pool: pop slot index
        Pool-->>Spawn: slot pointer
    end
    Spawn->>Spawn: initContext (trampoline + SP + frame ptr)
    Spawn->>LIFO: pushLifo(coro)
    Spawn-->>Caller: *Task(T)

    Note over Caller: Caller continues; may call .join() next

    Disp->>LIFO: popLocal (LIFO slot wins)
    LIFO-->>Disp: child coroutine
    Disp->>Child: context.swap into child's stack
    Child->>Child: runs user fn via trampoline
    Child->>Child: hits end / blocks / yields
    Child->>Disp: context.swap back
    Disp->>Disp: branch on pending (done / yield / park)
```

If the caller does `.join()` immediately after spawn — the common
case — the [direct-handoff path](/architecture/direct-handoff/)
catches this: `join` claims the child back from the LIFO slot via
a single CAS, dispatches it inline, returns its result. Zero
park, zero unpark, zero queue ops.

If the caller goes off and does other work first, the child gets
stolen by another M (or run by this M on a later dispatch tick),
and `join` later parks on the child's done flag via the parking
lot.

## How to read the rest of this chapter

**If you want the high-level model:**
1. [Stackful by design](/architecture/stackful-design/) — why no async/await.
2. [The M:N scheduler](/architecture/mn-scheduler/) — Ms, Ps, dispatch.
3. [Work stealing](/architecture/work-stealing/) — the queue design.

**If you want the wait/wake substrate:**
4. [The parking lot](/architecture/parking-lot/) — universal wait/wake.
5. [The Parker](/architecture/parker/) — OS-level park (`__ulock_wait` / futex).
6. [Direct handoff](/architecture/direct-handoff/) — the spawn-then-join optimization.

**If you want memory + correctness:**
7. [The slab arena](/architecture/slab-arena/) — stack allocation without VM-lock cliffs.
8. [Stack growth on demand](/architecture/stack-growth/) — guard pages + SIGSEGV.
9. [The reactor](/architecture/reactor/) and [its backends](/architecture/reactor-backends/) — non-blocking I/O on kqueue / epoll / io_uring / IOCP.
10. [The context switch](/architecture/context-switch/) — AAPCS64 wide-save asm.
11. [Memory model](/architecture/memory-model/) — atomic orderings per region.

**If you want the algorithm-level deep-dives:**
12. [Cancellation internals](/architecture/cancellation-internals/) — Cancel + parking-lot integration.
13. [Channels internals](/architecture/channels-internals/) — Spsc/Mpmc/Oneshot/Watch/Broadcast layouts.
14. [Chase-Lev deque](/architecture/chase-lev-deque/) — the WSQ algorithm in isolation.
15. [Vyukov MPMC](/architecture/vyukov-mpmc/) — the bounded MPMC ring algorithm.
16. [Semaphore (FIFO)](/architecture/semaphore-algorithm/) — the fair-queueing semaphore.

## Design principles

These show up across every page. They're not invented for Volt —
they're the conventions you'd find in Tokio's design docs, Go's
runtime source, the `parking_lot` crate's README, and the Linux
kernel's lockdep notes. Volt borrows; the value-add is the
integration.

1. **Correctness first; port well-known algorithms.** Tokio's WSQ.
   Chase-Lev. Vyukov MPMC. `parking_lot`'s validator. Go's
   `gopark`/`goready` direct-handoff. We don't invent new algorithms
   in performance-critical paths; we port the ones that have been
   model-checked, fuzzed, or deployed at scale.
2. **Atomic ops need ordering justification.** Every `.acquire` /
   `.release` / `.acq_rel` should be paired against a matching
   read or write in the [memory-model](/architecture/memory-model/)
   doc. If the ordering is `.monotonic`, the comment says why
   it's safe.
3. **No raw pointers across yield points.** A coroutine can resume
   on a different worker thread. Anything threadlocal-cached must
   be re-read after every potential yield.
4. **Stackful means stack contents preserved across suspension.**
   Heap pointers stashed on a coroutine's stack live as long as
   the coroutine. This is what makes the synchronous-shape API
   possible — Pin-free, lifetime-free.
5. **Explicit allocators.** Zig idiom. One allocator in
   `Runtime.Config`; the runtime hands it to primitives that need
   it. No global allocator. No fallback to libc malloc.

## A note on staleness

The doc tree is kept honest: every page links to specific source
files and line numbers where the implementation lives. If a page
diverges from `src/`, the source is authoritative — file an issue
or send a PR. The pages are intended to read forever; the source
is the ground truth.

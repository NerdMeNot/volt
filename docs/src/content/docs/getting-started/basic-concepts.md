---
title: Basic concepts
description: The mental model under Volt — coroutines, workers, the reactor, the parking lot, the slab arena. Five things; everything else is composition.
---

You don't strictly need this page to write Volt programs —
`volt.spawn`, `Task.join`, and the sync primitives behave the way
your intuition from `std.Thread` already expects. But debugging,
performance tuning, and reading source code go much faster once you
have the model.

There are five moving parts. Everything else is built from them.

## 1. The coroutine

A function plus its own stack. `volt.spawn(fn, args)` allocates one
and returns a typed `*Task(T)` handle.

The stack is a **slot in the runtime's slab arena**: a 256 KiB
virtual reservation per coroutine. Only the top 16 KiB is committed
RW at first; deeper recursion grows the stack page-by-page via a
SIGSEGV handler. The bottom page is the guard — overflow there aborts
the process cleanly with a stack trace instead of corrupting heap.

Idle resident memory per coroutine: ~16 KiB (one body page). Idle
virtual: 256 KiB per slot. See [The slab arena](/architecture/) for
the design.

## 2. The worker (M)

An OS thread that runs coroutines. Volt creates `getCpuCount()`
workers by default. The thread calling `Runtime.run` becomes M[0]
when `run` enters its dispatch loop.

Each worker owns:

- A **fixed-256 work-stealing queue** (Chase-Lev-style) for runnable coroutines.
- A **single-slot LIFO cache** for spawn-chain locality — the most
  recently spawned coroutine sits here so the next dispatch can grab
  it without queue ops.
- An **MPMC mailbox** that receives cross-worker pushes
  (unparks-from-elsewhere, queue overflow).
- A **Parker** (`__ulock_wait` on Darwin) for when there's nothing
  to do.

The "P" in the source code (`src/p.zig`) is the scheduler state; the
"M" (`src/worker.zig`) is the OS thread. In Phase 1 they're bound
1:1; later phases may detach.

## 3. The reactor

The kqueue interface to the kernel. One reactor per runtime. Single
poller at a time: any worker can CAS-claim the "I'm polling" flag,
call `kevent`, dispatch woken coroutines back to worker queues, then
release the claim.

Coroutines doing I/O (`accept`, `read`, `writeAll`) park here. Sleep
parks here too — `EVFILT_TIMER` is a kqueue event like any other.

Linux (epoll) and Windows (IOCP) backends are planned; today this
is Darwin-only.

## 4. The parking lot

A sharded hash map from `*const anyopaque` (the "address you're
waiting on") to a FIFO list of waiting coroutines. One mechanism
serves every blocking primitive in Volt — `Mutex.lock`,
`Notify.wait`, `Spsc.recv`, `Task.join`, channel `send`-on-full,
the Parker for OS-level waits — all keyed on the address of their
own state field.

The architectural win: every primitive's waiter list is the same
data structure with the same race-handling. Adding a new sync
primitive doesn't introduce new wait/wake bugs — it reuses the
substrate.

See [The parking lot](/architecture/parking-lot/) for the
validator-under-lock pattern that closes the register-then-park race
generically.

## 5. The Cancel

A handle for **data-driven cancellation**. `volt.Cancel` carries an
atomic flag plus a waiter list. `Cancel.fire()` flips the flag and
unparks every coroutine that registered with it.

Functions opt in by accepting `*Cancel` as a parameter. Cancel-aware
blocking ops (`recvCancel`, `lockCancel`, etc.) check the flag under
their primitive's bucket lock and return `error.Cancelled` if fired.

This is Go's `context.Context` model in shape, with the
parking-lot integration making cancellation propagate cleanly through
arbitrary blocking. There's no `?` operator on every call, no
implicit per-coroutine cancel slot, no global "current task" lookup —
just a value you thread through.

## The lifecycle

```
                                 ┌─ deinit (joins workers, frees slab) ─┐
                                 │                                       │
   Runtime.init   ─►  rt.run(root) ─►  root runs synchronously
        │                  │                  │
        │                  ▼                  │
        │            [M0 dispatch loop]       │
        │                  │                  │
        │                  ▼                  │
        │            pop coroutine            │
        │              from queue ────┐       │
        │                  │          │       │
        │                  ▼          │       ▼
        │           context.swap into ── coroutine runs until it
        │                  ▲                  │
        │                  │                  │
        │            context.swap back ── coroutine.pending = .park
        │                  │                  │
        │                  ▼                  │
        │            register waiter, park    │
        │                  │                  │
        │                  ▼                  │
        │            next iter of dispatch   ◄┘
        │                  │
        │            (worker steals from peers,
        │             polls reactor, parks self
        │             on Parker if nothing to do)
        │                  │
        │                  ▼
        │            unparked by:
        │              • reactor event
        │              • another worker's unpark
        │              • Cancel.fire
        │                  │
        │                  ▼
        │            coroutine back on queue
        │                  │
        ▼                  ▼
   rt.deinit   ◄── root returns its value or error
```

## Suspension points

Volt only suspends at **explicit suspension points**. They are:

- **I/O** — `TcpStream.read` / `writeAll` / `connect`, `TcpListener.accept`.
- **Time** — `volt.sleep(ns)`.
- **Channels** — `Spsc.send` / `recv` (on full / empty),
  `Mpmc.send` / `recv`, `Oneshot.recv`, `Watch.changed`,
  `Broadcast.recv`.
- **Sync** — `Mutex.lock`, `Semaphore.acquire`, `Notify.wait`.
- **Join** — `Task.join`.
- **Explicit yield** — `volt.yield()`.

A function that doesn't call any of these runs to completion without
releasing its worker thread. CPU-bound loops should sprinkle
`volt.yield()` so the worker stays cooperative — and so cancellation
checkpoints work.

## Cancellation as data

Volt's cancellation diverges from Go in the data-flow shape:

```zig
const c = volt.Cancel.init(volt.runtime());
defer c.deinit();

const t = try volt.spawn(longRunning, .{&c});
volt.sleep(100 * std.time.ns_per_ms);
c.fire();  // wakes longRunning from any cancel-aware blocking op
```

The cancellation flows through whatever cancel-aware blocking call
`longRunning` is parked on. The waker doesn't need to know what
`longRunning` is doing.

For pure-CPU work that needs cancellation, call `c.checkpoint() catch
return` periodically — it's a single atomic load + branch when not
fired.

## Memory ownership

- **Stacks** — owned by `Runtime.stack_arena`. Slot reuse is
  per-P-pool LIFO; you never free a stack manually.
- **`Task(T)` handles** — heap-allocated by `volt.spawn`, freed by
  `t.join()`. Don't use `t` after `join`.
- **`Cancel`** — caller-owned (stack or heap). `deinit` asserts the
  waiter list is empty.
- **Channels / Mutex / etc.** — caller-owned. Channels with internal
  state (Watch, Broadcast) have explicit `init`/`deinit`; trivial
  primitives (Mutex) are zero-init structs.
- **The Runtime** — owned by `main` (or wherever you call
  `Runtime.init`). `deinit` releases everything.

## What's NOT here

- **No `async`/`await` keyword.** That's the design.
- **No implicit global runtime.** You construct one with
  `Runtime.init`. Library code that wants to suspend must be called
  from inside a coroutine on that Runtime; `volt.runtime()` looks it
  up via the threadlocal current coroutine.
- **No `spawn-and-detach-implicitly`.** Every `volt.spawn` returns a
  `*Task(T)` you're expected to join. Fire-and-forget without join
  leaks the Task struct.
- **No per-function allocator parameter.** The allocator is in
  `Runtime.Config`; the runtime hands it to primitives that need it.

## Where to go next

- [API Reference](/usage/runtime/) — the public surface, type by type.
- [Recipes](/cookbook/) — concrete patterns built from the five pieces.
- [Architecture](/architecture/) — how this is actually built inside.
- [Glossary](/getting-started/glossary/) — terms used throughout.

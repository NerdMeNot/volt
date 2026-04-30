---
title: Glossary
description: Terms used throughout the Volt docs and source. Read this when you're not sure what a word means.
---

## Coroutine

A function plus a stack the runtime can suspend and resume. In Volt
every concurrent unit of work is a coroutine. Spawned via
`volt.launch(fn, args)` (returns `*Job`) or `volt.spawn(fn, args)`
(returns `*Task(T)`).

Coroutines are *stackful*: each owns a real (growable) stack, so
ordinary control flow — recursion, exception unwinding,
stack-allocated locals — works across suspensions.

## Worker

An OS thread that runs coroutines. Volt creates `getCpuCount()`
workers by default. The bootstrap thread (caller of `volt.run`) is
worker 0; the rest are spawned by the runtime. Each worker owns a
Chase-Lev work-stealing deque.

## Reactor

The OS-level readiness or completion source. Per platform: kqueue
(Darwin/BSD), epoll (Linux), io_uring (Linux 5.4+, parallel
backend), IOCP (Windows, cross-compile only today). The reactor
parks coroutines waiting on I/O or timers and wakes them when the
kernel signals.

Volt uses one reactor per runtime, with a "single-poller-claim" — at
most one worker calls `reactor.poll()` at a time; events are pushed
to that worker's deque.

## Park

The universal suspension primitive. A `Park` is a single atomic
that holds either zero, the parked coroutine pointer, or a "wake
already arrived" sentinel. Every blocking primitive in Volt is built
on Park: channels, mutexes, semaphores, joins, sleeps, I/O waits.

Cancelling a coroutine pokes its current Park (tracked via the
`current_park` field on `Coroutine`), which is what makes
cancellation propagate through arbitrary blocking calls.

## Stack pool

A per-runtime pool of recycled coroutine stacks. When a coroutine
finishes, its stack returns to the pool instead of being unmapped.
The next `volt.launch` / `volt.spawn` pops from the pool before
falling back to `mmap` / `VirtualAlloc`. Drastically reduces the
spawn cost for steady-state spawn-and-complete workloads.

## Injection queue

A mutex-protected global queue used for cross-thread spawns and
reactor wakes. When a coroutine is unparked from a different worker
than the one that dispatched it, it goes here. Workers check the
injection queue after their local deque before stealing.

## Job vs Task

- `Job` is the handle from `volt.launch`. It can be cancelled and
  joined; join returns `error.Cancelled` / `error.StackOverflow` /
  void.
- `Task(T)` is the handle from `volt.spawn`. It wraps a `Job` and
  adds a typed `join()` that returns the user fn's value (or its
  error union plus the runtime errors).

## Scope

A structured-concurrency region. `volt.scope(body)` runs `body(*Scope)`;
when `body` returns or errors, the scope joins every child it
spawned. Child coroutines cannot outlive the scope's body. Equivalent
to Trio's nursery or Kotlin's `coroutineScope`.

## Channel family

Volt's message-passing types, all under `volt.channel.*`:

- `Channel(T)` — bounded MPMC queue (Vyukov ring).
- `Oneshot(T)` — single-sender, single-receiver, single-value.
- `Watch(T)` — single-slot "latest value" with change notification.
- `Broadcast(T)` — fan-out ring buffer; slow receivers get `.lagged(N)`.

All four share a unified error vocabulary (`error.Closed`,
`error.Cancelled`) — defined in `src/channel/errors.zig` and
re-exported as `volt.channel.{SendError, RecvError}`.

## select

`volt.select(.{...})` waits for the first of several channel
operations to complete. Returns a tagged union whose variant matches
the winning branch's field name. Cancels losing branches when one
wins.

## withTimeout

`volt.withTimeout(duration, fn, args)` runs `fn(args)` as a child
coroutine and races it against a deadline. Returns the value, or
`error.Timeout` if the deadline fires first; the child is cancelled
on timeout.

## Slab pool

The per-runtime stack-recycling pool. See **Stack pool** above —
"slab" is the implementation strategy (chunked allocation with a
mutex-protected free list, cap=256).

## Trampoline

The naked-asm function (`voltCoroEntry` on x86_64 / arm64) that runs
the first time a freshly-spawned coroutine resumes. Its only job is
to invoke the user's closure on the new stack. After this initial
call, the coroutine takes over.

## EventSource

The "subscribe-to-this-and-park" interface. A reactor wait, a Park
unpark, a channel waiter list — all expose an EventSource the
worker uses post-yield. `Coroutine.pending_event` points at the
current EventSource the coroutine is parked on. Internal; you
shouldn't see it.

## Cancellation point

A spot in your code where the cancel flag is checked. Every
suspension point is a cancellation point — Park surfaces
`error.Cancelled` if the coroutine was cancelled before or during the
park. For CPU-only loops, call `volt.yield()` to make an explicit
cancellation point.

## Stackful vs Stackless

- **Stackless**: each task is a state machine ~256-512 bytes.
  Requires `async`/`await` syntax. Function coloring; types differ
  per `async fn`. Pin-and-state-machine memory model.
- **Stackful**: each task owns a real growable stack ~4-16 KiB
  resident. No syntax change. Code reads like blocking I/O.

Volt is stackful. Tokio is stackless. Go is stackful. Rust async is
stackless. The choice is determined by language ergonomics; Zig has
no `async`/`await`, which is what makes stackful the right choice
here.

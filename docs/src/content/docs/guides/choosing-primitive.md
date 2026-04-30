---
title: Choosing a Primitive
description: Decision tree for picking the right Volt primitive for a problem.
---

You have N coroutines that need to coordinate. Which primitive
gets you there with the least bookkeeping? This guide is a
decision tree.

## "I want to pass values between coroutines"

| Shape | Use |
|---|---|
| One value, one consumer, one shot | `volt.channel.Oneshot(T)` |
| Many values, one or many producers, one or many consumers, with backpressure | `volt.channel.Channel(T)` |
| Many values, many consumers, every consumer sees every value | `volt.channel.Broadcast(T)` |
| One value that changes, many consumers want only the latest | `volt.channel.Watch(T)` |

If you find yourself wanting "every consumer sees every value
**with backpressure** to producers when any consumer is slow,"
that's a fan-out of N independent `Channel(T)` — Volt doesn't
provide a single primitive for it because the right behavior when
one consumer falls behind depends on your application.

## "I want exclusive access to mutable state"

| Shape | Use |
|---|---|
| Exclusive access to a struct | `volt.sync.Mutex` |
| Many readers / occasional writer | `volt.sync.RwLock` |
| Initialize a value lazily once | `volt.sync.OnceCell(T)` |

For `RwLock` to be worth its overhead, reads should outnumber
writes by ≥10× and the critical section should be substantial. For
a few field reads, plain `Mutex` is faster.

## "I want to bound concurrency"

| Shape | Use |
|---|---|
| At most N concurrent ops | `volt.sync.Semaphore.init(N)` |
| At most N rate-per-second | `volt.sync.Semaphore` + a refill `volt.Interval` |
| Token bucket | same as above |
| Connection pool | `volt.sync.Semaphore` + a free list (`volt.sync.Mutex`-protected) |

See the [rate-limiter cookbook](/cookbook/rate-limiter/) and the
[connection-pool cookbook](/cookbook/connection-pool/) for the full
patterns.

## "I want to wait for an event"

| Shape | Use |
|---|---|
| One waiter, one signaler | `volt.sync.Notify` |
| Many waiters, one signaler | `volt.sync.Notify` (`notifyAll`) |
| N tasks meet at a checkpoint, leader does aggregation | `volt.sync.Barrier.init(N)` |
| Wait for a child task | `Job.join()` / `Task.join()` |
| Wait for ANY of several events | `volt.select(.{...})` (channels only currently) |

`Notify` doesn't have a built-in mutex. If you need "wait, holding
mutex" semantics, lock the mutex *after* `wait()` returns —
Volt's Park is non-spurious so the typical condvar-loop is
unnecessary.

## "I want to spawn child coroutines and ensure they finish"

| Shape | Use |
|---|---|
| Static N children, all complete or all cancel | `volt.scope` |
| Dynamic N children, consume results as they finish | `volt.JoinSet(T)` |
| Manual lifecycle (rare) | `volt.launch` + manually track Jobs |

The `volt.scope` shape should be your default. Reach for `launch`
only when the child genuinely needs to outlive the calling
function (e.g., a TCP server's per-connection handler outlives its
spawning iteration of the accept loop).

## "I want to time-bound an operation"

| Shape | Use |
|---|---|
| Run with a deadline; cancel if exceeded | `volt.withTimeout(dur, fn, args)` |
| Periodic work | `volt.Interval.start(period)` |
| Suspend a coroutine | `volt.sleep(dur)` |
| Track elapsed time | `volt.Instant.now().elapsed()` |

`withTimeout` propagates cancellation through arbitrary nested
suspensions. You don't need to write the inner code to be
cancellation-aware — Park-cancel handles it.

## "I want explicit cancellation"

| Shape | Use |
|---|---|
| Cancel one coroutine | `Job.cancel()` |
| Cancel a hierarchical group | `volt.CancellationToken` + `linkParent` |
| Yield + check for cancel in a CPU loop | `try volt.yield()` |

`volt.CancellationToken` is for cases where you want a "stop button"
that aborts a tree of work without killing the parent task itself.
Most cancellation in Volt happens automatically (timeouts, scope
errors, parent cancel); reach for explicit tokens when those
implicit forms don't fit.

## "I want CPU-heavy work off the loop"

| Shape | Use |
|---|---|
| Single sync function | `volt.spawnBlocking(fn, args)` |
| Many concurrent sync functions | One coroutine per call, each `spawnBlocking` |
| File I/O on platforms without io_uring | `volt.fs` (uses spawnBlocking under the hood) |

`spawnBlocking` parks the *caller*, so concurrent blocking calls
need concurrent coroutines.

## "I want to handle a signal"

| Shape | Use |
|---|---|
| SIGINT or SIGTERM (typical Ctrl+C) | `volt.signal.shutdown()` |
| Just SIGINT | `volt.signal.ctrlC()` |
| Custom set | `volt.signal.SignalListener.init(set)` |

Each listener gets its own `signalfd`; multiple listeners with
overlapping sets work fine.

## "I want to inspect what's happening"

| Question | Answer |
|---|---|
| What tasks are alive? | `volt.observability.snapshot(alloc, rt)` |
| How many tasks ever spawned? | `volt.observability.count(rt)` |
| Per-worker stats? | `volt.observability.metrics(alloc, rt)` |
| Trace a code region? | `volt.tracing.span(opts, body)` |
| Detect leaked tasks in a test? | `volt.testing.assertNoLeaks` |

## Anti-patterns to recognize

- **A `Mutex` around a queue.** That's a `Channel(T)`. Use it.
- **A counter that everyone increments under a `Mutex`.** That's
  an `std.atomic.Value(u64)`. No mutex needed.
- **Polling a flag in a loop with `volt.yield`.** That's almost
  always a `Notify` waiting to happen.
- **`volt.launch` + a `Mutex`-protected `ArrayList(*Job)` + manual
  loop to join all.** That's `volt.scope`. Use it.
- **Threading `*CancellationToken` through every function.**
  Volt's cancel-propagation through Park usually makes this
  unnecessary; only reach for explicit tokens when the implicit
  cancel doesn't fit (e.g., a "stop this batch" button that's not
  a timeout).

## When in doubt

Channels move data between coroutines. Locks protect shared
mutable state. If you're using a lock to serialize *access to a
queue*, swap to a channel.

Most Volt code is some combination of:

- `volt.scope` for structured spawning
- `Channel(T)` for inter-coroutine communication
- `Mutex` for the small bits of shared state that are actually
  shared
- `withTimeout` for any operation that should have a deadline

If your code uses something more exotic, double-check it's
actually needed.

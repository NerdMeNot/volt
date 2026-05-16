---
title: Choosing a primitive
description: Decision tree for picking the right Volt primitive for a problem.
---

You have N coroutines that need to coordinate. Which primitive
gets you there with the least bookkeeping? This guide is a
decision tree against the actual current surface.

## "I want to pass values between coroutines"

| Shape | Use |
|---|---|
| One value, one consumer, one shot | `volt.Oneshot(T)` |
| Many values, exactly 1 producer + 1 consumer, bounded | `volt.Spsc(T, cap)` |
| Many values, M producers / N consumers, bounded | `volt.Mpmc(T, cap)` |
| Many values, many consumers, every consumer sees every value | `volt.Broadcast(T, cap)` |
| One value that changes, many consumers want only the latest | `volt.Watch(T)` |

If you want "every consumer sees every value **with backpressure**
to producers when any consumer is slow," that's a fan-out of N
independent `Mpmc(T, cap)` — Volt doesn't provide a single
primitive for it because the right behavior when one consumer
falls behind depends on your application.

`Spsc` is meaningfully faster than `Mpmc` (12 ns/op vs ~60 ns/op
at 1×1); use it when the 1:1 contract genuinely holds.

## "I want exclusive access to mutable state"

| Shape | Use |
|---|---|
| Exclusive access to a struct | `volt.Mutex` |
| Read-heavy access (no built-in RwLock) | Build one on the parking lot, or use `Mutex` |
| Lazy one-shot initialisation | `volt.Notify` permit-stored, or a `std.once.Once`-style atomic |

Volt does not ship `RwLock`, `Barrier`, or `OnceCell`. Reasons:

- `RwLock`: not core; short critical sections rarely benefit
  enough to justify the type. Build on the parking lot if needed.
- `Barrier`: a `Semaphore.init(0)` + N `release()` calls is the
  barrier. No dedicated type.
- `OnceCell`: an `std.atomic.Value(u8)` state flag plus a `Mutex`
  for the slow path. ~15 lines, application-specific.

## "I want to bound concurrency"

| Shape | Use |
|---|---|
| At most N concurrent ops | `volt.Semaphore.init(N)` |
| At most N rate-per-second | `Semaphore` + a refill coroutine that `sleep`s + `release()`s |
| Connection pool | `Semaphore` + a `Mutex`-protected free list |
| Leaky bucket | `Mpmc(void, cap)` + a drainer coroutine |

See the [rate-limiter recipe](/cookbook/rate-limiter/) and the
[connection-pool recipe](/cookbook/connection-pool/) for the full
patterns.

## "I want to wait for an event"

| Shape | Use |
|---|---|
| One waiter, one signaler | `volt.Notify` |
| Many waiters, one signaler | `volt.Notify.notifyAll()` |
| N tasks meet at a checkpoint | `Semaphore.init(0)` + N `release()` calls |
| Wait for a child task | `Task(T).join()` |
| Wait for ANY of several channels | No `select` in core — see below |

`Notify` doesn't have an attached mutex. If you need "wait,
holding mutex" semantics, lock the mutex *after* `wait()`
returns — Volt's parking lot wakes are non-spurious so the
typical condvar-loop is unnecessary.

For multi-channel select: Volt doesn't ship a `select` primitive
today. Workaround: fan-in via an `Mpmc(Tagged, cap)` where each
"branch" is a coroutine forwarding to the shared channel; the
main loop reads tagged messages.

## "I want to spawn child coroutines and ensure they finish"

| Shape | Use |
|---|---|
| One child, get its result | `const t = try volt.spawn(fn, args); t.join();` |
| N children, all must finish before the parent does | `volt.scope` + explicit `Task.join` per child |
| N children, propagate first error and cancel siblings | `volt.scope` (scope fires Cancel on body error) |
| Truly fire-and-forget (no join) | `_ = try volt.spawn(...)` — leaks the Task |

`volt.scope` is the **structured** spawn pattern. The body takes
a `*Cancel`; if the body returns an error, `scope` fires the
Cancel before propagating. Children opt into cancellation by
accepting `*Cancel` and using cancel-aware blocking ops.

`scope` does **not** auto-await children — the body is responsible
for its own joins. (This is intentional: auto-await without
explicit join order can mask error propagation bugs.)

There is no `JoinSet(T)` in core. For dynamic-N children, keep
a `std.ArrayList(*Task(T))` and join each at the end.

## "I want to time-bound an operation"

| Shape | Use |
|---|---|
| Run with a deadline; cancel if exceeded | `scope` + watchdog (see [Timeout with Retry](/cookbook/timeout-retry/)) |
| Periodic work | Loop with `volt.sleep` at the bottom |
| Suspend a coroutine | `volt.sleep(ns)` |
| Track elapsed time | `std.time.nanoTimestamp()` |

Volt doesn't ship `withTimeout`, `Interval`, or `Instant`. The
recipe at [Timeout with Retry](/cookbook/timeout-retry/) shows
the scope + watchdog idiom; it's ~10 lines and composes cleanly.

## "I want explicit cancellation"

| Shape | Use |
|---|---|
| Cancel a tree of work via a token | `volt.Cancel` (data flowing through `*Cancel` params) |
| Cancellable wait inside a primitive | `recvCancel(&c)`, `lockCancel(&c)`, etc. |
| Periodic check in a CPU loop | `try c.checkpoint()` |

`volt.Cancel` is Go's `context.Context` model. Pass `*Cancel`
through code; library functions take it as a parameter; leaf
blocking ops use the cancel-aware variant. See [Structured
Concurrency](/usage/structured-concurrency/).

There is no `Task.cancel()` — cancellation is data, not a method
on the handle. Fire a Cancel that the task is holding.

## "I want CPU-heavy work off the runtime"

Volt core has no `spawnBlocking`. Bridge via `std.Thread.spawn`
+ a `volt.Mpmc` channel — see [Offloading CPU
work](/cookbook/work-offload/).

## "I want to handle a signal"

Volt doesn't ship user-facing signal handling. The internal
signal handler is for SIGSEGV (stack growth) only. Wire
`std.posix.sigaction` directly; trigger `Notify.notifyAll` or
`Cancel.fire` from the signal handler.

## "I want to inspect what's happening at runtime"

`Runtime.dumpState()` writes per-P / global scheduler atomics to
stderr. Use for hang investigation.

Volt does not ship observability / tracing / metrics surfaces.
Build your own with `std.atomic.Value` counters per primitive,
or wait for a `volt-otel` library on top.

## Anti-patterns to recognize

- **A `Mutex` around a queue.** That's a `Mpmc(T, cap)` (or
  `Spsc(T, cap)` if 1:1). Use it.
- **A counter that everyone increments under a `Mutex`.** That's
  a `std.atomic.Value(u64)`. No mutex needed.
- **Polling a flag in a loop with `volt.yield`.** That's almost
  always a `Notify` waiting to happen.
- **`volt.spawn` + a `Mutex`-protected `ArrayList(*Task)` + manual
  loop to join all.** That's `volt.scope` + the loop. The
  Mutex isn't needed if all spawns happen sequentially in the
  scope body.
- **Threading `*Cancel` through every function "in case."** Only
  thread it through functions that actually park. CPU-only paths
  don't need it.

## When in doubt

Channels move data between coroutines. Locks protect shared
mutable state. If you're using a lock to serialize *access to a
queue*, swap to a channel.

Most Volt code is some combination of:

- `volt.scope` for structured spawning
- `Spsc` / `Mpmc` for inter-coroutine communication
- `Mutex` for the small bits of shared state that are actually
  shared
- `Cancel` + scope + watchdog for deadlines

If your code uses something more exotic, double-check it's
actually needed.

## See also

- [API Reference](/usage/runtime/) — full surface of each primitive.
- [Cookbook](/cookbook/) — concrete recipes built from these primitives.
- [Common pitfalls](/guides/common-pitfalls/) — what goes wrong and how to avoid it.

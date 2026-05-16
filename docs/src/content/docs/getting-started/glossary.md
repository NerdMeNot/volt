---
title: Glossary
description: Terms used throughout the Volt docs and source. Read this when you're not sure what a word means.
---

Alphabetical. Source-file references are the authoritative source
when prose drifts.

## Arena (slab arena)

The single `mmap` reservation owned by `Runtime.stack_arena`. All
coroutine stacks live as fixed-size slots within it. One mmap at
init, one munmap at deinit; per-slot `mprotect` is lazy (fires
once per slot's first use). See `src/stack.zig`.

## Broadcast

A 1:N channel with history. `volt.Broadcast(T, cap)`. Receivers
track their own cursor; slow receivers get `error.Lagged` when
they fall too far behind the producer.

## Cancel

A cancellation handle. `volt.Cancel`. Carries an atomic flag and a
waiter list. `c.fire()` flips the flag and unparks every coroutine
waiting on it. Cancel-aware blocking ops (`recvCancel`, `lockCancel`,
etc.) return `error.Cancelled` when fired. See `src/cancel.zig`.

## Cancellation point

A spot in code where a fired Cancel is observable. Every cancel-aware
blocking op is one. `Cancel.checkpoint()` is the explicit version
for CPU loops that don't otherwise park.

## Chase-Lev deque

The work-stealing queue algorithm Volt's per-worker WSQ uses. Owner
FIFO-pops from the bottom; thieves CAS-pop from the top. Fixed
capacity (256 slots) — overflow spills to the per-P mailbox. See
[Work stealing](/architecture/work-stealing/) and
`src/work_steal_queue.zig`.

## Coroutine

A function plus its own stack. The unit of concurrent work in Volt.
Spawned via `volt.spawn(fn, args)` from inside another coroutine or
`rt.spawn(fn, args)` from outside. Type-erased internally; the user
sees `*Task(T)`. See `src/coroutine.zig`.

## Direct handoff

The optimization in `Task.join` where if the joined-on coroutine is
still in the same worker's LIFO slot, `join` claims it via CAS and
dispatches it inline on the same stack — skipping the
park/unpark/wake round trip. See [Direct
handoff](/architecture/direct-handoff/).

## Driver thread

The OS thread that called `Runtime.run`. It becomes worker M[0] for
the duration of `run` and returns to being a regular thread after
`run` returns.

## Frame

The comptime-generated closure that holds a spawned function's
arguments and call-site shape. Allocated together with the `Task`
struct (`FrameWithTask`). Internal — you don't see it.

## kqueue

Darwin/BSD's event-notification mechanism. Volt's reactor is built
on kqueue. EVFILT_READ / EVFILT_WRITE / EVFILT_TIMER are the three
event filters Volt uses. See `src/reactor_kqueue.zig`.

## LIFO slot

Per-worker single-slot cache holding the most recently spawned
coroutine. Spawn-chain locality — the joiner can grab it back
without a queue op. See `src/p.zig`.

## M / P (M:N scheduler)

Borrowed from Go's runtime nomenclature. **M** is an OS thread
(`src/worker.zig`). **P** is the per-worker scheduler state (queue,
LIFO slot, mailbox, parker handle — `src/p.zig`). In Phase 1 they
bind 1:1; later phases may allow P↔M detach.

## Mailbox

The per-P MPMC queue that absorbs cross-worker pushes (unpark to
another P) and local-queue overflow. Treiber-stack-based. See
`src/worker.zig`.

## Mpmc

`volt.Mpmc(T, cap)`. A Vyukov bounded MPMC ring with per-cell
sequence counters. Block-on-full and block-on-empty go through the
parking lot. See `src/channel.zig`.

## Notify

`volt.Notify`. A "permit + parker" primitive. `notifyOne` releases
one permit (or wakes one waiter); `wait` consumes a permit (or
parks). Used as the building block for one-shot waits, barriers,
done-flags.

## Oneshot

`volt.Oneshot(T)`. A single-value 1:1 channel. `send` consumes the
value; a second `send` returns `error.Closed`. `recv` blocks until
the value lands.

## Parker

The per-worker OS-level wait/wake primitive. Built on
`__ulock_wait` (Darwin) or `futex` (Linux planned). State machine:
EMPTY ↔ WAITING ↔ NOTIFIED. Lost-wake-free. See `src/parker.zig`.

## Parking lot

A sharded hash map from address (`*const anyopaque`) to FIFO waiter
list. The universal wait/wake substrate. Validator-under-lock
closes the register-then-park race. See [The parking
lot](/architecture/parking-lot/) and `src/park.zig`.

## park_state

The per-coroutine atomic state machine (`RUNNING` / `NOTIFIED` /
`PARKED`) that closes the inner register-then-park race. Transition
to PARKED happens *after* the swap-back, inside dispatch — so a
cross-worker unpark fired while the coroutine is still on-CPU
can't cause double-dispatch.

## Reactor

The kqueue (Darwin) / epoll (Linux planned) / IOCP (Windows
planned) event source. One per Runtime. Single-poller-claim: at
most one worker calls the syscall at a time.

## Runtime

`volt.Runtime`. The whole scheduler — workers, reactor, parking
lot, slab arena. Created with `Runtime.init(config)`, torn down
with `deinit`. See `src/runtime.zig`.

## scope

`volt.scope(body)`. Runs `body(*Cancel)`. If `body` errors, scope
fires the Cancel before propagating. Lexical cancellation lifetime —
the minimum-viable "structured concurrency" primitive. Does **not**
auto-await child Tasks; the body is responsible for joins.

## Slab

The single mmap region behind the stack arena. "Slab" = chunked
allocation with fixed-size slots and a free-index stack.

## Spsc

`volt.Spsc(T, cap)`. A single-producer single-consumer ring buffer,
comptime-specialised by capacity. Block-on-full and block-on-empty
via the parking lot. Fastest channel in the family. See
`src/channel.zig`.

## Stackful

Each coroutine owns a real stack. Code can use regular control flow
across suspensions: recursion, large stack locals, pointers to
locals living across yields. Volt is stackful. Tokio (Rust) is
stackless. See [Stackful by design](/architecture/stackful-design/).

## Stackless

Each coroutine is a state machine ~hundreds of bytes. Requires
`async`/`await` syntax. Used by Rust async, JavaScript, C# async.
Zig has no `async`/`await` keyword, which is why Volt is stackful.

## Task(T)

`volt.Task(T)`. The typed handle returned by `volt.spawn`. `t.join()`
parks until the coroutine completes and returns its result (or
error union). Join frees the Task struct. See `src/task.zig`.

## Trampoline

The arm64 assembly function (`voltCoroEntry`) that runs the first
time a freshly-spawned coroutine resumes. Sets up SP and calls the
user closure. After this initial call, the coroutine runs normally.
See `src/context_arm64.zig`.

## Validator

A function pointer used in parking-lot `parkOn(addr, validator)`.
The parking lot calls it under the bucket lock to re-check the
condition (e.g. "is the channel still empty?") atomically with
queueing the waiter. Closes the register-then-park race.

## Vyukov MPMC

Dmitry Vyukov's bounded MPMC ring algorithm. Each cell carries a
sequence counter; producers / consumers advance via CAS. Volt's
`Mpmc` channel and the per-P mailbox both use this shape.

## Watch

`volt.Watch(T)`. A 1:N latest-value channel. Receivers poll
`rx.changed()` then `rx.borrow()`. Intermediate values are silently
dropped (latest-only). Built on a seqlock so readers don't block
writers.

## Work-stealing queue (WSQ)

The per-worker Chase-Lev-style deque. See [Work
stealing](/architecture/work-stealing/) and `src/work_steal_queue.zig`.

## yield

`volt.yield()`. Cooperative re-queue. The coroutine goes to the
worker's queue tail (FIFO — not LIFO slot). Used in CPU loops to
let other coroutines on the same worker make progress, and as a
no-cost cancellation checkpoint when combined with `Cancel`.

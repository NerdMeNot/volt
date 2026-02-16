---
title: Glossary
description: Definitions of key terms used throughout the Volt documentation.
sidebar:
  order: 4
---

Quick reference for terminology used in the Volt documentation and codebase.

## Runtime Concepts

**Runtime**
The engine that drives async tasks to completion. Owns the scheduler, I/O driver, timer wheel, and blocking pool. Created via `volt.run()` or `Io.init()`.

**Io**
The runtime handle, passed explicitly to functions like an `Allocator`. Provides `@"async"()`, `concurrent()`, and access to the runtime's subsystems. The type system ensures you have a running runtime before calling async APIs.

**Worker**
An OS thread managed by the scheduler. Each worker has a local task queue and participates in work stealing. Default count: one per logical CPU core.

**Blocking Pool**
A separate pool of OS threads for CPU-intensive or synchronous blocking work. Threads are spawned on demand (up to 512) and reclaimed after 10 seconds of idleness. Accessed via `io.concurrent()`.

## Task Model

**Task**
A lightweight unit of concurrent work scheduled on the runtime. Each task is a state machine weighing ~256-512 bytes, not a full coroutine stack.

**Future**
A type that represents an async operation in progress. Implements a `poll()` method that returns `.ready` (with a result) or `.pending` (not yet complete). Volt's futures are stackless state machines.

**Future(T)**
The handle returned by `io.@"async"()`. Call `.@"await"(io)` to get the result or `.cancel(io)` to cancel the operation. Alias: `volt.Future(T)`.

**FutureTask**
The internal wrapper that combines a `Future` with a task header (state, ref count, waker). You never interact with `FutureTask` directly.

**Poll / PollResult**
The return type of `Future.poll()`. Either `.ready` (with the output value) or `.pending` (not done yet, will be woken later).

## Scheduling

**LIFO Slot**
A per-worker single-task slot for the most recently spawned task. Provides temporal locality (the child likely touches the same cache lines as the parent). Capped at 3 consecutive polls per tick to prevent starvation.

**Local Queue**
A per-worker 256-slot lock-free ring buffer ([Chase-Lev deque](/algorithms/chase-lev-deque/)) holding pending tasks. The owner thread has fast, uncontested access; other threads can steal from it.

**Global Queue**
A mutex-protected injection queue shared by all workers. Tasks overflow here when local queues are full, and external code (timers, I/O completions) injects tasks here.

**Work Stealing**
When a worker runs out of local tasks, it steals half the queue from a randomly chosen victim worker. See [Work Stealing](/algorithms/work-stealing/).

**Cooperative Budget**
Each worker is allowed 128 polls per tick. After exhausting the budget, the current task is rescheduled to prevent starvation of other tasks.

## Wakers and Notification

**Waker**
A 16-byte callback that reschedules a task when an async event fires. Created by the scheduler, stored by async primitives (mutexes, channels, timers), and invoked when conditions are met.

**Callback / WakerFn**
The function pointer inside a waker. When called, it puts the associated task back on the scheduler's run queue.

## Sync Primitives

**Waiter**
An intrusive struct embedded directly in a future (e.g., `LockFuture`, `AcquireFuture`). Contains list pointers, a waker reference, and completion state. Zero-allocation: no heap alloc per contended wait.

**Intrusive List**
A linked list where the nodes are embedded in the data structures themselves (not separately allocated). Used for waiter queues in mutexes, semaphores, and channels.

## Channel Types

**Channel(T)**
Bounded MPMC (multi-producer, multi-consumer) channel backed by a lock-free [Vyukov ring buffer](/algorithms/vyukov-mpmc/). Requires an allocator and `deinit()`.

**Oneshot(T)**
A single-value channel: one sender, one receiver. Zero-allocation. Ideal for returning results from spawned tasks.

**BroadcastChannel(T)**
Fan-out channel: every subscribed receiver gets a copy of every sent message. Slow receivers may miss messages (ring buffer overwrites).

**Watch(T)**
Single-value channel with change notification. Only the latest value is kept. Ideal for configuration that changes at runtime.

## Two-Tier API

**tryX() (Tier 1)**
Non-blocking operations that work anywhere, with or without a runtime. Examples: `tryLock()`, `tryAcquire()`, `trySend()`, `tryRecv()`. Return immediately with success/failure.

**x(io) (Tier 2)**
Convenience methods that take the `io: volt.Io` handle and suspend the task until the operation completes. Examples: `mutex.lock(io)`, `ch.send(io, val)`, `sem.acquire(io, n)`.

## Additional Terms

**Convenience API**
The "Tier 2" methods that take `io: volt.Io` and suspend the calling task until the operation completes. For example, `mutex.lock(io)` and `ch.send(io, val)`. This is the recommended starting point for async code.

**Future API**
The lower-level API that returns `Future` objects for manual composition. For example, `mutex.lockFuture()` and `ch.sendFuture(val)`. Use when building combinators or integrating with custom schedulers.

**Waiter API**
The lowest-level API for direct intrusive list manipulation. For example, `ch.recvWait(&waiter)`. Intended for library authors building custom scheduler integrations.

**`@""` Syntax (Identifier Quoting)**
Standard Zig syntax for using reserved keywords as identifiers. Volt uses `@"async"` and `@"await"` because `async` and `await` are reserved keywords in Zig. Read `io.@"async"(fn, args)` as "io dot async". This is not Volt-specific.

**deinit Required**
Types that allocate memory (via an `Allocator` parameter) must have `deinit()` called to free it. In Volt, `Channel` and `BroadcastChannel` require `deinit()`. `Oneshot`, `Watch`, and all sync primitives do not.

**Zero-Allocation**
A design where no heap allocation occurs per operation. Volt's sync primitives embed waiter structs directly in futures instead of heap-allocating them. This is why Volt achieves 282 B/op vs Tokio's 1,868 B/op total across all benchmarks.

**Blocking Operation**
An operation that blocks the OS thread (not just the task). On a Volt worker thread, blocking stalls every task on that worker. Examples: `std.Thread.sleep`, `volt.net.resolve()`, `volt.fs.readFile()`. See [Common Pitfalls](/guides/common-pitfalls/#operations-that-block-the-thread).

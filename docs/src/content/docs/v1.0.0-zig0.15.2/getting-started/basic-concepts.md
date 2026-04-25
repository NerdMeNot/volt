---
title: Basic Concepts
description: Understand the fundamental programming model of Volt --
  async/await, the runtime, Groups, and cooperative scheduling.
sidebar:
  order: 3
slug: v1.0.0-zig0.15.2/getting-started/basic-concepts
---

Volt provides async programming for Zig through a simple `async`/`await` API backed by a high-performance work-stealing runtime. This page explains the core concepts you need to understand before writing async code.

## Async and Await

The primary API for concurrent work in Volt uses two operations:

* **`io.@"async"(func, args)`** -- launch a function as an async task, returns a `volt.Future(T)`
* **`future.@"await"(io)`** -- wait for the result of an async operation

```zig
const volt = @import("volt");

fn app(io: volt.Io) !void {
    // Launch an async task
    var f = try io.@"async"(fetchUser, .{@as(u64, 42)});

    // Await the result
    const user = f.@"await"(io);
    _ = user;
}

fn fetchUser(id: u64) []const u8 {
    _ = id;
    return "Alice";
}
```

The function you pass to `io.@"async"` is wrapped in a lightweight task (~256 bytes) and scheduled on the work-stealing runtime. The `Future(T)` returned is a handle to that task's result.

:::note[Why the `@""` syntax?]
Zig reserves `async` and `await` as keywords. Volt uses Zig's identifier quoting syntax (`@"async"`, `@"await"`) to work around this.
:::

### Launching Multiple Tasks

When you need to run several operations concurrently, launch them all before awaiting any:

```zig
fn app(io: volt.Io) !void {
    // Launch both concurrently
    var user_f = try io.@"async"(fetchUser, .{@as(u64, 42)});
    var posts_f = try io.@"async"(fetchPosts, .{@as(u64, 42)});

    // Await both results
    const user = user_f.@"await"(io);
    const posts = posts_f.@"await"(io);
    _ = user;
    _ = posts;
}
```

Both tasks run in parallel on the scheduler. The awaits block the calling task (not the thread) until each result is ready.

### Structured Concurrency with Groups

For spawning a dynamic number of tasks, use `volt.Group`:

```zig
fn app(io: volt.Io) !void {
    var group = volt.Group.init(io);

    // Spawn tasks into the group
    _ = group.spawn(processItem, .{@as(u32, 1)});
    _ = group.spawn(processItem, .{@as(u32, 2)});
    _ = group.spawn(processItem, .{@as(u32, 3)});

    // Wait for all tasks to complete
    group.wait();
}
```

Groups provide structured concurrency -- all spawned tasks are logically scoped to the group. You can also cancel all remaining tasks with `group.cancel()`.

### Cancellation

Individual futures can be cancelled:

```zig
fn app(io: volt.Io) !void {
    var f = try io.@"async"(longRunningTask, .{});

    // ... decide we don't need the result ...
    f.cancel(io);
}
```

***

## The Runtime

The **Runtime** is the engine that drives futures to completion. It owns three subsystems:

### 1. The Work-Stealing Scheduler

The scheduler manages a pool of worker threads, each with:

* A **LIFO slot** -- hot-path shortcut for the most recently spawned task (temporal locality)
* A **local queue** -- 256-slot lock-free ring buffer for pending tasks
* A **global injection queue** -- mutex-protected overflow queue shared by all workers
* A **cooperative budget** -- 128 polls per tick, preventing any single task from starving others

When a worker runs out of local tasks, it **steals** from other workers' queues. The steal target is chosen randomly to avoid contention. Idle workers park on a futex and are woken via a 64-bit bitmap with O(1) lookup using `@ctz`.

### 2. The I/O Driver

Platform-specific I/O multiplexing:

| Platform | Backend | Mechanism |
|----------|---------|-----------|
| Linux 5.1+ | io\_uring | Submission/completion queues in shared memory |
| macOS | kqueue | Edge-triggered event notification |
| Windows 10+ | IOCP | I/O Completion Ports |
| Linux (fallback) | epoll | Edge-triggered file descriptor polling |

The backend is auto-detected at startup. All backends present the same interface to the rest of the runtime.

### 3. The Blocking Pool

CPU-intensive or blocking operations must not run on I/O worker threads (they would stall all async tasks on that worker). The blocking pool provides dedicated threads for this:

```zig
// Offload CPU-heavy work to the blocking pool
fn processData(io: volt.Io, data: []const u8) !Hash {
    var f = try io.concurrent(computeExpensiveHash, .{data});
    const result = try f.@"await"(io);
    return result;
}
```

The pool auto-scales up to 512 threads (configurable) and reclaims idle threads after 10 seconds.

### Starting the Runtime

Two ways to start:

```zig
// Explicit pattern (recommended): create Io like an Allocator
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 8,
        .max_blocking_threads = 128,
        .blocking_keep_alive_ns = 30 * std.time.ns_per_s,
    });
    defer io.deinit();
    try io.run(myApp);
}

// Convenience shorthand: zero-config, auto-detect everything
pub fn main() !void {
    try volt.run(myApp);
}
```

The `Config` fields:

| Field | Default | Description |
|-------|---------|-------------|
| `num_workers` | 0 (auto) | Number of I/O worker threads. 0 means use CPU core count. |
| `max_blocking_threads` | 512 | Maximum threads in the blocking pool. |
| `blocking_keep_alive_ns` | 10s | How long idle blocking threads stay alive. |
| `backend` | `null` (auto) | Force a specific I/O backend. |

***

## Tasks and the Io Handle

A **task** is a lightweight unit of concurrent work scheduled on the runtime. All task operations go through the `io: volt.Io` handle.

### Creating Tasks

There are two primary ways to create tasks:

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    // 1. Async: launch a function as a concurrent task (most common)
    var f = try io.@"async"(myFunc, .{arg1, arg2});
    const result = f.@"await"(io);

    // 2. Concurrent: offload to the blocking thread pool (for CPU-bound work)
    var f2 = try io.concurrent(heavyCompute, .{data});
    const result2 = try f2.@"await"(io);
}
```

| Function | Use case | Runs on |
|----------|----------|---------|
| `io.@"async"(fn, args)` | General async work | Scheduler workers |
| `io.concurrent(fn, args)` | CPU-heavy or blocking I/O | Blocking pool threads (use `.@"await"(io)` for result) |

### Future(T)

`io.@"async"` returns a `volt.Future(T)` -- a handle to the async operation's result:

```zig
pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    var f = try io.@"async"(compute, .{@as(i32, 5)});

    // Await the result (suspends this task, not the thread)
    const result = f.@"await"(io);
    _ = result;

    // Or cancel the operation
    // f.cancel(io);
}
```

### Structured Concurrency with Groups

When you have multiple concurrent tasks, use `volt.Group` for structured concurrency:

```zig
pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    var group = volt.Group.init(io);

    // Spawn tasks into the group
    _ = group.spawn(fetchUser, .{id});
    _ = group.spawn(fetchPosts, .{id});
    _ = group.spawn(fetchComments, .{id});

    // Wait for all tasks to complete
    group.wait();

    // Or cancel all remaining tasks
    // group.cancel();
}
```

***

## Cooperative Scheduling

Volt uses **cooperative scheduling**: tasks voluntarily yield control to the runtime by returning `.pending` from `poll()`. This is different from preemptive scheduling (like OS threads) where the scheduler forcibly interrupts execution.

### Why Cooperative?

Cooperative scheduling avoids the overhead of context switches, signal handling, and stack switching. But it comes with a responsibility: **tasks must not block the worker thread.**

Bad patterns that block:

```zig
// BAD: Blocks the I/O worker thread
std.Thread.sleep(1_000_000_000);

// BAD: CPU-intensive loop without yielding
while (i < 1_000_000) : (i += 1) {
    result += expensiveComputation(i);
}
```

Good alternatives:

```zig
// GOOD: Async sleep (register with timer driver inside the runtime)
var slp = volt.time.sleep(volt.Duration.fromSecs(1));
// In an async context, register with the timer driver for automatic wakeup.
// For blocking contexts, use volt.time.blockingSleep(duration).

// GOOD: Offload to blocking pool
var f = try io.concurrent(batchCompute, .{data});
const result = try f.@"await"(io);
```

### Cooperative Budget

Even well-behaved tasks can accidentally starve others if they generate many sub-tasks. Volt enforces a **cooperative budget of 128 polls per tick**. After 128 polls, the worker forces the current task to yield, ensuring other tasks get CPU time.

This matches Tokio's budget and is invisible to application code -- you do not need to insert manual yield points.

### LIFO Slot and Fairness

When a task spawns a new child task, the child is placed in the worker's LIFO slot for temporal locality (the child likely touches the same cache lines). However, the LIFO slot is capped at 3 consecutive polls per tick to prevent starvation of tasks in the FIFO queue.

***

## Common Mistakes

### 1. Calling async methods without a runtime

Async operations require the `io: volt.Io` handle, which only exists inside a running runtime. The type system catches this at compile time.

```zig
// BAD: No runtime -- this won't compile
pub fn main() !void {
    var mutex = volt.sync.Mutex.init();
    mutex.lock(???); // No `io` handle available
}

// GOOD: Start a runtime first
pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    var mutex = volt.sync.Mutex.init();
    mutex.lock(io); // io handle available inside the runtime
    defer mutex.unlock();
}
```

### 2. Blocking the worker thread

`std.Thread.sleep` and CPU-heavy loops block the OS thread, starving all other tasks on that worker.

```zig
// BAD: Blocks the worker thread -- other tasks on this worker can't run
std.Thread.sleep(1_000_000_000);

// GOOD: Async sleep -- create and register with timer driver
var slp = volt.time.sleep(volt.Duration.fromSecs(1));
_ = slp; // Register with timer driver in async context

// GOOD: Offload CPU-heavy work to the blocking pool
var f = try io.concurrent(expensiveComputation, .{data});
const result = try f.@"await"(io);
```

### 3. Using `const` instead of `var` for futures

Futures are mutated when polled (`.@"await"` calls `poll()` internally). Declaring them as `const` prevents this.

```zig
// BAD: Won't compile -- @"await" mutates the future
const f = try io.@"async"(myFunc, .{});
const result = f.@"await"(io);

// GOOD: Use var so the future can be polled
var f = try io.@"async"(myFunc, .{});
const result = f.@"await"(io);
```

***

## Wakers and Notification

The **Waker** is the mechanism that connects async operations to the scheduler. When an I/O operation, timer, or sync primitive becomes ready, it calls `waker.wake()` to reschedule the waiting task.

### How Wakers Work

The Waker is a lightweight callback (16 bytes) that reschedules a task when an async event fires. When an operation like `mutex.lock(io)` cannot complete immediately, the runtime stores a waker in the waiter struct. When the mutex is later unlocked, the waker fires and the task is rescheduled for polling. You never interact with wakers directly when using the convenience APIs like `mutex.lock(io)`.

For the full waker lifecycle, internal API, and detailed diagrams, see [The Future Model](/v1.0.0-zig0.15.2/design/future-model/).

### Zero-Allocation Waiters

A key design principle: **waiter structs are embedded directly in the future, not heap-allocated.** When you call `mutex.lock()`, the returned `LockFuture` contains an intrusive list node. When the future is polled and the mutex is contended, that node is linked into the mutex's waiter list -- without any heap allocation.

This is why Volt achieves 282 B/op vs Tokio's 1,868 B/op total across all benchmarks: the waiter storage is part of the future's stack frame, not heap-allocated.

***

## Zig's std.Io

For how Volt relates to Zig's `std.Io` (in development, expected in 0.16) and the differences between the two approaches, see [Stackless vs Stackful](/v1.0.0-zig0.15.2/design/stackless-vs-stackful/).

***

## The Two-Tier API Pattern

Every sync primitive and channel in Volt provides two tiers. Understanding the difference is critical because **they have different runtime requirements**.

### Tier 1: `tryX()` -- No Runtime Required

These are pure atomic/lock-free operations that work anywhere -- in `main()`, in a plain thread, in a library, without ever starting a runtime. They return immediately with a success/failure result.

```zig
const volt = @import("volt");

pub fn main() !void {
    // No volt.run(), no Io.init() -- just plain code.

    var mutex = volt.sync.Mutex.init();
    if (mutex.tryLock()) {
        defer mutex.unlock();
        // critical section
    }

    var sem = volt.sync.Semaphore.init(3);
    if (sem.tryAcquire(1)) {
        defer sem.release(1);
        // got permit
    }
}
```

Filesystem operations are also runtime-free -- they are synchronous wrappers around OS calls:

```zig
// All of these work without a runtime
const data = try volt.fs.readFile(allocator, "config.json");
try volt.fs.writeFile("output.txt", "Hello!");
var file = try volt.fs.File.open("data.bin");
defer file.close();
```

Networking convenience functions (`net.listen`, `TcpStream.tryRead`, `UdpSocket.tryRecv`) also work without a runtime -- they are non-blocking OS socket operations.

### Tier 2: `x(io)` -- Runtime Required

These take the `io: volt.Io` handle as a parameter and suspend the calling task (not the OS thread) until the operation completes. Because all Tier 2 operations require the explicit `io` parameter, the type system prevents misuse at compile time -- you physically cannot call `mutex.lock(io)` without an `io` handle, and you cannot create an `io` handle without starting a runtime.

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(myApp); // Start the runtime, pass io to myApp
}

fn myApp(io: volt.Io) !void {
    var mutex = volt.sync.Mutex.init();

    // Blocking lock -- yields to scheduler if contended, returns when acquired
    mutex.lock(io);
    defer mutex.unlock();
    // critical section

    // Async task -- launch and await through the io handle
    const f = try io.@"async"(myFunc, .{});
    const result = f.@"await"(io);
    _ = result;
}
```

### What Requires What

| API | Runtime needed? | Notes |
|-----|----------------|-------|
| `mutex.tryLock()`, `sem.tryAcquire()`, `rwlock.tryReadLock()` | No | Works anywhere |
| `ch.trySend()`, `ch.tryRecv()` | No | Works anywhere |
| `fs.readFile()`, `File.open()`, `readDir()` | No | Synchronous I/O -- works anywhere |
| `net.listen()`, `stream.tryRead()`, `stream.writeAll()` | No | Blocking I/O -- works anywhere |
| `Duration`, `Instant`, `Instant.now()` | No | Just time math |
| `Mutex.init()`, `Semaphore.init()`, `Channel.init()` | No | Just initialization |
| `mutex.lock(io)`, `sem.acquire(io, n)`, `rwlock.readLock(io)` | **Yes** | Convenience methods that take `io` and suspend the task (not the OS thread) until acquired. |
| `ch.send(io, val)`, `ch.recv(io)` | **Yes** | Convenience methods that take `io` and suspend the task (not the OS thread) until send/recv completes. |
| `io.@"async"()`, `io.concurrent()` | **Yes** | Requires `io: volt.Io` -- enforced by the type system at compile time. |
| `future.@"await"(io)`, `future.cancel(io)` | **Yes** (transitively) | Operates on Futures returned by `io.@"async"()`. |
| `volt.Group` | **Yes** | Structured concurrency: `.spawn()`, `.wait()`, `.cancel()`. |
| `fs.readFileAsync()`, `AsyncFile` | **Yes** | Requires runtime I/O backend. |
| `shutdown.Shutdown`, `signal.AsyncSignal` | **Yes** | Requires runtime for async waiting. |

:::tip[Rule of thumb]
If the function name starts with `try` or is a standard file/socket operation, it works without a runtime. If it takes an `io` parameter, it needs the runtime -- and the type system ensures you have one.
:::

### Four API Levels

Volt actually provides four levels of abstraction, not just two. Most developers only need the first two:

| Level | Example | When to use |
|-------|---------|-------------|
| **1. Try (non-blocking)** | `mutex.tryLock()`, `ch.trySend(val)` | Hot paths, polling loops, code that must not block. Works without a runtime. |
| **2. Convenience (async)** | `mutex.lock(io)`, `ch.send(io, val)` | **Start here.** Most async code uses this level. Takes `io`, yields the task. |
| **3. Future** | `mutex.lockFuture()`, `ch.sendFuture(val)` | Manual future composition, combinators, custom scheduling logic. |
| **4. Waiter** | `ch.recvWait(&waiter)` | Custom scheduler integration, raw intrusive list manipulation. Library authors only. |

:::tip
**Start with the convenience API (`x(io)`).** Only drop to a lower level if you have a specific reason -- building a combinator, integrating with a custom scheduler, or optimizing a hot path.
:::

### Choosing the Right Tier

| Situation | Use |
|-----------|-----|
| Hot path, lock rarely contended | `tryLock()` |
| Must acquire, can yield | `mutex.lock(io)` |
| Polling a channel in a loop | `tryRecv()` |
| Waiting for the next message | `ch.recv(io)` |
| Checking semaphore availability | `tryAcquire(n)` |
| Rate limiting with backpressure | `sem.acquire(io, n)` |
| CLI tool, simple script | `tryX()` everywhere, no runtime needed |
| Server handling many connections | Use the runtime + `io` convenience APIs |

***

## Summary

| Concept | What it is |
|---------|-----------|
| **`io.@"async"` / `.@"await"`** | Launch a function as a concurrent task, await its result. The primary API for async work. |
| **`volt.Future(T)`** | A handle to an async operation's result. Supports `.@"await"(io)` and `.cancel(io)`. |
| **`volt.Group`** | Structured concurrency: spawn multiple tasks, wait for all or cancel all. |
| **`io.concurrent`** | Offload CPU-bound or blocking work to the dedicated blocking pool. |
| **Runtime** | The engine: scheduler + I/O driver + blocking pool. Created via `volt.run()` or `Io.init()`. |
| **Waker** | Lightweight callback (16 bytes) that reschedules a task when an async event fires (internal). |
| **Cooperative scheduling** | Tasks yield voluntarily by returning `.pending`. Budget of 128 polls/tick prevents starvation. |
| **Two-tier API** | `tryX()` for non-blocking, `x(io)` for blocking convenience -- available on all sync and channel types. |

Next: explore the [Usage](/v1.0.0-zig0.15.2/usage/runtime/) guides for detailed coverage of each module, or jump to the [Cookbook](/v1.0.0-zig0.15.2/cookbook/) for real-world patterns.

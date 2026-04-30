---
title: Basic Concepts
description: How Volt's stackful model works — coroutines, the scheduler, the reactor, and what "suspending at a wait point" actually does.
---

You don't have to know any of this to use Volt — `volt.run` and the
primitives behave the way `std.Thread.spawn` and `std.Thread.Mutex`
already trained you to expect. But understanding the model makes
debugging and performance tuning much easier when you need them.

## The four moving parts

Volt has exactly four things, and the rest is built from them:

- A **coroutine** is a function plus its own stack. `volt.launch(fn,
  args)` spawns one. The stack is virtual memory: the runtime
  reserves 8 MiB of address space per coroutine but only commits 1
  page (4-16 KiB) up front; more pages get committed on demand when
  the coroutine actually uses more stack.
- A **worker** is an OS thread that runs coroutines. Volt spawns
  `getCpuCount()` workers by default. Each owns a Chase-Lev
  work-stealing deque; idle workers steal from busy ones; a global
  injection queue handles cross-thread spawns and reactor wakes.
- The **reactor** is the OS-level readiness source. On Darwin/BSD
  it's kqueue; on Linux it's epoll (with parallel io_uring); on
  Windows it's IOCP. When a coroutine waits on I/O, sleep, or a
  timer, it parks; the reactor wakes it when the kernel signals
  readiness.
- A **Park** is the universal "wake me later" primitive. Every
  blocking primitive in Volt — channels, mutexes, semaphores,
  notifies, sleeps, I/O waits, joins — is implemented on top of
  `Park`. Cancelling a coroutine pokes the Park it's currently
  parked on; that's what makes timeouts propagate.

That's it. Everything else is composition.

## The lifecycle

```
volt.run(config, root_fn, args)
    │
    ▼
[Runtime starts: workers + reactor + injection queue + stack pool]
    │
    ▼
[Spawn root coroutine on worker 0]
    │
    ├── Coroutine runs synchronously until it suspends
    │   (calls volt.sleep / channel.recv / Mutex.lock / etc.)
    │
    ├── On suspension: save its registers + stack pointer,
    │   register a "wake reason" with the reactor / waiter list,
    │   switch back to the worker's scheduler context
    │
    ├── Worker picks up the next runnable coroutine
    │   (LIFO slot → local deque → steal → injection queue → reactor poll)
    │
    └── When wake fires: coroutine goes back on a worker's deque,
        gets dispatched, switches back into its saved context,
        continues from right after the suspending call.
    │
    ▼
[Root coroutine returns its value or error]
    │
    ▼
[Runtime drains, joins worker threads, deinits the reactor]
```

You will not see any of this in your code. You see `volt.run(config,
serve, .{})` and `serve` reads like a synchronous program. The
runtime does the bookkeeping.

## Suspension points

Volt only suspends at **explicit suspension points**. They are:

- **I/O**: `TcpStream.read` / `writeAll`, `TcpListener.accept`,
  filesystem ops, signal listeners, etc.
- **Time**: `volt.sleep(duration)`, `Interval.tick()`,
  `volt.withTimeout(...)`.
- **Channels**: `Channel.send` / `recv` (when full / empty),
  `Oneshot.recv`, `Watch.changed`, `Broadcast.recv`.
- **Sync primitives**: `Mutex.lock`, `RwLock.lockShared` /
  `lockExclusive`, `Semaphore.acquire`, `Notify.wait`, `Barrier.wait`.
- **Joining**: `Job.join`, `Task.join`, `JoinSet.joinNext`.
- **Explicit yield**: `volt.yield()` — used to give other coroutines
  a chance and to act as a cancellation check inside CPU-bound loops.

A function that doesn't call any of these will run to completion
without releasing its worker thread. If your work is CPU-bound, use
`volt.spawnBlocking` (off the main worker pool) or sprinkle
`volt.yield()` calls so cancellation can propagate.

## Cancellation

This is where Volt diverges most visibly from Go. Cancelling a
coroutine sets a flag *and* unparks whatever it's currently parked
on:

```zig
const j = try volt.launch(longRunning, .{});
volt.sleep(volt.Duration.fromMillis(100)) catch {};
j.cancel();   // wakes longRunning from sleep / I/O / channel — error.Cancelled bubbles
```

There is no `context.Context` to thread through every function and
no `?` operator on every call. The cancellation propagates *into*
whatever the task is doing because Park (the suspension primitive)
is cancellable, and every blocking primitive parks. `volt.withTimeout(dur,
fn, args)` is a watcher built on this.

For a CPU-only loop you have to ask explicitly:

```zig
while (work_remains) {
    try volt.yield();   // returns error.Cancelled if the task was cancelled
    // ... CPU work ...
}
```

## Memory: who owns what

- **Stacks**: owned by the runtime's stack pool. Slab-recycled on
  coroutine completion. You don't free them.
- **Job / Task handles**: heap-allocated by `volt.launch` / `volt.spawn`.
  *You own the handle.* Call `volt.destroyJob(j)` /
  `volt.destroyTask(t)` when you're done with it.
- **Channels, mutexes, etc.**: own whatever they allocate. Call
  their `deinit` (channels, JoinSet) or just let them go out of
  scope (Mutex, Semaphore — zero-allocation).
- **The Runtime**: owned by `volt.run`. Tears down on return.

## Structured concurrency, by default

The simplest Volt program looks like a synchronous program. The
*next* simplest uses `volt.scope`:

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(workerA, .{});
        try s.spawn(workerB, .{});
        // returning here joins both. If either errored, propagate.
    }
}.body);
```

Scopes are the recommended default for spawning more than one
coroutine. They're equivalent to Trio's nurseries / Kotlin's
`coroutineScope`. The guarantee is that **child coroutines cannot
outlive the scope's body** — when `body` returns, every child has
either completed or been cancelled-and-joined. You can't leak.

`volt.launch` (no scope) is for fire-and-forget cases where the
child genuinely needs to outlive the current scope. Reach for it
sparingly.

## What's NOT here

- **No async/await keyword.** That's the whole point. Code that
  suspends looks identical to code that doesn't.
- **No global runtime.** `volt.run` owns the runtime; library code
  that wants to suspend must be called from inside it. There's no
  init-on-first-use mode and no nested `volt.run`.
- **No goroutine-style "spawn and forget by default."** Use
  `volt.scope`.
- **No `*Allocator` parameter on every function.** You pass it once
  to `volt.run` (in the Config); the runtime hands it to
  primitives that need it.

## Where to go next

- [Quick Start](/getting-started/quick-start/) — three runnable
  programs.
- [Glossary](/getting-started/glossary/) — terms used throughout the
  docs and source.
- [Stackless vs Stackful](/design/stackless-vs-stackful/) — why
  Volt picked stackful and what the tradeoff actually buys you.

---
title: Architecture
description: How Volt fits together — Runtime, Worker, Reactor, Coroutine, Park, and how a typical wake travels through the system.
---

This is the system map. If you're trying to understand a stack
trace, debug a missed wake, or contribute to the runtime, start
here.

## The shape

```
            ┌────────────────────────────────────────────────────────────┐
            │                       volt.run(...)                        │
            │            (owns Runtime; tears down on return)            │
            └────────────────────────────────────────────────────────────┘
                                       │
            ┌──────────────────────────▼──────────────────────────┐
            │                      Runtime                         │
            │   workers[] · reactor · injection · stack_pool       │
            │   shutdown_flag · parked_workers bitmap              │
            └──────────────────────────┬──────────────────────────┘
                                       │
   ┌───────────────────────────────────┼───────────────────────────────────┐
   │                                   │                                   │
   ▼                                   ▼                                   ▼
┌─────┐  steal       ┌─────┐  inject  ┌──────────┐  poll        ┌──────────┐
│ W0  │ ◄──────────► │ W1  │ ────►    │Injection │              │ Reactor  │
│LIFO │              │LIFO │          │  queue   │              │ (kqueue/ │
│deque│              │deque│          └──────────┘              │  epoll/  │
└─────┘              └─────┘                                    │   IOCP)  │
                                                                └──────────┘
                                                                      ▲
                                                                      │ park / wake
                                                                      │
                                                                ┌──────────┐
                                                                │Coroutine │
                                                                │ stack +  │
                                                                │ context  │
                                                                │ + Park   │
                                                                └──────────┘
```

Five things, the rest is composition:

- **Runtime** (`src/runtime.zig`) — Owns everything. Created by
  `volt.run`. Holds the worker array, the reactor, the global
  injection queue, the stack pool, the parked-workers bitmap, and
  the shutdown flag.
- **Worker** (`src/scheduler/worker.zig`) — An OS thread plus a
  Chase-Lev work-stealing deque, a LIFO slot, and a Parker. Each
  worker has an ID and a bit in the runtime's `parked_workers`
  bitmap.
- **Reactor** (`src/io/reactor.zig` → `reactor_kqueue.zig` /
  `reactor_epoll.zig` / `reactor_iouring.zig` / `reactor_iocp.zig`)
  — The OS-level readiness/completion source. One per runtime,
  with single-poller-claim (only one worker calls
  `reactor.poll()` at a time).
- **Injection queue** (`src/scheduler/injection.zig`) — A
  mutex-protected global queue. Cross-thread spawns and reactor
  wakes go here; workers check it after their local deque before
  stealing.
- **Coroutine** (`src/coroutine/coroutine.zig`) — A function plus
  a stack plus saved registers (`Context`) plus a
  `current_park` field for cancellation propagation.

## Spawning a coroutine

`volt.launch(fn, args)`:

1. Comptime-specialize a closure for `fn` + `args` (one type per
   call site, see `src/coroutine/spawn.zig`).
2. `Runtime.createCoroutine`:
   - Pop a stack from `stack_pool` if available; otherwise `mmap` /
     `VirtualAlloc` a fresh 8 MiB reservation with 1 page committed.
   - Allocate the Coroutine struct with the closure pointer in the
     stack-top slot the trampoline reads.
   - Initialize the saved-context (registers + SP) so first dispatch
     lands in `voltCoroEntry`.
3. Push the Coroutine onto the calling worker's LIFO slot
   (or onto worker 0's deque if called outside a coroutine, e.g.
   from `volt.run`).
4. If the LIFO slot displaces an existing coroutine, push that one
   onto the local deque tail.
5. Return a `*Job` heap-allocated alongside the Coroutine.

## Dispatching: the worker loop

```
loop {
    1. Have a coroutine ready in the LIFO slot? swap-into it.
    2. Pop the local deque (LIFO for owner, FIFO for thieves).
       Got one? swap-into it.
    3. Drain a small burst from the injection queue. Got any?
       Push to local deque, retry from step 1.
    4. Try to steal from another worker's deque (random victim).
       Got one? swap-into it.
    5. Try to claim the reactor. If we own the claim:
       reactor.poll(timeout) → wakes deliver coroutines onto our
       deque. Release the claim; retry from step 1.
    6. Park: set our bit in parked_workers, condvar-wait.
}
```

The "swap-into" step is the assembly context switch
(`voltCtxSwap` in `src/coroutine/context_arm64.zig` /
`context_x86_64.S`). It saves the worker's callee-saved registers,
loads the coroutine's callee-saved registers, and `ret`s into
whatever address was at the top of the coroutine's saved stack —
either a normal return-point (if resuming) or the trampoline (if
first-dispatch).

## Suspending: the coroutine side

When a coroutine calls `volt.sleep`, `Channel.recv`, `Mutex.lock`,
or any blocking primitive:

1. The primitive registers an "I want to wake when X happens" with
   the appropriate **EventSource** — a reactor wait, a channel
   waiter list, a mutex waiter list, etc.
2. The primitive sets `coroutine.current_park = &this_park`. This
   is what makes cancellation work: cancelling pokes whatever Park
   the coroutine is currently parked on.
3. The coroutine calls `voltCtxSwap(&coro.ctx, coro.scheduler_ctx)`.
   This saves the coroutine's registers into `coro.ctx`, loads the
   worker's registers from `scheduler_ctx`, and `ret`s into the
   worker loop right after the previous swap-in.
4. The coroutine is now suspended. Worker continues from step 1
   above.

When the wake fires (reactor delivers, channel sends, mutex
unlocks), the EventSource pushes the coroutine back onto a worker's
deque. On next dispatch, the coroutine swap-ins, returns from
`voltCtxSwap`, and continues from right after the suspension call.

## Cancellation propagation

`Job.cancel()` does two things:

```zig
pub fn cancel(self: *Coroutine) void {
    self.cancel_flag.store(true, .release);
    const park_addr = self.current_park.load(.acquire);
    if (park_addr != 0) {
        const park: *Park = @ptrFromInt(park_addr);
        park.unpark();
    }
}
```

The flag store causes the next `parkCurrent()` call to return
`error.Cancelled` immediately. The unpark wakes the coroutine if
it's currently parked. So:

- Cancelled before the next suspend: caller sees `error.Cancelled`
  on next park.
- Cancelled while parked on I/O / sleep / channel / mutex: park
  surfaces `error.Cancelled` immediately.

This is what makes `volt.withTimeout(dur, fn, args)` work without
any cooperation from `fn`. The watcher calls `child.cancel()` when
the timer fires; whatever the child was parked on releases.

## I/O wake path

A typical TCP read:

```
coroutine: stream.read(&buf)
    │  syscall → EWOULDBLOCK
    │  reactor.registerWait(fd, .readable, target=&coro)
    │  coro.current_park = &this_park
    │  voltCtxSwap(&coro.ctx, scheduler_ctx)   ─── coroutine suspends here
    ▼
    [scheduler dispatches other work]
    [reactor.poll() blocks on kqueue_kevent / epoll_wait]
    [kernel delivers EVFILT_READ / EPOLLIN for fd]
    [reactor unparks the target coro]
    [coro lands back on a worker's deque]
    │
    ▼
    [worker swap-ins coro]
    │  voltCtxSwap returns
    │  reactor read syscall now succeeds
    │  return n bytes
    └──► caller continues
```

The "lands back on a worker's deque" step might be on a *different*
worker than the one the coroutine was last on. That's fine —
coroutines are not pinned to workers. Anything that was on the
coroutine's stack still works because the stack is virtual memory
that hasn't moved.

## Stack pool

`Done.subscribe` (in `src/coroutine/event_source.zig`) handles
coroutine completion. When a coroutine returns from its top-level
function:

1. The trampoline writes the result into the result slot.
2. Sets the coroutine's state to `.done`.
3. Wakes the joiner (if any) parked on `join_park`.
4. Pushes the coroutine's stack onto `Runtime.stack_pool` (skipping
   root coroutines, which `volt.run` owns).

The next `createCoroutine` call pops from the pool first. Saves a
~µs `mmap` / `mprotect` syscall pair per spawn, which is the
dominant cost in a tight spawn-and-complete loop.

## Worker waking

Idle workers don't poll. They condvar-wait with their bit set in
`Runtime.parked_workers` (a 256-bit bitmap sharded across 4 atomic
u64 words, indexed by `@ctz` for O(1) wake-one).

When a coroutine becomes runnable on a worker that's currently
busy, the runtime:

1. Pushes onto a worker's deque (or the injection queue if no
   worker is identifiable).
2. Calls `notifyOneWorker()` → pick a parked worker via `@ctz` on
   the bitmap, clear its bit, signal its condvar.
3. The woken worker rejoins the dispatch loop from step 1.

Multi-worker wakes always go through this path, which is why
cross-thread spawns and reactor wakes touch the injection queue
unconditionally — local-deque pushes alone wouldn't notify a
parked worker.

## File map

| Concept | Source |
|---|---|
| Runtime + Config | `src/runtime.zig` |
| Worker, dispatch loop | `src/scheduler/worker.zig` |
| Chase-Lev deque | `src/scheduler/deque.zig` |
| Injection queue | `src/scheduler/injection.zig` |
| TLS (current coro/worker/runtime) | `src/scheduler/current.zig` |
| Park primitive | `src/scheduler/park.zig` |
| Coroutine | `src/coroutine/coroutine.zig` |
| Context switch (asm) | `src/coroutine/context_{arm64.zig, x86_64.S}` |
| Stack alloc / pool | `src/coroutine/stack.zig`, `stack_pool.zig` |
| Stack overflow recovery | `src/coroutine/stack_overflow.zig` |
| Event sources | `src/coroutine/event_source.zig` |
| Reactor dispatcher | `src/io/reactor.zig` |
| kqueue backend | `src/io/reactor_kqueue.zig` |
| epoll backend | `src/io/reactor_epoll.zig` |
| io_uring backend | `src/io/reactor_iouring.zig` |
| IOCP backend | `src/io/reactor_iocp.zig` |
| Public API surface | `src/lib.zig` |

The whole runtime is ~10K lines. If you can read it, you can
modify it.

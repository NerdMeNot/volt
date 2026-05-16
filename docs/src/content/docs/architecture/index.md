---
title: Architecture
description: How Volt fits together — Runtime, Worker, Reactor, Coroutine, Park, and how a typical wake travels through the system.
---

:::caution
**Partially stale (2026-05-15).** Path references and some
implementation details (cancellation, EventSource, 8 MiB stacks)
predate the flattening + parking-lot migration that produced the
current tree. The high-level shape (Runtime owns workers,
work-stealing, reactor, suspend/resume via context switch) is still
correct. Authoritative source for specifics: read the actual
`src/*.zig` files. Path map below reflects the current tree;
conceptual details flagged inline.
:::

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
- **M (Worker)** (`src/worker.zig`) — An OS thread. Bound 1:1 to a P.
- **P (Processor)** (`src/p.zig`) — Owns the scheduler state: a fixed
  256-slot lock-free work-stealing queue, a single-slot LIFO cache,
  an MPMC mailbox (for cross-P pushes), and per-P Coroutine + stack
  pools. Each P has an ID and a bit in the runtime's `parked_workers`
  bitmap. The M:N split lets a future Phase 5 detach M from P during
  blocking syscalls.
- **Reactor** (`src/reactor_kqueue.zig` only currently; Linux/Windows
  backends planned) — The OS-level readiness source. One per runtime,
  with single-poller-claim (only one M calls `reactor.poll()` at a
  time).
- **Mailbox** (per-P, in `src/worker.zig`) — A lock-free MPMC Treiber
  stack. Cross-P pushes target a specific P's mailbox; reduces
  cache-line contention vs a single global injection queue.
- **Coroutine** (`src/coroutine.zig`) — A function plus a stack plus
  saved registers (`Context`). No cancellation/`current_park` field
  in current build — that's tracked work for later.

## Spawning a coroutine

`rt.spawn(fn, args)` — see `src/runtime.zig` `Runtime.spawn`:

1. Comptime-specialize a `Combined { frame: F, task: Task(T) }`
   struct for this (fn, args, return) tuple. Frame is at offset 0
   so voltCoroEntry's `*x19 = run_fn` cast still works.
2. Allocate the Combined (one allocator call).
3. Pop a Coroutine struct from the current P's pool (or
   `allocator.create` on miss).
4. Pop a 16 KiB stack from the current P's pool (or `alignedAlloc`
   on miss).
5. Initialize Frame + Coroutine + saved-context. The context's
   `lr = voltCoroEntry`, `sp = stack_top`, `x19 = &combined.frame`.
6. Initialize the Task half of Combined with `frame_destroy =
   &Combined.destroy` so Task.join eventually frees the whole
   combined allocation.
7. Push the Coroutine onto the current P's `lifo_slot` (with the
   evicted prior, if any, landing on the local WSQ).
8. `wakeOneParked` — wakes a sibling M if `num_searching == 0`.
9. Return `&combined.task`.

For spawns from outside a worker context (e.g. pre-`run()` setup),
the Combined goes to `ps[0].mailbox`.

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
(`voltCtxSwap` in `src/context_arm64.zig` — x86_64 backend planned).
It saves the worker's callee-saved registers (14 wide-save GPR +
NEON pairs), loads the coroutine's callee-saved registers, and
`ret`s into whatever address was at the top of the coroutine's
saved `lr` — either a normal return-point (if resuming) or
`voltCoroEntry` → trampoline (if first-dispatch).

## Suspending: the coroutine side

When a coroutine calls `Mutex.lock` (and the mutex is held),
`Spsc.recv` (on empty), `Task.join` (and the task isn't done yet),
or any blocking primitive:

1. The primitive enqueues the coroutine on its **parking lot** bucket
   (or, in the case of `sync.WaitQueue`, on its own waiter list —
   tracked for migration in #168). The bucket lock is held only across
   the few-pointer enqueue.
2. The primitive calls `runtime.park()` which sets
   `c.pending = .park`, then `context.swap(&c.ctx, c.main_ctx)`. This
   saves the coroutine's registers into `c.ctx`, loads the worker's
   registers from `c.main_ctx`, and `ret`s into the worker's dispatch
   loop right after the previous swap-in.
3. The worker's `.park` branch in `dispatch` does a final CAS
   RUNNING → PARKED on `c.park_state` to close a register-then-park
   race (see `docs/src/content/docs/architecture/parking-lot.md`).

When the wake fires, the primitive's "release" / "send" / "task done"
path calls `parkingLot.unparkOne(addr)` → pops the waiter from the
bucket and calls `runtime.unpark(coro)`. That pushes the coro back
onto a P's mailbox and `wakeOneParked`s. On next dispatch, the
coroutine swap-ins, returns from `runtime.park()`, and continues
from right after the suspension call.

## Cancellation propagation

Not implemented in the current build. The pre-stackful tree had a
`Coroutine.cancel` + `current_park` design; that was retired with the
flattening and not yet re-built. Re-landing it is on the roadmap but
unscoped.

## I/O wake path

A typical TCP read (see `src/net.zig` + `src/reactor_kqueue.zig`):

```
coroutine: stream.read(&buf)
    │  syscall → EWOULDBLOCK
    │  reactor.registerWait(fd, .readable)
    │  runtime.park()   ─── coroutine suspends here, swap to m.main_ctx
    ▼
    [worker dispatches other work]
    [some worker's findWork loop calls reactor.poll() — single-poller-claim]
    [kqueue delivers EVFILT_READ for fd]
    [reactor dispatches the waiting coro to a P's mailbox + wakeOneParked]
    │
    ▼
    [worker pops coro from mailbox, dispatch swaps in]
    │  runtime.park() returns
    │  caller retries the read syscall → succeeds
    │  return n bytes
    └──► caller continues
```

The "lands back on a P's mailbox" step might be on a *different*
worker than the one the coroutine was last on. That's fine —
coroutines are not pinned to workers. Anything that was on the
coroutine's stack still works because the stack memory hasn't moved.

## Coroutine completion

When a coroutine returns from its top-level function:

1. The trampoline writes the result into Frame's result slot.
2. Sets `coroutine.pending = .done` and swaps back to `c.main_ctx`.
3. Worker's `dispatch` sees `.done`:
   - Signals `task.done` (via `parkingLot.unparkOne(&done)`).
   - For tasks without a `Task` handle, calls `frame_destroy`.
   - Returns Coroutine + stack to the current P's pools.

Per-P pools cap at 64 (Coroutine struct) / unbounded (stack — see
`p.zig` for current sizing). Pool hits avoid an allocator round-trip
per spawn — the steady-state allocation cost is dominated by the
`Combined{Frame,Task}` create.

## Worker waking

Idle Ms don't poll. They block in `Parker.park()` with their bit
set in `Runtime.parked_workers` (a u64 bitmap — caps `MAX_WORKERS`
at 64).

When a coroutine becomes runnable while a sibling M is parked,
the runtime:

1. Pushes onto a P's lifo_slot or local queue (spawn) / mailbox
   (cross-P unpark).
2. Calls `wakeOneParked()`:
   - Anti-herd: if `num_searching > 0`, return immediately (a
     searching M will find the work on its next loop).
   - Otherwise: CAS-clear one bit in `parked_workers`, call that
     M's `parker.unpark()`.
3. The woken M re-enters its dispatch loop.

## Parker (lost-wake-free)

`src/parker.zig` — a single-atomic state machine over `u32`:
`EMPTY` (0) | `NOTIFIED` (1) | `WAITING` (2). The slow-path block
uses `__ulock_wait` on Darwin / `futex` on Linux (no
`std.Thread.Condition` — those were removed in Zig 0.16).

Two transitions matter:

- `park()`: cmpxchg `NOTIFIED → EMPTY` (fast path); if that fails,
  cmpxchg `EMPTY → WAITING` and block on `futexWait` until
  `unpark` writes a non-WAITING value.
- `unpark()`: `swap(NOTIFIED)`; if the prior value was `WAITING`,
  call `futexWake`.

The seq_cst swap in `unpark` + the seq_cst cmpxchg in `park` (step
EMPTY → WAITING) linearize through one modification order. Two
relative orders, both safe:

- park.cmpxchg first → state is WAITING → unpark's swap sees
  WAITING → futexWake → park returns from futexWait.
- unpark.swap first → state is NOTIFIED → park.cmpxchg fails →
  park returns immediately (no syscall).

Earlier versions had a 30 s park watchdog as defense-in-depth;
the current Parker doesn't. If a wake is lost (memory-ordering
bug), the symptom is a hang in `Runtime.deinit` at the `m.thread.join`
loop. The stress test exercises this nightly under cross-thread
churn.

## Shutdown protocol

`Runtime.deinit`:

```text
1. shutdown.store(true, .release)
2. for m in ms[1..]: m.parker.unpark()
3. for m in ms[1..]: m.thread.join()
4. for m in ms: m.deinit()
5. for p in ps: p.drainPools(allocator, STACK_SIZE)
6. allocator.free ms, ps; reactor.deinit; parking_lot.deinit
7. allocator.destroy(self)
```

Every M's dispatch loop checks `shutdown` after each find-work
miss. With the parker unparked in step 2, even Ms blocked in
`futexWait` wake up, observe shutdown, and exit. Step 3 then
returns immediately for all spawned Ms.

## File map

| Concept | Source |
|---|---|
| Runtime + Config | `src/runtime.zig` |
| M (worker thread) + Mailbox | `src/worker.zig` |
| P (processor/scheduler unit) + per-P pools | `src/p.zig` |
| Work-stealing queue (Chase-Lev-style, fixed 256) | `src/work_steal_queue.zig` |
| Parker (__ulock / futex) | `src/parker.zig` |
| Parking lot (sync wait queues) | `src/park.zig` |
| TLS current coroutine | `src/current.zig` |
| Coroutine struct + Frame factory | `src/coroutine.zig` |
| Context switch (asm) | `src/context_arm64.zig` (x86_64 planned) |
| Task(T) typed handle | `src/task.zig` |
| Spsc(T, cap) channel | `src/channel.zig` |
| Mutex/Notify/Semaphore | `src/sync.zig` |
| kqueue reactor (Darwin) | `src/reactor_kqueue.zig` |
| TCP listener/stream | `src/net.zig` |
| Public surface | `src/lib.zig` |

The runtime is roughly 5K lines of source (down from ~10K in
v0.x). If you can read it, you can modify it.

---
title: M:N scheduler design
description: Three-level (M:P:G) architecture for low-coordination-overhead concurrency.
---

# Volt scheduler — M:N architecture

## Problem statement

The current Volt scheduler is **two-level**: workers ARE OS threads.
Each worker owns its run queue, lifo_slot, parker, etc. This is simpler
than M:N but pays in two structural ways:

1. **Coordination overhead grows super-linearly with workers.** Every
   spawn touches a global injection queue + parker bitmap + num_searching
   counter. At 11 workers, each task takes ~900 ns vs ~150 ns at 1
   worker — most of that is cross-CPU cache traffic on shared
   atomics, not useful work.

2. **I/O blocks the worker.** When a worker is in `kevent` /
   `epoll_wait`, its run queue is stuck. We mitigate with
   single-poller-claim, but the work assigned to that worker can't
   progress.

Both are addressable with the same architectural pivot: **decouple the
scheduler unit from the OS thread.**

## The three-level model (M : P : G)

| Layer | Owns | Lifetime |
|---|---|---|
| **M** — OS thread | An attached P (when running); nothing else | Pool of N at startup; can grow for blocking syscalls |
| **P** — processor | Run queue, lifo_slot, mailbox, coroutine pool, stack pool, parking-lot shard | Fixed at startup, `= NumCPU` by default |
| **G** — coroutine | Stack, ctx, frame | One per spawn |

```
                          Runtime
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
            P[0]           P[1]          P[NumCPU-1]
       ┌─────┴─────┐  ┌─────┴─────┐
       │ run queue │  │ run queue │
       │ lifo_slot │  │ lifo_slot │
       │ mailbox   │  │ mailbox   │
       │ coro_pool │  │ coro_pool │
       │ stack_pool│  │ stack_pool│
       └─────▲─────┘  └─────▲─────┘
             │ attached     │ attached
            M[A]           M[B]   ... and detached M's in a pool

Goroutines (G's) live on a P's run queue or are currently running on
the M attached to that P.
```

### Invariants

- **A P has at most one M attached.** If P has no M, P is *idle*.
- **An M has at most one P attached.** If M has no P, M is in the
  pool, waiting for work.
- **A G runs on exactly one M at any time.** It can migrate between
  P's across park/unpark cycles (an unpark can target any P).

## Detach / attach protocol

The reason for the third level: M can voluntarily release its P when
it knows it's about to block on something the kernel handles.

```
M starts dispatch loop:
  loop:
    G = P.pickNext()
    if !G:
      P.state = idle
      M returns to pool
      done
    run G on M
    if G hits a syscall that may block (e.g. reactor.poll):
      M.detachP()          # P goes back to runtime; another M can attach
      M does the syscall
      after syscall: M.tryReattach() or M re-enters pool
    else:
      G yields/parks/dones → swap to main → loop
```

The detach happens **before** the blocking syscall. The Runtime sees a
P with work and no attached M; an idle M from the pool attaches to it
and starts running its G's. The original M, when its syscall returns,
either reattaches to the same P (if still idle) or any idle P, or
re-enters the pool.

### Net effect

A reactor poll no longer stalls the queue of G's that happened to be
on the polling worker. The P containing those G's gets picked up by
another M, runs to completion or its own park point. The reactor M,
when ready, has fresh work waiting.

## What goes in P

Everything currently in `Worker` that isn't OS-thread-specific moves
to P:

| Today | M:N |
|---|---|
| `Worker.local` (WSQ) | `P.local` |
| `Worker.lifo_slot` | `P.lifo_slot` |
| `Worker.parker` | **stays on M** — it's an OS-thread sleep |
| `Worker.main_ctx` | **stays on M** — it's where ctx-swap returns |
| `Worker.thread` | becomes `M.thread` |
| `Worker.id` | becomes `P.id` |
| (new) | `P.mailbox` — MPSC, target for cross-P spawns |
| (new) | `P.coro_pool` — recycled Coroutine structs |
| (new) | `P.stack_pool` — recycled stacks |
| (new) | `P.state` — running/idle/detached |
| (new) | `P.attached_m` — `?*M` |
| `Runtime.injection` | retire — replaced by per-P mailboxes |
| `Runtime.parked_workers` bitmap | becomes `Runtime.idle_p` bitmap |
| `Runtime.num_searching` | becomes per-state-class counters |

The `parking_lot` stays as-is — it's address-keyed and doesn't care
about P/M.

## Stack model — page-size + guard + grow + per-P pool

### Layout per coroutine

```
[ guard page  | committed stack (initial 4 KB / 16 KB) | uncommitted PROT_NONE up to 8 MB ]
                                                                       ↑
                                                              stack top, grows down → hits committed boundary

Virtual reservation: 8 MB per coroutine (cheap — page tables only)
Initial committed:   4 KB on Linux / 16 KB on Darwin (page-size minimum)
Guard page:          1 page below the committed region (PROT_NONE)
                     stack overflow → SIGSEGV → handler grows committed region
```

### Why page-size minimum

The MMU operates at page granularity. Sub-page `PROT_NONE` isn't
possible — no guard means no overflow detection. Below page size, we'd
trade safety for memory, and Zig doesn't have the compiler stackmaps
needed to do Go-style copy-grow safely.

Page size on supported targets:
- Linux x86_64: **4 KB**
- Linux ARM64 (default kernel): 4 KB
- Darwin ARM64 (Apple Silicon): **16 KB** (hardware choice)
- Linux ARM64 (16K kernel, e.g. Asahi): 16 KB

### Grow protocol

SIGSEGV signal handler:
1. Catch fault address.
2. Look up which coroutine's stack region contains the address.
3. If it's within the virtual reservation and adjacent to committed:
   `mprotect(commit_top - PAGE, PAGE, PROT_READ | PROT_WRITE)`.
4. Resume — the faulting instruction retries and succeeds.
5. If it's beyond the virtual reservation (true overflow): abort.

The handler must be reentrant-safe and signal-safe. Standard pattern;
Go and Rust both implement it.

### Per-P stack pool

Each P holds a fixed-size LIFO of recycled stack regions (say, 64
slots). On spawn:
- Pop a region from P's pool. If empty, mmap a fresh one.
- Stack starts at `committed_top`; grows down.
- Initial commit is page-size; grows on faults.

On coroutine `.done`:
- Reset committed region back to page-size (mprotect down).
- Push region back to P's pool. If pool full, munmap.

**The win is here, not in absolute stack size.** Pool means
no syscall on the spawn fast path. Bulk mmap at runtime init or on
first overflow.

### Memory budget

10,000 idle coroutines:
- Virtual reservation: 80 GB (cheap — page tables ~80 MB)
- Committed: 40 MB (Linux) / 160 MB (Darwin)
- Each coroutine's resident set: 4 KB stack + ~8 KB page tables

This is in the same ballpark as Go's 2 KB + copy-grow (~40 MB resident
for 10K coroutines). Slightly more on Darwin because of the 16 KB
page floor.

## Direct handoff for spawn-then-immediately-join

The most common async pattern is **spawn one, await it**:

```zig
const result = (try rt.spawn(f, .{x})).join();
```

Today: spawn enqueues the child, parent eventually parks on the
child's `done`, dispatcher picks up child, runs it, child stores done,
dispatcher pops parent from injection, runs parent. **~7 atomic ops,
2 queue round-trips, 2 ctx swaps.**

With M:N + handoff: parent realizes the next thing it does is wait,
so it directly swaps to child. No queueing.

```
parent calls spawn(f, args):
   alloc Coroutine + Stack from P.pool
   initContext(child)
   if parent is about to join immediately AND P.lifo_slot is empty:
     parent.state = waiting_for_child
     parent.continuation_target = child  # store back-pointer
     ctx_swap(&parent.ctx, &child.ctx)   # direct switch, no queueing
   else:
     P.pushLifo(child)
     return Task handle as usual

child runs to .done:
   if c.continuation_target != null:        # parent is waiting on us
     parent = c.continuation_target
     parent.state = running
     ctx_swap(&c.ctx, &parent.ctx)          # direct return to parent
   else:
     normal done path — store done, unpark waiters
```

**~2 atomic ops, 0 queue round-trips, 2 ctx swaps.** Down from ~7 +
queue traffic. Estimated ~300 ns savings per spawn-join cycle for the
common case.

For spawn patterns that don't immediately join (fan-out, fire-and-
forget), the normal queue path runs.

## Per-P mailbox (cross-P spawn)

Replaces the single global injection queue.

```
P.mailbox: MPSC queue (many producers, one consumer = owning P)
```

When P_A's spawn fills its local queue / lifo_slot:
- Pick a target P_B (round-robin, or based on cache topology heuristic).
- Push the G to P_B's mailbox.
- If P_B is idle, signal it (set `P_B.has_work` bit; if its M is
  parked, wake).

Mailbox is **MPSC** — multiple workers push, only the owning P pops.
This means:
- Push: CAS on the mailbox tail (multiple writers possible).
- Pop: plain load on the head (owner only).

Mailbox impl: same shape as `InjectionQueue` today (Treiber stack) but
per-P. Net: 11 separate cache lines instead of 1 hot one.

### Stealing still happens, but rarer

A P that's empty (local + mailbox both empty) steals from a sibling P's
local queue. Same algorithm as today's WorkStealQueue. The mailbox is
the **primary** cross-P channel; stealing is the fallback.

## What survives from today

- **Parking lot** (`src/park.zig`) — unchanged. Address-keyed.
- **WorkStealQueue** (`src/work_steal_queue.zig`) — becomes
  `P.local`. Same algorithm.
- **Parker** (`src/parker.zig`) — stays on M. Each M has a parker for
  OS-thread sleep.
- **Coroutine struct + ctx swap asm** — minor changes (add
  `continuation_target`, `home_p` fields).
- **Reactor** — becomes M:N-aware: M doing kevent detaches its P
  before the syscall.
- **Stress test** — extends to exercise detach/attach + handoff.

## What changes

- **Worker → P + M split.** Largest single change. Most of
  `runtime.zig`.
- **Dispatch loop on M.** M picks/steals a P, runs it.
- **Spawn fast path.** Goes through P.lifo_slot or P.local; cross-P
  via mailbox.
- **Stack/coroutine allocation.** Through P's pools.
- **Run-on-root for `Runtime.run`.** Calling thread becomes an M
  attached to P[0]. Same shape as today's "driver thread is worker
  0", just expressed as M:P.

## Migration plan

Eight phases. Each gated on `zig build test` + `zig build stress`
passing.

1. **Introduce P as a separate type.** Rename current `Worker` to
   `M`, extract its scheduler state (queue, lifo_slot, etc.) into a
   new `P` struct. Each M has one P bound to it 1:1. No detach yet.
   Behavior unchanged. *Refactor, no semantic change.*

2. **Add P.mailbox.** Cross-thread spawns push to target P's mailbox
   instead of global injection queue. Retire the global injection
   queue. *Cross-thread cache traffic reduced.*

3. **Add P.coro_pool + P.stack_pool.** Per-P recycling for Coroutine
   and stack. *Allocator off the spawn fast path.*

4. **Implement stack mmap + guard + grow.** Each stack is a mmap'd
   region with a guard page. SIGSEGV handler grows on overflow.
   *Smaller initial commit; safe grow.*

5. **Implement detach / attach.** M can release its P before a
   blocking syscall; idle M attaches to a P with work. Reactor uses
   this. *I/O scaling fix.*

6. **Implement direct handoff.** Spawn-then-immediately-join uses
   `continuation_target` and bypasses queues. *spawn+join hot
   path wins.*

7. **Tune.** Mailbox capacity, stealing topology, parker bitmap
   layout. *Profile-guided.*

8. **Cleanup.** Retire obsolete code, document final memory model,
   update CLAUDE.md and BENCHMARKS.md.

Total: estimated **6-10 weeks** of focused work. Each phase is its
own commit (or small commit cluster) with a clear pass/fail bar.

## Risks

- **Stack grow signal handler.** The SIGSEGV handler must be
  reentrant + signal-safe. Standard pattern but easy to mess up.
  Reference implementations: Go's `runtime/signal_unix.go`, Boost's
  fiber stack handler.

- **Detach race during dispatch.** When M decides to detach but the
  attached P is mid-dispatch, the protocol needs careful ordering.
  Design uses an atomic `P.state` field with three states
  (idle / running / detached) and CAS transitions.

- **Continuation handoff edge cases.** What if the child also
  spawns-then-joins? Recursion through `continuation_target` must
  terminate. Solution: handoff only one level deep; deeper recursion
  falls back to queue path.

- **Cross-platform stack signal handling.** Linux SIGSEGV + Darwin
  EXC_BAD_ACCESS have different conventions. The current Volt only
  needs Darwin support; Linux comes later (task #149).

## Comparison to Go

The architecture mirrors Go's M:P:G exactly. Differences:

| Aspect | Go | Volt (proposed) |
|---|---|---|
| Compile-time stack-check prologues | Yes | No (mmap+guard instead) |
| Copy-grow stacks | Yes | No (mprotect-grow instead) |
| Compiler-emitted goexit | Yes | No (runtime trampoline) |
| Per-P G pool | Yes | Yes (P.coro_pool) |
| M:N detach | Yes | Yes |
| Direct handoff | Yes (`g.preempt`, `gogo`) | Yes (planned) |

The structural disadvantages we can't fix (compiler features) cost
maybe ~50–100 ns/op vs Go. Everything else we can match. Realistic
target: **200–350 ns/op at workers=NumCPU** for spawn+join, down
from current ~900 ns.

## Why this is the right time

Now is the cheapest time to do this restructure:

- **Parking lot is in place** and tested. Sync primitives use it.
  Reactor will. Doesn't conflict with M:N — orthogonal.
- **WSQ algorithm is solid.** Becomes P.local unchanged.
- **Stress test exists** to gate the rewrite.
- **No external consumers yet** — API churn is acceptable.
- **The smaller the codebase, the cheaper the refactor.** Volt is
  ~2,500 lines today. The rewrite touches maybe 1,200 of them.

Every primitive we add and every API consumer that lands makes this
rewrite more expensive. Doing it before the M:N pivot — building
RwLock, Broadcast, fs, process, etc. on top of the current scheduler
— means rewriting them when we pivot.

The decision is: **commit to M:N now and pay the 6-10 weeks once, or
defer and pay it in interest later.**

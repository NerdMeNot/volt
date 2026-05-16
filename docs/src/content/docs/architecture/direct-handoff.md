---
title: Direct handoff
description: The spawn-then-join fast path. How a Task.join after a fresh spawn skips the parking lot entirely by claiming the child from its LIFO slot.
---

`volt.spawn` followed immediately by `Task.join` is the most
common parent/child pattern in coroutine code. Naively it costs a
full park-and-wake round trip: caller pushes child to its M's
LIFO slot, child runs on some M (maybe the same, maybe stolen),
child sets done flag, child unparks caller, caller wakes,
caller reads result.

The runtime detects this pattern at `join` time and **dispatches
the child inline on the caller's stack instead.** Zero park, zero
unpark, zero kernel-level wake.

The pattern is borrowed from Go's `gopark`/`goready` design and
Tokio's spawn-then-await fast path. Same trick, same effect:
synchronization stays in userspace when the joiner and joinee
share an M.

## Mental model

> When you spawn a child, the runtime puts it into your worker's
> **LIFO slot** — a single-element parking spot reserved for "the
> next thing you'll probably want to run". If you immediately
> call `.join()`, the runtime checks "is my child still in the
> LIFO slot?" via a single CAS. If yes, it grabs the child back,
> runs it right there on the current stack, and returns its
> result. The whole interaction looks like a normal function
> call from your perspective.

The LIFO slot is owner-only — only the owning P pushes to it; only
the owning P (or a thief, though steal misses LIFO by design) pops
from it. That's what lets the CAS work: there's exactly one place
the child could be if no one else has touched it.

## The slow path (what we're skipping)

Without direct handoff, the spawn-then-join sequence is:

```
caller M[0]:
  spawn(child)
    → pushLifo(child)            // child sits in P[0].lifo_slot
  ...
  task.join()
    → parkOn(&self.done, ...)    // caller parks on the task's done flag
       → bucket lock acquire
       → check validator (done not set)
       → enqueue waiter
       → context.swap back to dispatch loop

dispatch loop M[0] (or whichever M picks up next):
  popLocal()
    → lifo_slot first              // returns child
  context.swap into child
  child runs, sets pending = .done
  context.swap back

dispatch loop:
  branch on .done
    → set done flag (release)
    → unparkOne(&self.done)
       → bucket lock acquire
       → pop waiter (= caller)
       → push caller to a P's mailbox
  loop

dispatch loop (eventually):
  popMailbox() returns caller
  context.swap into caller
  caller's parkOn returns
  caller reads result, returns
```

Two context switches into the worker's dispatch loop, two bucket
lock acquires, one wake. On `bench-mutex` (which is dominated by
park/wake), this was measured around 1.4 µs of kernel-level work
per cycle — most of it kqueue or `__ulock_wait` overhead.

## The fast path

`Task.join` opens with a fast-path attempt:

```zig
pub fn join(self: *Self) T {
    if (self.done.load(.acquire) == NOT_DONE and current.get() != null) {
        _ = runtime.tryDispatchInline(self.coro);
    }
    while (self.done.load(.acquire) == NOT_DONE) {
        // ... slow path: parkOn(&self.done, ...) ...
    }
    // ... read result, free Task ...
}
```

Reference: `src/task.zig` — `Task.join`.

`tryDispatchInline(target)` does:

1. **Claim from LIFO.** Call `P.tryRemoveLifo(target)` — a single
   CAS on the lifo_slot field, target → null. Returns `true` if
   we got `target` out of the slot, `false` if it had moved.

2. **Rewire `target.main_ctx`.** Point `target.main_ctx` at the
   caller's ctx. When the child does `context.swap(&self.ctx,
   self.main_ctx)` to suspend, the swap-back lands on the
   caller's stack, not the worker's dispatch-loop stack.

3. **`context.swap` into target.** Caller suspends; target runs.

4. **Handle the swap-back.** Target finished, yielded, or parked.
   Three cases:
   - `.done`: target completed normally. Mirror dispatch's
     `.done` branch: free target's coroutine + stack via the
     per-P pools, the done flag is now set, the joiner's loop
     reads the result and returns.
   - `.yield`: target re-queues itself; `tryDispatchInline`
     returns `false` (the join loop falls through to the slow
     path).
   - `.park`: target is mid-park; mirror dispatch's `.park`
     branch (CAS RUNNING → PARKED, handle the NOTIFIED race);
     `tryDispatchInline` returns `false`.

Reference: `src/runtime.zig` — `tryDispatchInline`, with the
three-arm switch on `pending`.

## The CAS

```zig
pub fn tryRemoveLifo(self: *P, target: *Coroutine) bool {
    const cur = self.lifo_slot.load(.acquire);
    if (cur != target) return false;
    return self.lifo_slot.cmpxchgStrong(target, null, .acq_rel, .acquire) == null;
}
```

Reference: `src/p.zig:124-131`.

This is the entire claim. One load + one CAS. On hit, the LIFO
slot is now null and `target` is exclusively ours. On miss
(target was evicted by another spawn or stolen — the latter
shouldn't happen since stealers don't touch LIFO, but the CAS is
defensive), we return false and the caller falls through.

## Why it's safe

Three races to think about:

### 1. Target was evicted from LIFO

Some other spawn happened between our spawn and our join,
evicting our `target` from the LIFO slot (the new spawn pushes,
the old occupant goes to the local queue or mailbox).
`tryRemoveLifo` sees `cur != target` (or the CAS fails) and
returns false. The slow path parks on `target.done`. Some M (this
one or another) picks `target` up from the local queue and runs it
normally.

No correctness impact — `target` runs, eventually sets done,
eventually unparks the joiner.

### 2. Target yielded instead of completing

We dispatched target inline; target hit a `volt.yield()` somewhere
and swapped back. `target.pending == .yield`. The handler in
`tryDispatchInline`'s `.yield` branch re-queues target via
`m.p.pushQueue(target)` (queue tail, FIFO — yields don't bounce
through LIFO). `tryDispatchInline` returns false; the join's
outer loop falls through to the slow path (`parkOn(&self.done)`).

The slight inefficiency: target gets re-queued to its own P's
queue, will eventually be dispatched again. No worse than the
no-handoff baseline.

### 3. Target parked instead of completing

We dispatched target inline; target hit a `parkOn` somewhere
(channel recv, mutex lock, etc.). `target.pending == .park`. The
handler does the same `park_state` transition that dispatch's
`.park` branch does: CAS RUNNING → PARKED, with a check for the
NOTIFIED race (if some other coro fired unpark while target was
mid-park, we re-queue target instead of leaving it parked).

`tryDispatchInline` returns false; the join's outer loop falls
through. Target's eventual completion will unpark the join via
the normal path.

### Stale `target.main_ctx`

After a `.yield` or `.park` return, `target.main_ctx` still points
at the caller's ctx — that's stale (the caller has moved on to
the slow path, possibly even parked itself). Won't cause a bug
because the next time `target` is dispatched, the worker's
`dispatch(rt, m, c)` resets `c.main_ctx = &m.main_ctx`. The
stale value is overwritten before any swap reads it.

## What it doesn't do (yet)

- **No handoff from the local WSQ.** Only from LIFO. The "spawn
  N tasks then join N tasks" pattern means by join-time only the
  last spawn is in LIFO; the rest are in the local queue.
  Extending to "pop specific item" from the WSQ would need a
  data-structure change (the lock-free Chase-Lev deque doesn't
  support arbitrary removal). Tracked as future work in the
  [multi-worker profile](/performance/multi-worker-profile/).

- **No cross-M handoff.** If target is in a sibling P's queue,
  we don't yank it across. Stealing handles that case via normal
  dispatch.

## Measured effect

On workloads where the joinee is reliably in LIFO at join time:

| Bench | Before | After | Δ |
|---|---|---|---|
| `bench-mutex` contended | 728 ns | 644 ns | +11.5 % |
| `bench-tcp-echo` median | 7,282 ns | 6,940 ns | +4.7 % |
| `bench-fanout-scaling` ratio @ w=11 | 1.82× | 1.58× | −13 % |
| `bench-parallel-compute` @ 8 workers | 6.44× speedup | 6.62× | +2.8 % |

On workloads where target is *not* in LIFO at join time (e.g.
spawn 1000, join 1000 in a loop — by join time all but one are in
the local queue), no measurable change. That's expected.

## Tried & rejected: handoff from local WSQ

We could extend `tryRemoveLifo` to also try popping target from
the local Chase-Lev queue. Two issues:

1. **The lock-free WSQ doesn't support arbitrary removal.**
   Chase-Lev is FIFO from owner, LIFO from thieves; pop-by-pointer
   isn't part of the algorithm. Adding it would require a
   different queue design (probably a linked list of queues with
   an extra "remove" path), losing some of the WSQ's
   wait-freedom.

2. **The win is bounded.** If target is in the local queue, it's
   already going to be dispatched next by this M (since the
   joiner just parked). The slow path takes one extra
   dispatch-loop iteration — nothing kernel-level. The benefit
   would be skipping that iteration; tens of nanoseconds.

Volt picked: keep LIFO direct-handoff, accept that WSQ-handoff
isn't worth the data-structure cost.

## Tried & rejected: greedier handoff (handoff before park)

Alternative: instead of `tryDispatchInline` running once and
falling through, we could loop — keep claiming and running
children until the joiner's target is done.

Doesn't fit the API model. `Task.join` waits for **one specific**
task. Running other tasks inline opportunistically would block on
*those* tasks completing, even though the joiner only cares about
its own target. We'd need cooperative inline-dispatch boundaries,
which is what the dispatch loop already provides.

## Further reading

- [The M:N scheduler](/architecture/mn-scheduler/) — the dispatch loop direct-handoff plugs into.
- [Work stealing](/architecture/work-stealing/) — the LIFO slot's relationship to the WSQ.
- [Multi-worker profile](/performance/multi-worker-profile/) — direct handoff is finding #3 in the perf investigation.
- Go's `runtime/proc.go` — `gopark`/`goready` is the original direct-handoff trick.
- Tokio's "spawn-then-await fast path" — same pattern in Rust's async world.

---
title: "Direct handoff on `Task.join` — how it works"
---

When a coroutine calls `task.join()` on a task it just spawned, the
runtime tries to **dispatch the spawned coro inline on the same M**
instead of parking the joiner and waiting for some other M to pick
the work up. This saves the `parkOn → wakeOneParked → futex_wake →
re-dispatch` round-trip (which is ~1.4 µs on Darwin's `__ulock`) for
the common spawn-then-immediately-join pattern.

The pattern matches Go's `gopark` / `goready` on `wg.Wait` — same
trick, same effect: synchronization stays in userspace when the
joinee and joiner are on the same M.

## Mechanism

Two pieces, both in `src/`:

- `P.tryRemoveLifo(target: *Coroutine) bool` (in `src/p.zig`) —
  single-CAS pop of `target` from `lifo_slot` if it's there.
  Returns `false` if `target` has already moved (evicted to local
  queue by another spawn, or stolen). Owner-only.

- `runtime.tryDispatchInline(target)` (in `src/runtime.zig`) —
  calls `tryRemoveLifo`, then rewires `target.main_ctx` to point at
  the caller's ctx so the swap-back lands on the caller's stack
  instead of the M's dispatch loop. Context-switches into target,
  runs it, then handles whichever of `.done` / `.yield` / `.park`
  fires at swap-back.

`Task.join` calls `tryDispatchInline(self.coro)` once before its
`parkOn` loop. On hit: target runs to completion, joiner returns
with no kernel hops. On miss: falls through to the standard
park-and-wait path silently.

## Why it's safe

Three races to think about, none of which need locking:

**Target was stolen before we got to join.** `tryRemoveLifo` does
an atomic CAS on the lifo slot. If `target != cur_in_slot`, we lose
and return `false`; the caller takes the slow path. No priority
inversion — the stealer or another worker is running `target` now.

**Target was already evicted by another spawn.** Same outcome —
lifo CAS misses, fall through. Target is in local queue, will be
picked up by normal dispatch.

**Target yields or parks instead of completing.** Handled by the
`switch (target.pending)` on swap-back:

- `.done`: standard cleanup (signal task.done, unpark joiner — which
  is us, no-op — free stack + Coroutine via per-P pools). Return
  `true`.
- `.yield`: re-queue target with `m.p.pushQueue(target)`. Return
  `false` — caller falls through to its parkOn.
- `.park`: target is mid-parking. Mirror `dispatch`'s `.park` branch
  (CAS RUNNING → PARKED, handle NOTIFIED race by re-queueing).
  Return `false`.

In `.yield` and `.park` cases, `target.main_ctx` is left pointing at
the caller's ctx — that's stale and gets overwritten on next
dispatch entry by `dispatch(rt, m, c)` which sets
`c.main_ctx = &m.main_ctx`. No leak.

## What it doesn't do (yet)

- **No handoff from the local WSQ.** Only from `lifo_slot`. The
  "spawn N tasks then join N tasks" pattern means by join-time only
  the last spawn is in lifo; the rest are in the local queue.
  Extending to local would need a "pop specific item" WSQ method
  that the lock-free queue doesn't support natively. Tracked as a
  known limit in `multi-worker-profile.md`.

- **No handoff across Ms.** If the target is in a sibling P's queue,
  we don't yank it. Stealing handles that case via normal dispatch.

## Measured effect

On the workloads where the joinee is reliably in lifo at join time:

| Bench | Before | After | Δ |
|---|---|---|---|
| `bench-mutex` contended | 728 ns | 644 ns | +11.5 % |
| `bench-tcp-echo` median | 7,282 ns | 6,940 ns | +4.7 % |
| `bench-fanout-scaling` ratio @ w=11 | 1.82× | 1.58× | −13 % |
| `bench-parallel-compute` @ 8 workers | 6.44× speedup | 6.62× | +2.8 % |

On workloads where it's *not* in lifo (the bench-spawn-hot pattern
described above), no measurable change. That's the expected behavior.

## Code references

- `src/p.zig` — `P.tryRemoveLifo` plus three unit tests covering
  hit, miss, and not-in-slot cases.
- `src/runtime.zig` — `tryDispatchInline` with the three-arm switch
  on `pending`, plus two integration tests (`inlineRoot`,
  `inlineMissRoot`) verifying the result flow and the
  evicted-from-lifo fall-through.
- `src/task.zig` — `Task.join` wires the fast path. Note the comment
  about `self` living inside the Combined{Frame, Task} allocation
  after #173 landed — `frame_destroy` is the only free path.

## See also

- `docs/src/content/docs/performance/multi-worker-profile.md` — broader scheduler
  investigation; direct handoff is finding #3 in that doc.
- `docs/src/content/docs/architecture/parking-lot.md` — the parking-lot path the
  fast path skips.
- `docs/src/content/docs/architecture/mn-scheduler.md` — the M:N scheduler design that
  determines what "current P's lifo slot" means.

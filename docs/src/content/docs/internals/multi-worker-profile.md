---
title: "Multi-worker scheduler — profile findings and tunings"
---

A single-session investigation (2026-05-15) of where Volt's multi-worker
scheduler spends time relative to single-worker, with profile-driven
fixes and a Go side-by-side comparison.

The session moved the spawn-hot multi-worker number from a measured **11×
behind Go** at the start to **2.84× behind** at the end, on a matched
benchmark shape. Most of that headline number was a benchmark artifact —
the rest was real wins from three specific changes.

## The starting picture

Volt's `bench-spawn-hot` (one driver spawns 1000 trivial coros, then
joins all 1000 individually, repeat for 10 s) showed dramatic
multi-worker degradation:

| Workers | ns/op | vs w=1 |
|---|---|---|
| 1 | 103 | 1.0× — **beats Go's 149 ns by 1.45×** |
| 2 | 649 | 6.3× slower |
| 4 | 1,705 | 16× slower |
| 8 | 1,920 | 19× slower |
| 11 | 1,584 | 15× slower |

The cliff between workers=1 and workers=2 is the tell: **the cost is
the *existence* of cross-M coordination**, not the number of Ms.
Going from 1 to 2 Ms adds 6×; going from 2 to 11 adds another ~3×.

## What samply showed (and didn't)

CPU sampling with `samply record` at workers={1, 2, 8} revealed the
unexpected: **no kernel hops in the profile.** `__ulock_wait` and
`__ulock_wake` did not appear in the top 30 hot functions at any
worker count. The "futex wake floor" theory was wrong for this
workload — workers spin in `tryFindAndDispatch` and rarely park long
enough to show up.

What did appear, ranked by inclusive %:

| At w=8 | incl% | Note |
|---|---|---|
| `start.main` | 87 % | ReleaseFast inlined ~everything into here |
| spawned-worker thread `entryFn` | 68 % | total time across all M[1..N] |
| `runtime.Runtime.spawn` | 10.5 % | spawn cost |
| `heap.SmpAllocator.alloc` | 6.4 % | allocator |
| `runtime.tryFindAndDispatch` | 1.7 % | dispatch loop body |

At workers=2 specifically, `SmpAllocator.alloc` jumped from 5 % to
23 %. First clue that allocation contention was real, but not the
whole story.

## Four findings, in the order they were uncovered

### 1. Runtime-level stat counters were a hidden cache-line ping-pong

`Runtime` had four `std.atomic.Value(u64)` diagnostic counters
(`stat_spawned`, `stat_done`, `stat_fairness_hits`,
`stat_unparks_to_inject`) touched on every spawn / done / unpark.
All four lived on adjacent words within the Runtime struct — a single
cache line, shared across every M.

The original profile didn't catch this because the **single-driver
bench has one thread doing all the spawning** — only one M ever
writes `stat_spawned`. Building a second bench (`bench-fanout-scaling`,
multiple driver coros) made the cost visible: per-driver throughput
collapsed 30× from w=1 to w=11.

**Fix**: sharded each counter onto its `P`. `dumpState` sums them at
read time. Atomic with `.monotonic` ordering — same machine cost as
a non-atomic add on ARM64 (single `LDADD`), explicitly safe for racing
readers.

**Receipt on `bench-fanout-scaling`, drivers = workers:**

| W | Before shard | After shard | Improvement |
|---|---|---|---|
| 2 | 75.8 ns | 57.4 ns | +24 % |
| 4 | 123.5 ns | 75.2 ns | +39 % |
| 8 | 210.6 ns | 121.3 ns | +42 % |
| 11 | 198.7 ns | 128.2 ns | +35 % |

Ratio w=11/w=1 dropped from 2.73× to 1.82×.

### 2. Cache-line padding on the remaining hot atomics didn't help

`num_searching` and `parked_workers` are read by every M on every
dispatch loop iter. Hypothesis: they're sharing a cache line, causing
ping-pong. Padded both to dedicated 128-byte aligned cache lines.

**Receipt**: spawn-hot-waitall median at w=11 moved from 631 → 597 ns
(within noise across 8-run samples). Tail-latency reduction wasn't
reproducible across more runs. **Reverted.**

Conclusion: the remaining gap isn't shared-atomic contention. The
profile pointed us in the wrong direction here.

### 3. Direct handoff in `Task.join` (Go-style `gopark`/`goready`)

The single-worker fast path was already ~100 ns/op. The cliff at w=2
suggested cross-M coordination — specifically, when a task is spawned
and then immediately joined on the same M, today's flow does:

1. Spawner pushes task to local P queue (LIFO slot).
2. Spawner calls `task.join()` → `parkOn` → suspends.
3. Some other M (possibly sibling) steals + dispatches the task.
4. Task's `.done` calls `parking_lot.unparkOne` → pushes joiner to
   a P's mailbox + `wakeOneParked`.
5. The joiner's M re-finds and re-dispatches it.

Steps 2-5 are pure cross-M overhead with no parallelism benefit.

**Fix**: implement Go's `gopark`/`goready` inline-dispatch pattern.
Added `P.tryRemoveLifo(target)` — single-CAS pop from lifo_slot — and
`runtime.tryDispatchInline(target)` that swaps target's `main_ctx` to
the caller's ctx, then context-switches into target inline. When target
finishes (`.done`), it swaps back to the caller. When target yields or
parks partway, we re-queue / no-op respectively and the caller falls
through to its normal `parkOn`.

`Task.join` calls `tryDispatchInline(self.coro)` before the parkOn
loop. On hit, no kernel hops; on miss (target evicted from lifo), no
harm.

**Receipts**:

| Bench | Before | After | Δ |
|---|---|---|---|
| `bench-mutex` contended | 728 ns | **644 ns** | **+11.5 %** |
| `bench-tcp-echo` | 7,282 ns | **6,940 ns** | +4.7 % |
| `bench-fanout-scaling` ratio @ w=11 | 1.82× | **1.58×** | **−13 %** |
| `bench-parallel-compute` @ 8 | 6.44× | 6.62× | +2.8 % |
| `bench-spawn-hot` @ w=11 (1000 joins) | 1,861 | 1,874 (noise) | — |
| `bench-scaling` ratio | 50× | 49× | — |

Direct handoff fires when the joinee is in the joiner's lifo slot —
that's true for spawn-then-immediately-join, the mutex bench's last
worker, TCP echo workers, and fanout drivers. It does NOT fire for
`bench-spawn-hot`'s shape (spawn 1000 then join 1000): by join-time,
only the LAST task is in lifo; the rest are in local queue. Extending
handoff to local queue requires a "pop specific item" WSQ operation
not natively supported. Known limitation.

### 4. Combined `Frame` + `Task` into one allocation

A bench-artifact comparison showed the real story. `bench-spawn-hot`
does 1000 individual `task.join()` calls per batch; Go's bench does
one `wg.Wait()`. Each `task.join` cleanup pays a frame_destroy +
task destroy. **Not apples-to-apples.**

Built `bench-spawn-hot-waitall` — same workload but with an atomic
counter + Notify barrier (one wait per batch, fast-path cleanup
joins after). Matched-shape numbers at w=11: **559 ns vs the
original 1700+ ns** — most of the "10× behind Go" was the
benchmark shape, not the scheduler.

Of the remaining gap, allocation count was the obvious target:
Volt's spawn does 4 allocations (Frame + Coroutine + Stack + Task)
vs Go's single `g` from `gFree` pool. The Coroutine and Stack come
from per-P pools after warmup, but Frame and Task each cost a fresh
`smp_allocator.create` on every spawn.

**Fix**: combine `Frame` and `Task` into one struct allocated as a
unit:

```zig
const Combined = struct {
    frame: F,
    task: Task(T),

    fn destroy(frame_ptr: *anyopaque, alloc: std.mem.Allocator) void {
        const fp: *F = @ptrCast(@alignCast(frame_ptr));
        const self_combined: *@This() = @fieldParentPtr("frame", fp);
        alloc.destroy(self_combined);
    }
};
```

Frame stays at offset 0 (the trampoline reads `*x19 = run_fn`).
`Task.join` no longer calls `allocator.destroy(self)` separately —
`self` lives inside the Combined; `frame_destroy` frees both.

**Receipts on `bench-spawn-hot-waitall`** (5-run medians):

| W | Pre-combined | Post-combined | Δ |
|---|---|---|---|
| 1 | 96 ns | 97 ns | — |
| 2 | 260 | **195** | **+25 %** |
| 4 | 219 | 200 | +9 % |
| 8 | 417 | **334** | **+20 %** |
| 11 | **631** | **475** | **+25 %** |

Plus +6 % on mutex, +5 % on fanout-scaling.

## Things tried that didn't land

- **Go-style `wakep` baton-pass** (#171): when the last spinning M
  commits to dispatch, wake another M to keep one spinning. Unguarded
  version improved spawn-hot 15-30 % but regressed Mutex 20 % and TCP
  14 %. Guarded variant (only baton-pass when local has more work)
  kept Mutex but made TCP wildly variable (8800-21000 range).
  Reverted both. The mechanism trades workloads against each other in
  our impl — Go's clean version must depend on infrastructure we
  don't have (cheaper wake primitives or different sync-primitive
  design).
- **Cache-line padding `num_searching` + `parked_workers`** (#160):
  null result. Variance reduction didn't reproduce across runs.

## Final side-by-side vs Go 1.26.0

`bench-spawn-hot-waitall` (Volt, Notify barrier per batch) vs
`spawn_hot.go` (Go, `wg.Wait` per batch). Same hardware,
darwin/arm64, ReleaseFast / `go build`:

| Workers | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | 106 ns | 137 ns | **0.77× (Volt 1.3× FASTER)** |
| 2 | 201 | 155 | 1.30× behind |
| 4 | 218 | 165 | 1.32× behind |
| 8 | 399 | 167 | 2.39× behind |
| 11 | 490 | 172 | **2.84× behind** |

`bench-fanout-scaling` (N independent drivers, each spawning into
its own P) vs the same shape in Go:

| W | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | 73 ns | 107 | **0.68× (Volt 1.5× FASTER)** |
| 11 | 117 | 106 | 1.10× — parity |

**Single-worker we beat Go on both shapes. Real-work multi-worker is
at parity. Synthetic spawn-heavy multi-worker is 2.84× behind.**

## What's left and what isn't worth grinding

The remaining 2.84× on synthetic spawn-heavy is the cost of:

- Per-spawn `smp_allocator.create(Combined)` (Coroutine + Stack come
  from pools; Combined still costs one allocator call).
- Cross-P cache-line transfers — every spawn writes to P[0]'s WSQ
  tail, every sibling steal reads it. Cross-core invalidations.
- Various stat increments and per-P bookkeeping.

Closing further requires deeper structural changes: per-Combined
pool keyed on fn-type, smaller Coroutine struct (removing legacy
`wait_next` field after sync-primitive migration), or different
WSQ design. All of these have non-trivial implications and weren't
on the "no architectural compromise" list.

The benchmark shape that exposes this — one driver spawning into
many workers with trivial work per task — is not what real Volt
users will write. They'll either have real work per task (which
amortizes coordination) or use a `WaitGroup`-like pattern naturally.

## Method notes

- Tool: samply 0.13.1
- Symbolication: offline `nm` lookup; samply didn't resolve inlined
  symbols (ReleaseFast inlines most code into `start.main`).
- Profiles saved to `/tmp/volt-profiles/{w1,w2,w8,waitall-w11*}.json.gz`.
- Symbol resolution script: `/tmp/profile_stacks2.py`.
- Side-by-side bench: `bench/go/spawn_hot.go`, `bench/go/fanout_scaling.go`,
  built with Go 1.26.0 (`proto run go`).

## Process lessons

1. **Build the matched bench first.** The "10× behind Go" claim was
   substantially a bench-shape artifact. We wasted attempts on baton-pass
   and padding before discovering it.
2. **Profile before guessing.** The shared-atomic theory was wrong;
   padding had no effect. Allocation count *was* the lever, and we
   only knew that after the matched bench let us see the real gap.
3. **% improvements are noise without absolute targets.** All
   improvements in this doc are quoted against measured Volt baselines
   *and* an absolute Go reference number on the same hardware.

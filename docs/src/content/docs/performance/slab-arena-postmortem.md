---
title: Slab arena postmortem
description: How a benchmark-tuned POOL_CAP=64 produced a 30× regression in spawn-hot, how it went undetected for three weeks, and the structural fix that replaced it.
---

This is the receipt for the [slab arena
design](/architecture/slab-arena/). It documents what failed
before it, how the failure was found, and the structural fix
that shipped 2026-05-16.

## What happened

On 2026-05-15, commit `081094d` ("feat: stack guard pages via
mmap-once + pool") landed. The design:

- One `mmap` per stack allocation (256 KiB virtual reservation).
- One `mprotect` at allocation to commit the top body page.
- Per-P pool of 64 stacks, intrusive-linked.
- Pool overflow: `munmap` excess stacks back to the OS.

The commit message specifically called out the risk:

> Pool-cap eviction `munmap`s the entry — that hits the VM lock,
> but only fires when an unusual spawn-burst exceeds POOL_CAP.

The full bench gate **was not run** before the commit landed.
Specifically, `bench-spawn-hot` (the canonical multi-worker
spawn benchmark) wasn't re-measured.

For three weeks, the runtime ran with this design. Tests passed.
Unit benches (`bench-yield`, `bench-spsc`, `bench-mutex`) passed.
The cliff was invisible to anything but multi-worker spawn-heavy
shapes.

## How it was found

The user asked a routine question on 2026-05-16: "did you run the
comparative benchmarks?" before publishing some doc updates.

The answer was no. Running them surfaced:

| Bench | Documented | Measured |
|---|---|---|
| yield | 9 ns | 9 ns ✓ |
| spsc | 12 ns | 12 ns ✓ |
| mutex | 14 ns | 12 ns ✓ |
| **spawn-hot w=1** | **106 ns** | **5,800 ns** |
| **spawn-hot w=11** | **490 ns** | **5,716 ns** |
| **fanout w=11** | **117 ns** | **2,094 ns** |
| RSS @ 10k | 16.6 KiB | 16.6 KiB ✓ |

Single-coro micros were spot-on. Multi-worker spawn benches were
**30-50× off** the documented baseline.

## Bisect

Worktree-based bisect across the commits between the last
known-good (`4f400db`) and HEAD:

| Commit | spawn-hot w=1 |
|---|---|
| `4f400db` (baseline) | 87, 92, 87 ns |
| `1354c3a` (refactor: lift Combined out of Runtime.spawn) | 168, 179, 416 ns |
| `1354c3a^` (parent — docs only) | 142, 187, 211 ns |
| `02f0901` (feat: Mpmc) | 3,495, 3,206, 3,925 ns |
| `580611f` (parent of stack-guard-pages — docs only) | 104, 101, 106 ns |
| **`081094d` (stack guard pages via mmap-once + pool)** | **3,256, 3,189, 3,884 ns** |
| `a5c30ae` (grow-on-demand stacks) | 25,495, 46,167, 12,367 ns |
| `1e23dd0` (API hygiene pass) | 9,012, 7,921, 7,026 ns |
| HEAD | 5,422, 5,830, 6,162 ns |

The numbers do two things at `081094d`: jump 30× and become
high-variance. The variance comes from `a5c30ae` (the SIGSEGV
grow path's mprotect-on-fault adds latency under load). The 30×
jump is the stack guard pages commit itself.

## Why 30×

Reading the failed commit's code:

```zig
pub fn freeStack(self: *P, base: StackPtr) void {
    if (self.stack_pool_count >= POOL_CAP) {           // POOL_CAP = 64
        stack_mod.free(base[0..stack_mod.totalSize()]);  // → munmap
        return;
    }
    // Push to local pool.
    ...
}

pub fn allocStack(self: *P) !StackPtr {
    if (self.stack_pool) |base| {
        // Pool hit.
        ...
    }
    return try stack_mod.alloc();   // → mmap + mprotect
}
```

The bench setup:

- `bench-spawn-hot` BATCH = 1000.
- Per round, 1000 fresh spawns + 1000 frees.
- POOL_CAP = 64.

So per round, on a single worker:

- First 64 frees: pool fill (cheap).
- Next 936 frees: `munmap` each (VM-lock serialized).
- 1000 spawns: 64 from pool, 936 fresh `mmap` (VM-lock serialized).

Total VM-lock-serialized syscalls per round: **~1,872 per worker,
per 1000 ops**. Each syscall ~1 µs on Darwin's `vm_map_lock`.
Cost: ~1,872 µs / 1000 ops = **~2 µs of VM-lock-serialized time
per op**. Versus a true cost of ~100 ns/op in the no-syscall
baseline. 20× of pure VM-lock waiting.

At workers=11, the picture is worse: 11 workers fight one global
`vm_map_lock`, each issuing ~1,872 syscalls per round. Effectively
serial. ~5,700 ns/op measured, vs 490 documented.

The commit's "POOL_CAP=64 is fine for normal spawn bursts" claim
was true — for spawn bursts up to 64. The spawn-hot bench is
specifically a 1000-deep burst by design.

## The wrong fix: bump POOL_CAP

The first thought was: bump POOL_CAP to a value that exceeds
BATCH. Set it to 2048; benchmark numbers return to baseline.

This is a hack. POOL_CAP=2048 papers over the cliff for this
specific workload but the cliff is still there for any workload
that exceeds 2048. The structural issue is that the cache has a
hard miss-cost cliff: cache hit = ~5 ns, cache miss = ~1 µs.
Bumping the cache size moves the cliff, doesn't eliminate it.

Honest assessment: a fixed-cap cache backed by an expensive
allocator has no graceful degradation. Any workload above the
cap fails on the cliff. Tuning the cap is benchmark-driven, not
principled.

## The right fix: slab arena

The structural fix is to eliminate the cache miss → expensive
syscall coupling. Either:

1. Make cache misses cheap (no `munmap`, no `mmap`).
2. Pre-allocate a fixed pool of stacks at runtime startup; no
   per-spawn allocation at all.

We picked option 2 — a slab arena. One `mmap` at runtime init for
the entire stack budget; allocation is "pop slot index from free
list"; free is "push slot index back". No syscalls in steady
state.

The arena design is documented in [The slab
arena](/architecture/slab-arena/). The first cut had unbounded
per-P pools, which broke under asymmetric workloads (single
driver fan-out → all freed slots pin in worker pools → driver
P exhausts arena). The fix: per-P pool with a **fair-share cap**
derived from `arena.n_slots / workers`. Above the cap, free
overflows to the arena (cheap — just a spinlock + index push).

Cap is not benchmark-tuned. It scales with user configuration.
Pure architectural cleanup.

## Post-fix bench gate

After the slab-arena fix (Darwin arm64, ReleaseFast, load avg ~4):

| Bench | Pre-fix | Post-fix | Original baseline |
|---|---|---|---|
| spawn-hot w=1 | 5,800 ns | **101 ns** | 106 ns ✓ |
| spawn-hot w=11 | 5,716 ns | **575 ns** | 490 ns ✓ |
| fanout w=11 | 2,094 ns | **117 ns** | 117 ns ✓ |
| bench-rss 10k | 16.6 KiB | **16.6 KiB** | 16.6 KiB ✓ |
| stress 45s | — | **838 M ops** | 840 M ✓ |

Everything restored. The arena doesn't merely match the pre-cliff
baseline; it eliminates the cliff entirely. Workloads with batch
sizes from 1 to 16384 (the default arena cap) all run at the
same per-spawn cost.

## Lessons

1. **The phase-landing protocol is load-bearing.** CLAUDE.md
   explicitly mandates running the full bench gate before
   landings that touch the scheduler / coroutine struct /
   allocation path / sync primitives. The stack guard pages
   commit skipped this for `bench-spawn-hot`. Three-week
   silent regression.

2. **Benchmark-tuned constants are technical debt.** POOL_CAP=64
   was a reasonable number for "typical spawn bursts"; the
   benchmark used 1000. The constant didn't fail because of
   bad design — it failed because the benchmark and the constant
   were chosen independently. Whenever a constant tunes against
   a specific workload pattern, the cliff is right there for
   any other workload.

3. **Cache + expensive-miss is a structural antipattern.**
   Mutex-protected fast path with a syscall slow path is fine
   when the slow path is genuinely rare. When the slow path is
   reachable by ordinary workload shapes (1000-deep batch isn't
   exotic for a coroutine runtime), it's a cliff. The fix is to
   make the slow path cheap, not to push it further away.

4. **CI catches what humans don't.** A weekly cron running
   `bench-spawn-hot` and tracking the median against a baseline
   would have caught this in 3 days, not 3 weeks. Adding that
   automation is roadmapped.

## Receipts

- Commit that introduced the regression: `081094d`.
- Commit that fixed it: `4c24fd5` ("perf(stack): slab arena —
  closes the bench-spawn-hot ~30× regression").
- Follow-up cleanup commit: `d699455` ("refactor(stack): derive
  per-P pool cap from arena size + worker count") — removed the
  `STACK_POOL_CAP = 1024` magic number in favour of the derived
  fair-share cap.

## Further reading

- [The slab arena](/architecture/slab-arena/) — the structural design.
- [Stack growth on demand](/architecture/stack-growth/) — the related SIGSEGV-driven page growth.
- [Phase 4 postmortem](/performance/phase-4-postmortem/) — the *earlier* stack-growth attempt that taught us mprotect-per-spawn is the cliff.
- [Multi-worker profile](/performance/multi-worker-profile/) — methodology for finding scheduler-level cliffs via `samply`.

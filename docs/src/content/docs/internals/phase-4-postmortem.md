---
title: "Phase 4 (stack growth) post-mortem"
---

## What happened

On 2026-05-14 we landed M:N Phase 4: replace heap-allocated 16 KiB stacks
with `mmap`'d 1 MiB reservations with a `PROT_NONE` guard page and a
SIGSEGV handler that grows the committed region on overflow. Within a
day it was reverted on 2026-05-15.

## Why it was reverted

`bench-spawn-hot` (a 10-second sustained spawn+join workload) crashed
with SIGSEGV in ~30-50 % of runs. Stack trace always pointed at
`coroutine.zig:143` — trampoline function entry — with fault address
`0xfffffffffffffff0`. This pattern says SP (or the trampoline's
incoming function-pointer load) was sitting at or near zero.

Every worker thread crashed simultaneously at the same site, which
ruled out a single bad coroutine and pointed at something common to
every fresh dispatch.

## What did NOT cause it

A directed bisect ruled out:

| Variant | Crashes? |
|---|---|
| Phase 4 with central pool disabled | yes |
| Phase 4 with all per-P pools disabled | yes |
| Phase 4 with `alignedAlloc` instead of `mmap` | yes |
| Phase 4 with the SIGSEGV handler not installed | yes |
| Phase 4 with `workers=1` (single thread) | yes |

So the bug wasn't in: the pool, the central pool's `pthread_mutex_init`
(though that was a separate real bug — uninitialized mutex on macOS),
the mmap path, the signal handler, or multi-threading.

The remaining surface that *might* still have been responsible: layout
shift of `Coroutine` (an `atomic Value(usize)` field was added), or
some subtle initialization-order issue in `Runtime.spawn`. Neither was
isolated cleanly.

## Why it wasn't caught before landing

Phase 4 shipped after running `zig build stress` (which passed) but
WITHOUT running `bench-spawn-hot` or `bench-spawn-join`. The crash was
probably present from day one of the Phase 4 work.

The phase-verification habit had drifted toward "one bench that exercises
multiple primitives + tests" instead of "every published bench, every
landing." Phase 4 was the cost of that drift.

## Lessons

1. **One change per commit.** Phase 4 mixed: new `stack.zig`,
   new `stack_overflow.zig`, new `Coroutine` field, new `Runtime` field,
   reworked `P.allocStack` / `P.freeStack`, SIGSEGV handler install,
   `pthread_mutex_init` call. Any of those individually would have been
   bisect-able. Mixed, none was.

2. **Run every bench.** Stress is necessary but not sufficient. The
   bench suite that must be green before any Phase landing is:

   - `bench-yield`
   - `bench-spsc`
   - `bench-mutex`
   - `bench-spawn-hot` (5 runs — high variance)
   - `bench-spawn-join` (3 runs across all worker counts)
   - `bench-parallel-compute`
   - `bench-tcp-echo`
   - `zig build stress` (3 runs)

3. **Profile, don't sequence.** Phase 4 was claimed as a perf win
   (lower idle RSS via mmap-grow). Profiling first would have shown
   the spawn-hot bench was already at smp_allocator-cached
   16-KiB-per-spawn cost, and the marginal win from mmap-grow was
   smaller than the regression we'd accept from the mmap syscall
   on cold paths. Phase 4 should have been pitched as a *correctness*
   landing (overflow safety), not a perf one.

## Plan for re-landing

Phase 4's correctness goal is real. Stack overflow today silently
corrupts heap. The re-landing plan:

1. **Step 4a: guard pages on fixed-size stacks.** Keep `alignedAlloc`
   stacks but page-align the allocation, `mprotect(PROT_NONE)` the
   bottom page, run all benches before commit. Overflow becomes a
   clean SIGSEGV; no grow needed. Smaller change, easier to verify.

2. **Step 4b: SIGSEGV handler that recognizes our stacks.** Install
   the handler, register each guard page in a process-wide table, on
   fault chain to default (clean abort with stack trace). All benches
   green before commit.

3. **Step 4c: mmap-based stacks (still fixed-size).** Replace
   `alignedAlloc` + `mprotect` with one `mmap` of (page + body). All
   benches green before commit.

4. **Step 4d: Grow on demand.** Now expand the reservation, the handler
   does `tryGrow`, the committed region slides down. All benches green
   before commit.

The original Phase 4 tried to do all four steps at once, plus a central
stack pool, plus pool restructuring. Don't repeat the mix.

## Update 2026-05-14: Step 4a attempted, reverted

Step 4a as scoped above (alignedAlloc + per-spawn mprotect) was
implemented, tested, and reverted in the same session.

What was implemented: `src/stack.zig` with `alloc()` = `alignedAlloc`
+ `mprotect(PROT_NONE)` on the bottom page, `free()` = `mprotect`
back to RW + `allocator.free`. `src/p.zig` pool reshaped so the
intrusive next-pointer lives at `usableOffset()` (since the bottom
page is now PROT_NONE and can't store metadata).

Verification: stress test passed (~140 M ops, parity with baseline),
unit tests passed, RSS unchanged (PROT_NONE pages have zero RSS on
Darwin — the guard costs only virtual address space).

What killed it: `bench-spawn-hot` regressed ~8× on 11 workers
(median ~5400 ns/op vs baseline ~670 ns/op). Single-worker was
unaffected. Isolating mprotect (kept the new layout, disabled the
syscall) returned multi-worker perf to ~850 ns. So `mprotect` is
the entire regression.

The mechanism: `mprotect` acquires the process-wide VM map lock
(Darwin's `vm_map_lock`; on Linux it's `mmap_sem`). Every cold
spawn (pool miss) does one mprotect on alloc; every over-cap free
does another. With 11 workers each hitting mprotect a few hundred
times per second under steady-state churn, they serialize on the
VM lock, and the visible cost becomes queue wait rather than
syscall time. Raising `POOL_CAP` to 2048 (covering the 1000-batch
working set) didn't fix it — work-stealing scatters frees across
P pools unevenly, so over-cap evictions still fire constantly.

Architectural conclusion: **any guard-page design must mprotect
each stack exactly once — at slab/runtime init or first lazy use —
and never on the per-spawn hot path.** Step 4a as scoped is wrong;
heap-backed stacks force mprotect-on-return because the allocator
can recycle the memory for non-stack uses.

The right next step combines what was 4a + 4c above into one
landing:

- Each stack is `mmap(PROT_NONE, page + body)` followed by
  `mprotect(PROT_RW)` on the body region — once per stack, never
  again. The bottom page is PROT_NONE by construction.
- Stacks live in a per-runtime (or per-P) pool. Free goes to the
  pool, never to munmap, never to mprotect. The pool grows
  lazily; munmap happens only at `Runtime.deinit`.

This is what Step 4b should have been from the start. Task #175
tracks the redesigned landing. Step 4c-as-handler (SIGSEGV
recognition) and step 4d (grow on demand) stay on the roadmap
unchanged.

Measurement caveat: the day this was run the host load average
was 60+ (shared machine, 37 users), so absolute numbers above are
not authoritative. The architectural finding stands because the
on/off pattern (mprotect → 8× slowdown, mprotect-off → baseline)
was consistent across every run regardless of magnitude. Numbers
should be re-validated on a quiet host before deciding the
per-stack mprotect cost is acceptable even for the mmap-once
design.

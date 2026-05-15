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

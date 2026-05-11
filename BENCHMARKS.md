# Volt vs Go — Comparative Benchmarks

> Not for bragging rights. Go is a massive, brilliantly-engineered runtime —
> 15 years of work by some of the best systems engineers alive — and we look
> at it as the target. Where Volt is ahead, we keep pushing; where Volt
> trails, we have specific work to do. Both honest.

Last measured: **2026-05-12**, macOS arm64 (Apple Silicon), Zig 0.16.0, Go 1.23.2.
Reproducible via `zig build compare`. Median of 11 iterations after 3 warmup rounds.

## Headline numbers

| Workload | Volt | Go | Volt / Go |
|---|---|---|---|
| yield (one-way ctx switch) | **10 ns/op** | 38 ns/op | **0.26× ★★** |
| spawn + waitgroup-wait | 3,868 ns/op | 147 ns/op | 26.31× |
| spawn + per-coro join | 4,026 ns/op | 145 ns/op | 27.77× |
| channel SPSC cap=16 | 175 ns/op | 33 ns/op | 5.30× |
| mutex contended (8 coros) | **41 ns/op** | 82 ns/op | **0.50× ★** |

★★ = ≥2× faster · ★ = 5-50% faster · blank = slower

## What the numbers mean

### Where Volt is ahead

- **Context switch (yield) — 3.7× faster.** Hand-written AAPCS64 / SysV-x86_64
  asm context swap with no scheduler-side bookkeeping per swap. Go's
  `runtime.Gosched` goes through the full scheduler each time. The 10 ns
  number is the raw cost of two register-set saves + restores plus an
  atomic state read — about as low as a stackful runtime can go.
- **Mutex saturated contention — 2× faster.** Volt's MCS lock-free queue
  with `unparkLocal` (skips inter-core wake when the handoff chain stays
  worker-local) drops the per-cycle cost dramatically. Fair FIFO is
  preserved by construction.

### Where Volt trails Go significantly

- **Spawn + join — 26× slower.** This is the headline gap. Go's per-P
  `gFree` slab + 2 KiB initial stacks with copy-grow are 15 years of
  optimization. Volt's per-worker `FramePool` + 1 MiB virtual stacks
  with mmap-grow are correct but not yet on parity. The remaining 3.6 µs
  per spawn lives in the **scheduler dispatch cycle** — Worker.findWork →
  dispatch → Done.subscribe → park/unpark — not in any single
  allocation or atomic op. We have a profiling bench
  (`zig build bench-spawn-profile`) to drive future optimization.
- **Channel SPSC — 5.5× slower.** Volt's Vyukov MPMC channel with
  guarding parks has more per-element overhead than Go's tightly tuned
  `chan`. Some is the cost of being lock-free MPMC; some is optimization
  we haven't done yet.

## What we measured

### yield (one-way ctx switch)

Two coroutines (Volt) or one goroutine (Go) ping-pong via the scheduler:
yield → re-dispatch → yield → ... 100,000 times. Wall time / (iters × 2) =
one-way ctx switch cost.

### spawn + waitgroup-wait

The canonical Go pattern: spawn N = 10,000 goroutines that each call
`wg.Done()`, then `wg.Wait()` for all. Volt analog: spawn N coros that
decrement an atomic counter and `notify.notifyOne()`, parent
`notify.wait()`. Measures spawn rate + the single batched wait.

### spawn + per-coro join

Volt's natural pattern: spawn N, then `for (jobs) |j| try j.join()`. The
sequential per-job join exposes 10k park/unpark cycles. Go's `chan` /
`WaitGroup` are the only join idioms — there's no per-goroutine handle —
so the Go side just calls the same `WaitGroup`-based function for
symmetry. The Volt number is for honesty: this is what `for-loop +
.join()` actually costs.

### channel SPSC cap=16

Single producer, single consumer. Producer sends 100,000 monotonic
u64s through a capacity-16 channel; consumer drains. Per-op = wall / N.

### mutex contended (8 coros)

8 coroutines, each acquires a shared mutex and increments a u64
50,000 times (400k total acquires). The classic saturated-contention
worst-case for a fair mutex. Per-op = wall / total_acquires.

## What's NOT measured (yet)

- **TCP echo throughput** — Volt's io_uring vs Go's netpoll.
- **HTTP request/response lifecycle** — full server on TCP.
- **Memory per idle coroutine** — Volt ~16 KiB resident vs Go ~8 KiB.
- **Cancellation latency** — time from `.cancel()` to coroutine exit.
- **Realistic mixed workload** — coroutines doing real work between
  lock acquisitions / channel ops. Most production code looks like
  this; the saturated micros aren't representative.

These are v1.x bench targets.

## How Volt's allocation differs from Go's

| Aspect | Go | Volt |
|---|---|---|
| Stack growth | Copy-grow (compiler stackmaps fix pointers) | Virtual-memory grow (mmap + mprotect on guard hit) |
| Initial committed stack | 2 KiB | 4 KiB (Linux x86) / 16 KiB (macOS arm64) — one page |
| Per-coro alloc | Per-P `gFree` slab pool | Per-worker `FramePool` (1 MiB mmap'd, comptime size classes) |
| Heap source | Go's `mheap` (invisible to user) | User passes allocator once at `volt.run`; per-worker FramePool sits on top |
| GC | Tracing GC with write barriers | None — user owns allocator lifecycle |

### Volt's allocation design in one paragraph

The user passes an allocator **once** at `volt.run(.{ .allocator = ... })`
and never again. After that, the spawn hot path bypasses the user
allocator entirely: each Worker has its own 1 MiB `FramePool` (one
mmap call at worker init) for spawn metadata, and its own `LocalPool`
of recycled mmap'd 1 MiB stack regions. The user's allocator is for
runtime startup, channel buffers (when user calls `Channel.init`), and
fallback when the FramePool is exhausted. Same shape as Go's per-P
caches, but with the user controlling the bulk allocator choice
instead of a GC-managed heap.

## Why Volt does what Go does — and where the gaps live

### Where Volt copied Go (well, with Zig adaptations)

- **Per-worker slab pool for spawn metadata.** Go's `gFree` per-P;
  Volt's `FramePool` per-Worker. Same idea, Zig has comptime size
  classes so the slot-class lookup is comptime-resolved.
- **Work-stealing scheduler with Chase-Lev deques.** Same algorithm
  as Go's per-P run queues.
- **LIFO slot for cache-warm continuations.** Go does this; Tokio
  also; we copied it.
- **Adaptive park/wake (parker bitmap).** Go's idle-P bitmap; Volt's
  parked-workers bitmap. Same shape.
- **Stack-overflow handler that grows on demand.** Go does morestack;
  Volt does mprotect on guard-page hit. Different mechanism (we don't
  have stackmaps), same outcome (transparent growth).

### Where Volt does it differently because of Zig

- **No GC.** User owns allocator. Means we can't copy-grow stacks
  (no stackmaps to fix pointers) — but we don't need a write barrier
  on every pointer write either.
- **Comptime specialization.** Spawn closure types are monomorphized
  at the call site. No virtual dispatch through `dyn Future`.
- **Intrusive containers everywhere.** MCS waiter nodes in
  `Coroutine`. `live_coroutines` link in `Coroutine`. No allocator
  pressure for queue nodes.
- **Explicit allocator.** User gets full control + predictability;
  pays in API verbosity vs Go's hidden runtime allocator.

### Where Volt has work to do

- **Scheduler dispatch cycle** is ~4 µs per coro on this bench; Go
  achieves ~150 ns. The work isn't in any one atomic op or allocator
  call — it's the accumulated cost of `Worker.findWork` + dispatch
  state setup + Done.subscribe + park/unpark. Closing this needs
  either a much tighter dispatch hot path or a different scheduler
  architecture (e.g. inline the Done path so completed coros don't
  round-trip through post-swap).
- **Channel send/recv overhead.** Volt is at 177 ns/op SPSC; Go is at
  32 ns/op. We use a Vyukov MPMC ring with parks-on-block; Go's `chan`
  has a specialized fast path for the SPSC case. Optimization target
  for v1.x.

## Running the benchmarks yourself

```sh
zig build compare
```

Prerequisites:
- Zig 0.16.0
- Go 1.23+

The orchestrator builds both binaries, runs them sequentially (to avoid
cache/thermal interference), and prints the table above. Each side
runs 11 iterations after 3 warmup rounds; the median is reported.

## Acknowledgments

- **Go runtime** — the gold standard for stackful concurrent runtimes.
  Every core pattern in Volt (per-P caches, work-stealing, idle-worker
  bitmap, stack management) traces back to something Go did first. Volt
  exists because Go has shown how good a stackful runtime can be.
- **Tokio** — Rust's stackless equivalent. We learned from its scheduler,
  cooperative budgeting, and sync-primitive designs.
- **may** (Rust stackful) — confirmed several design choices for the
  stackful approach in Rust; we made different tradeoffs but learned
  from theirs.
- **Vyukov MPMC** — bounded ring buffer paper, used for Volt's channel.

## Caveats

- Single machine, single OS (macOS arm64) — results on Linux x86_64
  may differ.
- Microbenchmarks, not real workloads — production performance
  depends on many factors not captured here.
- Both Zig and Go have compilation/runtime tradeoffs; some differences
  may reflect compiler optimization strategy rather than runtime design.
- Run-to-run variance is typically 5-15% on these benchmarks. The
  median-of-11 helps but doesn't eliminate it.
- Go is mature and battle-tested at massive scale; Volt is new. **Take
  the spawn+join gap seriously, not the yield/mutex wins.**

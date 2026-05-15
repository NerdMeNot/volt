# Volt benchmarks

> Honest measurements. Volt wins on some workloads, lags Go on others.
> The two sections below are the **Zig-native pitch** (what Volt is
> built for) and the **Go side-by-side** (transparency on the
> overlap). Both are receipts, not bragging rights.

All numbers: **Darwin arm64, 11 cores, ReleaseFast, smp_allocator.**
Date: 2026-05-16. Go reference: 1.26.0 on the same hardware.

Reproduce with `zig build bench-<name>` (Volt) or
`go build bench/go/<name>.go && ./<name>` (Go).

---

## Section 1: Zig-native metrics

What Volt is built for: tiny memory footprint, explicit allocation,
predictable performance, no GC pauses. These are the metrics that
matter for "stackful coroutines that cost what you ask them to cost."

### RSS per idle coroutine (`bench-rss`)

How much resident memory does a parked coroutine cost?

| N idle coros | Peak RSS | per-coro |
|---|---|---|
| 100 | 3,376 KiB | 33.8 KiB |
| 1,000 | 18,160 KiB | 18.2 KiB |
| 5,000 | 83,728 KiB | 16.7 KiB |
| 10,000 | 165,792 KiB | **16.6 KiB** |

The steady-state figure (16.6 KiB per idle coro at N=10k) is the
top-of-stack body region. Stacks reserve 256 KiB of virtual address
space per coro but commit only the top page initially (16 KiB on
Darwin, 4 KiB on Linux); the growable region below is PROT_NONE
and contributes zero RSS until SIGSEGV grows it on demand. The
bottom page is the true guard — overflow into it aborts cleanly
instead of corrupting the heap.

### Real-work multi-worker scaling (`bench-parallel-compute`)

256 CPU-bound tasks across N workers (XorShift, ~50 µs each):

| Workers | Per-task time | Speedup vs 1 |
|---|---|---|
| 1 | 85 µs | 1.00× |
| 2 | 42 µs | 1.99× |
| 4 | 22 µs | 3.75× |
| 8 | 13 µs | **6.62×** |

Near-ideal scaling. Real parallel work amortizes the scheduler's
coordination cost.

### Sustained mixed-primitive throughput (`bench-stress`)

3 phases × 15 s each: spawn-join, Mutex contention, Spsc channel
ping-pong, all under multi-worker load with a watchdog.

| Result | **~840 M total ops over 45 s** |

Used as a pre-merge gate — must pass before any scheduler change
lands. The Mutex redesign on the parking lot (2026-05-16) brought
phase 2 from ~600 ns/op to ~14 ns/op, lifting total throughput
~6× over the prior ~140 M baseline.

### Fan-out scaling (`bench-fanout-scaling`)

N driver coroutines, each running its own spawn+wait loop in
parallel. Measures whether the scheduler can keep multiple
independent producer/consumer pairs hot.

| Workers / drivers | ns/op | ratio vs w=1 |
|---|---|---|
| 1 | 73 | 1.00× |
| 2 | 64 | 0.86× |
| 4 | 89 | 1.20× |
| 8 | 120 | 1.63× |
| 11 | **117** | **1.59×** |

Workers=2 actually beats workers=1 (less contention per driver).
The ratio stays under 1.7× across the whole curve, which is what
"scales linearly enough for real work" looks like.

---

## Section 2: Go side-by-side

For transparency on the metrics that overlap with Go's strengths.
Same hardware, same bench shape, both runtimes' idiomatic patterns.

### Microbenchmark floor

| Bench | Volt | Go | Volt/Go |
|---|---|---|---|
| yield (one-way ctx switch) | **9 ns** | 42 ns | **0.21× — 4.7× faster** |
| Spsc send+recv cap=16 | **12 ns** | 33 ns | **0.36× — 2.8× faster** |
| TCP echo 64 × 16 RTT × 1 KB | **~7,000 ns** | 9,050 ns | **0.77× — 1.3× faster** |

### Spawn + wait shapes (same bench, both sides)

`bench-spawn-hot` (Volt, Notify barrier per batch) and
`spawn_hot.go` (Go, `wg.Wait` per batch). One driver, BATCH=1000,
10 s sustained.

| Workers | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | **106 ns** | 137 ns | **0.77× — 1.3× faster** |
| 2 | 201 | 155 | 1.30× |
| 4 | 218 | 165 | 1.32× |
| 8 | 399 | 167 | 2.39× |
| 11 | **490** | 172 | **2.84×** |

The single-worker fast path beats Go decisively. The multi-worker
gap concentrates at high worker count on synthetic spawn-heavy
workloads — a pattern where adding workers can only *hurt* because
there's no parallel work to amortize coordination. See
`docs/internals/multi-worker-profile.md` for the deep dive.

### Fan-out scaling (Volt vs Go, matched shapes)

N drivers, N workers, each driver spawns BATCH=100, `wg.Wait`,
loops for 4 s.

| Workers | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | 73 | 107 | **0.68× — 1.5× faster** |
| 4 | 89 | 92 | 0.97× — parity |
| 11 | **117** | 106 | **1.10× — parity** |

This is the workload work-stealing schedulers are *built* for, and
we're at parity with Go across the curve.

### Spawn-hot with 1000 individual joins (`bench-spawn-hot-individual`)

A **Volt-specific cost** — calling `task.join` N times per batch is
not what Go users write; they use `wg.Wait`. Tracked separately
because some Volt API patterns may do this.

| Workers | ns/op |
|---|---|
| 1 | ~100 |
| 11 | ~1,700 |

Each `task.join` does a frame_destroy + Combined cleanup. With 1000
calls per batch, that overhead dominates. The canonical bench
(`bench-spawn-hot` above) is the fair comparison vs Go.

### Mutex contended

8 coros × 50,000 acquires, NumCPU workers.

| Workload | Volt | Go | Volt/Go |
|---|---|---|---|
| Mutex contended | **14 ns** | 81 ns | **0.17× — 5.8× faster** |

The rewrite (2026-05-16) replaced the pthread-mutex-backed
WaitQueue with a parking-lot state machine + brief spin loop.
Short critical sections (counter increments, hashmap reads) almost
always release within the spin window, avoiding the park+unpark
round trip. Pre-rewrite this bench was 8× behind Go.

---

## Methodology

- **Run-to-run variance is real** on spawn benches (15-40 %).
  Single-number claims are misleading. The numbers above are
  **5-run medians** unless otherwise noted.
- **Build mode**: `.ReleaseFast` for Volt benches, `go build`
  (default optimizations) for Go.
- **Allocator**: `std.heap.smp_allocator` for Volt.
- **GOMAXPROCS**: matches the Volt `workers` setting (set via env
  var `VOLT_BENCH_WORKERS` for Volt benches that support it).

## Receipt benches gate scheduler changes

Any landing that touches the scheduler, coroutine struct, sync
primitives, or allocation path must show no regression on:

- `bench-rss` (memory footprint)
- `bench-scaling` (multi-worker latency curve)
- `bench-fanout-scaling` (multi-driver parallelism)
- The full microbench suite (`yield`, `spsc`, `mutex`, `tcp-echo`)
- `stress` (3 runs, all PASS)

If a change moves the median by < 20 % on the bench it claims to
improve, it doesn't land — the change isn't clearly worth the
complexity over the noise floor. This rule is set after a series of
attempted "improvements" that turned out to be noise. See the
`#171 baton-pass` and `#160 cache-padding` entries in the profile
doc for examples of changes that didn't clear the bar.

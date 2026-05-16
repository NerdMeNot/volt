---
title: Benchmarks
description: How Volt measures itself. Go is the scale reference, not the target. Methodology, receipts, and what each bench actually measures.
---

Numbers below exist to answer one question: **is Volt in an
acceptable performance range for a stackful coroutine runtime?**
Not "is Volt faster than Go." Go has been optimised for 15+ years
by people with deep systems expertise and a much larger user
base; expecting Volt to beat Go everywhere would be naive, and
most of the time when we do beat it, it's because Go pays a cost
we don't (GC, function colouring, write barriers) rather than
because we out-engineered them.

We use Go as the **scale reference** because it's the closest
mainstream production runtime to what Volt aims to be —
stackful-ish (goroutines), M:N scheduled, network-first. When a
Volt number lands within ~5× of Go, we know we're in a sensible
neighbourhood. When it lands within ~1.5×, we know there's
nothing structurally wrong. When Volt is faster, we treat that
as worth investigating (usually it's because Go is doing
something Volt isn't, like GC write barriers — not because we
won the design).

All numbers: **Darwin arm64, 11 cores, ReleaseFast, smp_allocator.**
Date: 2026-05-16 (post slab-arena landing). Go reference:
1.26.0 on the same hardware. System load avg ~5 during the run —
micros are load-insensitive; spawn-hot can drift 10–20% on a
quieter host.

Reproduce with `zig build bench-<name>` for Volt or
`go run bench/go/<name>.go` for the Go side.

## What we measure

Three categories of bench:

1. **Receipt benches** — track properties Volt claims (per-coro
   RSS, parallel-compute speedup, scaling curves). Gate scheduler
   changes: no landing if a receipt regresses.
2. **Micro-benches** — single-primitive latency (yield, channel
   send/recv, Mutex lock/unlock). Bound from below by hardware.
3. **Comparison benches** — same shape on both sides, Go as the
   scale reference. Catches regressions where Volt drifts away
   from a reasonable range.

## Receipts

### RSS per idle coroutine (`bench-rss`)

How much resident memory does a parked coroutine cost?

| N idle coros | Peak RSS | per-coro |
|---|---|---|
| 100 | 3,571 KiB | 34.9 KiB |
| 1,000 | 18,677 KiB | 18.2 KiB |
| 5,000 | 85,655 KiB | 16.7 KiB |
| 10,000 | 169,558 KiB | **16.6 KiB** |

At steady state 16.6 KiB per idle coro — the top-of-stack body
region only. Stacks reserve 256 KiB of virtual address space but
commit only the top page initially (16 KiB on Darwin, 4 KiB on
Linux); the rest is PROT_NONE and contributes zero RSS until
SIGSEGV grows it on demand. The bottom page is the true guard —
overflow aborts cleanly instead of corrupting heap.

The N=100 result (34.9 KiB/coro) shows the runtime's baseline
overhead — the slab arena's virtual reservation has trivial RSS
impact, but the per-worker scheduler state + reactor + parking
lot + smp_allocator pages add up to a few MiB regardless of
spawn count. Per-coro overhead asymptotes to 16.6 KiB at scale.

### Parallel-compute scaling (`bench-parallel-compute`)

256 CPU-bound tasks (XorShift, ~50 µs each) across N workers:

| Workers | Per-task time | Speedup vs 1 |
|---|---|---|
| 1 | 91 µs | 1.00× |
| 2 | 45 µs | 2.00× |
| 4 | 24 µs | 3.66× |
| 8 | 15 µs | **5.76×** |

Near-ideal for an 8-of-11 cores load on a host that's not
exclusively running this benchmark. This is what work-stealing
schedulers are built for; the receipt confirms ours behaves like
one.

### Sustained mixed throughput (`bench-stress`)

Three 15s phases — spawn-join, Mutex contention, Spsc channel —
all multi-worker with a watchdog:

| | |
|---|---|
| Total ops over 45 s | **~838 M** |

Used as a pre-merge gate. The Mutex redesign (parking-lot + spin)
and slab-arena landing both held this number stable; large
regressions are immediately visible.

### Fan-out scaling (`bench-fanout-scaling`)

N driver coroutines, each running its own spawn-join loop:

| Drivers / workers | ns/op | ratio vs w=1 |
|---|---|---|
| 1 | 63 | 1.00× |
| 2 | 55 | 0.87× |
| 4 | 77 | 1.22× |
| 8 | 115 | 1.83× |
| 11 | **117** | **1.86×** |

Workers=2 beats workers=1 (less contention per driver). The full
1→11 ratio stays under 2×, which is the shape we want from
work-stealing on real parallel work.

## Comparisons against Go

Same shape, both sides, same hardware. Numbers below are 5-run
medians. Volt/Go ratios reported so you can see scale; nothing
here is a "we win" claim.

### Primitive latency

| Bench | Volt | Go | Volt/Go | Notes |
|---|---|---|---|---|
| yield (one-way ctx switch) | 9 ns | 42 ns | 0.21× | Stackful + no GC write barriers makes this Volt-favoured. |
| Spsc send+recv (cap=16) | 12 ns | 33 ns | 0.36× | Comptime-specialised channel vs Go's generic chan. |
| Mpmc send+recv (1P × 1C) | 54 ns | — | — | Vyukov bounded ring + parking-lot blocking. |
| Mutex contended (8 × 50k) | 15 ns | 81 ns | 0.18× | Volt spins through the hot lock release; Go also spins but pays write-barrier costs on the wakers. |
| TCP echo (64 × 16 RTT × 1 KB) | 8,449 ns | 9,050 ns | 0.93× | Both route through OS networking — gap small. |

### Spawn + wait

`bench-spawn-hot` (Volt) vs `spawn_hot.go` (Go). One driver,
BATCH = 1000, Notify barrier on Volt's side, `wg.Wait` on Go's
side. 10s sustained.

| Workers | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | 101 ns | 136 ns | 0.74× |
| 2 | 138 | 159 | 0.87× |
| 4 | 161 | 204 | 0.79× |
| 8 | 389 | 182 | 2.14× |
| 11 | 575 | 213 | **2.70×** |

Volt's single-worker fast path is sub-Go, then the gap widens
with worker count. This is a synthetic spawn-heavy shape where
adding workers can only hurt — there's no parallel work to
amortise the extra coordination. On real-work multi-worker
shapes (fan-out, TCP, parallel-compute) we're at parity or
better. See [Multi-worker profile](/performance/multi-worker-profile/)
for the deep dive into the 2.70× and why we don't think it's
worth optimising into the ground.

### Fan-out (matched, both sides)

N drivers, N workers, each driver does BATCH=100 + `wg.Wait`,
4s.

| Workers | Volt | Go | Volt/Go |
|---|---|---|---|
| 1 | 63 | 108 | 0.58× |
| 4 | 77 | 89 | 0.86× |
| 11 | 117 | 107 | 1.10× |

Within ~10% of Go across the curve. This is the receipt that
says work-stealing is working as intended on real parallel
workloads.

## Methodology

- **Run-to-run variance** is real on spawn benches (15–40%).
  All spawn numbers are 5-run medians.
- **Build mode**: `.ReleaseFast` for Volt; `go build` (default)
  for Go.
- **Allocator**: `std.heap.smp_allocator` for Volt.
- **GOMAXPROCS** matches Volt's `workers` setting (set via
  `VOLT_BENCH_WORKERS` for benches that support it).
- **System load** during measurement is recorded with the run;
  loaded hosts produce inflated numbers on multi-worker
  benches.

## Receipt gate for scheduler changes

Any landing that touches the scheduler, coroutine struct, sync
primitives, or allocation path must hold:

- `bench-rss` — memory footprint
- `bench-scaling` — multi-worker latency curve
- `bench-fanout-scaling` — multi-driver parallelism
- Full micro suite (`yield`, `spsc`, `mpmc`, `mutex`, `tcp-echo`)
- `stress` (45 s, must PASS)

A change that moves the bench it claims to improve by <20%
doesn't land — under the noise floor. Two prior "improvements"
that didn't clear the bar:

- `#171 baton-pass` — improved spawn-hot 15-30% but regressed
  Mutex 20%, TCP 14%. Reverted.
- `#160 cache-line padding` — no measurable improvement.
  Reverted.

The phase-landing protocol is mandatory. Skipping it has cost
real time — see [Phase 4
postmortem](/performance/phase-4-postmortem/) (the mprotect-per-
spawn revert) and [Slab arena postmortem](/performance/slab-arena-postmortem/)
(the POOL_CAP=64 cliff that went undetected for three weeks).

## Reproducing

```sh
# Volt side
zig build bench-yield         # ~9 ns/op
zig build bench-spsc          # 12 ns/op
zig build bench-mpmc          # 54-157 ns/op (1×1 / 2×2 / 4×4)
zig build bench-mutex         # 15 ns/op (contended)
zig build bench-spawn-hot     # 101-575 ns/op (workers=1 .. 11)
zig build bench-fanout-scaling
zig build bench-tcp-echo      # ~8,500 ns/RTT
zig build bench-rss           # 16.6 KiB/coro at N=10k
zig build bench-parallel-compute
zig build stress              # 45s, ~840M ops

# Go side
cd bench/go
go run spawn_hot.go
go run fanout_scaling.go
```

5-run medians are the spec; spawn-heavy benches single-shot can
drift 15–40%.

## Further reading

- [Multi-worker profile](/performance/multi-worker-profile/) — `samply`-driven investigation of the workers=11 gap.
- [Phase 4 postmortem](/performance/phase-4-postmortem/) — the mprotect-per-spawn revert.
- [Slab arena postmortem](/performance/slab-arena-postmortem/) — the POOL_CAP=64 cliff.
- BENCHMARKS.md (top-level) — the source-of-truth file; this page mirrors it.

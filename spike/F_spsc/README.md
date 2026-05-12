# POC-F — SPSC channel fast path

## Hypothesis

Volt's Vyukov MPMC ring at 180 ns/op is 5.6× slower than Go's 33 ns/op
because it pays the MPMC-correctness overhead (per-slot sequence
atomic, both sides CAS) on every op. For the common case of one
producer + one consumer (3-stage pipeline, network handler chain,
DataFrame stream), a comptime-specialized SPSC ring with just
head/tail atomics matches Go.

## Success criterion

≤ 35 ns/op for SPSC send+recv at cap=16. Match Go's chan.

## Implementation

- `spsc.zig` — comptime-generic `Spsc(T, cap)` (cap must be POT). Head
  and tail in separate cache-lines (128 B align) to avoid false
  sharing. 4 atomics per send+recv pair:
  - send: tail.load(.acquire), ring write, head.store(.release)
  - recv: head.load(.acquire), ring read, tail.store(.release)
- `bench_spsc.zig` — two threads, 200 K send+recv pairs. Busy-spin on
  full/empty (POC; production would park-on-block).

## How to run

```sh
zig build spike-F
```

## Result

- **Status:** **PASS — DECISIVE** ✓
- **Achieved:** **29 ns/op** (median of 11, min 27, max 34)
- **Date measured:** 2026-05-12 on macOS arm64, ReleaseFast

| Comparison | Number |
|---|---|
| POC-F SPSC | **29 ns/op** |
| Go chan cap=16 | 33 ns/op |
| Volt today (Vyukov MPMC) | 180 ns/op |
| **POC-F vs Go** | **1.14× FASTER than Go** |
| **POC-F vs Volt today** | **6.2× faster** |

## Caveat: busy-spin only (no park-on-block)

POC-F's bench has both producer and consumer running flat-out, so the
ring is rarely full or empty. Busy-spin on the rare wait is fine for
this measurement. In real workloads, send/recv may block for ms at a
time — busy-spin would be a CPU hog.

The v2 production channel should:
- Stay lock-free in the fast path (POC-F's 29 ns/op)
- Park the blocked side after ~1-2 µs of spin (waste-budget for cache
  warmth gains)
- Wake via the runtime's task scheduler (NOT via OS thread park)

Go's chan does this. POC-F's bench measures the FAST PATH only, which
is what matters for throughput. The park-on-block path is a correctness
concern, not a hot-path-performance concern.

## What this means for the v2 architecture

- Replace `src/channel/Channel.zig` (Vyukov MPMC) with two channel
  types:
  - `Spsc(T, cap)` — comptime SPSC. Fastest. Constrained to one
    sender + one receiver per channel instance.
  - `Mpmc(T, cap)` — Vyukov-style for the general case. Keep current
    perf or improve.
- User picks the right one at `Channel.init`. Comptime checks if
  possible (the type system can detect single-sender single-receiver
  scope-bound channels and pick SPSC automatically — Zig comptime
  enables this).

## Two of three headlines now beat Go

Combining with POC-C:

| Workload | Volt today | POC | Go | POC vs Go |
|---|---|---|---|---|
| spawn+join | 4,163 ns | **POC-C: 93 ns** | 149 ns | **1.6× faster** |
| channel SPSC | 180 ns | **POC-F: 29 ns** | 33 ns | **1.14× faster** |
| TCP echo | 10,960 ns | POC-H: TBD | 9,050 ns | ? |

If POC-H lands at-or-better-than Go on TCP echo, the v2 architecture is
fully validated for the user's three headline workloads. Branch 1
(PASS — proceed to rewrite) is essentially confirmed.

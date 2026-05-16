---
title: Benchmarking
description: The bench gate, what each bench measures, how to read the numbers, and how to add your own.
---

The bench gate is mandatory for any landing that touches the
scheduler, coroutine struct, sync primitives, or allocation path.
See [Contributing](/appendix/contributing/) for the full
protocol; this page covers what each bench does and how to read
its output.

## Running

Each bench is its own executable:

```sh
zig build bench-yield                # ~9 ns/op one-way ctx switch
zig build bench-spsc                 # ~12 ns/op send+recv
zig build bench-mpmc                 # ~54-157 ns/op (1×1 / 2×2 / 4×4)
zig build bench-mutex                # ~15 ns/op contended
zig build bench-spawn-hot            # ~100-575 ns/op (workers=1..11)
zig build bench-fanout-scaling       # N drivers × N workers
zig build bench-scaling              # single-driver curve
zig build bench-parallel-compute     # speedup curve
zig build bench-tcp-echo             # ~8,500 ns/RTT
zig build bench-rss                  # 16.6 KiB/coro at N=10000
zig build stress                     # 45s mixed-primitive, must PASS
```

Output goes to stdout. Each bench prints a small table or summary
line; see `bench/*.zig` for the exact format.

For environment knobs:

```sh
VOLT_BENCH_WORKERS=4 zig-out/bin/volt-bench-spawn-hot
```

Override worker count for benches that respect the env var
(`spawn-hot`, `scaling`).

## What each one measures

### `bench-yield`

Two coroutines yielding back and forth on a single worker. Pure
dispatch-loop cost: `volt.yield()` re-queues the current
coroutine to its worker's queue tail, the dispatch loop pops the
other one, `context.swap` runs, repeat.

The ~9 ns/op is the assembly swap (~28 instructions) plus the
queue push/pop and the loop bookkeeping. Bound below by the
CPU's branch-predictor / cache-hit profile on this exact code.

### `bench-spsc`

Single-producer + single-consumer ring at `cap=16`. End-to-end
send + recv, no parking (the steady-state hits the ring fast
path, never goes to the parking lot).

12 ns/op for full round-trip is close to the cycle floor on
arm64 — the cells are on separate cache lines, the modulo is a
bitmask (cap is power-of-2 comptime), the only synchronisation
is acquire/release on the head/tail counters.

### `bench-mpmc`

Vyukov MPMC ring at 3 shapes: 1×1, 2×2, 4×4 (producers ×
consumers). The 1×1 case shows the Vyukov fast path (single
CAS); 4×4 shows the CAS retry cost under contention.

Numbers: 1×1 ~54 ns, 2×2 ~91 ns, 4×4 ~157 ns. Scales sublinearly
under contention.

### `bench-mutex`

8 coroutines, 50K increments each, single shared `volt.Mutex`.
The mutex sits on the parking lot. Fast path: 1 CAS on the state
word. Slow path: spin briefly, CAS to CONTENDED, park.

15 ns/op under 8-way contention. Compare to Go's
`sync.Mutex` benched the same shape: 81 ns/op. The gap is mostly
Go paying write-barrier costs on parked goroutine pointers.

### `bench-spawn-hot`

Canonical multi-worker spawn. Driver spawns BATCH=1000 tasks,
waits on a Notify barrier when all complete, repeats for 10s.

This is the bench that caught the [slab arena
cliff](/performance/slab-arena-postmortem/). At `workers=1` it's
101 ns/op; at `workers=11` it's 575 ns/op (synthetic spawn-heavy
shape where adding workers can only hurt).

Take 5-run medians; variance is 15-40%.

### `bench-fanout-scaling`

N driver coroutines, N workers, each driver running its own
spawn-join loop. Real parallel work — the workers fan out.

Drivers = workers (= N) keeps each P busy with its own driver,
near-zero work-stealing. Scaling ratio (w=N vs w=1) shows the
coordination overhead per added worker. Target: under 2× across
1→11.

### `bench-scaling`

Single-driver curve. Adding workers can only hurt this shape
(no parallel work). Receipt for "what's the cost of an idle
worker spinning."

### `bench-parallel-compute`

256 CPU-bound tasks (XorShift, ~50 µs each) across N workers.
Measures the actual speedup curve — what work-stealing schedulers
are built for.

Target: 6.6×+ at 8 workers on an 11-core host. Near-ideal.

### `bench-tcp-echo`

64 clients × 16 RTT × 1 KB. End-to-end TCP echo, both client
and server running on the same Runtime. Routes through the
kqueue reactor for every read/write.

~8,500 ns/RTT (Volt) vs ~9,050 ns/RTT (Go). Both runtimes pay
the OS networking syscall cost; gap is small.

### `bench-rss`

Spawn N idle coroutines (all parked on a Notify), sample peak
RSS. Reports per-coroutine memory.

| N | per-coro |
|---|---|
| 100 | 34.9 KiB (runtime overhead dominates) |
| 1,000 | 18.2 KiB |
| 5,000 | 16.7 KiB |
| 10,000 | 16.6 KiB |

The 16.6 KiB asymptote is one Darwin page (the body region of
the slab arena's slot). The slot's full 256 KiB reservation is
PROT_NONE and contributes zero RSS until used.

### `stress`

45 seconds, three 15-second phases: spawn-join, Mutex contended,
Spsc channel. Multi-worker (default = NumCPU). Watchdog catches
hangs.

Total throughput is reported at the end (~838M ops across 45s on
Darwin arm64 11-core). Used as a pre-merge gate; any regression
larger than ~5% on the total is investigated.

Run 3 times. If all three PASS, the gate is clear.

## How to read the numbers

Five rules:

1. **5-run medians for spawn-heavy benches.** Variance is real
   (15-40%). Single-shot numbers mislead.
2. **System load matters.** Capture `uptime` alongside the run.
   Comparison only holds across runs at similar load.
3. **`ReleaseFast` build only.** Debug numbers are 5-50× off
   and meaningless for perf claims.
4. **Use the Go reference for scale, not as a target.** Go has
   15+ years of optimisation; matching it everywhere isn't the
   goal. The goal is "within a sensible range."
5. **A change that moves a bench by less than 20% is noise.**
   Don't claim improvements under that threshold; don't fail
   landings for regressions under it.

See [Benchmarks](/performance/benchmarks/) for the full
methodology and current measurement table.

## Adding a new bench

Each bench is a standalone executable under `bench/`. The shape:

```zig
// bench/bench_your_thing.zig
const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn root(n: u32) !u64 {
    const start = nanosNow();
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        // ... do the thing N times ...
    }
    return @intCast(nanosNow() - start);
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    const n: u32 = 100_000;
    const elapsed_ns = try (try rt.run(root, .{n}));
    std.debug.print("your thing: {d} ns/op ({d} ops in {d} ns)\n", .{
        elapsed_ns / n, n, elapsed_ns,
    });
}
```

Register the bench in `build.zig`:

```zig
const benches = [_]struct { name: []const u8, src: []const u8, desc: []const u8 }{
    // ... existing entries ...
    .{ .name = "your-thing", .src = "bench/bench_your_thing.zig", .desc = "..." },
};
```

Then `zig build bench-your-thing`.

## Profiling

For a flame graph or function-level profile:

```sh
# macOS — recommended
samply record -- zig-out/bin/volt-bench-spawn-hot

# Linux
perf record --call-graph dwarf ./zig-out/bin/volt-bench-spawn-hot
perf report
```

`samply` opens the Firefox Profiler UI with the trace loaded.
Functions you'll see hot: `voltCoroSwap` (ctx switch),
`Worker.dispatch` / `dispatch` (the loop), the channel `recv` /
`send` slow paths when channels are hot, `parkOn`.

If you see anything else dominating, that's worth investigating.
The architecture chapter (especially [Multi-worker
profile](/performance/multi-worker-profile/)) has real
sampling-driven receipts.

## Memory footprint

`bench-rss` is the per-coroutine receipt. For runtime overhead:

```sh
zig-out/bin/volt-bench-rss
```

Read the table; per-coro should asymptote to ~16.6 KiB on Darwin
(one body page) regardless of N — that's the design contract of
the slab arena.

For diagnostic snapshots in production, `rt.dumpState()` prints
per-P spawn/done counters. No live per-coroutine memory query
today; use OS-level RSS measurement.

## See also

- [Benchmarks](/performance/benchmarks/) — the full results table.
- [Contributing](/appendix/contributing/) — phase-landing protocol.
- [Multi-worker profile](/performance/multi-worker-profile/) — profile-driven tuning receipts.
- [Slab arena postmortem](/performance/slab-arena-postmortem/) — what a 30× regression looked like.

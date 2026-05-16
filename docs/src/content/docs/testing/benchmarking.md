---
title: Benchmarking
description: zig build bench — what gets measured, what the numbers mean, and how to add your own.
---

```sh
zig build bench
```

Runs the core benchmark suite in `ReleaseFast`. Output is a small
table:

```
spawn + immediate complete: 622 ns/op (200000 ops in 124492000 ns)
yield ping-pong (one-way switch): 71 ns/op (200000 ops in 14301000 ns)
channel SPSC cap=16: 191 ns/op (100000 ops in 19135000 ns)
mutex lock/unlock: 1397 ns/op (400000 ops in 559121000 ns)
```

These are the four primitives most likely to be in your hot path.
The numbers are end-to-end including the dispatch loop —
ReleaseFast on a typical Apple Silicon workstation.

## What's being measured

### `spawn + immediate complete`

```zig
fn benchSpawn(n: u32) !void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const j = try volt.launch(noop, .{});
        try j.join();
        volt.destroyJob(j);
    }
}
fn noop() void {}
```

One spawn + one join + one destroy per iteration. Includes a
`mmap`/`mprotect` syscall pair on the first N iterations (until
the slab pool warms up), then near-zero stack alloc cost from
recycled stacks.

The slab pool effect: this benchmark is dominated by per-spawn
stack reuse. Without the pool, each iteration would cost an
extra ~µs for the syscall. With the pool, you're seeing the
context-switch cost plus a CAS or two.

### `yield ping-pong`

```zig
fn pingPong() !void {
    var i: u32 = 0;
    while (i < N) : (i += 1) try volt.yield();
}
```

Two coroutines yielding back and forth. Pure dispatch loop cost.
~70 ns per swap is the assembly context switch (~10ns) plus the
deque push/pop and the worker's loop bookkeeping.

### `channel SPSC cap=16`

```zig
// One producer, one consumer, 100K messages, capacity 16.
```

End-to-end Channel send + recv. Includes the Vyukov ring CAS
plus the wake protocol when the channel hits empty/full and a
waiter parks.

If you're sending small messages at high rate, this is the
benchmark closest to your workload. ~190 ns/op = ~5M messages/sec
on this hardware.

### `mutex lock/unlock`

```zig
// 8 coroutines, 50K increments each, single shared mutex.
```

Heavily contended mutex. The number is per-acquire: 1397 ns under
8-way contention is the FIFO waiter list serialization cost.
Uncontended `tryLock` is single-CAS — measure separately if your
workload is uncontended.

## Comparing across runs

A few caveats for interpreting numbers:

- **Noise is real.** Run the bench 5 times and look at the
  median; single-run numbers vary 5-10%.
- **CPU governor matters.** On Linux, `performance` governor
  gives more reproducible numbers than `ondemand`.
- **Background load matters.** Close everything else. Even
  another process eating one core changes results meaningfully.

## Comparing to other runtimes

A common ask: "is this faster than Tokio?" The answer depends on
the workload — Volt is built on different tradeoffs (stackful
vs stackless, see [Stackless vs Stackful](/architecture/stackful-design/))
so direct comparisons can mislead.

The honest take:

- For thousands of concurrent active operations with complex
  per-task work, Volt's stackful model gives simpler code and
  comparable throughput.
- For millions of parked tasks with tiny per-task state, Tokio's
  stackless model uses dramatically less memory.

The bench numbers above are useful for tracking regressions
across Volt versions, not for cross-runtime comparisons. We don't
ship a Tokio comparison harness because it'd compare apples to
oranges.

## Adding your own benchmark

`bench/bench_core.zig` has the existing harness. The shape:

```zig
fn benchYourThing(n: u32) !void {
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, runBench, .{n});
    print("your thing: {d} ns/op ({d} ops in {d} ns)\n", .{ wall / n, n, wall });
}

fn runBench(n: u32) !u64 {
    const start = volt.Instant.now();
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        // ... do the thing N times ...
    }
    return start.elapsed().asNanos();
}
```

Register your bench in `main()`:

```zig
pub fn main() !void {
    try benchSpawn(200_000);
    try benchYield(200_000);
    try benchChannel(100_000);
    try benchMutex(8, 50_000);
    try benchYourThing(100_000);   // ← add here
}
```

Then `zig build bench`.

## Profiling under bench

For a flame graph or function-level profile:

```sh
# macOS
xcrun xctrace record --template "Time Profiler" --launch ./zig-out/bin/volt-bench

# Linux
perf record --call-graph dwarf ./zig-out/bin/volt-bench
perf report
```

Functions you'll see at the top: `voltCtxSwap` (ctx switch),
`Worker.dispatch` (the loop), `Channel.recvSlowPath` (when
channels are hot), `parkCurrent`. If you see anything else
dominating, that's worth investigating.

## Memory footprint

Volt allocates per-coroutine stacks; the stack pool reuses them.
Steady-state memory should match the slab pool size + the
runtime's own allocations. To see live counts:

```zig
const m = try volt.observability.metrics(alloc, rt);
defer m.deinit(alloc);

std.log.info("stack pool hits: {d}, misses: {d}", .{
    m.stack_pool_hits, m.stack_pool_misses,
});
```

A high miss rate means the pool is undersized for your spawn
rate; a high hit rate means recycling is working as intended.

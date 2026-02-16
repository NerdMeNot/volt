---
title: Benchmarking
description: How to run Volt benchmarks, interpret results, write new benchmarks, and profile performance.
---

Volt includes a comprehensive benchmark suite that measures the performance of sync primitives, channels, and async task coordination. The benchmarks use median-based statistics for robustness against outliers.

## Running benchmarks

### Quick start

```bash
# Build and run the full benchmark suite
zig build bench

# Machine-readable JSON output (used by the comparison tool)
zig build bench -- --json
```

The benchmark binary is compiled with `ReleaseFast` optimization regardless of the build mode you specify.

### Available build steps

| Command | Description |
|---------|-------------|
| `zig build bench` | Run the Volt benchmark suite |
| `zig build bench -- --json` | JSON output for automated processing |
| `zig build compare` | Side-by-side comparison with Tokio |

## Benchmark structure

The benchmark suite (`bench/volt_bench.zig`) is organized into four tiers that measure progressively more complex operations:

### Tier 1: Sync fast path (1M ops)

Single-thread, no runtime. Measures raw data-structure cost -- CAS, atomics, memory barriers.

| Benchmark | Operation |
|-----------|-----------|
| Mutex | `tryLock()` / `unlock()` |
| RwLock (read) | `tryReadLock()` / `readUnlock()` |
| RwLock (write) | `tryWriteLock()` / `writeUnlock()` |
| Semaphore | `tryAcquire(1)` / `release(1)` |
| OnceCell get | `get()` on an initialized cell |
| OnceCell set | `set()` on a fresh cell each time |
| Notify | `notifyOne()` + `waitWith()` |
| Barrier | `waitWith()` on barrier(1) |

### Tier 2: Channel fast path (100K ops)

Single-thread, no runtime. Measures channel buffer operations without scheduling overhead.

| Benchmark | Operation |
|-----------|-----------|
| Channel send | `trySend()` to fill buffer |
| Channel recv | `tryRecv()` to drain buffer |
| Channel roundtrip | `trySend()` + `tryRecv()` on cap=1 |
| Oneshot | `send()` + `tryRecv()` |
| Broadcast | 1 sender, 4 receivers |
| Watch | `send()` + `borrow()` |

### Tier 3: Async multi-task (10K ops)

Full async runtime with multiple workers. Measures real-world contention: scheduling, waking, and backpressure.

| Benchmark | Operation |
|-----------|-----------|
| MPMC | 4 producers + 4 consumers, buffer=1000 |
| Mutex contended | 4 tasks contending on one mutex |
| RwLock contended | 4 readers + 2 writers |
| Semaphore contended | 8 tasks, 2 permits |

### Tier 4: Task scheduling (1K ops)

Full async runtime. Measures the scheduler's core task lifecycle: spawn overhead, batch throughput, and blocking pool round-trip.

| Benchmark | Operation |
|-----------|-----------|
| Spawn + await | Serial `spawn()` + `await()` of a noop task (scheduler round-trip) |
| Spawn batch | Spawn 100 noop tasks + await all (measures throughput, ~ns per task) |
| Blocking spawn | `spawnBlocking()` + wait for a noop (blocking pool overhead) |

### Configuration

All configuration constants are defined at the top of `bench/volt_bench.zig` and must match `bench/rust_bench/src/main.rs` exactly for fair comparison:

```zig
const SYNC_OPS: usize = 1_000_000;    // Tier 1
const CHANNEL_OPS: usize = 100_000;   // Tier 2
const ASYNC_OPS: usize = 10_000;      // Tier 3
const ITERATIONS: usize = 10;
const WARMUP: usize = 5;
const NUM_WORKERS: usize = 4;
```

## Reading benchmark output

### Text output

```
  TIER 1: SYNC FAST PATH (1000000 ops, single-thread, try_*)
  ------------------------------------------------------------------------
  Mutex tryLock/unlock                        6.6 ns/op     151515152 ops/sec
  RwLock tryReadLock/readUnlock               6.7 ns/op     149253731 ops/sec
```

- **ns/op**: Nanoseconds per operation (lower is better). Computed as `median_ns / ops_per_iter`.
- **ops/sec**: Operations per second (higher is better). Computed as `1e9 / ns_per_op`.

### JSON output

The `--json` flag produces structured output with additional fields:

```json
{
  "metadata": {
    "runtime": "Volt",
    "sync_ops": 1000000,
    "iterations": 10,
    "warmup": 5,
    "workers": 4
  },
  "benchmarks": {
    "mutex": {
      "ns_per_op": 6.60,
      "ops_per_sec": 151515152,
      "median_ns": 6600000,
      "min_ns": 6500000,
      "ops_per_iter": 1000000,
      "bytes_per_op": 0.00,
      "allocs_per_op": 0.0000
    }
  }
}
```

- **bytes_per_op**: Heap bytes allocated per operation (tracked by a counting allocator wrapper).
- **allocs_per_op**: Number of heap allocation calls per operation.
- **median_ns**: Median of all measured iterations (the primary statistic).
- **min_ns**: Minimum across all iterations (useful for detecting lower bounds).

### Statistics methodology

Each benchmark runs `WARMUP` (5) discarded iterations followed by `ITERATIONS` (10) measured iterations. The reported ns/op uses the **median** of the measured iterations, which is robust against GC pauses, background processes, and other outliers.

## Allocation tracking

The benchmark suite wraps the allocator with a `CountingAllocator` that atomically tracks total bytes and allocation count. This is reset before each timed iteration, so the reported bytes/op and allocs/op reflect a single run:

```zig
const CountingAllocator = struct {
    inner: Allocator,
    bytes_allocated: std.atomic.Value(usize),
    alloc_count: std.atomic.Value(usize),
    // ...
};
```

This allows measuring the allocation overhead of each primitive without modifying the primitive code itself.

## Writing new benchmarks

To add a new benchmark:

1. Add a function in `bench/volt_bench.zig` following the existing pattern:

```zig
fn benchMyPrimitive() BenchResult {
    var stats = Stats{};

    // Warmup
    for (0..WARMUP) |_| {
        // Run the operation SYNC_OPS times
    }

    // Measured iterations
    for (0..ITERATIONS) |_| {
        alloc_counter.reset();
        const start = std.time.nanoTimestamp();

        for (0..SYNC_OPS) |_| {
            // Your operation here
        }

        stats.add(@intCast(std.time.nanoTimestamp() - start));
    }

    return .{
        .stats = stats,
        .ops_per_iter = SYNC_OPS,
        .total_bytes = alloc_counter.getAllocBytes(),
        .total_allocs = alloc_counter.getAllocCount(),
    };
}
```

2. Call it from `main()` and add it to both the text and JSON output sections.

3. If comparing with Tokio, add a matching benchmark in `bench/rust_bench/src/main.rs` with the same operation count and configuration.

### Best practices

- Use `std.mem.doNotOptimizeAway()` to prevent the compiler from eliding work.
- Always include warmup iterations to stabilize caches and branch predictors.
- For Tier 3 (async) benchmarks, create the runtime once and reuse it across iterations.
- Match the Tokio benchmark exactly: same operation count, same thread count, same buffer sizes.

## Profiling

### macOS (Instruments)

Build the benchmark binary and profile with Instruments:

```bash
# Build the benchmark
zig build bench

# Profile with Instruments (Time Profiler)
xcrun xctrace record --template 'Time Profiler' \
  --launch zig-out/bench/volt_bench

# Or use the Instruments GUI
open zig-out/bench/volt_bench  # Then attach with Instruments
```

### Linux (perf)

```bash
# Build the benchmark
zig build bench

# Record perf data
perf record -g zig-out/bench/volt_bench

# Analyze
perf report
```

### Flamegraphs

```bash
perf record -F 99 -g zig-out/bench/volt_bench
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
```

### Tips

- Profile with `ReleaseFast` (the default for benchmarks) to see realistic hot paths.
- Focus on Tier 3 benchmarks for scheduling-related bottlenecks.
- Compare flamegraphs before and after optimization to verify the change addressed the right hot path.
- The `debug_scheduler` flag in `Scheduler.zig` can be set to `true` for detailed tracing, but this significantly slows execution.

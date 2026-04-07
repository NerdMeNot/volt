---
title: Comparing with Tokio
description: How Volt benchmarks against Tokio, the comparison framework, current results, and methodology.
---

Volt includes a head-to-head comparison framework that runs identical benchmarks against [Tokio](https://github.com/tokio-rs/tokio), the Rust async I/O runtime that inspired much of Volt's architecture. Volt's scheduler, sync primitives, cooperative budgeting, and ScheduledIo state machine are all adapted from Tokio's design -- we benchmark against Tokio to keep ourselves honest, not to claim superiority.

## Running the comparison

```bash
zig build compare
```

This single command:

1. Builds the Tokio benchmark (`cargo build --release` in `bench/rust_bench/`)
2. Runs the Volt benchmark (`zig-out/bench/volt_bench --json`)
3. Runs the Tokio benchmark (`bench/rust_bench/target/release/volt_rust_bench --json`)
4. Parses both JSON outputs and prints a formatted comparison table

### Prerequisites

- **Zig 0.15.2+** for Volt
- **Rust 1.86+** with Cargo for Tokio
- Both toolchains must be in your `PATH`

## The comparison framework

### Architecture

```
bench/
  volt_bench.zig              # Volt benchmarks (Zig)
  compare.zig                 # Comparison driver (Zig)
  rust_bench/
    Cargo.toml                # Tokio dependency
    src/main.rs               # Tokio benchmarks (Rust)
```

The `compare.zig` driver is a standalone Zig program that:

1. Resolves the project root directory from its own executable path.
2. Invokes both benchmark binaries with `--json` flags via `std.process.Child`.
3. Parses the JSON output into `BenchmarkResults` structs.
4. Computes winners using a 5% tolerance band (ratios within 0.95--1.05 are reported as ties).
5. Prints a Unicode box-drawing table with ANSI color coding.

### Matching methodology

Both benchmark suites share identical configuration constants to ensure a fair comparison:

| Constant | Value | Applies to |
|----------|-------|------------|
| `SYNC_OPS` | 1,000,000 | Tier 1 |
| `CHANNEL_OPS` | 100,000 | Tier 2 |
| `ASYNC_OPS` | 10,000 | Tier 3 |
| `ITERATIONS` | 10 | All tiers |
| `WARMUP` | 5 | All tiers |
| `NUM_WORKERS` | 4 | Tier 3 |
| `MPMC_BUFFER` | 1,024 | MPMC benchmark |
| `CONTENDED_MUTEX_TASKS` | 4 | Contended mutex |
| `CONTENDED_SEM_TASKS` | 8 | Contended semaphore |
| `CONTENDED_SEM_PERMITS` | 2 | Contended semaphore |

Both sides use the same statistical methodology: median of 10 iterations after 5 warmup iterations discarded.

### Allocation tracking

Both sides track heap allocations:

- **Volt**: `CountingAllocator` wrapping `GeneralPurposeAllocator`, using atomics for thread safety.
- **Tokio**: Custom `GlobalAlloc` wrapper around `System`, using `AtomicUsize` counters.

This allows comparing not just speed but memory efficiency (bytes per operation).

### What each tier measures

**Tier 1 (Sync fast path)**: Both sides call `try_lock`/`try_acquire` -- the synchronous, non-blocking API. No runtime overhead. This isolates the raw cost of the data structure: CAS operations, atomic fences, and memory barriers.

**Tier 2 (Channel fast path)**: Both sides call `try_send`/`try_recv` -- synchronous buffer operations. No scheduling or waking. This isolates the channel's ring buffer implementation.

**Tier 3 (Async multi-task)**: Both sides use their full runtimes. Volt uses `Io.spawn()` with `FutureTask` state machines. Tokio uses `tokio::spawn()` with `async`/`.await`. This measures real-world contention including scheduling, waking, and backpressure.

**Tier 4 (Task scheduling)**: Both sides measure the raw cost of spawning and awaiting tasks. This isolates the scheduler's task lifecycle overhead: spawn, schedule, poll, and join.

## Current results

Test platform: MacBook Pro (Apple M3 Pro, 11 cores, 18 GB RAM), macOS arm64, Zig 0.15.2, Rust 1.86.0 (Tokio 1.43).

### Synchronization -- Uncontended

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex | 31.8 ns | 28.2 ns | 0 | 0 | Tokio +1.1x |
| RwLock (read) | 25.3 ns | 27.1 ns | 0 | 0 | Volt +1.1x |
| RwLock (write) | 20.7 ns | 25.2 ns | 0 | 0 | Volt +1.2x |
| Semaphore | 22.7 ns | 33.7 ns | 0 | 0 | Volt +1.5x |

### Synchronization -- Contended

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex (4 tasks) | 91.7 ns | 207.9 ns | 0.1 | 0.2 | Volt +2.3x |
| RwLock (4R + 2W) | 149.5 ns | 247.4 ns | 0.1 | 0.2 | Volt +1.7x |
| Semaphore (8T, 2 permits) | 139.4 ns | 323.0 ns | 0.2 | 0.2 | Volt +2.3x |

### Channels

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Channel send | 11.1 ns | 16.3 ns | 21 | 9 | Volt +1.5x |
| Channel recv | 11.4 ns | 22.3 ns | 21 | 9 | Volt +2.0x |
| Channel roundtrip | 23.1 ns | 37.8 ns | 0 | 0 | Volt +1.6x |
| Channel MPMC (4P + 4C) | 73.3 ns | 132.8 ns | 1.8 | 2.1 | Volt +1.8x |
| Oneshot | 27.1 ns | 51.5 ns | 0 | 72 | Volt +1.9x |
| Broadcast (4 receivers) | 95.2 ns | 143.8 ns | 16 | 126.9 | Volt +1.5x |
| Watch | 45.7 ns | 145.4 ns | 0 | 0 | Volt +3.2x |

### OnceCell

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| OnceCell get (hot path) | 2.0 ns | 1.0 ns | 0 | 0 | Tokio +2.0x |
| OnceCell set | 41.8 ns | 90.8 ns | 0 | 64 | Volt +2.2x |

### Coordination

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Barrier | 50.9 ns | 1,312.8 ns | 0 | 1,064 | Volt +25.8x |
| Notify | 15.6 ns | 18.9 ns | 0 | 0 | Volt +1.2x |

### Task Scheduling

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Spawn + await | 29,597 ns | 18,946 ns | 80 | 128 | Tokio +1.6x |
| Spawn batch (per task) | 601.0 ns | 611.4 ns | 80 | 136 | Tie |
| Blocking spawn | 25,037 ns | 12,483 ns | 64 | 256 | Tokio +2.0x |

### Summary

```
Volt wins: 16 / 21    Tokio wins: 4 / 21    Tie: 1 / 21

Total bytes per op:   Volt 284.1    Tokio 1,867.6  (6.6x less)
Total allocs per op:  Volt 3.0      Tokio 17.1     (5.7x fewer)
```

## Where Volt wins and why

Volt leads in 16 of 21 benchmarks. The primary advantages come from language-level properties and a zero-allocation architecture:

**Intrusive waiters** -- Waiter nodes are embedded directly in futures on the stack, eliminating the heap allocations that Tokio makes for waiter bookkeeping. This is the single largest factor: 284 bytes/op vs 1,868 bytes/op across all benchmarks.

**Comptime specialization** -- Generic lock and channel types are monomorphized at compile time with concrete waker types, removing the virtual dispatch Tokio pays through `dyn Future` trait objects and `Waker` vtables.

**Vyukov MPMC ring buffer** -- The bounded channel uses a lock-free ring buffer with per-slot sequence counters, power-of-2 bitmask indexing, and interleaved slot layout for spatial locality. The channel MPMC benchmark now shows Volt at 73ns vs Tokio at 133ns.

**Lock-free semaphore release** -- The `fast_waiter` atomic slot allows `release()` to serve the most recent waiter via a single atomic swap, bypassing the mutex entirely. Under the contended semaphore benchmark (8 tasks, 2 permits), this reduced latency from ~220 ns to ~139 ns, flipping the result from Tokio +1.2x to Volt +2.3x. See [Fast Waiter Slot](/design/fast-waiter-slot/) for the design.

**O(1) bitmap worker waking** -- The scheduler uses `@ctz` on a packed 64-bit bitmap to find idle workers in constant time, where Tokio scans a list.

**Zero-allocation oneshot and barrier** -- Tokio's oneshot allocates a shared `Arc<Inner>` (72 bytes) and its barrier allocates tracking state (1,064 bytes). Volt uses stack-embedded atomics for both.

## Where Tokio wins and why

Tokio outperforms Volt in four benchmarks, with two being significant:

**Spawn + await** (by ~1.6x) -- Tokio's `tokio::spawn` has years of optimization for the single-task spawn-and-join pattern, including optimized `JoinHandle` internals and a highly tuned `RawTask` implementation. Volt's `FutureTask` setup overhead is higher per task.

**Blocking spawn** (by ~2.0x) -- Tokio's blocking pool has years of tuning for thread wake latency and reuse. The gap widened from earlier measurements, likely due to condvar/futex interaction differences between Tokio's `parking_lot` and Zig's `std.Thread.Condition`.

**Mutex uncontended** (by ~1.1x) and **OnceCell get** (by ~2.0x) -- Minor advantages from Tokio's parking_lot-style adaptive spinning and Rust's extremely optimized `std::sync::Once` implementation. The OnceCell hot path is a single relaxed atomic load in both implementations, so the 2x gap likely reflects measurement noise at the sub-nanosecond scale.

## Interpreting results

### Caveats

- These are **microbenchmarks** on a single machine (Apple M3 Pro, macOS arm64). Results on Linux x86_64 may differ significantly due to different cache hierarchies, memory ordering costs, and kernel scheduling.
- Run-to-run variance is typically 5--15%. The 5% tie band accounts for this.
- Zig and Rust have different compilation models. Some differences may reflect compiler optimization strategy rather than runtime design.
- Tokio is mature and battle-tested at scale. Volt is new and less proven in production.
- Bytes-per-op reflects allocator overhead in the benchmark harness, not necessarily application-level memory usage.

### When to re-run

Re-run the comparison after:

- Changing any sync primitive, channel, or scheduler code
- Updating the Zig or Rust compiler version
- Changing the benchmark configuration constants
- Testing on a different platform

### Adding a new benchmark to the comparison

1. Add the benchmark to both `bench/volt_bench.zig` and `bench/rust_bench/src/main.rs` with identical configuration.
2. Add a field to the `BenchmarkResults` struct in `bench/compare.zig`.
3. Add a mapping in the `getBenchEntry` function.
4. Add a `printRow` call in the appropriate section of `main()`.

## Acknowledgments

Tokio has been the gold standard for async I/O runtimes since 2018. Volt's scheduler, sync primitives, cooperative budgeting, and ScheduledIo state machine are all adapted from Tokio's design. Every core architectural pattern in Volt traces back to something the Tokio team got right first. We would not be here without their years of design, iteration, and documentation. We benchmark against Tokio to keep ourselves honest, not to claim superiority.

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
| Mutex | 6.6 ns | 7.9 ns | 0 | 0 | Volt +1.2x |
| RwLock (read) | 6.7 ns | 7.8 ns | 0 | 0 | Volt +1.2x |
| RwLock (write) | 6.4 ns | 8.1 ns | 0 | 0 | Volt +1.3x |
| Semaphore | 6.4 ns | 7.5 ns | 0 | 0 | Volt +1.2x |

### Synchronization -- Contended

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex (4 tasks) | 53.5 ns | 76.9 ns | 0.1 | 0.2 | Volt +1.4x |
| RwLock (4R + 2W) | 50.0 ns | 97.2 ns | 0.1 | 0.2 | Volt +1.9x |
| Semaphore (8T, 2 permits) | 220.2 ns | 180.8 ns | 0.2 | 0.2 | Tokio +1.2x |

### Channels

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Channel send | 2.7 ns | 5.9 ns | 16 | 9 | Volt +2.2x |
| Channel recv | 6.3 ns | 9.6 ns | 16 | 9 | Volt +1.5x |
| Channel roundtrip | 6.3 ns | 12.5 ns | 0 | 0 | Volt +2.0x |
| Channel MPMC (4P + 4C) | ~75 ns | ~65 ns | 1.8 | 2.2 | Tokio ~1.1x |
| Oneshot | 4.1 ns | 15.2 ns | 0 | 72 | Volt +3.8x |
| Broadcast (4 receivers) | 22.9 ns | 50.3 ns | 16 | 126.9 | Volt +2.2x |
| Watch | 13.3 ns | 44.1 ns | 0 | 0 | Volt +3.3x |

### OnceCell

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| OnceCell get (hot path) | 0.4 ns | 0.6 ns | 0 | 0 | Volt +1.4x |
| OnceCell set | 9.6 ns | 29.2 ns | 0 | 64 | Volt +3.0x |

### Coordination

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Barrier | 15.6 ns | 359.2 ns | 0 | 1064 | Volt +23.1x |
| Notify | 9.8 ns | 9.3 ns | 0 | 0 | Tie |

### Task Scheduling

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Spawn + await | 6,459 ns | 7,819 ns | 80 | 128 | Volt +1.2x |
| Spawn batch (per task) | 100.1 ns | 246.7 ns | 80 | 136 | Volt +2.5x |
| Blocking spawn | ~6,400 ns | 6,236 ns | 72 | 256 | Tokio ~1.1x |

### Summary

```
Volt wins: 17 / 21    Tokio wins: 3 / 21    Tie: 1 / 21

Total bytes per op:   Volt 282.2    Tokio 1,867.7  (6.6x less)
Total allocs per op:  Volt 3.0      Tokio 17.1     (5.7x fewer)
```

## Where Volt wins and why

Volt leads in 17 of 21 benchmarks. The primary advantages come from language-level properties and a zero-allocation architecture:

**Intrusive waiters** -- Waiter nodes are embedded directly in futures on the stack, eliminating the heap allocations that Tokio makes for waiter bookkeeping. This is the single largest factor: 282 bytes/op vs 1,868 bytes/op across all benchmarks.

**Comptime specialization** -- Generic lock and channel types are monomorphized at compile time with concrete waker types, removing the virtual dispatch Tokio pays through `dyn Future` trait objects and `Waker` vtables.

**Vyukov MPMC ring buffer** -- The bounded channel uses a lock-free ring buffer with per-slot sequence counters, giving excellent throughput in single-producer and few-producer cases.

**O(1) bitmap worker waking** -- The scheduler uses `@ctz` on a packed 64-bit bitmap to find idle workers in constant time, where Tokio scans a list.

**Zero-allocation oneshot and barrier** -- Tokio's oneshot allocates a shared `Arc<Inner>` (72 bytes) and its barrier allocates tracking state (1,064 bytes). Volt uses stack-embedded atomics for both.

**Task scheduling** -- Spawn+await and batch spawn are faster thanks to the LIFO slot fast path and lower per-task overhead (no `Arc<Task>` boxing). Batch spawn amortizes scheduling cost effectively, achieving ~100 ns/task vs Tokio's ~247 ns/task.

## Where Tokio wins and why

Tokio outperforms Volt in three areas:

**Contended semaphore** (by ~1.2x) -- Tokio's locking path benefits from `parking_lot`-style adaptive spinning: a hybrid strategy that spins briefly before parking the thread, tuned over years of production feedback. Volt's intrusive waiter approach skips spinning and goes straight to futex-based parking, which costs more under short hold times with high thread counts.

**MPMC channel** (by ~1.1x) -- Tokio delegates to `async_channel` (crossbeam-style) which has been tuned over years for multi-producer multi-consumer throughput. Volt uses a Vyukov MPMC bounded ring buffer with power-of-2 bitmask indexing, interleaved slot layout, and lock-free single-waiter fast path. The gap has narrowed from ~2.2x to ~1.1x through these optimizations.

**Blocking spawn** (by ~1.1x) -- Tokio's blocking pool has years of tuning for thread wake latency and reuse. Volt now matches Tokio closely after removing unnecessary yield phases, adding spin-after-complete in the blocking thread, and signaling the condvar outside the mutex.

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

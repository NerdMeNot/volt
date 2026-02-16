# Volt Benchmarks

Performance comparison between Volt and [Tokio](https://github.com/tokio-rs/tokio).

## Standing on Tokio's Shoulders

Tokio has been the gold standard for async I/O runtimes since 2018. It powers networking,
synchronization, and task scheduling in a huge fraction of the Rust ecosystem, and its design
has influenced every async runtime that followed -- including this one.

Volt wouldn't exist without Tokio. Its ScheduledIo state machine, its work-stealing
scheduler, its sync primitives, its approach to cooperative budgeting -- we studied and learned
from all of it. Where Volt differs, it's because Zig's comptime specialization, value
semantics, and zero-cost intrusive data structures open up paths that weren't available in Rust,
not because we found flaws in Tokio's design. Every architectural decision in Volt traces back
to something the Tokio team got right first.

We benchmark against Tokio to keep ourselves honest, not to claim superiority. Tokio is a
mature, battle-tested runtime with years of production use powering services at massive scale.
Volt is new. These numbers are a snapshot in time on one machine -- your mileage will vary.

## Test Platform

- **Machine**: MacBook Pro (Apple M3 Pro, 11 cores, 18 GB RAM)
- **OS**: macOS (arm64)
- **Zig**: 0.15.2
- **Rust**: 1.86.0 (Tokio 1.43)
- **Statistics**: Median of 10 iterations after 5 warmup discarded

## Running Benchmarks

```bash
zig build compare
```

This builds and runs both Volt (Zig) and Tokio (Rust) benchmarks with identical
configurations, then prints a side-by-side comparison table.

## Results

### Synchronization -- Uncontended

Single-thread lock/unlock cycle with no contention.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex | 6.6 ns | 7.9 ns | 0 | 0 | Volt +1.2x |
| RwLock (read) | 6.7 ns | 7.8 ns | 0 | 0 | Volt +1.2x |
| RwLock (write) | 6.4 ns | 8.1 ns | 0 | 0 | Volt +1.3x |
| Semaphore | 6.4 ns | 7.5 ns | 0 | 0 | Volt +1.2x |

### Channels

Message-passing primitives.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Channel send | 2.7 ns | 5.9 ns | 16 | 9 | Volt +2.2x |
| Channel recv | 6.3 ns | 9.6 ns | 16 | 9 | Volt +1.5x |
| Channel roundtrip | 6.3 ns | 12.5 ns | 0 | 0 | Volt +2.0x |
| Oneshot | 4.1 ns | 15.2 ns | 0 | 72 | Volt +3.8x |
| Broadcast (4 receivers) | 22.9 ns | 50.3 ns | 16 | 126.9 | Volt +2.2x |
| Watch | 13.3 ns | 44.1 ns | 0 | 0 | Volt +3.3x |

### OnceCell

One-time initialization primitive.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| OnceCell get (hot path) | 0.4 ns | 0.6 ns | 0 | 0 | Volt +1.4x |
| OnceCell set | 9.6 ns | 29.2 ns | 0 | 64 | Volt +3.0x |

### Coordination

Multi-party synchronization.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Barrier | 15.6 ns | 359.2 ns | 0 | 1064 | Volt +23.1x |
| Notify | 9.8 ns | 9.3 ns | 0 | 0 | Tie |

### Contended (Async, Multi-Task)

Multiple async tasks competing for the same resource on 4 worker threads.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex (4 tasks) | 53.5 ns | 76.9 ns | 0.1 | 0.2 | Volt +1.4x |
| RwLock (4R + 2W) | 50.0 ns | 97.2 ns | 0.1 | 0.2 | Volt +1.9x |
| Semaphore (8T, 2 permits) | 220.2 ns | 180.8 ns | 0.2 | 0.2 | Tokio +1.2x |
| Channel MPMC (4P + 4C) | 113.7 ns | 50.6 ns | 1.8 | 2.2 | Tokio +2.2x |

### Task Scheduling

Task spawn, join, and blocking pool overhead.

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Spawn + await (noop) | 6,459 ns | 7,819 ns | 80 | 128 | Volt +1.2x |
| Spawn batch (100 tasks) | 100.1 ns | 246.7 ns | 80 | 136 | Volt +2.5x |
| Blocking spawn + wait | 13,865 ns | 6,236 ns | 72 | 256 | Tokio +2.2x |

### Summary

```
Volt wins: 17 / 21    Tokio wins: 3 / 21    Tie: 1 / 21

Total bytes per op:        Volt 282.2     Tokio 1,867.7  (6.6x less)
Total allocs per op:       Volt 3.0       Tokio 17.1     (5.7x fewer)
```

## Where Tokio Wins

Tokio outperforms Volt in three areas:

- **Contended semaphore** (by 1.2x). Tokio's locking path benefits from `parking_lot`-style
  adaptive spinning: a hybrid strategy that spins briefly before parking the thread, tuned
  over years of production feedback. Volt's intrusive waiter approach skips spinning and goes
  straight to futex-based parking, which costs more under short hold times with high thread
  counts.

- **MPMC channel** (by 2.2x). Tokio delegates to a crossbeam-style segmented queue with
  per-segment backoff, purpose-built for multi-producer multi-consumer throughput. Volt
  uses a Vyukov MPMC bounded ring buffer, which is simpler and faster in SPSC/MPSC modes
  but pays more under symmetric contention from both sides.

- **Blocking spawn** (by 2.2x). Tokio's blocking pool has years of tuning for thread
  wake latency and reuse. Volt's blocking pool uses a simpler strategy that incurs
  higher overhead per spawn.

## What Helps Volt

Most of Volt's advantages come from Zig's language properties and a zero-allocation
architecture rather than algorithmic breakthroughs:

- **Intrusive waiters** -- waiter nodes are embedded directly in futures on the stack,
  eliminating the heap allocations Tokio makes for waiter bookkeeping. This is the single
  largest factor: 282 bytes/op vs 1,868 bytes/op across all benchmarks.

- **Comptime specialization** -- generic lock and channel types are monomorphized at compile
  time with concrete waker types, removing the virtual dispatch Tokio pays through
  `dyn Future` trait objects and `Waker` vtables.

- **Vyukov MPMC ring buffer** -- the bounded channel uses a lock-free ring buffer with
  per-slot sequence counters, giving excellent throughput in the common single-producer and
  few-producer cases.

- **O(1) bitmap worker waking** -- the scheduler uses `@ctz` on a packed bitmap to find idle
  workers in constant time, where Tokio scans a list.

- **Zero-allocation oneshot and barrier** -- Tokio's oneshot allocates a shared `Arc<Inner>`
  (72 bytes) and its barrier allocates tracking state (1,064 bytes). Volt uses
  stack-embedded atomics for both, explaining the 3.8x and 23.1x gaps respectively.

- **Batch task spawning** -- Volt's spawn pushes directly to the current worker's LIFO slot
  or local queue without going through the global injection queue, giving 2.5x better
  throughput when spawning many tasks from within the runtime.

The work-stealing scheduler, the cooperative budgeting, the ScheduledIo state machine,
the sync primitive designs -- these are all Tokio's ideas, adapted for Zig. Volt's
performance comes from applying Tokio's proven architecture in a language that gives us
value semantics, comptime generics, and zero-cost intrusive containers.

## Caveats

- Single machine, single OS -- results on Linux x86_64 may differ significantly
- Microbenchmarks, not real workloads -- production performance depends on many factors
- Zig and Rust have different compilation models; some differences may reflect compiler
  optimization strategy rather than runtime design
- Run-to-run variance is typically 5-15% on these benchmarks
- Tokio is mature and battle-tested at scale; Volt is new and less proven in production
- Bytes-per-op reflects allocator overhead in the benchmark harness, not necessarily
  application-level memory usage

## Acknowledgments

- [Tokio](https://github.com/tokio-rs/tokio) -- the runtime that defined how async I/O should work. We would not be here without the years of design, iteration, and documentation that the Tokio team invested. Every core pattern in Volt -- the scheduler, the state machine, the sync primitives, cooperative budgeting -- was learned from Tokio.
- [Mio](https://github.com/tokio-rs/mio) -- platform I/O abstraction we study for every backend
- [parking_lot](https://github.com/Amanieu/parking_lot) -- adaptive locking strategies
- [Crossbeam](https://github.com/crossbeam-rs/crossbeam) -- lock-free channel designs
- [Vyukov MPMC](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue) -- bounded ring buffer for channels

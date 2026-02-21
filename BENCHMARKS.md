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

| Benchmark | Volt | Tokio | B/op (Volt) | B/op (Tokio) | Winner |
|-----------|----------|-------|--------------|--------------|--------|
| Mutex | 31.8 ns | 28.2 ns | 0 | 0 | Tokio +1.1x |
| RwLock (read) | 25.3 ns | 27.1 ns | 0 | 0 | Volt +1.1x |
| RwLock (write) | 20.7 ns | 25.2 ns | 0 | 0 | Volt +1.2x |
| Semaphore | 22.7 ns | 33.7 ns | 0 | 0 | Volt +1.5x |

### Synchronization -- Contended

Multiple async tasks competing for the same resource on 4 worker threads.

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

## Where Tokio Wins

Tokio outperforms Volt in four benchmarks, with two being significant:

- **Spawn + await** (by 1.6x). Tokio's `tokio::spawn` has years of optimization for the
  single-task spawn-and-join pattern, including optimized `JoinHandle` internals and a highly
  tuned `RawTask` implementation. Volt's `FutureTask` setup overhead is higher per task.

- **Blocking spawn** (by 2.0x). Tokio's blocking pool has years of tuning for thread
  wake latency and reuse. The gap reflects condvar/futex interaction differences between
  Tokio's `parking_lot` and Zig's `std.Thread.Condition`.

- **Mutex uncontended** (by 1.1x) and **OnceCell get** (by 2.0x). Minor advantages from
  Tokio's `parking_lot`-style adaptive spinning and Rust's extremely optimized `std::sync::Once`
  implementation. The OnceCell hot path is a single relaxed atomic load in both implementations,
  so the 2x gap likely reflects measurement noise at the sub-nanosecond scale.

## What Helps Volt

Most of Volt's advantages come from Zig's language properties and a zero-allocation
architecture rather than algorithmic breakthroughs:

- **Intrusive waiters** -- waiter nodes are embedded directly in futures on the stack,
  eliminating the heap allocations Tokio makes for waiter bookkeeping. This is the single
  largest factor: 284 bytes/op vs 1,868 bytes/op across all benchmarks.

- **Comptime specialization** -- generic lock and channel types are monomorphized at compile
  time with concrete waker types, removing the virtual dispatch Tokio pays through
  `dyn Future` trait objects and `Waker` vtables.

- **Vyukov MPMC ring buffer** -- the bounded channel uses a lock-free ring buffer with
  per-slot sequence counters, power-of-2 bitmask indexing, and interleaved slot layout for
  spatial locality. The channel MPMC benchmark shows Volt at 73ns vs Tokio at 133ns (1.8x).

- **Lock-free semaphore release** -- the `fast_waiter` atomic slot allows `release()` to
  serve the most recent waiter via a single atomic swap, bypassing the mutex entirely. Under
  the contended semaphore benchmark (8 tasks, 2 permits), this delivers 139ns vs Tokio's 323ns
  (2.3x).

- **O(1) bitmap worker waking** -- the scheduler uses `@ctz` on a packed bitmap to find idle
  workers in constant time, where Tokio scans a list.

- **Zero-allocation oneshot and barrier** -- Tokio's oneshot allocates a shared `Arc<Inner>`
  (72 bytes) and its barrier allocates tracking state (1,064 bytes). Volt uses
  stack-embedded atomics for both.

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

---
title: Performance Tuning
description: Optimize Volt for your workload — worker count, LIFO slots, cooperative budgeting, contention reduction, and benchmark-driven workflow.
---

Volt is designed to be fast by default, but real workloads benefit from tuning. This guide covers the knobs available and when to turn them.

## Worker Count

The `num_workers` config controls how many OS threads run the async scheduler. The default (`0`) auto-detects based on CPU count.

```zig
const volt = @import("volt");

// Auto-detect (one worker per CPU core)
try volt.runWith(allocator, .{}, myServer);

// Explicit: 4 workers
try volt.runWith(allocator, .{ .num_workers = 4 }, myServer);

// Manual runtime setup
var io = try volt.Runtime.init(allocator, .{
    .num_workers = 8,
    .max_blocking_threads = 256,
});
defer io.deinit();
```

### Guidelines

| Workload | Recommended Workers |
|----------|-------------------|
| I/O-bound (web server, proxy) | CPU count (default) |
| Mixed I/O + compute | CPU count - 1 (leave room for blocking pool) |
| Compute-heavy with I/O | CPU count / 2 (offload compute to blocking pool) |
| Testing / debugging | 1 (deterministic execution) |

More workers is not always better. Each worker adds memory overhead (local queue, stack, LIFO slot) and increases contention on the global queue.

## LIFO Slot and Cache Locality

Each worker has a **LIFO slot** -- a single-task fast path that bypasses the local queue entirely. When a task wakes another task on the same worker, the woken task goes into the LIFO slot and runs next.

This matters because:

- The woken task likely accesses the same cache lines as the waker (temporal locality).
- Skipping the queue reduces latency for ping-pong patterns (mutex lock/unlock, channel send/recv).
- The scheduler caps LIFO polls at `MAX_LIFO_POLLS_PER_TICK = 3` to prevent starvation of queued tasks.

### When LIFO Helps

- **Mutex contention**: `unlock()` wakes the next waiter. If that waiter runs immediately on the same core, the mutex's memory is still hot in L1 cache.
- **Channel ping-pong**: Producer sends, consumer runs on same worker, consumes, producer runs again.
- **Barrier release**: Leader wakes all participants; the first one runs via LIFO.

### When LIFO Hurts

If many tasks constantly wake each other, LIFO can cause a small set of tasks to monopolize workers while other tasks starve. The `MAX_LIFO_POLLS_PER_TICK` cap exists for this reason.

You cannot disable LIFO via config -- it is always active. If you see starvation in profiling, redesign the wake pattern (e.g., batch work into fewer tasks).

## Cooperative Budgeting

The scheduler enforces a budget of **128 polls per tick** (`BUDGET_PER_TICK`). After 128 task polls, the worker performs maintenance:

1. Check the global queue for new tasks
2. Process I/O completions from the backend
3. Fire expired timers
4. Update the adaptive global queue interval

This prevents a single long-running future chain from starving I/O and timers.

### Implications for Your Code

- Futures that call many sub-futures in a single `poll()` consume budget rapidly.
- If your future does O(1000) work per poll, consider yielding manually.
- `volt.task.yield()` is an engine internal -- it is a no-op hint that the scheduler may use to preempt.

### Adaptive Global Queue Interval

The scheduler uses EWMA (exponentially weighted moving average) to estimate average task poll duration. It adjusts how often it checks the global queue:

- Fast tasks (< 1us each) --> check every ~128 polls
- Slow tasks (> 100us each) --> check every ~8 polls

This self-tuning happens automatically. You do not need to configure it.

## Reducing Contention

Contention is the primary bottleneck in async runtimes. Here is how to minimize it.

### Choose the Right Primitive

| Pattern | Wrong Choice | Right Choice |
|---------|-------------|-------------|
| Shared config | `Mutex` protecting a struct | `Watch` channel |
| Request/response | `Channel` with capacity 1 | `Oneshot` |
| Rate limiting | `Mutex` + counter | `Semaphore` |
| One-time init | `Mutex` + `bool` flag | `OnceCell` |
| Read-heavy data | `Mutex` | `RwLock` |

See the [Choosing a Primitive](/guides/choosing-primitive/) guide for a full decision tree.

### Avoid Shared State When Possible

Channels move data between tasks without sharing. If you can model your problem as message passing instead of shared state, do it.

```zig
// WORSE: Shared state with mutex
var mutex = volt.sync.Mutex.init();
var shared_counter: u64 = 0;

fn incrementCounter() void {
    if (mutex.tryLock()) {
        defer mutex.unlock();
        shared_counter += 1;
    }
}

// BETTER: Channel-based accumulation
var ch = try volt.channel.bounded(u64, allocator, 1024);

fn sendIncrement() void {
    _ = ch.trySend(1);
}

fn accumulator() void {
    var total: u64 = 0;
    while (true) {
        switch (ch.tryRecv()) {
            .value => |v| total += v,
            .empty => return,
            .closed => return,
        }
    }
}
```

### Partition State Across Workers

Instead of one `Mutex`-protected map, use N maps (one per worker) and route by key hash:

```zig
const NUM_SHARDS = 16;
var shards: [NUM_SHARDS]struct {
    mutex: volt.sync.Mutex,
    data: SomeMap,
} = undefined;

fn getShard(key: u64) *@TypeOf(shards[0]) {
    return &shards[key % NUM_SHARDS];
}
```

## Memory Allocation in Hot Paths

Allocation is the hidden enemy of async performance. Each `std.heap.page_allocator.alloc()` is a syscall. In hot paths:

1. **Pre-allocate buffers** before entering the event loop.
2. **Use arena allocators** for request-scoped data.
3. **Pool long-lived objects** (connections, task contexts).
4. **Avoid `ArrayList.append` in poll functions** -- it may reallocate.

```zig
// Pre-allocate a buffer pool before the event loop
var pool: [256][4096]u8 = undefined;
var free_list: std.ArrayList(usize) = .empty;
try free_list.ensureTotalCapacity(allocator, 256);
for (0..256) |i| free_list.appendAssumeCapacity(i);
```

The blocking pool (`io.concurrent`) is the right place for allocation-heavy work. It runs on separate threads that will not starve the async scheduler.

## Blocking Pool Tuning

The blocking pool spawns OS threads on demand for CPU-intensive or blocking I/O work.

```zig
var io = try volt.Runtime.init(allocator, .{
    .max_blocking_threads = 512,          // Max concurrent blocking tasks
    .blocking_keep_alive_ns = 10 * std.time.ns_per_s, // Idle thread timeout
});
```

- **`max_blocking_threads`**: Upper bound on OS threads. Default 512. Lower this if memory is constrained.
- **`blocking_keep_alive_ns`**: How long idle blocking threads survive before exiting. Default 10 seconds. Increase for bursty workloads; decrease to reclaim memory faster.

## Benchmark-Driven Optimization Workflow

Never optimize without measuring. Follow this workflow:

1. **Establish a baseline** using `zig build bench` or a custom benchmark.
2. **Profile** with `perf record` (Linux) or Instruments (macOS).
3. **Identify the bottleneck** -- is it lock contention? Allocation? Syscalls? Cache misses?
4. **Make one change** and re-benchmark.
5. **Run correctness tests** (`zig build test-all`) after every optimization.

### Built-in Benchmarks

```bash
# Full benchmark suite (sync, channel, async, task scheduling)
zig build bench

# Compare against Tokio (Rust) baselines
zig build compare
```

### What to Look For

| Metric | Healthy | Concerning |
|--------|---------|------------|
| Uncontended mutex | < 15ns | > 50ns |
| Channel send/recv roundtrip | < 10ns | > 100ns |
| Semaphore acquire/release | < 15ns | > 50ns |
| Contended mutex (4 threads) | < 200ns | > 1000ns |
| Task spawn + await | < 10,000ns | > 50,000ns |

The benchmarks under `bench/` compare Volt against Tokio. Volt wins 17/21 benchmarks. Tokio leads in contended semaphore (1.2x), MPMC channel (2.2x), and blocking pool spawn (2.2x). Volt's architecture is [derived from Tokio's design](https://github.com/NerdMeNot/volt/blob/main/BENCHMARKS.md#standing-on-tokios-shoulders).

---
title: Work-Stealing Scheduler
description: How Volt distributes tasks across worker threads using a
  work-stealing algorithm derived from Tokio's multi-threaded scheduler.
slug: v1.0.0-zig0.15.2/v110/algorithms/work-stealing
---

Work stealing is the core scheduling algorithm in Volt. It enables efficient distribution of async tasks across multiple worker threads without centralized coordination. When a worker runs out of local work, it steals tasks from other workers rather than sitting idle.

## Why Work Stealing for Async Runtimes

Async I/O runtimes face a fundamental scheduling problem: tasks arrive unpredictably (network packets, timer expirations, user spawns) and have wildly varying execution times. A static partitioning scheme would leave some workers overloaded while others sit idle.

Work stealing solves this by making the **common case fast** (local queue access, no synchronization) while providing a **fallback** for imbalance (steal from busy workers). The asymmetry is key: the owner thread has fast, uncontested access to its own queue, and contention only occurs during steal operations.

```
Worker 0 (busy)         Worker 1 (idle)         Worker 2 (busy)
+---+---+---+---+       +---+---+---+---+       +---+---+---+---+
| A | B | C | D |       |   |   |   |   |       | X | Y |   |   |
+---+---+---+---+       +---+---+---+---+       +---+---+---+---+
        |                       ^
        +--- steal half --------+
                                |
                        +---+---+---+---+
                        | A | B |   |   |   Worker 1 now has work
                        +---+---+---+---+
```

## The Volt Scheduling Loop

Each worker thread runs a loop that searches for work in a strict priority order. The order is designed so that the cheapest sources (no synchronization) are checked first, with progressively more expensive sources tried only when cheaper ones are exhausted.

```
findWork() priority:

1. Pre-fetched Global Batch    No lock needed, already local
       |
       v (empty)
2. Global Queue (periodic)     Every N ticks, batch-pull up to 64 tasks
       |
       v (not this tick / empty)
3. Local Queue                 LIFO slot (cap 3/tick), then FIFO ring buffer
       |
       v (empty)
4. Work Stealing               Random victim, steal half of their queue
       |
       v (nothing found)
5. Global Queue (final)        One last check before sleeping
       |
       v (empty)
6. Park                        Futex sleep with 10ms timeout
```

Step 1 requires no synchronization. Step 2 takes a mutex but amortizes it across up to 64 tasks -- crucially, it runs **before** the local queue to prevent starvation (see below). Step 3 has no synchronization. Step 4 uses lock-free CAS operations. Step 6 uses a futex for efficient OS-level sleep.

### Why global queue checks before local queue

The periodic global queue check (step 2) must run before the local queue check (step 3). Without this ordering, fast-cycling tasks in the LIFO slot (e.g., channel producer wakes consumer, consumer wakes producer) create a perpetual local work supply. The worker never reaches the global queue check, and tasks routed to the global queue (such as budget-yielded tasks or externally spawned tasks) starve indefinitely.

This matches Tokio's design where the injector queue is checked before local work on periodic ticks.

The corresponding implementation in `Scheduler.zig`:

```zig
fn findWork(self: *Self) ?*Header {
    // 1. Check buffered global queue batch first (free, no lock)
    if (self.pollGlobalBatch()) |task| return task;

    // 2. PERIODIC: Check global queue BEFORE local queue.
    //    Prevents starvation when LIFO-cycling tasks keep local queue full.
    if (self.tick % self.global_queue_interval == 0) {
        if (self.fetchGlobalBatch()) |task| return task;
    }

    // 3. Check local queue with LIFO limiting
    if (self.pollLocalQueue()) |task| return task;

    // 4. Try stealing from other workers
    if (self.scheduler.config.enable_stealing) {
        if (self.tryStealWithLimit()) |task| return task;
    }

    // 5. Final check of global queue before parking
    return self.fetchGlobalBatch();
}
```

## Random Victim Selection

When stealing, the worker must choose a victim. Volt uses a randomized approach rather than round-robin to avoid pathological patterns where multiple idle workers all try to steal from the same victim simultaneously.

### xorshift64+ PRNG

Each worker has a per-worker PRNG instance using the xorshift64+ algorithm with shift triplet `[17, 7, 16]`. This is the same PRNG family used by Tokio. It is extremely fast (a few bitwise operations, no divisions) and has a period of 2^64 - 1.

```zig
pub const FastRand = struct {
    one: u32,
    two: u32,

    pub fn fastrand(self: *FastRand) u32 {
        var s1 = self.one;
        const s0 = self.two;
        s1 ^= s1 << 17;
        s1 = s1 ^ s0 ^ (s1 >> 7) ^ (s0 >> 16);
        self.one = s0;
        self.two = s1;
        return s0 +% s1;
    }
};
```

The PRNG is seeded with a combination of worker index and timestamp to ensure each worker produces a different sequence:

```zig
const seed: u64 = @as(u64, @truncate(ts)) +% @as(u64, index) *% 0x9E3779B97F4A7C15;
```

The constant `0x9E3779B97F4A7C15` is the golden ratio scaled to 64 bits, a standard technique for hash mixing.

### Lemire's Bias-Free Modulo

Converting a random `u32` into a uniform value in `[0, n)` is tricky. The naive `rand % n` approach introduces bias when `n` does not evenly divide `2^32`. Volt uses Daniel Lemire's fast, bias-free reduction:

```zig
pub fn fastrand_n(self: *FastRand, n: u32) u32 {
    const mul: u64 = @as(u64, self.fastrand()) *% @as(u64, n);
    return @truncate(mul >> 32);
}
```

This multiplies the random value by `n` as a 64-bit product, then takes the upper 32 bits. The result is uniformly distributed in `[0, n)` without any division instruction. On modern CPUs, a 64-bit multiply is significantly faster than a division.

The steal loop starts at the random victim and wraps around:

```zig
fn trySteal(self: *Self) ?*Header {
    const num_workers = self.scheduler.workers.len;
    const start = self.rand.fastrand_n(@intCast(num_workers));

    for (0..num_workers) |i| {
        const victim_id = (@as(usize, start) + i) % num_workers;
        if (victim_id == self.index) continue;

        if (WorkStealQueue.stealInto(&victim.run_queue, &self.run_queue)) |task| {
            return task;
        }
    }
    return null;
}
```

## Half-Steal Strategy

When a steal succeeds, the stealer takes **half** of the victim's queue (rounded up). This is a deliberate design choice:

* **Why not steal one?** One task may complete quickly, forcing another steal immediately. The overhead of repeated steal attempts would dominate.
* **Why not steal all?** Taking everything would just move the imbalance from one worker to another.
* **Why half?** Half is the optimal split that minimizes the expected number of steal operations needed to balance a workload. After one steal, both workers have roughly equal work.

The steal count is capped at `CAPACITY / 2 = 128` tasks to prevent oversized steals:

```zig
var to_steal = available -% (available / 2);  // ceiling(available / 2)
if (to_steal > CAPACITY / 2) to_steal = CAPACITY / 2;
```

## Adaptive Global Queue Polling

Workers check the global injection queue periodically, not on every tick. The check interval self-tunes based on observed task execution times using an Exponential Weighted Moving Average (EWMA):

```
new_avg = 0.1 * sample_ns + 0.9 * old_avg
interval = clamp(1ms / avg_task_ns, 8, 255)
```

| Task Duration | Computed Interval | Rationale |
|---------------|-------------------|-----------|
| 1us           | 255 (max)         | Fast tasks; local queue drains fast, check rarely |
| 10us          | 100               | Moderate tasks |
| 50us          | 20 (default)      | Default estimate for mixed workloads |
| 100us         | 10                | Slow tasks; check often to prevent starvation |
| 1ms+          | 8 (min)           | Very slow tasks; check as often as possible |

The target latency of 1ms means: "a task sitting in the global queue should wait at most ~1ms before a worker picks it up." The EWMA smoothing factor of 0.1 means the interval adapts gradually, avoiding oscillation from short-term spikes.

```zig
fn updateAdaptiveInterval(self: *Self, tasks_run: u32) void {
    if (tasks_run == 0) return;
    const batch_ns: u64 = @intCast(@max(0, end_ns - self.batch_start_ns));
    const current_avg = batch_ns / tasks_run;

    const new_avg: u64 = @intFromFloat(
        (@as(f64, @floatFromInt(current_avg)) * EWMA_ALPHA) +
        (@as(f64, @floatFromInt(self.avg_task_ns)) * (1.0 - EWMA_ALPHA)),
    );
    self.avg_task_ns = @max(1, new_avg);

    const tasks_per_target = TARGET_GLOBAL_QUEUE_LATENCY_NS / self.avg_task_ns;
    self.global_queue_interval = @intCast(
        @min(MAX_GLOBAL_QUEUE_INTERVAL, @max(MIN_GLOBAL_QUEUE_INTERVAL, tasks_per_target))
    );
}
```

## The "Last Searcher Must Notify" Protocol

The most subtle part of the scheduler is ensuring no tasks are stranded -- sitting in queues while all workers sleep. Volt implements Tokio's "last searcher must notify" chain-reaction protocol.

### The Problem

Without coordination, a lost wakeup can occur:

1. Worker A enqueues a task and checks: "is anyone searching?" Yes, Worker B is. So A does not wake anyone.
2. Worker B finishes searching and finds nothing. It parks.
3. The task sits in the queue. No one is looking for it.

### The Solution

Workers track a global `num_searching` counter. The protocol has three rules:

**Rule 1:** When a task is enqueued and `num_searching == 0`, wake one parked worker. That worker enters "searching" state (`num_searching += 1`).

**Rule 2:** When a searching worker finds work, it calls `transitionFromSearching()`. If it was the **last** searcher (`num_searching` drops to 0), it wakes another parked worker to continue searching.

**Rule 3:** When a searching worker parks without finding work, if it was the last searcher, it does a final check of all queues before sleeping.

This creates a chain reaction: each worker that finds work ensures the next searcher is awake, guaranteeing all queued tasks eventually get processed.

```
Task enqueued      Worker 0 wakes     Worker 0 finds work    Worker 1 wakes
num_searching=0 -> num_searching=1 -> num_searching=0     -> num_searching=1
                   (Rule 1)           (Rule 2: last!)        (chain continues)
```

## Comparison with Tokio

| Aspect | Volt | Tokio |
|--------|----------|-------|
| Worker wake lookup | O(1) via `@ctz` on 64-bit bitmap | Linear scan of worker array |
| LIFO slot | Yes, with eviction to ring buffer | Yes, with eviction |
| Local queue | 256-slot Chase-Lev deque | 256-slot Chase-Lev deque |
| Global batch pull | Adaptive size (4-64), 32-task buffer | Batch fetch |
| PRNG | xorshift64+ with Lemire modulo | Same |
| Cooperative budget | 128 ops/task + 128 polls/tick | 128 ops/task + budget/tick |
| Searcher limiting | ~50% cap via atomic counter | ~50% cap |
| Park mechanism | Futex with 10ms timeout | Condvar-based |
| Adaptive interval | EWMA-based (0.1 alpha) | Fixed interval (61) |

The most significant difference is worker wake lookup. Tokio uses a linear scan to find an idle worker, which is O(N) in the number of workers. Volt uses a 64-bit bitmap where each bit represents one worker; finding the lowest-indexed parked worker is a single `@ctz` (count trailing zeros) instruction -- O(1) regardless of worker count.

### Source Files

* Scheduler loop and worker: `src/internal/scheduler/Scheduler.zig`
* WorkStealQueue and task state: `src/internal/scheduler/Header.zig`

## References

* Robert D. Blumofe and Charles E. Leiserson, "Scheduling Multithreaded Computations by Work Stealing," *Journal of the ACM*, 46(5):720–748, 1999. [doi:10.1145/324133.324234](https://doi.org/10.1145/324133.324234)
* David Chase and Yossi Lev, "Dynamic Circular Work-Stealing Deque," *SPAA '05*, 2005. [doi:10.1145/1073970.1073974](https://doi.org/10.1145/1073970.1073974)
* Tokio scheduler source: [`tokio/src/runtime/scheduler/multi_thread`](https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime/scheduler/multi_thread)
* Daniel Lemire, "Fast Random Integer Generation in an Interval," *ACM Transactions on Modeling and Computer Simulation*, 29(1), 2019. [doi:10.1145/3230636](https://doi.org/10.1145/3230636)

---
title: Adaptive Scheduling
description: How Volt self-tunes its scheduling parameters at runtime, including
  cooperative budgeting, LIFO limiting, adaptive global queue intervals, and
  worker parking with bitmap-based idle management.
slug: v1.0.0-zig0.15.2/algorithms/adaptive-scheduling
---

Volt's scheduler does not use fixed parameters for all workloads. It adapts several key behaviors at runtime based on observed task characteristics and system load. This page documents the adaptive mechanisms and the invariants they maintain.

## Cooperative Budgeting

An async runtime must prevent any single task from monopolizing a worker thread. Volt has two budget layers:

### Per-task budget (Context.pollProceed)

Each task gets a budget of 128 operations per poll. Futures that loop internally (lock → unlock → lock, or channel send → recv in a tight loop) call `ctx.pollProceed()` before each iteration. When the budget hits zero, the future returns `.pending` and the task is routed to the **global queue** via a `.yield` poll result -- not the LIFO slot. This prevents a single task from hogging a worker indefinitely.

```zig
// Every sync primitive and channel future calls this in .init state:
if (!ctx.pollProceed()) return .pending;
```

The `.yield` path is distinct from `.pending`: yielded tasks bypass the LIFO slot and go directly to the global injection queue, ensuring other tasks get a chance to run before the yielded task is re-polled.

### Per-worker budget (tick-level)

Each worker is allowed 128 task polls per **tick** before yielding for maintenance.

```
BUDGET_PER_TICK = 128

Worker tick cycle:
  1. Set budget = 128
  2. Record batch_start_ns
  3. Poll I/O completions (worker 0 also polls timers)
  4. Loop: findWork() -> executeTask() -> budget -= 1
     ... until budget == 0 or no work found
  5. Update adaptive interval (EWMA)
  6. Reset budget, LIFO counter
  7. If no tasks were run: park
```

The budget of 128 matches Tokio's `BUDGET_PER_TICK`. This value was chosen empirically as a balance between throughput (larger budget = fewer maintenance interruptions) and latency (smaller budget = more frequent timer and I/O checks).

```zig
// In Worker.run():
while (!self.scheduler.shutdown.load(.acquire)) {
    self.tick +%= 1;
    self.batch_start_ns = std.time.nanoTimestamp();

    if (self.index == 0) {
        _ = self.scheduler.pollTimers();
    }
    _ = self.scheduler.pollIo();

    var tasks_run: u32 = 0;
    while (self.budget > 0) {
        if (self.findWork()) |task| {
            self.executeTask(task);
            self.budget -|= 1;
            tasks_run += 1;
        } else {
            break;
        }
    }

    self.updateAdaptiveInterval(tasks_run);
    self.budget = BUDGET_PER_TICK;
    self.lifo_polls_this_tick = 0;
    // ...
}
```

## LIFO Slot Limiting

Each worker has a LIFO slot -- a single atomic pointer that holds the most recently scheduled task. Executing the newest task first exploits temporal cache locality: the task was just woken, so its data is likely still in L1/L2 cache.

However, uncapped LIFO execution creates a starvation risk:

```
Pathological LIFO pattern:
  Task A runs -> wakes Task B (goes to LIFO slot)
  Task B runs -> wakes Task A (goes to LIFO slot)
  Task A runs -> wakes Task B (goes to LIFO slot)
  ...forever. Tasks in the ring buffer NEVER execute.
```

Volt caps LIFO polls at `MAX_LIFO_POLLS_PER_TICK = 3` per tick. After 3 consecutive LIFO executions, the LIFO slot is flushed to the ring buffer, making its task visible to the normal FIFO order and to work stealers:

```zig
fn pollLocalQueue(self: *Self) ?*Header {
    if (self.lifo_polls_this_tick < MAX_LIFO_POLLS_PER_TICK) {
        if (self.lifo_slot.swap(null, .acquire)) |task| {
            self.lifo_polls_this_tick +|= 1;
            return task;
        }
    } else {
        // LIFO cap reached -- flush to ring buffer
        if (self.lifo_slot.swap(null, .acquire)) |task| {
            self.run_queue.push(task, &self.scheduler.global_queue);
        }
    }

    return self.run_queue.pop();
}
```

The flush is critical. Without it, the worker would skip the LIFO slot (already at cap) and find the ring buffer empty, concluding there is no work. With the flush, the LIFO task becomes a ring buffer task and is found on the very next `run_queue.pop()`.

## Adaptive Global Queue Interval

Workers check the global injection queue periodically, not on every task. The check frequency self-tunes using an Exponential Weighted Moving Average (EWMA) of observed task poll times.

### The Tuning Formula

```
avg_task_ns = 0.1 * current_sample + 0.9 * previous_avg
interval = clamp(1ms / avg_task_ns, 8, 255)
```

The idea: if tasks are fast (microseconds), the worker completes many tasks per millisecond, so the global queue can wait longer between checks. If tasks are slow (hundreds of microseconds), fewer tasks per millisecond means the global queue should be checked more often to prevent starvation.

```zig
fn updateAdaptiveInterval(self: *Self, tasks_run: u32) void {
    if (tasks_run == 0) return;

    const end_ns = std.time.nanoTimestamp();
    const batch_ns: u64 = @intCast(@max(0, end_ns - self.batch_start_ns));
    const current_avg = batch_ns / tasks_run;

    // EWMA: new_avg = (current * 0.1) + (old * 0.9)
    const new_avg: u64 = @intFromFloat(
        (@as(f64, @floatFromInt(current_avg)) * EWMA_ALPHA) +
        (@as(f64, @floatFromInt(self.avg_task_ns)) * (1.0 - EWMA_ALPHA)),
    );
    self.avg_task_ns = @max(1, new_avg);

    // interval = TARGET_LATENCY / avg_task_time
    const tasks_per_target = TARGET_GLOBAL_QUEUE_LATENCY_NS / self.avg_task_ns;
    const clamped = @min(MAX_GLOBAL_QUEUE_INTERVAL, @max(MIN_GLOBAL_QUEUE_INTERVAL, tasks_per_target));
    self.global_queue_interval = @intCast(clamped);
}
```

### Parameter Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `TARGET_GLOBAL_QUEUE_LATENCY_NS` | 1ms (1,000,000ns) | Target max latency for global queue starvation |
| `EWMA_ALPHA` | 0.1 | Smoothing factor (10% weight to new sample) |
| `MIN_GLOBAL_QUEUE_INTERVAL` | 8 | Minimum ticks between checks |
| `MAX_GLOBAL_QUEUE_INTERVAL` | 255 | Maximum ticks between checks |
| `DEFAULT_AVG_TASK_NS` | 50us (50,000ns) | Initial estimate for task duration |

The EWMA smoothing factor of 0.1 means the interval changes gradually. A single outlier task (e.g., a CPU-intensive computation) will not cause the interval to spike. It takes roughly 10-20 samples for the average to converge to a new steady state.

The default initial estimate of 50us yields an initial interval of `1ms / 50us = 20`, meaning the global queue is checked every 20th tick at startup.

## Worker Parking and Searching States

When a worker exhausts all work sources, it parks (sleeps) using a futex. The parking protocol is carefully designed to avoid lost wakeups.

### The Parking Sequence

```
Worker parking protocol:

1. Check if we have local tasks -> if yes, don't park
2. Set transitioning_to_park = true
3. Flush LIFO slot to ring buffer (make tasks stealable)
4. Double-check: ring buffer empty AND global queue empty?
   -> If not, cancel park, clear flag, resume
5. Transition to parked in IdleState
   -> If we were the last searcher, do final queue check
6. Futex sleep with timeout
7. On wake: clear transitioning_to_park
8. Determine wake reason:
   - Bitmap bit cleared -> claimed by notification (become searcher)
   - Bitmap bit still set -> timeout wake (not a searcher)
```

The `transitioning_to_park` flag is critical for lost wakeup prevention. Without it:

```
Race condition (without flag):
  Worker A:                          Waker thread:
  1. LIFO slot empty
  2. Run queue empty
  3.                                 4. Push task to Worker A's LIFO slot
  5. Futex sleep                        (Worker A is about to sleep!)
  Result: Task stuck in LIFO slot, Worker A sleeping
```

With the flag:

```
Safe sequence (with flag):
  Worker A:                          Waker thread:
  1. Set transitioning_to_park=true
  2. Flush LIFO (empty)
  3. Check run_queue (empty)         4. tryScheduleLocal() checks flag
  5. Park                               -> flag is true, returns false
                                     5. Push to global queue instead
                                     6. Call wakeWorkerIfNeeded()
  Result: Task in global queue, another worker woken or A wakes on timeout
```

```zig
pub fn tryScheduleLocal(self: *Self, task: *Header) bool {
    // CRITICAL: Check transitioning flag to prevent lost wakeup
    if (self.transitioning_to_park.load(.acquire)) {
        return false;  // Caller should use global queue
    }
    // ... push to LIFO slot
}
```

## Idle State Management with Bitmap

The `IdleState` struct coordinates worker parking and waking:

```zig
pub const IdleState = struct {
    num_searching: std.atomic.Value(u32),   // Workers actively looking for work
    num_parked: std.atomic.Value(u32),      // Workers sleeping
    parked_bitmap: std.atomic.Value(u64),   // Bit per worker: 1 = parked
    num_workers: u32,
};
```

### The Bitmap

The 64-bit `parked_bitmap` has one bit per worker (supporting up to 64 workers). Bit `i` is set when worker `i` is parked.

**Waking a worker** uses `@ctz` (count trailing zeros) to find the lowest-indexed parked worker in O(1):

```zig
pub fn claimParkedWorker(self: *IdleState) ?usize {
    while (true) {
        const bitmap = self.parked_bitmap.load(.seq_cst);
        if (bitmap == 0) return null;

        const worker_id: usize = @ctz(bitmap);        // O(1) lookup
        const mask: u64 = @as(u64, 1) << @intCast(worker_id);

        const prev = self.parked_bitmap.fetchAnd(~mask, .seq_cst);
        if (prev & mask == 0) continue;  // Lost race, retry

        _ = self.num_parked.fetchSub(1, .seq_cst);
        _ = self.num_searching.fetchAdd(1, .seq_cst);
        return worker_id;
    }
}
```

This is a significant improvement over Tokio, which uses a linear scan to find an idle worker. With 64 workers, Tokio scans up to 64 entries; Volt executes a single `@ctz` instruction.

### Searcher Limiting

At most ~50% of workers can be in the "searching" state at any time:

```zig
pub fn tryBecomeSearcher(self: *IdleState, num_workers: u32) bool {
    const state = self.num_searching.load(.seq_cst);
    if (2 * state >= num_workers) return false;  // Too many searchers
    _ = self.num_searching.fetchAdd(1, .seq_cst);
    return true;
}
```

This prevents thundering herd behavior. When work suddenly appears after an idle period:

```
Without searcher limiting (8 workers, all parked):
  Task enqueued -> wake worker 0 -> wake worker 1 -> ... -> wake ALL
  All 8 workers compete to steal from each other = wasted work

With searcher limiting (8 workers, all parked):
  Task enqueued -> wake worker 0 (searching)
  Worker 0 finds task -> transitions from searching -> wakes worker 1
  Worker 1 finds task -> transitions from searching -> wakes worker 2
  ...chain continues only as long as there is work to find
```

The 50% cap is a heuristic, not a hard limit. It is checked non-atomically (`load` then `fetchAdd`), so it may briefly exceed 50%. This is fine -- it is an optimization, not a correctness requirement.

### Wake Reason Detection

After a parked worker wakes from the futex, it needs to know **why** it woke:

* **Claimed by notification**: Another thread called `claimParkedWorker()`, which cleared this worker's bitmap bit and incremented `num_searching`. The worker should enter searching state.
* **Timeout or spurious**: The futex timed out (10ms default). The bitmap bit is still set. The worker removes itself from the parked set and does not enter searching state.

```zig
if (!self.scheduler.idle_state.isWorkerParked(self.index)) {
    // Bitmap bit cleared -> claimed by notification
    self.is_searching = true;
} else {
    // Bitmap bit still set -> timeout wake
    const mask: u64 = @as(u64, 1) << @intCast(self.index);
    _ = self.scheduler.idle_state.parked_bitmap.fetchAnd(~mask, .seq_cst);
    _ = self.scheduler.idle_state.num_parked.fetchSub(1, .seq_cst);
    self.is_searching = false;
}
```

## Summary of Adaptive Parameters

| Parameter | Scope | Adaptation Method | Range |
|-----------|-------|-------------------|-------|
| `global_queue_interval` | Per-worker | EWMA of task poll time | 8-255 ticks |
| `lifo_polls_this_tick` | Per-worker, per-tick | Counter, reset each tick | 0-3 |
| `budget` | Per-worker, per-tick | Countdown, reset each tick | 0-128 |
| `is_searching` | Per-worker | Protocol-driven (notify/park) | bool |
| `num_searching` | Global | Atomic counter, 50% cap | 0-N/2 |
| `parked_bitmap` | Global | Atomic bitmap | 64 bits |
| `park_timeout` | Per-worker | Worker 0 uses next timer; others use 10ms | Variable |

These parameters work together to create a scheduler that is responsive under light load (workers wake quickly via bitmap, short park timeouts), efficient under heavy load (LIFO for locality, budget prevents starvation), and self-tuning (adaptive interval, EWMA smoothing).

### Source Files

* Worker and adaptive logic: `src/internal/scheduler/Scheduler.zig`
* Task state machine: `src/internal/scheduler/Header.zig`

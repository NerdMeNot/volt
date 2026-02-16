---
title: Architecture Overview
description: High-level architecture of the Volt runtime -- how the scheduler, I/O driver, timer wheel, and blocking pool fit together.
---

Volt is a production-grade async I/O runtime for Zig, modeled after Tokio's battle-tested architecture. It uses stackless futures (~256--512 bytes per task) instead of stackful coroutines (16--64KB per task), enabling millions of concurrent tasks on commodity hardware.

## Layered architecture

```
+=====================================================================+
|                        User Application                             |
|        volt.run(myServer)  /  io.@"async"(...)  /  io.concurrent() |
+=====================================================================+
                                |
                                v
+=====================================================================+
|                      User-Facing API                                |
|                                                                     |
|  volt.Io       volt.Future   volt.Group      volt.sync.*            |
|  volt.channel.*  volt.net.*  volt.time.*     volt.fs.*              |
|  volt.signal   volt.process  volt.stream     volt.shutdown          |
+=====================================================================+
                                |
                                v
+=====================================================================+
|                   Engine Internals (not user-facing)                 |
|                                                                     |
|  volt.task.*    volt.future.*    volt.async_ops.*                   |
|  (Timeout, Select, Join, Race, FnFuture, FutureTask)                |
+=====================================================================+
                                |
                                v
+=====================================================================+
|                         Runtime Layer                                |
|                                                                     |
|  +------------------+  +---------------+  +-------------------+     |
|  | Work-Stealing    |  | I/O Driver    |  | Timer Wheel       |     |
|  | Scheduler        |  |               |  |                   |     |
|  |                  |  | Platform      |  | 5 levels, 64      |     |
|  | N workers with   |  | backend       |  | slots/level       |     |
|  | 256-slot ring    |  | abstraction   |  | O(1) insert/poll  |     |
|  | buffers, LIFO    |  |               |  |                   |     |
|  | slot, global     |  +---------------+  +-------------------+     |
|  | injection queue  |                                               |
|  +------------------+  +---------------------------------------+    |
|                        | Blocking Pool                         |    |
|                        | On-demand threads (up to 512)         |    |
|                        | 10s idle timeout, auto-shrink         |    |
|                        +---------------------------------------+    |
+=====================================================================+
                                |
                                v
+=====================================================================+
|                       Platform Backends                             |
|                                                                     |
|  +----------+  +---------+  +--------+  +----------+               |
|  | io_uring |  | kqueue  |  |  IOCP  |  |  epoll   |               |
|  | Linux    |  | macOS   |  | Windows|  | Linux    |               |
|  | 5.1+     |  | 10.12+  |  | 10+    |  | fallback |               |
|  +----------+  +---------+  +--------+  +----------+               |
+=====================================================================+
```

## Key components

### Runtime (`src/runtime.zig`)

The `Runtime` struct is the top-level entry point. It owns and coordinates all other components:

```zig
pub const Runtime = struct {
    allocator: Allocator,
    sched_runtime: *SchedulerRuntime,  // Owns the scheduler
    blocking_pool: BlockingPool,        // Dedicated blocking threads
    shutdown: std.atomic.Value(bool),
};
```

Users interact with the runtime through `init`, `@"async"`, and `deinit`:

```zig
var io = try Io.init(allocator, .{
    .num_workers = 0,              // Auto-detect (CPU count)
    .max_blocking_threads = 512,
    .backend = null,               // Auto-detect I/O backend
});
defer io.deinit();

var f = try io.@"async"(compute, .{42});
const result = f.@"await"(io);
```

### Scheduler (`src/internal/scheduler/Scheduler.zig`)

The heart of the runtime. A multi-threaded, work-stealing task scheduler derived from Tokio's multi-threaded scheduler. Each worker thread owns a local task queue and participates in cooperative work stealing when idle. See the [Scheduler](/internals/scheduler/) page for full details.

Worker threads can also execute tasks while waiting for a join to complete (work-stealing join pattern). When `JoinHandle.join()` is called from a worker, `helpUntilComplete()` pops tasks from the worker's queues -- including the LIFO slot where the target task likely sits -- instead of spinning. Non-worker threads use `blockOnComplete()`, which pulls from the global queue and executes tasks inline.

### I/O driver (`src/internal/backend.zig`)

Abstracts platform-specific async I/O behind a unified `Backend` interface. The scheduler owns the I/O backend and polls for completions on each tick. See the [I/O Driver](/internals/io-driver/) page.

### Timer wheel (`src/internal/scheduler/TimerWheel.zig`)

A hierarchical timer wheel with 5 levels and 64 slots per level, providing O(1) insertion and O(1) next-expiry lookup using bit-manipulation. Covers durations from 1ms to ~10.7 days, with an overflow list for longer timers.

```
Level 0:  64 slots x   1ms =    64ms range
Level 1:  64 slots x  64ms =   4.1s range
Level 2:  64 slots x   4s  =   4.3m range
Level 3:  64 slots x   4m  =   4.5h range
Level 4:  64 slots x   4h  =  10.7d range
Overflow: linked list for timers beyond ~10 days
```

Worker 0 is the primary timer driver. It polls the timer wheel at the start of each tick. Other workers contribute to I/O polling but not timer polling, avoiding contention on the timer mutex.

### Blocking pool (`src/internal/blocking.zig`)

A separate thread pool for CPU-intensive or synchronous blocking work. Threads are spawned on demand and exit after 10 seconds of idleness. See the [Blocking Pool](/internals/blocking-pool/) page.

## Thread model

Volt uses three categories of threads:

```
+--------------------------------------------+
|  Worker Threads (N, default = CPU count)   |
|  - Run the scheduler loop                  |
|  - Poll futures (async tasks)              |
|  - Poll I/O completions                    |
|  - Poll timers (worker 0 only)             |
+--------------------------------------------+

+--------------------------------------------+
|  Blocking Pool Threads (0 to 512)          |
|  - Spawned on demand                       |
|  - CPU-intensive or blocking I/O           |
|  - 10s idle timeout, auto-shrink           |
+--------------------------------------------+
```

Worker threads are created during `Io.init()` and run until `deinit()`. Blocking pool threads are created dynamically when `concurrent()` is called and no idle thread is available.

### Thread-local context

Each worker thread has thread-local storage for runtime context:

```zig
threadlocal var current_worker_idx: ?usize = null;
threadlocal var current_scheduler: ?*Scheduler = null;
threadlocal var current_header: ?*Header = null;
```

- `current_worker_idx` -- Used by schedule callbacks to push to the local LIFO slot instead of the global queue.
- `current_scheduler` -- Used by `io.@"async"()` and sleep/timeout to access the scheduler from within a task.
- `current_header` -- Used by async primitives (sleep, mutex.lock) to find the currently executing task and register wakers.

## How components interact

### Task spawning flow

Internally, `io.@"async"(fn, args)` wraps the function in a `FnFuture` and
calls into the engine's `Runtime.spawn`:

```
Runtime.spawn(F, future)
    |
    +--> FutureTask(F).create(allocator, future)
    |        |
    |        +--> Sets up vtable, result storage, scheduler callbacks
    |
    +--> Scheduler.spawn(&task.header)
             |
             +--> transitionToScheduled() (IDLE -> SCHEDULED)
             +--> task.ref() (scheduler holds a reference)
             +--> global_queue.push(task)
             +--> wakeWorkerIfNeeded()

    When spawning from a worker thread, Runtime.spawn() detects
    the worker context and calls spawnFromWorker() instead:

    +--> spawnFromWorker(worker_idx, &task.header)
             |
             +--> transitionToScheduled()
             +--> task.ref()
             +--> worker.tryScheduleLocal(task)  // LIFO slot
                  (falls back to global queue if worker is parking)
```

The `spawnFromWorker` path is critical for the spawn+await fast path: the task lands in the LIFO slot, so the same worker executes it immediately without a round-trip through the global queue.

### Task execution flow

```
Worker.executeTask(task)
    |
    +--> transitionFromSearching()    // Chain notification protocol
    |
    +--> task.transitionToRunning()   // SCHEDULED -> RUNNING
    |
    +--> task.poll()                  // Calls FutureTask.pollImpl()
    |        |
    |        +--> future.poll(&ctx)   // User's Future.poll()
    |
    +--> (result = .complete)
    |        |
    |        +--> task.transitionToComplete()
    |        +--> task.unref() -> task.drop() if last ref
    |
    +--> (result = .pending)
             |
             +--> prev = task.transitionToIdle()  // RUNNING -> IDLE
             |        (atomically clears notified, returns prev state)
             |
             +--> if prev.notified:
                      task.transitionToScheduled()
                      worker.tryScheduleLocal(task) or global_queue.push(task)
```

### I/O completion flow

```
Worker tick
    |
    +--> scheduler.pollIo()
    |        |
    |        +--> io_mutex.tryLock()  // Non-blocking, only one worker polls
    |        +--> backend.wait(completions, timeout=0)
    |        +--> for each completion:
    |                 task = @ptrFromInt(completion.user_data)
    |                 task.transitionToScheduled()
    |                 global_queue.push(task)
    |                 wakeWorkerIfNeeded()
    |
    +--> scheduler.pollTimers()  // Worker 0 only
             |
             +--> timer_mutex.lock()
             +--> timer_wheel.poll()
             +--> wake expired tasks
```

### Shutdown flow

```
Runtime.deinit()
    |
    +--> blocking_pool.deinit()      // Drain queue, join all threads
    |
    +--> scheduler.shutdown.store(true)
    |
    +--> Wake all workers (futex)
    |
    +--> Each worker:
    |        +--> Drain LIFO slot (cancel + drop)
    |        +--> Drain run queue (cancel + drop)
    |        +--> Drain batch buffer (cancel + drop)
    |        +--> Thread exits
    |
    +--> Join all worker threads
    |
    +--> timer_wheel.deinit() (free heap-allocated entries)
    +--> backend.deinit()
    +--> Free workers array, completions buffer
```

## Waker Lifecycle: End-to-End

The following diagram shows how a task moves through the system when it suspends on an I/O operation and is later woken:

```
User Task              Scheduler              I/O Driver
   |                      |                       |
   | future.poll()        |                       |
   |   -> .pending        |                       |
   |                      |                       |
   | store waker in       |                       |
   | ScheduledIo          |                       |
   +--------------------->|                       |
   |                      |                       |
   | (task suspended,     |                       |
   |  worker runs other   |                       |
   |  tasks)              |                       |
   |                      |    [I/O event]        |
   |                      |<----------------------+
   |                      |                       |
   |                      | waker.wake()          |
   |                      | -> transitionToScheduled()
   |                      | -> push to run queue  |
   |                      |                       |
   | future.poll()        |                       |
   |<---------------------+                       |
   |   -> .ready(value)   |                       |
   |                      |                       |
   | result returned      |                       |
   | to caller            |                       |
```

The same flow applies to all waker-based operations: mutex unlock wakes a lock waiter, channel send wakes a recv waiter, timer expiry wakes a sleep future.

---

## Comparison with Tokio

Volt closely follows Tokio's architecture with Zig-specific adaptations:

| Component | Tokio | Volt |
|-----------|-------|----------|
| Task state | `AtomicUsize` with bit packing | `packed struct(u64)` with CAS |
| Work-steal queue | Chase-Lev deque (256 slots) | Chase-Lev deque (256 slots) |
| LIFO slot | Separate atomic pointer | Separate atomic pointer |
| Global queue | `Mutex<VecDeque>` | `Mutex` + intrusive linked list |
| Timer wheel | Hierarchical (6 levels) | Hierarchical (5 levels) |
| Worker parking | `AtomicUsize` packed state | Bitmap + futex |
| Idle coordination | `num_searching` counter | `num_searching` + parked bitmap |
| Worker wakeup | Linear scan for idle worker | O(1) `@ctz` on bitmap |
| I/O driver | mio (epoll/kqueue/IOCP) | Direct platform backends |
| Waker | `RawWaker` + vtable | `RawWaker` + vtable (same pattern) |
| Blocking pool | `spawn_blocking()` | `concurrent()` (Volt user API) |
| Cooperative budget | 128 polls per tick | 128 polls per tick |

## Key constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `BUDGET_PER_TICK` | 128 | Max polls per tick per worker |
| `MAX_LIFO_POLLS_PER_TICK` | 3 | LIFO cap to prevent starvation |
| `MAX_GLOBAL_QUEUE_BATCH_SIZE` | 64 | Max tasks per global batch pull |
| `MAX_WORKERS` | 64 | Max workers (bitmap width) |
| `PARK_TIMEOUT_NS` | 10ms | Default park timeout |
| `WorkStealQueue.CAPACITY` | 256 | Ring buffer slots per worker |
| `NUM_LEVELS` | 5 | Timer wheel levels |
| `SLOTS_PER_LEVEL` | 64 | Slots per timer wheel level |
| `LEVEL_0_SLOT_NS` | 1ms | Level 0 timer resolution |

## Zig's std.Io and Volt's Future

Zig's [`std.Io`](https://github.com/ziglang/zig/tree/master/lib/std/Io) (in development, expected in 0.16) is a standard library I/O interface — an explicit handle passed like `Allocator`, with `async()`, `concurrent()`, and first-class cancellation via method calls (no async/await keywords). It ships with a `Threaded` backend (OS thread pool) plus proof-of-concept `IoUring` and `Kqueue` backends using stackful coroutines.

`std.Io` provides I/O primitives (Mutex, Condition, Queue), but not a work-stealing scheduler, cooperative budgeting, or the rich sync/channel primitives Volt offers. Volt complements `std.Io` the same way Tokio complements Rust's `std::future`:

- **Scheduling**: Volt's work-stealing scheduler with LIFO slot, Chase-Lev deque, and cooperative budgeting (128 polls/tick) — `std.Io.Threaded` uses a simple thread pool
- **Sync primitives**: RwLock, Semaphore, Barrier, Notify, OnceCell with zero-allocation waiters — `std.Io` has Mutex and Condition
- **Channels**: Vyukov MPMC Channel, Oneshot, Broadcast, Watch, Select — `std.Io` has Queue
- **Structured concurrency**: `joinAll`, `race`, `select` combinators

The goal is to complement the standard library, not compete with it. As `std.Io` stabilizes, Volt may adopt it as a backend while preserving the higher-level abstractions.

## Source files

| File | Purpose |
|------|---------|
| `src/runtime.zig` | Top-level Runtime struct |
| `src/internal/scheduler/Scheduler.zig` | Work-stealing scheduler |
| `src/internal/scheduler/Header.zig` | Task state machine + WorkStealQueue |
| `src/internal/scheduler/TimerWheel.zig` | Hierarchical timer wheel |
| `src/internal/scheduler/Runtime.zig` | Scheduler runtime wrapper |
| `src/internal/blocking.zig` | Blocking thread pool |
| `src/internal/backend.zig` | Platform I/O backend interface |
| `src/internal/backend/kqueue.zig` | macOS kqueue backend |
| `src/internal/backend/io_uring.zig` | Linux io_uring backend |
| `src/internal/backend/epoll.zig` | Linux epoll backend |
| `src/internal/backend/iocp.zig` | Windows IOCP backend |

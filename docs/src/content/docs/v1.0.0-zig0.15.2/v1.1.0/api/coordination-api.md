---
title: Coordination API
description: API reference for Notify, Barrier, and OnceCell -- task
  coordination primitives in Volt.
slug: v1.0.0-zig0.15.2/v110/api/coordination-api
---

Coordination primitives handle task signaling and synchronization without protecting shared data. For mutual exclusion (Mutex, RwLock) and concurrency limiting (Semaphore), see the [Sync API](/v1.0.0-zig0.15.2/api/sync-api/).

All primitives are **async-aware** (yield to the scheduler when waiting) and **zero-allocation** (waiter structs embedded in futures). No `deinit()` needed.

## Notify

Task notification. Wake one or all waiting tasks without passing data.

```zig
const Notify = volt.sync.Notify;
```

### Construction

```zig
var notify = Notify.init();
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `notifyOne` | `fn notifyOne(self: *Notify) void` | Wake one waiting task (FIFO). |
| `notifyAll` | `fn notifyAll(self: *Notify) void` | Wake all waiting tasks. |
| `wait` | `fn wait(self: *Notify, io: volt.Io) void` | Convenience: suspend until notified. Takes the `Io` handle. |
| `waitFuture` | `fn waitFuture(self: *Notify) NotifyFuture` | Returns a Future that resolves when notified. |
| `waitWith` | `fn waitWith(self: *Notify, waiter: *Waiter) void` | Add a waiter. |
| `cancelWait` | `fn cancelWait(self: *Notify, waiter: *Waiter) void` | Remove a waiter. |

### Convenience: `wait(io)`

The simplest way to wait for a notification. Suspends the current task until `notifyOne()` or `notifyAll()` is called.

```zig
fn consumer(io: volt.Io, notify: *volt.sync.Notify) void {
    notify.wait(io);  // suspends until notified
    // continue processing
}
```

### Waiter

```zig
const Waiter = volt.sync.notify.Waiter;

var waiter = Waiter.init();
waiter.setWaker(@ptrCast(&ctx), wakeFn);
notify.waitWith(&waiter);
// After wake: waiter.isNotified() == true
```

Methods on Waiter: `init()`, `initWithWaker(ctx, fn)`, `setWaker(ctx, fn)`, `isReady()`, `isNotified()`, `reset()`.

### Example: Producer Wakes Consumer

A common pattern where one task produces data and another task waits to consume it. The producer writes data into a shared location and then calls `notifyOne()` to wake the consumer.

Without Notify, the consumer would need to spin-poll the shared data, wasting CPU. With Notify, the consumer sleeps efficiently and is woken precisely when data is available.

```zig
const std = @import("std");
const volt = @import("volt");

const WorkQueue = struct {
    notify: volt.sync.Notify,
    mutex: volt.sync.Mutex,
    head: ?*WorkItem,

    const WorkItem = struct {
        payload: []const u8,
        next: ?*WorkItem,
    };

    fn init() WorkQueue {
        return .{
            .notify = volt.sync.Notify.init(),
            .mutex = volt.sync.Mutex.init(),
            .head = null,
        };
    }

    /// Producer: push work and wake one waiting consumer.
    fn push(self: *WorkQueue, io: volt.Io, item: *WorkItem) void {
        // Acquire mutex to modify the linked list.
        self.mutex.lock(io);
        defer self.mutex.unlock();
        item.next = self.head;
        self.head = item;

        // Wake exactly one consumer. If no consumer is waiting,
        // the notification is stored as a permit -- the next call
        // to wait() will return immediately instead of blocking.
        self.notify.notifyOne();
    }

    /// Consumer: wait for work to arrive, then pop it.
    fn pop(self: *WorkQueue, io: volt.Io) ?*WorkItem {
        self.notify.wait(io);  // suspends until notified

        self.mutex.lock(io);
        defer self.mutex.unlock();
        if (self.head) |item| {
            self.head = item.next;
            item.next = null;
            return item;
        }
        return null;
    }

    /// Non-blocking pop for contexts where you cannot suspend.
    fn tryPop(self: *WorkQueue) ?*WorkItem {
        if (self.mutex.tryLock()) {
            defer self.mutex.unlock();
            if (self.head) |item| {
                self.head = item.next;
                item.next = null;
                return item;
            }
        }
        return null;
    }
};

var queue = WorkQueue.init();

// --- Producer task ---
fn producerTask(io: volt.Io) void {
    var item = WorkQueue.WorkItem{ .payload = "process this row", .next = null };
    queue.push(io, &item);
}

// --- Consumer task ---
fn consumerTask(io: volt.Io) void {
    if (queue.pop(io)) |work| {
        std.log.info("got work: {s}", .{work.payload});
    }
}
```

### Example: Broadcast Shutdown Signal

Use `notifyAll()` to wake every waiting task at once -- for example, to signal that the server is shutting down.

```zig
const volt = @import("volt");

var shutdown_signal = volt.sync.Notify.init();

// Worker tasks each wait on the same Notify:
fn workerTask(io: volt.Io) void {
    shutdown_signal.wait(io);
    std.log.info("shutting down, cleaning up resources", .{});
}

// Main task triggers shutdown:
fn initiateShutdown() void {
    // Wake ALL waiting workers simultaneously.
    shutdown_signal.notifyAll();
}
```

***

## Barrier

Synchronization point. Blocks until N tasks arrive, then releases all.

```zig
const Barrier = volt.sync.Barrier;
```

### Construction

```zig
var barrier = Barrier.init(4); // 4 tasks must arrive
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `wait` | `fn wait(self: *Barrier, io: volt.Io) bool` | Convenience: arrive and suspend until all tasks arrive. Returns `true` if this was the leader (last to arrive). |
| `waitFuture` | `fn waitFuture(self: *Barrier) BarrierFuture` | Returns a Future that resolves when all tasks arrive. |
| `waitWith` | `fn waitWith(self: *Barrier, waiter: *Waiter) bool` | Arrive at barrier. Returns `true` if this was the last arrival. |

### Convenience: `wait(io)`

The simplest way to arrive at a barrier. Suspends until all N tasks have arrived.

```zig
fn workerPhase(io: volt.Io, barrier: *volt.sync.Barrier) void {
    // Phase 1 work...
    const is_leader = barrier.wait(io);  // suspends until all arrive
    if (is_leader) {
        // This task was the last to arrive -- do leader work
    }
    // Phase 2 work...
}
```

### Waiter

```zig
const Waiter = volt.sync.barrier.Waiter;

var waiter = Waiter.init();
if (barrier.waitWith(&waiter)) {
    // This task was the leader (last to arrive)
}
// waiter.is_leader.load(.acquire) == true for the leader
// waiter.isReleased() == true for all tasks after release
```

### Leader Election

The last task to arrive is the "leader". Check via the return value of `wait(io)` or `waiter.is_leader.load(.acquire)`. The leader can perform one-time finalization before all tasks proceed.

### Example: Parallel Computation with Checkpoint

Four worker tasks each process a chunk of data, then synchronize at a barrier before the second phase begins. This ensures no worker starts phase 2 until all workers have finished phase 1.

```zig
const std = @import("std");
const volt = @import("volt");

const NUM_WORKERS = 4;

const ParallelJob = struct {
    barrier: volt.sync.Barrier,
    // Each worker writes to its own slot -- no locking needed.
    phase1_results: [NUM_WORKERS]f64,
    combined_result: f64,

    fn init() ParallelJob {
        return .{
            .barrier = volt.sync.Barrier.init(NUM_WORKERS),
            .phase1_results = [_]f64{0} ** NUM_WORKERS,
            .combined_result = 0,
        };
    }

    /// Each worker calls this with its own index.
    fn runWorker(self: *ParallelJob, io: volt.Io, worker_id: usize, data_chunk: []const f64) void {
        // --- Phase 1: independent computation ---
        var sum: f64 = 0;
        for (data_chunk) |val| {
            sum += val;
        }
        self.phase1_results[worker_id] = sum;

        // --- Barrier: wait for all workers ---
        const is_leader = self.barrier.wait(io);

        if (is_leader) {
            // The leader (last to arrive) combines all partial results.
            // At this point, every phase1_results[i] is written.
            var total: f64 = 0;
            for (self.phase1_results) |partial| {
                total += partial;
            }
            self.combined_result = total;
        }

        // After the barrier releases, all workers can read combined_result.
    }
};

// Usage:
var job = ParallelJob.init();

// Spawn NUM_WORKERS tasks, each calling job.runWorker(io, id, chunk).
// After all return, job.combined_result holds the global sum.
```

:::tip
The leader election pattern lets you avoid a separate "reduce" step. The last worker to arrive already knows all other workers are done, so it can safely aggregate the results without additional synchronization.
:::

***

## OnceCell

Lazy one-time initialization. Safe for concurrent access.

```zig
const OnceCell = volt.sync.OnceCell;
```

### Construction

```zig
var cell = OnceCell(ExpensiveResource).init();
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `fn get(self: *const OnceCell(T)) ?*const T` | Get the value if initialized. Lock-free. |
| `getOrInit` | `fn getOrInit(self: *OnceCell(T), io: volt.Io, comptime init_fn: fn() T) *const T` | Convenience: get or initialize, suspending if another task is initializing. |
| `getOrInitFuture` | `fn getOrInitFuture(self: *OnceCell(T), comptime init_fn: fn() T) GetOrInitFuture` | Returns a Future for get-or-init. |
| `getOrInitWith` | `fn getOrInitWith(self: *OnceCell(T), waiter: *InitWaiter) ?*const T` | Waiter-based init. |
| `set` | `fn set(self: *OnceCell(T), value: T) bool` | Set value. Returns `false` if already initialized. |
| `isInitialized` | `fn isInitialized(self: *const OnceCell(T)) bool` | Check state. |

### Convenience: `getOrInit(io, fn)`

The simplest way to lazily initialize a value. The first caller runs the init function; subsequent callers suspend (if needed) and receive the cached result.

```zig
var db_pool_cell = volt.sync.OnceCell(DbPool).init();

fn getPool(io: volt.Io) *const DbPool {
    return db_pool_cell.getOrInit(io, createDbPool);
}
```

### State Machine

```
EMPTY --> INITIALIZING --> INITIALIZED
```

* `get()` is lock-free (atomic load).
* `getOrInit(io, fn)` uses CAS to race for INITIALIZING state. The winner initializes; losers suspend until done.
* After INITIALIZED, all calls return instantly (0.4ns in benchmarks).

### Example: Init-Once Database Pool

A global database connection pool that is created on first access. No matter how many tasks call `getPool()` concurrently, the pool is created exactly once.

```zig
const std = @import("std");
const volt = @import("volt");

const DbPool = struct {
    host: []const u8,
    port: u16,
    max_connections: u32,
    // In a real implementation: actual connection handles, etc.
};

/// Global, lazily initialized. No allocator needed for OnceCell itself.
var db_pool_cell = volt.sync.OnceCell(DbPool).init();

fn createDbPool() DbPool {
    // This function runs exactly once, even if 100 tasks call getPool()
    // at the same time. The first task to arrive runs this; all others
    // suspend and then receive the same pointer.
    std.log.info("initializing database pool (this runs once)", .{});
    return DbPool{
        .host = "db.internal.example.com",
        .port = 5432,
        .max_connections = 20,
    };
}

/// Safe to call from any task, any thread, at any time.
/// First call initializes, all subsequent calls return in ~0.4ns.
fn getPool(io: volt.Io) *const DbPool {
    return db_pool_cell.getOrInit(io, createDbPool);
}

// --- In request handler tasks ---
fn handleRequest(io: volt.Io) void {
    const pool = getPool(io);
    std.log.info("using pool at {s}:{d}", .{ pool.host, pool.port });
    // ... use pool ...
}
```

### Example: Lazy TLS Configuration

Initialize a TLS configuration once, then share the immutable config across all connection-handling tasks.

```zig
const std = @import("std");
const volt = @import("volt");

const TlsConfig = struct {
    cipher_suites: []const u8,
    min_version: u16,
    max_version: u16,
    session_ticket_key: [32]u8,
};

var tls_config_cell = volt.sync.OnceCell(TlsConfig).init();

fn loadTlsConfig() TlsConfig {
    // Expensive: reads certificates from disk, generates session keys.
    // Runs once, no matter how many tasks call getTlsConfig().
    var key: [32]u8 = undefined;
    std.crypto.random.bytes(&key);

    return TlsConfig{
        .cipher_suites = "TLS_AES_256_GCM_SHA384",
        .min_version = 0x0303, // TLS 1.2
        .max_version = 0x0304, // TLS 1.3
        .session_ticket_key = key,
    };
}

fn getTlsConfig(io: volt.Io) *const TlsConfig {
    return tls_config_cell.getOrInit(io, loadTlsConfig);
}

// You can also use set() if you want to initialize from outside:
fn initFromExternalConfig(config: TlsConfig) bool {
    // Returns false if someone already initialized it.
    return tls_config_cell.set(config);
}
```

:::note
`getOrInit(io, fn)` is the preferred API. It handles the race internally: the first caller to transition state from EMPTY to INITIALIZING does the work, and all other callers suspend until INITIALIZED. After initialization, `get()` and `getOrInit()` are a single atomic load -- effectively free.
:::

***

## Thread Safety

All sync primitives are safe for concurrent access from multiple tasks and threads. They use:

* **Atomic operations** for lock-free fast paths
* **Intrusive linked lists** for zero-allocation waiter queues
* **Batch waking** (wake outside the critical section) to minimize lock hold time

No raw pointers survive across yield points. Waiters are designed to be stack-allocated by the waiting task.

***

## Choosing the Right Primitive

Use this table to pick the correct primitive for your use case.

| Use Case | Primitive | Why |
|----------|-----------|-----|
| Protect mutable shared state | **Mutex** | Simplest exclusive lock. One holder at a time. |
| Read-heavy shared state with rare writes | **RwLock** | Readers proceed in parallel; only writers block. |
| Limit concurrent access (connection pool, rate limiter) | **Semaphore** | N permits = N concurrent holders. Flexible counting. |
| Signal "something happened" without data | **Notify** | `notifyOne()` for producer/consumer, `notifyAll()` for broadcast signals. |
| Wait for N tasks to reach a checkpoint | **Barrier** | All tasks block until the last one arrives. Leader election included. |
| Expensive one-time initialization | **OnceCell** | First caller initializes, all others get the cached value. Lock-free after init. |
| Pass a single value between two tasks | **Oneshot** channel | See the [Channels API](/v1.0.0-zig0.15.2/api/channel-api). One send, one recv. |
| Multi-producer message queue | **Channel** | See the [Channels API](/v1.0.0-zig0.15.2/api/channel-api). Bounded MPMC queue. |

### Decision Flowchart

1. **Do you need to pass data between tasks?** Use a Channel or Oneshot (see [Channels API](/v1.0.0-zig0.15.2/api/channel-api)).
2. **Do you need to protect shared mutable state?**
   * Reads vastly outnumber writes? Use **RwLock**.
   * Otherwise? Use **Mutex**.
3. **Do you need to limit concurrency (not protect state)?** Use **Semaphore**.
4. **Do you need to signal events without data?**
   * Wake one waiter? `Notify.notifyOne()`.
   * Wake all waiters? `Notify.notifyAll()`.
5. **Do you need all tasks to reach a synchronization point?** Use **Barrier**.
6. **Do you need lazy one-time initialization?** Use **OnceCell**.

---
title: Notify & OnceCell
description: Task notification primitives and lazy one-time initialization in Volt.
slug: v1.0.0-zig0.15.2/usage/notify-oncecell
---

## Notify

`Notify` is an async-aware task notification primitive, similar to a condition variable but designed for task coordination. It allows one task to signal another that an event has occurred, without transferring data.

### Initialization

```zig
const volt = @import("volt");

var notify = volt.sync.Notify.init();
```

Zero-allocation, no `deinit` required.

### Core operations

| Method | Behavior |
|--------|----------|
| `notifyOne()` | Wake one waiting task, or store a permit if none are waiting |
| `notifyAll()` | Wake all waiting tasks (does not store a permit) |
| `waitWith(waiter)` | Register a waiter; returns `true` if a permit was consumed immediately |
| `cancelWait(waiter)` | Remove a waiter from the queue |

### Permit semantics

`notifyOne()` has permit-based semantics to handle the "notify before wait" race:

* If no task is waiting, `notifyOne` stores a single permit.
* The next `waitWith` call consumes that permit and returns `true` immediately.
* Permits do not accumulate: multiple `notifyOne` calls with no waiters store at most one permit.

`notifyAll()` does **not** store a permit. If no tasks are waiting, it is a no-op.

### Basic usage

```zig
var notify = volt.sync.Notify.init();

// Producer side:
fn produceData() void {
    // ... generate data ...
    shared_buffer = data;
    notify.notifyOne(); // Wake one consumer
}

// Consumer side (low-level waiter):
fn consumeData() void {
    var waiter = volt.sync.notify.Waiter.init();
    waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

    if (!notify.waitWith(&waiter)) {
        // No permit available. Yield to scheduler.
        // Will be woken when notifyOne() is called.
    }
    // waiter.isNotified() is now true
    processData(shared_buffer);
}
```

### Async: wait(io)

`wait(io)` waits for a notification asynchronously, yielding to the scheduler until `notifyOne()` or `notifyAll()` is called:

```zig
notify.wait(io);
// Notification was received -- proceed
processData(shared_buffer);
```

Pass the `io: volt.Io` handle so the notify can yield to the scheduler and resume the task when a notification arrives.

### Advanced: waitFuture()

For manual future composition, `waitFuture()` returns a `WaitFuture` implementing the `Future` trait:

```zig
var future = notify.waitFuture();
// Poll through your scheduler...
// When future.poll() returns .ready, notification was received.
```

### Notify all

Wake every waiting task at once:

```zig
// Wake all consumers to process a batch
notify.notifyAll();
```

### Cancellation

Remove a waiter from the queue. Safe to call even if already notified:

```zig
notify.cancelWait(&waiter);

// Or on the future:
future.cancel();
```

### Diagnostics

```zig
notify.hasPermit();    // bool -- is a permit stored?
notify.waiterCount();  // usize -- number of queued waiters
```

### Example: work-available signal

```zig
var work_notify = volt.sync.Notify.init();

// Worker loop
fn workerLoop(io: volt.Io) void {
    while (running) {
        work_notify.wait(io); // Yields until work is available
        // Process all available work
        while (work_queue.tryPop()) |item| {
            process(item);
        }
    }
}

// Submitter
fn submitWork(item: WorkItem) void {
    work_queue.push(item);
    work_notify.notifyOne(); // Wake one worker
}
```

***

## OnceCell

`OnceCell(T)` stores a value that is initialized exactly once. Multiple tasks can race to initialize it, but only one succeeds. All other callers wait and then receive the same value.

This is the async equivalent of `std.once` or Go's `sync.Once`, but it returns a pointer to the computed value.

### Initialization

```zig
const volt = @import("volt");

// Start empty
var cell = volt.sync.OnceCell(ExpensiveConfig).init();

// Or start with a known value
var cell2 = volt.sync.OnceCell(u32).initWith(42);
```

### Check and get

```zig
if (cell.isInitialized()) {
    const ptr = cell.get().?; // *T
    useConfig(ptr.*);
}

// Const-safe access
const const_ptr = cell.getConst(); // ?*const T
```

### set()

Set the value explicitly. Returns `false` if already initialized:

```zig
if (cell.set(loadConfig())) {
    // We initialized it
} else {
    // Someone else already initialized it
}

// Value is always the first one set
const config = cell.get().?.*;
```

### getOrInit (async)

The primary use case: get the value, initializing it on first access. `getOrInit` takes a comptime initialization function and returns a `GetOrInitFuture`:

```zig
const initFn = struct {
    fn f() ExpensiveConfig {
        return loadConfigFromDisk();
    }
}.f;

var future = cell.getOrInit(initFn);
// Poll through your runtime...
// When future.poll() returns .ready, result is *ExpensiveConfig
const config_ptr = result.ready;
```

If the cell is already initialized, the future resolves immediately on the first poll. If another task is currently initializing, the future suspends until initialization completes.

### Low-level: getOrInitWith

```zig
const initFn = struct {
    fn f() u32 {
        return computeExpensiveValue();
    }
}.f;

var waiter = volt.sync.once_cell.InitWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

if (cell.getOrInitWith(initFn, &waiter)) |value_ptr| {
    // Got it immediately (already initialized or we initialized it)
    useValue(value_ptr.*);
} else {
    // Another task is initializing. Yield to scheduler.
    // When woken, waiter.isComplete() is true.
    const value_ptr = cell.get().?;
    useValue(value_ptr.*);
}
```

### State machine

`OnceCell` has three states:

| State | Meaning |
|-------|---------|
| `empty` | No value yet, first caller will initialize |
| `initializing` | A task is running the init function, others wait |
| `initialized` | Value is set, all access is lock-free |

Transitions are atomic CAS operations. Once initialized, `get()` and `isInitialized()` are simple acquire-load operations with no locking.

### Cancellation

Cancel a pending wait for initialization:

```zig
cell.cancelWait(&waiter);

// Or on the future:
future.cancel();
```

Note: cancelling a wait does not cancel the initialization itself. If a task is running the init function, it will complete regardless.

### Example: lazy singleton

```zig
var db_pool = volt.sync.OnceCell(ConnectionPool).init();

fn getPool() *ConnectionPool {
    const initFn = struct {
        fn f() ConnectionPool {
            return ConnectionPool.create(.{
                .host = "localhost",
                .port = 5432,
                .max_connections = 20,
            });
        }
    }.f;

    // First caller initializes; subsequent callers get cached value.
    // In sync context, use getOrInitWith:
    var waiter = volt.sync.once_cell.InitWaiter.init();
    if (db_pool.getOrInitWith(initFn, &waiter)) |pool| {
        return pool;
    }
    // In async context, would yield here and re-check
    unreachable; // Simplified for example
}
```

### Example: cached computation

```zig
var expensive_result = volt.sync.OnceCell(Matrix).init();

fn getOrCompute() *Matrix {
    if (expensive_result.get()) |cached| {
        return cached;
    }

    const initFn = struct {
        fn f() Matrix {
            return computeTransformMatrix();
        }
    }.f;

    var waiter = volt.sync.once_cell.InitWaiter.init();
    return expensive_result.getOrInitWith(initFn, &waiter).?;
}
```

***

## Choosing between Notify and OnceCell

| Need | Primitive |
|------|-----------|
| Signal "something happened" without data | `Notify` |
| Lazy one-time initialization | `OnceCell` |
| Repeated wake/sleep cycles | `Notify` (reusable, permit-based) |
| Cache an expensive computation | `OnceCell` |
| Coordinate producer/consumer | `Notify` + shared buffer |
| Thread-safe singleton | `OnceCell` |

Both primitives are zero-allocation and use intrusive linked lists for their waiter queues. Once an `OnceCell` is initialized, all subsequent access is a single atomic load with no locking.

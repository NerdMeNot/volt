---
title: Migration Guide
description: Migrate from blocking I/O and std.Thread patterns to Volt async tasks.
---

This guide walks through converting existing Zig code that uses blocking I/O and `std.Thread` to use Volt's async runtime.

## Conceptual Differences

| Blocking Model | Volt Model |
|---------------|---------------|
| One OS thread per connection | Many tasks share few worker threads |
| Thread blocks on syscall | Task yields, worker runs other tasks |
| `std.Thread.Mutex` blocks the OS thread | `volt.sync.Mutex` yields to scheduler |
| 16-64KB stack per thread | 256-512 bytes per task (stackless future) |
| Thousands of concurrent connections | Millions of concurrent tasks |

The key insight: in Volt, "waiting" means the task is suspended and the worker thread is free to run other tasks. Nothing blocks.

## Step 1: Wrap Your Entry Point

Before (blocking):

```zig
pub fn main() !void {
    var server = try startServer(8080);
    defer server.stop();
    server.serve();
}
```

After (Volt):

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Runtime) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    while (listener.tryAccept() catch null) |result| {
        _ = io.@"async"(handleClient, .{result.stream}) catch continue;
    }
}
```

`volt.run()` initializes the runtime (scheduler, I/O driver, timer wheel, blocking pool) and runs your function as the root task. When it returns, the runtime shuts down.

For more control over configuration:

```zig
pub fn main() !void {
    try volt.runWith(allocator, .{
        .num_workers = 4,
        .max_blocking_threads = 128,
    }, server);
}
```

Or manage the runtime manually:

```zig
pub fn main() !void {
    var io = try volt.Runtime.init(allocator, .{ .num_workers = 4 });
    defer io.deinit();
    try io.run(server);
}
```

## Step 2: Replace std.Thread with io.@"async"

Before (OS threads):

```zig
var threads: [num_workers]std.Thread = undefined;
for (&threads) |*t| {
    t.* = try std.Thread.spawn(.{}, workerFn, .{shared_state});
}
for (threads) |t| t.join();
```

After (Volt tasks):

```zig
const volt = @import("volt");

fn runWorkers(io: volt.Runtime) void {
    var futures: [num_workers]@TypeOf(io.@"async"(workerFn, .{shared_state}) catch unreachable) = undefined;

    for (&futures) |*f| {
        f.* = io.@"async"(workerFn, .{shared_state}) catch return;
    }

    // Wait for all using Group
    var group = volt.Group.init(io);
    for (&futures) |*f| {
        _ = f.@"await"(io);
    }
    group.wait();
}
```

Key differences:
- Tasks are lightweight (~256 bytes) vs threads (~8MB default stack on Linux).
- Tasks cooperatively yield; threads are preemptively scheduled by the OS.
- `@"async"` returns a `Future` with `@"await"(io)`, `cancel()`, and `isDone()`.

## Step 3: Replace std.Thread.Mutex with volt.sync.Mutex

Before (blocking mutex):

```zig
var mutex: std.Thread.Mutex = .{};
var shared: SharedState = .{};

fn worker() void {
    mutex.lock();         // Blocks the OS thread
    defer mutex.unlock();
    shared.update();
}
```

After (async-aware mutex):

```zig
var mutex = volt.sync.Mutex.init();
var shared: SharedState = .{};

fn worker() void {
    // Non-blocking attempt (preferred in hot paths)
    if (mutex.tryLock()) {
        defer mutex.unlock();
        shared.update();
        return;
    }

    // If contended, use the waiter-based API
    var waiter = volt.sync.mutex.Waiter.init();
    if (!mutex.lockWait(&waiter)) {
        // Task yields here -- worker thread runs other tasks
        // When the lock is released, waiter is woken
    }
    defer mutex.unlock();
    shared.update();
}
```

The `tryLock()` path is lock-free (CAS loop, no mutex). The waiter path only takes an internal mutex on the slow path. The key advantage: while your task waits for the lock, the worker thread is running other tasks instead of blocking.

### Using the Convenience API

For integration with the scheduler, use the sync convenience methods that take an `Runtime` handle:

```zig
// Lock the mutex -- suspends the task until the lock is acquired
mutex.lock(io);
defer mutex.unlock();
shared.update();
```

## Step 4: Replace std.Thread.Condition with volt.sync.Notify

Before (blocking condition variable):

```zig
var mutex: std.Thread.Mutex = .{};
var cond: std.Thread.Condition = .{};
var ready = false;

fn producer() void {
    mutex.lock();
    ready = true;
    cond.signal();
    mutex.unlock();
}

fn consumer() void {
    mutex.lock();
    while (!ready) cond.wait(&mutex);
    mutex.unlock();
    // proceed
}
```

After (async Notify):

```zig
var notify = volt.sync.Notify.init();

fn producer() void {
    // Set your condition, then notify
    notify.notifyOne();  // Wake one waiter
    // or: notify.notifyAll();  // Wake all waiters
}

fn consumer() void {
    var waiter = volt.sync.notify.Waiter.init();
    notify.waitWith(&waiter);
    if (!waiter.isNotified()) {
        // Yield to scheduler -- will be woken by notifyOne/notifyAll
    }
    // proceed
}
```

## Step 5: Handle Blocking Operations

:::note[`spawnBlocking` was renamed to `concurrent`]
If you're migrating from an older version of Volt, `io.spawnBlocking(fn, args)` has been renamed to `io.concurrent(fn, args)`. The behavior is identical -- it runs the function on the blocking thread pool. Update all call sites.
:::

Some operations are inherently blocking (DNS resolution, file I/O on some platforms, CPU-intensive computation). These must not run on async worker threads or they will starve other tasks.

Use the **blocking pool** via `io.concurrent`:

```zig
fn doResolve(io: volt.Runtime) !void {
    // Blocking DNS resolution on a separate OS thread
    const handle = try io.concurrent(struct {
        fn resolve(host: []const u8) !volt.net.Address {
            return volt.net.resolveFirst(std.heap.page_allocator, host, 443);
        }
    }.resolve, .{"example.com"});

    const addr = try handle.wait();
    _ = addr;
}
```

The blocking pool uses separate OS threads (up to `max_blocking_threads`, default 512) with idle timeout. Threads are spawned on demand and reclaimed after `blocking_keep_alive_ns` of inactivity.

### What Belongs in the Blocking Pool

| Operation | Blocking Pool? | Why |
|-----------|---------------|-----|
| DNS resolution (`getaddrinfo`) | Yes | Blocking syscall |
| File I/O (no io_uring) | Yes | Blocking syscall on some platforms |
| CPU-heavy computation (hashing, compression) | Yes | Starves I/O workers |
| Memory allocation (large) | Maybe | `mmap` can block |
| Sleep / delay | No | Use `volt.time.sleep()` instead |
| Network I/O | No | Already async via kqueue/epoll/io_uring |

## Step 6: Replace Channels

If you were using a custom thread-safe queue:

Before:

```zig
var queue: ThreadSafeQueue(Task) = .{};
var mutex: std.Thread.Mutex = .{};

fn produce(item: Task) void {
    mutex.lock();
    defer mutex.unlock();
    queue.push(item);
}
```

After:

```zig
var ch = try volt.channel.bounded(Task, allocator, 1024);
defer ch.deinit();

fn produce(item: Task) void {
    switch (ch.trySend(item)) {
        .ok => {},
        .full => {}, // Handle backpressure
        .closed => {},
    }
}

fn consume() void {
    switch (ch.tryRecv()) {
        .value => |item| process(item),
        .empty => {},
        .closed => return,
    }
}
```

The `Channel` uses a Vyukov/crossbeam-style lock-free ring buffer. The `trySend`/`tryRecv` paths are fully lock-free (CAS on head/tail). Only the waiter lists use a mutex.

## Common Pitfalls

### 1. Calling blocking code on async workers

If you call `std.Thread.sleep()` or any blocking syscall directly from a task, the entire worker thread blocks. Other tasks on that worker stall.

```zig
// BAD: Blocks the worker thread
fn myTask() void {
    std.Thread.sleep(1_000_000_000); // 1 second -- DO NOT DO THIS
}

// GOOD: Use async sleep
fn myTask() void {
    volt.time.blockingSleep(volt.time.Duration.fromSecs(1));
}

// GOOD: Or offload to blocking pool
fn myTask(io: volt.Runtime) void {
    _ = io.concurrent(struct {
        fn work() void {
            std.Thread.sleep(1_000_000_000);
        }
    }.work, .{}) catch {};
}
```

### 2. Holding locks across yield points

When a task yields (returns `.pending` from a future), it may resume on a different worker thread. If it holds a `std.Thread.Mutex`, the unlock happens on a different thread than the lock -- which is undefined behavior for OS mutexes.

Volt's `Mutex` handles this correctly because it uses an internal lock-free state machine, not an OS mutex for the public API.

### 3. Forgetting to deinit channels

`Channel` and `BroadcastChannel` allocate heap memory for their ring buffers. Always `deinit()` them. `Oneshot` and `Watch` are zero-allocation and need no cleanup.

### 4. Using too many workers

More workers means more contention on the global queue and more memory. Start with the default (CPU count) and measure before adding more.

## Migration Checklist

- [ ] Wrap entry point with `volt.run()` or `volt.runWith()`
- [ ] Replace `std.Thread.spawn` with `io.@"async"` (returns a `Future`)
- [ ] Replace `thread.join()` with `f.@"await"(io)`
- [ ] Replace `JoinHandle` types with `Future`
- [ ] Replace `std.Thread.Mutex` with `volt.sync.Mutex` (use `mutex.lock(io)` convenience)
- [ ] Replace condition variables with `volt.sync.Notify`
- [ ] Move blocking operations to `io.concurrent` (renamed from `spawnBlocking`)
- [ ] Remove `io.spawnFuture` calls -- use sync convenience methods instead
- [ ] Remove `volt.async_ops` imports -- use `io.@"async"` and direct patterns instead
- [ ] Replace custom queues with `volt.channel.bounded` (use `channel.send(io, val)` / `channel.recv(io)`)
- [ ] Replace `std.Thread.sleep` with `volt.time.blockingSleep`
- [ ] Add `defer ch.deinit()` for all `Channel` and `BroadcastChannel` instances
- [ ] Run `zig build test-all` to verify correctness

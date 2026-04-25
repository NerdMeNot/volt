---
title: Runtime
description: Creating and configuring the Volt runtime, spawning tasks, and
  managing the application lifecycle.
slug: v1.0.0-zig0.15.2/usage/runtime
---

The `Io` handle is the primary entry point for Volt. It owns the runtime — the work-stealing scheduler, the blocking thread pool, and the I/O driver. Create it explicitly with `init`/`deinit` like an `Allocator`.

:::note[Not everything needs the runtime]
The `tryX()` APIs (`tryLock`, `tryAcquire`, `trySend`, `tryRecv`) and all synchronous filesystem/networking operations work **without** a runtime. You only need the runtime for async APIs (`mutex.lock(io)`, `sem.acquire(io, n)`, `io.@"async"(func, args)`), async file I/O, and signal handling. All runtime-dependent operations go through `io: volt.Io` -- the type system ensures you have a runtime before you can call them. See [Basic Concepts](/v1.0.0-zig0.15.2/getting-started/basic-concepts/) for the full breakdown.
:::

## Explicit pattern (recommended)

Create `Io` explicitly — you control the allocator, configuration, and lifecycle:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 4,
        .max_blocking_threads = 128,
        .blocking_keep_alive_ns = 30 * std.time.ns_per_s,
    });
    defer io.deinit();

    try io.run(server);
}

fn server(io: volt.Io) void {
    // This runs inside the runtime.
    // All Volt APIs (net, sync, channel, time) are available here.
    _ = io;
}
```

This is the recommended pattern for production use. You get full control over memory (any `std.mem.Allocator` works) and can detect leaks with GPA.

## Zero-config shorthand

For quick scripts and prototyping, `volt.run()` creates an `Io` handle with sensible defaults (`page_allocator`, auto-detected worker count) and cleans up automatically:

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Io) void {
    _ = io;
    // This runs inside the runtime.
}
```

For custom configuration without managing `Io` yourself, use `volt.runWith`:

```zig
try volt.runWith(gpa.allocator(), .{
    .num_workers = 4,
}, server);
```

### Config fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `num_workers` | `usize` | `0` (auto) | I/O worker thread count. `0` means one per logical CPU. |
| `max_blocking_threads` | `usize` | `512` | Upper limit on blocking pool threads. |
| `blocking_keep_alive_ns` | `u64` | 10 seconds | Idle blocking threads exit after this duration. |
| `backend` | `?BackendType` | `null` (auto) | Force a specific I/O backend (io\_uring, kqueue, epoll, IOCP). |

When `num_workers` is `0`, the runtime queries the OS for the number of logical CPUs and creates one worker thread per core.

## Advanced: Direct Runtime access

For advanced use cases (custom schedulers, library integration), you can access the underlying `Runtime` through `Io`:

```zig
const volt = @import("volt");

pub fn main() !void {
    var io = try volt.Io.init(std.heap.page_allocator, .{
        .num_workers = 2,
    });
    defer io.deinit();

    try io.run(myApp);
}

fn myApp(io: volt.Io) !void {
    // Access the underlying runtime if needed
    const scheduler = io.runtime.getScheduler();
    _ = scheduler;
}
```

### Async primitives

Sync primitives accept the `io` handle directly for async acquisition. No manual future spawning needed:

```zig
var mutex = volt.sync.Mutex.init();

// Acquire the mutex asynchronously -- suspends until lock is held
mutex.lock(io);
defer mutex.unlock();
```

The `io` handle lets the primitive yield to the scheduler when contended and resume the calling task when the resource becomes available.

### Offloading blocking work

CPU-intensive or legacy blocking I/O should run on the blocking pool so the async workers stay responsive:

```zig
var f = try io.concurrent(computeHash, .{data});
const hash = try f.@"await"(io);
```

Blocking pool threads are created on demand (up to `max_blocking_threads`) and reclaimed after the keep-alive timeout.

:::caution[Operations that block the thread]
Some Volt APIs are synchronous and will block the calling OS thread. On a worker thread, this stalls every task on that worker.

**Blocking APIs (use on main thread or blocking pool only):**

| API | What it does | Async alternative |
|-----|-------------|-------------------|
| `volt.run(fn)` | Blocks main thread until runtime exits | N/A (intentional) |
| `volt.net.resolve()` | DNS lookup via `getaddrinfo` | Wrap in `io.concurrent()` |
| `volt.fs.readFile()` | Synchronous file read | `io.concurrent(volt.fs.readFile, .{...})` |
| `volt.fs.writeFile()` | Synchronous file write | `io.concurrent(volt.fs.writeFile, .{...})` |
| `std.Thread.sleep()` | Blocks the OS thread | `volt.time.sleep(duration)` |

**Safe on worker threads (these yield the task, not the thread):**

`mutex.lock(io)`, `ch.send(io, val)`, `ch.recv(io)`, `sem.acquire(io, n)`, `volt.time.sleep(dur)`, `stream.tryRead()`, `stream.tryWrite()`

See [Common Pitfalls](/v1.0.0-zig0.15.2/guides/common-pitfalls/#operations-that-block-the-thread) for the full catalog.
:::

## Task spawning from within async context

Inside an async context (from functions passed to `io.run` or spawned futures), use the `io` handle:

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    // Spawn concurrent async tasks.
    // `@"async"` uses Zig's identifier quoting (`async` is a reserved keyword).
    // `@as(u64, 42)` provides an explicit type annotation for the integer literal.
    var user_f = try io.@"async"(fetchUser, .{user_id});
    var posts_f = try io.@"async"(fetchPosts, .{user_id});

    // Await both results
    const user = user_f.@"await"(io);
    const posts = posts_f.@"await"(io);

    // Use results...
    _ = user;
    _ = posts;
}
```

:::note[Why the `@""` syntax?]
Zig reserves `async` and `await` as keywords. The `@"async"` and `@"await"` syntax is Zig's standard mechanism for using reserved words as identifiers, keeping the API familiar to developers coming from other async runtimes.
:::

### Available task functions

| Function | Returns | Description |
|----------|---------|-------------|
| `io.@"async"(func, args)` | `volt.Future(T)` | Spawn async task, returns a future |
| `f.@"await"(io)` | `T` | Await a future's result |
| `io.concurrent(func, args)` | `!ConcurrentFuture(T)` | Run on the blocking thread pool, call `.@"await"(io)` for result |

## Shutdown and cleanup

Call `io.deinit()` to shut down the runtime. This:

1. Sets the shutdown flag (atomic store).
2. Stops the blocking pool (joins idle threads, waits for active ones).
3. Stops the scheduler (signals workers, joins threads, frees task memory).
4. Frees the runtime allocation (if the `Io` handle owns it).

Always use `defer io.deinit()` immediately after `init` to guarantee cleanup even on error paths:

```zig
var io = try volt.Io.init(allocator, .{});
defer io.deinit();
```

For servers that need to drain in-flight requests before exiting, see [Signals & Shutdown](/v1.0.0-zig0.15.2/usage/signals-shutdown/).

## Thread-local runtime access

Inside a runtime, the current `Runtime` pointer is stored in a thread-local variable. Access it with:

```zig
const runtime_mod = @import("volt").internal.runtime;

// Returns ?*Runtime -- null if not inside a runtime context.
const rt = runtime_mod.getRuntime();

// Panics if not inside a runtime context.
const rt2 = runtime_mod.runtime();
```

This is primarily useful for library code that needs to access the scheduler or blocking pool without threading the runtime through every function signature.

## Architecture at a glance

```
main() thread
  |
  v
Io.init(allocator, config)
  |-- Scheduler (N worker threads, work-stealing deques)
  |-- BlockingPool (on-demand threads, up to max_blocking_threads)
  |-- I/O Driver (platform backend: io_uring / kqueue / epoll / IOCP)
  |
  v
io.run(myApp)  -->  @"async"  -->  @"await"
  |
  v
io.deinit()
```

Each worker thread runs a tight loop: poll local deque, steal from siblings, check global queue, poll I/O, advance timers. Tasks are stackless futures weighing approximately 256-512 bytes, enabling millions of concurrent tasks.

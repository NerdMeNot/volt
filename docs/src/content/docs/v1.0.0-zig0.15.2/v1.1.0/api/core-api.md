---
title: Core API
description: Runtime lifecycle, configuration, task spawning, and error handling in Volt.
slug: v1.0.0-zig0.15.2/v110/api/core-api
---

Every Volt application starts here. The core API gives you the runtime -- the engine that manages worker threads, I/O polling, timers, and task scheduling behind the scenes. You configure it once at startup, hand it your root function, and it takes care of the rest.

Most applications need just two things from this page: an entry point (`volt.run` or `volt.runWith`) and the `Config` struct. The Future system and manual `Runtime` management are for advanced use cases like custom schedulers, library integration, or composing async pipelines.

### At a Glance

```zig
const std = @import("std");
const volt = @import("volt");

// Explicit pattern (recommended) -- create Io like an Allocator:
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 4,              // pin to 4 cores
        .max_blocking_threads = 128,   // cap blocking pool
    });
    defer io.deinit();

    try io.run(myApp);
}

fn myApp(io: volt.Io) void {
    _ = io;
    // Everything inside here runs on the async runtime.
    // volt.task, volt.sync, volt.channel, volt.net, volt.time -- all available.
}
```

```zig
// Convenience shorthand -- zero config, auto-detect everything:
pub fn main() !void {
    try volt.run(myApp);
}
```

## Module Import

```zig
const volt = @import("volt");
```

All types are accessed through the `io` namespace: `volt.Runtime`, `volt.Config`, `volt.task`, `volt.sync`, etc.

## Entry Points

### `volt.Io.init` / `io.run` (Recommended)

```zig
pub fn init(allocator: std.mem.Allocator, config: Config) !Io
pub fn run(self: Io, comptime func: anytype) anyerror!FnPayload(@TypeOf(func))
pub fn deinit(self: *Io) void
```

The primary entry point. Create `Io` explicitly — like an `Allocator`, you `init`/`deinit`/pass it through. This is the recommended pattern for production because you control the allocator, configuration, and lifecycle.

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }

    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 4,
        .max_blocking_threads = 64,
    });
    defer io.deinit();

    try io.run(serve);
}

fn serve(io: volt.Io) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    while (listener.tryAccept() catch null) |result| {
        const future = io.@"async"(handleClient, .{result.stream}) catch continue;
        _ = future;
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.tryRead(&buf) catch return orelse continue;
        if (n == 0) return; // Client disconnected
        stream.writeAll(buf[0..n]) catch return;
    }
}
```

### `volt.run` / `volt.runWith` (Convenience)

```zig
pub fn run(comptime func: anytype) anyerror!PayloadType(@TypeOf(func))
pub fn runWith(allocator: std.mem.Allocator, config: Config, comptime func: anytype) anyerror!PayloadType(@TypeOf(func))
```

Convenience shorthands that create `Io`, run the function, and clean up. `volt.run` uses `page_allocator` with default config. `volt.runWith` accepts a custom allocator and config.

```zig
const volt = @import("volt");

// Zero-config:
pub fn main() !void {
    try volt.run(myApp);
}

// Custom config:
pub fn main() !void {
    try volt.runWith(allocator, .{ .num_workers = 4 }, myApp);
}
```

:::tip
`volt.run` and `volt.runWith` are equivalent to `Io.init` + `io.run` + `io.deinit`. Use them for quick scripts; use the explicit `Io` pattern for production.
:::

***

## Core Types

### `volt.Future(T)`

```zig
pub fn Future(comptime T: type) type
```

The handle returned by `io.@"async"()`. Represents a spawned async task that will produce a value of type `T`. This replaces the old `JoinHandle` pattern.

| Method | Signature | Description |
|--------|-----------|-------------|
| `@"await"` | `fn @"await"(self: *Self, io: volt.Io) Result` | Suspend until the task completes and return its result. Takes the `Io` handle. |
| `cancel` | `fn cancel(self: *Self, io: volt.Io) Result` | Cancel the task and wait for completion. This is NOT fire-and-forget -- it blocks until the task finishes. |
| `isDone` | `fn isDone(self: *const Self) bool` | Check if the task has completed (does not consume the result). |

```zig
fn example(io: volt.Io) !void {
    // Spawn an async task -- returns a Future
    const future = try io.@"async"(fetchUser, .{user_id});

    // Await the result (suspends the current task until complete)
    const user = future.@"await"(io);

    // Or cancel and wait
    const metrics_future = try io.@"async"(emitMetrics, .{event});
    _ = metrics_future.cancel(io);
}
```

### `volt.Group`

```zig
pub const Group = volt.Group;
```

A task group for structured concurrency. Spawn multiple tasks into the group and wait for all of them to complete.

| Method | Signature | Description |
|--------|-----------|-------------|
| `spawn` | `fn spawn(self: *Group, func: anytype, args: anytype) bool` | Spawn a task into the group. Returns `true` if successfully spawned. |
| `wait` | `fn wait(self: *Group) void` | Wait for all spawned tasks to complete. |
| `cancel` | `fn cancel(self: *Group) void` | Cancel all tasks in the group. |
| `taskCount` | `fn taskCount(self: *Group) usize` | Return the number of tasks in the group. |

```zig
fn example(io: volt.Io) !void {
    var group = volt.Group.init(io);

    _ = group.spawn(fetchUser, .{id});
    _ = group.spawn(fetchPosts, .{id});

    // Wait for all tasks in the group
    group.wait();
}
```

***

## Runtime

### `volt.Runtime`

The underlying async I/O runtime. Combines a work-stealing scheduler, I/O driver, blocking pool, and timer wheel. Most users should use `Io` instead of `Runtime` directly.

#### `init`

```zig
pub fn init(allocator: Allocator, config: Config) !Runtime
```

Initialize the runtime. Spawns worker threads (based on config) and sets up the I/O backend. Prefer `Io.init()` which heap-allocates and manages the runtime for you.

#### `deinit`

```zig
pub fn deinit(self: *Runtime) void
```

Shut down the runtime. Signals all workers to stop, joins threads, frees resources.

#### `run`

```zig
pub fn run(self: *Runtime, comptime func: anytype) anyerror!FnPayload(@TypeOf(func))
```

Run a function on the runtime, blocking the calling thread until complete. Creates a `FnFuture` from the function, spawns it, and blocks via `@"await"()`. The function receives a non-owning `Io` handle.

```zig
var io = try volt.Io.init(allocator, .{});
defer io.deinit();
try io.run(myServer);
```

:::note
`Runtime` is an internal type. Most users should use `Io` methods (`io.@"async"`, `io.concurrent`) instead of interacting with `Runtime` directly.
:::

#### `getBlockingPool`

```zig
pub fn getBlockingPool(self: *Runtime) *BlockingPool
```

Access the blocking pool directly.

#### `getScheduler`

```zig
pub fn getScheduler(self: *Runtime) *Scheduler
```

Access the underlying work-stealing scheduler.

***

## High-Level Task API

These are the primary methods on `io: volt.Io` for spawning and awaiting work. They use Zig's `@""` quoting syntax because `async` and `await` are reserved keywords.

:::note[Why `@"async"` and `@"await"`?]
`async` and `await` are reserved keywords in Zig. The `@""` syntax is Zig's standard identifier quoting -- not Volt-specific. Read `io.@"async"` as "io dot async". See [Common Pitfalls](/v1.0.0-zig0.15.2/guides/common-pitfalls/#the-async--await-syntax) for more.
:::

### `io.@"async"` (Spawn)

```zig
pub fn @"async"(
    io: volt.Io,
    comptime func: anytype,
    args: anytype,
) !IoFuture(FnReturnType(@TypeOf(func)))
```

Spawn a plain function as a concurrent async task. The function is wrapped in a single-poll `FnFuture` and scheduled on the work-stealing runtime. Returns a `volt.Future(T)`. The call itself returns an error union because it may fail to allocate the task.

```zig
fn add(a: i32, b: i32) i32 { return a + b; }

const future = try io.@"async"(add, .{ 1, 2 });
const result = future.@"await"(io); // 3
```

The function can return any type, including error unions:

```zig
fn fetchData(url: []const u8) ![]u8 {
    // ...
}

const future = try io.@"async"(fetchData, .{"https://example.com"});
const data = future.@"await"(io); // Propagates errors
```

### `future.@"await"` (Await)

```zig
pub fn @"await"(self: Future(T), io: volt.Io) T
```

Suspend the current task until the future completes, then return its result. Takes the `Io` handle so the scheduler can poll other tasks while waiting.

```zig
const future = try io.@"async"(fetchUser, .{user_id});
const user = future.@"await"(io);
```

### `io.concurrent` (Blocking Work)

```zig
pub fn concurrent(
    io: volt.Io,
    comptime func: anytype,
    args: anytype,
) !*BlockingHandle(FnReturnType(@TypeOf(func)))
```

Run a function on the blocking thread pool. Returns a `BlockingHandle` that you call `.wait()` on to get the result. Use for CPU-intensive work or blocking I/O that would starve async workers.

```zig
// Offload a heavy computation to the blocking pool
const handle = try io.concurrent(computeSha256, .{file_bytes});
const digest = try handle.wait();
```

The blocking pool uses separate OS threads (up to `max_blocking_threads`). Threads are spawned on demand and reclaimed after idle timeout.

***

## Configuration

### `volt.Config`

```zig
pub const Config = struct {
    /// Number of I/O worker threads (0 = auto based on CPU count).
    num_workers: usize = 0,

    /// Maximum blocking pool threads (default: 512).
    max_blocking_threads: usize = 512,

    /// Blocking thread idle timeout in nanoseconds (default: 10 seconds).
    blocking_keep_alive_ns: u64 = 10 * std.time.ns_per_s,

    /// I/O backend type (null = auto-detect).
    backend: ?BackendType = null,
};
```

| Field | Default | Description |
|-------|---------|-------------|
| `num_workers` | `0` (auto) | Worker thread count. 0 = `std.Thread.getCpuCount()`. |
| `max_blocking_threads` | `512` | Upper bound on blocking pool threads. |
| `blocking_keep_alive_ns` | 10s | Idle blocking threads exit after this duration. |
| `backend` | `null` (auto) | I/O backend: `.io_uring`, `.kqueue`, `.epoll`, `.iocp`, or `null` for auto-detect. |

### Configuration Profiles

Different workloads benefit from different configurations. Here are recommended starting points:

```zig
const std = @import("std");
const volt = @import("volt");

/// High-throughput web server: many connections, mostly I/O-bound.
/// Workers match CPU count so each core handles its own event loop.
/// Large blocking pool absorbs occasional disk I/O or DNS lookups.
const web_server_config = volt.Config{
    .num_workers = 0, // auto-detect CPU count
    .max_blocking_threads = 256,
    .blocking_keep_alive_ns = 30 * std.time.ns_per_s, // keep threads warm
};

/// CPU-heavy pipeline: data parsing, compression, image processing.
/// Fewer I/O workers since most work happens on blocking threads.
/// Blocking pool sized to saturate cores without over-subscribing.
const cpu_pipeline_config = volt.Config{
    .num_workers = 2, // minimal I/O workers
    .max_blocking_threads = 16, // bounded to avoid context-switch overhead
    .blocking_keep_alive_ns = 60 * std.time.ns_per_s,
};

/// Latency-sensitive service: trading, gaming, real-time control.
/// Pinned worker count prevents OS scheduling jitter.
/// Short keep-alive so idle threads release resources quickly.
const low_latency_config = volt.Config{
    .num_workers = 4, // fixed, no auto-detect
    .max_blocking_threads = 8,
    .blocking_keep_alive_ns = 2 * std.time.ns_per_s, // reclaim fast
    .backend = .io_uring, // lowest latency on Linux 5.11+
};
```

***

## Spawn and Await Examples

### Spawn and Await with Error Handling

This example shows the typical lifecycle of spawning a task with `io.@"async"()`, awaiting it, and falling back gracefully:

```zig
const std = @import("std");
const volt = @import("volt");
const log = std.log.scoped(.worker);

const UserProfile = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
};

fn fetchUserProfile(user_id: u64) UserProfile {
    // Simulate fetching a user profile from a database or API.
    // In a real application, this would involve I/O.
    return .{
        .id = user_id,
        .name = "Alice",
        .email = "alice@example.com",
    };
}

fn fetchUserPosts(user_id: u64) u32 {
    // Simulate counting user posts.
    _ = user_id;
    return 42;
}

fn handleRequest(io: volt.Io, user_id: u64) !void {
    // Spawn two concurrent tasks to fetch data in parallel
    const profile_future = try io.@"async"(fetchUserProfile, .{user_id});
    const posts_future = try io.@"async"(fetchUserPosts, .{user_id});

    // Await both results. @"await" suspends the current task
    // until the spawned task completes.
    const profile = profile_future.@"await"(io);
    const post_count = posts_future.@"await"(io);

    log.info("User {s} (id={}) has {} posts", .{
        profile.name,
        profile.id,
        post_count,
    });
}
```

### Fire-and-Forget Tasks

If you do not need the result, simply discard the future. The spawned task continues running to completion:

```zig
fn emitMetrics(event_name: []const u8, latency_ns: u64) void {
    // Send metrics to a stats collector. We do not care about
    // the result -- if it fails, we just lose that data point.
    _ = event_name;
    _ = latency_ns;
}

fn processRequest(io: volt.Io, payload: []const u8) !void {
    const start = std.time.nanoTimestamp();

    // ... process the request ...
    _ = payload;

    const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

    // Fire-and-forget: spawn the metrics task and discard the future
    _ = try io.@"async"(emitMetrics, .{ "request_processed", elapsed });
}
```

### Multi-Task Coordination

The `io` handle provides helpers for common multi-task patterns:

```zig
const volt = @import("volt");

fn coordinateTasks(io: volt.Io) !void {
    // joinAll: wait for all tasks, fail on first error.
    // Returns a tuple of results in the same order as the futures.
    const f1 = try io.@"async"(fetchUser, .{id});
    const f2 = try io.@"async"(fetchPosts, .{id});
    const user, const posts = io.joinAll(.{ f1, f2 }) catch return;

    // tryJoinAll: wait for all tasks, collecting successes AND errors.
    // Never fails early -- every task runs to completion.
    const results = io.tryJoinAll(.{ f1, f2, f3 });
    for (results) |result| {
        switch (result) {
            .ok => |value| handleSuccess(value),
            .err => |e| handleError(e),
        }
    }

    // race: first to complete wins, cancel the rest.
    // All futures must have the same Output type.
    const f_primary = try io.@"async"(fetchFromPrimary, .{key});
    const f_replica = try io.@"async"(fetchFromReplica, .{key});
    const fastest_result = io.race(.{ f_primary, f_replica }) catch return;

    // select: first to complete wins, others keep running.
    // Returns both the result and the index of the winning task.
    const sel_result, const winner_index = io.select(.{ f1, f2 }) catch return;
}
```

***

## Version

```zig
pub const version = struct {
    pub const major = 0;
    pub const minor = 3;
    pub const patch = 0;
    pub const string = "0.3.0";
};
```

Access via `volt.version.string` or individual components.

***

## Common Patterns

### Web Server Bootstrap

A complete startup pattern for a production web server, including allocator setup, configuration, graceful shutdown wiring, and connection handling:

```zig
const std = @import("std");
const volt = @import("volt");
const log = std.log.scoped(.server);

pub fn main() !void {
    // 1. Set up a leak-detecting allocator for the runtime.
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) {
            log.err("Memory leak detected on shutdown", .{});
        }
    }
    const allocator = gpa.allocator();

    // 2. Initialize shutdown handler BEFORE the runtime.
    //    This registers SIGINT/SIGTERM so Ctrl+C triggers clean exit.
    var shutdown_handler = try volt.shutdown.Shutdown.init();
    defer shutdown_handler.deinit();

    // 3. Start the runtime with a tuned configuration.
    //    The root function receives `io: volt.Io` from the runtime.
    var io = try volt.Io.init(allocator, .{
        .num_workers = 0, // auto-detect CPU count
        .max_blocking_threads = 128,
    });
    defer io.deinit();

    try io.run(struct {
        fn entry(rt_io: volt.Io) void {
            // This is the root task -- runs on the scheduler.
            serveHttp(rt_io, &shutdown_handler) catch |err| {
                log.err("Server error: {}", .{err});
            };
        }
        var shutdown_handler: *volt.shutdown.Shutdown = undefined;
    }.entry);

    // 4. After io.run returns, the runtime is fully stopped.
    log.info("Server exited cleanly", .{});
}

fn serveHttp(io: volt.Io, shutdown_handler: *volt.shutdown.Shutdown) !void {
    var listener = try volt.net.listen("0.0.0.0:8080");
    defer listener.close();

    log.info("Listening on :8080", .{});

    while (!shutdown_handler.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            // Track in-flight work for graceful drain
            var guard = shutdown_handler.startWork();

            _ = io.@"async"(struct {
                fn handle(stream: volt.net.TcpStream, work: *volt.shutdown.WorkGuard) void {
                    defer work.deinit(); // Decrements pending count on exit

                    var conn = stream;
                    defer conn.close();

                    var buf: [4096]u8 = undefined;
                    while (true) {
                        const n = conn.tryRead(&buf) catch return orelse continue;
                        if (n == 0) return;
                        conn.writeAll(buf[0..n]) catch return;
                    }
                }
            }.handle, .{ result.stream, &guard }) catch continue;
        } else {
            // No pending connection -- check shutdown between polls
            if (shutdown_handler.isShutdown()) break;
            std.Thread.sleep(1_000_000); // 1ms
        }
    }

    // Drain: wait for in-flight requests to finish (up to 10 seconds)
    log.info("Shutting down, waiting for {} pending requests...", .{
        shutdown_handler.pendingCount(),
    });
    if (!shutdown_handler.waitPendingTimeout(volt.time.Duration.fromSecs(10))) {
        log.warn("Timed out waiting for pending requests", .{});
    }
}
```

### Background Worker Pattern

Spawn long-running background tasks alongside the main server loop. This pattern is common for periodic cleanup, metric flushing, or queue processing:

```zig
const std = @import("std");
const volt = @import("volt");
const log = std.log.scoped(.worker);

/// Background worker that periodically flushes an in-memory buffer
/// to disk. Runs on the blocking pool so it never stalls I/O workers.
fn flushLoop(io: volt.Io, interval_secs: u64, shutdown: *volt.shutdown.Shutdown) void {
    while (!shutdown.isShutdown()) {
        // Sleep without blocking I/O workers
        std.Thread.sleep(interval_secs * std.time.ns_per_s);

        // Flush is CPU/disk-bound, so run it on the blocking pool
        const handle = io.concurrent(flushBufferToDisk, .{}) catch continue;
        _ = handle.wait() catch {};
    }
    log.info("Flush worker exiting", .{});
}

fn flushBufferToDisk() void {
    // Simulate writing buffered data to disk.
    // In a real system, this would serialize and fsync.
    std.Thread.sleep(50_000_000); // 50ms simulated I/O
}

/// Start the server with a background flush worker alongside it.
fn startWithBackgroundWorker(io: volt.Io, shutdown: *volt.shutdown.Shutdown) !void {
    // Spawn the background flush worker.
    // It runs concurrently with the main accept loop.
    const worker_future = try io.@"async"(flushLoop, .{
        io,
        @as(u64, 30), // flush every 30 seconds
        shutdown,
    });

    // Run the main server loop (blocks until shutdown)
    serveRequests(shutdown);

    // After the server loop exits, wait for the worker to finish
    _ = worker_future.@"await"(io);
}

fn serveRequests(shutdown: *volt.shutdown.Shutdown) void {
    // Main accept loop (simplified)
    while (!shutdown.isShutdown()) {
        std.Thread.sleep(10_000_000); // 10ms poll interval
    }
}
```

### Graceful Io Setup with GPA

A minimal but complete pattern for setting up Io with proper resource cleanup. This is the recommended starting template for any Volt application:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    // GeneralPurposeAllocator catches leaks and double-frees in debug builds.
    // In ReleaseFast, it compiles to a thin wrapper over the page allocator.
    var gpa = std.heap.GeneralPurposeAllocator(.{
        // Optional: enable stack traces for allocation tracking.
        // Helpful when debugging leaks, but has overhead.
        .stack_trace_frames = 8,
    }){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) {
            @panic("Memory leak detected -- check allocation sites above");
        }
    }

    // Create Io explicitly -- like an Allocator, you init/deinit/pass it through.
    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 0, // auto-detect
    });
    defer io.deinit(); // Joins all worker threads, frees scheduler memory

    // Run the application. The return value propagates from appMain().
    try io.run(appMain);
}

fn appMain(io: volt.Io) !void {
    // Application logic runs here, inside the async runtime.
    // All volt.task, volt.sync, volt.channel, and volt.net APIs are available.

    const future = try io.@"async"(doWork, .{});
    _ = future.@"await"(io);
}

fn doWork() void {
    // Your application logic
}
```

:::tip
The three-step pattern -- allocator setup, Io init, defer cleanup -- ensures that resources are released in the correct order even when errors occur. Always `defer io.deinit()` immediately after `init()` succeeds.
:::

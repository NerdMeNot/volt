---
title: Task API
description: Complete API reference for task spawning, Future(T), combinators
  (joinAll, race, select), and cooperative scheduling in Volt.
slug: v1.0.0-zig0.15.2/api/task-api
---

Tasks are the unit of concurrency in Volt -- lightweight async functions that run on the work-stealing scheduler. Think of them as green threads that cost ~256-512 bytes each (not 8MB like OS threads), so you can run millions concurrently.

The typical workflow: **spawn** a function as a task with `io.@"async"()`, do other work, then **await** the result with `future.@"await"(io)`. The scheduler distributes tasks across worker threads automatically, stealing work from busy workers to keep all cores fed. You write sequential-looking code; the runtime handles parallelism.

All spawn and combinator operations go through an explicit `io: volt.Io` handle, following the same pattern Zig uses for `Allocator` -- if a function needs async capabilities, it takes an `io` parameter.

For coordinating multiple tasks, Volt provides combinators inspired by Rust's futures:

* **joinAll** -- wait for all tasks, fail on first error (scatter-gather)
* **tryJoinAll** -- wait for all tasks, collect successes *and* errors (partial failure tolerance)
* **race** -- first to finish wins, cancel the rest (redundant requests, cache vs. DB)
* **select** -- first to finish wins, others keep running (event multiplexing)

### At a Glance

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    // Spawn a function as a concurrent task
    const future = try io.@"async"(fetchUser, .{user_id});
    const user = future.@"await"(io);  // await the result

    // Fire and forget -- discard the future
    _ = try io.@"async"(emitMetrics, .{event});

    // Parallel fetch -- both run simultaneously
    const f1 = try io.@"async"(fetchUser, .{id});
    const f2 = try io.@"async"(fetchPosts, .{id});
    const user, const posts = try io.joinAll(.{ f1, f2 });

    // First response wins, cancel the slower one
    const fast = try io.@"async"(fetchFromCache, .{key});
    const slow = try io.@"async"(fetchFromDb, .{key});
    const result = try io.race(.{ fast, slow });

    // CPU-heavy work goes to the blocking pool (won't starve I/O workers)
    const handle = try io.concurrent(computeSha256, .{data});
    const digest = try handle.wait();
}
```

All spawn and combinator methods are on the `io: volt.Io` handle.

## Spawning Tasks

### `io.@"async"`

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

#### Example: Parallel HTTP Fetch

Fetch a user profile and their posts concurrently, then combine the results.
Both requests execute simultaneously on separate workers. `joinAll` blocks
until both complete.

```zig
const volt = @import("volt");

const User = struct {
    id: u64,
    name: []const u8,
};

const Post = struct {
    title: []const u8,
    body: []const u8,
};

fn fetchUser(user_id: u64) !User {
    // Simulates an HTTP GET /users/{id} call.
    // In production, this would use volt.net.TcpStream to talk to a server.
    _ = user_id;
    return User{ .id = 1, .name = "Alice" };
}

fn fetchPosts(user_id: u64) ![]const Post {
    // Simulates an HTTP GET /users/{id}/posts call.
    _ = user_id;
    return &[_]Post{
        .{ .title = "First post", .body = "Hello world" },
    };
}

fn loadUserProfile(io: volt.Io, user_id: u64) !void {
    // Kick off both requests concurrently. Each @"async" schedules
    // the function on the work-stealing runtime immediately.
    const user_f = try io.@"async"(fetchUser, .{user_id});
    const posts_f = try io.@"async"(fetchPosts, .{user_id});

    // joinAll suspends until BOTH tasks finish. If either returns
    // an error, it propagates immediately. The other task keeps
    // running (it is not cancelled automatically).
    const user, const posts = try io.joinAll(.{ user_f, posts_f });

    // At this point both results are available.
    std.debug.print("User: {s}, Posts: {d}\n", .{ user.name, posts.len });
}

pub fn main() !void {
    try volt.run(struct {
        fn entry(io: volt.Io) !void {
            try loadUserProfile(io, 42);
        }
    }.entry);
}
```

### Convenience Methods on Sync Primitives

Instead of the old `spawnFuture` pattern, sync primitives now offer direct convenience methods that take an `io` handle. These suspend the current task (not the worker thread) until the operation completes:

```zig
const volt = @import("volt");
const sync = volt.sync;

var config_mutex = sync.Mutex.init();

fn updateConfig(io: volt.Io, new_config: Config) !void {
    // Validate concurrently while we wait for the lock
    const valid_f = try io.@"async"(validateConfig, .{new_config});

    // Convenience method: suspends until the lock is acquired.
    // No spawnFuture needed -- the Io handle drives the poll loop.
    config_mutex.lock(io);
    defer config_mutex.unlock();

    const valid_config = valid_f.@"await"(io);
    shared_config = valid_config;
}

fn validateConfig(config: Config) !Config {
    if (config.max_connections == 0) return error.InvalidConfig;
    return config;
}
```

Similarly for channels:

```zig
// Blocking send/recv that suspend the task, not the worker thread
channel.send(io, value);        // suspends until slot available
const val = channel.recv(io);   // suspends until value available
```

### `io.concurrent`

```zig
pub fn concurrent(
    io: volt.Io,
    comptime func: anytype,
    args: anytype,
) !*BlockingHandle(FnReturnType(@TypeOf(func)))
```

Run a function on the blocking thread pool. Returns a `BlockingHandle` that you call `.wait()` on to get the result. Use for CPU-intensive work or blocking I/O that would starve async workers.

```zig
const handle = try io.concurrent(struct {
    fn hash(data: []const u8) [32]u8 {
        return std.crypto.hash.sha2.Sha256.hash(data, .{});
    }
}.hash, .{input_data});
const digest = try handle.wait();
```

The blocking pool uses separate OS threads (up to `max_blocking_threads`). Threads are spawned on demand and reclaimed after idle timeout.

#### Example: File Processing Pipeline

Read a large CSV file on the blocking pool (it uses synchronous `read()`
syscalls that would block an async worker), then parse rows on the blocking pool.

```zig
const std = @import("std");
const volt = @import("volt");

const Record = struct {
    name: []const u8,
    value: f64,
};

/// Reads a file synchronously. This MUST run on the blocking pool
/// because std.fs.cwd().readFileAlloc() issues blocking read() syscalls
/// that would stall an async I/O worker thread.
fn readFile(path: []const u8) ![]const u8 {
    return std.fs.cwd().readFileAlloc(
        std.heap.page_allocator,
        path,
        10 * 1024 * 1024, // 10 MB limit
    );
}

/// Parses CSV lines into records. CPU-bound work that benefits from
/// running on the blocking pool to keep async workers free for I/O.
fn parseCsv(raw: []const u8) ![]Record {
    var records: std.ArrayList(Record) = .empty;
    var lines = std.mem.splitScalar(u8, raw, '\n');

    // Skip header line.
    _ = lines.next();

    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var fields = std.mem.splitScalar(u8, line, ',');
        const name = fields.next() orelse continue;
        const value_str = fields.next() orelse continue;
        const value = std.fmt.parseFloat(f64, value_str) catch continue;

        try records.append(std.heap.page_allocator, Record{
            .name = name,
            .value = value,
        });
    }

    return records.items;
}

fn processDataFile(io: volt.Io, path: []const u8) !void {
    // Step 1: Read file on blocking pool. The async worker that
    // runs this task is NOT blocked -- it continues polling other
    // tasks while the blocking thread does the read.
    const read_handle = try io.concurrent(readFile, .{path});
    const raw_data = try read_handle.wait();

    // Step 2: Parse on blocking pool (CPU-bound).
    const parse_handle = try io.concurrent(parseCsv, .{raw_data});
    const records = try parse_handle.wait();

    std.debug.print("Parsed {d} records\n", .{records.len});
}
```

***

## `volt.Future(T)`

Returned by `io.@"async"()`. Represents a spawned async task that will produce a value of type `T`.

```zig
const Future = volt.Future;
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `@"await"` | `fn @"await"(self: *Self, io: volt.Io) Result` | Suspend until the task completes and return its result. Takes the `Io` handle. |
| `cancel` | `fn cancel(self: *Self, io: volt.Io) Result` | Cancel the task and wait for completion. This is NOT fire-and-forget -- it blocks until the task finishes. |
| `isDone` | `fn isDone(self: *const Self) bool` | Check if the task has completed (does not consume the result). |

### Usage Patterns

#### Pattern 1: Await Result

The most common pattern. Spawn work, do something else, then collect
the result. `@"await"` suspends the calling task until the spawned
task completes.

```zig
fn fetchReport(io: volt.Io, report_id: u64) !Report {
    // Spawn the database query as a background task.
    const future = try io.@"async"(queryDatabase, .{report_id});

    // The calling task continues executing here. The query runs
    // concurrently on another worker.
    logAccess(report_id);

    // Suspend until the query completes.
    const report = future.@"await"(io);
    return report;
}
```

#### Pattern 2: Check Completion

Use `isDone` when you want to check if a task has completed without blocking.

```zig
fn progressLoop(io: volt.Io) !void {
    const future = try io.@"async"(longComputation, .{});

    var ticks: u32 = 0;
    while (!future.isDone()) {
        // Do other work while we wait.
        ticks += 1;
        processIncomingMessages();
    }

    // isDone() returned true, so @"await" will return immediately.
    const result = future.@"await"(io);
    std.debug.print("Done after {d} ticks: {}\n", .{ ticks, result });
}
```

#### Pattern 3: Fire and Forget

Discard the future to let the task run in the background. The task
continues running to completion, but the result is discarded.

```zig
fn emitAnalyticsEvent(io: volt.Io, event: AnalyticsEvent) !void {
    // We do not want to wait for the network call to finish.
    // Spawn and discard the future.
    _ = try io.@"async"(sendToAnalytics, .{event});
}

fn sendToAnalytics(event: AnalyticsEvent) !void {
    // This runs to completion even though nobody is waiting for it.
    // If it fails, the error is silently dropped.
    _ = event;
}
```

#### Pattern 4: Cancel with Cleanup

`cancel(io)` requests cancellation and waits for the task to complete.
It is NOT fire-and-forget -- the call blocks until the task finishes.
If the task has already completed, `cancel` returns the result immediately.

```zig
fn fetchWithTimeout(io: volt.Io, url: []const u8, deadline_ms: u64) !?[]const u8 {
    const fetch_f = try io.@"async"(httpGet, .{url});

    // Spawn a sleep as the deadline timer.
    const timer_f = try io.@"async"(struct {
        fn sleep(io_inner: volt.Io, ms: u64) void {
            io_inner.sleep(volt.time.Duration.fromMillis(ms));
        }
    }.sleep, .{ io, deadline_ms });

    // Race them: whichever finishes first wins.
    // select() does NOT cancel the loser -- we handle that ourselves.
    const result, const winner_index = try io.select(.{ fetch_f, timer_f });

    if (winner_index == 0) {
        // Fetch completed before the timer. Cancel the timer and wait.
        _ = timer_f.cancel(io);
        return result;
    } else {
        // Timer fired first. Cancel the fetch and wait.
        _ = fetch_f.cancel(io);
        return null; // Timed out.
    }
}
```

### Cancellation

Cancellation is cooperative. When `cancel(io)` is called:

1. The `CANCELLED` flag is set on the task's packed atomic state.
2. The call blocks until the task completes (it is not fire-and-forget).
3. The task checks the cancellation flag at yield points (when `poll()` returns `.pending`).
4. If not shielded (`shield_depth == 0`), the task is dropped.

Tasks can protect critical sections from cancellation using the shield mechanism in the task header.

***

## Combinators

Higher-level functions for coordinating multiple tasks.

### `joinAll`

```zig
pub fn joinAll(io: volt.Io, futures: anytype) JoinAllResult(@TypeOf(futures))
```

Wait for all tasks to complete. Returns a tuple of results. Fails on the first error (remaining tasks continue running).

```zig
const user_f = try io.@"async"(fetchUser, .{id});
const posts_f = try io.@"async"(fetchPosts, .{id});

const user, const posts = try io.joinAll(.{ user_f, posts_f });
```

The argument must be a tuple of `volt.Future` values. The return type is a tuple of the corresponding output types, wrapped in an error union.

#### Example: Scatter-Gather from Multiple Services

Query three independent microservices concurrently, then combine the
results into a unified response. This is the classic scatter-gather
pattern.

```zig
const volt = @import("volt");

const Inventory = struct { in_stock: bool, quantity: u32 };
const Pricing = struct { price_cents: u64, currency: []const u8 };
const Reviews = struct { avg_rating: f32, count: u32 };

fn fetchInventory(product_id: u64) !Inventory {
    // GET /inventory-service/products/{id}
    _ = product_id;
    return .{ .in_stock = true, .quantity = 42 };
}

fn fetchPricing(product_id: u64) !Pricing {
    // GET /pricing-service/products/{id}
    _ = product_id;
    return .{ .price_cents = 1999, .currency = "USD" };
}

fn fetchReviews(product_id: u64) !Reviews {
    // GET /review-service/products/{id}
    _ = product_id;
    return .{ .avg_rating = 4.7, .count = 328 };
}

const ProductPage = struct {
    inventory: Inventory,
    pricing: Pricing,
    reviews: Reviews,
};

fn buildProductPage(io: volt.Io, product_id: u64) !ProductPage {
    // Scatter: spawn all three requests concurrently.
    const inv_f = try io.@"async"(fetchInventory, .{product_id});
    const price_f = try io.@"async"(fetchPricing, .{product_id});
    const review_f = try io.@"async"(fetchReviews, .{product_id});

    // Gather: wait for all three. If ANY service returns an error,
    // joinAll propagates it immediately. The remaining tasks keep
    // running in the background (they are not cancelled).
    const inv, const price, const reviews = try io.joinAll(.{
        inv_f,
        price_f,
        review_f,
    });

    // All three succeeded. Combine into a unified response.
    return ProductPage{
        .inventory = inv,
        .pricing = price,
        .reviews = reviews,
    };
}
```

### `tryJoinAll`

```zig
pub fn tryJoinAll(io: volt.Io, futures: anytype) TryJoinAllResult(@TypeOf(futures))
```

Wait for all tasks, collecting both successes and errors. Does not fail early -- waits for every task.

```zig
const results = io.tryJoinAll(.{ f1, f2, f3 });

for (results) |result| {
    switch (result) {
        .ok => |value| handleSuccess(value),
        .err => |e| handleError(e),
    }
}
```

#### Example: Partial Failure Handling

When you need results from as many sources as possible, even if some
fail. Unlike `joinAll`, this does not short-circuit on the first error.

```zig
fn fetchFromAllRegions(io: volt.Io, key: []const u8) !void {
    const us_f = try io.@"async"(fetchFromRegion, .{ "us-east-1", key });
    const eu_f = try io.@"async"(fetchFromRegion, .{ "eu-west-1", key });
    const ap_f = try io.@"async"(fetchFromRegion, .{ "ap-south-1", key });

    // Wait for ALL three, even if some fail.
    const us_result, const eu_result, const ap_result = io.tryJoinAll(.{
        us_f,
        eu_f,
        ap_f,
    });

    // Count successes and use the first available result.
    var success_count: u32 = 0;
    var first_value: ?[]const u8 = null;

    inline for (.{ us_result, eu_result, ap_result }) |result| {
        switch (result) {
            .ok => |value| {
                success_count += 1;
                if (first_value == null) first_value = value;
            },
            .err => |e| {
                std.log.warn("Region failed: {}", .{e});
            },
        }
    }

    if (first_value) |value| {
        std.debug.print("Got value from {d}/3 regions: {s}\n", .{
            success_count,
            value,
        });
    } else {
        return error.AllRegionsFailed;
    }
}

fn fetchFromRegion(region: []const u8, key: []const u8) ![]const u8 {
    _ = region;
    _ = key;
    return "cached-value";
}
```

### `TaskResult`

```zig
pub fn TaskResult(comptime T: type, comptime E: type) type {
    return union(enum) {
        ok: T,
        err: E,

        pub fn unwrap(self) !T;
        pub fn isOk(self) bool;
        pub fn isErr(self) bool;
    };
}
```

Individual result type used by `tryJoinAll`.

### `race`

```zig
pub fn race(io: volt.Io, futures: anytype) RaceResult(@TypeOf(futures))
```

First task to complete wins. Remaining tasks are cancelled.

```zig
const fast_f = try io.@"async"(fetchFromCache, .{key});
const slow_f = try io.@"async"(fetchFromDb, .{key});

// Whichever finishes first wins; the other is cancelled
const result = try io.race(.{ fast_f, slow_f });
```

#### Example: Cache vs. Database Race

A common latency optimization: try the cache and the database
simultaneously. Whichever responds first is used. The slower path
is cancelled, freeing its resources.

```zig
const volt = @import("volt");

const UserProfile = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
};

/// Fast path: check the in-memory cache. Returns immediately if the
/// key is present, or an error if there is a cache miss.
fn fetchFromCache(user_id: u64) !UserProfile {
    _ = user_id;
    // Simulate a cache miss 50% of the time.
    // In production: look up user_id in a hash map or Redis client.
    return error.CacheMiss;
}

/// Slow path: full database query. Always succeeds (eventually), but
/// has higher latency due to network round-trip and disk I/O.
fn fetchFromDatabase(user_id: u64) !UserProfile {
    _ = user_id;
    // Simulate database latency.
    // In production: send SQL query over a TCP connection.
    return UserProfile{
        .id = 1,
        .name = "Alice",
        .email = "alice@example.com",
    };
}

fn getUser(io: volt.Io, user_id: u64) !UserProfile {
    // Both run concurrently. The race combinator returns the result
    // of whichever task completes first and cancels the other.
    //
    // Common outcomes:
    //   - Cache hit:  cache returns in ~1ms, DB cancelled.
    //   - Cache miss: cache returns error.CacheMiss immediately,
    //                 race treats this as an error and it propagates.
    //
    // To handle cache misses gracefully, wrap the cache in a function
    // that returns a nullable instead of an error:
    const cache_f = try io.@"async"(fetchFromCacheOrNull, .{user_id});
    const db_f = try io.@"async"(fetchFromDatabase, .{user_id});

    const result = try io.race(.{ cache_f, db_f });
    return result;
}

/// Wrapper that converts cache miss errors into null so race()
/// does not short-circuit on a miss.
fn fetchFromCacheOrNull(user_id: u64) !UserProfile {
    return fetchFromCache(user_id) catch |err| switch (err) {
        error.CacheMiss => {
            // Not an error -- just not in cache. Let the database win.
            // Return a sentinel that we filter out, or re-design with
            // select() for more control. For simplicity, fall through
            // to the database by returning the DB result directly.
            return fetchFromDatabase(user_id);
        },
        else => return err,
    };
}
```

### `select`

```zig
pub fn select(io: volt.Io, futures: anytype) SelectResult(@TypeOf(futures))
```

First task to complete is returned. Remaining tasks keep running (not cancelled).

```zig
const f1 = try io.@"async"(watchForUpdates, .{});
const f2 = try io.@"async"(watchForSignals, .{});

const first_result = try io.select(.{ f1, f2 });
// Other task continues running
```

#### Example: Multi-Source Event Loop

Listen for events from multiple sources. `select` returns the first
event that fires, along with the index indicating which source produced
it. The other tasks remain active for future selects.

```zig
fn eventLoop(io: volt.Io) !void {
    const config_f = try io.@"async"(watchConfigChanges, .{});
    const health_f = try io.@"async"(watchHealthChecks, .{});
    const metric_f = try io.@"async"(watchMetricAlerts, .{});

    // select returns (result, winner_index).
    const event, const source = try io.select(.{
        config_f,
        health_f,
        metric_f,
    });

    switch (source) {
        0 => std.debug.print("Config changed: {s}\n", .{event}),
        1 => std.debug.print("Health check: {s}\n", .{event}),
        2 => std.debug.print("Metric alert: {s}\n", .{event}),
        else => unreachable,
    }

    // The other two tasks are still running. You can select on
    // them again, or cancel them when shutting down.
}
```

***

## Cooperative Scheduling

### Task Budget

Each worker enforces a budget of 128 polls per tick. After the budget is exhausted, the worker performs maintenance (check global queue, process I/O, fire timers) before resuming task execution.

This prevents a single task from monopolizing a worker thread.

### LIFO Slot

When a task wakes another task on the same worker, the woken task goes into the LIFO slot and runs next. This provides cache locality for ping-pong patterns (mutex lock/unlock, channel send/recv).

The LIFO slot is capped at `MAX_LIFO_POLLS_PER_TICK = 3` to prevent starvation.

### Task Lifecycle

```
IDLE ──spawn──> SCHEDULED ──run──> RUNNING ──yield──> IDLE
  |                                    |                |
  |                                    v                |
  └────────────────────────────── COMPLETE <────────────┘
```

* **IDLE**: Not scheduled. Waiting for I/O, timer, or sync primitive.
* **SCHEDULED**: In a run queue. Waiting for a worker to pick it up.
* **RUNNING**: Currently executing on a worker thread.
* **COMPLETE**: Finished. Result is available via `Future(T)`.

### Wakeup Protocol

When a waker fires (e.g., lock released, channel value available):

* If the task is **IDLE**: transition to SCHEDULED and queue it.
* If the task is **RUNNING** or **SCHEDULED**: set the `notified` flag.

After `poll()` returns `.pending`, the worker atomically transitions RUNNING to IDLE and checks the `notified` flag. If set, the task is immediately rescheduled. This prevents lost wakeups when a waker fires while the task is still running.

***

## Task Memory

Tasks are stackless futures (~256-512 bytes per task). This is fundamentally different from stackful coroutines (16-64KB per task) or OS threads (~8MB stack).

| Approach | Per-Task Memory | Max Concurrent |
|----------|----------------|----------------|
| OS Threads | ~8MB | Thousands |
| Stackful Coroutines | 16-64KB | Tens of thousands |
| Volt Tasks (Stackless) | 256-512 bytes | Millions |

The task header (`Header`) is 64 bytes and contains:

* Packed atomic state (64-bit: lifecycle, flags, ref count, shield depth)
* Intrusive list pointers (next/prev)
* VTable for type-erased operations (poll, drop, schedule, reschedule)

***

## Structured Concurrency Patterns

The primitives above compose into higher-level patterns for real applications.
This section shows three common designs.

### Supervised Workers

Spawn N worker tasks, monitor them, and restart any that fail. This is
useful for long-running server processes that must stay alive.

```zig
const std = @import("std");
const volt = @import("volt");
const channel = volt.channel;

const WorkerEvent = union(enum) {
    started: u32,    // worker_id
    failed: u32,     // worker_id
    shutdown: void,
};

/// Each worker processes jobs from a shared channel. If it encounters
/// a fatal error, it reports back to the supervisor via the event channel.
fn worker(
    id: u32,
    jobs: *channel.Channel(Job),
    events: *channel.Channel(WorkerEvent),
) void {
    _ = events.trySend(.{ .started = id });

    while (true) {
        const recv = jobs.tryRecv();
        switch (recv) {
            .value => |job| {
                processJob(job) catch {
                    // Report failure so the supervisor can restart us.
                    _ = events.trySend(.{ .failed = id });
                    return;
                };
            },
            .empty => continue,
            .closed => return,
        }
    }
}

fn processJob(job: Job) !void {
    _ = job;
}

const Job = struct { payload: []const u8 };

/// The supervisor spawns workers and restarts them on failure.
/// It runs until a shutdown event is received.
fn supervisor(io: volt.Io, worker_count: u32) !void {
    var allocator = std.heap.page_allocator;

    var jobs = try channel.bounded(Job, allocator, 256);
    defer jobs.deinit();

    var events = try channel.bounded(WorkerEvent, allocator, 64);
    defer events.deinit();

    // Spawn initial workers. Store futures so we can track them.
    var futures: [16]?volt.Future(void) = .{null} ** 16;
    for (0..worker_count) |i| {
        futures[i] = io.@"async"(worker, .{
            @as(u32, @intCast(i)),
            &jobs,
            &events,
        }) catch null;
    }

    // Monitor loop: watch for failures and restart.
    while (true) {
        const event = events.tryRecv();
        switch (event) {
            .value => |ev| switch (ev) {
                .failed => |id| {
                    std.log.warn("Worker {d} failed, restarting", .{id});
                    futures[id] = io.@"async"(worker, .{
                        id,
                        &jobs,
                        &events,
                    }) catch null;
                },
                .shutdown => {
                    // Cancel all workers and exit.
                    for (&futures) |*f| {
                        if (f.*) |future| {
                            _ = future.cancel(io);
                            f.* = null;
                        }
                    }
                    return;
                },
                .started => |id| {
                    std.log.info("Worker {d} started", .{id});
                },
            },
            .empty => continue,
            .closed => return,
        }
    }
}
```

### Task Tree with Cancellation Propagation

Spawn a parent task that creates children. When the parent is cancelled,
it propagates cancellation to all children before exiting. This ensures
no orphaned tasks linger after a timeout or shutdown.

```zig
const volt = @import("volt");

fn parentTask(io: volt.Io, urls: []const []const u8) ![]const u8 {
    // Spawn a child task for each URL.
    var futures: [8]?volt.Future([]const u8) = .{null} ** 8;
    const count = @min(urls.len, futures.len);

    for (urls[0..count], 0..) |url, i| {
        futures[i] = io.@"async"(childFetch, .{url}) catch null;
    }

    // Wait for all children. If the parent is cancelled while
    // waiting (e.g., by a timeout), we land in the errdefer
    // and cancel all children.
    errdefer {
        for (&futures) |*f| {
            if (f.*) |future| _ = future.cancel(io);
        }
    }

    // Collect results.
    var best: ?[]const u8 = null;
    for (futures[0..count]) |*f| {
        if (f.*) |future| {
            const data = future.@"await"(io);
            if (best == null or data.len > best.?.len) {
                best = data;
            }
        }
    }

    return best orelse error.AllChildrenFailed;
}

fn childFetch(url: []const u8) ![]const u8 {
    _ = url;
    return "response-body";
}

/// Usage: cancel the parent and all children are cleaned up.
fn runWithTimeout(io: volt.Io) !void {
    const parent_f = try io.@"async"(parentTask, .{
        io,
        &[_][]const u8{ "/api/a", "/api/b", "/api/c" },
    });

    // Set up a timeout. If the parent does not finish in 5 seconds,
    // cancel it. The errdefer inside parentTask will cascade the
    // cancellation to all children.
    const timer_f = try io.@"async"(struct {
        fn wait(io_inner: volt.Io) void {
            io_inner.sleep(volt.time.Duration.fromSecs(5));
        }
    }.wait, .{io});

    const _, const winner = try io.select(.{ parent_f, timer_f });

    if (winner == 1) {
        // Timer won -- parent timed out. Cancel it and wait.
        _ = parent_f.cancel(io);
        std.debug.print("Parent timed out, all children cancelled\n", .{});
    } else {
        _ = timer_f.cancel(io);
    }
}
```

### Pipeline with Channels Between Stages

Build a multi-stage processing pipeline where each stage is a task
connected to the next by a channel. Data flows from producer through
transformers to consumer. Closing the input channel causes each stage
to drain and shut down in order.

```zig
const std = @import("std");
const volt = @import("volt");
const channel = volt.channel;

const RawEvent = struct {
    timestamp: u64,
    payload: []const u8,
};

const ParsedEvent = struct {
    timestamp: u64,
    event_type: []const u8,
    value: f64,
};

const EnrichedEvent = struct {
    parsed: ParsedEvent,
    region: []const u8,
    alert: bool,
};

/// Stage 1: Parse raw events and forward to the next stage.
fn parseStage(
    input: *channel.Channel(RawEvent),
    output: *channel.Channel(ParsedEvent),
) void {
    while (true) {
        const recv = input.tryRecv();
        switch (recv) {
            .value => |raw| {
                // Parse the raw payload into a structured event.
                const parsed = ParsedEvent{
                    .timestamp = raw.timestamp,
                    .event_type = "metric",
                    .value = std.fmt.parseFloat(f64, raw.payload) catch 0.0,
                };
                _ = output.trySend(parsed);
            },
            .empty => continue,
            .closed => {
                // Upstream closed. Close our output to signal
                // the next stage.
                output.close();
                return;
            },
        }
    }
}

/// Stage 2: Enrich parsed events with metadata.
fn enrichStage(
    input: *channel.Channel(ParsedEvent),
    output: *channel.Channel(EnrichedEvent),
) void {
    while (true) {
        const recv = input.tryRecv();
        switch (recv) {
            .value => |parsed| {
                const enriched = EnrichedEvent{
                    .parsed = parsed,
                    .region = "us-east-1",
                    .alert = parsed.value > 100.0,
                };
                _ = output.trySend(enriched);
            },
            .empty => continue,
            .closed => {
                output.close();
                return;
            },
        }
    }
}

/// Stage 3: Consume enriched events (write to database, send alerts).
fn sinkStage(input: *channel.Channel(EnrichedEvent)) void {
    var count: u64 = 0;
    var alerts: u64 = 0;

    while (true) {
        const recv = input.tryRecv();
        switch (recv) {
            .value => |event| {
                count += 1;
                if (event.alert) alerts += 1;
            },
            .empty => continue,
            .closed => {
                std.debug.print(
                    "Pipeline complete: {d} events, {d} alerts\n",
                    .{ count, alerts },
                );
                return;
            },
        }
    }
}

/// Wire up the pipeline: producer -> parse -> enrich -> sink.
fn runPipeline(io: volt.Io) !void {
    var allocator = std.heap.page_allocator;

    // Create channels between stages. The capacity acts as
    // backpressure: if a downstream stage is slow, the upstream
    // stage blocks on trySend returning .full.
    var raw_ch = try channel.bounded(RawEvent, allocator, 128);
    defer raw_ch.deinit();

    var parsed_ch = try channel.bounded(ParsedEvent, allocator, 128);
    defer parsed_ch.deinit();

    var enriched_ch = try channel.bounded(EnrichedEvent, allocator, 128);
    defer enriched_ch.deinit();

    // Spawn each stage as a task.
    const parse_f = try io.@"async"(parseStage, .{ &raw_ch, &parsed_ch });
    const enrich_f = try io.@"async"(enrichStage, .{ &parsed_ch, &enriched_ch });
    const sink_f = try io.@"async"(sinkStage, .{&enriched_ch});

    // Feed data into the pipeline.
    for (0..1000) |i| {
        _ = raw_ch.trySend(RawEvent{
            .timestamp = i,
            .payload = "42.5",
        });
    }

    // Close the input channel to signal "no more data". This
    // cascades: parse closes parsed_ch, enrich closes enriched_ch,
    // and sink drains and exits.
    raw_ch.close();

    // Wait for all stages to finish draining.
    _ = try io.joinAll(.{ parse_f, enrich_f, sink_f });
}
```

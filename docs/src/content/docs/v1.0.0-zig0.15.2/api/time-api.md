---
title: Time API
description: Complete API reference for Duration, Instant, Sleep, Interval,
  Deadline, and timeout utilities in Volt.
slug: v1.0.0-zig0.15.2/api/time-api
---

Every networked application needs to deal with time: request timeouts, retry backoffs, periodic health checks, rate limiting, latency measurement, deadline propagation. The time module gives you the building blocks for all of these.

**Duration** and **Instant** are your everyday types -- how long something should take, and when something started. **Sleep**, **Interval**, and **Deadline** build on top of them for common async patterns. All timer operations integrate with the runtime's hierarchical timer wheel, so sleeping a task doesn't block an OS thread.

### At a Glance

```zig
const volt = @import("volt");
const Duration = volt.Duration;

// Measure how long something takes
const start = volt.Instant.now();
doExpensiveWork();
std.log.info("took {d}ms", .{start.elapsed().asMillis()});

// Duration arithmetic (all saturating, no overflow)
const timeout = Duration.fromSecs(5);
const doubled = timeout.mul(2);      // 10s
const with_margin = timeout.add(Duration.fromMillis(500)); // 5.5s

// Async sleep -- yields to scheduler, other tasks keep running
var sleep = volt.time.sleep(Duration.fromMillis(100));

// Periodic timer -- fires every 30 seconds
var ticker = volt.time.Interval.init(Duration.fromSecs(30));

// Deadline propagation -- pass through a multi-step pipeline
var dl = volt.time.Deadline.init(Duration.fromSecs(5));
if (dl.isExpired()) return error.TimedOut;
std.log.info("{d}ms remaining", .{dl.remaining().asMillis()});
```

Time primitives live under `volt.time`.

```zig
const volt = @import("volt");
const time = volt.time;
```

Common re-exports at the top level:

```zig
const Duration = volt.Duration;  // volt.time.Duration
const Instant = volt.Instant;    // volt.time.Instant
```

***

## Duration

A span of time with nanosecond precision. All arithmetic saturates (no overflow).

```zig
const Duration = time.Duration;
```

### Construction

| Method | Example |
|--------|---------|
| `fromNanos(n)` | `Duration.fromNanos(500)` |
| `fromMicros(n)` | `Duration.fromMicros(100)` |
| `fromMillis(n)` | `Duration.fromMillis(250)` |
| `fromSecs(n)` | `Duration.fromSecs(5)` |
| `fromSeconds(n)` | Alias for `fromSecs` |
| `fromMins(n)` | `Duration.fromMins(2)` |
| `fromMinutes(n)` | Alias for `fromMins` |
| `fromHours(n)` | `Duration.fromHours(1)` |
| `fromDays(n)` | `Duration.fromDays(7)` |

### Constants

```zig
Duration.ZERO  // 0 nanoseconds
Duration.MAX   // u64 max nanoseconds
```

### Conversion

| Method | Returns | Description |
|--------|---------|-------------|
| `asNanos()` | `u64` | Nanoseconds |
| `asMicros()` | `u64` | Microseconds (truncated) |
| `asMillis()` | `u64` | Milliseconds (truncated) |
| `asSecs()` | `u64` | Seconds (truncated) |
| `asSeconds()` | `u64` | Alias for `asSecs` |
| `asMins()` | `u64` | Minutes (truncated) |
| `asMinutes()` | `u64` | Alias for `asMins` |
| `asHours()` | `u64` | Hours (truncated) |
| `asDays()` | `u64` | Days (truncated) |

### Arithmetic

All operations use saturating arithmetic.

| Method | Signature | Description |
|--------|-----------|-------------|
| `add` | `fn add(self, other: Duration) Duration` | Saturating add |
| `sub` | `fn sub(self, other: Duration) Duration` | Saturating subtract (floors at zero) |
| `mul` | `fn mul(self, n: u64) Duration` | Saturating multiply |
| `div` | `fn div(self, n: u64) Duration` | Integer divide |

```zig
const timeout = Duration.fromSecs(5);
const doubled = timeout.mul(2);       // 10 seconds
const halved = timeout.div(2);        // 2.5 seconds
const combined = timeout.add(Duration.fromMillis(500)); // 5.5 seconds
```

#### Example: Retry with exponential backoff

Duration arithmetic makes it straightforward to build backoff strategies.
Each call to `mul(2)` doubles the delay, and `cmp` enforces a ceiling so
the delay never exceeds a reasonable bound.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;

const max_retries = 5;
const initial_delay = Duration.fromMillis(100);
const max_delay = Duration.fromSecs(10);

/// Retry an operation with exponential backoff.
/// The delay doubles after each failure: 100ms, 200ms, 400ms, 800ms, 1600ms.
fn retryWithBackoff(
    comptime fetchFn: fn ([]const u8) anyerror![]const u8,
    url: []const u8,
) ![]const u8 {
    var delay = initial_delay;

    for (0..max_retries) |attempt| {
        const result = fetchFn(url) catch |err| {
            if (attempt == max_retries - 1) return err;

            std.log.warn("attempt {d}/{d} failed: {}, retrying in {d}ms", .{
                attempt + 1,
                max_retries,
                err,
                delay.asMillis(),
            });

            // Wait before the next attempt.
            volt.time.blockingSleep(delay);

            // Double the delay, but cap at max_delay.
            delay = delay.mul(2);
            if (delay.cmp(max_delay) == .gt) {
                delay = max_delay;
            }

            continue;
        };
        return result;
    }
    unreachable;
}
```

### Comparison

| Method | Returns | Description |
|--------|---------|-------------|
| `cmp(other)` | `std.math.Order` | Compare two durations |
| `isZero()` | `bool` | Check if duration is zero |

### System Conversion

```zig
pub fn toTimespec(self: Duration) std.posix.timespec
```

Convert to a POSIX timespec for use with syscalls.

***

## Instant

A point in time using the monotonic clock. Not affected by system clock adjustments.

```zig
const Instant = time.Instant;
```

### Construction

```zig
const start = Instant.now();
```

Also available as a convenience:

```zig
const start = time.now();
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `elapsed()` | `fn elapsed(self) Duration` | Time since this instant |
| `durationSince(earlier)` | `fn durationSince(self, earlier: Instant) Duration` | Time between two instants |
| `add(duration)` | `fn add(self, d: Duration) Instant` | Add duration to instant |
| `sub(duration)` | `fn sub(self, d: Duration) Instant` | Subtract duration from instant |
| `isBefore(other)` | `fn isBefore(self, other: Instant) bool` | Ordering |
| `isAfter(other)` | `fn isAfter(self, other: Instant) bool` | Ordering |

```zig
const start = time.now();
doExpensiveWork();
const elapsed = start.elapsed();
std.log.info("Took {}ms", .{elapsed.asMillis()});
```

#### Example: Measuring request handling latency

Use `Instant` to measure end-to-end latency of a request handler. Because the
monotonic clock is unaffected by NTP adjustments or wall-clock jumps, it gives
stable measurements even on long-running servers.

```zig
const std = @import("std");
const volt = @import("volt");
const Instant = volt.Instant;

const LatencyStats = struct {
    total_ns: u64 = 0,
    count: u64 = 0,
    max_ns: u64 = 0,

    fn record(self: *LatencyStats, elapsed: volt.Duration) void {
        const ns = elapsed.asNanos();
        self.total_ns += ns;
        self.count += 1;
        if (ns > self.max_ns) self.max_ns = ns;
    }

    fn avgMillis(self: *const LatencyStats) u64 {
        if (self.count == 0) return 0;
        return (self.total_ns / self.count) / std.time.ns_per_ms;
    }
};

var stats: LatencyStats = .{};

fn handleRequest(request: *const Request, response: *Response) !void {
    const start = Instant.now();
    defer {
        const elapsed = start.elapsed();
        stats.record(elapsed);
        std.log.info("request {s} completed in {d}us", .{
            request.path,
            elapsed.asMicros(),
        });
    }

    // Simulate work: parse, query database, serialize response.
    try parseBody(request);
    const rows = try db.query(request.params);
    try response.writeJson(rows);
}
```

***

## Sleep

Async-aware sleep that integrates with the timer wheel.

```zig
const Sleep = time.Sleep;
```

### Construction

```zig
var sleep = Sleep.init(Duration.fromMillis(100));
```

Convenience functions:

```zig
var sleep = time.sleep(Duration.fromSecs(1));
var sleep = time.sleepUntil(some_instant);
```

### Waiter API

```zig
const SleepWaiter = time.SleepWaiter;

var waiter = SleepWaiter.init();
waiter.setWaker(@ptrCast(&ctx), wakeFn);

if (!sleep.wait(&waiter)) {
    // Yield to scheduler. Woken when duration elapses.
}
// waiter.isComplete() == true
```

### Blocking Sleep

For use outside async context (e.g., in `main()` before runtime starts):

```zig
time.blockingSleep(Duration.fromSecs(1));
```

This calls `std.Thread.sleep` internally and blocks the calling thread.

#### Example: Rate-limited connection handler

In a server that accepts connections, sleep can throttle the accept loop
when the system is under pressure. The sleep is async-aware, so other tasks
on the same runtime continue making progress while this one waits.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;

const max_connections = 1024;
var active_connections: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

fn acceptLoop(listener: *volt.net.TcpListener) !void {
    while (true) {
        const current = active_connections.load(.acquire);

        if (current >= max_connections) {
            // Back off when at capacity. This yields the task so the
            // runtime can service existing connections and free slots.
            std.log.warn("at connection limit ({d}), backing off 100ms", .{current});
            var pause = volt.time.sleep(Duration.fromMillis(100));
            var waiter = volt.time.SleepWaiter.init();
            if (!pause.wait(&waiter)) {
                // Yield to scheduler; woken after 100ms.
                return; // Re-polled by the runtime.
            }
            continue;
        }

        const conn = try listener.accept();
        _ = active_connections.fetchAdd(1, .release);
        // Hand off to a connection handler task.
        try spawnConnectionHandler(conn);
    }
}

fn spawnConnectionHandler(conn: volt.net.TcpStream) !void {
    // Handle connection, then decrement count on completion.
    defer _ = active_connections.fetchSub(1, .release);
    try handleConnection(conn);
}
```

### SleepWaiter Methods

| Method | Description |
|--------|-------------|
| `init()` | Create a new waiter |
| `setWaker(ctx, fn)` | Set waker callback |
| `isComplete()` | Check if sleep elapsed |
| `reset()` | Reset for reuse |

***

## Interval

Recurring timer that fires at regular intervals.

```zig
const Interval = time.Interval;
```

### Construction

```zig
var interval = Interval.init(Duration.fromMillis(100));
```

Convenience:

```zig
var interval = time.interval(Duration.fromSecs(1));
```

### Tick

```zig
var waiter = time.IntervalWaiter.init();
if (!interval.tick(&waiter)) {
    // Yield. Woken on next tick.
}
// Handle tick
```

### Missed Tick Behavior

```zig
const MissedTickBehavior = time.MissedTickBehavior;
```

| Variant | Behavior |
|---------|----------|
| `.burst` | Fire immediately for each missed tick (catch up) |
| `.skip` | Skip missed ticks, schedule from now |
| `.delay` | Next tick is one full interval from now |

```zig
interval.setMissedTickBehavior(.skip);
```

### Reset

```zig
interval.reset(); // Restart the interval from now
```

#### Example: Periodic upstream health check

Use an interval to check the health of an upstream service at regular
intervals. If a check takes longer than the period, `.skip` mode avoids
a burst of back-to-back checks that would hammer an already-struggling
upstream.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;
const Interval = volt.time.Interval;

const HealthStatus = enum { healthy, degraded, down };

const UpstreamChecker = struct {
    endpoint: []const u8,
    status: HealthStatus = .healthy,
    consecutive_failures: u32 = 0,

    /// Run the health check loop. Checks the upstream every 30 seconds.
    /// Uses .skip mode so a slow check does not cause a burst of retries.
    fn run(self: *UpstreamChecker) void {
        var tick_timer = Interval.init(Duration.fromSecs(30));
        tick_timer.setMissedTickBehavior(.skip);

        while (true) {
            var waiter = volt.time.IntervalWaiter.init();
            if (!tick_timer.tick(&waiter)) {
                // Yield to the scheduler until the next 30-second tick.
                return; // Will be re-polled by the runtime.
            }

            self.performCheck();
        }
    }

    fn performCheck(self: *UpstreamChecker) void {
        const start = volt.Instant.now();
        const reachable = pingEndpoint(self.endpoint);
        const latency = start.elapsed();

        if (!reachable) {
            self.consecutive_failures += 1;
            if (self.consecutive_failures >= 3) {
                self.status = .down;
                std.log.err("upstream {s} is DOWN after {d} failures", .{
                    self.endpoint,
                    self.consecutive_failures,
                });
            }
            return;
        }

        self.consecutive_failures = 0;

        // Mark degraded if latency exceeds 2 seconds.
        if (latency.cmp(Duration.fromSecs(2)) == .gt) {
            self.status = .degraded;
            std.log.warn("upstream {s} is degraded ({d}ms)", .{
                self.endpoint,
                latency.asMillis(),
            });
        } else {
            self.status = .healthy;
        }
    }
};
```

### IntervalWaiter Methods

| Method | Description |
|--------|-------------|
| `init()` | Create a new waiter |
| `setWaker(ctx, fn)` | Set waker callback |
| `isComplete()` | Check if tick fired |
| `reset()` | Reset for reuse |

***

## Deadline

Timeout tracking for operations.

```zig
const Deadline = time.Deadline;
```

### Construction

```zig
var deadline = Deadline.init(Duration.fromSecs(5));
```

Convenience:

```zig
var deadline = time.deadline(Duration.fromMillis(500));
```

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `isExpired()` | `bool` | Whether the deadline has passed |
| `remaining()` | `Duration` | Time until deadline (zero if expired) |
| `reset(duration)` | `void` | Reset with a new duration from now |

```zig
var deadline = time.deadline(Duration.fromSecs(5));

while (!deadline.isExpired()) {
    if (try doSomeWork()) break;
}

if (deadline.isExpired()) {
    return error.TimedOut;
}
```

#### Example: Deadline propagation through a multi-step pipeline

When an operation consists of several sequential steps, pass a single
`Deadline` through all of them. Each step can check the remaining budget
and fail early rather than starting work it cannot finish. This mirrors
the pattern of Go's `context.Context` deadline propagation.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;
const Deadline = volt.time.Deadline;
const TimeoutError = volt.time.TimeoutError;

const Order = struct {
    id: u64,
    items: []const Item,
};
const Item = struct { sku: []const u8, qty: u32 };
const Receipt = struct { order_id: u64, total_cents: u64 };

/// Process an order end-to-end within the given deadline.
/// The deadline is checked before each step. If any step would start
/// with zero time remaining, we return TimedOut immediately instead of
/// risking a partial operation.
fn processOrder(order: *const Order, dl: *Deadline) (TimeoutError || anyerror)!Receipt {
    // Step 1: Validate inventory. Give up if less than 1s remains --
    // the database round-trip alone takes ~500ms under load.
    try dl.check();
    if (dl.remaining().cmp(Duration.fromSecs(1)) == .lt) {
        std.log.warn("order {d}: not enough time for inventory check", .{order.id});
        return error.TimedOut;
    }
    try checkInventory(order.items);

    // Step 2: Charge payment.
    try dl.check();
    const charge = try chargePayment(order);

    // Step 3: Send confirmation. This is best-effort -- if the deadline
    // is nearly up we still return success since payment already went through.
    if (!dl.isExpired()) {
        sendConfirmation(order) catch |err| {
            std.log.warn("order {d}: confirmation failed: {}, will retry later", .{
                order.id, err,
            });
        };
    }

    return Receipt{ .order_id = order.id, .total_cents = charge };
}

/// Top-level handler. Creates the deadline and passes it down.
fn handleOrderRequest(order: *const Order) !Receipt {
    // The entire order must complete within 5 seconds.
    var dl = Deadline.init(Duration.fromSecs(5));
    return processOrder(order, &dl);
}
```

### Waiter API

```zig
const DeadlineWaiter = time.DeadlineWaiter;

var waiter = DeadlineWaiter.init();
waiter.setWaker(@ptrCast(&ctx), wakeFn);
// Register with timer driver for automatic wakeup on expiry
```

***

## TimeoutError

```zig
pub const TimeoutError = error{ TimedOut };
```

Standard error type for timeout conditions. Used by `Deadline` and the `Timeout` combinator in `volt.async_ops`.

***

## Timer Driver

For runtime implementors. The `TimerDriver` manages the timer wheel and fires expired timers.

```zig
const TimerDriver = time.TimerDriver;
const TimerHandle = time.TimerHandle;
```

| Function | Description |
|----------|-------------|
| `time.getDriver()` | Get the thread-local timer driver |
| `time.setDriver(driver)` | Set the thread-local timer driver |

The timer driver is initialized automatically by the runtime. User code typically does not interact with it directly.

***

## Common Patterns

### Retry with jittered backoff

Plain exponential backoff can cause "thundering herd" problems when many
clients retry at the same instant. Adding random jitter spreads retries
across time, reducing contention on the upstream service.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;

/// Retry a fallible operation with exponential backoff and full jitter.
/// The delay for attempt N is a random value in [0, min(max_delay, base * 2^N)].
fn retryWithJitter(
    comptime T: type,
    comptime op: fn () anyerror!T,
    max_retries: u32,
) !T {
    const base_delay = Duration.fromMillis(200);
    const max_delay = Duration.fromSecs(30);
    var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    const random = prng.random();

    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        if (op()) |result| {
            return result;
        } else |err| {
            if (attempt >= max_retries) return err;

            // Compute the ceiling: base * 2^attempt, capped at max_delay.
            const shift: u6 = @intCast(@min(attempt, 20));
            const ceiling_ms = @min(
                max_delay.asMillis(),
                base_delay.asMillis() *| (@as(u64, 1) << shift),
            );

            // Full jitter: pick a random value in [0, ceiling].
            const jittered_ms = if (ceiling_ms == 0) 0 else random.intRangeAtMost(u64, 0, ceiling_ms);
            const delay = Duration.fromMillis(jittered_ms);

            std.log.warn("attempt {d}/{d} failed: {}, retrying in {d}ms", .{
                attempt + 1,
                max_retries,
                err,
                delay.asMillis(),
            });

            volt.time.blockingSleep(delay);
        }
    }
}
```

### Periodic background task

A background task that runs on a fixed schedule until a shutdown signal
is received. The interval resets after each iteration, so the period
measures start-to-start rather than end-to-start.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;
const Interval = volt.time.Interval;

const MetricsReporter = struct {
    endpoint: []const u8,
    shutdown: *std.atomic.Value(bool),

    /// Flush metrics to the reporting endpoint every 60 seconds.
    fn run(self: *MetricsReporter) !void {
        // The first tick fires after one full period (not immediately).
        var ticker = Interval.init(Duration.fromSecs(60));

        // Use .delay mode: if a flush takes 5 seconds, the next one
        // starts 60 seconds after the slow flush finishes. This avoids
        // piling up flushes when the endpoint is slow.
        ticker.setMissedTickBehavior(.delay);

        while (!self.shutdown.load(.acquire)) {
            var waiter = volt.time.IntervalWaiter.init();
            if (!ticker.tick(&waiter)) {
                // Yield to scheduler. The runtime will re-poll this
                // task when the waiter fires.
                return;
            }

            self.flush() catch |err| {
                // Log and continue. A single failed flush should not
                // stop the reporter. Metrics are best-effort.
                std.log.err("metrics flush to {s} failed: {}", .{
                    self.endpoint, err,
                });
            };
        }

        // Final flush on shutdown so we do not lose the last window.
        self.flush() catch {};
    }

    fn flush(self: *MetricsReporter) !void {
        const start = volt.Instant.now();
        try sendMetrics(self.endpoint, collectMetrics());
        std.log.info("metrics flushed in {d}ms", .{start.elapsed().asMillis()});
    }
};
```

### Operation timeout with fallback

When an operation might be slow, use a `Deadline` to enforce a timeout and
fall back to a cached or default value. This keeps the caller from
blocking indefinitely on a degraded dependency.

```zig
const std = @import("std");
const volt = @import("volt");
const Duration = volt.Duration;
const Deadline = volt.time.Deadline;

const PricingInfo = struct {
    price_cents: u64,
    currency: []const u8,
    is_stale: bool,
};

/// Fetch live pricing, falling back to a cached price if the remote
/// service does not respond within the deadline.
fn getPricing(product_id: []const u8, timeout: Duration) PricingInfo {
    var dl = Deadline.init(timeout);

    // Attempt the live fetch. If it finishes before the deadline, return it.
    if (fetchLivePrice(product_id)) |live| {
        if (!dl.isExpired()) {
            return .{
                .price_cents = live.price_cents,
                .currency = live.currency,
                .is_stale = false,
            };
        }
        // Even though we got a result, the deadline expired during the call.
        // Log a warning -- the caller may want to know latency is too high.
        std.log.warn("live pricing for {s} arrived after deadline", .{product_id});
    } else |err| {
        std.log.warn("live pricing for {s} failed: {}", .{ product_id, err });
    }

    // Fall back to cache. If the cache is also empty, return a safe default.
    if (getCachedPrice(product_id)) |cached| {
        return .{
            .price_cents = cached.price_cents,
            .currency = cached.currency,
            .is_stale = true,
        };
    }

    std.log.err("no cached price for {s}, using fallback", .{product_id});
    return .{
        .price_cents = 0,
        .currency = "USD",
        .is_stale = true,
    };
}
```

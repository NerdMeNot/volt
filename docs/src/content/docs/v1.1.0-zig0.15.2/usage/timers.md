---
title: Timers
description: Async sleep, timeouts, and recurring intervals in Volt.
---

The `time` module provides duration/instant types and three async timer primitives: `Sleep`, `Deadline`, and `Interval`.

## Duration and Instant

### Duration

A `Duration` represents a span of time with nanosecond precision. All arithmetic saturates instead of overflowing.

```zig
const volt = @import("volt");
const Duration = volt.time.Duration;

// Creation
const d1 = Duration.fromNanos(500);
const d2 = Duration.fromMicros(100);
const d3 = Duration.fromMillis(250);
const d4 = Duration.fromSecs(5);
const d5 = Duration.fromMins(2);
const d6 = Duration.fromHours(1);
const d7 = Duration.fromDays(7);

// Conversion
d4.asNanos();   // 5_000_000_000
d4.asMicros();  // 5_000_000
d4.asMillis();  // 5_000
d4.asSecs();    // 5

// Arithmetic (saturating)
const sum = d3.add(d4);       // 5.25 seconds
const diff = d4.sub(d3);      // 4.75 seconds
const scaled = d3.mul(4);     // 1 second
const halved = d4.div(2);     // 2.5 seconds

// Comparison
const order = d3.cmp(d4);     // .lt
const is_zero = Duration.ZERO.isZero(); // true

// Constants
_ = Duration.ZERO;  // 0ns
_ = Duration.MAX;   // u64 max nanoseconds

// Readable aliases
const timeout = Duration.fromSeconds(30);
const period = Duration.fromMinutes(5);
```

### Instant

An `Instant` represents a point on the monotonic clock, suitable for measuring elapsed time:

```zig
const Instant = volt.time.Instant;

const start = Instant.now();

// ... do work ...

const elapsed = start.elapsed(); // Duration
std.debug.print("Took {d}ms\n", .{elapsed.asMillis()});

// Comparisons
const a = Instant.now();
const b = a.add(Duration.fromSecs(1));
a.isBefore(b); // true
b.isAfter(a);  // true

// Difference between instants
const diff = b.durationSince(a); // ~1 second
```

---

## Sleep

`Sleep` suspends execution for a specified duration. It uses the waiter pattern consistent with all other Volt primitives.

### Creation

```zig
const time = volt.time;

// Sleep for a duration
var sleep = time.Sleep.init(Duration.fromMillis(100));

// Sleep until a specific instant
var sleep2 = time.Sleep.initUntil(Instant.now().add(Duration.fromSecs(5)));
```

### Convenience constructors

```zig
var sleep = time.sleep(Duration.fromMillis(500));
var sleep2 = time.sleepUntil(deadline_instant);
```

### Waiter API

```zig
var waiter = time.SleepWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

if (!sleep.wait(&waiter)) {
    // Sleep has not elapsed yet. Yield to scheduler.
    // Waiter will be woken when the deadline passes.
}
// waiter.isComplete() is true -- sleep is done.
```

### Polling (non-blocking check)

```zig
if (sleep.tryWait()) {
    // Duration has elapsed
} else {
    // Still waiting
}
```

### Cancellation

```zig
sleep.cancelWait(&waiter);
```

### Blocking sleep

For non-async contexts (tests, initialization code):

```zig
time.blockingSleep(Duration.fromMillis(100));
```

This calls `std.Thread.sleep` under the hood and blocks the current OS thread.

---

## Deadline / Timeout

A `Deadline` tracks a point in time after which an operation should abort. Use it to implement timeouts on any async operation.

### Creation

```zig
const time = volt.time;

// Expire 5 seconds from now
var deadline = time.Deadline.init(Duration.fromSecs(5));

// Or use the convenience function
var deadline2 = time.deadline(Duration.fromSecs(5));
```

### Checking expiration

```zig
if (deadline.isExpired()) {
    return error.TimedOut;
}

// Remaining time
const remaining = deadline.remaining(); // Duration (zero if expired)
```

### Waiter API

```zig
var waiter = time.DeadlineWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

if (!deadline.wait(&waiter)) {
    // Deadline has not expired yet. Yield to scheduler.
    // Woken when deadline expires.
}
// waiter.isComplete() is true -- deadline has passed.
```

### Timeout pattern

Use a `Deadline` with an async task to implement timeouts on any operation:

```zig
// Set a 5 second deadline
var deadline = time.Deadline.init(Duration.fromSecs(5));

// Spawn the async operation
var read_f = try io.@"async"(readFromStream, .{stream, &buf});

// Check if the deadline has expired before awaiting
if (deadline.isExpired()) {
    return error.TimedOut;
}

const result = read_f.@"await"(io);
handleResult(result);
```

### Pattern: operation with retry and timeout

```zig
fn fetchWithTimeout(url: []const u8) !Response {
    var deadline = volt.time.Deadline.init(Duration.fromSecs(30));

    var attempts: usize = 0;
    while (attempts < 3) : (attempts += 1) {
        if (deadline.isExpired()) {
            return error.TimedOut;
        }

        const result = tryFetch(url) catch |err| {
            if (err == error.ConnectionRefused) {
                // Wait before retry, but respect the overall deadline
                const backoff = Duration.fromMillis(100).mul(attempts + 1);
                const remaining = deadline.remaining();
                const wait_time = if (backoff.asNanos() < remaining.asNanos()) backoff else remaining;
                volt.time.blockingSleep(wait_time);
                continue;
            }
            return err;
        };

        return result;
    }

    return error.MaxRetriesExceeded;
}
```

---

## Interval

An `Interval` fires repeatedly at a fixed period. It compensates for drift so that ticks stay aligned to the original schedule.

### Creation

```zig
const time = volt.time;

// Tick every 100ms
var interval = time.Interval.init(Duration.fromMillis(100));

// Or use the convenience function
var interval2 = time.interval(Duration.fromSecs(1));
```

### Tick loop

```zig
while (running) {
    var waiter = time.IntervalWaiter.init();
    waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

    if (!interval.tick(&waiter)) {
        // Next tick has not arrived. Yield to scheduler.
    }
    // Tick fired -- do periodic work
    collectMetrics();
    reportHealth();
}
```

### Non-blocking tick

```zig
if (interval.tryTick()) {
    // A tick period has elapsed since the last tick
    doPeriodicWork();
}
```

### Missed tick behavior

When processing takes longer than the interval period, the `MissedTickBehavior` controls what happens:

| Behavior | Description |
|----------|-------------|
| `burst` | Fire immediately for each missed tick (catch up) |
| `skip` | Skip missed ticks and schedule from now |
| `delay` | Delay the next tick from now (default) |

```zig
var interval = time.Interval.init(Duration.fromMillis(100));
interval.missed_tick_behavior = .skip; // Skip missed ticks
```

### Pattern: periodic health check

```zig
fn healthCheckLoop() void {
    var interval = volt.time.Interval.init(Duration.fromSecs(30));

    while (running) {
        var waiter = volt.time.IntervalWaiter.init();
        if (!interval.tick(&waiter)) {
            // Yield to scheduler
        }

        const health = checkSystemHealth();
        if (!health.ok) {
            alertOps(health);
        }
        reportMetrics(health);
    }
}
```

---

## Timer driver integration

The timer primitives can integrate with the runtime's `TimerDriver` for event-loop-driven wakeups instead of polling. The timer driver uses a hierarchical timer wheel (similar to Tokio's) for efficient scheduling of thousands of concurrent timers.

```zig
const driver = volt.time.getDriver();
if (driver) |d| {
    var entry: volt.time.TimerHandle = undefined;
    try d.register(&entry, deadline, &waiter);
    // Timer will fire automatically through the event loop
}
```

In most cases you do not need to interact with the timer driver directly -- the `Sleep`, `Deadline`, and `Interval` types handle registration automatically when running inside the Volt runtime.

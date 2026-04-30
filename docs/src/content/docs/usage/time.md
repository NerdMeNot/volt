---
title: Time
description: sleep, withTimeout, Interval, Duration, Instant — handling time inside coroutines.
---

Volt's time API has two layers:

- **Value types** (`volt.Duration`, `volt.Instant`) — model-agnostic
  measurements. Usable anywhere, including outside the runtime.
- **Coroutine-aware operations** (`volt.sleep`, `volt.withTimeout`,
  `volt.Interval`) — suspend the calling coroutine until a deadline.

## Duration and Instant

```zig
const d = volt.Duration.fromMillis(50);          // 50ms
const d2 = volt.Duration.fromSecs(2);             // 2 seconds
const total = d.add(d2);                          // 2.05s

const start = volt.Instant.now();
// ... do work ...
const elapsed = start.elapsed();
std.debug.print("took {} ms\n", .{ elapsed.asMillis() });
```

| Method | What it returns |
|---|---|
| `Duration.fromNanos(n)` / `fromMicros` / `fromMillis` / `fromSecs` / `fromMins` / `fromHours` / `fromDays` | construct a Duration |
| `d.asNanos()` / `asMillis()` / `asSecs()` / etc. | extract |
| `d.add(other)` / `sub(other)` / `mul(n)` / `div(n)` | arithmetic |
| `Instant.now()` | current monotonic timestamp |
| `inst.elapsed()` | Duration since the instant |
| `inst.add(d)` / `sub(d)` | Instant arithmetic |
| `inst.isBefore(other)` / `isAfter(other)` | comparison |

Internally, `Instant` reads `clock_gettime(CLOCK_MONOTONIC)` on
POSIX or `QueryPerformanceCounter` on Windows. Monotonic — never
goes backwards across NTP adjustments.

## sleep

```zig
try volt.sleep(volt.Duration.fromMillis(50));
```

Suspends the calling coroutine for at least the given duration. The
reactor wakes it via a kqueue `EVFILT_TIMER` (Darwin), epoll
`timerfd` (Linux), io_uring `TIMEOUT` (when using io_uring), or
`CreateTimerQueueTimer` (Windows).

`sleep` is cancellable — a cancelled coroutine surfaces
`error.Cancelled` from sleep immediately. This is how
`volt.withTimeout` works.

## withTimeout — race work against a deadline

```zig
const result = volt.withTimeout(
    volt.Duration.fromSecs(2),
    fetchUser,
    .{user_id},
);

const user = result catch |err| switch (err) {
    error.Timeout => return defaultUser(),
    else => return err,
};
```

`volt.withTimeout(duration, fn, args)`:

1. Spawns `fn(args)` as a child coroutine.
2. Spawns a watcher that sleeps for `duration`.
3. Whichever finishes first wins; the other is cancelled.
4. Returns the user fn's value, or `error.Timeout` if the deadline
   fired first.

The implementation parks the parent on a Park that can be woken by
either the child completing or the timer firing. Cancellation is
prompt because Park is cancellable through any nested wait point —
even uncooperative blocking calls.

### Composing timeouts

`withTimeout` composes naturally:

```zig
fn fetchAllWithTimeout() !void {
    try volt.withTimeout(volt.Duration.fromSecs(5), fetchAll, .{});
}
fn fetchAll() !void {
    try volt.withTimeout(volt.Duration.fromSecs(2), fetchOne, .{"user"});
    try volt.withTimeout(volt.Duration.fromSecs(2), fetchOne, .{"posts"});
}
```

The outer 5-second timeout cancels everything inside if the total
runs over. Each inner call has its own 2-second budget. The cancels
nest correctly because Park-cancel propagates uniformly through any
suspension.

## Interval — repeating timer

```zig
var tick = volt.Interval.start(volt.Duration.fromSecs(1));

while (running) {
    try tick.tick();          // suspend until next interval
    try emitMetrics();
}
```

`Interval.start(period)` creates an interval that fires immediately
on the first tick, then every `period` after that. Use
`Interval.startAfter(period)` if you want the first tick to delay
by `period`.

```zig
tick.setMissedTickPolicy(.skip);   // default: catch up (burst)
```

Missed-tick policies (when the consumer is slower than the period):

- **`.burst`** (default) — fire as many times as needed to catch up.
- **`.skip`** — drop missed ticks; next tick is at the next aligned
  boundary.
- **`.delay`** — reset the schedule; next tick is `period` from now.

Pick by what you want when the consumer falls behind: `burst` for
metrics where every tick must record, `skip` for periodic
heartbeats where you only care about cadence going forward.

## Patterns

### Periodic flush with shutdown

```zig
fn flusher(quit: *volt.sync.Notify) !void {
    var tick = volt.Interval.start(volt.Duration.fromMillis(100));
    while (true) {
        // Race tick against shutdown signal.
        // (Not using volt.select here because Notify isn't a Channel —
        //  use a small dedicated select wrapper or an explicit check.)
        try tick.tick();
        if (quitFlag.load(.acquire)) return;
        try flushBuffer();
    }
}
```

### Bounded retry with backoff

```zig
fn withRetry(comptime f: anytype, args: anytype) !@TypeOf(@call(.auto, f, args)) {
    var attempt: u32 = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        const r = volt.withTimeout(
            volt.Duration.fromMillis(100 << attempt),  // 100ms, 200ms, 400ms, ...
            f,
            args,
        );
        return r catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
    }
    return error.GaveUp;
}
```

### Deadline propagation

For request-scoped deadlines that propagate naturally without being
threaded through args, wrap the entire request in a single
`withTimeout`:

```zig
fn handleRequest(conn: TcpStream) !Response {
    return volt.withTimeout(volt.Duration.fromSecs(30), serve, .{conn});
}
```

If anything inside `serve` takes longer than 30 seconds total, the
whole call surfaces `error.Timeout` and every coroutine spawned
within it is cancelled.

## What about high-resolution timers?

The default reactor timers have ~ms granularity (and worse on busy
systems). For nanosecond-precise work, use `Instant.now()` + a
spin loop or `volt.yield()` — but be aware that the worker thread
isn't guaranteed to wake at exactly the requested instant on a
loaded system. If you need hard-real-time guarantees, you're in OS
RT scheduling territory and Volt isn't the right tool.

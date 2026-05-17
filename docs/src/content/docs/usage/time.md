---
title: Time
description: volt.sleep parks the coroutine on the kqueue timer. volt.yield re-queues immediately. That's the time surface.
---

Volt's time surface is intentionally minimal. Two functions:

```zig
volt.sleep(ns: u64);   // park for ≥ns nanoseconds via the reactor's timer
volt.yield();           // re-queue to the worker's tail; FIFO
```

That's it. No `Interval`, no `Duration`, no `withTimeout`, no
`now()`. Higher-level timer patterns compose from `sleep` + a
spawned watchdog (see below).

## `volt.sleep(ns: u64)`

```zig
volt.sleep(50 * std.time.ns_per_ms);    // sleep 50 ms
volt.sleep(2 * std.time.ns_per_s);      // sleep 2 s
```

Registers a timer with the reactor (`EVFILT_TIMER` on Darwin
kqueue; `timerfd` on epoll; `IORING_OP_TIMEOUT` on io_uring;
`CreateThreadpoolTimer` on Windows IOCP), with the current
coroutine as the wake identity, then parks. When the kernel
delivers the timer event, the reactor unparks the coroutine — it
ends up on a worker's queue and resumes.

Argument is nanoseconds (`u64`). Use `std.time.ns_per_{ms,s,min}`
constants for clarity. Kernel timer resolution on Darwin arm64 is
bounded below by ~1 µs; shorter requested sleeps round up.

**`sleep` does not block the worker thread.** The coroutine parks
on the reactor; the worker runs other coroutines. This is what
makes "sleep 50ms on every connection" scale — `getCpuCount()`
workers cover thousands of concurrent sleepers.

**`sleep` is not cancel-aware** at the API level. To cancel a
sleep, you'd need a separate watchdog pattern:

```zig
fn sleepCancellable(ns: u64, c: *volt.Cancel) error{Cancelled}!void {
    // Race the sleep against the Cancel via Notify.
    var done = volt.Notify.init();
    defer done.deinit();

    _ = try volt.spawn(struct {
        fn t(n: u64, d: *volt.Notify) void {
            volt.sleep(n);
            d.notifyOne();
        }
    }.t, .{ ns, &done });

    // ... wait on whichever fires first via parking-lot multiplexing ...
}
```

A first-class cancel-aware `sleep` may land later. For now,
`sleep` itself runs to completion; cancellation of the surrounding
work has to happen at the next cancel-aware blocking op.

## `volt.yield()`

```zig
volt.yield();
```

Re-queues the current coroutine to its worker's queue **tail**.
Other queued coroutines on this worker run first; this coroutine
resumes on the next pop. FIFO, not LIFO — yields don't bounce
through the LIFO slot.

Use cases:

- **CPU-bound loop cooperation.** A loop with no other suspension
  points can use `yield()` periodically so sibling coroutines on
  the same worker make progress:

  ```zig
  fn cpuLoop(n: u64) u64 {
      var sum: u64 = 0;
      var i: u64 = 0;
      while (i < n) : (i += 1) {
          if (i % 10_000 == 0) volt.yield();
          sum +%= heavy(i);
      }
      return sum;
  }
  ```

- **Cancellation checkpoint.** With a `*Cancel`, combine yield
  with checkpoint to make cancellation propagate:

  ```zig
  while (...) {
      try c.checkpoint();    // error.Cancelled if fired
      volt.yield();          // let other coros (incl. the canceller) make progress
      // ... CPU work ...
  }
  ```

For I/O-bound code, you don't need `yield` — the next `read` /
`recv` / `accept` already parks the coroutine.

## What about `Instant` / `Duration` / `Interval` / `withTimeout`?

Not yet in Volt core. The patterns they'd encode are library
territory or trivially built from `sleep` + spawn + channels:

- **Timeout on an op:** spawn a watchdog that sleeps then fires
  a Cancel; pass that Cancel to the op's cancel-aware variant.

  ```zig
  fn withTimeout(ns: u64, comptime body: anytype) !void {
      try volt.scope(struct {
          fn b(c: *volt.Cancel) anyerror!void {
              _ = try volt.spawn(struct {
                  fn watchdog(n: u64, ctx_c: *volt.Cancel) void {
                      volt.sleep(n);
                      ctx_c.fire();
                  }
              }.watchdog, .{ ns, c });
              try body(c);
          }
      }.b);
  }
  ```

  Then `try withTimeout(1 * std.time.ns_per_s, doWorkWithCancel);`
  cancels `doWorkWithCancel` if it takes more than 1 second.

- **Periodic tick:** loop with `volt.sleep` at the bottom. Drift
  correction (sleep until the next deadline rather than for a
  duration) is straightforward with `std.time.nanoTimestamp()`.

A polished `volt.timer` module may land in a future release. For
now, the recipe approach keeps Volt core small and lets users
compose what they actually need.

## Reactor and timers

Internally, every `sleep` is a reactor timer registration. The
per-platform mechanism differs — `EVFILT_TIMER` on Darwin kqueue,
`timerfd` registered into epoll on Linux, `IORING_OP_TIMEOUT` on
io_uring, `CreateThreadpoolTimer` on Windows IOCP — but the
contract is identical: the kernel's timer heap holds the
registration, the reactor's single-poller-claim ensures one
worker at a time harvests, and timer delivery scales to thousands
of concurrent sleepers without per-timer thread overhead.

See [The reactor](/architecture/reactor/) for the design and
[its backends](/architecture/reactor-backends/) for the per-platform
syscall walkthroughs.

## See also

- [Structured Concurrency](/usage/structured-concurrency/) — Cancel + watchdog patterns for timeouts.
- [Cookbook: timeout with retry](/cookbook/timeout-retry/) — concrete example using the watchdog pattern.
- [Cookbook: graceful drain](/cookbook/graceful-drain/) — shutdown via sleep + Cancel.

---
title: Rate Limiter
description: Bound concurrent requests with Semaphore. Bound rate-per-interval with Semaphore + a refill ticker.
---

Two related problems with one primitive. Volt's `Semaphore` handles
both.

## Bounded concurrency

Limit how many of *something* are happening at once:

```zig
var sem = volt.sync.Semaphore.init(8);   // at most 8 concurrent

fn handle(conn: TcpStream, sem: *volt.sync.Semaphore) void {
    sem.acquire(1);
    defer sem.release(1);
    // ... up to 8 of these run in parallel ...
}
```

Beyond 8, the 9th caller of `acquire(1)` parks until someone
`release`s. Acquisition is FIFO and respects the requested-N — a
caller asking for 4 permits can't be jumped by a caller asking for 1,
even if 1 permit is free.

This is the canonical pattern for connection pools, parallelism
caps, "max in-flight database queries," etc.

## Rate-per-interval (token bucket)

Limit how many of something happen *per second* (or per minute, etc.):

- Semaphore with `burst` initial permits — max burst capacity.
- Refill coroutine that releases `rate_per_interval` permits every
  interval, capped at `burst`.

```zig
const std = @import("std");
const volt = @import("volt");

const RateLimiter = struct {
    sem: volt.sync.Semaphore,
    burst: u32,
    rate_per_sec: u32,

    fn init(rate_per_sec: u32, burst: u32) RateLimiter {
        return .{
            .sem = volt.sync.Semaphore.init(burst),
            .burst = burst,
            .rate_per_sec = rate_per_sec,
        };
    }

    fn acquire(self: *RateLimiter) void {
        self.sem.acquire(1);
    }
};

fn refiller(rl: *RateLimiter) void {
    var tick = volt.Interval.start(volt.Duration.fromSecs(1));
    while (true) {
        tick.tick() catch return;
        const avail = rl.sem.available();
        const headroom = rl.burst -| avail;
        const refill = @min(rl.rate_per_sec, headroom);
        if (refill > 0) rl.sem.release(refill);
    }
}

fn worker(rl: *RateLimiter, id: u32) void {
    rl.acquire();
    std.debug.print("worker {d} got a token\n", .{id});
}
```

## Properties

- **Burstable**: a quiet client gets up to `burst` tokens
  immediately, then `rate` tokens/sec sustained.
- **Fair**: FIFO acquire order — longest-waiting caller goes first.
- **Cancellable**: workers parked on `acquire` surface
  `error.Cancelled` on cancel.

## Variant: per-key rate limiter

For "rate-limit per user," wrap a hashmap keyed by user ID:

```zig
const PerKeyLimiter = struct {
    limits: std.StringHashMap(*RateLimiter),
    mu: volt.sync.Mutex = .{},
    alloc: std.mem.Allocator,
    rate: u32,
    burst: u32,

    fn acquire(self: *PerKeyLimiter, key: []const u8) !void {
        self.mu.lock();
        const entry = try self.limits.getOrPut(key);
        if (!entry.found_existing) {
            const rl = try self.alloc.create(RateLimiter);
            rl.* = RateLimiter.init(self.rate, self.burst);
            entry.value_ptr.* = rl;
        }
        const rl = entry.value_ptr.*;
        self.mu.unlock();          // release before acquiring the per-key sem
        rl.acquire();
    }
};
```

Don't hold the outer mutex across `rl.acquire()` — that would
serialize every key-lookup behind every blocked acquire.

## Why not other shapes?

Volt's primitives compose into all the rate-limit variants:

- **Leaky bucket**: `Channel(void)` of capacity = burst, plus a
  receiver that drains at rate. Producers `trySend({})`;
  fullness = backpressure.
- **Fixed window**: counter + periodic reset coroutine. Just
  `volt.sync.Mutex` for the counter.
- **Sliding log**: deque of recent timestamps; check on each
  request.

Pick the shape that fits your fairness preference; the building
blocks are the same.

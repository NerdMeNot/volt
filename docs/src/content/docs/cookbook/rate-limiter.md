---
title: Rate Limiter
description: Bound concurrency with Semaphore. Build rate-per-second with Semaphore + a refill ticker. Per-key limiters via a hashmap.
---

Two related problems with one primitive. Volt's `Semaphore`
handles both.

## Bounded concurrency

Limit how many of *something* are happening at once:

```zig
var sem = volt.Semaphore.init(8);    // at most 8 concurrent
defer sem.deinit();

fn handle(conn: volt.net.TcpStream, sem: *volt.Semaphore) void {
    sem.acquire();
    defer sem.release();
    // ... up to 8 of these run in parallel ...
}
```

Beyond 8, the 9th caller of `acquire()` parks until someone
`release`s. Acquisition order is FIFO via the parking lot — the
longest-waiting caller goes first.

This is the canonical pattern for connection caps, parallelism
limits on a worker pool, "max in-flight database queries," etc.

## Rate-per-interval (token bucket)

Limit how many of something happen *per second* (or per minute):

- Semaphore with `burst` initial permits — max burst capacity.
- Refill coroutine that releases up to `rate_per_sec` permits
  every second, capped at `burst` total.

```zig
const std = @import("std");
const volt = @import("volt");

const RateLimiter = struct {
    sem: volt.Semaphore,
    burst: u32,
    rate_per_sec: u32,
    available_estimate: std.atomic.Value(i32),   // approx; not exact

    pub fn init(rate_per_sec: u32, burst: u32) RateLimiter {
        return .{
            .sem = volt.Semaphore.init(burst),
            .burst = burst,
            .rate_per_sec = rate_per_sec,
            .available_estimate = std.atomic.Value(i32).init(@intCast(burst)),
        };
    }

    pub fn deinit(self: *RateLimiter) void {
        self.sem.deinit();
    }

    pub fn acquire(self: *RateLimiter) void {
        self.sem.acquire();
        _ = self.available_estimate.fetchSub(1, .acq_rel);
    }
};

fn refiller(rl: *RateLimiter, cancel: *volt.Cancel) void {
    while (!cancel.isFired()) {
        volt.sleep(1 * std.time.ns_per_s);
        const avail = rl.available_estimate.load(.acquire);
        const headroom = @as(i32, @intCast(rl.burst)) - avail;
        const refill: u32 = @intCast(@min(@as(i32, @intCast(rl.rate_per_sec)), @max(headroom, 0)));
        var i: u32 = 0;
        while (i < refill) : (i += 1) {
            rl.sem.release();
            _ = rl.available_estimate.fetchAdd(1, .acq_rel);
        }
    }
}

fn worker(rl: *RateLimiter, id: u32) void {
    rl.acquire();
    std.debug.print("worker {d} got a token\n", .{id});
    // ... use the token ...
}
```

## Properties

- **Burstable**: a quiet client gets up to `burst` tokens
  immediately, then `rate` tokens/sec sustained.
- **Fair**: FIFO acquire order via the parking lot — longest-
  waiting caller goes first.
- **Cancel-aware**: workers parked on `acquire` can be woken via
  `acquireCancel(*Cancel)` if you wire a Cancel into them.

The `available_estimate` is an approximation — it can drift under
heavy concurrent acquire/release. The Semaphore's internal state
is authoritative; the estimate exists only to tell the refiller
how many permits to add. For an exact "permits available" query,
the Semaphore would need an `available()` method (not in the
current API).

## Variant: per-key rate limiter

For "rate-limit per user," wrap a hashmap keyed by user ID:

```zig
const PerKeyLimiter = struct {
    limits: std.StringHashMap(*RateLimiter),
    mu: volt.Mutex,
    alloc: std.mem.Allocator,
    rate: u32,
    burst: u32,

    pub fn init(alloc: std.mem.Allocator, rate: u32, burst: u32) PerKeyLimiter {
        return .{
            .limits = std.StringHashMap(*RateLimiter).init(alloc),
            .mu = volt.Mutex.init(),
            .alloc = alloc,
            .rate = rate,
            .burst = burst,
        };
    }

    pub fn acquire(self: *PerKeyLimiter, key: []const u8) !void {
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

## Variant: leaky bucket

For "drain at constant rate, no burst":

```zig
// Use a Mpmc(void, cap) channel. Producers trySend; full → backpressure.
// A receiver drains at a fixed rate.

var bucket = volt.Mpmc(void, 100).init();
defer bucket.deinit();

fn produceMaybe() error{Backpressure}!void {
    bucket.trySend({}) catch return error.Backpressure;
}

fn drainer() void {
    while (true) {
        _ = bucket.recv() catch return;
        volt.sleep(10 * std.time.ns_per_ms);  // 100/sec drain
    }
}
```

Producer never blocks; oversize traffic gets `error.Backpressure`
back. The drainer ensures sustained rate stays at 100/sec.

## Variant: fixed window

For "N requests per fixed window (e.g., minute)":

```zig
const FixedWindow = struct {
    count: std.atomic.Value(u32),
    limit: u32,

    fn allow(self: *FixedWindow) bool {
        return self.count.fetchAdd(1, .acq_rel) < self.limit;
    }
};

fn windowReset(w: *FixedWindow) void {
    while (true) {
        volt.sleep(60 * std.time.ns_per_s);
        w.count.store(0, .release);
    }
}
```

Cheaper than token-bucket; cliff at window boundary (the dreaded
"100 requests at :59 + 100 at :01 = 200 within 2 seconds" problem).

## Picking a shape

| Pattern | Use | Fairness | Burst |
|---|---|---|---|
| Bounded concurrency | `Semaphore.acquire/release` | FIFO | n/a |
| Token bucket (rate + burst) | `Semaphore` + refiller coro | FIFO | burst configurable |
| Leaky bucket (rate, no burst) | `Mpmc(void, cap)` + drainer | n/a | no burst |
| Fixed window (N per period) | atomic counter + reset coro | first-come | full window at start |

## See also

- [Sync primitives: Semaphore](/usage/sync/) — the API.
- [Channels: Mpmc](/usage/channels/) — for the leaky-bucket variant.
- [Connection Pool](/cookbook/connection-pool/) — concrete use of bounded concurrency.

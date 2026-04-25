---
title: Rate Limiter
description: Token bucket rate limiter using Semaphore and Interval for
  per-client and global rate limiting.
slug: v1.0.0-zig0.15.2/v110/cookbook/rate-limiter
---

Rate limiting protects services from overload by capping how many operations can occur within a time window. This recipe implements a token bucket algorithm using `Semaphore` for the bucket and `Interval` for refilling tokens.

:::tip[What you'll learn]
* How the **token bucket algorithm** works and why it is the go-to choice for rate limiting
* Using **Semaphore** as a bounded token store with atomic acquire/release
* Using **Interval** for periodic refill without drift accumulation
* Extending a single limiter into **per-client rate limiting** with a shared map
:::

## Token Bucket Rate Limiter

The token bucket algorithm models a bucket that holds up to `capacity` tokens:

1. A bucket starts full with `capacity` tokens.
2. Every `refill_interval`, `refill_amount` tokens are added back (capped at capacity).
3. Each operation consumes one token. If the bucket is empty, the operation is rejected immediately.

The `Semaphore` is a natural fit here because its permits *are* the tokens. `tryAcquire` is a non-blocking check, and `release` adds permits back without exceeding the configured maximum. The `Interval` timer fires at a steady cadence and drives the refill loop.

```zig
const std = @import("std");
const volt = @import("volt");

pub const RateLimiter = struct {
    /// Semaphore acts as the token bucket.
    /// Available permits = available tokens.
    tokens: volt.sync.Semaphore,

    /// Interval timer for periodic refill. Each tick adds `refill_amount`
    /// tokens back into the bucket. Because Interval tracks elapsed time
    /// internally, multiple ticks fire if the caller falls behind -- so
    /// tokens accumulate correctly even under load.
    refill_timer: volt.time.Interval,

    /// How many tokens to add per refill tick.
    refill_amount: u32,

    /// Maximum tokens (bucket capacity). Refills never push the count
    /// above this value.
    capacity: u32,

    pub fn init(capacity: u32, refill_amount: u32, refill_interval: volt.Duration) RateLimiter {
        return .{
            .tokens = volt.sync.Semaphore.init(capacity),
            .refill_timer = volt.time.Interval.init(refill_interval),
            .refill_amount = refill_amount,
            .capacity = capacity,
        };
    }

    /// Try to consume one token. Returns true if allowed, false if
    /// rate-limited. This is non-blocking: it never waits for tokens
    /// to become available.
    pub fn tryAcquire(self: *RateLimiter) bool {
        // Refill first so that freshly-available tokens are visible
        // to the caller on the same poll cycle.
        self.refill();

        // Attempt to take a single token from the bucket.
        return self.tokens.tryAcquire(1);
    }

    /// Drain all elapsed refill ticks and add the corresponding tokens.
    /// The while loop handles the case where several intervals passed
    /// since the last call -- each tick adds `refill_amount` tokens.
    pub fn refill(self: *RateLimiter) void {
        while (self.refill_timer.tryTick()) {
            // release() adds permits back into the semaphore, but the
            // semaphore clamps internally so we never exceed capacity.
            self.tokens.release(self.refill_amount);
        }
    }

    /// Approximate snapshot of available tokens. Because tryAcquire and
    /// refill can race on other threads, treat this as a best-effort
    /// diagnostic rather than a gating decision.
    pub fn availableTokens(self: *RateLimiter) usize {
        return self.tokens.availablePermits();
    }
};
```

## Usage in a Server

The following example shows a complete accept loop that applies a global rate limiter to every incoming connection. Requests that exceed the limit receive a `429 Too Many Requests` response and are closed immediately.

```zig
const std = @import("std");
const volt = @import("volt");

/// Global rate limiter: 100 requests/second sustained rate.
/// The bucket holds 100 tokens and refills 10 tokens every 100ms,
/// so the steady-state throughput is 10 * (1000/100) = 100 req/s.
var global_limiter = RateLimiter.init(
    100,                            // capacity (also the max burst size)
    10,                             // refill amount per tick
    volt.Duration.fromMillis(100),    // refill interval
);

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Io) void {
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(8080),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    std.debug.print("Server listening on 0.0.0.0:8080\n", .{});

    // Accept loop -- poll for new connections.
    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var f = io.@"async"(handleConnection, .{result.stream}) catch {
                handleConnection(result.stream);
                continue;
            };
            f.detach();
        } else {
            // No pending connection -- yield briefly to avoid busy-spinning.
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

fn handleConnection(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    // Gate every request through the global limiter.
    if (!global_limiter.tryAcquire()) {
        stream.writeAll("HTTP/1.1 429 Too Many Requests\r\n\r\n") catch return;
        return;
    }

    // Process the request normally.
    stream.writeAll("HTTP/1.1 200 OK\r\n\r\nHello\n") catch return;
}
```

## Per-Client Rate Limiting

A global limiter caps total throughput, but one aggressive client can consume the entire budget and starve everyone else. Per-client limiting gives each source address its own independent bucket.

The `PerClientLimiter` below maintains a hash map of `RateLimiter` instances keyed by IPv4 address. A `Mutex` protects the map so that concurrent tasks can safely look up or create limiters.

```zig
const std = @import("std");
const volt = @import("volt");

const ClientKey = u32; // IPv4 address packed into a u32.

const PerClientLimiter = struct {
    limiters: std.AutoHashMap(ClientKey, *RateLimiter),
    allocator: std.mem.Allocator,
    lock: volt.sync.Mutex,

    /// Per-client settings -- every new client gets an independent
    /// bucket with these parameters.
    capacity: u32,
    refill_amount: u32,
    refill_interval: volt.Duration,

    pub fn init(
        allocator: std.mem.Allocator,
        capacity: u32,
        refill_amount: u32,
        refill_interval: volt.Duration,
    ) PerClientLimiter {
        return .{
            .limiters = std.AutoHashMap(ClientKey, *RateLimiter).init(allocator),
            .allocator = allocator,
            .lock = volt.sync.Mutex.init(),
            .capacity = capacity,
            .refill_amount = refill_amount,
            .refill_interval = refill_interval,
        };
    }

    pub fn deinit(self: *PerClientLimiter) void {
        var it = self.limiters.valueIterator();
        while (it.next()) |limiter_ptr| {
            self.allocator.destroy(limiter_ptr.*);
        }
        self.limiters.deinit();
    }

    /// Check whether `client_ip` is allowed to proceed. Creates a new
    /// limiter on first contact so there is no setup step per client.
    pub fn tryAcquire(self: *PerClientLimiter, client_ip: ClientKey) bool {
        if (!self.lock.tryLock()) return false; // Contended -- reject conservatively.
        defer self.lock.unlock();

        const entry = self.limiters.getOrPut(client_ip) catch return false;
        if (!entry.found_existing) {
            const limiter = self.allocator.create(RateLimiter) catch return false;
            limiter.* = RateLimiter.init(
                self.capacity,
                self.refill_amount,
                self.refill_interval,
            );
            entry.value_ptr.* = limiter;
        }

        return entry.value_ptr.*.tryAcquire();
    }

    /// Return the number of tracked clients. Useful for monitoring
    /// and deciding when to run cleanup.
    pub fn clientCount(self: *PerClientLimiter) usize {
        return self.limiters.count();
    }
};
```

Combining both layers is straightforward -- check the global limiter first, then the per-client limiter:

```zig
fn handleConnection(conn: volt.net.TcpStream, client_ip: ClientKey) void {
    var stream = conn;
    defer stream.close();

    // Layer 1: global throughput cap.
    if (!global_limiter.tryAcquire()) {
        stream.writeAll("HTTP/1.1 429 Too Many Requests\r\n\r\n") catch return;
        return;
    }

    // Layer 2: per-client fairness cap.
    if (!per_client_limiter.tryAcquire(client_ip)) {
        stream.writeAll("HTTP/1.1 429 Too Many Requests\r\n\r\n") catch return;
        return;
    }

    stream.writeAll("HTTP/1.1 200 OK\r\n\r\nHello\n") catch return;
}
```

## Sliding Window Variant

The token bucket above is inherently bursty: a client can drain all tokens in a single instant and then wait for the next refill. If you need smoother rate enforcement, a sliding window approach tracks the timestamp of every recent request and rejects new ones once the window is full.

This variant is single-threaded (no atomics) and works well inside a per-connection handler. The ring buffer is fixed-size, so there are no allocations after initialization.

```zig
pub const SlidingWindowLimiter = struct {
    /// Timestamps of recent requests stored in a ring buffer.
    timestamps: [256]i128,
    head: usize,
    count: usize,

    /// Window duration in nanoseconds.
    window_ns: u64,

    /// Maximum requests allowed within the window.
    max_requests: usize,

    pub fn init(window: volt.Duration, max_requests: usize) SlidingWindowLimiter {
        return .{
            .timestamps = [_]i128{0} ** 256,
            .head = 0,
            .count = 0,
            .window_ns = window.asNanos(),
            .max_requests = @min(max_requests, 256),
        };
    }

    pub fn tryAcquire(self: *SlidingWindowLimiter) bool {
        const now = std.time.nanoTimestamp();
        const window_start = now - @as(i128, self.window_ns);

        // Evict entries that have fallen outside the window.
        while (self.count > 0) {
            const oldest_idx = (self.head + 256 - self.count) % 256;
            if (self.timestamps[oldest_idx] < window_start) {
                self.count -= 1;
            } else {
                break;
            }
        }

        // If the window is full, reject.
        if (self.count >= self.max_requests) return false;

        // Record this request and advance the head.
        self.timestamps[self.head] = now;
        self.head = (self.head + 1) % 256;
        self.count += 1;
        return true;
    }
};
```

## Choosing Parameters

| Scenario | Capacity | Refill Rate | Notes |
|----------|----------|-------------|-------|
| API endpoint | 60 | 1/second | 60 req/min steady, small bursts OK |
| Login attempts | 5 | 1/10s | Tight limit, slow refill to deter brute force |
| WebSocket messages | 100 | 20/100ms | High throughput, frequent refill keeps latency low |
| Background jobs | 10 | 10/second | Match upstream service capacity |

The key insight: `capacity` controls the maximum burst size, while `refill_amount / refill_interval` controls the sustained throughput. Set them independently based on your tolerance for bursts versus steady load. A large capacity with a slow refill allows big bursts followed by quiet periods; a small capacity with a fast refill enforces a nearly constant rate.

## Try It Yourself

Here are three extensions you can add to the `RateLimiter` to make it production-ready.

### 1. Burst allowance

Allow short bursts above the sustained rate by separating the bucket into a sustained portion and a burst reserve. The burst reserve refills more slowly, so clients can spike briefly but cannot sustain the higher rate.

```zig
pub const BurstRateLimiter = struct {
    /// Sustained token bucket -- always refilling.
    sustained: RateLimiter,

    /// Burst reserve -- refills more slowly, only used when the
    /// sustained bucket is empty.
    burst: RateLimiter,

    pub fn init(
        sustained_capacity: u32,
        sustained_refill: u32,
        sustained_interval: volt.Duration,
        burst_capacity: u32,
        burst_refill: u32,
        burst_interval: volt.Duration,
    ) BurstRateLimiter {
        return .{
            .sustained = RateLimiter.init(sustained_capacity, sustained_refill, sustained_interval),
            .burst = RateLimiter.init(burst_capacity, burst_refill, burst_interval),
        };
    }

    /// Try the sustained bucket first. If it is empty, dip into the
    /// burst reserve. If both are empty, reject.
    pub fn tryAcquire(self: *BurstRateLimiter) bool {
        if (self.sustained.tryAcquire()) return true;
        return self.burst.tryAcquire();
    }
};
```

Usage example: allow 100 req/s sustained with a burst reserve of 50 extra tokens that refills at 5/second:

```zig
var limiter = BurstRateLimiter.init(
    100, 10, volt.Duration.fromMillis(100),   // sustained: 100 cap, 10 per 100ms
    50,  5,  volt.Duration.fromSecs(1),       // burst: 50 cap, 5 per second
);
```

### 2. Rate limiter stats

Track how many requests were allowed, how many were rejected, and expose the current token count. This is essential for monitoring dashboards and alerting.

```zig
pub const TrackedRateLimiter = struct {
    inner: RateLimiter,

    /// Counters. Use atomics so stats can be read from a monitoring
    /// thread without taking the same lock as the request path.
    total_allowed: std.atomic.Value(u64),
    total_rejected: std.atomic.Value(u64),

    pub fn init(capacity: u32, refill_amount: u32, refill_interval: volt.Duration) TrackedRateLimiter {
        return .{
            .inner = RateLimiter.init(capacity, refill_amount, refill_interval),
            .total_allowed = std.atomic.Value(u64).init(0),
            .total_rejected = std.atomic.Value(u64).init(0),
        };
    }

    pub fn tryAcquire(self: *TrackedRateLimiter) bool {
        if (self.inner.tryAcquire()) {
            _ = self.total_allowed.fetchAdd(1, .monotonic);
            return true;
        }
        _ = self.total_rejected.fetchAdd(1, .monotonic);
        return false;
    }

    pub fn currentTokens(self: *TrackedRateLimiter) usize {
        return self.inner.availableTokens();
    }

    pub fn allowed(self: *TrackedRateLimiter) u64 {
        return self.total_allowed.load(.monotonic);
    }

    pub fn rejected(self: *TrackedRateLimiter) u64 {
        return self.total_rejected.load(.monotonic);
    }

    /// Rejection rate as a percentage (0.0 -- 100.0). Returns 0 if
    /// no requests have been seen yet.
    pub fn rejectionRate(self: *TrackedRateLimiter) f64 {
        const a = self.allowed();
        const r = self.rejected();
        const total = a + r;
        if (total == 0) return 0.0;
        return @as(f64, @floatFromInt(r)) / @as(f64, @floatFromInt(total)) * 100.0;
    }
};
```

### 3. Cleanup task for stale per-client limiters

In a long-running server, the per-client map grows without bound as new clients arrive. A periodic cleanup task walks the map and evicts entries whose buckets have been full (idle) for longer than a configurable TTL. This prevents unbounded memory growth.

```zig
const CleanupConfig = struct {
    /// How often the cleanup task runs.
    sweep_interval: volt.Duration,

    /// A client limiter is considered stale when its bucket is full
    /// (no tokens consumed) and this duration has passed since the
    /// last tryAcquire call.
    stale_after: volt.Duration,
};

/// Run this as a background task alongside the accept loop.
/// It periodically scans the per-client map and removes limiters
/// that have been idle (full bucket) for too long.
fn cleanupTask(
    limiter: *PerClientLimiter,
    config: CleanupConfig,
) void {
    var sweep_timer = volt.time.Interval.init(config.sweep_interval);

    while (true) {
        // Wait for the next sweep tick.
        while (!sweep_timer.tryTick()) {
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }

        if (!limiter.lock.tryLock()) continue;
        defer limiter.lock.unlock();

        // Collect keys to remove. We cannot remove during iteration,
        // so gather stale keys first.
        var stale_keys: [256]ClientKey = undefined;
        var stale_count: usize = 0;

        var it = limiter.limiters.iterator();
        while (it.next()) |entry| {
            const client_limiter = entry.value_ptr.*;

            // A full bucket means no tokens were consumed since the
            // last refill cycle -- the client is idle.
            if (client_limiter.availableTokens() >= client_limiter.capacity) {
                if (stale_count < stale_keys.len) {
                    stale_keys[stale_count] = entry.key_ptr.*;
                    stale_count += 1;
                }
            }
        }

        // Evict stale entries.
        for (stale_keys[0..stale_count]) |key| {
            if (limiter.limiters.fetchRemove(key)) |removed| {
                limiter.allocator.destroy(removed.value);
            }
        }

        if (stale_count > 0) {
            std.debug.print(
                "rate-limiter cleanup: evicted {} stale clients, {} remaining\n",
                .{ stale_count, limiter.limiters.count() },
            );
        }
    }
}
```

Spawn the cleanup task at server startup alongside the accept loop:

```zig
fn server(io: volt.Io) void {
    // ... listener setup ...

    // Spawn the cleanup task in the background.
    var f = io.@"async"(cleanupTask, .{
        &per_client_limiter,
        CleanupConfig{
            .sweep_interval = volt.Duration.fromSecs(60),
            .stale_after = volt.Duration.fromSecs(300),
        },
    }) catch {};
    f.detach();

    // Accept loop runs as before.
    while (true) {
        // ...
    }
}
```

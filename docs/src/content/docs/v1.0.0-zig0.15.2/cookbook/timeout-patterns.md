---
title: Timeout Patterns
description: Wrapping operations with Deadline, retry with exponential backoff,
  deadline propagation, and cascading timeouts.
slug: v1.0.0-zig0.15.2/cookbook/timeout-patterns
---

:::tip[What you'll learn]
* Using `Deadline` for time-bounded operations with explicit expiry checks
* Retry with exponential backoff and jitter to avoid thundering herd
* Propagating a single deadline through a chain of sub-operations
* Cascading timeouts that enforce per-layer budgets within an overall limit
* Extending idle timeouts on progress (keep-alive pattern)
* Wrapping async operations with timeout using the `Io` handle
:::

Timeouts prevent operations from hanging indefinitely. Volt provides `Deadline` for tracking expiration and the explicit `Io` handle for async timeout patterns. This recipe shows how to combine them for robust error handling.

***

## Pattern 1: Simple Deadline Check

**When to use:** You have a sequence of blocking or semi-blocking steps (connect, send, receive) and want to bail out between steps if a time budget has been exceeded.

The most basic pattern -- create a deadline and check it between operations. Each call to `isExpired()` compares the current monotonic time against the stored expiration instant, so there is no drift even if individual steps take longer than expected.

```zig
const std = @import("std");
const volt = @import("volt");

fn fetchHttpResponse(host: []const u8, timeout_ms: u64) ![]u8 {
    var deadline = volt.time.Deadline.init(volt.Duration.fromMillis(timeout_ms));

    // Step 1: Establish TCP connection to the server.
    var stream = try volt.net.connect(host);
    defer stream.close();

    // After the connect completes, check whether we have already
    // exhausted our time budget before doing more work.
    if (deadline.isExpired()) return error.TimedOut;

    // Step 2: Send the HTTP request line and headers.
    try stream.writeAll("GET /api/v1/health HTTP/1.1\r\nHost: ");
    try stream.writeAll(host);
    try stream.writeAll("\r\nConnection: close\r\n\r\n");

    if (deadline.isExpired()) return error.TimedOut;

    // Step 3: Read the response body, polling until EOF or expiry.
    var buf: [4096]u8 = undefined;
    var total: usize = 0;

    while (!deadline.isExpired()) {
        if (stream.tryRead(buf[total..])) |n| {
            if (n == 0) break; // EOF -- server closed the connection.
            total += n;
            if (total >= buf.len) break;
        } else |_| {
            return error.ReadFailed;
        }

        // Yield briefly so we do not spin-loop at 100% CPU while
        // waiting for the next chunk of data to arrive.
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }

    if (total == 0 and deadline.isExpired()) return error.TimedOut;
    return buf[0..total];
}
```

The key insight is that `isExpired()` is cheap -- it reads the monotonic clock and compares against a single stored timestamp. You can sprinkle checks liberally between steps without measurable overhead.

***

## Pattern 2: Retry with Exponential Backoff

**When to use:** An operation can fail due to transient issues (network blip, server overload, rate limiting) and has a reasonable chance of succeeding if retried after a short delay.

Exponential backoff increases the delay between retries so you do not hammer a struggling service. Adding random **jitter** is critical in production: without it, all clients that failed at the same time will retry at the same time, creating a thundering herd that makes the problem worse.

```zig
const std = @import("std");
const volt = @import("volt");

pub const RetryConfig = struct {
    max_retries: u32 = 3,
    initial_delay: volt.Duration = volt.Duration.fromMillis(100),
    max_delay: volt.Duration = volt.Duration.fromSecs(10),
    multiplier: u64 = 2,
    overall_timeout: volt.Duration = volt.Duration.fromSecs(30),

    /// Maximum random jitter added to each delay as a fraction of the
    /// current delay. 0.25 means up to 25% extra. This prevents
    /// synchronized retry storms across multiple clients.
    jitter_fraction: f64 = 0.25,
};

pub fn retryWithBackoff(
    comptime T: type,
    config: RetryConfig,
    comptime op: fn () anyerror!T,
) anyerror!T {
    var deadline = volt.time.Deadline.init(config.overall_timeout);
    var delay = config.initial_delay;

    // Seed a PRNG for jitter. Using the timestamp is fine here --
    // we only need enough randomness to desynchronize retries,
    // not cryptographic quality.
    var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    const random = prng.random();

    var attempt: u32 = 0;
    while (attempt <= config.max_retries) : (attempt += 1) {
        // Check the overall time budget before each attempt.
        if (deadline.isExpired()) return error.TimedOut;

        // Try the operation.
        if (op()) |result| {
            return result;
        } else |err| {
            // Last attempt -- propagate the underlying error rather
            // than masking it with a generic TimedOut.
            if (attempt == config.max_retries) return err;

            std.debug.print(
                "Attempt {d}/{d} failed: {}. Retrying in {d} ms.\n",
                .{ attempt + 1, config.max_retries + 1, err, delay.asMillis() },
            );

            // Add jitter: delay + random(0, delay * jitter_fraction).
            // This spreads retries across a window rather than having
            // every client retry at the exact same instant.
            const jitter_ns: u64 = if (config.jitter_fraction > 0.0) blk: {
                const max_jitter: u64 = @intFromFloat(
                    @as(f64, @floatFromInt(delay.nanos)) * config.jitter_fraction,
                );
                break :blk if (max_jitter > 0) random.uintLessThan(u64, max_jitter) else 0;
            } else 0;

            const actual_delay = delay.add(volt.Duration.fromNanos(jitter_ns));

            // Also clamp the sleep to the remaining budget so we do
            // not overshoot the overall timeout.
            const remaining = deadline.remaining();
            const sleep_duration = if (actual_delay.nanos < remaining.nanos)
                actual_delay
            else
                remaining;

            volt.time.blockingSleep(sleep_duration);

            // Double the base delay for the next attempt, capped at max.
            delay = delay.mul(config.multiplier);
            if (delay.nanos > config.max_delay.nanos) {
                delay = config.max_delay;
            }
        }
    }

    return error.TimedOut;
}
```

Usage:

```zig
const result = try retryWithBackoff([]u8, .{
    .max_retries = 5,
    .initial_delay = volt.Duration.fromMillis(200),
    .overall_timeout = volt.Duration.fromSecs(60),
    .jitter_fraction = 0.25,
}, fetchOrderFromCatalogService);
```

The jitter makes a real difference under load. Without it, 1000 clients that all fail at t=0 will all retry at t=100ms, t=300ms, t=700ms, and so on -- perfectly synchronized spikes. With 25% jitter, the first retry wave spreads across 100--125ms, the second across 300--375ms, giving the server breathing room to recover.

***

## Pattern 3: Deadline Propagation

**When to use:** A request handler calls multiple services or sub-operations, and you want a single overall time budget shared across all of them. If the first call is slow, the later calls get less time rather than each one independently waiting its full timeout.

Pass a single `Deadline` pointer through a chain of operations so the overall time budget is shared. Each function checks the shared deadline before starting work, and uses `remaining()` to cap its own I/O operations to whatever time is left.

```zig
const std = @import("std");
const volt = @import("volt");

const User = struct { id: u64, name: []const u8 };
const Post = struct { id: u64, title: []const u8, body: []const u8 };
const Recommendation = struct { item_id: u64, score: f32 };

const ApiResponse = struct {
    user: User,
    posts: []Post,
    recommendations: []Recommendation,
};

fn handleApiRequest(deadline: *volt.time.Deadline) !ApiResponse {
    // Each sub-operation shares the same deadline. If fetchUser
    // takes 3 of the 5 available seconds, fetchPosts and
    // fetchRecommendations split the remaining 2 seconds.
    const user = try fetchUser(deadline);
    const posts = try fetchPosts(deadline, user.id);
    const recommendations = try fetchRecommendations(deadline, user.id);

    return .{
        .user = user,
        .posts = posts,
        .recommendations = recommendations,
    };
}

fn fetchUser(deadline: *volt.time.Deadline) !User {
    // Bail immediately if a previous step already exhausted the budget.
    if (deadline.isExpired()) return error.TimedOut;

    // Use the remaining time to cap this sub-operation. This ensures
    // a slow user service does not consume more than the total budget.
    const remaining = deadline.remaining();
    if (remaining.isZero()) return error.TimedOut;

    // ... perform fetch within remaining time ...
    _ = remaining;
    return .{ .id = 42, .name = "alice" };
}

fn fetchPosts(deadline: *volt.time.Deadline, user_id: u64) ![]Post {
    if (deadline.isExpired()) return error.TimedOut;
    const remaining = deadline.remaining();
    if (remaining.isZero()) return error.TimedOut;

    // ... perform fetch within remaining time ...
    _ = user_id;
    _ = remaining;
    return &.{};
}

fn fetchRecommendations(deadline: *volt.time.Deadline, user_id: u64) ![]Recommendation {
    if (deadline.isExpired()) return error.TimedOut;
    const remaining = deadline.remaining();
    if (remaining.isZero()) return error.TimedOut;

    // ... perform fetch within remaining time ...
    _ = user_id;
    _ = remaining;
    return &.{};
}

// Entry point: 5 second budget for the entire request.
pub fn main() !void {
    var deadline = volt.time.Deadline.init(volt.Duration.fromSecs(5));
    const response = handleApiRequest(&deadline) catch |err| {
        if (err == error.TimedOut) {
            std.debug.print("Request timed out after 5 seconds\n", .{});
            return;
        }
        return err;
    };
    _ = response;
}
```

This pattern is especially valuable in microservice architectures where a single user-facing request fans out to multiple backends. Without a shared deadline, each backend call might independently wait its full timeout, causing the total latency to be the **sum** of all timeouts rather than a fixed budget.

***

## Pattern 4: Cascading Timeouts

**When to use:** You have a layered system (e.g., a reverse proxy or API gateway) where each layer should enforce its own tighter sub-timeout, but still respect the overall budget from the layer above.

Apply progressively tighter timeouts at each layer. The connection phase gets a short fixed budget (users notice connection delays more than transfer delays), while data transfer gets whatever remains from the caller's deadline.

```zig
const std = @import("std");
const volt = @import("volt");

fn proxyRequest(client_deadline: *volt.time.Deadline) !void {
    // Layer 1: Upstream connection -- 2 seconds max, but also
    // respect the client's overall deadline. We take the minimum
    // so that a generous connection timeout does not exceed the
    // caller's remaining budget.
    const connect_budget = @min(
        volt.Duration.fromSecs(2).nanos,
        client_deadline.remaining().nanos,
    );
    var connect_deadline = volt.time.Deadline.init(
        volt.Duration.fromNanos(connect_budget),
    );

    var upstream = connectUpstream(&connect_deadline) catch |err| {
        if (err == error.TimedOut) {
            // Distinguish connection timeout from transfer timeout
            // so the caller can report a more specific error.
            return error.UpstreamConnectTimeout;
        }
        return err;
    };
    defer upstream.close();

    // Layer 2: Header exchange -- 5 seconds max, but again capped
    // by whatever the client deadline has left.
    const header_budget = @min(
        volt.Duration.fromSecs(5).nanos,
        client_deadline.remaining().nanos,
    );
    var header_deadline = volt.time.Deadline.init(
        volt.Duration.fromNanos(header_budget),
    );

    try sendRequestHeaders(&upstream, &header_deadline);

    // Layer 3: Body transfer -- use whatever remains of the
    // client's deadline for the potentially large body.
    if (client_deadline.isExpired()) return error.TimedOut;
    try transferResponseBody(&upstream, client_deadline);
}

fn connectUpstream(deadline: *volt.time.Deadline) !volt.net.TcpStream {
    _ = deadline;
    unreachable; // Placeholder for actual connection logic.
}

fn sendRequestHeaders(upstream: *volt.net.TcpStream, deadline: *volt.time.Deadline) !void {
    _ = upstream;
    _ = deadline;
}

fn transferResponseBody(upstream: *volt.net.TcpStream, deadline: *volt.time.Deadline) !void {
    _ = upstream;
    _ = deadline;
}
```

The cascading approach gives you fine-grained control. A slow DNS lookup or TLS handshake (connection phase) fails fast at 2 seconds, while a large file download (transfer phase) can use the full remaining budget. Without cascading, you would either set one large timeout that masks connection issues or one small timeout that kills legitimate large transfers.

***

## Pattern 5: Extending Deadlines on Progress (Idle Timeout)

**When to use:** A long-running transfer should be allowed to continue as long as data is flowing, but should be killed if it stalls. This is the classic "idle timeout" or "keep-alive" pattern used by HTTP servers and file transfer protocols.

Instead of a fixed wall-clock deadline, reset the deadline every time you observe forward progress. This lets a 10 GB download run for hours as long as bytes keep arriving, while still detecting a stalled connection within 10 seconds.

```zig
const std = @import("std");
const volt = @import("volt");

fn downloadWithIdleTimeout(
    stream: *volt.net.TcpStream,
    writer: *std.io.AnyWriter,
) !usize {
    // Start with a 10-second idle timeout. The deadline resets
    // on every successful read, so a healthy transfer never expires.
    var deadline = volt.time.Deadline.init(volt.Duration.fromSecs(10));
    var buf: [8192]u8 = undefined;
    var total_bytes: usize = 0;

    while (true) {
        if (deadline.isExpired()) {
            std.debug.print(
                "Connection idle for 10s after {d} bytes. Aborting.\n",
                .{total_bytes},
            );
            return error.TimedOut;
        }

        if (stream.tryRead(&buf)) |n| {
            if (n == 0) break; // EOF -- transfer complete.

            // We received data -- reset the idle clock. This is
            // the key insight: the timeout measures inactivity,
            // not total elapsed time.
            deadline.reset(volt.Duration.fromSecs(10));

            try writer.writeAll(buf[0..n]);
            total_bytes += n;
        } else |_| {
            return error.ReadFailed;
        }

        // Brief yield to avoid spinning while waiting for the
        // next packet to arrive from the network.
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }

    return total_bytes;
}
```

The `reset()` call moves the expiration point forward to 10 seconds from *now*, not from the original start time. This is fundamentally different from `extend()`, which adds time to the *existing* expiration point. For idle timeouts, `reset()` is the correct choice because you want a fixed window of inactivity, regardless of how long the transfer has been running.

***

## Pattern 6: Async Timeout with the `Io` Handle

**When to use:** You are working inside the async runtime and want to race an operation against a time limit. Spawn the work as an async task and use a `Deadline` to enforce the budget, all through the explicit `Io` handle.

When working inside the async runtime, spawn the operation and enforce a deadline:

```zig
const std = @import("std");
const volt = @import("volt");

fn acceptConnection(io: *volt.Io, listener: *volt.net.TcpListener) !volt.net.TcpStream {
    // Perform the accept through the runtime.
    return listener.accept(io);
}

fn acceptWithTimeout(io: *volt.Io, listener: *volt.net.TcpListener) void {
    // Spawn the accept as an async task.
    var f = io.@"async"(acceptConnection, .{ io, listener }) catch {
        std.debug.print("Failed to spawn accept task\n", .{});
        return;
    };

    // Enforce a 30-second deadline. If the accept has not completed
    // by then, we treat it as a timeout.
    var deadline = volt.time.Deadline.init(volt.Duration.fromSecs(30));

    // Await the result, checking the deadline.
    if (!deadline.isExpired()) {
        const conn = f.@"await"(io);
        handleConnection(conn);
    } else {
        std.debug.print("No connection within 30s\n", .{});
    }
}

fn handleConnection(conn: anytype) void {
    _ = conn;
}
```

This approach integrates naturally with the explicit `Io` handle pattern. The `Deadline` tracks wall-clock time while `io.@"async"` and `f.@"await"` handle the async scheduling. You get clear separation between "what to do" (the spawned task) and "how long to wait" (the deadline).

***

## Try It Yourself

These exercises build on the patterns above. Each one addresses a real production concern.

### Exercise 1: Circuit Breaker

A circuit breaker tracks consecutive failures and "opens" (refuses new requests) after a threshold is exceeded. This prevents a cascade of slow timeouts from overwhelming your system when a downstream service is down.

```zig
const std = @import("std");
const volt = @import("volt");

pub const CircuitBreaker = struct {
    consecutive_timeouts: u32 = 0,
    threshold: u32,
    state: State = .closed,
    opened_at: ?volt.time.Instant = null,
    recovery_window: volt.Duration,

    pub const State = enum {
        /// Normal operation -- requests are allowed through.
        closed,
        /// Too many failures -- requests are rejected immediately.
        open,
        /// Recovery period -- one probe request is allowed through.
        half_open,
    };

    pub fn init(threshold: u32, recovery_window: volt.Duration) CircuitBreaker {
        return .{
            .threshold = threshold,
            .recovery_window = recovery_window,
        };
    }

    /// Call before each request. Returns error.CircuitOpen if the
    /// breaker has tripped and the recovery window has not elapsed.
    pub fn allowRequest(self: *CircuitBreaker) !void {
        switch (self.state) {
            .closed => return, // All clear.
            .open => {
                // Check if enough time has passed to try a probe.
                if (self.opened_at) |opened| {
                    if (opened.elapsed().nanos >= self.recovery_window.nanos) {
                        self.state = .half_open;
                        return; // Allow one probe request.
                    }
                }
                return error.CircuitOpen;
            },
            .half_open => return, // Probe request in progress.
        }
    }

    /// Call after a successful response to close the circuit.
    pub fn recordSuccess(self: *CircuitBreaker) void {
        self.consecutive_timeouts = 0;
        self.state = .closed;
        self.opened_at = null;
    }

    /// Call after a timeout or failure.
    pub fn recordTimeout(self: *CircuitBreaker) void {
        self.consecutive_timeouts += 1;

        if (self.state == .half_open) {
            // Probe failed -- back to open.
            self.state = .open;
            self.opened_at = volt.time.Instant.now();
            return;
        }

        if (self.consecutive_timeouts >= self.threshold) {
            self.state = .open;
            self.opened_at = volt.time.Instant.now();
            std.debug.print(
                "Circuit opened after {d} consecutive timeouts\n",
                .{self.consecutive_timeouts},
            );
        }
    }
};
```

Try extending it: add a `recordError` variant that opens the circuit on non-timeout errors too, or add a callback that fires when the state transitions.

### Exercise 2: Timeout Budget Tracker

When a multi-step operation times out, it is often hard to tell *which* step was slow. A budget tracker logs the time consumed by each sub-operation so you can identify bottlenecks.

```zig
const std = @import("std");
const volt = @import("volt");

pub const BudgetTracker = struct {
    entries: [16]Entry = undefined,
    count: usize = 0,
    overall_start: volt.time.Instant,
    overall_budget: volt.Duration,

    pub const Entry = struct {
        label: []const u8,
        elapsed_ms: u64,
    };

    pub fn init(budget: volt.Duration) BudgetTracker {
        return .{
            .overall_start = volt.time.Instant.now(),
            .overall_budget = budget,
        };
    }

    /// Time a sub-operation and record how long it took.
    /// Returns the operation's result, or error.TimedOut if the
    /// overall budget has been exceeded.
    pub fn track(
        self: *BudgetTracker,
        label: []const u8,
        comptime op: fn () anyerror!void,
    ) !void {
        const start = volt.time.Instant.now();

        const result = op();

        const elapsed = start.elapsed();
        if (self.count < self.entries.len) {
            self.entries[self.count] = .{
                .label = label,
                .elapsed_ms = elapsed.asMillis(),
            };
            self.count += 1;
        }

        // Check overall budget after the operation completes.
        if (self.overall_start.elapsed().nanos >= self.overall_budget.nanos) {
            self.printReport();
            return error.TimedOut;
        }

        return result;
    }

    /// Print a breakdown showing which sub-operation consumed the
    /// most time. The slowest entry is marked for easy scanning.
    pub fn printReport(self: *const BudgetTracker) void {
        const total_ms = self.overall_start.elapsed().asMillis();

        // Find the slowest entry.
        var max_ms: u64 = 0;
        var max_idx: usize = 0;
        for (self.entries[0..self.count], 0..) |entry, i| {
            if (entry.elapsed_ms > max_ms) {
                max_ms = entry.elapsed_ms;
                max_idx = i;
            }
        }

        std.debug.print("--- Timeout Budget Report ({d} ms total) ---\n", .{total_ms});
        for (self.entries[0..self.count], 0..) |entry, i| {
            const marker: []const u8 = if (i == max_idx) " <-- SLOWEST" else "";
            std.debug.print("  {s}: {d} ms{s}\n", .{ entry.label, entry.elapsed_ms, marker });
        }
        std.debug.print("---\n", .{});
    }
};
```

Try extending it: make `track` generic so it can return a value, or add percentage-of-budget calculations to the report.

### Exercise 3: Strategy-Based Retry

Different error types deserve different retry strategies. A connection reset should be retried immediately (the server probably just restarted), while a 429 Too Many Requests response should back off aggressively, and a 400 Bad Request should never be retried at all.

```zig
const std = @import("std");
const volt = @import("volt");

pub const RetryStrategy = enum {
    /// Retry immediately with no delay. Use for transient
    /// connection errors where the server is likely back already.
    immediate,

    /// Retry with exponential backoff. Use for rate limits or
    /// server overload where the server needs time to recover.
    exponential_backoff,

    /// Do not retry. The error is permanent (bad input, auth
    /// failure, resource not found).
    no_retry,
};

pub fn classifyError(err: anyerror) RetryStrategy {
    return switch (err) {
        // Connection errors -- server may have restarted or a
        // transient network issue occurred. Retry quickly.
        error.ConnectionRefused,
        error.ConnectionReset,
        error.BrokenPipe,
        => .immediate,

        // Overload / rate limit -- back off to give the server
        // time to drain its queue.
        error.RateLimited,
        error.ServiceUnavailable,
        error.TimedOut,
        => .exponential_backoff,

        // Permanent errors -- retrying will not help.
        error.BadRequest,
        error.Unauthorized,
        error.NotFound,
        => .no_retry,

        // Unknown errors default to backoff as a safe bet.
        else => .exponential_backoff,
    };
}

pub fn retryWithStrategy(
    comptime T: type,
    overall_timeout: volt.Duration,
    max_retries: u32,
    comptime op: fn () anyerror!T,
) anyerror!T {
    var deadline = volt.time.Deadline.init(overall_timeout);
    var backoff_delay = volt.Duration.fromMillis(100);
    const max_delay = volt.Duration.fromSecs(10);

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    const random = prng.random();

    var attempt: u32 = 0;
    while (attempt <= max_retries) : (attempt += 1) {
        if (deadline.isExpired()) return error.TimedOut;

        if (op()) |result| {
            return result;
        } else |err| {
            const strategy = classifyError(err);

            switch (strategy) {
                .no_retry => return err,

                .immediate => {
                    // No delay, but yield the thread briefly so we
                    // do not spin if the server is still restarting.
                    std.Thread.sleep(1 * std.time.ns_per_ms);
                },

                .exponential_backoff => {
                    // Add jitter to prevent thundering herd.
                    const jitter_ns = random.uintLessThan(
                        u64,
                        @max(1, backoff_delay.nanos / 4),
                    );
                    const delay = backoff_delay.add(volt.Duration.fromNanos(jitter_ns));

                    // Clamp to remaining budget.
                    const remaining = deadline.remaining();
                    const actual = if (delay.nanos < remaining.nanos) delay else remaining;

                    volt.time.blockingSleep(actual);

                    // Grow the delay for next time.
                    backoff_delay = backoff_delay.mul(2);
                    if (backoff_delay.nanos > max_delay.nanos) {
                        backoff_delay = max_delay;
                    }
                },
            }

            if (attempt == max_retries) return err;
        }
    }

    return error.TimedOut;
}
```

Try extending it: add per-strategy retry limits (e.g., at most 2 immediate retries before switching to backoff), or integrate the circuit breaker from Exercise 1 so that repeated failures trip the breaker.

***

## Summary

| Pattern | Use when |
|---------|----------|
| Deadline check | Simple sequential operations with explicit checkpoints |
| Retry + backoff | Transient failures (network blips, overloaded services) |
| Deadline propagation | Multi-step request with a shared time budget |
| Cascading timeouts | Layered systems (proxy, gateway) with per-layer limits |
| Extending deadlines | Long transfers with idle detection (keep-alive) |
| Async timeout with `Io` | Async operations inside the runtime using `io.@"async"` |

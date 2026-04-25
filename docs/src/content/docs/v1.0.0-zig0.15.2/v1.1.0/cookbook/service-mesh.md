---
title: Service Mesh
description: A reverse proxy combining connection pooling, rate limiting, health
  checks, graceful shutdown, and config hot-reload into one application.
slug: v1.0.0-zig0.15.2/v110/cookbook/service-mesh
---

This is the capstone recipe. It combines patterns from across the cookbook into a single realistic application: a reverse proxy / API gateway that accepts client connections, routes them to backend servers through a connection pool, enforces per-client rate limits, monitors backend health, and shuts down gracefully.

If you have not read the foundation recipes, start there first. This recipe references patterns from:

* [Echo Server](/v1.0.0-zig0.15.2/cookbook/echo-server) -- TCP accept loop and per-connection tasks
* [Connection Pool](/v1.0.0-zig0.15.2/cookbook/connection-pool) -- semaphore-gated resource pool
* [Rate Limiter](/v1.0.0-zig0.15.2/cookbook/rate-limiter) -- token bucket with semaphore and interval
* [Parallel Tasks](/v1.0.0-zig0.15.2/cookbook/parallel-tasks) -- health checks with `race`
* [Graceful Drain](/v1.0.0-zig0.15.2/cookbook/graceful-drain) -- signal-triggered shutdown with `WorkGuard`
* [Config Hot-Reload](/v1.0.0-zig0.15.2/cookbook/config-hot-reload) -- live routing updates via `Watch`

:::tip[What you will learn]
- **Combining multiple patterns** into a cohesive production application
- **Reverse proxy structure** -- accept, route, forward, respond
- **Backend health monitoring** -- periodic probes with automatic failover
- **Per-client rate limiting** -- protecting backends from overload
- **Connection pooling** -- reusing backend connections with semaphore gating
- **Graceful drain** -- stopping safely under SIGINT/SIGTERM
:::

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

// -- Configuration ------------------------------------------------------------

const Config = struct {
    listen_port: u16 = 8080,
    backends: [MAX_BACKENDS]Backend = undefined,
    backend_count: usize = 0,
    max_conns_per_backend: usize = 32,
    rate_limit_per_sec: u32 = 100,
    health_check_interval_ms: u64 = 5000,
    drain_timeout_secs: u64 = 30,
};

const Backend = struct {
    name: [64]u8 = undefined,
    name_len: usize = 0,
    host: [64]u8 = undefined,
    host_len: usize = 0,
    port: u16 = 0,
    healthy: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),

    pub fn nameStr(self: *const Backend) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn hostStr(self: *const Backend) []const u8 {
        return self.host[0..self.host_len];
    }
};

const MAX_BACKENDS = 8;

// -- Gateway state ------------------------------------------------------------

const Gateway = struct {
    config: Config,
    shutdown: volt.shutdown.Shutdown,
    pool_sem: volt.sync.Semaphore,
    rate_tokens: std.atomic.Value(u32),
    request_count: std.atomic.Value(u64),

    pub fn init(config: Config) !Gateway {
        return .{
            .config = config,
            .shutdown = try volt.shutdown.Shutdown.init(),
            .pool_sem = volt.sync.Semaphore.init(
                config.max_conns_per_backend * config.backend_count,
            ),
            .rate_tokens = std.atomic.Value(u32).init(config.rate_limit_per_sec),
            .request_count = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *Gateway) void {
        self.shutdown.deinit();
    }
};

// -- Entry point --------------------------------------------------------------

pub fn main() !void {
    var config = Config{};
    addBackend(&config, "backend-1", "127.0.0.1", 9001);
    addBackend(&config, "backend-2", "127.0.0.1", 9002);

    var gw = try Gateway.init(config);
    defer gw.deinit();

    // Start background tasks.
    const health_thread = std.Thread.spawn(
        .{}, healthChecker, .{&gw},
    ) catch null;
    const refill_thread = std.Thread.spawn(
        .{}, rateLimitRefiller, .{&gw},
    ) catch null;

    // Run the accept loop inside the Volt runtime.
    try volt.run(acceptLoop, .{&gw});

    // Drain phase.
    std.debug.print("\nShutdown initiated.\n", .{});
    std.debug.print("Requests served: {d}\n", .{
        gw.request_count.load(.monotonic),
    });

    if (gw.shutdown.hasPendingWork()) {
        std.debug.print("Draining in-flight requests...\n", .{});
        if (gw.shutdown.waitPendingTimeout(
            volt.Duration.fromSecs(config.drain_timeout_secs),
        )) {
            std.debug.print("All requests drained.\n", .{});
        } else {
            std.debug.print("Drain timeout.\n", .{});
        }
    }

    if (health_thread) |t| t.detach();
    if (refill_thread) |t| t.detach();
    std.debug.print("Gateway stopped.\n", .{});
}

fn acceptLoop(io: volt.Io, gw: *Gateway) void {
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(gw.config.listen_port),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    std.debug.print("Gateway listening on :{d}\n", .{gw.config.listen_port});

    while (!gw.shutdown.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            var guard = gw.shutdown.startWork();

            var f = io.@"async"(handleClient, .{
                gw, result.stream, &guard,
            }) catch {
                handleClient(gw, result.stream, &guard);
                continue;
            };
            f.detach();
        } else {
            if (gw.shutdown.isShutdown()) break;
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

fn addBackend(config: *Config, name: []const u8, host: []const u8, port: u16) void {
    if (config.backend_count >= MAX_BACKENDS) return;
    var b = &config.backends[config.backend_count];
    const nl = @min(name.len, b.name.len);
    @memcpy(b.name[0..nl], name[0..nl]);
    b.name_len = nl;
    const hl = @min(host.len, b.host.len);
    @memcpy(b.host[0..hl], host[0..hl]);
    b.host_len = hl;
    b.port = port;
    config.backend_count += 1;
}

// -- Client handling ----------------------------------------------------------

fn handleClient(
    gw: *Gateway,
    conn: volt.net.TcpStream,
    guard: *volt.shutdown.WorkGuard,
) void {
    var stream = conn;
    defer stream.close();
    defer guard.deinit();

    // Step 1: Rate limiting -- consume a token.
    // If no tokens are available, reject immediately with 429.
    const tokens = gw.rate_tokens.load(.monotonic);
    if (tokens == 0) {
        stream.writeAll(
            "HTTP/1.1 429 Too Many Requests\r\n" ++
            "Content-Length: 19\r\n\r\nRate limit exceeded\n",
        ) catch {};
        return;
    }
    _ = gw.rate_tokens.fetchSub(1, .monotonic);

    // Step 2: Read the client request.
    var buf: [8192]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = stream.tryRead(buf[total..]) catch return orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) return;
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |_| break;
    }

    // Step 3: Select a healthy backend (round-robin).
    const backend = selectBackend(gw) orelse {
        stream.writeAll(
            "HTTP/1.1 502 Bad Gateway\r\n" ++
            "Content-Length: 22\r\n\r\nNo healthy backends.\n",
        ) catch {};
        return;
    };

    // Step 4: Acquire a connection pool permit.
    if (!gw.pool_sem.tryAcquire(1)) {
        stream.writeAll(
            "HTTP/1.1 503 Service Unavailable\r\n" ++
            "Content-Length: 18\r\n\r\nPool exhausted.\n",
        ) catch {};
        return;
    }
    defer gw.pool_sem.release(1);

    // Step 5: Forward request to backend.
    forwardRequest(stream, backend, buf[0..total]);

    _ = gw.request_count.fetchAdd(1, .monotonic);
}

fn selectBackend(gw: *Gateway) ?*Backend {
    // Simple round-robin: pick the next healthy backend.
    const count = gw.config.backend_count;
    const start = gw.request_count.load(.monotonic) % count;

    for (0..count) |offset| {
        const idx = (start + offset) % count;
        const backend = &gw.config.backends[idx];
        if (backend.healthy.load(.monotonic)) return backend;
    }
    return null;
}

fn forwardRequest(
    client: volt.net.TcpStream,
    backend: *Backend,
    request: []const u8,
) void {
    var client_conn = client;

    // Connect to the backend.
    const addr = volt.net.Address.fromHostPort(backend.hostStr(), backend.port);
    var upstream = volt.net.TcpStream.connect(addr) catch {
        client_conn.writeAll(
            "HTTP/1.1 502 Bad Gateway\r\n" ++
            "Content-Length: 20\r\n\r\nBackend unreachable\n",
        ) catch {};
        return;
    };
    defer upstream.close();

    // Forward the client's request to the backend.
    upstream.writeAll(request) catch {
        client_conn.writeAll(
            "HTTP/1.1 502 Bad Gateway\r\n" ++
            "Content-Length: 19\r\n\r\nUpstream write err\n",
        ) catch {};
        return;
    };

    // Read the backend's response and forward it to the client.
    var resp_buf: [16384]u8 = undefined;
    var resp_total: usize = 0;
    while (resp_total < resp_buf.len) {
        const n = upstream.tryRead(resp_buf[resp_total..]) catch break orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) break;
        resp_total += n;

        // For simplicity, forward as soon as we have data.
        // A production proxy would parse Content-Length or chunked encoding.
        client_conn.writeAll(resp_buf[0..resp_total]) catch return;
        resp_total = 0;
    }

    if (resp_total > 0) {
        client_conn.writeAll(resp_buf[0..resp_total]) catch {};
    }
}

// -- Health checking ----------------------------------------------------------

fn healthChecker(gw: *Gateway) void {
    while (!gw.shutdown.isShutdown()) {
        for (gw.config.backends[0..gw.config.backend_count]) |*backend| {
            const addr = volt.net.Address.fromHostPort(
                backend.hostStr(), backend.port,
            );

            const was_healthy = backend.healthy.load(.monotonic);
            const is_healthy = probeBackend(addr);
            backend.healthy.store(is_healthy, .monotonic);

            // Log state changes.
            if (was_healthy and !is_healthy) {
                std.debug.print("[health] {s} DOWN\n", .{backend.nameStr()});
            } else if (!was_healthy and is_healthy) {
                std.debug.print("[health] {s} UP\n", .{backend.nameStr()});
            }
        }

        std.Thread.sleep(gw.config.health_check_interval_ms * std.time.ns_per_ms);
    }
}

fn probeBackend(addr: volt.net.Address) bool {
    // TCP connect probe: if we can establish a connection, the backend
    // is considered alive.
    if (volt.net.TcpStream.connect(addr)) |conn| {
        var stream = conn;
        stream.close();
        return true;
    } else |_| {
        return false;
    }
}

// -- Rate limit refiller ------------------------------------------------------

fn rateLimitRefiller(gw: *Gateway) void {
    while (!gw.shutdown.isShutdown()) {
        std.Thread.sleep(1 * std.time.ns_per_s);

        // Refill tokens up to the configured limit.
        // This implements a simple token bucket: tokens are added at a
        // fixed rate and capped at the maximum.
        const limit = gw.config.rate_limit_per_sec;
        gw.rate_tokens.store(limit, .monotonic);
    }
}
```

## Walkthrough

### How the pieces fit together

The gateway is a composition of five independent patterns, each handling one concern:

| Concern | Pattern | Recipe |
|---------|---------|--------|
| Accept & route | TCP listener + task spawning | [Echo Server](/v1.0.0-zig0.15.2/cookbook/echo-server) |
| Connection reuse | Semaphore-gated permits | [Connection Pool](/v1.0.0-zig0.15.2/cookbook/connection-pool) |
| Rate limiting | Atomic token bucket | [Rate Limiter](/v1.0.0-zig0.15.2/cookbook/rate-limiter) |
| Backend health | Periodic TCP probes | [Parallel Tasks](/v1.0.0-zig0.15.2/cookbook/parallel-tasks) |
| Clean shutdown | WorkGuard + drain timeout | [Graceful Drain](/v1.0.0-zig0.15.2/cookbook/graceful-drain) |

Each pattern is self-contained. You could remove rate limiting (delete the token check) or health checking (delete the background thread) without affecting the other components. This composability is the point: production systems are built by layering simple patterns.

### Request flow

A client request passes through five gates before reaching a backend:

1. **Accept** -- `tryAccept` yields the TCP stream and a `WorkGuard` is registered.
2. **Rate limit** -- An atomic token is consumed. If no tokens remain, the client receives 429.
3. **Request read** -- The full HTTP request is buffered (up to 8 KiB).
4. **Backend selection** -- Round-robin picks the next healthy backend.
5. **Pool permit** -- A semaphore permit is acquired. If the pool is full, the client receives 503.
6. **Forward** -- The request is sent to the backend, and the response is streamed back.

Each gate can reject the request independently. This defense-in-depth means backends are protected from overload at multiple layers.

### Backend selection

The round-robin selector skips unhealthy backends by checking the atomic `healthy` flag. The flag is updated by the health checker thread asynchronously. This means there is a window (up to `health_check_interval_ms`) where a backend could be marked healthy but actually down. In practice this is fine because the `forwardRequest` function handles connection failures by returning 502.

For more sophisticated routing (weighted round-robin, least connections, consistent hashing), replace `selectBackend` with the appropriate algorithm.

### Rate limiting design

The rate limiter uses a simple token bucket: tokens are refilled every second up to the cap. Each request consumes one token atomically. This is a global rate limit (shared across all clients).

For per-client limiting, you would use a hash map from client IP to individual token counters. The [Rate Limiter](/v1.0.0-zig0.15.2/cookbook/rate-limiter) recipe shows this pattern in detail.

### Graceful drain

The shutdown sequence follows [Graceful Drain](/v1.0.0-zig0.15.2/cookbook/graceful-drain) exactly:

1. Signal caught -- `isShutdown()` returns true, accept loop exits.
2. No new connections accepted.
3. In-flight requests continue -- each has a `WorkGuard`.
4. `waitPendingTimeout` blocks until all guards are released or timeout.
5. Background threads are detached (they check `isShutdown` and exit).

## Variations

### Circuit breaker

Track backend failures and stop sending traffic after a threshold. After a cooldown period, allow one probe request through to see if the backend has recovered.

```zig
const CircuitState = enum { closed, open, half_open };

const CircuitBreaker = struct {
    state: std.atomic.Value(u8), // CircuitState as u8
    failure_count: std.atomic.Value(u32),
    last_failure_ts: std.atomic.Value(i64),

    const FAILURE_THRESHOLD = 5;
    const COOLDOWN_MS = 10_000;

    pub fn init() CircuitBreaker {
        return .{
            .state = std.atomic.Value(u8).init(@intFromEnum(CircuitState.closed)),
            .failure_count = std.atomic.Value(u32).init(0),
            .last_failure_ts = std.atomic.Value(i64).init(0),
        };
    }

    pub fn allowRequest(self: *CircuitBreaker) bool {
        const state: CircuitState = @enumFromInt(self.state.load(.monotonic));
        switch (state) {
            .closed => return true,
            .open => {
                // Check if cooldown has elapsed.
                const now = std.time.milliTimestamp();
                const last = self.last_failure_ts.load(.monotonic);
                if (now - last > COOLDOWN_MS) {
                    // Transition to half-open: allow one probe.
                    self.state.store(@intFromEnum(CircuitState.half_open), .monotonic);
                    return true;
                }
                return false;
            },
            .half_open => return false, // Only one probe at a time.
        }
    }

    pub fn recordSuccess(self: *CircuitBreaker) void {
        self.failure_count.store(0, .monotonic);
        self.state.store(@intFromEnum(CircuitState.closed), .monotonic);
    }

    pub fn recordFailure(self: *CircuitBreaker) void {
        const count = self.failure_count.fetchAdd(1, .monotonic) + 1;
        self.last_failure_ts.store(std.time.milliTimestamp(), .monotonic);
        if (count >= FAILURE_THRESHOLD) {
            self.state.store(@intFromEnum(CircuitState.open), .monotonic);
        }
    }
};
```

### Path-based routing

Route requests to different backend pools based on the URL path prefix. This turns the gateway into a basic API router.

```zig
const Route = struct {
    prefix: []const u8,
    backend_indices: [MAX_BACKENDS]usize,
    backend_count: usize,
};

const routes = [_]Route{
    .{
        .prefix = "/api/",
        .backend_indices = .{ 0, 1, undefined, undefined, undefined, undefined, undefined, undefined },
        .backend_count = 2,
    },
    .{
        .prefix = "/static/",
        .backend_indices = .{ 2, undefined, undefined, undefined, undefined, undefined, undefined, undefined },
        .backend_count = 1,
    },
};

fn routeByPath(gw: *Gateway, request: []const u8) ?*Backend {
    // Extract path from the request line.
    const line_end = std.mem.indexOf(u8, request, "\r\n") orelse return null;
    var parts = std.mem.splitScalar(u8, request[0..line_end], ' ');
    _ = parts.next(); // skip method
    const path = parts.next() orelse return null;

    // Find the matching route.
    for (routes) |route| {
        if (std.mem.startsWith(u8, path, route.prefix)) {
            // Select a healthy backend from this route's pool.
            for (route.backend_indices[0..route.backend_count]) |idx| {
                const backend = &gw.config.backends[idx];
                if (backend.healthy.load(.monotonic)) return backend;
            }
            return null; // All backends for this route are down.
        }
    }

    // No route matched -- fall back to default backend selection.
    return selectBackend(gw);
}
```

### Metrics collection

Track per-backend latency and request counts for monitoring. This data can be exposed as a `/metrics` endpoint for Prometheus-style scraping.

```zig
const BackendMetrics = struct {
    requests: std.atomic.Value(u64),
    errors: std.atomic.Value(u64),
    total_latency_ms: std.atomic.Value(u64),

    pub fn init() BackendMetrics {
        return .{
            .requests = std.atomic.Value(u64).init(0),
            .errors = std.atomic.Value(u64).init(0),
            .total_latency_ms = std.atomic.Value(u64).init(0),
        };
    }

    pub fn recordRequest(self: *BackendMetrics, latency_ms: u64, success: bool) void {
        _ = self.requests.fetchAdd(1, .monotonic);
        _ = self.total_latency_ms.fetchAdd(latency_ms, .monotonic);
        if (!success) {
            _ = self.errors.fetchAdd(1, .monotonic);
        }
    }

    pub fn avgLatencyMs(self: *const BackendMetrics) u64 {
        const reqs = self.requests.load(.monotonic);
        if (reqs == 0) return 0;
        return self.total_latency_ms.load(.monotonic) / reqs;
    }
};

// Usage in forwardRequest:
fn forwardRequestWithMetrics(
    client: volt.net.TcpStream,
    backend: *Backend,
    metrics: *BackendMetrics,
    request: []const u8,
) void {
    const start = volt.Instant.now();
    forwardRequest(client, backend, request);
    const latency = start.elapsed().asMillis();

    // For simplicity, we assume success if we got this far.
    // A production proxy would inspect the response status.
    metrics.recordRequest(latency, true);
}
```

## What to Build Next

Once you have the gateway working, consider adding:

* **TLS termination** -- decrypt incoming TLS connections and forward plain HTTP to backends
* **Request/response logging** -- structured JSON logs for each request with timing, status, and backend info
* **Connection keep-alive to backends** -- reuse upstream TCP connections across requests to reduce handshake overhead
* **Hot config reload** -- use a [Watch channel](/v1.0.0-zig0.15.2/cookbook/config-hot-reload) to update routing rules without restarting

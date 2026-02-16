---
title: Parallel Tasks
description: Concurrent task patterns with Group, race, and select for health checks, speculative execution, and fan-out work.
---

Most real applications need to do several things at once -- check multiple services, fetch from the fastest mirror, or split a batch across workers. Volt's task patterns (`io.@"async"`, `Group`, `race`, `select`) make these patterns safe and ergonomic. This recipe makes them the star of the show.

:::tip[What you will learn]
- **`io.@"async"` + `f.@"await"(io)`** -- spawn a task and await its result
- **`Group`** -- run N tasks in parallel and collect all results
- **`race`** -- wait for the first task to complete, cancel the rest
- **`select`** -- wait for the first task to complete, keep the rest running
- **Fan-out / scatter-gather** -- split work across workers and collect results
:::

## Complete Example

This example builds a health-check dashboard that probes multiple services concurrently and reports their status.

```zig
const std = @import("std");
const volt = @import("volt");

// -- Service definitions ------------------------------------------------------

const Service = struct {
    name: []const u8,
    host: []const u8,
    port: u16,
};

const services = [_]Service{
    .{ .name = "api",      .host = "127.0.0.1", .port = 8001 },
    .{ .name = "database",  .host = "127.0.0.1", .port = 5432 },
    .{ .name = "cache",     .host = "127.0.0.1", .port = 6379 },
    .{ .name = "search",    .host = "127.0.0.1", .port = 9200 },
};

const HealthResult = struct {
    name: []const u8,
    healthy: bool,
    latency_ms: u64,
};

// -- Entry point --------------------------------------------------------------

pub fn main() !void {
    try volt.run(dashboard);
}

fn dashboard(io: volt.Io) void {
    std.debug.print("=== Service Health Dashboard ===\n\n", .{});

    // ── Pattern 1: Group ────────────────────────────────────────────────
    // Check ALL services in parallel. Spawn one task per service
    // and await all results.
    std.debug.print("--- Check All (Group) ---\n", .{});
    checkAllServices(io);

    // ── Pattern 2: race ────────────────────────────────────────────────
    // Find the fastest responding service. race returns the first result
    // and cancels the remaining tasks.
    std.debug.print("\n--- Fastest Service (race) ---\n", .{});
    findFastestService(io);

    // ── Pattern 3: Collect with error handling ─────────────────────────
    // Check all services but tolerate partial failures. Collect both
    // successes and errors instead of short-circuiting.
    std.debug.print("\n--- Partial Failures ---\n", .{});
    checkWithPartialFailures(io);

    // ── Pattern 4: select ──────────────────────────────────────────────
    // Wait for the first response while keeping other checks running
    // in the background for logging.
    std.debug.print("\n--- First Response (select) ---\n", .{});
    selectFirstResponse(io);
}

// -- Pattern 1: Group (await all) ---------------------------------------------

fn checkAllServices(io: volt.Io) void {
    // Spawn a health check task for each service.
    var futures: [services.len]@TypeOf(io.@"async"(checkService, .{services[0]}) catch unreachable) = undefined;
    for (services, 0..) |svc, i| {
        futures[i] = io.@"async"(checkService, .{svc}) catch {
            std.debug.print("  Failed to spawn check for {s}\n", .{svc.name});
            return;
        };
    }

    // Await ALL checks. This blocks until every task has returned,
    // so the total wall-clock time equals the slowest check.
    var results: [services.len]HealthResult = undefined;
    for (&futures, 0..) |*f, i| {
        results[i] = f.@"await"(io) catch {
            results[i] = .{ .name = services[i].name, .healthy = false, .latency_ms = 0 };
            continue;
        };
    }

    // Print the results table.
    var healthy_count: usize = 0;
    for (results) |r| {
        const status: []const u8 = if (r.healthy) "UP" else "DOWN";
        std.debug.print("  {s:<12} {s:<6} {d}ms\n", .{
            r.name, status, r.latency_ms,
        });
        if (r.healthy) healthy_count += 1;
    }
    std.debug.print("  {d}/{d} services healthy\n", .{
        healthy_count, services.len,
    });
}

fn checkService(svc: Service) HealthResult {
    const start = volt.Instant.now();

    // Attempt a TCP connect as a health probe. If the connection
    // succeeds, the service is considered healthy.
    const addr = volt.net.Address.fromHostPort(svc.host, svc.port);
    if (volt.net.TcpStream.connect(addr)) |conn| {
        var stream = conn;
        stream.close();
        return .{
            .name = svc.name,
            .healthy = true,
            .latency_ms = start.elapsed().asMillis(),
        };
    } else |_| {
        return .{
            .name = svc.name,
            .healthy = false,
            .latency_ms = start.elapsed().asMillis(),
        };
    }
}

// -- Pattern 2: race ----------------------------------------------------------

fn findFastestService(io: volt.Io) void {
    // Spawn the same health check for each service, then race them.
    // The first task to complete wins; remaining tasks continue in
    // the background.
    var futures: [services.len]@TypeOf(io.@"async"(checkService, .{services[0]}) catch unreachable) = undefined;
    for (services, 0..) |svc, i| {
        futures[i] = io.@"async"(checkService, .{svc}) catch return;
    }

    // Await the first one to complete.
    const fastest = futures[0].@"await"(io) catch {
        std.debug.print("  race failed\n", .{});
        return;
    };
    for (futures[1..]) |*f| {
        var ff = f;
        ff.detach();
    }

    std.debug.print("  Fastest: {s} responded in {d}ms\n", .{
        fastest.name, fastest.latency_ms,
    });
}

// -- Pattern 3: Collect with error handling -----------------------------------

fn checkWithPartialFailures(io: volt.Io) void {
    var futures: [services.len]@TypeOf(io.@"async"(checkService, .{services[0]}) catch unreachable) = undefined;
    for (services, 0..) |svc, i| {
        futures[i] = io.@"async"(checkService, .{svc}) catch return;
    }

    // Await each future individually. Collect both successes and errors
    // instead of short-circuiting, so you always get a result for every task.
    var up: usize = 0;
    var down: usize = 0;
    for (&futures) |*f| {
        if (f.@"await"(io)) |r| {
            if (r.healthy) {
                up += 1;
            } else {
                down += 1;
            }
        } else |_| {
            down += 1;
        }
    }
    std.debug.print("  Up: {d}, Down: {d}\n", .{ up, down });
}

// -- Pattern 4: select --------------------------------------------------------

fn selectFirstResponse(io: volt.Io) void {
    var futures: [services.len]@TypeOf(io.@"async"(checkService, .{services[0]}) catch unreachable) = undefined;
    for (services, 0..) |svc, i| {
        futures[i] = io.@"async"(checkService, .{svc}) catch return;
    }

    // Await the first future. The remaining futures continue running
    // in the background. This is useful when you want the first result
    // for a fast response path but still want the other checks to
    // complete (e.g., for logging).
    const first = futures[0].@"await"(io) catch {
        std.debug.print("  select failed\n", .{});
        return;
    };
    for (futures[1..]) |*f| {
        var ff = f;
        ff.detach();
    }

    std.debug.print("  First response: {s} ({d}ms)\n", .{
        first.name, first.latency_ms,
    });

    // The remaining tasks will complete in the background.
    // You could join them later if needed.
}
```

## Walkthrough

### Await all: wait for everything

Spawning N futures with `io.@"async"` and awaiting each with `f.@"await"(io)` is the concurrent equivalent of a for loop. Instead of checking services one at a time (total time = sum of all latencies), it runs them all in parallel (total time = max latency). The result array has the same order as the input futures, so `results[0]` always corresponds to `futures[0]`.

Use this pattern when you need **all** results before you can proceed -- building a dashboard, aggregating data from multiple sources, or validating multiple preconditions.

### Race: first one wins

Spawn N futures and await the first one. Detach the rest so they continue in the background. This is ideal for:

- **Fastest mirror selection** -- send the same request to N mirrors, use the first response
- **Redundant health checks** -- if any path is healthy, the system is reachable
- **Timeout via racing** -- race your operation against a sleep task (see Variations)

Detached futures release their resources when they complete.

### Collect with error handling: tolerate partial failure

When you need a result for **every** task regardless of errors, await each future individually and handle errors per-task. This gives you fine-grained control over how each failure is handled:

- Success -- use the result
- Error -- record the failure and continue

This pattern is essential for dashboards, batch processing, and any scenario where partial results are better than no results.

### Select: first result, keep the rest

Await one future while detaching the remaining futures. The other tasks continue running in the background. Use this when:

- You want a fast initial response but still need the other results for logging or metrics
- You are implementing a "respond immediately, process in background" pattern
- You need to collect all results eventually but want to start acting on the first one

## Variations

### Scatter-gather with timeout

Race all health checks against a deadline. If no service responds within the timeout, report a failure immediately rather than waiting indefinitely.

```zig
const std = @import("std");
const volt = @import("volt");

fn checkAllWithTimeout(io: volt.Io, timeout: volt.Duration) ![services.len]HealthResult {
    var futures: [services.len]@TypeOf(io.@"async"(checkService, .{services[0]}) catch unreachable) = undefined;
    for (services, 0..) |svc, i| {
        futures[i] = try io.@"async"(checkService, .{svc});
    }

    // Use a deadline to enforce a hard upper bound on wall-clock time.
    var deadline = volt.time.Deadline.init(timeout);

    var results: [services.len]HealthResult = undefined;
    for (&futures, 0..) |*f, i| {
        if (deadline.isExpired()) return error.TimedOut;
        results[i] = f.@"await"(io) catch {
            results[i] = .{ .name = services[i].name, .healthy = false, .latency_ms = 0 };
            continue;
        };
    }

    return results;
}
```

### Speculative execution

Send the same request to two different backends. Use the first response and discard the other. This is the classic "hedged request" pattern used by systems like Google's Bigtable.

```zig
const std = @import("std");
const volt = @import("volt");

fn speculativeFetch(
    io: volt.Io,
    primary: volt.net.Address,
    secondary: volt.net.Address,
) ![]u8 {
    // Send the same request to both backends.
    var f1 = try io.@"async"(fetchFrom, .{primary});
    var f2 = try io.@"async"(fetchFrom, .{secondary});

    // Await the first one; detach the slower backend.
    const result = f1.@"await"(io);
    f2.detach();
    return result;
}

fn fetchFrom(addr: volt.net.Address) ![]u8 {
    var stream = try volt.net.TcpStream.connect(addr);
    defer stream.close();

    try stream.writeAll("GET /data HTTP/1.1\r\nConnection: close\r\n\r\n");

    var buf: [4096]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = stream.tryRead(buf[total..]) catch |err| return err orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) break;
        total += n;
    }
    return buf[0..total];
}
```

The cost is doubled network traffic, but latency drops to the minimum of the two backends. Use this pattern sparingly -- only for latency-critical paths where the extra load is acceptable.

### Fan-out work distribution

Split a large batch of items across N worker tasks and await all results. This is the concurrent equivalent of a parallel for loop.

```zig
const std = @import("std");
const volt = @import("volt");

const NUM_WORKERS = 4;

fn processBatchParallel(io: volt.Io, items: []const Item) ![NUM_WORKERS]BatchResult {
    // Divide items into roughly equal chunks.
    const chunk_size = (items.len + NUM_WORKERS - 1) / NUM_WORKERS;

    var futures: [NUM_WORKERS]@TypeOf(io.@"async"(emptyBatch, .{}) catch unreachable) = undefined;
    for (0..NUM_WORKERS) |i| {
        const start = i * chunk_size;
        const end = @min(start + chunk_size, items.len);
        if (start >= items.len) {
            // Fewer items than workers -- spawn a no-op.
            futures[i] = try io.@"async"(emptyBatch, .{});
        } else {
            futures[i] = try io.@"async"(processChunk, .{
                items[start..end],
            });
        }
    }

    // Await all workers. Total time = slowest chunk.
    var results: [NUM_WORKERS]BatchResult = undefined;
    for (&futures, 0..) |*f, i| {
        results[i] = try f.@"await"(io);
    }
    return results;
}

fn processChunk(chunk: []const Item) BatchResult {
    var total: u64 = 0;
    for (chunk) |item| {
        total += processItem(item);
    }
    return .{ .count = chunk.len, .total = total };
}

fn emptyBatch() BatchResult {
    return .{ .count = 0, .total = 0 };
}

const Item = struct { value: u64 };
const BatchResult = struct { count: usize, total: u64 };

fn processItem(item: Item) u64 {
    // Simulate CPU work.
    return item.value *% 31;
}
```

The chunk-based approach ensures even distribution. If items have highly variable processing times, consider using a channel-based work queue instead (see [Producer/Consumer](/cookbook/producer-consumer)) so fast workers can steal from slow ones.

## Choosing the Right Combinator

| Pattern | Waits for | On error | Remaining futures | Best for |
|---------|-----------|----------|-------------------|----------|
| Await all | All tasks | Per-task handling | All awaited | Aggregation, batch processing |
| Await all (partial) | All tasks | Collects all | All awaited | Dashboards, partial-failure tolerance |
| Race (await + detach) | First task | Returns first | Detached | Fastest mirror, timeout via racing |
| Select (await + detach) | First task | Returns first | Detached, can await later | Fast initial response, background work |

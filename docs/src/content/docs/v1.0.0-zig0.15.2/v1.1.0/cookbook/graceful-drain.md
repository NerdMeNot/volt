---
title: Graceful Drain
description: Signal-triggered shutdown with Shutdown and WorkGuard, connection
  draining, in-flight request tracking, and ordered cleanup.
slug: v1.0.0-zig0.15.2/v110/cookbook/graceful-drain
---

A production server must shut down cleanly: stop accepting new connections, wait for in-flight requests to finish, and release resources in the correct order. Volt's `Shutdown` module provides the building blocks.

:::tip[What you'll learn]
* Signal handling with `Shutdown` -- catching SIGINT and SIGTERM to initiate graceful shutdown
* `WorkGuard` for in-flight tracking -- RAII-based counter that never loses track of active requests
* Drain with timeout -- bounded waiting so shutdown never hangs indefinitely
* Ordered resource cleanup -- why teardown order matters and how `defer` enforces it
* Health check during drain -- returning 503 so load balancers stop sending traffic
:::

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

const log = std.log.scoped(.server);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // ── Phase 1: Initialize ──────────────────────────────────────────────
    //
    // Shutdown.init() registers signal handlers for SIGINT and SIGTERM.
    // This must happen before the accept loop so signals are caught from
    // the start. The defer ensures signal registration is cleaned up last,
    // after everything else has been torn down.
    var shutdown = volt.shutdown.Shutdown.init() catch |err| {
        std.debug.print("Failed to init signal handler: {}\n", .{err});
        return;
    };
    defer shutdown.deinit();

    var listener = try volt.net.listen("0.0.0.0:8080");
    defer listener.close();

    std.debug.print("Server listening on :8080 (Ctrl+C to stop)\n", .{});

    // ── Phase 2: Accept loop ─────────────────────────────────────────────
    var connection_count: u64 = 0;

    try volt.run(acceptLoop, .{ &shutdown, &listener, &connection_count });

    // ── Phase 3: Drain ───────────────────────────────────────────────────
    //
    // At this point the accept loop has exited. No new connections will be
    // accepted because we broke out of the while loop. Existing connections
    // are still being served by their spawned tasks.
    std.debug.print("\n--- Shutdown initiated ---\n", .{});

    if (shutdown.triggerSignal()) |sig| {
        std.debug.print("Signal: {}\n", .{sig});
    }

    std.debug.print("Connections served: {}\n", .{connection_count});
    std.debug.print("In-flight requests: {}\n", .{shutdown.pendingCount()});

    // Wait up to 30 seconds for in-flight work to complete.
    // The timeout is a hard upper bound -- if buggy handlers hang, the
    // server will still exit. In production, tune this to your longest
    // expected request duration plus a small buffer.
    if (shutdown.hasPendingWork()) {
        std.debug.print("Waiting for in-flight requests to finish...\n", .{});
        if (shutdown.waitPendingTimeout(volt.Duration.fromSecs(30))) {
            std.debug.print("All requests completed.\n", .{});
        } else {
            std.debug.print("Timeout! {} requests still pending.\n", .{
                shutdown.pendingCount(),
            });
        }
    }

    // ── Phase 4: Cleanup ─────────────────────────────────────────────────
    //
    // Zig's defer runs in reverse order of declaration:
    //   1. listener.close()   -- closes the listening socket
    //   2. shutdown.deinit()  -- releases signal handler registration
    //   3. gpa.deinit()       -- verifies no memory leaks (debug builds)
    //
    // This ordering is deliberate. The listener must close before the
    // shutdown handler is released, because closing the listener may
    // generate errors that reference shutdown state. The allocator check
    // runs last so it can catch leaks from any prior teardown step.
    std.debug.print("Server stopped.\n", .{});
}

fn acceptLoop(
    io: volt.Io,
    shutdown: *volt.shutdown.Shutdown,
    listener: *volt.net.TcpListener,
    connection_count: *u64,
) void {
    while (!shutdown.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            connection_count.* += 1;

            // CRITICAL ORDERING: Register in-flight work BEFORE spawning
            // the handler. If you called startWork() inside the handler,
            // there would be a race window between spawn returning and the
            // guard being created. During that window, the main thread
            // could observe pendingCount() == 0 and proceed with shutdown,
            // silently dropping the in-flight request. By calling
            // startWork() here in the accept loop, the pending count is
            // incremented synchronously before the task is even created,
            // so the drain phase can never miss it.
            var guard = shutdown.startWork();

            // Log the peer address for observability. This is useful in
            // production to correlate drain logs with specific clients.
            var addr_buf: [64]u8 = undefined;
            const addr_str = result.stream.peerAddr().ipString(&addr_buf);
            std.debug.print("[{}] Accepted connection from {s}\n", .{
                connection_count.*,
                addr_str,
            });

            var f = io.@"async"(handleConnection, .{
                result.stream,
                &guard,
                connection_count.*,
            }) catch {
                // If spawn fails, handle inline and clean up.
                // The guard is still valid -- its deinit will decrement
                // the pending count whether we run inline or in a task.
                handleConnection(result.stream, &guard, connection_count.*);
                continue;
            };
            f.detach();
        } else {
            // No pending connection. Poll shutdown more frequently than
            // a full sleep interval to keep latency low. The 10ms sleep
            // prevents busy-spinning while still responding to Ctrl+C
            // within a reasonable window.
            if (shutdown.isShutdown()) break;
            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }
}

fn handleConnection(
    conn: volt.net.TcpStream,
    guard: *volt.shutdown.WorkGuard,
    id: u64,
) void {
    var stream = conn;
    defer stream.close();

    // When this function exits -- whether normally, on error, or on
    // panic -- the work guard decrements the pending count. This is
    // what makes the drain phase aware that this connection is done.
    defer guard.deinit();

    var addr_buf: [64]u8 = undefined;
    const addr_str = stream.peerAddr().ipString(&addr_buf);
    std.debug.print("[{}] Handling connection from {s}\n", .{ id, addr_str });

    // Simulate request processing.
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.tryRead(&buf) catch return orelse {
            std.Thread.sleep(5 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) break;

        // Echo back (simulate work).
        stream.writeAll(buf[0..n]) catch return;
    }

    std.debug.print("[{}] Connection from {s} closed\n", .{ id, addr_str });
}
```

## Walkthrough

### Signal handling

`Shutdown.init()` registers handlers for `SIGINT` (Ctrl+C) and `SIGTERM` (process kill). The `isShutdown()` method polls for these signals with a non-blocking check: it first reads an atomic flag (fast path), then falls through to a non-blocking signal receive (slow path). Once a signal is detected, the atomic flag is set so all subsequent calls return immediately without touching the signal handler.

This two-tier design means the accept loop can call `isShutdown()` on every iteration without any syscall overhead after the first signal arrives.

### WorkGuard for tracking in-flight requests

`shutdown.startWork()` atomically increments a pending counter and returns a `WorkGuard`. When the guard's `deinit()` runs (via `defer`), it decrements the counter and signals the condition variable if the count reaches zero. This ensures the count is always accurate, even if the handler panics or returns early.

The key ordering constraint: call `startWork()` **before** spawning the handler task. This is not optional -- it is a correctness requirement. Consider the alternative:

1. Main thread calls `io.@"async"(handleConnection, ...)`.
2. Scheduler queues the task but has not run it yet.
3. Main thread checks `isShutdown()` -- a signal arrived.
4. Main thread calls `pendingCount()` -- returns 0 because the handler has not called `startWork()` yet.
5. Main thread proceeds with shutdown, dropping the connection.

By calling `startWork()` in the accept loop, step 4 will see a count of at least 1, and the drain phase will wait for the handler to finish.

### Drain with timeout

`waitPendingTimeout` blocks the main thread using a condition variable until either:

1. All work guards have been released (pending count reaches 0), or
2. The timeout expires (30 seconds in this example).

The timeout is a safety net. Without it, a single hung connection handler would prevent the server from ever exiting. After the timeout, the server proceeds with shutdown regardless -- long-running requests are terminated when the listener closes and streams are dropped by their owning tasks.

Choose a timeout value based on your longest expected request duration. For HTTP APIs, 30 seconds is a common default. For WebSocket servers or long-polling endpoints, you may need longer.

### Cleanup ordering

Resources are released in reverse initialization order using Zig's `defer`:

1. **Listener socket closes** -- stops accepting new connections. Any clients attempting to connect after this point receive a connection refused error.
2. **Shutdown handler releases signal registration** -- restores default signal behavior. This must happen after the listener closes because closing the listener may trigger error paths that check shutdown state.
3. **Allocator verifies no leaks** -- in debug builds, the GPA will report any allocations that were not freed. This runs last so it can catch leaks from any prior teardown step.

## Manual Trigger

You can trigger shutdown programmatically, for example from an admin endpoint:

```zig
fn adminHandler(shutdown: *volt.shutdown.Shutdown, stream: volt.net.TcpStream) void {
    var conn = stream;
    defer conn.close();

    var buf: [256]u8 = undefined;
    const n = conn.tryRead(&buf) catch return orelse return;

    if (std.mem.startsWith(u8, buf[0..n], "POST /shutdown")) {
        shutdown.trigger();
        conn.writeAll("HTTP/1.1 200 OK\r\n\r\nShutting down\n") catch {};
    }
}
```

Calling `shutdown.trigger()` sets the same atomic flag that signal delivery would. The accept loop's next call to `isShutdown()` will return `true`, and the drain phase proceeds identically. The `triggerSignal()` method will return `null` for manual triggers, which you can use to distinguish programmatic shutdown from signal-driven shutdown in your logs.

## Two-Phase Shutdown

For servers that need to stop accepting before draining, split the shutdown into phases:

```zig
fn twoPhaseShutdown(shutdown: *volt.shutdown.Shutdown) void {
    // Phase 1: Stop accepting, close listener.
    // (Handled by breaking out of the accept loop above.)

    // Phase 2: Let in-flight requests finish with a bounded wait.
    _ = shutdown.waitPendingTimeout(volt.Duration.fromSecs(30));

    // Phase 3: Force-close remaining connections.
    // (Streams are dropped when tasks exit. Any tasks still running
    // after the timeout will have their streams closed when the
    // runtime tears down.)
}
```

The two-phase approach is useful when your server has multiple listener sockets or when you need to perform intermediate cleanup (such as deregistering from a service discovery system) between stopping acceptance and draining.

## Health Check During Drain

While draining, health check endpoints should report the server as unhealthy so load balancers stop sending traffic:

```zig
fn healthCheck(shutdown: *volt.shutdown.Shutdown, stream: volt.net.TcpStream) void {
    var conn = stream;
    defer conn.close();

    if (shutdown.isShutdown()) {
        // Return 503 so the load balancer marks this instance as down
        // and stops routing new requests to it. Most load balancers
        // (ALB, nginx, HAProxy) will remove the instance from the pool
        // after 2-3 consecutive 503 responses.
        conn.writeAll("HTTP/1.1 503 Service Unavailable\r\n\r\n") catch {};
    } else {
        conn.writeAll("HTTP/1.1 200 OK\r\n\r\n") catch {};
    }
}
```

A common pattern is to start returning 503 as soon as shutdown is triggered, even before the drain phase begins. This gives the load balancer time to stop routing traffic while existing requests finish naturally.

## Cleanup Ordering Checklist

When shutting down a server, release resources in this order:

1. **Signal handler** -- stop accepting new signals
2. **Listener socket** -- stop accepting new connections
3. **In-flight connections** -- wait for drain, then force-close
4. **Background tasks** -- cancel or wait
5. **External connections** -- close database pools, upstream connections
6. **Channels** -- close to unblock any waiting consumers
7. **Allocator** -- verify no leaks (debug builds)

## Try it yourself

These exercises build on the complete example above. Each one adds a feature you will likely need in a production server.

### Second-signal force-kill

The first Ctrl+C starts a graceful drain. If the operator presses Ctrl+C again during the drain, the server should exit immediately without waiting for in-flight requests. This is a standard pattern in tools like Docker and systemd-managed services.

```zig
// Replace the Phase 3 drain block with this:

// ── Phase 3: Drain with force-kill support ─────────────────────────
std.debug.print("\n--- Shutdown initiated (Ctrl+C again to force exit) ---\n", .{});
std.debug.print("In-flight requests: {}\n", .{shutdown.pendingCount()});

if (shutdown.hasPendingWork()) {
    // Spawn a thread to watch for the second signal.
    // If the user presses Ctrl+C again, we exit immediately.
    const force_kill_thread = std.Thread.spawn(.{}, struct {
        fn run(s: *volt.shutdown.Shutdown) void {
            // The shutdown is already triggered, so we reset the
            // signal handler to catch the NEXT signal.
            s.signal_handler.reset() catch return;
            // Block until a second signal arrives.
            _ = s.signal_handler.recv() catch return;
            std.debug.print("\nForce exit -- not waiting for drain.\n", .{});
            std.process.exit(1);
        }
    }.run, .{&shutdown}) catch null;

    std.debug.print("Waiting for in-flight requests to finish...\n", .{});
    if (shutdown.waitPendingTimeout(volt.Duration.fromSecs(30))) {
        std.debug.print("All requests completed.\n", .{});
    } else {
        std.debug.print("Timeout! {} requests still pending.\n", .{
            shutdown.pendingCount(),
        });
    }

    // Clean up the force-kill thread if drain finished normally.
    if (force_kill_thread) |t| t.detach();
}
```

### Drain progress indicator

During a long drain, silence from the server can be unsettling. Print the remaining connection count every second so operators can see progress:

```zig
// Replace the drain waiting block with this:

if (shutdown.hasPendingWork()) {
    std.debug.print("Draining...\n", .{});

    const deadline = volt.Duration.fromSecs(30);
    const interval = volt.Duration.fromSecs(1);
    var elapsed = volt.Duration.fromSecs(0);

    while (shutdown.hasPendingWork() and elapsed.asSecs() < deadline.asSecs()) {
        if (shutdown.waitPendingTimeout(interval)) {
            std.debug.print("All requests completed.\n", .{});
            break;
        }
        elapsed = elapsed.add(interval);
        const remaining = shutdown.pendingCount();
        const secs_left = deadline.asSecs() - elapsed.asSecs();
        std.debug.print(
            "  ... {} connections still draining ({} seconds until timeout)\n",
            .{ remaining, secs_left },
        );
    } else {
        if (shutdown.hasPendingWork()) {
            std.debug.print("Timeout! {} requests still pending.\n", .{
                shutdown.pendingCount(),
            });
        }
    }
}
```

This produces output like:

```
Draining...
  ... 12 connections still draining (29 seconds until timeout)
  ... 8 connections still draining (28 seconds until timeout)
  ... 3 connections still draining (27 seconds until timeout)
All requests completed.
```

### Graceful channel shutdown

If your server uses channels for internal communication (for example, a work queue between the accept loop and a pool of worker tasks), you must close them before exiting. Otherwise, consumers blocked on `recv()` will hang forever:

```zig
const volt = @import("volt");

var work_queue: volt.channel.Channel(Request) = undefined;
var result_queue: volt.channel.Channel(Response) = undefined;

fn serverMain() !void {
    work_queue = try volt.channel.Channel(Request).init(allocator, 128);
    result_queue = try volt.channel.Channel(Response).init(allocator, 128);

    var shutdown = try volt.shutdown.Shutdown.init();
    defer shutdown.deinit();

    // ... accept loop, spawn workers ...

    // ── Drain phase ────────────────────────────────────────────────────
    std.debug.print("Stopping accept loop...\n", .{});

    // Step 1: Wait for in-flight requests to finish.
    _ = shutdown.waitPendingTimeout(volt.Duration.fromSecs(30));

    // Step 2: Close channels to unblock any consumers waiting on recv().
    // close() signals all waiting consumers with a ChannelClosed error,
    // allowing their tasks to exit cleanly. The order matters: close the
    // work queue first (producers stop), then the result queue (consumers
    // stop).
    work_queue.close();
    result_queue.close();

    // Step 3: Wait briefly for worker tasks to observe the close and exit.
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Step 4: Free channel memory.
    work_queue.deinit();
    result_queue.deinit();

    std.debug.print("All channels closed, server exiting.\n", .{});
}
```

The key insight is the ordering: drain in-flight requests first, then close channels, then free memory. If you close channels before draining, in-flight handlers that try to send results will get unexpected errors. If you free memory before closing, consumers will read from freed memory.

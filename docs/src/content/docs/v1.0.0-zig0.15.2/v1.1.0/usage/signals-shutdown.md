---
title: Signals & Shutdown
description: Handling OS signals and implementing graceful shutdown in Volt servers.
slug: v1.0.0-zig0.15.2/v110/usage/signals-shutdown
---

Production servers need to handle termination signals (Ctrl+C, `kill`) and shut down cleanly -- draining in-flight requests, closing connections, and flushing buffers. Volt provides `AsyncSignal` for signal handling and `Shutdown` for coordinating graceful shutdown.

## Signal handling

### AsyncSignal

`AsyncSignal` wraps OS signal delivery into an async-aware API with waiter-based notification.

#### Convenience constructors

```zig
const volt = @import("volt");

// Handle Ctrl+C (SIGINT)
var ctrl_c = try volt.signal.AsyncSignal.ctrlC();
defer ctrl_c.deinit();

// Handle SIGTERM (termination request)
var term = try volt.signal.AsyncSignal.terminate();
defer term.deinit();

// Handle SIGHUP (hangup/reload)
var hup = try volt.signal.AsyncSignal.hangup();
defer hup.deinit();

// Handle both SIGINT and SIGTERM (common for servers)
var shutdown_sig = try volt.signal.AsyncSignal.shutdown();
defer shutdown_sig.deinit();
```

#### Custom signal sets

```zig
var signals = volt.signal.SignalSet.empty();
signals.add(.SIGINT);
signals.add(.SIGTERM);
signals.add(.SIGHUP);

var handler = try volt.signal.AsyncSignal.init(signals);
defer handler.deinit();
```

#### Polling for signals

Non-blocking check:

```zig
if (try handler.tryRecv()) |sig| {
    switch (sig) {
        .SIGINT => log.info("Received SIGINT", .{}),
        .SIGTERM => log.info("Received SIGTERM", .{}),
        .SIGHUP => reloadConfig(),
        else => {},
    }
}
```

#### Async wait

Register a waiter that is woken when a signal arrives:

```zig
var waiter = volt.signal.SignalWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

if (!try handler.wait(&waiter)) {
    // No signal pending. Yield to scheduler.
    // When woken, waiter.isReady() is true.
}

if (waiter.signal()) |sig| {
    log.info("Received signal: {}", .{sig});
}
```

#### Signal future

`SignalFuture` implements the `Future` trait. To race an accept against a shutdown signal, spawn both as async tasks and check which completes first:

```zig
var future = volt.signal.signalFuture(&handler);

// Race accept against shutdown signal using async tasks
var accept_f = try io.@"async"(acceptTask, .{listener});
var signal_f = try io.@"async"(signalTask, .{&handler});
// First to complete wins -- check isDone()
```

#### Cancellation

```zig
handler.cancelWait(&waiter);
```

### Blocking signal helpers

For simple scripts that just need to wait for a signal:

```zig
// Block until Ctrl+C
try volt.signal.waitForCtrlC();

// Block until SIGINT or SIGTERM, returns which signal fired
const sig = try volt.signal.waitForShutdown();
```

### Event loop integration

`AsyncSignal` exposes a file descriptor (Unix) or event handle (Windows) for integration with I/O event loops:

```zig
const fd = handler.getFd();
// Register fd for READABLE events in your event loop.
// When readable, call:
try handler.handleReadable();
// This reads pending signals and wakes registered waiters.
```

***

## Shutdown coordinator

The `Shutdown` type combines signal handling with pending-work tracking for clean server shutdown.

### Initialization

```zig
var shutdown = try volt.shutdown.Shutdown.init();
defer shutdown.deinit();
```

This automatically listens for SIGINT and SIGTERM. For custom signals:

```zig
var signals = volt.signal.SignalSet.empty();
signals.add(.SIGINT);
signals.add(.SIGHUP);

var shutdown = try volt.shutdown.Shutdown.initWithSignals(signals);
defer shutdown.deinit();
```

### Checking shutdown state

```zig
while (!shutdown.isShutdown()) {
    // Accept and handle connections...
    if (try listener.tryAccept()) |result| {
        handleConnection(result.stream);
    }
}
```

`isShutdown` polls for signals non-blockingly and returns `true` once a signal is received or `trigger()` is called.

### Manual trigger

Trigger shutdown programmatically (e.g., from an HTTP `/shutdown` endpoint):

```zig
shutdown.trigger();

// Check what triggered it
if (shutdown.triggerSignal()) |sig| {
    log.info("Shutdown triggered by {}", .{sig});
} else {
    log.info("Shutdown triggered manually", .{});
}
```

### Shutdown future

For use with `Select` to race accept against shutdown:

```zig
var future = shutdown.future();
// ShutdownFuture implements poll/cancel
```

### Tracking pending work

The `WorkGuard` pattern tracks in-flight requests so the server can wait for them to complete before exiting:

```zig
fn handleConnection(shutdown_ref: *volt.shutdown.Shutdown, stream: TcpStream) void {
    // Register this work
    var guard = shutdown_ref.startWork();
    defer guard.deinit(); // Decrements pending count when done

    // Process the request
    processRequest(stream);
}
```

Query pending work:

```zig
shutdown.pendingCount();    // usize -- number of active WorkGuards
shutdown.hasPendingWork();  // bool
```

### Waiting for drain

After triggering shutdown, wait for all in-flight work to complete:

```zig
// Wait up to 30 seconds (default) for pending work
const all_done = shutdown.waitPending();
if (!all_done) {
    log.warn("Timed out waiting for pending work", .{});
}

// Custom timeout
const all_done2 = shutdown.waitPendingTimeout(Duration.fromSecs(60));
```

***

## Complete graceful shutdown pattern

```zig
const volt = @import("volt");
const std = @import("std");
const log = std.log;

pub fn main() !void {
    var shutdown = try volt.shutdown.Shutdown.init();
    defer shutdown.deinit();

    var listener = try volt.net.listen("0.0.0.0:8080");
    defer listener.close();

    log.info("Server listening on :8080. Press Ctrl+C to stop.", .{});

    // Accept loop
    while (!shutdown.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            // Track this connection
            var guard = shutdown.startWork();

            // Spawn handler task (in production, use io.@"async")
            handleRequest(result.stream);

            guard.complete(); // Mark work as done
        }
    }

    log.info("Shutdown signal received. Draining...", .{});

    // Wait for in-flight requests to finish
    const drained = shutdown.waitPendingTimeout(
        volt.time.Duration.fromSecs(30),
    );

    if (drained) {
        log.info("All requests completed. Shutting down.", .{});
    } else {
        log.warn("Timed out. {} requests still pending.", .{
            shutdown.pendingCount(),
        });
    }
}

fn handleRequest(stream: volt.net.TcpStream) void {
    var conn = stream;
    defer conn.close();
    // Process...
}
```

## CancelToken

The `CancelToken` is an internal primitive used by `Select` operations. It provides atomic "first to claim wins" semantics:

```zig
var token = volt.sync.cancel_token.CancelToken.init();

// Multiple branches race to claim
if (token.tryClaimWinner(branch_id)) {
    // This branch won
} else {
    // Another branch already claimed
}

token.isCancelledFor(branch_id); // true if this branch lost
```

`CancelToken` is primarily used inside `SelectContext` (see [Select](/v1.0.0-zig0.15.2/usage/select/)) and does not typically appear in application code.

## Platform notes

| Platform | Signal mechanism |
|----------|-----------------|
| Linux | `signalfd` -- file descriptor that receives signals |
| macOS | `kqueue` with `EVFILT_SIGNAL` |
| Windows | `SetConsoleCtrlHandler` with event objects |

The `AsyncSignal` abstraction hides these differences. On all platforms, signal delivery is converted into a waitable event that integrates with the async event loop.

---
title: TCP Echo Server
description: A complete TCP echo server with one coroutine per connection and bounded concurrency via Semaphore.
---

The full thing, then explanation:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(serve, .{}));
}

fn serve() !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();
    std.debug.print("echo server listening on :8080\n", .{});

    while (true) {
        const conn = try listener.accept();
        _ = try volt.spawn(handle, .{conn});
    }
}

fn handle(conn: volt.net.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

Test it:

```sh
zig build run                                # runs the program
echo "hello volt" | nc 127.0.0.1 8080         # in another terminal
# → hello volt
```

## Walkthrough

### Bootstrap

```zig
var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
defer rt.deinit();
try (try rt.run(serve, .{}));
```

Two calls. The runtime owns the worker pool, the reactor, the
parking lot, the slab arena. The driver thread becomes M[0] when
`rt.run` enters its dispatch loop and joins the worker pool. When
`serve` returns (normally or via error), `rt.run` returns; the
deferred `rt.deinit` tears everything down.

The double `try` is because `rt.run(fn, args)` returns
`!user_fn_return_type` — runtime errors on the outside, your fn's
errors on the inside. `serve` returns `!void`, so `rt.run` returns
`!!void`. The inner `try` strips the outer; `main` propagates the
inner.

### Bind + listen

```zig
var listener = try volt.net.TcpListener.bind(.any4(8080));
defer listener.close();
```

`.any4(8080)` is `0.0.0.0:8080` — listen on every interface. Use
`.loopback4(8080)` for local-only, or `Address.parse4("192.168.1.50", 8080)`
for a specific interface.

`bind` does the full setup: `socket()`, `setsockopt(SO_REUSEADDR)`,
`bind()`, `listen()` with backlog=128, set `O_NONBLOCK`. Errors
return on the spot.

### Accept loop

```zig
while (true) {
    const conn = try listener.accept();
    _ = try volt.spawn(handle, .{conn});
}
```

`listener.accept()` parks the calling coroutine on kqueue's
`EVFILT_READ` for the listener fd. The reactor wakes it when the
kernel says a connection is ready, and `accept` returns the new
`TcpStream`.

We spawn one coroutine per connection. `volt.spawn` returns a
`*Task(void)` (since `handle` returns void); we discard it with
`_ =`. The Task struct leaks per connection — fine if the
connection rate is bounded; see the "bounded concurrency"
variant below for the fix.

### Per-connection handler

```zig
fn handle(conn: volt.net.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

`s.read` and `s.writeAll` both park on the reactor (`EVFILT_READ` /
`EVFILT_WRITE`) when the syscall returns `EAGAIN`. Both retry the
syscall after the wake. The handler reads as straight-line
synchronous code.

`return` on any error or peer-close exits the loop; the deferred
`s.close()` runs; the coroutine completes. The runtime's `.done`
branch frees the coroutine + stack via the per-P pools.

## Variants

### Limit total connections (backpressure)

Use a Semaphore to bound concurrency:

```zig
fn serveBounded() !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();

    var sem = volt.Semaphore.init(1000);
    defer sem.deinit();

    while (true) {
        sem.acquire();                            // parks if full
        const conn = try listener.accept();
        _ = try volt.spawn(handleWithSem, .{ conn, &sem });
    }
}

fn handleWithSem(conn: volt.net.TcpStream, sem: *volt.Semaphore) void {
    defer sem.release();
    handle(conn);
}
```

Now the server accepts at most 1000 active connections. Beyond
that, `sem.acquire()` parks the accept loop until a handler
finishes.

### Drain on shutdown via Cancel

If you want the server to stop accepting new connections but
finish active ones, build a Cancel that fires on shutdown and
have the accept loop close the listener:

```zig
fn serveDrain() !void {
    var rt_runtime = volt.runtime();

    var c = volt.Cancel.init(rt_runtime);
    defer c.deinit();

    var listener = try volt.net.TcpListener.bind(.any4(8080));
    // Note: defer close fires AFTER the scope below, on shutdown path
    defer listener.close();

    // Spawn a watcher that closes the listener fd on cancel.
    // Closing the fd causes any in-flight accept() to return an
    // error, breaking the accept loop.
    _ = try volt.spawn(struct {
        fn watch(cancel: *volt.Cancel, l: *volt.net.TcpListener) void {
            // (Wait for cancel — uses a Notify wired to fire.)
            // For brevity, sketch only:
            // ... wait until cancel.isFired() ...
            l.close();
        }
    }.watch, .{ &c, &listener });

    // Run the accept loop until the listener errors:
    while (true) {
        const conn = listener.accept() catch break;
        _ = try volt.spawn(handle, .{conn});
    }

    // Existing handlers continue running until they hit EOF / error
    // on their own sockets.
}
```

(Wiring `Cancel` to wake the watcher cleanly needs a `Notify` or a
`Mpmc` since `Cancel` itself doesn't park-and-wake on demand.
The graceful-drain recipe shows the full shape.)

### Reading from a specific interface

```zig
const addr = try volt.net.Address.parse4("192.168.1.50", 8080);
var listener = try volt.net.TcpListener.bind(addr);
```

`parse4(host, port)` takes the host and port separately; returns
`error.InvalidAddress` on a bad host string.

## What's NOT here

- **Ctrl-C handling.** Volt's signal handler is internal
  (SIGSEGV-only). Wire `std.posix.sigaction` directly if you need
  graceful Ctrl-C; trigger `c.fire()` from the signal handler.
- **Per-connection deadlines.** No `withTimeout` primitive ships
  today; the [Timeout with Retry](/cookbook/timeout-retry/) recipe
  shows the scope+watchdog pattern.

## See also

- [Networking](/usage/networking/) — TcpListener / TcpStream / Address API.
- [Structured Concurrency](/usage/structured-concurrency/) — Cancel + scope.
- [Connection Pool](/cookbook/connection-pool/) — the inverse pattern.
- [Graceful drain](/cookbook/graceful-drain/) — full drain-on-shutdown shape.

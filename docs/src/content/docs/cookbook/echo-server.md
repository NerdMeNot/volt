---
title: TCP Echo Server
description: A complete TCP echo server with one coroutine per connection and graceful Ctrl-C shutdown.
---

The full thing, then explanation:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
}

fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();
    std.debug.print("echo server listening on :8080 (Ctrl+C to stop)\n", .{});

    var shutdown = try volt.signal.shutdown();
    defer shutdown.deinit();

    while (true) {
        if (shutdown.handler.read()) |_| {
            std.debug.print("\nshutdown signal received; draining\n", .{});
            return;
        } else |_| {}

        const conn = listener.accept() catch |err| switch (err) {
            error.Cancelled => return,
            else => return err,
        };
        _ = try volt.launch(handle, .{conn});
    }
}

fn handle(conn: volt.io.TcpStream) void {
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
zig build run-echo                            # runs from examples/echo_server.zig
echo "hello volt" | nc 127.0.0.1 8080         # in another terminal
# → hello volt
```

## Walkthrough

### Bootstrap

```zig
try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
```

One call. The runtime owns the worker pool, the reactor, the
injection queue, and the per-task stacks. When `serve` returns
(either normally or via error), the runtime tears down.

### Bind + listen

```zig
var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
defer listener.close();
```

`any4(8080)` is `0.0.0.0:8080` — listen on every interface. Use
`loopback4(8080)` if you only want local connections, or
`parse("192.168.1.50:8080")` for a specific interface.

`bind` does the full setup: non-blocking socket, `SO_REUSEADDR`,
bind, listen with default backlog. Errors return on the spot.

### Shutdown listener

```zig
var shutdown = try volt.signal.shutdown();
defer shutdown.deinit();
```

`shutdown()` watches `SIGINT` + `SIGTERM`. The handler exposes a
non-blocking `.read()` that returns `error.WouldBlock` when no
signal has fired and a SignalSet when one has. We poll it at the
top of every accept iteration.

### Accept loop

```zig
while (true) {
    if (shutdown.handler.read()) |_| return else |_| {}

    const conn = listener.accept() catch |err| switch (err) {
        error.Cancelled => return,
        else => return err,
    };
    _ = try volt.launch(handle, .{conn});
}
```

`listener.accept()` suspends the calling coroutine on the listener
fd. The reactor wakes it when the kernel says a connection is
ready. We then spawn one coroutine per connection — `volt.launch`
returns a `*Job` that we deliberately discard with `_ =`; the
runtime will reap the coroutine on its own when it finishes.

The `error.Cancelled` arm handles the case where the runtime is
shutting down — the listener wait gets cancelled and `accept`
surfaces it.

### Per-connection handler

```zig
fn handle(conn: volt.io.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;                         // peer closed
        s.writeAll(buf[0..n]) catch return;
    }
}
```

`s.read` and `s.writeAll` both suspend on `EWOULDBLOCK`, get woken
by the reactor when ready, retry the syscall. The handler reads
like a synchronous program.

`return` on any error or peer-close exits the loop, the `defer
s.close()` runs, and the coroutine completes. The runtime's
`Done.subscribe` recycles its stack into the slab pool and frees
the `Coroutine` struct.

## Variants

### Limit total connections (backpressure)

Use a Semaphore to bound concurrency:

```zig
var sem = volt.sync.Semaphore.init(1000);

while (true) {
    sem.acquire(1);
    const conn = try listener.accept();
    _ = try volt.launch(handleWithSem, .{ conn, &sem });
}

fn handleWithSem(conn: volt.io.TcpStream, sem: *volt.sync.Semaphore) void {
    defer sem.release(1);
    handle(conn);
}
```

Now the server accepts at most 1000 active connections. Beyond
that, `sem.acquire(1)` parks the accept loop until a handler
finishes.

### Drain (don't kill) on shutdown

If you want active handlers to **finish their current request**
before the program exits, wrap spawning in a `volt.scope`:

```zig
fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            var shutdown = try volt.signal.shutdown();
            defer shutdown.deinit();

            while (true) {
                if (shutdown.handler.read()) |_| return else |_| {}
                const conn = listener.accept() catch |err| switch (err) {
                    error.Cancelled => return,
                    else => return err,
                };
                try s.spawn(handle, .{conn});
            }
        }
    }.body);
    // After this returns: every handler has finished.
}
```

The scope joins every spawned handler before returning. New
connections aren't accepted (the loop has exited); existing ones
finish their work.

### Read deadlines

Wrap `handle` in a per-connection timeout:

```zig
fn handle(conn: volt.io.TcpStream) void {
    volt.withTimeout(volt.Duration.fromSecs(60), handleInner, .{conn}) catch {};
}

fn handleInner(conn: volt.io.TcpStream) !void {
    var s = conn;
    defer s.close();
    // ... blocking handler ...
}
```

If the connection hangs idle for 60s, the timeout fires; cancel
propagates through `s.read` (Park-cancellable through any wait
point); `handle` exits cleanly.

## Source

[`examples/echo_server.zig`](https://github.com/NerdMeNot/volt/blob/main/examples/echo_server.zig) in the repo.

```sh
zig build run-echo
```

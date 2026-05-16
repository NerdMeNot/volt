---
title: Talk to the network
description: TCP echo, end-to-end. The reactor mental model. Why your code reads like blocking I/O when nothing actually blocks.
---

Spawn and join you've seen. The point of a coroutine runtime is
**concurrent I/O**, so this page builds a TCP echo server and a
client that talks to it — both written in the same synchronous
style.

## The server

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(echoServer, .{}));
}

fn echoServer() !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();
    std.debug.print("listening on 0.0.0.0:8080\n", .{});
    while (true) {
        const conn = try listener.accept();
        _ = try volt.spawn(echoOne, .{conn});
    }
}

fn echoOne(conn: volt.net.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;          // peer closed
        s.writeAll(buf[0..n]) catch return;
    }
}
```

```sh
zig build run
# listening on 0.0.0.0:8080
```

In another terminal:

```sh
echo "hello volt" | nc 127.0.0.1 8080
# hello volt
```

## What "suspends at every wait point" means

Three calls in the server suspend the calling coroutine:

| Call | What it suspends on |
|---|---|
| `listener.accept()` | kqueue `EVFILT_READ` on the listener fd — wakes when a SYN arrives. |
| `s.read(&buf)` | kqueue `EVFILT_READ` on the connection fd — wakes when data is buffered. |
| `s.writeAll(buf)` | If the kernel send buffer is full, kqueue `EVFILT_WRITE` — wakes when there's room. |

While `echoServer` is parked on `accept()`, the worker thread that
was running it is free to run something else. While `echoOne` is
parked on `read()`, same. **The worker thread never blocks the
kernel call itself.** Volt's sockets are `O_NONBLOCK`; the runtime
gets `EAGAIN`, registers a kqueue event, and parks the coroutine.

This is what the README means by "code reads like blocking I/O" —
the API is synchronous, the implementation is event-driven.

## Spawn per connection

```zig
const conn = try listener.accept();
_ = try volt.spawn(echoOne, .{conn});
```

`echoServer` accepts a connection, then spawns a fresh coroutine to
handle just that one connection. The parent goes back to `accept`;
the child reads and writes that socket until the peer closes. One
coroutine per connection.

At 16 KiB per coroutine and `max_concurrent_stacks = 16384`
(default), this server can hold 16k concurrent connections out of
the box. Bump `max_concurrent_stacks` in `Config` if you need more —
each slot reserves 256 KiB of virtual address space (cheap on 64-bit)
and only the active top page is committed (16 KiB RSS each).

## Reactor mental model

```
        ┌─────────────────────────────────┐
        │      volt.net.TcpStream         │
        │   read / writeAll / etc.        │
        └────────────┬────────────────────┘
                     │ EAGAIN
                     ▼
        ┌─────────────────────────────────┐
        │  register kqueue EVFILT_READ    │
        │  park current coroutine         │
        └────────────┬────────────────────┘
                     │
                     ▼
              (worker runs other coros)
                     │
                     ▼  kernel: "fd ready"
        ┌─────────────────────────────────┐
        │  reactor.poll() → unpark coro   │
        │  → coro back on a worker queue  │
        │  → context.swap into coro stack │
        └────────────┬────────────────────┘
                     │
                     ▼
              read / write returns
```

One worker at a time claims "reactor poller" via a single CAS; it
polls `kevent`, dispatches woken coroutines back to a worker
queue, then releases the claim. The other workers do regular
work-stealing in parallel. See [The kqueue reactor](/architecture/)
for the design.

## Cancellation across I/O

If you have a `*Cancel` token, every I/O op has a cancel-aware
variant — or you can wire the standard variant under a `Cancel`
via `volt.scope`:

```zig
fn handleWithDeadline(conn: volt.net.TcpStream) !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            // Watchdog: cancel after 30 seconds.
            _ = try volt.spawn(watchdog, .{ c, 30 * std.time.ns_per_s });
            try doEcho(conn, c);
        }
    }.body);
}

fn watchdog(c: *volt.Cancel, ns: u64) void {
    volt.sleep(ns);
    c.fire();
}
```

When `c.fire()` runs, any coroutine parked on a cancel-aware blocking
op (e.g. a `recvCancel`) wakes with `error.Cancelled`. The scope's
`body` then returns with the error, and `scope` fires the Cancel
again to propagate cleanup. (Cancel-aware I/O ops are part of the
public surface; see [Networking](/usage/networking/).)

## A client

For symmetry, here's a client that connects to the echo server,
sends one message, prints the reply, and exits:

```zig
fn echoClient() !void {
    var s = try volt.net.TcpStream.connect(.loopback4(8080));
    defer s.close();

    try s.writeAll("hello volt\n");

    var buf: [128]u8 = undefined;
    const n = try s.read(&buf);
    std.debug.print("server said: {s}", .{buf[0..n]});
}
```

`connect` parks on `EVFILT_WRITE` until the kernel finishes the TCP
handshake. `writeAll` parks on `EVFILT_WRITE` if the send buffer is
full. `read` parks on `EVFILT_READ` until data arrives. Three
suspension points, none visible at the call site.

## What's next

You now have the model: spawn coroutines for concurrent work, the
runtime parks them on the reactor when they wait, the worker thread
stays available the entire time. Everything else in the docs is
either:

- **Recipes** — concrete patterns built from these pieces ([Cookbook](/cookbook/)).
- **API reference** — the surface of each type ([API Reference](/usage/runtime/)).
- **Architecture** — how the pieces fit together inside ([Architecture](/architecture/)).

Recommended next reads in order:

- [Basic concepts](/getting-started/basic-concepts/) — explicit model.
- [The Runtime](/usage/runtime/) — `Config`, lifetime, knobs.
- [Channels](/usage/channels/) — when you need coroutines to talk
  to each other.
- [The M:N scheduler](/architecture/mn-scheduler/) — once you're
  curious how this is actually built.

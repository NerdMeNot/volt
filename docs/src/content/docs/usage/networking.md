---
title: Networking
description: TcpListener, TcpStream, Address — coroutine-aware TCP I/O.
---

Volt's networking surface is intentionally small for v1.0: TCP
sockets and an `Address` type. UDP and Unix sockets are planned;
TLS is out of scope (build it on top of `TcpStream`).

## Address

`std.net.Address`-aligned constructors. Pick the form that matches
how you have the address.

```zig
// Explicit octets:
const a = volt.io.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);
const a6 = volt.io.Address.initIp6(.{0} ** 16, 8080);

// Common defaults:
const lo = volt.io.Address.loopback4(8080);   // 127.0.0.1
const lo6 = volt.io.Address.loopback6(8080);  // ::1
const any = volt.io.Address.any4(8080);       // 0.0.0.0 — listen on every interface
const any6 = volt.io.Address.any6(8080);      // ::

// String parsing:
const a = try volt.io.Address.parseIp4("192.168.1.50", 8080);
const a = try volt.io.Address.parse("127.0.0.1:8080");
const a = try volt.io.Address.parse("[::1]:9090");

// Inspection:
a.family();       // posix.AF.INET | AF.INET6
a.getPort();      // u16, native byte order
a.osSockLen();    // for bind/connect/accept
```

`parse` recognizes `host:port` for IPv4 and `[host]:port` for IPv6.
Currently the IPv6 string parser only handles `::1` and `::`;
full IPv6 text parsing is planned. For non-trivial IPv6 use
`initIp6` directly.

## TcpListener

```zig
var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
defer listener.close();

while (true) {
    const conn = try listener.accept();   // suspends; reactor wakes us
    _ = try volt.launch(handle, .{conn});
}
```

`bind(addr)`:

- Creates a non-blocking socket.
- Sets `SO_REUSEADDR`.
- Binds to `addr`.
- Listens with default backlog.

Returns a `TcpListener` you must `close()` (or let the runtime tear
down at shutdown).

`accept()` suspends the calling coroutine on the listener fd until
the kernel says a connection is ready, then returns a
`TcpStream`. The new stream is also non-blocking and registered
with the reactor.

```zig
const local: volt.io.Address = try listener.localAddress();
// Useful when you bound port=0 (kernel-assigned port).
```

## TcpStream

```zig
fn handle(conn: volt.io.TcpStream) void {
    var s = conn;
    defer s.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;                       // peer closed
        s.writeAll(buf[0..n]) catch return;
    }
}
```

`TcpStream` methods (all suspend on `WouldBlock`):

| Method | Description |
|---|---|
| `read(&buf) !usize` | Read up to `buf.len` bytes; returns 0 on peer close |
| `write(buf) !usize` | Write up to `buf.len` bytes; returns count actually written |
| `writeAll(buf) !void` | Write the entire buffer, looping over partial writes |
| `close()` | Close the fd; remove from reactor |

Every read/write registers a wait on the reactor when the kernel
returns `EWOULDBLOCK`. The reactor wakes the coroutine when the
kernel signals readiness; the loop retries the syscall.

### Connecting

```zig
var stream = try volt.io.TcpStream.connect(volt.io.Address.parse("127.0.0.1:8080") catch unreachable);
defer stream.close();
try stream.writeAll("ping");
```

`connect(addr)` does a non-blocking `connect()`, suspends on
writability if the kernel returned `EINPROGRESS`, then checks
`SO_ERROR` to surface connection failures. Returns the connected
stream.

## Cancellation through I/O

Cancelling a coroutine parked on `read` / `accept` / `connect`
unparks it; the next syscall returns `error.Cancelled`. This is
how `volt.withTimeout` cancels a hung connection:

```zig
const data = volt.withTimeout(
    volt.Duration.fromSecs(5),
    readAll,
    .{&stream},
) catch |err| switch (err) {
    error.Timeout => return error.ConnectionTimedOut,
    else => return err,
};
```

You don't need a deadline socket option or per-syscall timeout.

## Server pattern with graceful shutdown

```zig
fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    var shutdown = try volt.signal.shutdown();
    defer shutdown.deinit();

    while (true) {
        // Non-blocking shutdown check.
        if (shutdown.handler.read()) |_| return else |_| {}

        const conn = listener.accept() catch |err| switch (err) {
            error.Cancelled => return,
            else => return err,
        };
        _ = try volt.launch(handle, .{conn});
    }
}
```

For a more elegant shutdown that uses `volt.select`, see the
[graceful drain cookbook](/cookbook/graceful-drain/).

## What's NOT here

- **TLS / SSL** — build on top of `TcpStream`. A `volt-tls` crate
  is planned.
- **DNS resolution** — would block the worker. Use
  `volt.spawnBlocking` with `getaddrinfo` if you need it. A
  proper async DNS resolver is planned.
- **HTTP** — Volt is a runtime, not a framework. `volt-http` is a
  separate library.
- **UDP / Unix sockets** — planned, not yet shipped.

## Low-level access

If you need to register a non-TCP fd with the reactor (custom
syscalls, FFI), `volt.io.lowlevel.*` exposes the building blocks:

```zig
try volt.io.lowlevel.setNonblock(my_fd);
try volt.io.lowlevel.waitReadable(my_fd);
const n = try volt.io.lowlevel.read(my_fd, &buf);
```

Most application code shouldn't touch these; they're for
integration and library authors.

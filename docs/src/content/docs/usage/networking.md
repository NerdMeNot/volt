---
title: Networking
description: TcpListener, TcpStream, Address — TCP/IPv4 with non-blocking sockets parked on the reactor (kqueue / epoll / io_uring / IOCP). That's the network surface today.
---

Volt ships **TCP/IPv4** across every reactor backend (kqueue on
Darwin, epoll and io_uring on Linux, IOCP on Windows). That's the
network surface: `volt.net.TcpListener`, `volt.net.TcpStream`,
`volt.net.Address`. UDP, IPv6, Unix sockets, DNS resolution, TLS —
all live in downstream libraries by design (see [Roadmap](/appendix/roadmap/)).

The whole API is non-blocking under the hood; sockets are
`O_NONBLOCK`, and `EAGAIN` yields to the reactor.

## `volt.net.Address`

```zig
pub const Address = struct {
    pub fn loopback4(port: u16) Address { ... }                          // 127.0.0.1:port
    pub fn any4(port: u16) Address { ... }                                // 0.0.0.0:port
    pub fn parse4(host: []const u8, port: u16) error{InvalidAddress}!Address { ... }
};
```

Constructors are explicit about IPv4 (`*4` suffix) so adding IPv6
later doesn't require renaming. `parse4` accepts dotted-quad
("192.168.1.10"); anything else returns `error.InvalidAddress`.

```zig
const a = volt.net.Address.loopback4(8080);
const b = volt.net.Address.any4(8080);
const c = try volt.net.Address.parse4("192.168.1.10", 5432);
```

## `volt.net.TcpListener`

```zig
var listener = try volt.net.TcpListener.bind(.any4(8080));
defer listener.close();

while (true) {
    const conn = try listener.accept();
    _ = try volt.spawn(handle, .{conn});
}
```

`bind(addr)` returns a TcpListener with the socket bound, listening
(`backlog = 128`), `SO_REUSEADDR` set, and `O_NONBLOCK` on. The
underlying fd is owned by the TcpListener; `close()` releases it.

`accept()` parks the coroutine on kqueue `EVFILT_READ` until the
kernel says "incoming connection ready", then `accept4()`s it and
returns a `TcpStream`. The returned stream is also `O_NONBLOCK`.

```zig
const addr = try listener.localAddress();
```

`localAddress` returns the actual bound address. Useful when you
bound to port 0 (let the kernel pick) and want to print the port
to your client.

## `volt.net.TcpStream`

```zig
pub const TcpStream = struct {
    pub fn connect(addr: Address) !TcpStream { ... }
    pub fn close(self: *TcpStream) void { ... }
    pub fn read(self: *TcpStream, buf: []u8) !usize { ... }
    pub fn readFull(self: *TcpStream, buf: []u8) !usize { ... }
    pub fn write(self: *TcpStream, buf: []const u8) !usize { ... }
    pub fn writeAll(self: *TcpStream, buf: []const u8) !void { ... }
};
```

### `connect(addr)`

Opens an `O_NONBLOCK` socket, issues `connect()`, and parks on
`EVFILT_WRITE` until the handshake completes. Returns a connected
`TcpStream`.

```zig
var s = try volt.net.TcpStream.connect(.loopback4(8080));
defer s.close();
```

### `read(buf)`

```zig
var buf: [4096]u8 = undefined;
const n = try s.read(&buf);
if (n == 0) {
    // peer closed
} else {
    process(buf[0..n]);
}
```

Reads up to `buf.len` bytes. Returns the count. Parks on
`EVFILT_READ` until data is buffered. `n == 0` means peer closed
the connection cleanly.

### `readFull(buf)`

Reads until `buf` is full or peer closes. Returns the number of
bytes actually read (will be `buf.len` on success, less on EOF).
Saves a loop around `read`.

### `write(buf)` / `writeAll(buf)`

```zig
try s.writeAll(message);
```

`write` returns how many bytes were sent in one syscall (may be
short if the kernel buffer is full). `writeAll` loops until every
byte is sent, parking on `EVFILT_WRITE` between iterations.

### Error vocabulary

| Error | When |
|---|---|
| `error.BindFailed` | bind() syscall returned -1 |
| `error.ListenFailed` | listen() syscall returned -1 |
| `error.AcceptFailed` | accept4() returned an error other than EAGAIN |
| `error.ConnectFailed` | connect() failed; kernel returned an error after handshake |
| `error.SocketCreateFailed` | socket() returned -1 |
| `error.FcntlGetFailed` / `error.FcntlSetFailed` | O_NONBLOCK probe / set failed |
| `error.GetSockNameFailed` | localAddress query failed |

No `error.Closed` from network ops — TCP doesn't distinguish "this
end closed" from "this end is at EOF". Use `n == 0` from `read` as
the EOF signal.

## Full echo server

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
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

One coroutine per connection, 16 KiB committed RSS per connection,
N workers cycling through the reactor's wake queue. Works the same
on workers=1 and workers=`getCpuCount()`.

## What's NOT here

Out of scope for Volt core — these live in downstream libraries:

- **UDP** — different syscall surface; no fan-out from one fd to N
  reading coroutines without buffering.
- **IPv6** — `Address.any6` / `loopback6` / `parse6` will land
  with the wider net library.
- **Unix domain sockets** — same shape as TCP but different
  syscall.
- **DNS resolution** — async DNS is its own substantial design
  (cancellable name lookups, EDNS, caching).
- **TLS** — needs C interop (BoringSSL / rustls / OpenSSL), opt-in
  via a `volt-tls` extension.
- **HTTP** — too large for runtime core.

See [Roadmap](/appendix/roadmap/) for what lives where.

## Cancellation

`accept` / `read` / `writeAll` aren't cancel-aware at the API level
today. To cancel an in-flight I/O op, fire a Cancel that some
other coroutine observes, and have it `close()` the stream — the
in-flight parked op will wake with an error from the kernel (the
fd is closed under it). This is the workaround until cancel-aware
I/O variants land.

```zig
fn handleWithCancel(conn: volt.net.TcpStream, c: *volt.Cancel) void {
    var s = conn;
    defer s.close();
    // Spawn a watcher that closes s when c fires:
    _ = volt.spawn(struct {
        fn watch(stream: *volt.net.TcpStream, cancel: *volt.Cancel) void {
            // ... wait on cancel via a notify ...
            stream.close();
        }
    }.watch, .{ &s, c }) catch {};
    // ... use s.read / s.writeAll normally ...
}
```

Cancel-aware I/O (`readCancel`, `writeAllCancel`, `acceptCancel`)
is on the roadmap; for now, the close-the-fd pattern works.

## See also

- [The reactor](/architecture/reactor/) — how parking on read-readiness actually works (kqueue / epoll / io_uring / IOCP).
- [The reactor backends](/architecture/reactor-backends/) — per-platform syscall walkthroughs.
- [Recipes: TCP echo server](/cookbook/echo-server/) — production-shape echo with graceful shutdown.
- [Recipes: connection pool](/cookbook/connection-pool/) — sharing a pool of TcpStreams across coroutines.

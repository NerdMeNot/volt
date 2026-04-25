---
title: Networking
description: TCP, UDP, and Unix domain socket networking with Volt.
slug: v1.0.0-zig0.15.2/usage/networking
---

The `net` module provides production-grade networking with non-blocking I/O, async futures, and full socket option control.

## Socket types

| Type | Description |
|------|-------------|
| `TcpListener` | Accept incoming TCP connections |
| `TcpStream` | Bidirectional TCP connection |
| `TcpSocket` | Socket builder for pre-connection configuration |
| `UdpSocket` | Connectionless datagram socket |
| `UnixStream` | Unix domain stream socket |
| `UnixListener` | Unix domain socket listener |
| `UnixDatagram` | Unix domain datagram socket |
| `Address` | IPv4/IPv6 socket address |

:::caution[Handle all four outcomes from non-blocking reads]
Every `tryRead`, `tryAccept`, and `tryRecv` returns **four** possible results. Missing any of them causes bugs:

| Result | Meaning | What to do |
|--------|---------|-----------|
| `n > 0` | Got data | Process it |
| `null` | Would block (no data ready) | Retry later (`orelse continue`) |
| `n == 0` | Peer closed the connection (FIN) | Clean up and return |
| `error` | Connection reset or failure | Log and return |

**Copy-paste template for every read loop:**

```zig
while (true) {
    const n = stream.tryRead(&buf) catch return orelse continue;
    if (n == 0) return; // Peer disconnected
    processData(buf[0..n]);
}
```

See [Common Pitfalls](/v1.0.0-zig0.15.2/guides/common-pitfalls/#the-three-way-io-pattern) for detailed examples of what goes wrong when you skip outcomes.
:::

## TCP server

### Quick start with convenience functions

```zig
const volt = @import("volt");

// Bind to an address string
var listener = try volt.net.listen("0.0.0.0:8080");
defer listener.close();

// Or bind to a port on all interfaces
var listener2 = try volt.net.listenPort(8080);
defer listener2.close();
```

### Accept loop

`tryAccept` returns an `AcceptResult` containing the new stream and the peer address:

```zig
while (true) {
    if (try listener.tryAccept()) |result| {
        var stream = result.stream;
        const peer = result.peer_addr;
        // Handle connection...
        handleClient(&stream, peer);
    }
}
```

### Async accept

`accept()` returns an `AcceptFuture` for scheduler integration:

```zig
var future = listener.accept();
// Poll through your runtime...
// When ready, result contains the stream and peer address.
```

## TCP client

### Quick connect

```zig
var stream = try volt.net.connect("127.0.0.1:8080");
defer stream.close();
```

### Connect with DNS resolution

```zig
var stream = try volt.net.connectHost(allocator, "example.com", 443);
defer stream.close();
```

### Custom socket options

Use `TcpSocket` as a builder to configure options before connecting:

```zig
var socket = try volt.net.TcpSocket.newV4();

// Set buffer sizes
try socket.setRecvBufferSize(65536);
try socket.setSendBufferSize(65536);

// Enable TCP keepalive
try socket.setKeepalive(.{
    .time = volt.time.Duration.fromSecs(60),
});

// Enable TCP_NODELAY (disable Nagle's algorithm)
try socket.setNodelay(true);

// Connect
var stream = try socket.connect(addr);
defer stream.close();
```

## TCP stream I/O

### Non-blocking read/write

```zig
var buf: [4096]u8 = undefined;

// tryRead returns ?usize -- null means WouldBlock
if (try stream.tryRead(&buf)) |n| {
    if (n == 0) {
        // Connection closed by peer
        return;
    }
    processData(buf[0..n]);
}

// tryWrite returns ?usize -- null means WouldBlock
if (try stream.tryWrite(response_data)) |n| {
    // n bytes were written
}

// writeAll blocks (retries) until all data is written
try stream.writeAll("HTTP/1.1 200 OK\r\n\r\nHello!");
```

### Async futures

```zig
// Read future
var read_future = stream.read(&buf);

// Write future
var write_future = stream.write(data);

// Write-all future
var write_all_future = stream.writeAll(data);

// Readiness futures (wait for socket to be readable/writable)
var readable_future = stream.readable();
var writable_future = stream.writable();
```

### Peek

Read data without consuming it from the receive buffer:

```zig
const n = try stream.peek(&buf);
if (n > 0) {
    // Peeked n bytes, data is still in the buffer
}
```

### Shutdown

Shut down one or both directions of the connection:

```zig
try stream.shutdown(.write); // No more writes (sends FIN)
try stream.shutdown(.read);  // No more reads
try stream.shutdown(.both);  // Both directions
```

### Splitting a stream

Split a `TcpStream` into independent read and write halves for concurrent I/O:

```zig
// Borrowed split -- halves reference the original stream
const halves = stream.split_halves();
var reader = halves.read;
var writer = halves.write;

// Owned split -- halves can be moved to different tasks
const owned = try stream.intoSplit(allocator);
var owned_reader = owned.read;  // Can move to reader task
var owned_writer = owned.write; // Can move to writer task
```

## UDP

### One-to-many (sendTo/recvFrom)

```zig
var socket = try volt.net.UdpSocket.bind(volt.net.Address.fromPort(8080));
defer socket.close();

var buf: [1024]u8 = undefined;

// Receive from any sender
if (try socket.tryRecvFrom(&buf)) |result| {
    const data = buf[0..result.len];
    const sender = result.addr;

    // Echo back to sender
    _ = try socket.trySendTo(data, sender);
}
```

### One-to-one (connect + send/recv)

```zig
var socket = try volt.net.UdpSocket.bind(volt.net.Address.fromPort(0));
try socket.connect(server_addr);

_ = try socket.trySend("hello");
const n = try socket.tryRecv(&buf) orelse 0;
```

### Socket options

```zig
try socket.setBroadcast(true);
try socket.setMulticastLoop(true);
try socket.setMulticastTtl(2);
try socket.setTtl(64);

// Join a multicast group
try socket.joinMulticast(multicast_addr, interface_addr);
try socket.leaveMulticast(multicast_addr, interface_addr);
```

## Unix domain sockets

Unix sockets provide local inter-process communication, faster than TCP loopback.

### Stream (connection-oriented)

```zig
// Server
var listener = try volt.net.UnixListener.bind("/tmp/myapp.sock");
defer listener.close();

if (try listener.tryAccept()) |result| {
    var stream = result.stream;
    defer stream.close();
    // Handle connection...
}

// Client
var stream = try volt.net.UnixStream.connect("/tmp/myapp.sock");
defer stream.close();
try stream.writeAll("hello");
```

### Datagram (connectionless)

```zig
var sock = try volt.net.UnixDatagram.bind("/tmp/myapp-dgram.sock");
defer sock.close();

_ = try sock.trySendTo("message", "/tmp/other.sock");

var buf: [1024]u8 = undefined;
if (try sock.tryRecvFrom(&buf)) |result| {
    processMessage(buf[0..result.len]);
}
```

## DNS resolution

DNS lookups are blocking operations. Use them from the blocking pool in production:

```zig
// Resolve all addresses
var result = try volt.net.resolve(allocator, "example.com", 443);
defer result.deinit();

for (result.addresses) |addr| {
    // Try connecting to each address
}

// Resolve first address only
const addr = try volt.net.resolveFirst(allocator, "example.com", 443);
var stream = try volt.net.TcpStream.connect(addr);
```

## Address

The `Address` type wraps `sockaddr_storage` and supports both IPv4 and IPv6:

```zig
// Parse from string
const addr = try volt.net.Address.parse("127.0.0.1:8080");

// From port only (binds to 0.0.0.0)
const addr2 = volt.net.Address.fromPort(8080);

// Get components
const port = addr.port();
const family = addr.family(); // AF.INET or AF.INET6
```

## Connection Lifecycle and Peer Disconnect

Understanding how TCP connections end is essential for writing robust servers.

### Orderly shutdown (FIN)

When the remote peer closes the connection gracefully, `tryRead` returns `0` bytes. This is not an error -- it means the peer sent a FIN packet:

```zig
const n = stream.tryRead(&buf) catch return orelse continue;
if (n == 0) {
    // Peer closed the connection (FIN received).
    // No more data will arrive. Clean up and return.
    return;
}
```

### Connection reset (RST)

If the remote peer crashes, the network drops, or the connection is forcibly closed, `tryRead` returns an error (typically `ConnectionResetByPeer` or `BrokenPipe`):

```zig
const n = stream.tryRead(&buf) catch |err| {
    // Connection was reset or aborted.
    // Log the error and clean up.
    std.debug.print("Connection error: {}\n", .{err});
    return;
};
```

### Write to a closed connection

Writing to a connection after the peer has closed it produces `BrokenPipe`. Always handle write errors:

```zig
stream.writeAll(response) catch |err| {
    // Peer may have disconnected between read and write.
    std.debug.print("Write failed: {}\n", .{err});
    return;
};
```

### Summary

| `tryRead` result | Meaning |
|------------------|---------|
| `n > 0` | Data received |
| `null` | Would block (no data ready, try again) |
| `n == 0` | Orderly shutdown -- peer sent FIN |
| `error` | Connection reset, abort, or network failure |

For a complete example that handles all these cases, see the [Echo Server](/v1.0.0-zig0.15.2/cookbook/echo-server/) cookbook.

***

## Async server pattern

A complete async TCP server skeleton:

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Io) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            _ = io.@"async"(handleClient, .{result.stream}) catch continue;
        }
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.tryRead(&buf) catch return orelse continue;
        if (n == 0) return; // Client disconnected
        stream.writeAll(buf[0..n]) catch return;
    }
}
```

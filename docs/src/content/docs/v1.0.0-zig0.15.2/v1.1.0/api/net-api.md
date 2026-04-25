---
title: Networking API
description: Complete API reference for TCP, UDP, and Unix socket types in Volt.
slug: v1.0.0-zig0.15.2/v110/api/net-api
---

The networking module is where Volt meets the real world. It wraps TCP, UDP, and Unix domain sockets with an async-friendly API that handles the tedious parts -- non-blocking I/O, partial writes, address parsing, socket options -- so you can focus on your protocol.

Unlike raw `std.posix` calls, everything here integrates with the runtime's I/O driver. Reads and writes that would block instead yield to the scheduler, letting other tasks make progress. Socket options like `TCP_NODELAY`, keepalive, and buffer sizes are exposed as typed methods instead of raw `setsockopt` calls.

:::note[Three-way I/O pattern]
Every `tryRead`, `tryAccept`, and `tryRecv` returns four possible outcomes: data, would-block (`null`), peer-close (`0`), and error. Handling only two causes bugs. See [Common Pitfalls](/v1.0.0-zig0.15.2/guides/common-pitfalls/#the-three-way-io-pattern) for the copy-paste template.
:::

**What you'll use most:**

* `net.listen("0.0.0.0:8080")` to start a server
* `net.connect("host:port")` to connect to one
* `TcpStream.tryRead` / `TcpStream.tryWrite` for non-blocking I/O
* `stream.split()` when you need concurrent reads and writes on the same connection

### At a Glance

```zig
const volt = @import("volt");
const net = volt.net;

// Server: listen, accept, handle
var listener = try net.listen("0.0.0.0:8080");
defer listener.close();

if (try listener.tryAccept()) |result| {
    var stream = result.stream;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    if (try stream.tryRead(&buf)) |n| {
        stream.writeAll(buf[0..n]) catch {};
    }
}

// Client: connect and send
var stream = try net.connect("127.0.0.1:8080");
defer stream.close();
try stream.setNodelay(true);  // low-latency mode
_ = try stream.tryWrite("GET /health HTTP/1.1\r\n\r\n");

// UDP: fire-and-forget datagrams
var sock = try net.UdpSocket.bind(net.Address.fromPort(0));
defer sock.close();
_ = try sock.trySendTo("ping", try net.Address.parse("10.0.0.1:9000"));

// DNS resolution (blocking -- run on blocking pool in async context)
const addr = try net.resolveFirst(allocator, "api.example.com", 443);
var stream = try net.TcpStream.connect(addr);
```

All types live under `volt.net`.

```zig
const volt = @import("volt");
const net = volt.net;
```

## Convenience Functions

| Function | Description |
|----------|-------------|
| `net.listen(addr_str)` | Create a TCP listener from an address string (e.g., `"0.0.0.0:8080"`) |
| `net.listenPort(port)` | Create a TCP listener on all interfaces |
| `net.connect(addr_str)` | Connect to a TCP server from an address string |
| `net.resolve(alloc, host, port)` | DNS hostname resolution (blocking) |
| `net.resolveFirst(alloc, host, port)` | Resolve and return the first address |
| `net.connectHost(alloc, host, port)` | Connect with DNS resolution (blocking) |

```zig
// Server
var listener = try net.listen("0.0.0.0:8080");
defer listener.close();

// Client
var stream = try net.connect("127.0.0.1:8080");
defer stream.close();
```

***

## Address

```zig
const Address = net.Address;
```

IPv4/IPv6 socket address.

### Construction

| Method | Description |
|--------|-------------|
| `parse(str)` | Parse `"host:port"` string |
| `fromPort(port)` | `0.0.0.0:port` (any IPv4 interface) |
| `loopbackV4(port)` | `127.0.0.1:port` |
| `loopbackV6(port)` | `[::1]:port` |
| `fromStd(std_addr)` | Convert from `std.net.Address` |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `port` | `u16` | Port number |
| `family` | `u32` | Address family (`AF.INET` or `AF.INET6`) |
| `sockaddr` | `*const posix.sockaddr` | Pointer to underlying sockaddr |

***

## TcpSocket

```zig
const TcpSocket = net.TcpSocket;
```

Socket builder for configuring TCP options before connecting or listening.

### Construction

```zig
var socket = try TcpSocket.newV4();  // IPv4
var socket = try TcpSocket.newV6();  // IPv6
```

### Configuration Methods

All return `!void` and can be chained in sequence.

| Method | Description |
|--------|-------------|
| `setReuseAddr(bool)` | Enable `SO_REUSEADDR` |
| `setSendBufferSize(usize)` | Set `SO_SNDBUF` |
| `setRecvBufferSize(usize)` | Set `SO_RCVBUF` |
| `setNoDelay(bool)` | Enable/disable `TCP_NODELAY` |
| `setKeepalive(Keepalive)` | Configure TCP keepalive |
| `setLinger(?Duration)` | Set `SO_LINGER` (`null` to disable) |

### Getters

| Method | Returns |
|--------|---------|
| `getReuseAddr()` | `!bool` |
| `getSendBufferSize()` | `!usize` |
| `getRecvBufferSize()` | `!usize` |
| `getNoDelay()` | `!bool` |

### Connect / Listen

```zig
var stream = try socket.connect(addr);  // Returns TcpStream
var listener = try socket.listen(addr, backlog);  // Returns TcpListener
```

### Keepalive Configuration

```zig
const Keepalive = net.tcp.Keepalive;

try socket.setKeepalive(.{
    .time = volt.Duration.fromSecs(60),    // Time before first probe
    .interval = volt.Duration.fromSecs(5), // Interval between probes
    .retries = 3,                         // Max probe count
});
```

### Close

```zig
socket.close();
```

### Example: Production Socket Setup

A real-world server socket with all the typical options for a low-latency service.

```zig
/// Configure a production-ready TCP listener with tuned socket options.
/// These settings are appropriate for a low-latency HTTP or RPC server.
fn createProductionListener(bind_addr: net.Address) !net.TcpListener {
    var socket = try net.TcpSocket.newV4();
    errdefer socket.close();

    // Allow rapid restart without TIME_WAIT blocking the port.
    try socket.setReuseAddr(true);

    // Disable Nagle's algorithm: send small packets immediately.
    // Critical for request-response protocols where latency matters
    // more than throughput. Without this, the kernel may buffer small
    // writes for up to 40ms waiting for more data.
    try socket.setNoDelay(true);

    // Size the kernel buffers for the expected workload.
    // 256 KiB send buffer handles bursts of outgoing data (e.g., large
    // HTTP responses) without blocking the application. 128 KiB receive
    // buffer is sufficient for most request payloads.
    try socket.setSendBufferSize(256 * 1024);
    try socket.setRecvBufferSize(128 * 1024);

    // Enable TCP keepalive to detect dead connections.
    // Without this, a connection where the remote host crashes or
    // the network is severed will remain open forever, leaking
    // file descriptors and memory.
    try socket.setKeepalive(.{
        .time = volt.Duration.fromSecs(60),    // First probe after 60s idle
        .interval = volt.Duration.fromSecs(10), // Subsequent probes every 10s
        .retries = 5,                          // Give up after 5 unanswered probes
    });

    // Use a generous linger timeout: on close, allow up to 5 seconds
    // for buffered data to drain before the kernel resets the connection.
    try socket.setLinger(volt.Duration.fromSecs(5));

    // Backlog of 1024: the kernel will queue up to this many
    // completed TCP handshakes waiting for accept(). Under heavy
    // load, connections beyond this limit receive RST.
    return socket.listen(bind_addr, 1024);
}
```

***

## TcpListener

```zig
const TcpListener = net.TcpListener;
```

TCP server socket that accepts incoming connections.

### Construction

```zig
var listener = try TcpListener.bind(Address.fromPort(8080));
defer listener.close();

// Or with options:
var socket = try TcpSocket.newV4();
try socket.setReuseAddr(true);
var listener = try socket.listen(addr, 128);
defer listener.close();
```

### Accept

#### `tryAccept`

```zig
pub fn tryAccept(self: *TcpListener) !?AcceptResult
```

Non-blocking accept. Returns `null` if no pending connection (EAGAIN/EWOULDBLOCK).

```zig
const AcceptResult = struct {
    stream: TcpStream,
    peer_addr: Address,
};
```

```zig
while (true) {
    if (try listener.tryAccept()) |result| {
        handleConnection(result.stream, result.peer_addr);
    }
}
```

#### `accept` (Future)

```zig
pub fn accept(self: *TcpListener) AcceptFuture
```

Returns a Future that resolves with the next connection.

### Diagnostics

| Method | Returns | Description |
|--------|---------|-------------|
| `localAddr` | `Address` | The bound address (useful when port was 0) |
| `close` | `void` | Close the listener socket |

### Example: Simple HTTP Request Parser

A minimal server that accepts connections, reads an HTTP request line
and headers, and routes based on method and path.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

/// Parsed HTTP request line.
const HttpRequest = struct {
    method: []const u8,
    path: []const u8,
    version: []const u8,
    header_data: []const u8,
};

/// Read bytes from a non-blocking stream with retry.
/// Returns total bytes read into buf, or error.
fn readWithRetry(stream: *net.TcpStream, buf: []u8) !usize {
    var total: usize = 0;
    for (0..200) |_| {
        if (try stream.tryRead(buf[total..])) |n| {
            if (n == 0) return total; // EOF
            total += n;
            // Check if we have a complete HTTP header (ends with \r\n\r\n)
            if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) {
                return total;
            }
        } else {
            std.Thread.sleep(1_000_000); // 1ms
        }
    }
    return total;
}

/// Parse the first line of an HTTP request: "GET /path HTTP/1.1\r\n"
fn parseRequestLine(raw: []const u8) !HttpRequest {
    const header_end = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse
        return error.IncompleteHeaders;
    const first_line_end = std.mem.indexOf(u8, raw, "\r\n") orelse
        return error.MalformedRequest;
    const first_line = raw[0..first_line_end];

    // Split "GET /path HTTP/1.1" into three tokens
    var iter = std.mem.splitScalar(u8, first_line, ' ');
    const method = iter.next() orelse return error.MalformedRequest;
    const path = iter.next() orelse return error.MalformedRequest;
    const version = iter.next() orelse return error.MalformedRequest;

    return .{
        .method = method,
        .path = path,
        .version = version,
        .header_data = raw[first_line_end + 2 .. header_end],
    };
}

/// Handle one HTTP connection: read request, send response.
fn handleHttpConnection(stream: *net.TcpStream, peer: net.Address) void {
    _ = peer;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    const n = readWithRetry(stream, &buf) catch return;
    if (n == 0) return;

    const request = parseRequestLine(buf[0..n]) catch {
        const bad_request = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        _ = stream.tryWrite(bad_request) catch {};
        return;
    };

    // Route based on method and path
    if (std.mem.eql(u8, request.method, "GET")) {
        if (std.mem.eql(u8, request.path, "/health")) {
            const response =
                "HTTP/1.1 200 OK\r\n" ++
                "Content-Type: text/plain\r\n" ++
                "Content-Length: 2\r\n" ++
                "\r\n" ++
                "ok";
            _ = stream.tryWrite(response) catch {};
        } else {
            const not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            _ = stream.tryWrite(not_found) catch {};
        }
    } else {
        const not_allowed = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
        _ = stream.tryWrite(not_allowed) catch {};
    }
}

/// Start listening and dispatch connections.
fn runHttpServer() !void {
    var listener = try net.listen("0.0.0.0:8080");
    defer listener.close();

    while (true) {
        if (try listener.tryAccept()) |result| {
            var stream = result.stream;
            handleHttpConnection(&stream, result.peer_addr);
        }
    }
}
```

***

## TcpStream

```zig
const TcpStream = net.TcpStream;
```

A connected TCP socket with async read/write.

### Construction

```zig
var stream = try TcpStream.connect(addr);
defer stream.close();
```

### Reading

#### `tryRead`

```zig
pub fn tryRead(self: *TcpStream, buf: []u8) !?usize
```

Non-blocking read. Returns bytes read, `null` if would block, `0` if peer closed.

#### `read` (Future)

```zig
pub fn read(self: *TcpStream, buf: []u8) ReadFuture
```

Returns a Future that resolves with bytes read.

#### `peek`

```zig
pub fn peek(self: *TcpStream, buf: []u8) PeekFuture
```

Read without consuming (MSG\_PEEK).

### Writing

#### `tryWrite`

```zig
pub fn tryWrite(self: *TcpStream, data: []const u8) !?usize
```

Non-blocking write. Returns bytes written or `null` if would block.

#### `writeAll`

```zig
pub fn writeAll(self: *TcpStream, data: []const u8) WriteAllFuture
```

Returns a Future that writes all data (handles partial writes internally).

### Readiness

| Method | Returns | Description |
|--------|---------|-------------|
| `readable` | `ReadableFuture` | Future that resolves when data is available |
| `writable` | `WritableFuture` | Future that resolves when socket can accept writes |
| `ready(interest)` | `ReadyFuture` | Wait for specific readiness (read, write, or both) |

### Shutdown

```zig
pub fn shutdown(self: *TcpStream, how: ShutdownHow) !void
```

```zig
const ShutdownHow = enum { read, write, both };
```

### Split

Split a stream into independent read and write halves for concurrent access.

#### Borrowed Split

```zig
const halves = stream.split();
var reader = halves.read_half;  // ReadHalf
var writer = halves.write_half; // WriteHalf
// Lifetimes tied to stream
```

#### Owned Split

```zig
const halves = stream.intoSplit();
var reader = halves.read_half;  // OwnedReadHalf
var writer = halves.write_half; // OwnedWriteHalf
// Can be moved to different tasks
```

### Diagnostics

| Method | Returns | Description |
|--------|---------|-------------|
| `localAddr` | `!Address` | Local address |
| `peerAddr` | `!Address` | Remote address |
| `nodelay` | `!bool` | TCP\_NODELAY state |
| `setNodelay(bool)` | `!void` | Set TCP\_NODELAY |
| `close` | `void` | Close the socket |

### Example: Read/Write Split for a Chat-Style Protocol

When you need to read and write on the same connection concurrently -- for
example, in a chat client that must receive messages while the user is
typing -- use `split()` or `intoSplit()` to get independent halves.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

/// A framed message: 4-byte big-endian length prefix followed by payload.
const MAX_MSG_LEN = 65536;

/// Send a length-prefixed message on the write half.
fn sendMessage(writer: *net.tcp.WriteHalf, payload: []const u8) !void {
    if (payload.len > MAX_MSG_LEN) return error.MessageTooLarge;

    // Write the 4-byte length header
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(payload.len), .big);

    // Send header then body. tryWrite may return null (would block)
    // or a partial write, so we loop until all bytes are sent.
    for ([_][]const u8{ &len_buf, payload }) |chunk| {
        var sent: usize = 0;
        while (sent < chunk.len) {
            if (try writer.tryWrite(chunk[sent..])) |n| {
                sent += n;
            } else {
                std.Thread.sleep(1_000_000); // 1ms backoff
            }
        }
    }
}

/// Receive a length-prefixed message on the read half.
/// Returns the payload slice within buf, or null on EOF.
fn recvMessage(reader: *net.tcp.ReadHalf, buf: []u8) !?[]const u8 {
    // Read the 4-byte length header
    var len_buf: [4]u8 = undefined;
    var header_read: usize = 0;
    while (header_read < 4) {
        if (try reader.tryRead(len_buf[header_read..])) |n| {
            if (n == 0) return null; // Connection closed
            header_read += n;
        } else {
            std.Thread.sleep(1_000_000);
        }
    }

    const payload_len: usize = std.mem.readInt(u32, &len_buf, .big);
    if (payload_len > buf.len) return error.MessageTooLarge;

    // Read the full payload
    var body_read: usize = 0;
    while (body_read < payload_len) {
        if (try reader.tryRead(buf[body_read..payload_len])) |n| {
            if (n == 0) return error.UnexpectedEof;
            body_read += n;
        } else {
            std.Thread.sleep(1_000_000);
        }
    }

    return buf[0..payload_len];
}

/// Run a connection handler that reads and writes independently.
fn handleChatConnection(stream: *net.TcpStream) !void {
    // Borrow-split: both halves reference the original stream.
    // This is safe because TCP read and write paths are independent
    // in the kernel -- they use separate buffers.
    const halves = stream.split();
    var reader = halves.read_half;
    var writer = halves.write_half;

    var recv_buf: [MAX_MSG_LEN]u8 = undefined;

    // In a real application, the reader and writer would run in
    // separate tasks. Here we show the sequential form.
    while (true) {
        // Read an incoming message
        const msg = try recvMessage(&reader, &recv_buf) orelse break;

        // Echo it back (a real chat server would broadcast to all clients)
        try sendMessage(&writer, msg);
    }
}
```

***

## UdpSocket

```zig
const UdpSocket = net.UdpSocket;
```

Connectionless datagram socket.

### Construction

```zig
var socket = try UdpSocket.bind(Address.fromPort(8080));
defer socket.close();

// Unbound (any port)
var socket = try UdpSocket.unbound();
```

### One-to-Many (sendTo / recvFrom)

#### `trySendTo`

```zig
pub fn trySendTo(self: *UdpSocket, data: []const u8, addr: Address) !?usize
```

Send a datagram to a specific address. Returns bytes sent or `null` if would block.

#### `tryRecvFrom`

```zig
pub fn tryRecvFrom(self: *UdpSocket, buf: []u8) !?RecvFromResult
```

```zig
const RecvFromResult = struct {
    len: usize,
    addr: Address,
};
```

### One-to-One (connect + send / recv)

```zig
try socket.connect(server_addr);

_ = try socket.trySend("hello");
const n = try socket.tryRecv(&buf) orelse 0;
```

### Socket Options

| Method | Description |
|--------|-------------|
| `setBroadcast(bool)` | Enable/disable `SO_BROADCAST` |
| `setMulticastLoop(bool)` | Enable/disable multicast loopback |
| `setMulticastTtl(u32)` | Set multicast TTL |
| `setTtl(u32)` | Set IP TTL |
| `setTos(u8)` | Set IP TOS (type of service) |
| `joinMulticastV4(group, iface)` | Join IPv4 multicast group |
| `leaveMulticastV4(group, iface)` | Leave IPv4 multicast group |

### Example: Simple Request-Response Protocol (DNS-Style)

UDP is well suited for request-response protocols where each message fits in a
single datagram. This example implements a minimal key-value lookup service
using a fixed-format binary protocol.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;
const Address = net.Address;

// --- Wire format ---
// Request:  [1 byte opcode] [1 byte key_len] [key_len bytes key]
// Response: [1 byte status]  [2 bytes val_len] [val_len bytes value]
//
// Opcodes: 0x01 = GET
// Status:  0x00 = OK, 0x01 = NOT_FOUND, 0x02 = ERROR

const OP_GET: u8 = 0x01;
const STATUS_OK: u8 = 0x00;
const STATUS_NOT_FOUND: u8 = 0x01;
const STATUS_ERROR: u8 = 0x02;

/// Build a GET request packet into buf. Returns the slice of buf that was filled.
fn buildGetRequest(buf: []u8, key: []const u8) ![]const u8 {
    if (key.len > 255 or key.len + 2 > buf.len) return error.KeyTooLong;
    buf[0] = OP_GET;
    buf[1] = @intCast(key.len);
    @memcpy(buf[2 .. 2 + key.len], key);
    return buf[0 .. 2 + key.len];
}

/// Parse a response packet. Returns the value payload on success.
fn parseResponse(raw: []const u8) ![]const u8 {
    if (raw.len < 3) return error.ResponseTooShort;
    const status = raw[0];
    if (status == STATUS_NOT_FOUND) return error.NotFound;
    if (status != STATUS_OK) return error.ServerError;
    const val_len = std.mem.readInt(u16, raw[1..3], .big);
    if (raw.len < 3 + val_len) return error.ResponseTruncated;
    return raw[3 .. 3 + val_len];
}

/// Client: send a GET request and wait for the response.
fn lookupKey(
    socket: *net.UdpSocket,
    server_addr: Address,
    key: []const u8,
) ![]const u8 {
    var req_buf: [258]u8 = undefined;
    const request = try buildGetRequest(&req_buf, key);

    // Send the request datagram
    _ = try socket.trySendTo(request, server_addr) orelse
        return error.WouldBlock;

    // Wait for the reply (with timeout via retry count)
    var resp_buf: [512]u8 = undefined;
    for (0..100) |_| {
        if (try socket.tryRecvFrom(&resp_buf)) |result| {
            return parseResponse(resp_buf[0..result.len]);
        }
        std.Thread.sleep(10_000_000); // 10ms
    }
    return error.Timeout;
}

/// Server: listen for GET requests and respond from a static table.
fn runLookupServer(bind_port: u16) !void {
    var socket = try net.UdpSocket.bind(Address.fromPort(bind_port));
    defer socket.close();

    // Static lookup table (in practice, this would be a real data store)
    const Entry = struct { key: []const u8, value: []const u8 };
    const table = [_]Entry{
        .{ .key = "api.example.com", .value = "93.184.216.34" },
        .{ .key = "db.internal", .value = "10.0.1.50" },
    };

    var recv_buf: [512]u8 = undefined;
    var resp_buf: [512]u8 = undefined;

    while (true) {
        const result = try socket.tryRecvFrom(&recv_buf) orelse continue;
        const data = recv_buf[0..result.len];

        if (data.len < 2 or data[0] != OP_GET) {
            // Malformed request: send error status
            resp_buf[0] = STATUS_ERROR;
            std.mem.writeInt(u16, resp_buf[1..3], 0, .big);
            _ = try socket.trySendTo(resp_buf[0..3], result.addr);
            continue;
        }

        const key_len: usize = data[1];
        if (data.len < 2 + key_len) continue; // Truncated, drop it
        const key = data[2 .. 2 + key_len];

        // Look up the key
        var found: ?[]const u8 = null;
        for (&table) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                found = entry.value;
                break;
            }
        }

        if (found) |value| {
            resp_buf[0] = STATUS_OK;
            std.mem.writeInt(u16, resp_buf[1..3], @intCast(value.len), .big);
            @memcpy(resp_buf[3 .. 3 + value.len], value);
            _ = try socket.trySendTo(resp_buf[0 .. 3 + value.len], result.addr);
        } else {
            resp_buf[0] = STATUS_NOT_FOUND;
            std.mem.writeInt(u16, resp_buf[1..3], 0, .big);
            _ = try socket.trySendTo(resp_buf[0..3], result.addr);
        }
    }
}
```

***

## Unix Sockets

Unix domain sockets for local IPC. Only available on Unix-like systems.

### UnixStream

```zig
const UnixStream = net.UnixStream;

var stream = try UnixStream.connect("/tmp/my.sock");
defer stream.close();

try stream.writeAll("Hello!");
var buf: [1024]u8 = undefined;
const n = try stream.read(&buf);
```

Methods mirror `TcpStream`: `tryRead`, `tryWrite`, `writeAll`, `close`, `shutdown`.

### UnixListener

```zig
const UnixListener = net.UnixListener;

var listener = try UnixListener.bind("/tmp/my.sock");
defer listener.close();

if (try listener.tryAccept()) |result| {
    handleConnection(result.stream);
}
```

### UnixDatagram

```zig
const UnixDatagram = net.UnixDatagram;

var socket = try UnixDatagram.bind("/tmp/my.sock");
defer socket.close();

// Send to specific path
try socket.sendTo("hello", try UnixAddr.fromPath("/tmp/other.sock"));

// Receive from any
const result = try socket.recvFrom(&buf);

// Socket pairs for IPC
var sockets = try UnixDatagram.pair();
defer sockets[0].close();
defer sockets[1].close();
```

### Example: Socket Pair for Parent-Child IPC

`UnixDatagram.pair()` creates two connected datagram sockets -- one for each
end of the conversation. This is the standard pattern for structured IPC
between a parent process and a child (or between two cooperating threads).
Each side can send and receive independently.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

/// Simple command/response protocol between parent and child.
const Command = enum(u8) {
    ping = 0x01,
    get_status = 0x02,
    shutdown = 0xFF,
};

const Response = enum(u8) {
    pong = 0x01,
    status_ok = 0x02,
    status_busy = 0x03,
    ack = 0xFF,
};

/// Child side: read commands from the socket, send responses.
fn childWorker(sock: *net.UnixDatagram) void {
    var buf: [256]u8 = undefined;

    while (true) {
        // tryRecv works because pair() creates connected sockets --
        // no address needed for send/recv.
        const n = sock.tryRecv(&buf) catch break orelse {
            std.Thread.sleep(1_000_000); // 1ms
            continue;
        };
        if (n == 0) break;

        const cmd: Command = @enumFromInt(buf[0]);
        const reply: Response = switch (cmd) {
            .ping => .pong,
            .get_status => .status_ok,
            .shutdown => .ack,
        };

        _ = sock.trySend(&[_]u8{@intFromEnum(reply)}) catch break;

        if (cmd == .shutdown) break;
    }
}

/// Parent side: send commands, collect responses.
fn parentController() !void {
    // Create a connected socket pair. sockets[0] is the parent end,
    // sockets[1] is the child end.
    var sockets = try net.UnixDatagram.pair();
    defer sockets[0].close();
    defer sockets[1].close();

    // Spawn the child worker on a separate thread.
    const child_thread = try std.Thread.spawn(.{}, childWorker, .{&sockets[1]});

    // Send a ping and read the pong
    _ = try sockets[0].trySend(&[_]u8{@intFromEnum(Command.ping)});

    var resp_buf: [256]u8 = undefined;
    for (0..100) |_| {
        if (try sockets[0].tryRecv(&resp_buf)) |n| {
            if (n > 0) {
                const resp: Response = @enumFromInt(resp_buf[0]);
                std.debug.assert(resp == .pong);
                break;
            }
        }
        std.Thread.sleep(1_000_000);
    }

    // Ask for status
    _ = try sockets[0].trySend(&[_]u8{@intFromEnum(Command.get_status)});
    for (0..100) |_| {
        if (try sockets[0].tryRecv(&resp_buf)) |_| break;
        std.Thread.sleep(1_000_000);
    }

    // Tell the child to shut down
    _ = try sockets[0].trySend(&[_]u8{@intFromEnum(Command.shutdown)});
    child_thread.join();
}
```

***

## DNS Resolution

DNS resolution is blocking and should be called from the blocking pool in async contexts.

```zig
// Blocking resolve
var result = try net.resolve(allocator, "example.com", 443);
defer result.deinit();

for (result.addresses) |addr| {
    // Try connecting to each address
}

// Resolve first only
const addr = try net.resolveFirst(allocator, "example.com", 443);
var stream = try TcpStream.connect(addr);

// Connect with resolution in one call
var stream = try net.connectHost(allocator, "example.com", 443);
```

***

## Connection Lifecycle

This section shows the complete lifecycle of a TCP connection from both
the client and server perspectives. Understanding this flow is important
for correct resource cleanup, especially when errors occur mid-stream.

### Client Lifecycle

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

fn clientLifecycle(allocator: std.mem.Allocator) !void {
    // 1. RESOLVE: turn a hostname into an IP address.
    //    This is blocking, so in an async context you would run it
    //    on the blocking pool.
    const addr = try net.resolveFirst(allocator, "api.example.com", 443);

    // 2. CONNECT: establish the TCP connection.
    //    This performs the three-way handshake (SYN -> SYN-ACK -> ACK).
    var stream = try net.TcpStream.connect(addr);

    // 3. CONFIGURE: set socket options after connection.
    //    NoDelay is especially important for request-response protocols.
    try stream.setNoDelay(true);

    // 4. WRITE: send a request.
    const request = "GET /status HTTP/1.1\r\nHost: api.example.com\r\n\r\n";
    var written: usize = 0;
    while (written < request.len) {
        if (try stream.tryWrite(request[written..])) |n| {
            written += n;
        } else {
            std.Thread.sleep(1_000_000);
        }
    }

    // 5. SHUTDOWN WRITE: signal that we are done sending.
    //    The server will see EOF on its read side. We can still read
    //    the response -- only the write direction is closed.
    try stream.shutdown(.write);

    // 6. READ: consume the response until EOF.
    var response_buf: [4096]u8 = undefined;
    var total_read: usize = 0;
    while (total_read < response_buf.len) {
        if (try stream.tryRead(response_buf[total_read..])) |n| {
            if (n == 0) break; // EOF: server closed its write side
            total_read += n;
        } else {
            std.Thread.sleep(1_000_000);
        }
    }

    // 7. CLOSE: release the file descriptor.
    //    Any buffered data that hasn't been sent yet is discarded
    //    (unless SO_LINGER is set). The kernel sends FIN to the peer.
    stream.close();
}
```

### Server Lifecycle

```zig
fn serverLifecycle() !void {
    // 1. BIND + LISTEN: create the listener socket.
    var listener = try net.listen("0.0.0.0:8080");
    defer listener.close();

    // 2. ACCEPT LOOP: wait for incoming connections.
    while (true) {
        const result = try listener.tryAccept() orelse {
            std.Thread.sleep(1_000_000);
            continue;
        };
        var conn = result.stream;

        // 3. READ: receive the client's request.
        var buf: [4096]u8 = undefined;
        var total: usize = 0;
        for (0..500) |_| {
            if (try conn.tryRead(buf[total..])) |n| {
                if (n == 0) break;
                total += n;
                // Check for end-of-request marker
                if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break;
            } else {
                std.Thread.sleep(1_000_000);
            }
        }

        // 4. WRITE: send the response.
        const response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        var written: usize = 0;
        while (written < response.len) {
            if (try conn.tryWrite(response[written..])) |n| {
                written += n;
            } else {
                std.Thread.sleep(1_000_000);
            }
        }

        // 5. SHUTDOWN + CLOSE: signal EOF and release resources.
        //    shutdown(.write) sends FIN so the client knows the response
        //    is complete. Then close() releases the file descriptor.
        conn.shutdown(.write) catch {};
        conn.close();
    }
}
```

***

## Common Patterns

### Accept Loop with Per-Connection Tasks

In a real server, each accepted connection should be handled independently
so that one slow client does not block others. This pattern shows how to
spawn a thread per connection. In an async context, you would spawn a task
instead.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

/// Per-connection state, heap-allocated so the thread owns it.
const ConnectionContext = struct {
    stream: net.TcpStream,
    peer_addr: net.Address,
    allocator: std.mem.Allocator,

    fn run(self: *ConnectionContext) void {
        defer {
            self.stream.close();
            self.allocator.destroy(self);
        }

        var buf: [4096]u8 = undefined;
        // Read-echo loop
        while (true) {
            const n = self.stream.tryRead(&buf) catch break orelse {
                std.Thread.sleep(1_000_000);
                continue;
            };
            if (n == 0) break; // Client disconnected

            // Echo back what we received
            var sent: usize = 0;
            while (sent < n) {
                sent += self.stream.tryWrite(buf[sent..n]) catch break orelse {
                    std.Thread.sleep(1_000_000);
                    continue;
                };
            }
        }
    }
};

fn acceptLoop(allocator: std.mem.Allocator) !void {
    var listener = try net.listen("0.0.0.0:9000");
    defer listener.close();

    while (true) {
        const result = try listener.tryAccept() orelse {
            std.Thread.sleep(1_000_000);
            continue;
        };

        // Heap-allocate connection context so the spawned thread owns it
        const ctx = try allocator.create(ConnectionContext);
        ctx.* = .{
            .stream = result.stream,
            .peer_addr = result.peer_addr,
            .allocator = allocator,
        };

        // Spawn a thread to handle this connection.
        // In production, use a thread pool or async tasks instead.
        const thread = std.Thread.spawn(.{}, ConnectionContext.run, .{ctx}) catch {
            ctx.stream.close();
            allocator.destroy(ctx);
            continue;
        };
        thread.detach();
    }
}
```

### Graceful Connection Shutdown

Correctly shutting down a TCP connection requires draining reads after
signaling write-EOF. Abruptly closing without shutdown can cause the peer
to receive RST instead of the final data, leading to "connection reset"
errors.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;

/// Gracefully shut down a TCP connection.
///
/// 1. Stop sending (FIN to peer)
/// 2. Drain any remaining incoming data
/// 3. Close the file descriptor
///
/// This ensures the peer receives all data we sent and has time to
/// process our FIN before we tear down the connection.
fn gracefulShutdown(stream: *net.TcpStream) void {
    // Step 1: Signal that we will not send any more data.
    // The peer's next read will return EOF (0 bytes).
    stream.shutdown(.write) catch {};

    // Step 2: Drain the read side. The peer may still be sending data
    // (e.g., the last few bytes of a response). If we close without
    // reading, the kernel may send RST, which discards data the peer
    // has already buffered.
    var drain_buf: [1024]u8 = undefined;
    for (0..50) |_| {
        const n = stream.tryRead(&drain_buf) catch break orelse {
            std.Thread.sleep(1_000_000);
            continue;
        };
        if (n == 0) break; // Peer also closed -- clean shutdown
    }

    // Step 3: Close the socket. At this point both sides have
    // exchanged FIN, so the connection enters TIME_WAIT cleanly.
    stream.close();
}
```

### UDP Multicast Subscriber

Join a multicast group to receive messages published by any sender on the
local network. This is useful for service discovery, real-time data feeds,
or cluster heartbeats.

```zig
const std = @import("std");
const volt = @import("volt");
const net = volt.net;
const Address = net.Address;

/// Subscribe to a multicast group and print received messages.
///
/// Multicast groups are in the range 224.0.0.0 - 239.255.255.255.
/// All hosts that join the same group on the same port will receive
/// every datagram sent to that group address.
fn multicastSubscriber() !void {
    // Bind to the multicast port on all interfaces.
    // SO_REUSEADDR allows multiple subscribers on the same host.
    var socket = try net.UdpSocket.bind(Address.fromPort(5000));
    defer socket.close();
    try socket.setReuseAddr(true);

    // Join the multicast group 239.1.2.3 on all interfaces (0.0.0.0).
    // The first argument is the group address as 4 bytes.
    // The second argument is the local interface to join on;
    // 0.0.0.0 means the kernel picks the default interface.
    const group_addr = [4]u8{ 239, 1, 2, 3 };
    const any_iface = [4]u8{ 0, 0, 0, 0 };
    try socket.joinMulticastV4(group_addr, any_iface);

    // Optionally control how many hops multicast packets can traverse.
    // TTL=1 means local subnet only. Increase for wider distribution.
    try socket.setMulticastTtlV4(1);

    // Receive loop
    var buf: [1500]u8 = undefined;
    while (true) {
        if (try socket.tryRecvFrom(&buf)) |result| {
            const msg = buf[0..result.len];
            // Process the multicast message.
            // result.addr tells you who sent it.
            _ = msg;
        } else {
            std.Thread.sleep(10_000_000); // 10ms
        }
    }

    // When done, leave the group to stop receiving
    // (also happens automatically when the socket is closed).
    try socket.leaveMulticastV4(group_addr, any_iface);
}

/// Publish a message to the multicast group.
fn multicastPublisher() !void {
    var socket = try net.UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Set TTL so the packet stays on the local subnet
    try socket.setMulticastTtlV4(1);

    // Send to the multicast group address + port
    const group = try Address.parse("239.1.2.3:5000");
    const message = "heartbeat:node-42:healthy";
    _ = try socket.trySendTo(message, group);
}
```

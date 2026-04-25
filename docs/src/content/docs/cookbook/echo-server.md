---
title: Echo Server
description: A complete TCP echo server that accepts multiple clients concurrently and echoes data back.
---

A TCP echo server is the "hello world" of network programming. Despite its simplicity, it exercises the core building blocks you will use in any networked application: binding a listener, accepting connections without blocking, spawning concurrent tasks, and managing the read/write lifecycle of each connection.

:::tip[What you will learn]
- **TcpListener binding** -- creating a server socket and listening for connections
- **Non-blocking accept loop** -- polling for new connections without stalling other work
- **Per-connection task spawning** -- using the work-stealing scheduler to handle thousands of clients
- **Read/write lifecycle** -- correctly handling partial reads, peer disconnects, and write errors
:::

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    // volt.run boots the Volt runtime (scheduler, I/O driver, timers)
    // and blocks until the provided function returns.
    try volt.run(server);
}

fn server(io: volt.Runtime) void {
    // Bind to all interfaces on port 8080.
    // Under the hood this creates a socket, sets SO_REUSEADDR (so you can
    // restart immediately without TIME_WAIT errors), binds, and calls
    // listen() -- all in one step.
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(8080),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    const port = listener.localAddr().port();
    std.debug.print("Echo server listening on 0.0.0.0:{}\n", .{port});
    std.debug.print("Test: echo 'hello' | nc localhost {}\n\n", .{port});

    // Accept loop -- continuously poll for new connections.
    while (true) {
        if (listener.tryAccept() catch null) |result| {
            // Spawn a lightweight task for each connection.  This keeps the
            // accept loop free to handle new arrivals immediately rather than
            // blocking until an existing client disconnects.
            var f = io.@"async"(handleClient, .{result.stream}) catch {
                // If the scheduler cannot accept more work (unlikely),
                // fall back to handling this client inline.  The accept
                // loop will stall until this client disconnects, but the
                // connection is not dropped.
                handleClient(result.stream);
                continue;
            };
            f.detach();
        } else {
            // No connection pending -- the underlying accept4 returned
            // EAGAIN.  Sleep briefly to avoid a tight spin loop that would
            // burn CPU for no reason.
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close(); // Always close, regardless of how we exit.

    var buf: [4096]u8 = undefined;

    while (true) {
        // tryRead is non-blocking: it returns null immediately when the
        // kernel has no data ready (EAGAIN / EWOULDBLOCK).  This is the
        // same semantic as mio / kqueue / epoll edge-triggered mode.
        const n = stream.tryRead(&buf) catch |err| {
            std.debug.print("read error: {}\n", .{err});
            return;
        } orelse {
            // Would block -- yield briefly and retry.
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };

        // A zero-length read means the peer performed an orderly shutdown
        // (sent FIN).  There is nothing left to echo, so exit cleanly.
        if (n == 0) return;

        // Echo the exact bytes back.  writeAll handles partial writes
        // internally: if the kernel accepts only part of the buffer,
        // writeAll retries with the remainder until everything is sent
        // or an error occurs.
        stream.writeAll(buf[0..n]) catch |err| {
            std.debug.print("write error: {}\n", .{err});
            return;
        };
    }
}
```

## Step-by-Step Walkthrough

### 1. Booting the runtime

```zig
try volt.run(server);
```

`volt.run` initializes the Volt runtime -- the work-stealing scheduler, the platform I/O driver (kqueue on macOS, io_uring or epoll on Linux, IOCP on Windows), and the timer wheel. It then calls your function on a worker thread and blocks `main` until it returns. You do not need to configure anything for a basic server; `volt.run` uses sensible defaults (one worker per CPU core).

### 2. Binding the listener

```zig
var listener = volt.net.TcpListener.bind(
    volt.net.Address.fromPort(8080),
) catch |err| { ... };
defer listener.close();
```

`TcpListener.bind` performs four operations in sequence:

1. Creates a TCP socket (`socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)`)
2. Sets `SO_REUSEADDR` so you can restart the server immediately without waiting for `TIME_WAIT` sockets to expire
3. Binds to the requested address
4. Calls `listen()` with a default backlog

Use `Address.fromPort(port)` to listen on all interfaces (`0.0.0.0`). For localhost-only binding, use `Address.parse("127.0.0.1:8080")` instead.

### 3. The accept loop

```zig
while (true) {
    if (listener.tryAccept() catch null) |result| {
        var f = io.@"async"(handleClient, .{result.stream}) catch {
            handleClient(result.stream);
            continue;
        };
        f.detach();
    } else {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}
```

`tryAccept()` is non-blocking: it wraps `accept4` with `SOCK_NONBLOCK` and returns `null` when no connection is pending (the kernel returned `EAGAIN`). This is important because a blocking accept would stall the entire worker thread, preventing it from processing other tasks on the same scheduler.

When there is nothing to accept, the loop sleeps for 1 ms. This is a simple throttle to avoid wasting CPU cycles. In a production server you would register interest with the I/O driver and yield until the kernel signals a new connection.

### 4. Spawning per-connection tasks

```zig
var f = io.@"async"(handleClient, .{result.stream}) catch {
    handleClient(result.stream);
    continue;
};
f.detach();
```

Each call to `io.@"async"` creates a lightweight task (approximately 256 bytes of overhead) and places it on the work-stealing scheduler. This means:

- The accept loop returns immediately to handle the next connection.
- The scheduler distributes client tasks across all available worker threads.
- Thousands of connections can be served concurrently without creating OS threads.

The `catch` fallback handles the rare case where the scheduler is saturated. Rather than dropping the connection, we process it inline. The accept loop will stall, but the client still gets served.

### 5. The read/write lifecycle

```zig
const n = stream.tryRead(&buf) catch |err| { ... } orelse {
    std.Thread.sleep(1 * std.time.ns_per_ms);
    continue;
};
if (n == 0) return;
stream.writeAll(buf[0..n]) catch |err| { ... };
```

Three outcomes are possible on every read:

| Result | Meaning | Action |
|--------|---------|--------|
| `n > 0` | Data received | Echo it back with `writeAll` |
| `n == 0` | Peer sent FIN (orderly close) | Exit the handler; `defer` closes our end |
| `null` (orelse) | No data ready (`EAGAIN`) | Sleep briefly and retry |

`writeAll` is used instead of a single `write` because the kernel may accept only a portion of the buffer (a *partial write*). `writeAll` retries internally until all bytes are sent or a hard error occurs. Common write errors include `ConnectionReset` (peer crashed) and `BrokenPipe` (peer closed before we finished writing).

### 6. Cleanup with `defer`

```zig
defer stream.close();
```

By placing the close at the top of `handleClient`, the socket file descriptor is released no matter how the function exits -- normal return, read error, or write error. This prevents file descriptor leaks under all code paths, which is critical for long-running servers.

## Try It Yourself

Below are three progressively more interesting variations. Each one builds on the base echo server above. Copy the complete example, then make the changes described.

### Variation 1: Send a welcome message on connect

When a client connects, greet them before entering the echo loop. This demonstrates writing to the stream before any reads.

```zig
fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    // Send a welcome banner before entering the echo loop.
    // This is the first thing the client sees after the TCP handshake.
    stream.writeAll("Welcome to the echo server! Type anything and press Enter.\n") catch |err| {
        std.debug.print("welcome write failed: {}\n", .{err});
        return;
    };

    var buf: [4096]u8 = undefined;

    while (true) {
        const n = stream.tryRead(&buf) catch |err| {
            std.debug.print("read error: {}\n", .{err});
            return;
        } orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };

        if (n == 0) return;

        stream.writeAll(buf[0..n]) catch |err| {
            std.debug.print("write error: {}\n", .{err});
            return;
        };
    }
}
```

### Variation 2: Track and print a connection counter

Use an atomic counter so every spawned task can safely increment it without a mutex. This is useful for basic observability -- knowing how many clients your server has handled since startup.

```zig
const std = @import("std");
const volt = @import("volt");

/// Atomic counter shared across all connection tasks.
/// Using .monotonic ordering is sufficient here because we only need
/// a rough count, not synchronization with other memory.
var connection_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Runtime) void {
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(8080),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    const port = listener.localAddr().port();
    std.debug.print("Echo server listening on 0.0.0.0:{}\n", .{port});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            // Increment BEFORE spawning so the count is visible immediately.
            const num = connection_count.fetchAdd(1, .monotonic) + 1;
            std.debug.print("[conn #{}] new client from {}\n", .{
                num,
                result.stream.peerAddr(),
            });

            var f = io.@"async"(handleClient, .{result.stream}) catch {
                handleClient(result.stream);
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer {
        std.debug.print("[server] client disconnected (total served: {})\n", .{
            connection_count.load(.monotonic),
        });
        stream.close();
    }

    var buf: [4096]u8 = undefined;

    while (true) {
        const n = stream.tryRead(&buf) catch |err| {
            std.debug.print("read error: {}\n", .{err});
            return;
        } orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };

        if (n == 0) return;

        stream.writeAll(buf[0..n]) catch |err| {
            std.debug.print("write error: {}\n", .{err});
            return;
        };
    }
}
```

### Variation 3: Uppercase echo (line-based protocol)

Instead of echoing raw bytes, accumulate input until a newline is found, convert the line to uppercase, and send it back. This introduces a simple application-level protocol and demonstrates buffering input across multiple reads -- something you will need in any real protocol parser.

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Runtime) void {
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(8080),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    const port = listener.localAddr().port();
    std.debug.print("Uppercase echo server on 0.0.0.0:{}\n", .{port});
    std.debug.print("Test: nc localhost {}\n\n", .{port});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var f = io.@"async"(handleClient, .{result.stream}) catch {
                handleClient(result.stream);
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    stream.writeAll("Type a line and press Enter. I will shout it back.\n") catch return;

    // We need to buffer across reads because a single read may return
    // only part of a line (the client has not pressed Enter yet), or it
    // may contain multiple lines if the client pastes text quickly.
    var line_buf: [4096]u8 = undefined;
    var line_len: usize = 0;

    var read_buf: [1024]u8 = undefined;

    while (true) {
        const n = stream.tryRead(&read_buf) catch |err| {
            std.debug.print("read error: {}\n", .{err});
            return;
        } orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };

        if (n == 0) return; // Peer disconnected.

        // Scan the newly received bytes for newlines.  There may be
        // zero, one, or many newlines in a single read.
        for (read_buf[0..n]) |byte| {
            if (byte == '\n') {
                // We have a complete line.  Convert to uppercase in place.
                for (line_buf[0..line_len]) |*c| {
                    c.* = std.ascii.toUpper(c.*);
                }

                // Send the uppercased line back, followed by a newline.
                stream.writeAll(line_buf[0..line_len]) catch return;
                stream.writeAll("\n") catch return;

                // Reset the line buffer for the next line.
                line_len = 0;
            } else {
                // Accumulate bytes until we see a newline.
                if (line_len < line_buf.len) {
                    line_buf[line_len] = byte;
                    line_len += 1;
                }
                // If the line exceeds the buffer, extra bytes are silently
                // dropped.  A production server would send an error and
                // close the connection instead.
            }
        }
    }
}
```

Test this with `nc`:

```
$ nc localhost 8080
Type a line and press Enter. I will shout it back.
hello world
HELLO WORLD
Volt is fast
VOLT IS FAST
```

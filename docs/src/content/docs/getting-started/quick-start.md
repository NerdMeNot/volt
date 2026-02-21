---
title: Quick Start
description: Build a TCP echo server with Volt in under 30 lines of Zig.
sidebar:
  order: 2
---

This guide walks through a complete TCP echo server -- from runtime initialization to handling connections -- with a line-by-line explanation.

## The Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    try volt.run(serve);
}

fn serve(io: volt.Io) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    std.debug.print("Listening on 0.0.0.0:8080\n", .{});

    // `catch null` converts errors to null so the while loop exits on error.
    // `|result|` unwraps the non-null AcceptResult (stream + peer address).
    while (listener.tryAccept() catch null) |result| {
        // `io.@"async"` spawns handleClient as a lightweight task (~256 bytes).
        // `@"async"` uses Zig's identifier quoting since `async` is a reserved keyword.
        _ = io.@"async"(handleClient, .{result.stream}) catch continue;
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        // `catch return` exits on error, `orelse continue` retries on would-block (null).
        const n = stream.tryRead(&buf) catch return orelse continue;
        if (n == 0) return; // Client disconnected (FIN)
        stream.writeAll(buf[0..n]) catch return;
    }
}
```

:::note[What's with the `@"async"` syntax?]
`async` and `await` are reserved keywords in Zig. The `@""` syntax is Zig's standard way to use reserved words as identifiers -- it's not Volt-specific. Read `io.@"async"(fn, args)` as "io dot async" and `f.@"await"(io)` as "f dot await". You'll see this throughout every Volt example.
:::

## Step-by-Step Walkthrough

### 1. Import Volt

```zig
const volt = @import("volt");
```

The `volt` module is the single entry point. All sub-modules are accessed through it: `volt.net`, `volt.sync`, `volt.channel`, `volt.time`, and so on.

### 2. Start the runtime

```zig
pub fn main() !void {
    try volt.run(serve);
}
```

`volt.run()` is the zero-config entry point. It:

1. Creates an `Io` handle with default settings (auto-detected worker count, page allocator)
2. Wraps your function in a `FnFuture` and spawns it on the work-stealing scheduler
3. Blocks `main` until the function completes
4. Cleans up all resources on return

For more control, create `Io` explicitly — like an `Allocator`, you `init`/`deinit`/pass it through:

```zig
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var io = try volt.Io.init(gpa.allocator(), .{
        .num_workers = 4,              // Fixed worker count
        .max_blocking_threads = 64,    // Cap blocking pool
    });
    defer io.deinit();

    try io.run(serve);
}
```

### 3. Bind a TCP listener

```zig
var listener = volt.net.listen("0.0.0.0:8080") catch return;
defer listener.close();
```

`volt.net.listen()` is a convenience function that parses an address string and binds a `TcpListener`. Under the hood it calls `Address.parse()` followed by `TcpListener.bind()`.

Other listener options:

```zig
// Bind to a specific port on all interfaces
var listener = try volt.net.listenPort(8080);

// Full control via TcpSocket builder
var socket = try volt.net.TcpSocket.newV4();
try socket.setReuseAddr(true);
try socket.bind(volt.net.Address.fromPort(8080));
var listener = try socket.listen(128); // backlog
```

### 4. Accept connections

```zig
while (listener.tryAccept() catch null) |result| {
    _ = io.@"async"(handleClient, .{result.stream}) catch continue;
}
```

`tryAccept()` is the non-blocking accept call. It returns:

- `AcceptResult` containing `.stream` (a `TcpStream`) and `.peer_addr` (the client's `Address`) -- when a connection is ready
- `null` -- when no connection is pending (would block)
- An error -- on actual failure

Each accepted connection is handed off to a new task via `io.@"async"()` (where `io` is the `volt.Io` handle passed to the root function). This spawns `handleClient` as a lightweight async task (~256 bytes) on the work-stealing scheduler and returns a `volt.Future`. The `.{result.stream}` syntax passes the `TcpStream` as the function argument.

### 5. Echo data back

```zig
fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.tryRead(&buf) catch return orelse continue;
        if (n == 0) return;
        stream.writeAll(buf[0..n]) catch return;
    }
}
```

Inside the handler:

- **`stream.tryRead(&buf)`** attempts a non-blocking read. It returns the number of bytes read, `null` if it would block, or an error.
- **`orelse continue`** handles the would-block case by looping back to retry.
- **`n == 0`** means the client closed the connection (EOF).
- **`stream.writeAll(buf[0..n])`** writes the full buffer back. Unlike `tryWrite`, `writeAll` loops internally until all bytes are sent.

## Running It

Add Volt as a dependency in your `build.zig.zon`, then:

```bash
zig build run
```

Test with netcat in another terminal:

```bash
echo "hello volt" | nc localhost 8080
```

You should see `hello volt` echoed back.

## What Happens Under the Hood

When you call `volt.run(serve)`, here is what the runtime does:

1. **Worker threads start.** The scheduler spawns N worker threads (default: number of CPU cores). Each worker has a local task queue (256-slot ring buffer) and a LIFO slot for hot-path locality.

2. **Your function becomes a task.** `serve` is wrapped in a `FnFuture` -- a single-poll future that calls your function once. This future is placed in the global injection queue.

3. **A worker picks it up.** One of the idle workers wakes (via futex), pulls the task from the global queue, and begins executing `serve()`.

4. **Each `@"async"` creates a new task.** When `io.@"async"(handleClient, .{result.stream})` is called, a new task is created and pushed to the current worker's LIFO slot. If that slot is full, it goes to the local queue. If the local queue is also full, it overflows to the global queue.

5. **Work stealing keeps everyone busy.** If a worker runs out of tasks in its own queue, it steals from other workers' queues. The steal order is randomized to avoid contention.

6. **Cooperative budgeting prevents starvation.** Each worker has a budget of 128 polls per tick. After 128 polls, the current task is rescheduled so other tasks get a turn.

7. **Cleanup on exit.** When `serve()` returns (or all tasks complete), the runtime shuts down workers and frees resources.

## Concurrent Tasks Example

Here is a more advanced example showing concurrent task coordination:

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(app);
}

fn app(io: volt.Io) !void {
    // Launch concurrent async operations
    var user_f = try io.@"async"(fetchUser, .{@as(u64, 42)});
    var posts_f = try io.@"async"(fetchPosts, .{@as(u64, 42)});

    // Await both results
    const user = user_f.@"await"(io);
    const posts = posts_f.@"await"(io);
    _ = user;
    _ = posts;
}

fn fetchUser(id: u64) []const u8 {
    _ = id;
    return "Alice";
}

fn fetchPosts(user_id: u64) u32 {
    _ = user_id;
    return 15;
}
```

`io.@"async"` returns a `volt.Future(T)` that you `.@"await"(io)` to get the result. For structured concurrency with many tasks, use `volt.Group`:

```zig
fn app(io: volt.Io) !void {
    var group = volt.Group.init(io);

    // Spawn multiple tasks into the group
    _ = group.spawn(fetchUser, .{@as(u64, 1)});
    _ = group.spawn(fetchUser, .{@as(u64, 2)});
    _ = group.spawn(fetchUser, .{@as(u64, 3)});

    // Wait for all tasks to complete
    group.wait();

    // Or cancel all remaining tasks
    // group.cancel();
}
```

| Pattern | Use case |
|---------|----------|
| `io.@"async"(fn, args)` + `.@"await"(io)` | Launch one task, get its result |
| `volt.Group` + `.spawn()` + `.wait()` | Structured concurrency: spawn many, wait for all |
| `volt.Group` + `.cancel()` | Cancel all tasks in the group |
| `future.cancel(io)` | Cancel a single async operation |

## Graceful Shutdown Example

For production servers, you typically want to catch signals and drain connections:

```zig
const volt = @import("volt");

pub fn main() !void {
    var shutdown = try volt.shutdown.Shutdown.init();
    defer shutdown.deinit();

    var listener = try volt.net.listen("0.0.0.0:8080");
    defer listener.close();

    while (!shutdown.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            // Track in-flight work
            var work = shutdown.startWork();
            defer work.deinit();
            handleConnection(result.stream);
        }
    }

    // Wait for in-flight requests to finish (with timeout)
    _ = shutdown.waitPendingTimeout(volt.Duration.fromSecs(5));
}

fn handleConnection(stream: volt.net.TcpStream) void {
    var conn = stream;
    defer conn.close();
    // ... handle the request ...
}
```

The `Shutdown` type catches SIGINT and SIGTERM, and the `WorkGuard` returned by `startWork()` tracks active connections so you know when it is safe to exit.

## Next Steps

Now that you have a working server, learn about the programming model:

- [Basic Concepts](/getting-started/basic-concepts/) -- The runtime, async/await, Groups, and cooperative scheduling
- [Runtime Configuration](/usage/runtime/) -- Worker count, blocking pool, backend selection
- [Networking](/usage/networking/) -- TCP, UDP, Unix sockets, DNS
- [Channels](/usage/channels/) -- MPMC, Oneshot, Broadcast, Watch

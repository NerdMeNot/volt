---
title: Quick Reference
description: One-page cheat sheet for the Volt API -- organized by task, not by module.
sidebar:
  order: 0
slug: v1.0.0-zig0.15.2/guides/quick-reference
---

A compact lookup for "how do I do X in Volt?" Copy-paste the pattern you need.

## Starting the Runtime

```zig
const volt = @import("volt");

// Zero-config (auto worker count, page allocator)
pub fn main() !void {
    try volt.run(myApp);
}

// Custom config
pub fn main() !void {
    try volt.runWith(allocator, .{
        .num_workers = 4,
        .max_blocking_threads = 128,
    }, myApp);
}

// Explicit (full control, recommended for production)
pub fn main() !void {
    var io = try volt.Io.init(allocator, .{ .num_workers = 4 });
    defer io.deinit();
    try io.run(myApp);
}

fn myApp(io: volt.Io) void {
    // All Volt APIs available here
    _ = io;
}
```

***

## Spawning Tasks

| Pattern | Code |
|---------|------|
| Spawn + await | `var f = try io.@"async"(fn, .{args}); const result = f.@"await"(io);` |
| Fire-and-forget | `_ = try io.@"async"(fn, .{args});` |
| Blocking pool | `var f = try io.concurrent(fn, .{args}); const result = try f.@"await"(io);` |
| Group (structured) | `var g = volt.Group.init(io); _ = g.spawn(fn, .{args}); g.wait();` |
| Cancel | `f.cancel(io);` or `g.cancel();` |

***

## Sync Primitives

| Primitive | Create | Try (no runtime) | Convenience (needs `io`) | Release |
|-----------|--------|-------------------|--------------------------|---------|
| Mutex | `var m = volt.sync.Mutex.init();` | `if (m.tryLock()) { ... }` | `m.lock(io);` | `m.unlock();` |
| RwLock | `var rw = volt.sync.RwLock.init();` | `if (rw.tryReadLock()) { ... }` | `rw.readLock(io);` | `rw.readUnlock();` |
| | | `if (rw.tryWriteLock()) { ... }` | `rw.writeLock(io);` | `rw.writeUnlock();` |
| Semaphore | `var s = volt.sync.Semaphore.init(n);` | `if (s.tryAcquire(1)) { ... }` | `s.acquire(io, 1);` | `s.release(1);` |
| Notify | `var n = volt.sync.Notify.init();` | — | `n.wait(io);` | `n.notifyOne();` / `n.notifyAll();` |
| Barrier | `var b = volt.sync.Barrier.init(n);` | — | `b.wait(io);` | (auto-releases) |
| OnceCell | `var o = volt.sync.OnceCell(T).init();` | `o.get()` | `o.getOrInit(io, initFn)` | — |

**All sync primitives are zero-allocation.** No `deinit()` needed.

***

## Channels

| Type | Create | deinit? | Send | Receive |
|------|--------|:-------:|------|---------|
| Channel | `var ch = try volt.channel.bounded(T, alloc, cap);` | Yes | `ch.trySend(val)` / `ch.send(io, val)` | `ch.tryRecv()` / `ch.recv(io)` |
| Oneshot | `var os = volt.channel.oneshot(T);` | No | `_ = os.sender.send(val);` | `os.receiver.tryRecv()` / `os.receiver.recv(io)` |
| Broadcast | `var bc = try volt.channel.broadcast(T, alloc, cap);` | Yes | `_ = bc.send(val);` | `rx.tryRecv()` / `rx.recv(io)` |
| Watch | `var wt = volt.channel.watch(T, initial);` | No | `wt.send(val);` | `rx.borrow()` / `rx.changed(io)` |

**Rule: if you passed an allocator, you must `defer ch.deinit()`.**

***

## Networking

### Bind and accept

```zig
var listener = try volt.net.listen("0.0.0.0:8080");
defer listener.close();

while (true) {
    if (try listener.tryAccept()) |result| {
        var stream = result.stream;
        _ = io.@"async"(handle, .{stream}) catch continue;
    }
}
```

### Connect

```zig
var stream = try volt.net.connect("127.0.0.1:8080");
defer stream.close();

// With DNS
var stream2 = try volt.net.connectHost(allocator, "example.com", 443);
defer stream2.close();
```

### Read template (handle all four outcomes)

```zig
const n = stream.tryRead(&buf) catch return orelse continue;
if (n == 0) return; // Peer closed
processData(buf[0..n]);
```

### Write

```zig
stream.writeAll(data) catch return;
```

### UDP

```zig
var sock = try volt.net.UdpSocket.bind(volt.net.Address.fromPort(8080));
defer sock.close();

if (try sock.tryRecvFrom(&buf)) |result| {
    _ = try sock.trySendTo(result.data, result.addr);
}
```

***

## Timers

```zig
// Duration constructors
const d1 = volt.Duration.fromSecs(5);
const d2 = volt.Duration.fromMillis(100);
const d3 = volt.Duration.fromNanos(1000);

// Create a sleep (use with timer driver or poll manually)
var slp = volt.time.sleep(volt.Duration.fromSecs(1));
// For blocking contexts (non-async):
volt.time.blockingSleep(volt.Duration.fromSecs(1));

// Instant (monotonic clock)
const start = volt.time.Instant.now();
// ... do work ...
const elapsed = start.elapsed();
```

***

## Signals and Shutdown

```zig
// Wait for Ctrl+C
volt.signal.waitForCtrlC();

// Graceful shutdown
var shutdown = try volt.shutdown.Shutdown.init();
defer shutdown.deinit();

while (!shutdown.isShutdown()) {
    // ... accept connections ...
}

// Wait for in-flight work
_ = shutdown.waitPendingTimeout(volt.Duration.fromSecs(5));
```

***

## Key Rules

1. **`io` means async.** If a function takes `io: volt.Io`, it yields the task (safe on worker threads). If it doesn't take `io` and does I/O, it blocks the thread.

2. **Allocator means `deinit()`.** If you passed an allocator to create it (`Channel`, `BroadcastChannel`), `defer x.deinit()`. Everything else is zero-allocation.

3. **Use `var` for futures.** Futures are mutated during polling. `const` won't compile.

4. **Handle all four I/O outcomes.** `tryRead` returns: data (`n > 0`), would-block (`null`), peer-close (`n == 0`), error. Don't skip any.

5. **Don't block worker threads.** No `std.Thread.sleep`, no CPU loops, no sync file I/O. Use `io.concurrent()` for blocking work, `volt.time.sleep()` for delays.

***

See [Common Pitfalls](/v1.0.0-zig0.15.2/guides/common-pitfalls/) for detailed explanations of each footgun with bad/good code pairs.

---
title: Connection Pool
description: A bounded pool of reusable connections gated by Semaphore + a free-list Mutex-protected stack. ~50 lines, works for any reusable resource.
---

The classic "max N concurrent connections, reuse them when idle"
pattern. Volt's `Semaphore` + `Mutex` plus a free list gets you
there in ~50 lines. Works for any reusable resource — TCP
connections, DB sessions, HTTP/2 streams.

## The Pool

```zig
const std = @import("std");
const volt = @import("volt");

pub fn Pool(comptime Conn: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        semaphore: volt.Semaphore,
        mu: volt.Mutex,
        free: std.ArrayList(*Conn),
        max: u32,

        connect_fn: *const fn (std.mem.Allocator) anyerror!*Conn,
        close_fn: *const fn (*Conn, std.mem.Allocator) void,

        pub fn init(
            alloc: std.mem.Allocator,
            max: u32,
            connect_fn: *const fn (std.mem.Allocator) anyerror!*Conn,
            close_fn: *const fn (*Conn, std.mem.Allocator) void,
        ) Self {
            return .{
                .allocator = alloc,
                .semaphore = volt.Semaphore.init(max),
                .mu = volt.Mutex.init(),
                .free = std.ArrayList(*Conn).init(alloc),
                .max = max,
                .connect_fn = connect_fn,
                .close_fn = close_fn,
            };
        }

        pub fn deinit(self: *Self) void {
            self.mu.lock();
            for (self.free.items) |c| self.close_fn(c, self.allocator);
            self.free.deinit();
            self.mu.unlock();
            self.mu.deinit();
            self.semaphore.deinit();
        }

        /// Block until a permit is available; return a fresh-or-recycled Conn.
        pub fn acquire(self: *Self) !*Conn {
            self.semaphore.acquire();
            errdefer self.semaphore.release();

            self.mu.lock();
            if (self.free.pop()) |c| {
                self.mu.unlock();
                return c;
            }
            self.mu.unlock();

            return try self.connect_fn(self.allocator);
        }

        /// Return a Conn to the pool for reuse.
        pub fn release(self: *Self, c: *Conn) void {
            self.mu.lock();
            self.free.append(c) catch {
                // Free list out of memory — just close the connection.
                self.mu.unlock();
                self.close_fn(c, self.allocator);
                self.semaphore.release();
                return;
            };
            self.mu.unlock();
            self.semaphore.release();
        }
    };
}
```

## Using it for TCP

```zig
const TcpConn = struct {
    stream: volt.net.TcpStream,
    addr: volt.net.Address,
};

fn connectTcp(alloc: std.mem.Allocator) !*TcpConn {
    const c = try alloc.create(TcpConn);
    c.addr = volt.net.Address.loopback4(6379);
    c.stream = try volt.net.TcpStream.connect(c.addr);
    return c;
}

fn closeTcp(c: *TcpConn, alloc: std.mem.Allocator) void {
    var s = c.stream;
    s.close();
    alloc.destroy(c);
}

fn useConn(pool: *Pool(TcpConn)) !void {
    const conn = try pool.acquire();
    defer pool.release(conn);

    try conn.stream.writeAll("PING\r\n");
    var buf: [16]u8 = undefined;
    _ = try conn.stream.read(&buf);
}

fn root() !void {
    var pool = Pool(TcpConn).init(std.heap.smp_allocator, 16, connectTcp, closeTcp);
    defer pool.deinit();

    // 100 coros sharing 16 connections:
    var tasks: [100]*volt.Task(void) = undefined;
    for (&tasks, 0..) |*t, i| {
        _ = i;
        t.* = try volt.spawn(struct {
            fn run(p: *Pool(TcpConn)) void {
                _ = useConn(p) catch {};
            }
        }.run, .{&pool});
    }
    for (tasks) |t| t.join();
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

## Why this design

- **Semaphore bounds total connections.** `acquire()` parks the
  caller if `max` connections are already checked out. The 17th
  caller of `pool.acquire()` waits until a peer calls `release`.
- **Free list is the cache.** A connection goes back to the free
  list on `release` rather than being closed. Next `acquire`
  pops from there instead of opening a fresh TCP connection.
- **Mutex protects the free list.** Brief — only held to push or
  pop a single pointer. Doesn't backpressure acquires.

Both the Semaphore and the Mutex are needed: the Semaphore bounds
concurrency, the Mutex protects the cache data structure.

## Lazy vs eager warm-up

The pattern above lazy-creates connections. Cold pools start empty
and warm up as demand rises. For eager warm-up, prepopulate the
free list:

```zig
pub fn warmUp(self: *Self, n: u32) !void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const c = try self.connect_fn(self.allocator);
        self.mu.lock();
        try self.free.append(c);
        self.mu.unlock();
    }
}
```

Note: warmUp doesn't acquire semaphore permits — the connections
are sitting in the free list, not "in use". The Semaphore only
counts active checkouts.

## Health checks

Real pools validate connections on acquire — close stale ones,
reconnect:

```zig
pub fn acquireHealthy(self: *Self, is_alive_fn: *const fn (*Conn) bool) !*Conn {
    self.semaphore.acquire();
    errdefer self.semaphore.release();

    while (true) {
        self.mu.lock();
        const maybe = self.free.pop();
        self.mu.unlock();
        if (maybe) |c| {
            if (is_alive_fn(c)) return c;
            self.close_fn(c, self.allocator);
            continue;
        }
        return try self.connect_fn(self.allocator);
    }
}
```

The Semaphore permit stays held across reconnect — we don't
release it until we either return a Conn or error out.

## Cancellation

A coroutine parked on `semaphore.acquire()` doesn't wake on
Cancel by default; use `acquireCancel(*Cancel)` for the
cancel-aware variant (or wrap acquire in a watchdog-cancel
scope per the [timeout-retry](/cookbook/timeout-retry/) recipe).

The `errdefer self.semaphore.release()` ensures the permit goes
back if `connect_fn` fails. Don't omit it — leaking permits
exhausts the pool.

## Maximum-idle policy

The recipe above keeps every released connection forever (up to
the Semaphore cap). For a "max idle = K" policy where excess
released connections are closed:

```zig
pub fn releaseWithIdleCap(self: *Self, c: *Conn, idle_cap: usize) void {
    self.mu.lock();
    if (self.free.items.len < idle_cap) {
        self.free.append(c) catch {
            self.mu.unlock();
            self.close_fn(c, self.allocator);
            self.semaphore.release();
            return;
        };
        self.mu.unlock();
    } else {
        self.mu.unlock();
        self.close_fn(c, self.allocator);
    }
    self.semaphore.release();
}
```

After `idle_cap` connections are cached, additional releases close
the connection instead of caching. Useful when peak demand is
much higher than steady-state.

## See also

- [Sync primitives: Semaphore](/usage/sync/) — bounded concurrency primitive.
- [Sync primitives: Mutex](/usage/sync/) — the cache lock.
- [Networking](/usage/networking/) — TcpStream API.
- [Echo Server](/cookbook/echo-server/) — the server side.

---
title: Connection Pool
description: A bounded pool of reusable connections, gated by Semaphore + a free-list Mutex-protected stack.
---

The classic "max N concurrent connections, reuse them when idle"
pattern. Volt's `Semaphore` + `Mutex` plus a free list gets you
there in ~50 lines.

```zig
const std = @import("std");
const volt = @import("volt");

fn Pool(comptime Conn: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        semaphore: volt.sync.Semaphore,
        mu: volt.sync.Mutex = .{},
        free: std.array_list.Managed(*Conn),
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
                .semaphore = volt.sync.Semaphore.init(max),
                .free = std.array_list.Managed(*Conn).init(alloc),
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
        }

        pub fn acquire(self: *Self) !*Conn {
            self.semaphore.acquire(1);
            errdefer self.semaphore.release(1);

            self.mu.lock();
            if (self.free.pop()) |c| {
                self.mu.unlock();
                return c;
            }
            self.mu.unlock();

            return try self.connect_fn(self.allocator);
        }

        pub fn release(self: *Self, c: *Conn) void {
            self.mu.lock();
            self.free.append(c) catch {
                self.mu.unlock();
                self.close_fn(c, self.allocator);
                self.semaphore.release(1);
                return;
            };
            self.mu.unlock();
            self.semaphore.release(1);
        }
    };
}
```

## Using it

```zig
const TcpConn = struct {
    stream: volt.io.TcpStream,
    addr: volt.io.Address,
};

fn connectTcp(alloc: std.mem.Allocator) !*TcpConn {
    const c = try alloc.create(TcpConn);
    c.addr = try volt.io.Address.parse("127.0.0.1:6379");
    c.stream = try volt.io.TcpStream.connect(c.addr);
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
```

## Why this design

- **Semaphore bounds total connections.** `acquire(1)` parks the
  caller if `max` connections are already checked out.
- **Free list is the cache.** A connection goes back to the free
  list on `release` rather than being closed. Next `acquire` pops
  from there.
- **Mutex protects the free list.** Brief — only held to push or
  pop a single pointer. Doesn't backpressure acquires.

Both the Semaphore and the Mutex are needed: the Semaphore bounds
concurrency, the Mutex protects the cache.

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

## Health checks

Real pools validate connections on acquire — close stale ones,
reconnect:

```zig
pub fn acquire(self: *Self) !*Conn {
    self.semaphore.acquire(1);
    errdefer self.semaphore.release(1);

    while (true) {
        self.mu.lock();
        const maybe = self.free.pop();
        self.mu.unlock();
        if (maybe) |c| {
            if (self.is_alive_fn(c)) return c;
            self.close_fn(c, self.allocator);
            continue;
        }
        return try self.connect_fn(self.allocator);
    }
}
```

## Cancellation

A coroutine parked on `semaphore.acquire(1)` wakes when cancelled.
The `errdefer self.semaphore.release(1)` ensures the permit goes
back on the cancel path. Connection cleanup on early exit happens
through `defer pool.release(conn)` at the call site.

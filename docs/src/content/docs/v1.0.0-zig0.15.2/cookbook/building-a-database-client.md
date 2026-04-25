---
title: Building a Database Client
description: Build a minimalistic Postgres client library on top of Volt, then
  show how end users consume it in their own projects.
slug: v1.0.0-zig0.15.2/cookbook/building-a-database-client
---

Most async runtimes prove their value not in benchmarks but in the libraries built on top of them. This recipe walks through creating a small but realistic Postgres client library powered by Volt, then shows how a separate application project would depend on that library. The goal is to demonstrate the full lifecycle: **library author accepts `io: volt.Io`**, library handles connections and queries internally, and **application author passes a single `Io` handle** down to every library that needs async.

:::tip[What you will learn]
* **The `Io`-as-parameter pattern** -- how library code accepts the runtime handle without owning it
* **Connection lifecycle** -- connecting, querying, and closing over TCP using Volt networking
* **Spawning work inside a library** -- using `io.@"async"` from library code, not just application code
* **Composing libraries** -- two independent Volt-based libraries sharing one runtime
* **How end users consume your library** -- the public API surface and initialization pattern
:::

## Part 1: The Library (`zig-pg`)

This is what the library author writes. The library never calls `Io.init` -- it receives an `Io` handle from whoever is using it.

### Complete Example

```zig
//! zig-pg -- A minimalistic Postgres client built on Volt.
//!
//! Usage:
//!   const pg = @import("zig-pg");
//!   var db = try pg.Client.connect(io, .{ .host = "127.0.0.1", .port = 5432 });
//!   defer db.close();
//!   const rows = try db.query("SELECT id, name FROM users WHERE active = true");

const std = @import("std");
const volt = @import("volt");

// ────────────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────────────

pub const ConnectOptions = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5432,
    user: []const u8 = "postgres",
    database: []const u8 = "postgres",
};

pub const Row = struct {
    columns: [MAX_COLUMNS]Column = undefined,
    len: u8 = 0,

    pub fn get(self: *const Row, index: usize) ?[]const u8 {
        if (index >= self.len) return null;
        const col = self.columns[index];
        return col.data[0..col.len];
    }
};

pub const QueryResult = struct {
    rows: [MAX_ROWS]Row = undefined,
    row_count: usize = 0,

    pub fn iter(self: *const QueryResult) []const Row {
        return self.rows[0..self.row_count];
    }
};

const MAX_COLUMNS = 16;
const MAX_COLUMN_DATA = 256;
const MAX_ROWS = 128;

const Column = struct {
    data: [MAX_COLUMN_DATA]u8 = undefined,
    len: u16 = 0,
};

// ────────────────────────────────────────────────────────────────────────
// Client
// ────────────────────────────────────────────────────────────────────────

pub const Client = struct {
    /// The Volt runtime handle -- borrowed, not owned.
    /// This is the key pattern: the library stores the handle so every
    /// method can spawn async work without the caller passing io again.
    io: volt.Io,
    stream: volt.net.TcpStream,
    connected: bool,

    /// Connect to a Postgres server.
    ///
    /// The `io` handle is stored for the lifetime of the client. All
    /// async operations (queries, pipelining, reconnection) use this
    /// single handle, which routes work to whatever runtime the
    /// application created.
    pub fn connect(io: volt.Io, opts: ConnectOptions) !Client {
        // Resolve and connect via TCP.
        const addr = volt.net.Address.parse(opts.host, opts.port) catch
            return error.InvalidAddress;

        var stream = volt.net.TcpStream.connect(addr) catch
            return error.ConnectionRefused;
        errdefer stream.close();

        // Perform the Postgres startup handshake.
        // In a real library this would send a StartupMessage with
        // protocol version 3.0, handle AuthenticationOk /
        // AuthenticationMD5Password, and wait for ReadyForQuery.
        // Here we send a simplified startup packet.
        var startup_buf: [256]u8 = undefined;
        const startup_len = writeStartupMessage(
            &startup_buf,
            opts.user,
            opts.database,
        );
        stream.writeAll(startup_buf[0..startup_len]) catch
            return error.HandshakeFailed;

        // Wait for server response (ReadyForQuery = 'Z').
        var resp: [1024]u8 = undefined;
        const n = waitForResponse(&stream, &resp) catch
            return error.HandshakeFailed;

        if (n == 0 or resp[0] != 'R')
            return error.HandshakeFailed;

        return .{
            .io = io,
            .stream = stream,
            .connected = true,
        };
    }

    /// Send a query and collect the result set.
    ///
    /// This is synchronous from the caller's perspective but internally
    /// uses Volt's non-blocking I/O. In a production library you would
    /// return a streaming iterator instead of collecting all rows.
    pub fn query(self: *Client, sql: []const u8) !QueryResult {
        if (!self.connected) return error.NotConnected;

        // Send the Query message ('Q' + length + sql + null terminator).
        var msg_buf: [4096]u8 = undefined;
        const msg_len = writeQueryMessage(&msg_buf, sql);
        self.stream.writeAll(msg_buf[0..msg_len]) catch
            return error.QueryFailed;

        // Read response rows.
        var result = QueryResult{};
        var resp_buf: [8192]u8 = undefined;

        while (true) {
            const n = waitForResponse(&self.stream, &resp_buf) catch
                return error.QueryFailed;
            if (n == 0) break;

            // Parse response messages. In real Postgres wire protocol:
            // 'T' = RowDescription, 'D' = DataRow, 'C' = CommandComplete,
            // 'Z' = ReadyForQuery.
            switch (resp_buf[0]) {
                'D' => {
                    // DataRow -- parse columns and add to result.
                    if (result.row_count < MAX_ROWS) {
                        result.rows[result.row_count] = parseDataRow(
                            resp_buf[0..n],
                        );
                        result.row_count += 1;
                    }
                },
                'C' => continue, // CommandComplete -- more messages follow.
                'Z' => break, // ReadyForQuery -- done.
                'E' => return error.QueryFailed, // ErrorResponse.
                else => continue,
            }
        }

        return result;
    }

    /// Run a query on the blocking pool (for CPU-intensive result processing).
    ///
    /// This demonstrates spawning work from inside a library. The `io`
    /// handle stored at connect time makes this possible without the
    /// caller threading the handle through every call.
    pub fn queryAsync(self: *Client, sql: []const u8) !volt.IoFuture(QueryResult) {
        return self.io.@"async"(queryWrapper, .{ self, sql });
    }

    /// Execute a statement that returns no rows (INSERT, UPDATE, DELETE).
    pub fn exec(self: *Client, sql: []const u8) !void {
        _ = try self.query(sql);
    }

    /// Close the connection.
    pub fn close(self: *Client) void {
        if (self.connected) {
            // Send Terminate message ('X' + length).
            var term = [_]u8{ 'X', 0, 0, 0, 4 };
            self.stream.writeAll(&term) catch {};
            self.stream.close();
            self.connected = false;
        }
    }

    // ── Internal helpers ────────────────────────────────────────────

    fn queryWrapper(client: *Client, sql: []const u8) QueryResult {
        return client.query(sql) catch QueryResult{};
    }

    fn writeStartupMessage(buf: []u8, user: []const u8, database: []const u8) usize {
        // Postgres v3.0 startup: length(i32) + version(i32) + params + \0
        var pos: usize = 0;

        // Skip length field (fill in later).
        pos += 4;

        // Protocol version 3.0 (196608 = 0x00030000).
        buf[pos] = 0x00;
        buf[pos + 1] = 0x03;
        buf[pos + 2] = 0x00;
        buf[pos + 3] = 0x00;
        pos += 4;

        // "user" parameter.
        const user_key = "user";
        @memcpy(buf[pos..][0..user_key.len], user_key);
        pos += user_key.len;
        buf[pos] = 0;
        pos += 1;
        @memcpy(buf[pos..][0..user.len], user);
        pos += user.len;
        buf[pos] = 0;
        pos += 1;

        // "database" parameter.
        const db_key = "database";
        @memcpy(buf[pos..][0..db_key.len], db_key);
        pos += db_key.len;
        buf[pos] = 0;
        pos += 1;
        @memcpy(buf[pos..][0..database.len], database);
        pos += database.len;
        buf[pos] = 0;
        pos += 1;

        // Trailing null byte.
        buf[pos] = 0;
        pos += 1;

        // Fill in the length (includes itself).
        const len: u32 = @intCast(pos);
        buf[0] = @intCast((len >> 24) & 0xFF);
        buf[1] = @intCast((len >> 16) & 0xFF);
        buf[2] = @intCast((len >> 8) & 0xFF);
        buf[3] = @intCast(len & 0xFF);

        return pos;
    }

    fn writeQueryMessage(buf: []u8, sql: []const u8) usize {
        // 'Q' + length(i32) + sql + \0
        buf[0] = 'Q';
        const body_len: u32 = @intCast(4 + sql.len + 1);
        buf[1] = @intCast((body_len >> 24) & 0xFF);
        buf[2] = @intCast((body_len >> 16) & 0xFF);
        buf[3] = @intCast((body_len >> 8) & 0xFF);
        buf[4] = @intCast(body_len & 0xFF);
        @memcpy(buf[5..][0..sql.len], sql);
        buf[5 + sql.len] = 0;
        return 5 + sql.len + 1;
    }

    fn waitForResponse(stream: *volt.net.TcpStream, buf: []u8) !usize {
        var attempts: u32 = 0;
        while (attempts < 1000) : (attempts += 1) {
            if (stream.tryRead(buf) catch |err| return err) |n| {
                return n;
            }
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
        return error.Timeout;
    }

    fn parseDataRow(data: []const u8) Row {
        // Simplified parser -- a real implementation would decode the
        // Postgres DataRow wire format (field count + per-field lengths).
        var row = Row{};
        if (data.len > 7) {
            const payload = data[7..];
            var col = Column{};
            const len: u16 = @intCast(@min(payload.len, MAX_COLUMN_DATA));
            @memcpy(col.data[0..len], payload[0..len]);
            col.len = len;
            row.columns[0] = col;
            row.len = 1;
        }
        return row;
    }
};
```

### Key design decisions

**1. Store `io`, don't own it.**

```zig
pub const Client = struct {
    io: volt.Io,      // borrowed -- caller owns the runtime
    stream: volt.net.TcpStream,
    connected: bool,
```

The `Io` handle is 8 bytes (a pointer to the Runtime). Storing it in the client means every method can spawn async work without the caller passing `io` to every call. This is the same pattern as storing an `Allocator` in a container.

**2. `connect` receives `io` once.**

```zig
pub fn connect(io: volt.Io, opts: ConnectOptions) !Client {
```

The application passes `io` at creation time. From that point on, the library has everything it needs. The caller never thinks about the runtime again.

**3. Library can spawn tasks internally.**

```zig
pub fn queryAsync(self: *Client, sql: []const u8) !volt.IoFuture(QueryResult) {
    return self.io.@"async"(queryWrapper, .{ self, sql });
}
```

Because the library stores `io`, it can spawn work onto the shared scheduler. The task runs on the application's worker pool -- no extra threads, no separate runtime.

***

## Part 2: The Application

This is what the end user of `zig-pg` writes. They create a single `Io` handle and pass it to every library.

### Complete Example

```zig
const std = @import("std");
const volt = @import("volt");
const pg = @import("zig-pg");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // One runtime for the entire application.
    // Every library, every component shares this single handle.
    var io = try volt.Io.init(gpa.allocator(), .{ .num_workers = 4 });
    defer io.deinit();

    try io.run(app);
}

fn app(io: volt.Io) !void {
    // Connect to the database -- passes io once at creation time.
    var db = try pg.Client.connect(io, .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = "myapp",
        .database = "myapp_prod",
    });
    defer db.close();

    // Run a query.
    const result = try db.query("SELECT id, name FROM users LIMIT 10");

    for (result.iter()) |row| {
        if (row.get(0)) |id| {
            std.debug.print("user: {s}\n", .{id});
        }
    }

    // Fire off an async query while doing other work.
    var future = try db.queryAsync("SELECT count(*) FROM events");

    // ... do other work here ...

    const count_result = future.@"await"(io) catch {
        std.debug.print("async query failed\n", .{});
        return;
    };
    _ = count_result;
}
```

### What the end user sees

The public API is four methods:

| Method | Description |
|--------|-------------|
| `Client.connect(io, opts)` | Open a connection. Pass `io` once. |
| `client.query(sql)` | Run a query, get rows back. |
| `client.queryAsync(sql)` | Run a query on the scheduler, get a Future. |
| `client.exec(sql)` | Execute a statement (no result rows). |
| `client.close()` | Close the connection. |

The user never interacts with Volt's internals. They do not need to know about `TcpStream`, `tryRead`, or the Postgres wire protocol. The library abstracts all of that behind a clean API that happens to accept `io: volt.Io` at the entry point.

***

## Part 3: Composing Multiple Libraries

The real power of the `Io`-as-parameter pattern emerges when an application depends on *multiple* Volt-based libraries. Each library stores the same `Io` handle, and all work runs on the same scheduler.

```zig
const std = @import("std");
const volt = @import("volt");
const pg = @import("zig-pg");         // Postgres client
const redis = @import("zig-redis");   // Redis client (hypothetical)

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // One runtime. One set of worker threads. One I/O driver.
    var io = try volt.Io.init(gpa.allocator(), .{ .num_workers = 8 });
    defer io.deinit();

    try io.run(app);
}

fn app(io: volt.Io) !void {
    // Both libraries receive the SAME io handle.
    // They share the same worker pool, timer wheel, and I/O driver.
    var db = try pg.Client.connect(io, .{
        .host = "127.0.0.1",
        .database = "myapp",
    });
    defer db.close();

    var cache = try redis.Client.connect(io, .{
        .host = "127.0.0.1",
        .port = 6379,
    });
    defer cache.close();

    // Parallel query: database + cache lookup at the same time.
    var db_future = try db.queryAsync("SELECT * FROM products WHERE id = 42");
    var cache_future = try cache.getAsync("product:42");

    // Both futures run concurrently on the shared scheduler.
    // No extra threads, no extra runtimes.
    const db_result = db_future.@"await"(io) catch QueryResult{};
    const cache_result = cache_future.@"await"(io) catch null;

    if (cache_result) |cached| {
        std.debug.print("cache hit: {s}\n", .{cached});
    } else {
        // Cache miss -- use the database result.
        for (db_result.iter()) |row| {
            if (row.get(0)) |val| {
                std.debug.print("db: {s}\n", .{val});
            }
        }
    }
}
```

In Tokio, this "just works" because the runtime is a hidden thread-local. In Volt, it works because both libraries accept `io: volt.Io` explicitly. The advantage of the explicit approach: **you can never accidentally create two runtimes**. If a function needs async, it says so in its signature.

## Walkthrough

### The Io flow

```
main()
  └─ Io.init()          ← one runtime created here
       │
       ├─ pg.Client.connect(io, ...)
       │    └─ stores io internally
       │    └─ uses io.@"async"() for async queries
       │
       ├─ redis.Client.connect(io, ...)
       │    └─ stores io internally
       │    └─ uses io.@"async"() for async gets
       │
       └─ app logic
            └─ all futures share the same worker pool
```

Every arrow in this diagram passes the same 8-byte `Io` handle. There is exactly one scheduler, one I/O driver, and one timer wheel for the entire process.

### Why not a global runtime?

Tokio uses a thread-local global to store the current runtime handle. This works well in practice but has edge cases:

* Accidentally calling `Runtime::new()` inside a library creates a second runtime with its own thread pool.
* Nested `block_on` calls panic.
* Testing requires careful setup to ensure the right runtime is active.

Volt's explicit approach avoids all of these. If a function does not take `io: volt.Io`, it cannot access the runtime -- period. There is no hidden state to get wrong.

## Try It Yourself

### Variation 1: Add a connection pool to the library

Wrap the `Client` in a pool that manages multiple connections. The pool itself stores `io` and creates new clients as needed:

```zig
pub const Pool = struct {
    io: volt.Io,
    opts: ConnectOptions,
    semaphore: volt.sync.Semaphore,
    // ... free list of Client instances ...

    pub fn init(io: volt.Io, opts: ConnectOptions, max_conns: usize) Pool {
        return .{
            .io = io,
            .opts = opts,
            .semaphore = volt.sync.Semaphore.init(max_conns),
            // ...
        };
    }

    pub fn acquire(self: *Pool) !*Client {
        // Wait for a permit, then return an idle client or create one.
        if (!self.semaphore.tryAcquire(1)) return error.PoolExhausted;
        // ... pop from free list or Client.connect(self.io, self.opts) ...
    }

    pub fn release(self: *Pool, client: *Client) void {
        // Return to free list, release permit.
        _ = client;
        self.semaphore.release(1);
    }
};
```

The end user creates one pool and shares it across their application:

```zig
var pool = pg.Pool.init(io, .{ .database = "myapp" }, 10);
// ... pass &pool to request handlers ...
```

### Variation 2: Add query timeouts

Use `volt.time.Deadline` to give queries a maximum execution time:

```zig
pub fn queryWithTimeout(self: *Client, sql: []const u8, timeout_ms: u64) !QueryResult {
    var deadline = volt.time.Deadline.init(
        volt.time.Duration.fromMillis(timeout_ms),
    );

    // Send query...
    // Read with deadline check:
    while (!deadline.isExpired()) {
        if (self.stream.tryRead(&resp_buf) catch null) |n| {
            // ... process response ...
        }
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
    return error.QueryTimeout;
}
```

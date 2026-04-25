---
title: Building a Chat Library
description: Build a reusable chat server library on top of Volt, then show how
  applications compose it with other Volt-based libraries.
slug: v1.0.0-zig0.15.2/v110/cookbook/building-a-chat-library
---

The [Chat Server](/v1.0.0-zig0.15.2/cookbook/chat-server) recipe builds a standalone chat server. This recipe takes a different angle: we build a **reusable chat library** that any application can embed. The library handles rooms, message routing, and client I/O internally. The application provides configuration, authentication hooks, and business logic on top.

This is the pattern you would follow to build any server-side library on Volt -- an HTTP framework, a message broker, a game server. The core idea: **accept `io: volt.Io`, never create your own runtime**.

:::tip[What you will learn]
* **Packaging a Volt-based server as a library** -- clean public API, hidden internals
* **The `io: volt.Io` contract** -- how library code spawns tasks on the caller's runtime
* **Room-based message routing** -- using channels and hashmaps to multiplex messages
* **Hook pattern** -- letting applications inject behavior (auth, filtering) without touching library code
* **End-to-end composition** -- an application that combines the chat library with a database client
:::

## Part 1: The Library (`zig-chat`)

### Complete Example

```zig
//! zig-chat -- An embeddable chat server library built on Volt.
//!
//! Usage:
//!   const chat = @import("zig-chat");
//!   var server = try chat.Server.init(io, allocator, .{ .port = 7000 });
//!   defer server.deinit();
//!   try server.run();

const std = @import("std");
const volt = @import("volt");

// ────────────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────────────

pub const MAX_MSG = 512;
pub const MAX_NAME = 32;
pub const MAX_ROOM = 32;

/// A chat message. Fixed-size so the broadcast channel can copy it
/// by value without heap allocation.
pub const Message = struct {
    sender: [MAX_NAME]u8 = undefined,
    sender_len: u8 = 0,
    room: [MAX_ROOM]u8 = undefined,
    room_len: u8 = 0,
    body: [MAX_MSG]u8 = undefined,
    body_len: u16 = 0,

    pub fn create(sender: []const u8, room: []const u8, body: []const u8) Message {
        var msg: Message = .{};
        const slen: u8 = @intCast(@min(sender.len, MAX_NAME));
        @memcpy(msg.sender[0..slen], sender[0..slen]);
        msg.sender_len = slen;
        const rlen: u8 = @intCast(@min(room.len, MAX_ROOM));
        @memcpy(msg.room[0..rlen], room[0..rlen]);
        msg.room_len = rlen;
        const blen: u16 = @intCast(@min(body.len, MAX_MSG));
        @memcpy(msg.body[0..blen], body[0..blen]);
        msg.body_len = blen;
        return msg;
    }

    pub fn system(room: []const u8, body: []const u8) Message {
        return create("system", room, body);
    }

    pub fn senderName(self: *const Message) []const u8 {
        return self.sender[0..self.sender_len];
    }

    pub fn roomName(self: *const Message) []const u8 {
        return self.room[0..self.room_len];
    }

    pub fn text(self: *const Message) []const u8 {
        return self.body[0..self.body_len];
    }

    pub fn format(self: *const Message, out: []u8) []const u8 {
        return std.fmt.bufPrint(out, "[#{s}] {s}: {s}", .{
            self.roomName(),
            self.senderName(),
            self.text(),
        }) catch self.text();
    }
};

/// Configuration for the chat server.
pub const Config = struct {
    port: u16 = 7000,
    max_clients: usize = 256,
    ring_capacity: usize = 128,
    /// Optional authentication hook. Return true to allow the connection.
    /// If null, all connections are accepted.
    auth_fn: ?*const fn (name: []const u8) bool = null,
    /// Optional message filter. Return true to allow the message.
    /// Use this for profanity filtering, rate limiting, etc.
    filter_fn: ?*const fn (msg: *const Message) bool = null,
};

const BC = volt.channel.BroadcastChannel(Message);

// ────────────────────────────────────────────────────────────────────────
// Server
// ────────────────────────────────────────────────────────────────────────

pub const Server = struct {
    /// The Volt runtime handle -- borrowed from the application.
    io: volt.Io,
    allocator: std.mem.Allocator,
    config: Config,
    broadcast: *BC,
    client_count: std.atomic.Value(u32),

    /// Initialize the chat server.
    ///
    /// `io` is stored for the lifetime of the server. All client
    /// handling tasks are spawned onto the application's scheduler.
    pub fn init(io: volt.Io, allocator: std.mem.Allocator, config: Config) !Server {
        const bc = try allocator.create(BC);
        bc.* = try volt.channel.broadcast(
            Message,
            allocator,
            config.ring_capacity,
        );

        return .{
            .io = io,
            .allocator = allocator,
            .config = config,
            .broadcast = bc,
            .client_count = std.atomic.Value(u32).init(0),
        };
    }

    pub fn deinit(self: *Server) void {
        self.broadcast.deinit();
        self.allocator.destroy(self.broadcast);
    }

    /// Run the accept loop. Blocks until the listener is closed.
    ///
    /// Each connected client becomes a lightweight task on the shared
    /// scheduler. The server itself does not create any threads --
    /// everything runs on the worker pool that the application set up.
    pub fn run(self: *Server) void {
        var listener = volt.net.TcpListener.bind(
            volt.net.Address.fromPort(self.config.port),
        ) catch |err| {
            std.debug.print("chat: bind failed: {}\n", .{err});
            return;
        };
        defer listener.close();

        const port = listener.localAddr().port();
        std.debug.print("chat: listening on port {}\n", .{port});

        while (true) {
            if (listener.tryAccept() catch null) |result| {
                if (self.client_count.load(.monotonic) >=
                    self.config.max_clients)
                {
                    var stream = result.stream;
                    stream.writeAll("Server full\n") catch {};
                    stream.close();
                    continue;
                }

                var rx = self.broadcast.subscribe();
                var f = self.io.@"async"(clientHandler, .{
                    self,
                    result.stream,
                    &rx,
                }) catch {
                    var stream = result.stream;
                    stream.close();
                    continue;
                };
                f.detach();
            } else {
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }
    }

    /// Get the number of currently connected clients.
    pub fn clientCount(self: *const Server) u32 {
        return self.client_count.load(.monotonic);
    }

    // ── Internal ────────────────────────────────────────────────────

    fn clientHandler(
        self: *Server,
        conn: volt.net.TcpStream,
        rx: *BC.Receiver,
    ) void {
        var stream = conn;
        defer {
            stream.close();
            _ = self.client_count.fetchSub(1, .monotonic);
        }
        _ = self.client_count.fetchAdd(1, .monotonic);

        // Prompt for username.
        stream.writeAll("Name: ") catch return;

        var buf: [1024]u8 = undefined;
        var name_buf: [MAX_NAME]u8 = undefined;
        var name_len: u8 = 0;

        while (name_len == 0) {
            if (stream.tryRead(&buf) catch null) |n| {
                if (n == 0) return;
                const trimmed = trimLine(buf[0..n]);
                if (trimmed.len == 0) continue;
                name_len = @intCast(@min(trimmed.len, MAX_NAME));
                @memcpy(name_buf[0..name_len], trimmed[0..name_len]);
            } else {
                std.Thread.sleep(5 * std.time.ns_per_ms);
            }
        }

        const name = name_buf[0..name_len];

        // Run auth hook if configured.
        if (self.config.auth_fn) |auth| {
            if (!auth(name)) {
                stream.writeAll("Authentication failed\n") catch {};
                return;
            }
        }

        // Default room.
        const room = "general";

        _ = self.broadcast.send(Message.system(room, join_msg: {
            var tmp: [MAX_MSG]u8 = undefined;
            const text = std.fmt.bufPrint(&tmp, "** {s} joined **\n", .{name}) catch "** joined **\n";
            break :join_msg text;
        }));

        stream.writeAll("Joined #general. Type messages and press Enter.\n") catch {};

        // Main client loop.
        while (true) {
            // Read from client.
            if (stream.tryRead(&buf) catch null) |n| {
                if (n == 0) break;

                const msg = Message.create(name, room, buf[0..n]);

                // Run filter hook if configured.
                const allowed = if (self.config.filter_fn) |filter|
                    filter(&msg)
                else
                    true;

                if (allowed) {
                    _ = self.broadcast.send(msg);
                }
            }

            // Relay messages to client.
            while (true) {
                switch (rx.tryRecv()) {
                    .value => |msg| {
                        var fmt_buf: [MAX_MSG + MAX_NAME + MAX_ROOM + 8]u8 = undefined;
                        const formatted = msg.format(&fmt_buf);
                        stream.writeAll(formatted) catch break;
                    },
                    .lagged => |count| {
                        var lag_buf: [64]u8 = undefined;
                        const lag_msg = std.fmt.bufPrint(
                            &lag_buf,
                            "[missed {} messages]\n",
                            .{count},
                        ) catch break;
                        stream.writeAll(lag_msg) catch break;
                    },
                    .empty => break,
                    .closed => return,
                }
            }

            std.Thread.sleep(5 * std.time.ns_per_ms);
        }

        _ = self.broadcast.send(Message.system(room, leave_msg: {
            var tmp: [MAX_MSG]u8 = undefined;
            const text = std.fmt.bufPrint(&tmp, "** {s} left **\n", .{name}) catch "** left **\n";
            break :leave_msg text;
        }));
    }

    fn trimLine(input: []const u8) []const u8 {
        var s = input;
        while (s.len > 0 and (s[s.len - 1] == '\n' or s[s.len - 1] == '\r')) {
            s = s[0 .. s.len - 1];
        }
        return s;
    }
};
```

### Key design decisions

**1. Library never calls `Io.init`.**

The application creates the runtime. The library just uses it. This is the golden rule for any Volt-based library.

**2. `Server.init(io, allocator, config)` -- the entry point.**

Same pattern as the database client: accept `io` once, store it, use it for all async operations. The library's `run()` method spawns client handler tasks onto the application's shared scheduler.

**3. Hook functions for extensibility.**

```zig
pub const Config = struct {
    auth_fn: ?*const fn (name: []const u8) bool = null,
    filter_fn: ?*const fn (msg: *const Message) bool = null,
};
```

Instead of the library implementing authentication or filtering directly, it accepts function pointers from the application. This keeps the library focused on chat mechanics while letting applications inject their own logic.

***

## Part 2: The Application

An application that combines the chat library with a hypothetical database for user authentication and message logging.

### Complete Example

```zig
const std = @import("std");
const volt = @import("volt");
const chat = @import("zig-chat");
const pg = @import("zig-pg");  // from the database client cookbook

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // ── One runtime for everything ─────────────────────────────────
    var io = try volt.Io.init(allocator, .{ .num_workers = 8 });
    defer io.deinit();

    try io.run(app);
}

fn app(io: volt.Io) !void {
    // ── Database (for auth + logging) ──────────────────────────────
    var db = try pg.Client.connect(io, .{
        .host = "127.0.0.1",
        .database = "chatapp",
    });
    defer db.close();

    // Store db pointer in a global so auth hook can access it.
    db_handle = &db;

    // ── Chat server ────────────────────────────────────────────────
    var server = try chat.Server.init(io, std.heap.page_allocator, .{
        .port = 7000,
        .max_clients = 100,
        .ring_capacity = 256,
        .auth_fn = authenticateUser,
        .filter_fn = filterMessage,
    });
    defer server.deinit();

    std.debug.print("Chat app running on port 7000\n", .{});
    std.debug.print("Database connected\n", .{});

    // This blocks -- the chat server runs its accept loop on
    // the shared scheduler. Database queries from the auth hook
    // run on the same worker pool.
    server.run();
}

// ── Application hooks ──────────────────────────────────────────────────

var db_handle: ?*pg.Client = null;

/// Called by the chat library when a user tries to connect.
/// We check the database to see if the username is registered.
fn authenticateUser(name: []const u8) bool {
    const db = db_handle orelse return false;

    // Query the users table. This runs on the shared Volt scheduler --
    // the same workers that handle chat I/O.
    var query_buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(
        &query_buf,
        "SELECT 1 FROM users WHERE name = '{s}' AND active = true",
        .{name},
    ) catch return false;

    const result = db.query(sql) catch return false;
    return result.row_count > 0;
}

/// Called by the chat library before broadcasting a message.
/// We reject empty messages and messages that are too long.
fn filterMessage(msg: *const chat.Message) bool {
    const text = msg.text();
    if (text.len == 0) return false;
    if (text.len > 200) return false; // Keep messages reasonable.
    return true;
}
```

### What the end user sees

```
main()
  └─ Io.init(num_workers=8)       ← one runtime
       │
       ├─ pg.Client.connect(io)    ← stores io, uses it for queries
       │
       ├─ chat.Server.init(io)     ← stores io, spawns client tasks
       │    ├─ auth_fn → calls db.query() on same scheduler
       │    └─ filter_fn → pure function, no async needed
       │
       └─ server.run()             ← blocks, accept loop on scheduler
            └─ per-client tasks    ← spawned via io.@"async"
```

Everything -- database queries from the auth hook, chat message routing, client I/O -- runs on the same 8 worker threads. There is one I/O driver polling kqueue/epoll, one timer wheel, one scheduler. No resource duplication.

***

## Part 3: How Users Discover and Use Your Library

When you publish `zig-chat` as a Zig package, users add it to their `build.zig.zon`:

```zig
.dependencies = .{
    .volt = .{ .url = "..." },
    .@"zig-chat" = .{ .url = "..." },
},
```

And in their `build.zig`:

```zig
const chat_dep = b.dependency("zig-chat", .{});
exe.root_module.addImport("zig-chat", chat_dep.module("zig-chat"));
```

The user's code is then:

```zig
const volt = @import("volt");
const chat = @import("zig-chat");

pub fn main() !void {
    var io = try volt.Io.init(allocator, .{});
    defer io.deinit();

    var server = try chat.Server.init(io, allocator, .{ .port = 9000 });
    defer server.deinit();

    try io.run(server.run);
}
```

Three lines to embed a full chat server into any application. The library user does not need to understand broadcast channels, TCP accept loops, or subscriber cursors. They get a `Server` with a `run()` method and optional hooks for customization.

## The Pattern, Summarized

Every Volt-based library follows the same three rules:

| Rule | Why |
|------|-----|
| **Accept `io: volt.Io` at creation time** | The application owns the runtime; the library borrows it |
| **Store `io` in your struct** | So methods can spawn work without the caller passing io every time |
| **Never call `Io.init`** | One runtime per process, created by the application |

This is identical to how Zig's standard library handles allocators. If you know `fn foo(allocator: Allocator)`, you already understand `fn bar(io: volt.Io)`.

## Try It Yourself

### Variation 1: Add room support

Extend the server to support multiple rooms with `/join #room` commands. Each room gets its own broadcast channel, and clients only receive messages from rooms they have joined.

### Variation 2: Add message persistence

Use the database client to log every message to a `messages` table. The chat library's filter hook is a natural place to insert this -- or add a dedicated `on_message` hook that fires after broadcast.

### Variation 3: Add WebSocket support

Replace the raw TCP accept loop with an HTTP upgrade handler. The client handler stays the same -- only the transport changes. This is the kind of layering that the `io: volt.Io` pattern enables cleanly.

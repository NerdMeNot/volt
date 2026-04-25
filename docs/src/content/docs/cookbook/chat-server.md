---
title: Chat Server
description: A multi-client chat server using BroadcastChannel for message fan-out with graceful disconnect handling.
---

:::tip[What you'll learn]
- Using `BroadcastChannel` for fan-out to multiple clients
- The per-client subscriber pattern (each connection gets its own cursor)
- Handling lagged receivers that fall behind the ring buffer
- Message lifetime management with fixed-size value types
:::

This recipe builds a simple chat server where every connected client sees every message. A `BroadcastChannel` handles fan-out, and each client gets its own subscriber so messages are never lost (unless a slow reader lags behind the ring buffer).

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_MSG_BODY = 512;
const RING_CAPACITY = 64; // broadcast ring buffer capacity

/// Fixed-size message struct stored by value in the ring buffer.
/// Using a fixed-size struct avoids lifetime issues -- the broadcast
/// channel copies the entire struct into the ring, so no pointers
/// dangle after the sender's stack frame returns.
const ChatMessage = struct {
    body: [MAX_MSG_BODY]u8 = undefined,
    body_len: u16 = 0,

    pub fn create(text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const len: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..len], text[0..len]);
        msg.body_len = len;
        return msg;
    }

    pub fn text(self: *const ChatMessage) []const u8 {
        return self.body[0..self.body_len];
    }
};

const BC = volt.channel.BroadcastChannel(ChatMessage);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // The broadcast channel carries chat messages to all subscribers.
    // Every call to send() copies a ChatMessage into the shared ring buffer.
    var chat = try volt.channel.broadcast(ChatMessage, allocator, RING_CAPACITY);
    defer chat.deinit();

    volt.run(server, .{&chat});
}

fn server(io: volt.Runtime, chat: *BC) void {
    var listener = try volt.net.TcpListener.bind(
        volt.net.Address.fromPort(7000),
    );
    defer listener.close();

    const port = listener.localAddr().port();
    std.debug.print("Chat server on port {}\n", .{port});
    std.debug.print("Test: nc localhost {}\n\n", .{port});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            // Each client gets its own subscriber -- an independent cursor
            // into the shared ring buffer. This means every client receives
            // every message published after it subscribed.
            var rx = chat.subscribe();
            var f = io.@"async"(clientTask, .{
                result.stream,
                chat,
                &rx,
            }) catch {
                var stream = result.stream;
                stream.close();
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }
}

fn clientTask(
    conn: volt.net.TcpStream,
    chat: *BC,
    rx: *BC.Receiver,
) void {
    var stream = conn;
    defer stream.close();

    // Announce join.
    _ = chat.send(ChatMessage.create("** A new user joined **"));

    var buf: [1024]u8 = undefined;

    while (true) {
        // 1. Check for incoming data from this client.
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) break; // TCP peer closed its side.

            // Wrap the raw bytes in a ChatMessage. The struct is copied
            // by value into the ring buffer, so buf can be reused
            // immediately on the next iteration.
            _ = chat.send(ChatMessage.create(buf[0..n]));
        }

        // 2. Relay broadcast messages back to this client.
        while (true) {
            switch (rx.tryRecv()) {
                .value => |msg| {
                    stream.writeAll(msg.text()) catch break;
                },
                .lagged => |count| {
                    // This client's cursor fell behind the write head.
                    // The ring buffer overwrote messages it had not read.
                    var lag_buf: [64]u8 = undefined;
                    const lag_msg = std.fmt.bufPrint(
                        &lag_buf,
                        "[missed {} messages]\n",
                        .{count},
                    ) catch break;
                    stream.writeAll(lag_msg) catch break;
                },
                .empty => break, // Caught up -- nothing more right now.
                .closed => return, // Channel was closed (server shutting down).
            }
        }

        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    _ = chat.send(ChatMessage.create("** A user left **"));
}
```

## Walkthrough

### Why BroadcastChannel?

A regular `Channel` is multi-producer, single-consumer -- only one task can receive each message. For chat, every client needs to receive every message. `BroadcastChannel` provides exactly this: each `subscribe()` call creates an independent cursor into the shared ring buffer. Messages stay in the ring until all active subscribers have read them (or the ring wraps around).

This is the same fan-out pattern Tokio's `broadcast::channel` provides.

### Why a fixed-size ChatMessage instead of `[]const u8`?

The broadcast channel stores values **by copy**. If you broadcast a `[]const u8` slice, the channel copies the *fat pointer* (pointer + length), not the bytes it points to. When those bytes live in a stack buffer like `var buf: [1024]u8`, the pointer becomes invalid as soon as the sender's stack frame reuses that buffer on the next loop iteration.

A fixed-size struct solves this entirely:

```zig
const ChatMessage = struct {
    body: [512]u8 = undefined,
    body_len: u16 = 0,
    // ...
};
```

When the channel copies this struct into the ring, it copies all 514 bytes -- the character data comes along for the ride. No dangling pointers, no allocator needed.

The tradeoff is that every ring slot is 514 bytes regardless of message length. For a ring capacity of 64, that is about 32 KB -- a reasonable price for a chat server that avoids heap allocation on every message.

### The per-client subscriber pattern

Each call to `chat.subscribe()` returns a new `Receiver` with its own read cursor initialized to the current write position. This means:

- Messages sent **before** the subscription are not visible to the new subscriber.
- Each subscriber advances independently -- a slow reader does not block fast readers.
- The subscriber is lightweight (just a cursor index and a pointer to the shared ring).

```zig
// In the accept loop: one subscriber per client
var rx = chat.subscribe();
var f = io.@"async"(clientTask, .{ result.stream, chat, &rx }) catch continue;
f.detach();
```

### Handling lagged receivers

If a slow client cannot keep up, its cursor falls behind the write head and messages are overwritten in the ring buffer. The `.lagged` result tells the client how many messages it missed:

```zig
.lagged => |count| {
    // count = number of messages this receiver missed
},
```

This is a deliberate design choice -- it prevents one slow client from blocking all others. You can tune the ring buffer capacity to trade memory for lag tolerance:

```zig
// Larger buffer = more messages before lagging occurs
var chat = try volt.channel.broadcast(ChatMessage, allocator, 256);
```

### Graceful disconnect

When `tryRead` returns `n == 0`, the TCP peer has closed its side of the connection (a FIN packet). The handler breaks out of the loop, the `defer stream.close()` runs, and a departure message is broadcast to all remaining clients.

This ordering matters: the departure broadcast happens **after** `defer` has been registered but **before** it executes, so the stream is still open if anything needs it during the broadcast.

## Try it yourself

The following variations build on the complete example above. Each one is self-contained -- you can drop it in and run it.

### Variation 1: Add usernames

The first message from each client is treated as their username. Subsequent messages are prefixed with that name.

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_MSG_BODY = 512;
const MAX_NAME = 32;
const RING_CAPACITY = 64;

const ChatMessage = struct {
    name: [MAX_NAME]u8 = undefined,
    name_len: u8 = 0,
    body: [MAX_MSG_BODY]u8 = undefined,
    body_len: u16 = 0,

    pub fn system(text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const len: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..len], text[0..len]);
        msg.body_len = len;
        return msg;
    }

    pub fn fromUser(name: []const u8, text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const nlen: u8 = @intCast(@min(name.len, MAX_NAME));
        @memcpy(msg.name[0..nlen], name[0..nlen]);
        msg.name_len = nlen;
        const blen: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..blen], text[0..blen]);
        msg.body_len = blen;
        return msg;
    }

    pub fn userName(self: *const ChatMessage) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn text(self: *const ChatMessage) []const u8 {
        return self.body[0..self.body_len];
    }

    /// Format as "[name] body" or just "body" for system messages.
    pub fn format(self: *const ChatMessage, out: []u8) []const u8 {
        if (self.name_len == 0) {
            const len = @min(self.body_len, out.len);
            @memcpy(out[0..len], self.body[0..len]);
            return out[0..len];
        }
        return std.fmt.bufPrint(out, "[{s}] {s}", .{
            self.userName(),
            self.text(),
        }) catch self.text();
    }
};

const BC = volt.channel.BroadcastChannel(ChatMessage);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var chat = try volt.channel.broadcast(ChatMessage, allocator, RING_CAPACITY);
    defer chat.deinit();

    volt.run(server, .{&chat});
}

fn server(io: volt.Runtime, chat: *BC) void {
    var listener = try volt.net.TcpListener.bind(
        volt.net.Address.fromPort(7000),
    );
    defer listener.close();

    std.debug.print("Chat server on port {}\n", .{listener.localAddr().port()});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var rx = chat.subscribe();
            var f = io.@"async"(clientTask, .{
                result.stream,
                chat,
                &rx,
            }) catch {
                var stream = result.stream;
                stream.close();
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }
}

fn clientTask(
    conn: volt.net.TcpStream,
    chat: *BC,
    rx: *BC.Receiver,
) void {
    var stream = conn;
    defer stream.close();

    // Prompt the client for a username.
    stream.writeAll("Enter your name: ") catch return;

    var buf: [1024]u8 = undefined;
    var username: [MAX_NAME]u8 = undefined;
    var username_len: u8 = 0;

    // Wait for the first message -- that becomes the username.
    while (username_len == 0) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) return; // Disconnected before sending a name.
            // Strip trailing newline/carriage return.
            var name = buf[0..n];
            while (name.len > 0 and (name[name.len - 1] == '\n' or name[name.len - 1] == '\r')) {
                name = name[0 .. name.len - 1];
            }
            if (name.len == 0) continue;
            username_len = @intCast(@min(name.len, MAX_NAME));
            @memcpy(username[0..username_len], name[0..username_len]);
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }

    const name_slice = username[0..username_len];

    // Announce join.
    var join_buf: [MAX_MSG_BODY]u8 = undefined;
    const join_text = std.fmt.bufPrint(&join_buf, "** {s} joined **", .{name_slice}) catch "** A user joined **";
    _ = chat.send(ChatMessage.system(join_text));

    while (true) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) break;
            _ = chat.send(ChatMessage.fromUser(name_slice, buf[0..n]));
        }

        // Relay messages to this client.
        while (true) {
            switch (rx.tryRecv()) {
                .value => |msg| {
                    var fmt_buf: [MAX_MSG_BODY + MAX_NAME + 4]u8 = undefined;
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

    var leave_buf: [MAX_MSG_BODY]u8 = undefined;
    const leave_text = std.fmt.bufPrint(&leave_buf, "** {s} left **", .{name_slice}) catch "** A user left **";
    _ = chat.send(ChatMessage.system(leave_text));
}
```

### Variation 2: Add a "/who" command

Building on the username variation, this adds a shared user registry. When a client sends `/who`, they get a list of all connected users. The registry is protected by a `Mutex` because multiple client tasks access it concurrently.

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_MSG_BODY = 512;
const MAX_NAME = 32;
const MAX_USERS = 256;
const RING_CAPACITY = 64;

const ChatMessage = struct {
    name: [MAX_NAME]u8 = undefined,
    name_len: u8 = 0,
    body: [MAX_MSG_BODY]u8 = undefined,
    body_len: u16 = 0,

    pub fn system(text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const len: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..len], text[0..len]);
        msg.body_len = len;
        return msg;
    }

    pub fn fromUser(name: []const u8, text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const nlen: u8 = @intCast(@min(name.len, MAX_NAME));
        @memcpy(msg.name[0..nlen], name[0..nlen]);
        msg.name_len = nlen;
        const blen: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..blen], text[0..blen]);
        msg.body_len = blen;
        return msg;
    }

    pub fn userName(self: *const ChatMessage) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn text(self: *const ChatMessage) []const u8 {
        return self.body[0..self.body_len];
    }

    pub fn format(self: *const ChatMessage, out: []u8) []const u8 {
        if (self.name_len == 0) {
            const len = @min(self.body_len, out.len);
            @memcpy(out[0..len], self.body[0..len]);
            return out[0..len];
        }
        return std.fmt.bufPrint(out, "[{s}] {s}", .{
            self.userName(),
            self.text(),
        }) catch self.text();
    }
};

const BC = volt.channel.BroadcastChannel(ChatMessage);

/// Thread-safe user registry. Tracks connected usernames so "/who" can
/// list them. Uses a fixed-size array to avoid heap allocation per join.
const UserRegistry = struct {
    names: [MAX_USERS][MAX_NAME]u8 = undefined,
    lengths: [MAX_USERS]u8 = [_]u8{0} ** MAX_USERS,
    count: usize = 0,
    lock: volt.sync.Mutex = volt.sync.Mutex.init(),

    pub fn add(self: *UserRegistry, name: []const u8) void {
        if (!self.lock.tryLock()) return;
        defer self.lock.unlock();
        if (self.count >= MAX_USERS) return;
        const len: u8 = @intCast(@min(name.len, MAX_NAME));
        @memcpy(self.names[self.count][0..len], name[0..len]);
        self.lengths[self.count] = len;
        self.count += 1;
    }

    pub fn remove(self: *UserRegistry, name: []const u8) void {
        if (!self.lock.tryLock()) return;
        defer self.lock.unlock();
        for (0..self.count) |i| {
            const entry = self.names[i][0..self.lengths[i]];
            if (std.mem.eql(u8, entry, name)) {
                // Swap-remove: move the last entry into this slot.
                if (i < self.count - 1) {
                    const last = self.count - 1;
                    @memcpy(
                        self.names[i][0..self.lengths[last]],
                        self.names[last][0..self.lengths[last]],
                    );
                    self.lengths[i] = self.lengths[last];
                }
                self.count -= 1;
                return;
            }
        }
    }

    /// Write a newline-separated list of connected users into the buffer.
    pub fn listUsers(self: *UserRegistry, out: []u8) []const u8 {
        if (!self.lock.tryLock()) return "Unable to list users\n";
        defer self.lock.unlock();
        if (self.count == 0) return "No users connected\n";

        var pos: usize = 0;
        const header = "Connected users:\n";
        if (pos + header.len <= out.len) {
            @memcpy(out[pos..][0..header.len], header);
            pos += header.len;
        }
        for (0..self.count) |i| {
            const name = self.names[i][0..self.lengths[i]];
            // Write "  name\n"
            if (pos + 2 + name.len + 1 > out.len) break;
            out[pos] = ' ';
            out[pos + 1] = ' ';
            pos += 2;
            @memcpy(out[pos..][0..name.len], name);
            pos += name.len;
            out[pos] = '\n';
            pos += 1;
        }
        return out[0..pos];
    }
};

var users = UserRegistry{};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var chat = try volt.channel.broadcast(ChatMessage, allocator, RING_CAPACITY);
    defer chat.deinit();

    volt.run(server, .{&chat});
}

fn server(io: volt.Runtime, chat: *BC) void {
    var listener = try volt.net.TcpListener.bind(
        volt.net.Address.fromPort(7000),
    );
    defer listener.close();

    std.debug.print("Chat server on port {}\n", .{listener.localAddr().port()});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var rx = chat.subscribe();
            var f = io.@"async"(clientTask, .{
                result.stream,
                chat,
                &rx,
            }) catch {
                var stream = result.stream;
                stream.close();
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }
}

/// Strip trailing \r and \n from a slice.
fn trimLine(input: []const u8) []const u8 {
    var s = input;
    while (s.len > 0 and (s[s.len - 1] == '\n' or s[s.len - 1] == '\r')) {
        s = s[0 .. s.len - 1];
    }
    return s;
}

fn clientTask(
    conn: volt.net.TcpStream,
    chat: *BC,
    rx: *BC.Receiver,
) void {
    var stream = conn;
    defer stream.close();

    stream.writeAll("Enter your name: ") catch return;

    var buf: [1024]u8 = undefined;
    var username: [MAX_NAME]u8 = undefined;
    var username_len: u8 = 0;

    // Wait for the username.
    while (username_len == 0) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) return;
            const name = trimLine(buf[0..n]);
            if (name.len == 0) continue;
            username_len = @intCast(@min(name.len, MAX_NAME));
            @memcpy(username[0..username_len], name[0..username_len]);
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }

    const name_slice = username[0..username_len];
    users.add(name_slice);

    var join_buf: [MAX_MSG_BODY]u8 = undefined;
    const join_text = std.fmt.bufPrint(
        &join_buf,
        "** {s} joined **\n",
        .{name_slice},
    ) catch "** A user joined **\n";
    _ = chat.send(ChatMessage.system(join_text));

    while (true) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) break;

            const line = trimLine(buf[0..n]);

            // Handle the /who command locally -- only this client sees the result.
            if (std.mem.eql(u8, line, "/who")) {
                var who_buf: [4096]u8 = undefined;
                const listing = users.listUsers(&who_buf);
                stream.writeAll(listing) catch break;
                continue;
            }

            _ = chat.send(ChatMessage.fromUser(name_slice, buf[0..n]));
        }

        while (true) {
            switch (rx.tryRecv()) {
                .value => |msg| {
                    var fmt_buf: [MAX_MSG_BODY + MAX_NAME + 4]u8 = undefined;
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

    users.remove(name_slice);

    var leave_buf: [MAX_MSG_BODY]u8 = undefined;
    const leave_text = std.fmt.bufPrint(
        &leave_buf,
        "** {s} left **\n",
        .{name_slice},
    ) catch "** A user left **\n";
    _ = chat.send(ChatMessage.system(leave_text));
}
```

### Variation 3: Add message timestamps

This variation includes a timestamp in every `ChatMessage`. Receivers format the time when displaying the message, giving each client a consistent view of when messages were sent.

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_MSG_BODY = 512;
const MAX_NAME = 32;
const RING_CAPACITY = 64;

const ChatMessage = struct {
    name: [MAX_NAME]u8 = undefined,
    name_len: u8 = 0,
    body: [MAX_MSG_BODY]u8 = undefined,
    body_len: u16 = 0,
    /// Wall-clock timestamp in nanoseconds since the epoch.
    /// Captured at send time so all receivers see the same value.
    timestamp_ns: i128 = 0,

    pub fn system(text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const len: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..len], text[0..len]);
        msg.body_len = len;
        msg.timestamp_ns = std.time.nanoTimestamp();
        return msg;
    }

    pub fn fromUser(name: []const u8, text: []const u8) ChatMessage {
        var msg: ChatMessage = .{};
        const nlen: u8 = @intCast(@min(name.len, MAX_NAME));
        @memcpy(msg.name[0..nlen], name[0..nlen]);
        msg.name_len = nlen;
        const blen: u16 = @intCast(@min(text.len, MAX_MSG_BODY));
        @memcpy(msg.body[0..blen], text[0..blen]);
        msg.body_len = blen;
        msg.timestamp_ns = std.time.nanoTimestamp();
        return msg;
    }

    pub fn userName(self: *const ChatMessage) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn text(self: *const ChatMessage) []const u8 {
        return self.body[0..self.body_len];
    }

    /// Format as "HH:MM:SS [name] body" or "HH:MM:SS body" for system messages.
    pub fn format(self: *const ChatMessage, out: []u8) []const u8 {
        // Convert nanosecond timestamp to hour:minute:second.
        const epoch_secs: u64 = @intCast(@divTrunc(self.timestamp_ns, std.time.ns_per_s));
        const day_secs = epoch_secs % 86400;
        const hours = day_secs / 3600;
        const minutes = (day_secs % 3600) / 60;
        const seconds = day_secs % 60;

        if (self.name_len == 0) {
            return std.fmt.bufPrint(out, "{d:0>2}:{d:0>2}:{d:0>2} {s}", .{
                hours,
                minutes,
                seconds,
                self.text(),
            }) catch self.text();
        }
        return std.fmt.bufPrint(out, "{d:0>2}:{d:0>2}:{d:0>2} [{s}] {s}", .{
            hours,
            minutes,
            seconds,
            self.userName(),
            self.text(),
        }) catch self.text();
    }
};

const BC = volt.channel.BroadcastChannel(ChatMessage);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var chat = try volt.channel.broadcast(ChatMessage, allocator, RING_CAPACITY);
    defer chat.deinit();

    volt.run(server, .{&chat});
}

fn server(io: volt.Runtime, chat: *BC) void {
    var listener = try volt.net.TcpListener.bind(
        volt.net.Address.fromPort(7000),
    );
    defer listener.close();

    std.debug.print("Chat server on port {}\n", .{listener.localAddr().port()});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var rx = chat.subscribe();
            var f = io.@"async"(clientTask, .{
                result.stream,
                chat,
                &rx,
            }) catch {
                var stream = result.stream;
                stream.close();
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }
}

fn trimLine(input: []const u8) []const u8 {
    var s = input;
    while (s.len > 0 and (s[s.len - 1] == '\n' or s[s.len - 1] == '\r')) {
        s = s[0 .. s.len - 1];
    }
    return s;
}

fn clientTask(
    conn: volt.net.TcpStream,
    chat: *BC,
    rx: *BC.Receiver,
) void {
    var stream = conn;
    defer stream.close();

    stream.writeAll("Enter your name: ") catch return;

    var buf: [1024]u8 = undefined;
    var username: [MAX_NAME]u8 = undefined;
    var username_len: u8 = 0;

    while (username_len == 0) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) return;
            const name = trimLine(buf[0..n]);
            if (name.len == 0) continue;
            username_len = @intCast(@min(name.len, MAX_NAME));
            @memcpy(username[0..username_len], name[0..username_len]);
        } else {
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
    }

    const name_slice = username[0..username_len];

    var join_buf: [MAX_MSG_BODY]u8 = undefined;
    const join_text = std.fmt.bufPrint(
        &join_buf,
        "** {s} joined **\n",
        .{name_slice},
    ) catch "** A user joined **\n";
    _ = chat.send(ChatMessage.system(join_text));

    while (true) {
        if (stream.tryRead(&buf) catch null) |n| {
            if (n == 0) break;
            _ = chat.send(ChatMessage.fromUser(name_slice, buf[0..n]));
        }

        while (true) {
            switch (rx.tryRecv()) {
                .value => |msg| {
                    // Format includes the timestamp prefix.
                    var fmt_buf: [MAX_MSG_BODY + MAX_NAME + 16]u8 = undefined;
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

    var leave_buf: [MAX_MSG_BODY]u8 = undefined;
    const leave_text = std.fmt.bufPrint(
        &leave_buf,
        "** {s} left **\n",
        .{name_slice},
    ) catch "** A user left **\n";
    _ = chat.send(ChatMessage.system(leave_text));
}
```

The timestamp is captured once at send time via `std.time.nanoTimestamp()`. Every receiver sees the same value, which gives a consistent ordering even if relay to the client is delayed. The `format` function converts nanoseconds to `HH:MM:SS` (UTC) for display.

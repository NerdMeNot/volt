---
title: Channel API
description: Complete API reference for Channel, Oneshot, BroadcastChannel, Watch, and Select in Volt.
---

Channels let tasks communicate by sending values instead of sharing memory -- the "share by communicating" pattern. Instead of protecting a queue with a mutex, you send a message through a channel and the receiver gets its own copy. This eliminates an entire class of data races and makes concurrent code easier to reason about.

Volt provides four channel types, each for a different messaging pattern:

- **Channel** -- bounded MPMC queue for work distribution. Multiple producers push work, multiple consumers pull it. Backpressure is automatic: senders block when the buffer is full. Backed by a lock-free Vyukov ring buffer.
- **Oneshot** -- single-value, single-use. One task sends a result, another awaits it. The async equivalent of returning a value from a spawned task. Zero allocation.
- **BroadcastChannel** -- every subscriber gets every message. Perfect for event buses, log fanout, or "config changed" notifications. Slow receivers lag instead of blocking senders.
- **Watch** -- holds a single "latest value" with change notification. Ideal for shared configuration, feature flags, or health status that updates infrequently and readers only care about the most recent state. Zero allocation.

Use **Select** when you need to wait on multiple channels at once -- like a `select {}` in Go or `tokio::select!` in Rust.

### At a Glance

```zig
const volt = @import("volt");

fn example(io: volt.Io) void {
    // Work queue: producers push, consumers pull
    var ch = try volt.channel.bounded(u32, allocator, 256);
    defer ch.deinit();
    _ = ch.trySend(42);                // non-blocking send
    ch.send(io, 42);                   // convenience: suspends until sent
    const val = ch.recv(io);           // convenience: suspends until received
    const try_val = ch.tryRecv();      // non-blocking: .value, .empty, or .closed

    // One-shot result delivery (zero allocation)
    var os = volt.channel.oneshot(Result);
    _ = os.sender.send(result);        // exactly once
    const got = os.receiver.recv(io);  // RecvResult: .value or .closed
    const try_got = os.receiver.tryRecv(); // non-blocking: ?Result
    _ = got;
    _ = try_got;

    // Broadcast events to all subscribers
    var bus = try volt.channel.broadcast(Event, allocator, 64);
    defer bus.deinit();
    var rx = bus.subscribe();
    _ = bus.send(event);               // SendResult: .ok(usize) or .closed
    const msg = rx.recv(io);           // RecvResult: .value, .empty, .lagged, .closed
    _ = rx.tryRecv();                  // non-blocking: .value, .empty, .lagged, .closed
    _ = msg;

    // Latest-value config (zero allocation)
    var cfg = volt.channel.watch(Config, defaults);
    cfg.send(new_config);              // update
    var watch_rx = cfg.subscribe();
    _ = watch_rx.changed(io);          // ChangedResult: .changed or .closed
    const current = watch_rx.borrow(); // always the latest
}
```

All channel types live under `volt.channel`.

## Factory Functions

```zig
const volt = @import("volt");

// Bounded MPMC channel (requires allocator)
var ch = try volt.channel.bounded(u32, allocator, 256);
defer ch.deinit();

// Oneshot (zero allocation)
var os = volt.channel.oneshot(Result);

// Broadcast (requires allocator)
var bc = try volt.channel.broadcast(Event, allocator, 16);
defer bc.deinit();

// Watch (zero allocation)
var wt = volt.channel.watch(Config, default_config);
```

## Resource Management

:::tip
**Rule: if you passed an allocator, you must `deinit()`.** Forgetting `deinit()` on `Channel` or `BroadcastChannel` leaks the ring buffer. See [Common Pitfalls](/guides/common-pitfalls/#forgetting-deinit-on-channel-and-broadcastchannel) for details.
:::

| Type | Allocator | `deinit()` Required |
|------|-----------|-------------------|
| `Channel(T)` | Yes (ring buffer) | Yes |
| `BroadcastChannel(T)` | Yes (ring buffer) | Yes |
| `Oneshot(T)` | No | No |
| `Watch(T)` | No | No |

---

## Channel (Bounded MPMC)

```zig
const Channel = volt.channel.Channel;
```

A bounded multi-producer, multi-consumer channel backed by a Vyukov/crossbeam-style lock-free ring buffer.

### Construction

```zig
var ch = try Channel(u32).init(allocator, 256); // capacity 256
defer ch.deinit();
```

Or via the factory function:

```zig
var ch = try volt.channel.bounded(u32, allocator, 256);
defer ch.deinit();
```

### Send

#### `trySend`

```zig
pub fn trySend(self: *Channel(T), value: T) SendResult
```

Non-blocking send. Returns immediately.

```zig
const SendResult = enum { ok, full, closed };
```

```zig
switch (ch.trySend(42)) {
    .ok => {},         // Value was sent
    .full => {},       // Buffer is full (backpressure)
    .closed => {},     // Channel was closed
}
```

#### `send(io, value)` (Convenience)

```zig
pub fn send(self: *Channel(T), io: volt.Io, value: T) void
```

Convenience method that suspends the current task until the value is sent or the channel closes. Takes the `Io` handle.

```zig
ch.send(io, 42);  // suspends if buffer is full
```

#### `sendFuture(value)` (Future)

```zig
pub fn sendFuture(self: *Channel(T), value: T) SendFuture(T)
```

Returns a Future that resolves when the value is sent (or the channel closes). Use for advanced patterns like composing with other futures.

### Receive

#### `tryRecv`

```zig
pub fn tryRecv(self: *Channel(T)) TryRecvResult(T)
```

Non-blocking receive.

```zig
const TryRecvResult = union(enum) { value: T, empty, closed };
```

```zig
switch (ch.tryRecv()) {
    .value => |v| process(v),
    .empty => {},      // No value available
    .closed => {},     // Channel closed, drained
}
```

#### `recv(io)` (Convenience)

```zig
pub fn recv(self: *Channel(T), io: volt.Io) ?T
```

Convenience method that suspends the current task until a value is available. Returns `null` if the channel is closed and drained.

```zig
if (ch.recv(io)) |value| {
    process(value);
} else {
    // Channel closed
}
```

#### `recvFuture()` (Future)

```zig
pub fn recvFuture(self: *Channel(T)) RecvFuture(T)
```

Returns a Future that resolves with the next value. Use for advanced patterns like composing with other futures or spawning as a separate task.

### Lifecycle

#### `close`

```zig
pub fn close(self: *Channel(T)) void
```

Close the channel. Pending senders get `.closed`, pending receivers can drain remaining values.

#### `deinit`

```zig
pub fn deinit(self: *Channel(T)) void
```

Free the ring buffer. The channel must not be in use.

### Diagnostics

| Method | Returns | Description |
|--------|---------|-------------|
| `len` | `usize` | Approximate number of values in the buffer |
| `isEmpty` | `bool` | Whether the buffer appears empty |
| `isFull` | `bool` | Whether the buffer appears full |
| `isClosed` | `bool` | Whether the channel has been closed |

Note: `capacity` is a struct field (not a method) set at init time. Access it as `ch.capacity`.

### Waiter API

For advanced integration with custom schedulers:

```zig
const SendWaiter = volt.channel.channel_mod.SendWaiter;
const RecvWaiter = volt.channel.channel_mod.RecvWaiter;

var send_waiter = SendWaiter.init();
send_waiter.setWaker(@ptrCast(&ctx), wakeFn);

var recv_waiter = RecvWaiter.init();
recv_waiter.setWaker(@ptrCast(&ctx), wakeFn);
```

Completion check: `waiter.isComplete()` returns `true` when the operation succeeded or the channel closed. Check `waiter.status.load(.acquire)` to distinguish — `WAITER_COMPLETE` (1) means success, `WAITER_CLOSED` (2) means the channel was closed.

### Example: Three-Stage Processing Pipeline

This example demonstrates a data pipeline where each stage reads from one channel
and writes to the next: parse raw bytes, transform into records, then serialize
to output. Channels provide backpressure between stages automatically.

```zig
const volt = @import("volt");
const Channel = volt.channel.Channel;

const RawLine = struct {
    data: []const u8,
    line_number: u32,
};

const ParsedRecord = struct {
    timestamp: i64,
    level: enum { debug, info, warn, err },
    message: []const u8,
};

const OutputEntry = struct {
    formatted: []const u8,
    severity: u8,
};

fn runPipeline(io: volt.Io, allocator: std.mem.Allocator) !void {
    // Stage channels: raw lines -> parsed records -> output entries.
    // Capacity controls how far each stage can run ahead of the next.
    var raw_to_parsed = try Channel(RawLine).init(allocator, 64);
    defer raw_to_parsed.deinit();

    var parsed_to_output = try Channel(ParsedRecord).init(allocator, 32);
    defer parsed_to_output.deinit();

    // Stage 1: Reader -- pushes raw lines using convenience send.
    const reader_f = try io.@"async"(struct {
        fn run(ch_io: volt.Io, ch: *Channel(RawLine)) void {
            const lines = [_][]const u8{
                "2025-01-15T10:00:00Z INFO  Server started on :8080",
                "2025-01-15T10:00:01Z DEBUG Connection accepted from 192.168.1.1",
                "2025-01-15T10:00:02Z WARN  Slow query detected: 1200ms",
            };
            for (lines, 0..) |line, i| {
                ch.send(ch_io, .{
                    .data = line,
                    .line_number = @intCast(i + 1),
                });
            }
            // Signal no more input.
            ch.close();
        }
    }.run, .{ io, &raw_to_parsed });

    // Stage 2: Parser -- reads raw lines, writes parsed records.
    const parser_f = try io.@"async"(struct {
        fn run(
            ch_io: volt.Io,
            input: *Channel(RawLine),
            output: *Channel(ParsedRecord),
        ) void {
            while (input.recv(ch_io)) |raw| {
                output.send(ch_io, .{
                    .timestamp = 1705312800 + @as(i64, raw.line_number),
                    .level = .info,
                    .message = raw.data,
                });
            }
            // Input closed and drained. Propagate shutdown.
            output.close();
        }
    }.run, .{ io, &raw_to_parsed, &parsed_to_output });

    // Stage 3: Formatter -- reads parsed records, produces final output.
    var output_count: usize = 0;
    while (parsed_to_output.recv(io)) |record| {
        _ = record;
        output_count += 1;
    }

    // Wait for pipeline stages to finish.
    _ = reader_f.@"await"(io);
    _ = parser_f.@"await"(io);

    std.debug.print("Pipeline processed {} records\n", .{output_count});
}
```

Key points:
- Each stage is decoupled; it can be replaced or scaled independently.
- Backpressure is automatic: `send(io, value)` suspends the sender when the buffer fills.
- Shutdown propagates stage-by-stage via `close()`.
- `recv(io)` returns `null` when the channel is closed and drained, making `while` loops natural.

### Example: Fan-Out / Fan-In with Multiple Workers

Multiple consumer tasks pull work from a shared input channel and push results
to a shared output channel. The MPMC design means no extra routing is needed.

```zig
const volt = @import("volt");
const Channel = volt.channel.Channel;

const HttpRequest = struct {
    url: []const u8,
    method: enum { get, post },
    request_id: u64,
};

const HttpResponse = struct {
    request_id: u64,
    status: u16,
    body_len: usize,
};

fn fanOutFanIn(io: volt.Io, allocator: std.mem.Allocator) !void {
    var requests = try Channel(HttpRequest).init(allocator, 128);
    defer requests.deinit();

    var responses = try Channel(HttpResponse).init(allocator, 128);
    defer responses.deinit();

    const num_workers = 4;

    // Spawn worker pool. Each worker pulls from the same request channel.
    var worker_futures: [num_workers]volt.Future(void) = undefined;
    for (&worker_futures) |*f| {
        f.* = try io.@"async"(struct {
            fn work(
                w_io: volt.Io,
                req_ch: *Channel(HttpRequest),
                resp_ch: *Channel(HttpResponse),
            ) void {
                while (req_ch.recv(w_io)) |req| {
                    // Simulate handling the request.
                    resp_ch.send(w_io, .{
                        .request_id = req.request_id,
                        .status = 200,
                        .body_len = req.url.len * 10,
                    });
                }
            }
        }.work, .{ io, &requests, &responses });
    }

    // Producer: enqueue requests.
    const urls = [_][]const u8{
        "/api/users",
        "/api/orders",
        "/api/products",
        "/api/inventory",
        "/api/reports",
        "/api/health",
    };
    for (urls, 0..) |url, i| {
        requests.send(io, .{
            .url = url,
            .method = .get,
            .request_id = i,
        });
    }
    // Signal no more work.
    requests.close();

    // Wait for all workers to finish.
    for (&worker_futures) |*f| {
        _ = f.@"await"(io);
    }
    responses.close();

    // Collector: gather all responses.
    var completed: usize = 0;
    while (responses.recv(io)) |resp| {
        std.debug.print("Request {} -> status {}\n", .{ resp.request_id, resp.status });
        completed += 1;
    }
    std.debug.print("Completed {}/{} requests\n", .{ completed, urls.len });
}
```

Key points:
- Work distribution is implicit: whichever worker calls `recv(io)` first gets the next item.
- The result channel collects responses from all workers in arrival order.
- After closing the request channel, workers drain remaining items and exit when `recv(io)` returns `null`.

---

## Oneshot

```zig
const Oneshot = volt.channel.Oneshot;
```

Single-value channel. Exactly one send, exactly one receive. Zero allocation.

### Construction

```zig
var os = Oneshot(u32).init();
// or:
var os = volt.channel.oneshot(u32);
```

The `init()` returns a struct with `.sender` and `.receiver` fields.

### Sender

```zig
pub fn send(self: *Sender, value: T) bool
```

Send a value. Returns `true` if the receiver got it, `false` if receiver was already dropped. Can only be called once.

```zig
_ = os.sender.send(42);
```

### Receiver

#### `tryRecv`

```zig
pub fn tryRecv(self: *Receiver) ?T
```

Non-blocking receive. Returns `null` if no value yet.

#### `recv(io)` (Convenience)

```zig
pub fn recv(self: *Receiver, io: volt.Io) RecvResult
```

```zig
const RecvResult = union(enum) {
    value: T,   // Got the value
    closed,     // Sender was dropped without sending
};
```

Convenience method that suspends the current task until a value arrives or the sender is dropped. Returns a `RecvResult` indicating the outcome.

```zig
switch (os.receiver.recv(io)) {
    .value => |v| {
        // Got the value
        _ = v;
    },
    .closed => {
        // Sender dropped without sending
    },
}
```

#### `recvFuture()` (Future)

```zig
pub fn recvFuture(self: *Receiver) RecvFuture
```

Returns a Future that resolves when a value arrives. Use for advanced patterns like spawning as a task or composing with other futures.

#### `recvWait`

```zig
pub fn recvWait(self: *Receiver, waiter: *RecvWaiter) bool
```

Waiter-based receive. Returns `true` if value available immediately. On wake, check `waiter.value` for the value or `waiter.closed` for sender-dropped.

### RecvWaiter

```zig
const RecvWaiter = Oneshot(T).RecvWaiter;

var waiter = RecvWaiter.init();
waiter.setWaker(@ptrCast(&ctx), wakeFn);

if (!os.receiver.recvWait(&waiter)) {
    // Yield -- will be woken when value arrives
}
// waiter.value.? contains the value
```

### State Machine

```
empty --> value_sent --> (consumed)
  |                         ^
  +--> receiver_waiting ----+
  |
  +--> closed
```

### Example: Request-Response Pattern

A dispatcher sends work items over a shared Channel. Each work item carries
its own Oneshot so the requester can await the specific reply, similar to
RPC call/return semantics.

```zig
const volt = @import("volt");
const Channel = volt.channel.Channel;
const Oneshot = volt.channel.Oneshot;

const DnsQuery = struct {
    hostname: []const u8,
    query_type: enum { a, aaaa, mx, cname },
};

const DnsResult = struct {
    addresses: [4][]const u8,
    address_count: u8,
    ttl_seconds: u32,
};

// A request bundles the query with a Oneshot for the reply.
const DnsRequest = struct {
    query: DnsQuery,
    reply: *Oneshot(DnsResult).Sender,
};

fn dnsClient(io: volt.Io, request_queue: *Channel(DnsRequest)) !void {
    // Client side: submit a query and wait for the reply.
    var reply_channel = Oneshot(DnsResult).init();

    request_queue.send(io, .{
        .query = .{
            .hostname = "api.example.com",
            .query_type = .a,
        },
        .reply = &reply_channel.sender,
    });

    // Suspend until the resolver responds.
    switch (reply_channel.receiver.recv(io)) {
        .value => |result| {
            std.debug.print("Resolved with TTL={}s\n", .{result.ttl_seconds});
        },
        .closed => {
            std.debug.print("Resolver dropped the request\n", .{});
        },
    }
}

fn dnsResolver(io: volt.Io, queue: *Channel(DnsRequest)) void {
    while (queue.recv(io)) |req| {
        // Simulate DNS resolution.
        const result = DnsResult{
            .addresses = .{
                req.query.hostname, // Placeholder
                "",
                "",
                "",
            },
            .address_count = 1,
            .ttl_seconds = 300,
        };
        // Deliver the result directly to the waiting caller.
        _ = req.reply.send(result);
    }
}
```

Key points:
- Each Oneshot is used exactly once. The sender is embedded in the request struct.
- The pattern decouples "who does the work" from "who needs the result."
- If the resolver task exits without calling `send`, `recv(io)` returns `null` -- handle this as a timeout or error.

---

## BroadcastChannel

```zig
const BroadcastChannel = volt.channel.BroadcastChannel;
```

Every receiver gets every message. Ring buffer with configurable capacity.

### Construction

```zig
var bc = try BroadcastChannel(Event).init(allocator, 16);
defer bc.deinit();
// or:
var bc = try volt.channel.broadcast(Event, allocator, 16);
defer bc.deinit();
```

### Sending

```zig
pub fn send(self: *BroadcastChannel(T), value: T) SendResult
```

```zig
const SendResult = union(enum) {
    ok: usize,   // Number of active receivers
    closed,      // Channel was closed
};
```

Send to all receivers. Returns a `SendResult` indicating the number of active receivers (`.ok`) or that the channel was closed (`.closed`). Senders never block.

### Subscribing

```zig
pub fn subscribe(self: *BroadcastChannel(T)) Receiver(T)
```

Create a new receiver. The receiver starts at the current tail position (no historical messages).

### Receiver

#### `tryRecv`

```zig
pub fn tryRecv(self: *Receiver(T)) TryRecvResult(T)
```

```zig
const TryRecvResult = union(enum) {
    value: T,      // Got a message
    empty,         // No new messages
    lagged: u64,   // Missed messages (count of skipped)
    closed,        // Channel closed
};
```

#### `recv(io)` (Convenience)

```zig
pub fn recv(self: *Receiver(T), io: volt.Io) RecvResult(T)
```

```zig
const RecvResult = union(enum) {
    value: T,       // Got a message
    empty,          // No new messages (should not occur with blocking recv)
    lagged: u64,    // Missed messages (count of skipped)
    closed,         // Channel closed
};
```

Convenience method that suspends the current task until a message is available or the channel closes. Returns a `RecvResult` indicating the outcome.

```zig
while (true) {
    switch (rx.recv(io)) {
        .value => |event| handleEvent(event),
        .lagged => |count| log.warn("Missed {} events", .{count}),
        .closed => break,
        .empty => {},
    }
}
```

#### `recvFuture()` (Future)

```zig
pub fn recvFuture(self: *Receiver(T)) RecvFuture(T)
```

Returns a Future that resolves with the next message. Use for advanced patterns.

#### `recvWait`

```zig
pub fn recvWait(self: *Receiver(T), waiter: *RecvWaiter) bool
```

Waiter-based receive.

### Lagging

If a receiver falls behind, it skips to the latest available position. The `lagged` variant tells you how many messages were missed.

```zig
switch (rx.tryRecv()) {
    .value => |event| handleEvent(event),
    .lagged => |count| log.warn("Missed {} events", .{count}),
    .empty => {},
    .closed => break,
}
```

### Example: Typed Event Bus

A single broadcast channel carries a tagged union of event types. Different
subscribers filter for the events they care about -- a log sink only processes
log events, a metrics collector only processes metric events.

```zig
const volt = @import("volt");
const BroadcastChannel = volt.channel.BroadcastChannel;

const LogEntry = struct {
    timestamp: i64,
    level: enum { debug, info, warn, err },
    source: []const u8,
    message: []const u8,
};

const MetricEvent = struct {
    name: []const u8,
    value: f64,
    tags: [4][]const u8,
    tag_count: u8,
};

const HealthCheck = struct {
    service: []const u8,
    healthy: bool,
    latency_ms: u32,
};

// All event types in a single tagged union.
const AppEvent = union(enum) {
    log: LogEntry,
    metric: MetricEvent,
    health: HealthCheck,
};

fn eventBus(io: volt.Io, allocator: std.mem.Allocator) !void {
    // Buffer 64 events. Slow receivers will see .lagged instead of blocking senders.
    var bus = try BroadcastChannel(AppEvent).init(allocator, 64);
    defer bus.deinit();

    // Log sink: only cares about log events.
    var log_rx = bus.subscribe();
    const log_f = try io.@"async"(struct {
        fn run(sink_io: volt.Io, rx: *BroadcastChannel(AppEvent).Receiver) void {
            var log_count: usize = 0;
            while (true) {
                switch (rx.recv(sink_io)) {
                    .value => |event| switch (event) {
                        .log => |entry| {
                            _ = entry;
                            log_count += 1;
                        },
                        // Ignore metric and health events.
                        .metric, .health => {},
                    },
                    .lagged => |count| {
                        _ = count; // Missed some events
                    },
                    .closed => break,
                    .empty => {},
                }
            }
            std.debug.print("Log sink: processed {} log entries\n", .{log_count});
        }
    }.run, .{ io, &log_rx });

    // Metrics collector: only cares about metric events.
    var metrics_rx = bus.subscribe();
    const metrics_f = try io.@"async"(struct {
        fn run(sink_io: volt.Io, rx: *BroadcastChannel(AppEvent).Receiver) void {
            var sum: f64 = 0;
            var count: usize = 0;
            while (true) {
                switch (rx.recv(sink_io)) {
                    .value => |event| switch (event) {
                        .metric => |m| {
                            sum += m.value;
                            count += 1;
                        },
                        .log, .health => {},
                    },
                    .lagged, .empty => {},
                    .closed => break,
                }
            }
            if (count > 0) {
                std.debug.print("Metrics: avg = {d:.2}\n", .{sum / @as(f64, @floatFromInt(count))});
            }
        }
    }.run, .{ io, &metrics_rx });

    // Emit events. Every subscriber sees every event.
    _ = bus.send(.{ .log = .{
        .timestamp = 1705312800,
        .level = .info,
        .source = "http",
        .message = "Request received: GET /api/users",
    } });

    _ = bus.send(.{ .metric = .{
        .name = "http.request.duration_ms",
        .value = 42.5,
        .tags = .{ "method:GET", "path:/api/users", "", "" },
        .tag_count = 2,
    } });

    _ = bus.send(.{ .health = .{
        .service = "database",
        .healthy = true,
        .latency_ms = 3,
    } });

    bus.close();
    _ = log_f.@"await"(io);
    _ = metrics_f.@"await"(io);
}
```

Key points:
- Senders never block. If a receiver is slow, it gets a `.lagged` result with the count of missed events.
- Subscribers filter by matching on the union tag, ignoring irrelevant events.
- The buffer capacity (64 above) determines how far a receiver can fall behind before lagging occurs. Tune this based on your throughput needs.
- The convenience `recv(io)` method handles lagging transparently by skipping to the latest position.

---

## Watch

```zig
const Watch = volt.channel.Watch;
```

Single value with change notification. Receivers see only the latest value.

### Construction

```zig
var watch = Watch(Config).init(default_config);
// or:
var watch = volt.channel.watch(Config, default_config);
```

### Sending

```zig
pub fn send(self: *Watch(T), value: T) void
```

Update the value. Wakes all receivers that called `changed` or `changedWait`.

### Subscribing

```zig
pub fn subscribe(self: *Watch(T)) Receiver(T)
```

Create a receiver at the current version.

### Receiver

| Method | Returns | Description |
|--------|---------|-------------|
| `borrow` | `*const T` | Read-only pointer to the current value |
| `get` | `T` | Copy of the current value (mutex-protected) |
| `getAndUpdate` | `T` | Copy of the current value, marks it as seen |
| `hasChanged` | `bool` | Whether the value changed since last `markSeen` |
| `markSeen` | `void` | Mark the current version as seen |
| `changed` | `ChangedResult` (takes `io`) | Convenience: suspend until the value changes |
| `changedFuture` | `ChangedFuture` | Returns a Future for the next change |
| `changedWait` | `bool` (waiter-based) | Wait for the next change |
| `isClosed` | `bool` | Whether the sender has closed |

#### `changed(io)` (Convenience)

```zig
pub fn changed(self: *Receiver(T), io: volt.Io) ChangedResult
```

```zig
const ChangedResult = union(enum) {
    changed,   // Value has changed
    closed,    // Sender was closed
};
```

Convenience method that suspends the current task until the value changes or the sender closes. Returns a `ChangedResult` indicating the outcome.

```zig
fn watchForConfigChanges(io: volt.Io, rx: *Watch(Config).Receiver) void {
    while (true) {
        switch (rx.changed(io)) {
            .changed => {
                const new_config = rx.getAndUpdate();
                applyConfig(new_config);
            },
            .closed => break,
        }
    }
}
```

#### `changedFuture()` (Future)

```zig
pub fn changedFuture(self: *Receiver(T)) ChangedFuture
```

Returns a Future that resolves when the value changes. Use for advanced patterns.

```zig
var rx = watch.subscribe();

// Polling pattern (non-blocking)
if (rx.hasChanged()) {
    const config = rx.borrow();
    applyConfig(config);
    rx.markSeen();
}
```

### Example: Configuration Reload with Version Tracking

A Watch channel holds the active configuration. A reloader task updates it
periodically. Multiple worker tasks observe the latest config without polling
on every request. The version counter lets you log exactly which config
revision is active.

```zig
const volt = @import("volt");
const Watch = volt.channel.Watch;

const AppConfig = struct {
    max_connections: u32,
    request_timeout_ms: u64,
    log_level: enum { debug, info, warn, err },
    feature_flags: packed struct {
        enable_cache: bool = true,
        enable_compression: bool = false,
        enable_rate_limit: bool = true,
        _padding: u5 = 0,
    },
};

const default_config = AppConfig{
    .max_connections = 1000,
    .request_timeout_ms = 30_000,
    .log_level = .info,
    .feature_flags = .{},
};

fn configReloadDemo(io: volt.Io) !void {
    var config_watch = Watch(AppConfig).init(default_config);

    // Worker task: adapts behavior based on the latest config.
    var worker_rx = config_watch.subscribe();
    const worker_f = try io.@"async"(struct {
        fn run(w_io: volt.Io, rx: *Watch(AppConfig).Receiver) void {
            var config_version: u64 = 0;

            // Read initial config.
            var current = rx.getAndUpdate();
            config_version += 1;
            std.debug.print(
                "Worker: initial config v{} (max_conn={}, timeout={}ms)\n",
                .{ config_version, current.max_connections, current.request_timeout_ms },
            );

            // Watch for config changes.
            while (true) {
                switch (rx.changed(w_io)) {
                    .changed => {},
                    .closed => break,
                }
                current = rx.getAndUpdate();
                config_version += 1;
                std.debug.print(
                    "Worker: config updated to v{} (max_conn={}, timeout={}ms)\n",
                    .{ config_version, current.max_connections, current.request_timeout_ms },
                );
            }
        }
    }.run, .{ io, &worker_rx });

    // Simulate config reloads from an external source (file, API, etc.).
    std.Thread.sleep(1_000_000); // 1ms

    config_watch.send(.{
        .max_connections = 2000,
        .request_timeout_ms = 15_000,
        .log_level = .warn,
        .feature_flags = .{ .enable_compression = true },
    });

    std.Thread.sleep(1_000_000); // 1ms

    // sendModify lets you change a single field without replacing the whole struct.
    config_watch.sendModify(struct {
        fn modify(cfg: *AppConfig) void {
            cfg.max_connections = 5000;
        }
    }.modify);

    // Shutdown.
    config_watch.close();
    _ = worker_f.@"await"(io);
}
```

Key points:
- `Watch` only keeps the latest value. Intermediate updates are collapsed, so receivers always see the most recent config.
- `sendModify` is useful when you only need to change one field without reconstructing the whole struct.
- `subscribeNoInitial` skips the initial value if you only care about future changes.
- The convenience `changed(io)` suspends efficiently until the value actually changes, avoiding busy-wait loops.

---

## Select

```zig
const Selector = volt.channel.Selector;
```

Wait on multiple channel operations simultaneously. The first operation to complete wins.

### Construction

```zig
// Default max branches (from SelectContext.MAX_SELECT_BRANCHES)
var s = volt.channel.selector();

// Or specify max branches explicitly
var s = volt.channel.Selector(4).init();
```

### Adding Branches

```zig
pub fn addRecv(self: *Self, comptime T: type, channel: *Channel(T)) usize
```

Returns a branch index (0-based) that identifies which branch fired in the result.

### Selecting

```zig
pub fn trySelect(self: *Self) ?SelectResult                                // Non-blocking
pub fn selectBlocking(self: *Self) SelectResult                            // Blocks until ready
pub fn selectBlockingWithTimeout(self: *Self, timeout_ns: ?u64) SelectResult  // With timeout
```

```zig
const SelectResult = struct {
    branch: usize,   // Which branch completed (0-indexed)
    closed: bool,    // Whether the channel was closed
};
```

### Convenience Functions

```zig
// Select between two channels
const result = volt.channel.select_mod.select2Recv(u32, &ch1, []const u8, &ch2);

// Select between three channels
const result = volt.channel.select_mod.select3Recv(u32, &ch1, u8, &ch2, bool, &ch3);
```

### Resetting

```zig
pub fn reset(self: *Self) void
```

Clear all branches so the selector can be reused.

Select uses the `CancelToken` and `SelectContext` types internally to coordinate cancellation across multiple channel waiters.

### Example: Multiplexed Receiver with Timeout and Shutdown

A server loop that waits on three sources simultaneously: a work channel for
incoming tasks, a timer channel for periodic housekeeping, and a shutdown signal.
Select returns whichever fires first.

```zig
const volt = @import("volt");
const Channel = volt.channel.Channel;
const Selector = volt.channel.Selector;

const WorkItem = struct {
    id: u64,
    payload: []const u8,
    priority: enum { low, normal, high },
};

// Use a sentinel type for timer ticks and shutdown signals.
const TimerTick = struct { tick_number: u64 };
const ShutdownSignal = struct { reason: []const u8 };

fn multiplexedServer(io: volt.Io, allocator: std.mem.Allocator) !void {
    var work_ch = try Channel(WorkItem).init(allocator, 256);
    defer work_ch.deinit();

    var timer_ch = try Channel(TimerTick).init(allocator, 8);
    defer timer_ch.deinit();

    var shutdown_ch = try Channel(ShutdownSignal).init(allocator, 1);
    defer shutdown_ch.deinit();

    // Timer task: emits periodic ticks for housekeeping.
    const timer_f = try io.@"async"(struct {
        fn run(t_io: volt.Io, ch: *Channel(TimerTick)) void {
            var tick: u64 = 0;
            while (true) {
                std.Thread.sleep(10_000_000); // 10ms
                tick += 1;
                if (ch.trySend(.{ .tick_number = tick }) == .closed) return;
                _ = t_io;
            }
        }
    }.run, .{ io, &timer_ch });

    // Producer task: enqueues work items.
    const producer_f = try io.@"async"(struct {
        fn run(p_io: volt.Io, ch: *Channel(WorkItem)) void {
            const items = [_]WorkItem{
                .{ .id = 1, .payload = "process_order", .priority = .high },
                .{ .id = 2, .payload = "send_email", .priority = .normal },
                .{ .id = 3, .payload = "generate_report", .priority = .low },
            };
            for (items) |item| {
                std.Thread.sleep(5_000_000); // 5ms between items
                ch.send(p_io, item);
            }
        }
    }.run, .{ io, &work_ch });

    // Main select loop.
    var items_processed: usize = 0;
    var ticks_seen: usize = 0;
    var running = true;

    while (running) {
        var sel = Selector(3).init();
        const work_branch = sel.addRecv(WorkItem, &work_ch);
        const timer_branch = sel.addRecv(TimerTick, &timer_ch);
        const shutdown_branch = sel.addRecv(ShutdownSignal, &shutdown_ch);

        // Wait up to 50ms. If nothing fires, do a maintenance pass anyway.
        const result = sel.selectBlockingWithTimeout(50_000_000);

        if (result.closed) {
            // One of the channels closed unexpectedly.
            running = false;
        } else if (result.branch == work_branch) {
            switch (work_ch.tryRecv()) {
                .value => |item| {
                    std.debug.print("Processing work item #{} ({})\n", .{ item.id, @tagName(item.priority) });
                    items_processed += 1;
                },
                else => {},
            }
        } else if (result.branch == timer_branch) {
            switch (timer_ch.tryRecv()) {
                .value => |tick| {
                    _ = tick;
                    ticks_seen += 1;
                    // Periodic housekeeping: flush buffers, check health, etc.
                },
                else => {},
            }
        } else if (result.branch == shutdown_branch) {
            switch (shutdown_ch.tryRecv()) {
                .value => |sig| {
                    std.debug.print("Shutdown requested: {s}\n", .{sig.reason});
                    running = false;
                },
                else => {},
            }
        }
    }

    // Clean shutdown: close all channels so background tasks exit.
    work_ch.close();
    timer_ch.close();
    shutdown_ch.close();

    _ = timer_f.@"await"(io);
    _ = producer_f.@"await"(io);

    std.debug.print("Server exited: {} items processed, {} timer ticks\n", .{ items_processed, ticks_seen });
}
```

Key points:
- The Selector is re-created each iteration (it is stack-allocated and cheap to initialize). Call `reset()` if you prefer to reuse one across iterations.
- `selectBlockingWithTimeout` prevents the loop from hanging if no channels have data; the timeout acts as a fallback heartbeat.
- After Select identifies the ready branch, call `tryRecv` on the matching channel to retrieve the value. Select tells you **which** channel is ready; you still consume the value yourself.

---

## Error Handling

Channel operations use tagged unions rather than Zig errors for expected conditions (full, empty, closed). This avoids error union overhead on the fast path.

| Condition | trySend | tryRecv |
|-----------|---------|---------|
| Success | `.ok` | `.value` |
| Backpressure | `.full` | `.empty` |
| Closed | `.closed` | `.closed` |

Only initialization (`Channel.init`, `BroadcastChannel.init`) returns Zig errors (allocation failure).

The convenience methods (`send(io, value)`, `recv(io)`) handle these conditions internally: `send` suspends until space is available, `recv` returns `null` on close.

---

## Choosing the Right Channel

| Scenario | Channel Type | Why |
|----------|-------------|-----|
| Work queue with backpressure (1 or N producers, 1 or N consumers) | `Channel` | Bounded MPMC. Lock-free ring buffer keeps throughput high under contention. Buffer size controls memory and backpressure. |
| Return a single result from a spawned task | `Oneshot` | Zero allocation, exactly one send/recv. Ideal for request-response or task completion. |
| Broadcast events to all subscribers | `BroadcastChannel` | Every receiver gets every message. Slow receivers lag rather than block the sender. |
| Shared configuration or latest-value state | `Watch` | Only the most recent value is kept. Zero allocation. Receivers check at their own pace. |
| Wait on multiple channels at once | `Select` | Returns when the first channel is ready. Use for multiplexing work, timers, and signals in a single loop. |

**Quick decision flowchart:**

1. Do you need to send exactly one value? Use **Oneshot**.
2. Do all receivers need every message? Use **BroadcastChannel**.
3. Do receivers only need the latest value? Use **Watch**.
4. Otherwise, use **Channel** (bounded MPMC) for general-purpose message passing.
5. Need to wait on more than one of the above? Wrap them with **Select**.

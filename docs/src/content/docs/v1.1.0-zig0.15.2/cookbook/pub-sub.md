---
title: Pub/Sub System
description: Publish-subscribe messaging with BroadcastChannel, topic-based routing, subscriber management, and backpressure handling.
---

:::tip[What you'll learn]
- Event bus pattern with a central registry
- BroadcastChannel for fan-out delivery
- Topic routing with typed events using tagged unions
- Subscriber lifecycle (subscribe, receive, unsubscribe)
- Backpressure strategies for slow consumers
:::

This recipe builds a typed event bus where publishers emit structured events and subscribers receive only the event kinds they care about. It combines `BroadcastChannel` for fan-out with a topic registry and tagged unions for type-safe routing.

## Architecture

```
Publishers                    EventBus                      Subscribers
                        ┌───────────────────┐
  emit(.log, entry) ──> │                   │ ──> log_sub (filters .log)
                        │  BroadcastChannel │
  emit(.metric, m)  ──> │     (Event)       │ ──> metric_sub (filters .metric)
                        │                   │
  emit(.alert, a)   ──> │  single channel,  │ ──> ops_sub (filters .alert, .log)
                        │  typed events     │
                        └───────────────────┘
```

All events flow through a single `BroadcastChannel(Event)`. Subscribers receive every event and filter locally by variant, which keeps the bus simple and avoids per-topic channel proliferation.

## Defining Typed Events

Use a tagged union so every event carries its own type discriminator. This gives you compile-time exhaustiveness checking when handling events.

```zig
const std = @import("std");
const volt = @import("volt");

/// Severity level for log entries.
pub const Severity = enum { debug, info, warn, err };

pub const LogEntry = struct {
    severity: Severity,
    source: [64]u8,
    source_len: u8,
    message: [256]u8,
    message_len: u16,
    timestamp: i128,

    pub fn create(severity: Severity, source: []const u8, message: []const u8) LogEntry {
        var entry: LogEntry = undefined;
        entry.severity = severity;
        const slen = @min(source.len, 64);
        @memcpy(entry.source[0..slen], source[0..slen]);
        entry.source_len = @intCast(slen);
        const mlen = @min(message.len, 256);
        @memcpy(entry.message[0..mlen], message[0..mlen]);
        entry.message_len = @intCast(mlen);
        entry.timestamp = std.time.nanoTimestamp();
        return entry;
    }

    pub fn sourceSlice(self: *const LogEntry) []const u8 {
        return self.source[0..self.source_len];
    }

    pub fn messageSlice(self: *const LogEntry) []const u8 {
        return self.message[0..self.message_len];
    }
};

pub const MetricSample = struct {
    name: [64]u8,
    name_len: u8,
    value: f64,
    timestamp: i128,

    pub fn create(name: []const u8, value: f64) MetricSample {
        var sample: MetricSample = undefined;
        const nlen = @min(name.len, 64);
        @memcpy(sample.name[0..nlen], name[0..nlen]);
        sample.name_len = @intCast(nlen);
        sample.value = value;
        sample.timestamp = std.time.nanoTimestamp();
        return sample;
    }

    pub fn nameSlice(self: *const MetricSample) []const u8 {
        return self.name[0..self.name_len];
    }
};

pub const Alert = struct {
    level: enum { warning, critical },
    message: [256]u8,
    message_len: u16,
    timestamp: i128,

    pub fn create(level: @TypeOf(@as(Alert, undefined).level), message: []const u8) Alert {
        var alert: Alert = undefined;
        alert.level = level;
        const mlen = @min(message.len, 256);
        @memcpy(alert.message[0..mlen], message[0..mlen]);
        alert.message_len = @intCast(mlen);
        alert.timestamp = std.time.nanoTimestamp();
        return alert;
    }

    pub fn messageSlice(self: *const Alert) []const u8 {
        return self.message[0..self.message_len];
    }
};

/// Tagged union of all event types in the system.
/// Add new variants here as the system grows.
pub const Event = union(enum) {
    log: LogEntry,
    metric: MetricSample,
    alert: Alert,
};
```

The tagged union is the key design choice. Every event is self-describing -- subscribers can switch on the active variant without any string matching or runtime type checks.

## The Event Bus

The bus wraps a single `BroadcastChannel(Event)`. All publishers write into it and all subscribers read from it.

```zig
pub const EventBus = struct {
    const BC = volt.channel.BroadcastChannel(Event);

    channel: *BC,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !EventBus {
        const bc = try allocator.create(BC);
        bc.* = try BC.init(allocator, capacity);
        return .{
            .channel = bc,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EventBus) void {
        self.channel.deinit();
        self.allocator.destroy(self.channel);
    }

    /// Emit an event to all subscribers. Returns the number of active
    /// subscribers who will see the event.
    pub fn emit(self: *EventBus, event: Event) usize {
        return self.channel.send(event);
    }

    /// Subscribe to the event stream. The returned receiver yields every
    /// event; use a typed subscriber wrapper to filter by variant.
    pub fn subscribe(self: *EventBus) BC.Receiver {
        return self.channel.subscribe();
    }
};
```

## Typed Subscribers

Raw receivers see every event. Wrap them to filter for specific variants so each subscriber only processes the events it cares about.

```zig
/// A subscriber that only processes log events.
pub const LogSubscriber = struct {
    rx: EventBus.BC.Receiver,

    pub fn init(bus: *EventBus) LogSubscriber {
        return .{ .rx = bus.subscribe() };
    }

    /// Poll for the next log entry, skipping metrics and alerts.
    pub fn poll(self: *LogSubscriber) ?LogEntry {
        while (true) {
            switch (self.rx.tryRecv()) {
                .value => |event| switch (event) {
                    .log => |entry| return entry,
                    .metric, .alert => continue, // skip non-log events
                },
                .lagged => continue, // catch up
                .empty => return null,
                .closed => return null,
            }
        }
    }
};

/// A subscriber that only processes metric samples.
pub const MetricSubscriber = struct {
    rx: EventBus.BC.Receiver,

    pub fn init(bus: *EventBus) MetricSubscriber {
        return .{ .rx = bus.subscribe() };
    }

    pub fn poll(self: *MetricSubscriber) ?MetricSample {
        while (true) {
            switch (self.rx.tryRecv()) {
                .value => |event| switch (event) {
                    .metric => |sample| return sample,
                    .log, .alert => continue,
                },
                .lagged => continue,
                .empty => return null,
                .closed => return null,
            }
        }
    }
};

/// A subscriber that receives alerts and high-severity logs.
/// Demonstrates filtering across multiple event variants.
pub const OpsSubscriber = struct {
    rx: EventBus.BC.Receiver,

    pub fn init(bus: *EventBus) OpsSubscriber {
        return .{ .rx = bus.subscribe() };
    }

    pub const OpsEvent = union(enum) {
        log: LogEntry,
        alert: Alert,
    };

    pub fn poll(self: *OpsSubscriber) ?OpsEvent {
        while (true) {
            switch (self.rx.tryRecv()) {
                .value => |event| switch (event) {
                    .alert => |a| return .{ .alert = a },
                    .log => |entry| {
                        // Only surface warnings and errors to ops.
                        if (entry.severity == .warn or entry.severity == .err) {
                            return .{ .log = entry };
                        }
                        continue;
                    },
                    .metric => continue,
                },
                .lagged => continue,
                .empty => return null,
                .closed => return null,
            }
        }
    }
};
```

## Putting It All Together

```zig
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create the bus with a 64-slot ring buffer.
    var bus = try EventBus.init(allocator, 64);
    defer bus.deinit();

    // Create typed subscribers before publishing.
    var log_sub = LogSubscriber.init(&bus);
    var metric_sub = MetricSubscriber.init(&bus);
    var ops_sub = OpsSubscriber.init(&bus);

    // --- Publish a mix of events ---

    _ = bus.emit(.{ .log = LogEntry.create(.info, "http", "GET /index.html 200 12ms") });
    _ = bus.emit(.{ .metric = MetricSample.create("request_latency_ms", 12.0) });
    _ = bus.emit(.{ .log = LogEntry.create(.err, "db", "connection pool exhausted") });
    _ = bus.emit(.{ .alert = Alert.create(.critical, "Database connection pool exhausted") });
    _ = bus.emit(.{ .metric = MetricSample.create("active_connections", 0.0) });
    _ = bus.emit(.{ .log = LogEntry.create(.info, "http", "GET /health 200 1ms") });

    // --- Each subscriber sees only what it cares about ---

    std.debug.print("=== Log Subscriber ===\n", .{});
    while (log_sub.poll()) |entry| {
        std.debug.print("[{s}] {s}: {s}\n", .{
            @tagName(entry.severity),
            entry.sourceSlice(),
            entry.messageSlice(),
        });
    }

    std.debug.print("\n=== Metric Subscriber ===\n", .{});
    while (metric_sub.poll()) |sample| {
        std.debug.print("{s} = {d:.1}\n", .{
            sample.nameSlice(),
            sample.value,
        });
    }

    std.debug.print("\n=== Ops Subscriber (alerts + error logs) ===\n", .{});
    while (ops_sub.poll()) |ops_event| {
        switch (ops_event) {
            .alert => |a| std.debug.print("ALERT [{s}]: {s}\n", .{
                @tagName(a.level),
                a.messageSlice(),
            }),
            .log => |entry| std.debug.print("LOG [{s}] {s}: {s}\n", .{
                @tagName(entry.severity),
                entry.sourceSlice(),
                entry.messageSlice(),
            }),
        }
    }
}
```

Expected output:

```
=== Log Subscriber ===
[info] http: GET /index.html 200 12ms
[err] db: connection pool exhausted
[info] http: GET /health 200 1ms

=== Metric Subscriber ===
request_latency_ms = 12.0
active_connections = 0.0

=== Ops Subscriber (alerts + error logs) ===
LOG [err] db: connection pool exhausted
ALERT [critical]: Database connection pool exhausted
```

The log subscriber saw all three log entries. The metric subscriber saw both samples. The ops subscriber saw only the error-level log and the critical alert -- the two info-level logs were filtered out.

## Handling Backpressure

When subscribers fall behind, the broadcast ring buffer overwrites old messages. There are several strategies to handle this.

**Strategy 1: Accept and log lag (default)**

```zig
switch (rx.tryRecv()) {
    .lagged => |n| log.warn("subscriber lagged by {} msgs", .{n}),
    .value => |event| process(event),
    .empty => {},
    .closed => return,
}
```

**Strategy 2: Increase buffer capacity for bursty workloads**

```zig
// If you know alert processing is slow and bursts are common,
// allocate a larger ring buffer up front.
var bus = try EventBus.init(allocator, 1024);
```

**Strategy 3: Sampling -- skip to latest**

If the subscriber only cares about the most recent value (common for metric dashboards), drain to the newest:

```zig
var latest: ?MetricSample = null;
while (true) {
    switch (rx.tryRecv()) {
        .value => |event| switch (event) {
            .metric => |sample| latest = sample,
            else => {},
        },
        .lagged => {},
        .empty => break,
        .closed => return,
    }
}
if (latest) |sample| updateDashboard(sample);
```

**Strategy 4: Dedicated overflow channel**

Route lagged events to a separate channel for retry or audit:

```zig
switch (rx.tryRecv()) {
    .lagged => |n| {
        log.warn("subscriber lagged by {} messages, check overflow queue", .{n});
        overflow_counter.add(n);
    },
    .value => |event| process(event),
    .empty => {},
    .closed => return,
}
```

## Try It Yourself

Here are three extensions to build on this pattern:

**1. Predicate-based event filtering**

Instead of hard-coding which variants a subscriber accepts, pass a filter function:

```zig
pub fn FilteredSubscriber(comptime predicate: fn (Event) bool) type {
    return struct {
        rx: EventBus.BC.Receiver,

        pub fn init(bus: *EventBus) @This() {
            return .{ .rx = bus.subscribe() };
        }

        pub fn poll(self: *@This()) ?Event {
            while (true) {
                switch (self.rx.tryRecv()) {
                    .value => |event| {
                        if (predicate(event)) return event;
                        continue;
                    },
                    .lagged => continue,
                    .empty => return null,
                    .closed => return null,
                }
            }
        }
    };
}

// Usage: subscribe only to critical alerts.
const CriticalAlertSub = FilteredSubscriber(struct {
    fn f(event: Event) bool {
        return switch (event) {
            .alert => |a| a.level == .critical,
            else => false,
        };
    }
}.f);
```

**2. Dead letter queue for unconsumed events**

Track which events were not consumed by any subscriber. One approach: wrap `emit` to compare the subscriber count returned by `send` against an expected minimum, and route orphaned events to a fallback channel:

```zig
pub fn emitTracked(self: *EventBus, event: Event, dlq: *DeadLetterQueue) void {
    const receivers = self.channel.send(event);
    if (receivers == 0) {
        dlq.push(event);
    }
}

pub const DeadLetterQueue = struct {
    events: std.ArrayList(Event),

    pub fn init(allocator: std.mem.Allocator) DeadLetterQueue {
        return .{ .events = std.ArrayList(Event).init(allocator) };
    }

    pub fn deinit(self: *DeadLetterQueue) void {
        self.events.deinit();
    }

    pub fn push(self: *DeadLetterQueue, event: Event) void {
        self.events.append(event) catch {};
    }

    pub fn drain(self: *DeadLetterQueue) []const Event {
        defer self.events.clearRetainingCapacity();
        return self.events.items;
    }
};
```

**3. Subscriber count tracking**

Expose the number of active subscribers so monitoring tools can detect when a critical consumer drops off:

```zig
pub const MonitoredBus = struct {
    bus: EventBus,
    subscriber_count: std.atomic.Value(u32),

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !MonitoredBus {
        return .{
            .bus = try EventBus.init(allocator, capacity),
            .subscriber_count = std.atomic.Value(u32).init(0),
        };
    }

    pub fn subscribe(self: *MonitoredBus) EventBus.BC.Receiver {
        _ = self.subscriber_count.fetchAdd(1, .monotonic);
        return self.bus.subscribe();
    }

    pub fn unsubscribe(self: *MonitoredBus) void {
        _ = self.subscriber_count.fetchSub(1, .monotonic);
    }

    pub fn activeSubscribers(self: *const MonitoredBus) u32 {
        return self.subscriber_count.load(.monotonic);
    }
};
```

Combine all three -- predicate filtering, dead letter queue, and subscriber tracking -- to build a production-grade event bus with full observability.

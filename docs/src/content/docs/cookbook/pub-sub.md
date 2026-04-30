---
title: Pub/Sub Fan-Out
description: Broadcast(T) for events that should reach every subscriber, with .lagged(N) for slow consumers.
---

The "one publisher, many subscribers, every subscriber sees every
event" pattern. `volt.channel.Broadcast(T)` is the right tool.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Event = struct {
    kind: enum { user_joined, user_left, message },
    data: []const u8,
};

fn publisher(b: *volt.channel.Broadcast(Event)) !void {
    const events = [_]Event{
        .{ .kind = .user_joined, .data = "alice" },
        .{ .kind = .message, .data = "hello world" },
        .{ .kind = .user_left, .data = "alice" },
    };
    for (events) |e| {
        try b.send(e);
        try volt.sleep(volt.Duration.fromMillis(10));
    }
    b.close();
}

fn subscriber(name: []const u8, rx: *volt.channel.Broadcast(Event).Receiver) !void {
    while (true) {
        switch (try rx.recv()) {
            .value => |e| std.debug.print("[{s}] event: {s} → {s}\n", .{
                name, @tagName(e.kind), e.data,
            }),
            .lagged => |n| std.debug.print("[{s}] LAGGED, dropped {d}\n", .{ name, n }),
            .closed => return,
        }
    }
}
```

## Wiring it up with a scope

```zig
fn root() !void {
    const alloc = volt.currentRuntime().?.allocator;
    var b = try volt.channel.Broadcast(Event).init(alloc, 64);
    defer b.deinit();

    var rx_a = b.subscribe();
    var rx_b = b.subscribe();
    var rx_c = b.subscribe();

    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            const broker = scope_arg.?;
            try s.spawn(subscriber, .{ "A", broker.rx_a });
            try s.spawn(subscriber, .{ "B", broker.rx_b });
            try s.spawn(subscriber, .{ "C", broker.rx_c });
            try s.spawn(publisher, .{broker.b});
        }
    }.body);
}
```

`b.subscribe()` returns a `Receiver` synced to the current tail —
new subscribers see future events, never history. Each subscriber
runs at its own pace; slow ones get `.lagged(N)` instead of
backpressuring the publisher.

## Capacity and lag

The capacity argument is the maximum lag a slow subscriber can
absorb before getting `.lagged(N)`. If you set capacity = 64 and
a subscriber falls 80 messages behind, its next `recv()` returns
`.lagged(80 - 64) = .lagged(16)` and the cursor jumps to the
oldest still-available message (16 messages dropped).

Bigger capacity = more history kept per subscriber = more memory.
Pick by what you want to happen when consumers fall behind:

- **Capacity = 1**: never queue; slow subscribers see almost every
  message lagged.
- **Capacity = 100**: tolerate small bursts.
- **Capacity = 10000**: tolerate large bursts, accept the memory.

## When to use Broadcast vs Watch vs Channel

- Every subscriber should see every event, history-aware:
  **Broadcast**.
- Every subscriber wants only the latest value, history-oblivious:
  **Watch**.
- Single consumer (or work-distribution among multiple consumers):
  **Channel**.

If you find yourself wanting "every subscriber sees every event,
with backpressure to producers when any subscriber is slow" —
that's a per-subscriber `Channel(T)` and a publisher that loops
sending to all of them. Volt doesn't ship a "lossless broadcast
with global backpressure" because the right tradeoff depends on
your application (do you queue indefinitely? drop the slowest
consumer? something else?).

## Variant: filter at the subscriber

The subscriber decides what's interesting:

```zig
fn messageOnlySubscriber(rx: *volt.channel.Broadcast(Event).Receiver) !void {
    while (true) {
        switch (try rx.recv()) {
            .value => |e| if (e.kind == .message) handle(e),
            .lagged => |n| std.log.warn("missed {d} events", .{n}),
            .closed => return,
        }
    }
}
```

Filtering in the subscriber is fine — every subscriber gets a copy.
Filtering in the publisher would either need multiple Broadcasts
(per-topic) or a richer event type with subscriber metadata.

## Variant: per-topic broadcasts

For pub/sub by topic, hold a hashmap of `Broadcast(T)`:

```zig
var topics = std.StringHashMap(*volt.channel.Broadcast(Event)).init(alloc);

fn publish(self: *Self, topic: []const u8, e: Event) !void {
    if (self.topics.get(topic)) |b| try b.send(e);
}

fn subscribe(self: *Self, topic: []const u8) !*volt.channel.Broadcast(Event).Receiver {
    const entry = try self.topics.getOrPut(topic);
    if (!entry.found_existing) {
        entry.value_ptr.* = try alloc.create(volt.channel.Broadcast(Event));
        entry.value_ptr.*.* = try volt.channel.Broadcast(Event).init(alloc, 128);
    }
    var rx = entry.value_ptr.*.subscribe();
    // (subscribe returns Receiver by value; you'll likely heap-alloc it
    // for caller ownership or thread it through scope-spawned coros)
    return &rx;
}
```

Wrap with a Mutex around the map.

## Closing semantics

`b.close()` wakes every parked `recv()` with `.closed`. Subsequent
`recv()` calls also return `.closed` immediately. Once closed, the
Broadcast cannot be reopened — create a fresh one if you need
another round.

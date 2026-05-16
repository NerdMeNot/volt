---
title: Pub/Sub Fan-Out
description: Broadcast(T, cap) for events every subscriber should see. error.Lagged signals slow consumers; producer never blocks.
---

The "one publisher, many subscribers, every subscriber sees every
event (until they fall behind)" pattern. `volt.Broadcast(T, cap)`
is the right tool.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Event = struct {
    kind: enum { user_joined, user_left, message },
    data: []const u8,
};

fn publisher(b: *volt.Broadcast(Event, 64)) void {
    const events = [_]Event{
        .{ .kind = .user_joined, .data = "alice" },
        .{ .kind = .message,     .data = "hello world" },
        .{ .kind = .user_left,   .data = "alice" },
    };
    for (events) |e| {
        b.send(e);
        volt.sleep(10 * std.time.ns_per_ms);
    }
    b.close();
}

const SubscriberCtx = struct {
    name: []const u8,
    rx: volt.Broadcast(Event, 64).Receiver,
};

fn subscriber(ctx: *SubscriberCtx) void {
    while (true) {
        const e = ctx.rx.recv() catch |err| switch (err) {
            error.Closed => return,
            error.Lagged => {
                std.debug.print("[{s}] LAGGED\n", .{ctx.name});
                continue;
            },
        };
        std.debug.print("[{s}] event: {s} -> {s}\n", .{
            ctx.name, @tagName(e.kind), e.data,
        });
    }
}

fn root() !void {
    var b = volt.Broadcast(Event, 64).init();
    defer b.deinit();

    var ctx_a = SubscriberCtx{ .name = "A", .rx = b.receiver() };
    var ctx_b = SubscriberCtx{ .name = "B", .rx = b.receiver() };
    var ctx_c = SubscriberCtx{ .name = "C", .rx = b.receiver() };

    const s_a = try volt.spawn(subscriber, .{&ctx_a});
    const s_b = try volt.spawn(subscriber, .{&ctx_b});
    const s_c = try volt.spawn(subscriber, .{&ctx_c});

    publisher(&b);

    // Publisher already called close(); subscribers will exit on error.Closed.
    s_a.join();
    s_b.join();
    s_c.join();
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

## Why Broadcast (not Watch / not Mpmc)

| Channel | When |
|---|---|
| `Broadcast` | Every subscriber sees every event (with `error.Lagged` for slow consumers); producer never blocks. |
| `Watch` | Every subscriber wants only the latest value; intermediate values silently dropped. |
| `Mpmc` | Single consumer (or work-distribution among multiple consumers — each event goes to *one* consumer). |

For pub/sub, you want fan-out (every subscriber sees every
event), so Broadcast is the answer. The capacity sets the lag
tolerance.

## Capacity and lag

The cap is the maximum lag a slow subscriber can absorb before
getting `error.Lagged`. If you set `cap = 64` and a subscriber
falls 80 messages behind, its next `recv()` returns `error.Lagged`
and the cursor jumps to the oldest still-available message
(16 messages dropped).

Bigger capacity = more history kept per subscriber = more memory
for the ring buffer. Pick by what you want to happen when
consumers fall behind:

- **Capacity = 1**: never queue; slow subscribers see almost every
  message lagged.
- **Capacity = 100**: tolerate small bursts.
- **Capacity = 10000**: tolerate large bursts, accept the memory.

Capacity is comptime — `Broadcast(Event, 64)` and
`Broadcast(Event, 128)` are different types.

## When to use Broadcast vs Watch vs Mpmc

- Every subscriber should see every event, history-aware:
  **Broadcast**.
- Every subscriber wants only the latest value, history-oblivious:
  **Watch**.
- Single consumer (or work-distribution among multiple consumers):
  **Mpmc**.

If you find yourself wanting "every subscriber sees every event,
with backpressure to producers when any subscriber is slow" —
that's a per-subscriber `Mpmc(T, cap)` and a publisher that loops
sending to all of them. Volt doesn't ship a "lossless broadcast
with global backpressure" because the right tradeoff depends on
your application (do you queue indefinitely? drop the slowest
consumer? something else?).

## Variant: filter at the subscriber

The subscriber decides what's interesting:

```zig
fn messageOnlySubscriber(rx: volt.Broadcast(Event, 64).Receiver) void {
    var r = rx;
    while (true) {
        const e = r.recv() catch |err| switch (err) {
            error.Closed => return,
            error.Lagged => continue,
        };
        if (e.kind != .message) continue;
        handleMessage(e);
    }
}
```

Filtering in the subscriber is fine — every subscriber gets a copy.
Filtering in the publisher would either need multiple Broadcasts
(per-topic) or a richer event type with subscriber metadata.

## Variant: per-topic broadcasts

For pub/sub by topic, hold a hashmap of Broadcasts:

```zig
const Bus = struct {
    topics: std.StringHashMap(*volt.Broadcast(Event, 128)),
    mu: volt.Mutex,
    alloc: std.mem.Allocator,

    pub fn publish(self: *Bus, topic: []const u8, e: Event) void {
        self.mu.lock();
        const b = self.topics.get(topic);
        self.mu.unlock();
        if (b) |bc| bc.send(e);
    }

    pub fn subscribe(self: *Bus, topic: []const u8) !volt.Broadcast(Event, 128).Receiver {
        self.mu.lock();
        defer self.mu.unlock();
        const entry = try self.topics.getOrPut(topic);
        if (!entry.found_existing) {
            const bc = try self.alloc.create(volt.Broadcast(Event, 128));
            bc.* = volt.Broadcast(Event, 128).init();
            entry.value_ptr.* = bc;
        }
        return entry.value_ptr.*.receiver();
    }
};
```

A Mutex protects the topic map. `publish` releases the mutex
before calling `send` so concurrent subscribers don't block
publishers.

## Closing semantics

`b.close()` causes subsequent `recv()` calls to return
`error.Closed`. Subscribers parked on `recv()` wake immediately
with `error.Closed`. Once closed, the Broadcast cannot be
reopened — create a fresh one if you need another round.

The publisher in the example calls `close()` after its loop;
subscribers see `error.Closed` on their next `recv` and exit.

## See also

- [Channels: Broadcast](/usage/channels/) — the API.
- [Channels internals](/architecture/channels-internals/) — how Broadcast's ring + per-receiver cursors work.
- [Config hot-reload](/cookbook/config-hot-reload/) — when Watch is the right tool instead.

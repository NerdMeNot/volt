---
title: Channels
description: Channel, Oneshot, Watch, Broadcast, and select — message passing primitives in Volt.
---

Volt has four channel types and a `select`. They share a unified
error vocabulary — `error.Closed` and `error.Cancelled` — so a
single `catch` can cover the whole family.

| Type | Pattern | Allocates | Use case |
|---|---|---|---|
| `Channel(T)` | MPMC bounded queue | yes (init) | Work queues, pipelines with backpressure |
| `Oneshot(T)` | 1:1, single value | no | Hand a single result from producer to consumer |
| `Watch(T)` | 1:N, latest value | no | Config hot-reload, current-state propagation |
| `Broadcast(T)` | 1:N, history-aware | yes (init) | Pub/sub fan-out, event streams |

```
Channel(T):  many → ring buffer → many
   P1 ──┐                      ┌── C1
   P2 ──┼──[ A B C D E . . ]──┼── C2     bounded; full → backpressure
   P3 ──┘  cap-sized ring     └── C3

Oneshot(T):  single → slot → single
   P  ──── [   v   ] ──── C                 once; second send → Closed

Watch(T):  single → latest-value cell → many independent receivers
                  ┌── R1 (cursor=v3)
   P ── [v3] ─────┼── R2 (cursor=v2)        each rx polls rx.changed()
                  └── R3 (cursor=v3)         missed values → silent skip

Broadcast(T):  single → ring buffer → many independent receivers
                            ┌── R1 (cursor=4)
   P ── [3 4 5 6 7 8 9 10] ─┼── R2 (cursor=8)   slow rx → .lagged(N) tag
        cap-sized ring      └── R3 (cursor=10)  no producer backpressure
```

## Channel(T) — bounded MPMC

```zig
var ch = try volt.channel.Channel(u32).init(allocator, 64);
defer ch.deinit();

// Producer (any coroutine):
try ch.send(7);                 // suspends if full; error.Closed | Cancelled

// Consumer (any coroutine):
const v = try ch.recv();        // suspends if empty; error.Closed | Cancelled

// Close — wakes all waiters:
ch.close();
```

Capacity is rounded up to the next power of two with a floor of 2.
The implementation is a Vyukov MPMC ring with a closed-bit packed
into the tail counter; the fast path is one CAS.

### Non-blocking variants

```zig
switch (ch.trySend(value)) {
    .sent => {},
    .full => { /* backpressure — drop, retry, ... */ },
    .closed => {},
}

switch (ch.tryRecv()) {
    .value => |v| handle(v),
    .empty => {},
    .closed => return,
}
```

`trySend` / `tryRecv` are lock-free and callable from any thread.
The blocking `send` / `recv` variants are coroutine-only.

### When to use Channel

- Producer/consumer pipelines (fixed-buffer backpressure).
- Job queues where workers pull units of work.
- Anywhere the queue depth itself is the load-shedding signal.

## Oneshot(T) — single hand-off

```zig
var os = volt.channel.Oneshot(Result){};

// Sender:
try os.send(.{ .ok = 42 });    // first send wins; subsequent → error.Closed

// Receiver:
const r = try os.recv();        // suspends; returns value or error.Closed
```

Zero allocation. Useful for:

- "Eventual result" patterns — one task hands a value back to its
  parent.
- Race-style fan-out (`select`/`scope` first-wins).
- Building higher-level futures.

If you call `send` twice or `send` after `close`, the second call
returns `error.Closed` — the channel is "closed" from the
second-sender's perspective once the first send wins.

## Watch(T) — latest-value broadcast

```zig
var w = volt.channel.Watch(Config).init(initial_cfg);
defer w.deinit();

// Producer:
w.send(new_cfg);           // overwrites; doesn't queue

// Each consumer holds its own Receiver:
var rx = w.subscribe();
while (true) {
    try rx.changed();           // suspend until value updates
    const cfg = rx.current();   // snapshot copy
    applyConfig(cfg);
}
```

Receiver methods:

| Method | Returns |
|---|---|
| `current()` | Snapshot copy of the latest value |
| `hasChanged()` | True iff version > seen_version |
| `markSeen()` | Update seen_version without waiting |
| `changed()` | Suspend until version advances; `error.Closed` / `error.Cancelled` |

Watch is the right tool when you have a value that changes
periodically and consumers always want the latest — not history.
Slow consumers don't backpressure producers; they just see fewer
intermediate values.

Closing a Watch wakes all parked `changed()` calls with
`error.Closed`.

## Broadcast(T) — fan-out with history

```zig
var b = try volt.channel.Broadcast(Event).init(allocator, 128);
defer b.deinit();

// Producer:
try b.send(event);         // error.Closed if closed

// Each consumer subscribes:
var rx = b.subscribe();
while (true) {
    switch (try rx.recv()) {
        .value => |v| handle(v),
        .lagged => |n| std.log.warn("dropped {} events", .{n}),
        .closed => return,
    }
}
```

Capacity is the maximum lag tolerance. If a receiver falls more
than `capacity` messages behind, the next `recv` returns
`.lagged(N)` with `N` = number of dropped messages, and the
receiver's cursor jumps to the oldest available message. Slow
consumers don't backpressure producers.

The error path on `recv` is just `error.Cancelled` — `closed` is a
tagged-union return because consumers usually want to distinguish
"channel closed cleanly" from "I was cancelled."

## select — wait on the first ready

```zig
switch (try volt.select(.{
    .msg = volt.channel.OnRecv(u32){ .ch = &cmd_ch },
    .quit = volt.channel.OnRecv(void){ .ch = &shutdown_ch },
    .timeout = volt.channel.OnRecv(void){ .ch = &timeout_ch },
})) {
    .msg => |v| try handle(v),
    .quit => return,
    .timeout => continue,
}
```

The result is a tagged union with one variant per branch (named
after the branch's field name). `OnRecv(T)` is currently the only
branch type; `OnSend` and `OnTimeout` are planned.

Up to 16 branches. The implementation spawns one forwarder
coroutine per branch and races them; the first to receive a value
sends it into a Oneshot, main parks on that Oneshot, losers are
cancelled. Cancellable parks make the cleanup prompt.

### Lossy on simultaneous publish

If two branches publish at exactly the same instant and both
forwarders consume their values before main wakes and cancels them,
**one of those values is lost** — the loser's value was consumed
from its channel but never delivered to main. For most workloads
this is fine; if you can't tolerate lost values, use a `Channel(T)`
with manual multiplexing instead.

A lossless `select` with two-phase claim-before-consume is planned
once the model checker validates the interleavings.

## Closing semantics

| Channel | `close()` effect |
|---|---|
| `Channel(T)` | Wakes all parked senders + receivers with `error.Closed` |
| `Oneshot(T)` | Wakes parked receiver with `error.Closed`; subsequent sends fail |
| `Watch(T)` | Wakes all parked `changed()` calls with `error.Closed` |
| `Broadcast(T)` | All subsequent `recv()` return `.closed` (tagged-union, not error) |

Once closed, a channel cannot be reopened. Subsequent `send` calls
return `error.Closed` (or `.closed` for `Channel.trySend`).

## Picking the right channel

- One value, one consumer? `Oneshot`.
- Many producers and/or consumers, backpressure desired? `Channel`.
- Slow consumers OK to miss intermediate values, all want latest?
  `Watch`.
- Slow consumers should see history but not block producers?
  `Broadcast`.

If you find yourself wanting "all messages, no drops, multiple
consumers" — that's not a single channel. It's a fan-out of N
independent `Channel`s, one per consumer, and a producer that
sends to all of them. Volt does not provide a "lossless broadcast"
type because the right structure depends on what you want to do
when one consumer falls behind.

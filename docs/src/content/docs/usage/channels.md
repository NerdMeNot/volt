---
title: Channels
description: Spsc, Mpmc, Oneshot, Watch, Broadcast — five comptime-specialised channel shapes for the patterns you actually need.
---

Volt has five channel types. They share a unified error vocabulary
(`error.Closed`, `error.Cancelled` via the cancel-aware variants)
and all route their block-on-full / block-on-empty paths through
the parking lot.

| Type | Pattern | Backpressure | Use case |
|---|---|---|---|
| `Spsc(T, cap)` | 1 producer, 1 consumer, bounded | yes | Pipeline stages; fastest channel |
| `Mpmc(T, cap)` | N producers, M consumers, bounded | yes | Work queues, fan-out, fan-in |
| `Oneshot(T)` | 1 producer, 1 consumer, single value | n/a | Result handoff, races, futures |
| `Watch(T)` | 1 producer, N consumers, latest value | no (drops intermediate) | Config hot-reload, current state |
| `Broadcast(T, cap)` | 1 producer, N consumers, history-aware | no (slow rx → Lagged) | Pub/sub, event streams |

Capacity (`cap`) is **comptime**. `Spsc(T, 16)` and `Spsc(T, 32)` are
distinct types; the compiler specialises ring sizes at the call site.

## Spsc(T, cap) — single-producer / single-consumer

The fastest channel. One pair of pointers (head / tail) on separate
cache lines, comptime-specialised modulo. 12 ns/op on the bench
(see [Performance](/performance/)).

```zig
var ch: volt.Spsc(u64, 16) = .{};

// Producer:
try ch.send(7);              // suspends if full; error.Closed if closed
// Consumer:
const v = try ch.recv();     // suspends if empty; error.Closed if closed

// Cancel-aware variants:
try ch.sendCancel(7, &c);    // CancelError = error{Closed, Cancelled}
const v2 = try ch.recvCancel(&c);

// Close — wakes both ends:
ch.close();
```

The single-producer / single-consumer contract is enforced by you,
not the type. Concurrent `send` from two coroutines is undefined
behaviour; same for concurrent `recv`. If you need many-to-many,
use `Mpmc`.

Initialization is zero (`= .{}`); no `init`/`deinit` needed.

## Mpmc(T, cap) — multi-producer / multi-consumer

Vyukov bounded MPMC ring with per-cell sequence counters. Each
cell carries a `seq` that producers / consumers advance via CAS;
producers see "full" when `cell.seq < pos` (one lap behind),
consumers see "empty" when `cell.seq < pos + 1`.

```zig
var ch = volt.Mpmc(u64, 64).init();
defer ch.deinit();

// Any coroutine, any thread:
try ch.send(value);
const v = try ch.recv();

// Non-blocking variants:
ch.trySend(value) catch |e| switch (e) {
    error.Full => { /* backpressure */ },
    error.Closed => return,
};
const v2 = ch.tryRecv() catch |e| switch (e) {
    error.Empty => continue,
    error.Closed => return,
};

ch.close();   // wakes all parked senders + receivers with error.Closed
```

Use Mpmc for:

- Work queues (M producers enqueueing units, N workers consuming).
- Fan-in (N producers → 1 consumer with throughput limits).
- Any channel scenario where Spsc's 1:1 contract is too restrictive.

About 5× slower than Spsc per op due to the CAS-based producer /
consumer paths, but still under 150 ns at 4P × 4C. Cancel-aware
variants (`sendCancel`, `recvCancel`) follow the same pattern.

## Oneshot(T) — single-value handoff

```zig
var os: volt.Oneshot(Result) = .{};

// One sender, one receiver:
try os.send(.{ .value = 42 });    // error.Closed if already sent or closed
const r = try os.recv();          // parks until send or close
```

Zero allocation. The value lives in the Oneshot struct itself.

Used for:

- Result handoff: spawn a child, hand it a `*Oneshot`, child sends
  its result, parent receives.
- Race-style patterns: N children share one Oneshot, first to send
  wins, others see `error.Closed` on their attempt.
- Building higher-level futures / promises.

`send` is single-shot; second call returns `error.Closed`.
`recv` blocks; cancel-aware variant `recvCancel(&c)` returns
`error.Cancelled` when the held Cancel fires.

## Watch(T) — latest-value broadcast

1 producer, N consumers. Producer never blocks; consumers see the
**latest** published value, missing intermediate values silently.
Built on a seqlock so readers don't block writers.

```zig
var w = volt.Watch(Config).init(initial_config);
defer w.deinit();

// Producer (any coroutine):
w.send(new_config);    // overwrites; never blocks; wakes all parked receivers

// Each consumer:
var rx = w.receiver();
while (true) {
    try rx.changed();           // park until version > my cursor
    const cfg = rx.borrow();    // current latest (seqlock-spin on mid-write)
    applyConfig(cfg);
}
```

Watch is the right tool for "value changes periodically and
consumers always want the latest" — typical config-hot-reload
shape. Slow consumers see fewer intermediate values; they never
backpressure producers.

`w.close()` wakes every parked `changed()` call with
`error.Closed`. Subsequent `changed()` returns `error.Closed`
immediately.

## Broadcast(T, cap) — history-aware fan-out

1 producer, N consumers, bounded ring. Each receiver has its own
cursor; slow receivers get `error.Lagged` when they fall too far
behind, and their cursor jumps forward.

```zig
var b = volt.Broadcast(Event, 128).init();
defer b.deinit();

// Producer:
b.send(event);     // never blocks (overwrites if all receivers behind)

// Each consumer:
var rx = b.receiver();
while (true) {
    const event = rx.recv() catch |e| switch (e) {
        error.Closed => break,
        error.Lagged => continue,    // cursor advanced; missed N events
    };
    handle(event);
}
```

Use when:

- You want every consumer to see every event (within their lag
  budget).
- Slow consumers should not block the producer (different from
  Mpmc, which blocks the producer when full).
- Lost events on overrun are tolerable — and detectable via
  `error.Lagged`.

The capacity is the lag tolerance. A consumer that falls more than
`cap` events behind the producer gets `error.Lagged` on its next
`recv`; the cursor jumps to the oldest available message.

## Closing semantics

| Channel | `close()` |
|---|---|
| `Spsc` | Wakes both ends with `error.Closed`. Subsequent send/recv return `error.Closed`. |
| `Mpmc` | Wakes all parked senders/receivers with `error.Closed`. |
| `Oneshot` | Wakes parked receiver with `error.Closed`. Subsequent sends fail. |
| `Watch` | Wakes parked `changed()` with `error.Closed`. |
| `Broadcast` | All subsequent `recv` return `error.Closed`. |

Once closed, channels stay closed — no reopen.

## Picking the right channel

| You have… | Use |
|---|---|
| Producer-consumer pipeline, exactly 1:1 | `Spsc` |
| Work queue, M producers, N workers | `Mpmc` |
| One result, one consumer | `Oneshot` |
| Periodic value, consumers want latest | `Watch` |
| Event stream, slow consumers OK to lag | `Broadcast` |
| "Many consumers, every consumer sees every event, no drops, no producer block" | **Not a single channel** — fan out one Mpmc per consumer with a forwarder, or a per-consumer Spsc. |

The absence of a generic "lossless broadcast" is intentional: the
right structure depends on what you want when one consumer falls
behind. Volt doesn't pick for you.

## See also

- [Channels internals](/architecture/) — Vyukov MPMC, seqlock for Watch, ring with cursors for Broadcast.
- [Cookbook: pub/sub](/cookbook/pub-sub/) — Broadcast in a real pattern.
- [Cookbook: config hot-reload](/cookbook/config-hot-reload/) — Watch in a real pattern.

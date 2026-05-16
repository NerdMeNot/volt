---
title: Channels internals
description: How Volt's five channels are actually built. Vyukov MPMC ring, Spsc cache-line layout, Oneshot's atomic state machine, Watch's seqlock, Broadcast's per-receiver cursors.
---

The user-facing channel API ([Channels](/usage/channels/)) is five
shapes — Spsc, Mpmc, Oneshot, Watch, Broadcast. Each is a
different algorithm. This page is how they actually work.

The unifying piece is the parking lot: every channel that can
block uses the same `parkOn(addr, validator)` substrate, keyed on
some atomic field in the channel's state. We only cover the
**channel-specific** bits here; see [The parking
lot](/architecture/parking-lot/) for the substrate.

## Spsc — single-producer, single-consumer

Ring buffer with a head and tail counter, comptime-specialised
capacity. Sender bumps `tail`; receiver bumps `head`. The
difference is the count.

```
                                       cap = 8 (comptime)
                                ┌─── cells: [T; 8]
                                │
                          tail  ▼   head
                            │   ║   │
   ┌──┬──┬──┬──┬──┬──┬──┬──┬──┐
   │A │B │C │D │  │  │  │  │  │     producer writes at tail++
   └──┴──┴──┴──┴──┴──┴──┴──┴──┘     consumer reads at head++
                                   modular: tail % cap, head % cap
```

Reference: `src/channel.zig` (the `Spsc(T, cap)` type).

Layout choices:

- **Comptime capacity** → modular arithmetic compiles to bitmask
  (`cap` must be power-of-2; the type asserts this).
- **`head` and `tail` on separate cache lines.** Producer touches
  tail; consumer touches head. False sharing would dominate the
  fast path; explicit padding avoids it.
- **No mutex.** Single-producer / single-consumer means at most one
  thread per counter. Acquire/release ordering on counter bumps
  is sufficient.

### Block-on-full / block-on-empty

```zig
pub fn send(self: *Self, v: T) ChannelError!void {
    while (true) {
        const t = self.tail.load(.monotonic);
        if (t - self.head.load(.acquire) < cap) {
            self.cells[t % cap] = v;
            self.tail.store(t + 1, .release);
            parking_lot.unparkOne(rt, &self.head);  // wake recv if parked
            return;
        }
        if (self.closed.load(.acquire)) return error.Closed;
        // Park on &self.tail, waiting for recv to bump head.
        park.parkOn(&self.tail, sendValidator);
    }
}
```

The validator (called under the parking lot's bucket lock) re-checks
"is the channel still full?" — if recv has bumped head between the
top-of-loop check and the bucket-lock-acquisition, the validator
returns false and we retry instead of parking.

Same shape for `recv`, mirrored — parks on `&self.head`.

Performance receipt: **12 ns/op** for send-then-recv on
`bench-spsc cap=16`. The hot path is one load + one compare + one
store + one parking-lot fast-check (no bucket lock taken when no
one's parked).

## Mpmc — Vyukov bounded MPMC

Multiple producers + multiple consumers, bounded ring. The
algorithm is Dmitry Vyukov's MPMC ring with per-cell sequence
counters; it's the same shape Volt's per-P worker mailboxes use.

```
                  cap = 4
       ┌────────────┬────────────┬────────────┬────────────┐
  cell │ seq=0  v=? │ seq=0  v=? │ seq=0  v=? │ seq=0  v=? │
       └────────────┴────────────┴────────────┴────────────┘

   enqueue_pos ──┘    each producer CASes enqueue_pos++, then
                       writes cells[pos % cap].v and bumps
                       cells[pos % cap].seq to pos+1.

   dequeue_pos ──┘    each consumer CASes dequeue_pos++ when
                       cells[pos % cap].seq == pos+1, reads .v,
                       then bumps .seq to pos + cap.
```

Reference: `src/channel.zig` (the `Mpmc(T, cap)` type).

The protocol:

- **Producer at pos P** succeeds when `cells[P % cap].seq == P`
  (cell is ready to be filled at this lap). CAS `enqueue_pos`
  from P to P+1, write the value, then publish via `seq = P + 1`.
- **Consumer at pos C** succeeds when `cells[C % cap].seq == C + 1`
  (cell has been filled this lap). CAS `dequeue_pos` from C to
  C+1, read the value, then mark the cell free for the next lap
  via `seq = C + cap`.

Each cell's `seq` is the synchronisation point — it tells producers
and consumers exactly what lap the cell is on.

Why Vyukov's algorithm over a simpler lock-protected ring:

1. **Wait-free under low contention.** Producer and consumer
   don't contend on the same cache line unless the ring is full
   or empty.
2. **No false sharing across producers.** Each producer writes
   one cell; cells are far apart in memory (`cap * sizeof(T)`).
3. **Works with parking.** Parking is added at the "full" / "empty"
   boundaries; the steady-state lock-free fast path is untouched.

Block-on-full / block-on-empty paths look like Spsc's but with
the CAS retry inside. Reference: `src/channel.zig`'s Mpmc
implementation.

Performance receipt: **59 ns/op** at 1P × 1C, **157 ns/op** at
4P × 4C. About 5× Spsc's at 1×1 due to the CAS overhead;
scales reasonably under contention.

## Oneshot — single-value handoff

```
                 ┌──────────────────────────┐
                 │  state: atomic enum      │
                 │    EMPTY → READY → READ  │
                 │    or EMPTY → CLOSED     │
                 │                          │
                 │  value: T                │
                 │  waiters: parking-lot    │
                 │           keyed on &state │
                 └──────────────────────────┘
```

Reference: `src/channel.zig` (the `Oneshot(T)` type).

The state machine:

- **`send(v)`**: CAS state EMPTY → READY. If success, write
  `value = v` and unparkAll on `&state`. If state was already
  READY or CLOSED, return `error.Closed`.
- **`recv()`**: load state. If READY, CAS to READ, read value,
  return. If EMPTY, park on `&state`. If CLOSED, return
  `error.Closed`.

The release/acquire pair on the state CAS ensures the value write
happens-before the recv's read.

Single-shot: `send` only succeeds once. The Oneshot's `value`
field lives in the struct, no heap allocation.

## Watch — latest-value, seqlock-based

One producer publishes; N receivers borrow the latest. Producer
never blocks; receivers see only the latest value (intermediate
values silently dropped).

The trick is the **seqlock**: a counter that the writer increments
before and after each write. Readers spin on the counter parity
to detect concurrent writes.

```
                  ┌─────────────────────────────────┐
                  │   Watch(T)                      │
                  │                                 │
                  │   version: atomic u64           │
                  │   value: T  (regular memory)    │
                  │   closed: atomic bool           │
                  │   waiters: parking-lot keyed    │
                  │            on &version          │
                  └─────────────────────────────────┘

   writer:  version.fetch_add(1)        // mark "write in progress"
            // (odd version → mid-write)
            value = v
            version.fetch_add(1)        // mark "write done"
            // (even version again)
            unparkAll(rt, &version)

   reader:  loop:
              v1 = version.load(.acquire)
              if v1 is odd: spin (mid-write)
              snapshot = value
              v2 = version.load(.acquire)
              if v1 == v2: return snapshot
              else: retry (write happened during snapshot)
```

Reference: `src/channel.zig` (the `Watch(T)` type, `borrow` and
`changed`).

`Receiver.changed()` parks on `&watch.version` until version
> cursor; the validator checks both the version and the closed
flag.

Use case: config hot-reload. Producer updates the live config,
all consumers see the latest version, slow consumers don't
backpressure (they just miss intermediate updates).

## Broadcast — fan-out with history

One producer, N consumers, bounded ring. Each consumer has its
own cursor; slow consumers get `error.Lagged` when they fall too
far behind.

```
   ring (cap = 4):
   ┌──────┬──────┬──────┬──────┐
   │  v3  │  v4  │  v5  │  v6  │       producer publishes at tail++
   └──────┴──────┴──────┴──────┘
                                       wrapping when full
   head = 3       tail = 7

   receivers' cursors:
     R1.cursor = 7     (caught up; recv parks)
     R2.cursor = 5     (2 messages behind; recv returns v5, advances)
     R3.cursor = 2     (lagged! < head=3; recv returns error.Lagged,
                        cursor jumps to head)
```

Reference: `src/channel.zig` (the `Broadcast(T, cap)` type).

Producer:

```zig
pub fn send(self: *Self, v: T) void {
    const t = self.tail.load(.monotonic);
    const idx = t % cap;
    self.cells[idx] = v;
    self.tail.store(t + 1, .release);
    if (t - self.head.load(.acquire) >= cap) {
        self.head.store(t + 1 - cap, .release);  // ring full; advance head
    }
    parking_lot.unparkAll(rt, &self.tail);  // wake all parked receivers
}
```

Receiver:

```zig
pub fn recv(self: *Receiver) error{Closed, Lagged}!T {
    while (true) {
        const t = self.channel.tail.load(.acquire);
        const h = self.channel.head.load(.acquire);

        if (self.cursor < h) {
            // Lagged: producer wrapped past our cursor.
            self.cursor = h;
            return error.Lagged;
        }
        if (self.cursor < t) {
            const v = self.channel.cells[self.cursor % cap];
            self.cursor += 1;
            return v;
        }
        if (self.channel.closed.load(.acquire)) return error.Closed;
        // No new value; park on &channel.tail.
        park.parkOn(&self.channel.tail, recvValidator);
    }
}
```

The producer never blocks. Slow receivers detect lag via the
`cursor < head` check and the cursor jumps forward — the receiver
loses those intermediate messages but stays in sync. The
`error.Lagged` is the API's way to tell the caller "you missed
N messages" (the cursor delta is implicit; we just signal the
fact).

## Sync primitives are channels too, kinda

Semaphore is conceptually a channel of "permits". `acquire` is
recv (parks on empty), `release` is send (unparks one waiter).
The implementation is simpler than a channel — no value type,
just an atomic counter — but the parking-lot interaction is
identical.

```zig
pub fn acquire(self: *Semaphore) void {
    while (true) {
        const cur = self.count.load(.acquire);
        if (cur == 0) {
            park.parkOn(&self.count, acquireValidator);
            continue;
        }
        if (self.count.cmpxchgWeak(cur, cur - 1, .acq_rel, .acquire) == null) {
            return;
        }
    }
}

pub fn release(self: *Semaphore) void {
    _ = self.count.fetchAdd(1, .acq_rel);
    parking_lot.unparkOne(rt, &self.count);
}
```

Reference: `src/sync.zig`. The validator-under-lock pattern is
the same; the parking lot doesn't care that this is a sync
primitive vs a channel.

## Cancel-aware variants share one pattern

Every cancel-aware blocking op (sendCancel, recvCancel,
acquireCancel, etc.) follows the protocol described in
[Cancellation internals](/architecture/cancellation-internals/):

1. Fast path try (no parking).
2. Register a Waiter on the Cancel pointing to the primitive's
   address.
3. Park with a validator that checks both the primitive's state
   AND the Cancel flag.
4. Re-check Cancel.isFired() on wake.

The cancel-aware logic isn't channel-specific; it's the parking
lot + Cancel layer.

## Tried & rejected: traditional locked queue

Most "easy" channel implementations are a mutex around an array,
plus condition variables for blocking. Why we don't:

1. **Lock contention dominates** under multi-producer load.
   Vyukov's algorithm scales O(1) up to ring capacity; locked
   queue scales O(producers).
2. **No condvar primitive in Volt.** We'd build one on the
   parking lot anyway; might as well skip the abstraction and
   park directly on the channel state.
3. **Cache-line layout matters.** A mutex-protected queue has all
   readers contending on the mutex word's cache line; Vyukov's
   per-cell layout doesn't.

The cost: more complex implementation. Trade-off accepted.

## Tried & rejected: SPMC / MPSC channels as separate types

Common runtime libraries have 1-producer-many-consumers and
many-producers-1-consumer as separate types (e.g., Tokio's
`watch` is SPMC). Volt rolled them into Watch (SPMC) and Mpmc
(N×M) without a dedicated MPSC.

For MPSC, use Mpmc — the overhead of supporting one extra
consumer is negligible. The space win of a dedicated MPSC isn't
worth the extra type.

## Performance summary

| Workload | Volt | Go reference | Notes |
|---|---|---|---|
| `bench-spsc` cap=16 | 12 ns/op | 33 ns/op | Comptime cap → bitmask modulo |
| `bench-mpmc` 1P × 1C | 59 ns/op | — | CAS overhead vs Spsc |
| `bench-mpmc` 2P × 2C | 91 ns/op | — | One CAS retry per op typical |
| `bench-mpmc` 4P × 4C | 157 ns/op | — | Scales under contention |

All on Darwin arm64 ReleaseFast.

## Further reading

- [Channels](/usage/channels/) — the user API.
- [The parking lot](/architecture/parking-lot/) — the wait/wake substrate.
- [Cancellation internals](/architecture/cancellation-internals/) — how cancel-aware variants integrate.
- Dmitry Vyukov's "Bounded MPMC queue" — original algorithm description on 1024cores.net.
- Linux kernel's seqlock — same pattern Watch uses; canonical implementation.
- Tokio's `tokio::sync::watch` — Rust equivalent of Watch; same shape.
- Tokio's `tokio::sync::broadcast` — Rust equivalent of Broadcast; similar but with explicit per-receiver wakers (Volt's parking-lot model means we don't need explicit wakers).

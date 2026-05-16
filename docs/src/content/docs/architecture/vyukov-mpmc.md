---
title: Vyukov MPMC Queue
description: The bounded multi-producer multi-consumer ring buffer that powers volt.channel.Channel(T).
---

`volt.channel.Channel(T)` is a bounded MPMC queue. The underlying
ring buffer uses Dmitry Vyukov's well-known design: per-slot
sequence numbers as the synchronization point, with the head and
tail counters as separate atomics. Lock-free fast path; one CAS
per send and per receive in the uncontended case.

## The design

```
slots[i]: { value: T, seq: u64 }
head: u64    // index of the next item to consume
tail: u64    // index where the next item will be produced
```

Each slot carries its own sequence number. The ring is power-of-2
sized; the slot for index `i` is `slots[i & mask]`.

```
                         tail (next producer slot)
                          │
                          ▼
   ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
   │ seq=8 │ seq=9 │ seq=2 │ seq=3 │ seq=4 │ seq=5 │ seq=6 │ seq=7 │
   │  V    │  V    │   _   │   _   │   _   │   _   │   _   │   _   │
   └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
                  ▲
                  │
                head (next consumer slot)

  slot[i].seq tracks the lifecycle of slot i:
   • seq == pos      → empty AND it's producer's turn at index pos
   • seq == pos + 1  → full AND it's consumer's turn at index pos
   • seq == pos + cap → empty AND ready for the NEXT producer wrap
```

## Producer (send)

```
loop {
    let pos = tail.load(acquire)
    let slot = &slots[pos & mask]
    let seq = slot.seq.load(acquire)
    let diff = seq - pos

    if diff == 0:
        // Slot is empty AND it's our turn (seq matches expected).
        if tail.cmpxchg(pos, pos + 1, monotonic):
            slot.value = item
            slot.seq.store(pos + 1, release)   // mark slot full for consumer
            return ok
        // CAS lost — retry.
    elif diff < 0:
        return full
    else:
        // diff > 0: another producer beat us; reload pos.
        continue
}
```

The trick: `seq - pos` distinguishes states.

- `seq == pos` means "this slot is empty AND it's the slot for
  this producer's turn." Only one producer at this exact pos.
- `seq < pos` means "the consumer hasn't read the previous wrap
  yet." Queue is full.
- `seq > pos` means "another producer already filled this and the
  consumer is mid-read or the slot wrapped." Reload tail and retry.

The CAS on `tail` is what serializes producers — only one can
advance `tail` from any given value. Once you've successfully
CASed, the slot is yours; you write the value and bump the seq
to mark it full.

## Consumer (recv)

Mirror image:

```
loop {
    let pos = head.load(acquire)
    let slot = &slots[pos & mask]
    let seq = slot.seq.load(acquire)
    let diff = seq - (pos + 1)

    if diff == 0:
        // Slot is full and it's our slot.
        if head.cmpxchg(pos, pos + 1, monotonic):
            let value = slot.value
            slot.seq.store(pos + mask + 1, release)  // mark slot empty for next wrap
            return ok(value)
    elif diff < 0:
        return empty
    else:
        continue
}
```

The completed-empty marker `pos + mask + 1` puts the slot's seq
exactly where the *next* producer wrap will expect "empty AND my
turn." That's how producer and consumer leapfrog through the
ring without explicit handoff signals.

## The closed bit

Volt's `Channel(T)` packs a "closed" flag into the high bit of
`tail`:

```
tail: AtomicU64
   └─ bit 63: closed?
   └─ bits 0-62: counter
```

Senders check the closed bit on every load of `tail`; if set,
return `error.Closed`. Receivers check after every successful
read; if the queue drains AND the bit is set, return
`error.Closed` to subsequent recv calls.

This is "free" because tail is loaded anyway — no extra atomic.

## Capacity rounding

The ring is power-of-2 sized so we can mask instead of mod.
`Channel(T).init(allocator, requested_capacity)` rounds up:

```
floored = max(requested_capacity, 2)   // floor of 2 (cap=1 has degenerate edge cases)
cap = nextPowerOfTwo(floored)
```

So `Channel(T).init(alloc, 5)` actually has 8 slots; `init(alloc, 17)`
has 32. Memory rounded up to the next power of two.

## Why floor of 2?

Capacity 1 has a pathological case: if a producer fills the slot
and a consumer drains it without observing the slot's
intermediate sequence transitions, the producer can spin
indefinitely waiting for "my turn" even though the slot is
logically free. Floor of 2 avoids the degenerate case at the
cost of one extra slot for very small queues.

## Volt's wrapper layer

The pure ring is lock-free and lock-friendly, but `Channel(T)` is
*more* than the ring — it also has waiter lists for
`send`/`recv` (the blocking forms) when the ring is full or
empty. Those waiters are managed under a `Mutex`:

- `trySend` / `tryRecv` go through the ring directly. Lock-free,
  fast.
- `send` / `recv` do `tryX` first; on `.full` / `.empty`, take
  the mutex, enqueue a waiter, park.

The mutex is held briefly — push/pop a waiter, release. Waker
notification (when a slot frees up or a value arrives) does the
mutex briefly too. The hot path stays lock-free.

## Why not a queue without a mutex?

Lock-free MPMC with park-on-full is hard to get right; the
classic Dekker-style "register intent before checking again"
patterns have notoriously subtle missed-wake bugs. Volt
deliberately uses an unconditional mutex on the wake path because
"a few hundred nanoseconds per blocked call" is cheap compared to
"a missed wake hangs forever."

A model-checked Dekker fast path is on the roadmap; it'd matter
for super-high-throughput workloads but the simple version is
plenty for what Volt targets.

## File

`src/channel/Channel.zig`. The reference implementation is
`attic/vyukov_channel_reference.zig` from Dmitry Vyukov's
original C++ code.

## Reference

The original article:

> Vyukov, D. *Bounded MPMC queue.*
> https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

The whole technique is in that one page. The seq-per-slot scheme
generalizes well — variants of it show up in disruptor patterns,
LMAX-style ring buffers, etc.

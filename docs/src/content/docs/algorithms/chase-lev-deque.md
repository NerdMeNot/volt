---
title: Chase-Lev Deque
description: The lock-free work-stealing deque Volt uses for per-worker queues. Owner pushes/pops one end, thieves steal from the other.
---

A work-stealing deque is a queue with **two access modes**:

- The **owner** (one specific thread) pushes and pops from the
  *bottom*. LIFO from its perspective.
- **Thieves** (any other thread) steal from the *top*. FIFO from
  the deque's perspective.

The Chase-Lev algorithm makes both operations lock-free using
only atomic operations on two indices. Volt uses one Chase-Lev
deque per worker; the worker is the owner, every other worker is
a potential thief.

## The interface

```zig
pub fn Deque(comptime T: type, comptime capacity: usize) type {
    return struct {
        // Owner-only operations:
        pub fn push(self: *Self, value: T) void;
        pub fn pop(self: *Self) ?T;

        // Any-thread operations:
        pub fn steal(self: *Self) StealResult;
    };
}

pub const StealResult = union(enum) {
    success: T,
    empty,
    abort,    // contended; retry
};
```

`push` and `pop` are called only by the owning worker. `steal` is
called by anyone.

## State

```zig
const Self = struct {
    top: Atomic(usize),       // index where steals happen (lowest)
    bottom: Atomic(usize),    // index where push/pop happen (highest)
    buffer: [capacity]T,
};
```

`top` and `bottom` are 64-bit indices that grow monotonically. The
actual array slot for index `i` is `buffer[i % capacity]`. As
long as `bottom - top <= capacity`, the deque has work in
`[top, bottom)`.

## push (owner-only)

```zig
pub fn push(self: *Self, value: T) void {
    const b = self.bottom.load(.monotonic);
    self.buffer[b % capacity] = value;
    // Release ordering: thieves observing the new bottom must see
    // the value already written.
    self.bottom.store(b + 1, .release);
}
```

No CAS needed because there's only one owner. The release-store
is paired with thieves' acquire-load on `bottom`.

## pop (owner-only)

The interesting operation. The owner wants to take from the
bottom (LIFO), but a thief might be racing to take from the top.
The trick is to *speculatively* claim the bottom slot, then check
if a thief beat you.

```zig
pub fn pop(self: *Self) ?T {
    const b = self.bottom.load(.monotonic) - 1;
    self.bottom.store(b, .seq_cst);

    const t = self.top.load(.seq_cst);
    if (t > b) {
        // Empty: restore bottom.
        self.bottom.store(b + 1, .monotonic);
        return null;
    }

    const value = self.buffer[b % capacity];
    if (t < b) {
        // No race; the value is ours.
        return value;
    }

    // t == b: exactly one item, racing with a thief.
    // CAS top to claim it; whoever wins gets it.
    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
        // Thief got it.
        self.bottom.store(b + 1, .monotonic);
        return null;
    }
    self.bottom.store(b + 1, .monotonic);
    return value;
}
```

The seq_cst ordering on the speculative `bottom` decrement and
the `top` load is the synchronization point. The classic Chase-Lev
paper proves this is correct against concurrent steals.

## steal (any thread)

```zig
pub fn steal(self: *Self) StealResult {
    const t = self.top.load(.acquire);
    // Synchronization fence: ensure we observe the most recent
    // bottom value.
    std.atomic.fence(.seq_cst);
    const b = self.bottom.load(.acquire);

    if (t >= b) return .empty;

    const value = self.buffer[t % capacity];
    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
        // Another thief beat us.
        return .abort;
    }
    return .{ .success = value };
}
```

Each thief tries to CAS `top` from its observed value to `top+1`,
claiming exactly one item. Only one thief can win per `top` value.

The `.abort` return is intentional — it tells the caller "the
deque had work but I lost the race; retry or pick another
victim." Volt's worker loop treats `.abort` as "try a different
victim" rather than spinning.

## Why this is fast

- **No locks**. All synchronization is via atomics on two indices.
- **No contention between owner ops**. Push/pop only touch
  `bottom`; thieves only CAS `top`. The owner-vs-thief race is
  rare (only when there's exactly one item left).
- **Cache-friendly**. Owner ops are LIFO — the most recently
  pushed value is reused first, while it's still in cache.
- **Bounded memory**. Fixed-size circular buffer; no allocator on
  the hot path.

The trade-off is the fixed capacity. Volt sets each worker's
deque to 256 slots; if a worker tries to push when full, the
overflow goes to the global injection queue. In practice this
is rare — coroutines are consumed faster than they're produced
unless you're spawning in a tight loop.

## What "FIFO from the deque's perspective" means

The owner pushes new items at `bottom` and pops from `bottom-1`
(LIFO). Thieves take from `top` (FIFO):

```
                  ┌─── thieves steal from here (FIFO)
                  ▼
            top ──┐
                  │
       ┌───┬───┬───┬───┬───┬───┬───┬───┐
       │   │ T3│ T7│ T9│ T12│ T13│   │   │   buffer (cap=8, masked)
       └───┴───┴───┴───┴───┴───┴───┴───┘
                              │
                       bottom ┘
                  ▲
                  └─── owner pushes / pops from here (LIFO)
```

After a `push(T14)`:

```
       ┌───┬───┬───┬───┬───┬───┬───┬───┐
       │   │ T3│ T7│ T9│T12│T13│T14│   │
       └───┴───┴───┴───┴───┴───┴───┴───┘
        top↑                      bottom↑
```

After a thief `steal()`:

```
       ┌───┬───┬───┬───┬───┬───┬───┬───┐
       │   │   │ T7│ T9│T12│T13│T14│   │   T3 went to thief
       └───┴───┴───┴───┴───┴───┴───┴───┘
            top↑                  bottom↑
```

The owner sees its own work as a stack (most recent first).
Thieves see the deque as a queue (oldest first). Same data
structure, two views — top advances on steal, bottom advances on
push, neither index ever decreases (`top - bottom` always ≤ cap).

## Why FIFO for thieves

A worker's own coroutines tend to be cache-hot if they were just
pushed. Thieves take older items the owner is less likely to
revisit. This minimizes the cache cost of stealing — you don't
poach items the owner was about to use anyway.

The owner's LIFO mode also helps: the very next coroutine the
owner runs is the one it just spawned, which often depends on
data the owner just produced.

## File

`src/scheduler/deque.zig`. ~150 lines, self-contained, with
inline tests.

## Reference

The original paper:

> Chase, D. and Lev, Y. *Dynamic circular work-stealing deque.*
> SPAA '05.

The key insight in the paper is that the owner can speculatively
"claim" an item by decrementing `bottom` *before* checking the
race with thieves. If the race happened, restoring `bottom`
costs one more atomic. The expected case (no race) is just two
atomic ops total per pop.

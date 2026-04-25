---
title: Vyukov MPMC Bounded Queue
description: The lock-free multi-producer multi-consumer channel algorithm used
  in Volt, based on Dmitry Vyukov's bounded queue with per-slot sequence
  numbers.
slug: v1.0.0-zig0.15.2/algorithms/vyukov-mpmc
---

Volt's `Channel(T)` implements a bounded MPMC (multi-producer, multi-consumer) queue using Dmitry Vyukov's lock-free ring buffer algorithm. This is the same algorithm used by crossbeam-channel in the Rust ecosystem. The data path (send/receive) is fully lock-free; a separate mutex protects only the waiter lists for async operations.

## The Core Idea

Each slot in the ring buffer has a **sequence number** that encodes the slot's state. Producers and consumers use CAS (compare-and-swap) on shared `head` and `tail` positions to claim slots, then use the sequence number to confirm the slot is in the expected state. No locks are needed on the data path.

```
Ring buffer with capacity 4:

  slot[0]          slot[1]          slot[2]          slot[3]
+----------+     +----------+     +----------+     +----------+
| seq: 0   |     | seq: 1   |     | seq: 2   |     | seq: 3   |
| value: - |     | value: - |     | value: - |     | value: - |
+----------+     +----------+     +----------+     +----------+
  ^                                                  ^
  head=0                                             tail=0
```

Initially, each slot's sequence equals its index. This means all slots are "writable" from the producer's perspective.

## The Algorithm

### Send (Producer)

To send a value at position `tail`:

1. Load the current `tail` position.
2. Compute `idx = tail & buf_mask` (bitmask — buffer is always power-of-2).
3. Load `slot[idx].sequence`.
4. Compare `sequence` with `tail`:
   * If `sequence == tail`: The slot is writable. CAS `tail` to `tail + 1` to claim it.
   * If `sequence < tail` (as signed difference): The slot still has unconsumed data. Queue is **full**.
   * If `sequence > tail`: Another producer claimed this slot. Reload `tail` and retry.
5. After claiming: write the value, then store `sequence = tail + 1` (marking "has data").

```zig
pub fn trySend(self: *Self, value: T) SendResult {
    var tail = self.tail.load(.monotonic);
    while (true) {
        // Closed state encoded in tail bit 63 — free check on already-loaded value
        if (isClosedBit(tail)) return .closed;

        // Bitmask index: 1 cycle vs ~20 for modulo on ARM64
        const idx: usize = @intCast(tail & self.buf_mask);
        const slot = &self.slots[idx];
        const seq = slot.sequence.load(.acquire);

        const diff: i64 = @bitCast(seq -% tail);

        if (diff == 0) {
            // Slot writable -- claim by advancing tail
            if (self.tail.cmpxchgWeak(tail, tail +% 1, .acq_rel, .monotonic)) |new_tail| {
                tail = new_tail;
                continue;
            }
            // Claimed! Write value and publish
            slot.value = value;
            slot.sequence.store(tail +% 1, .release);

            // seq_cst: Dekker protocol — pairs with seq_cst store
            // in recvWait(). See design/channel-wakeup-protocol.
            if (self.has_recv_waiters.load(.seq_cst)) {
                self.wakeOneRecvWaiter();
            }
            return .ok;
        } else if (diff < 0) {
            return .full;
        } else {
            tail = self.tail.load(.monotonic);
        }
    }
}
```

### Receive (Consumer)

To receive a value at position `head`:

1. Load the current `head` position.
2. Compute `idx = head & buf_mask` (bitmask — buffer is always power-of-2).
3. Load `slot[idx].sequence`.
4. Compare `sequence` with `head + 1`:
   * If `sequence == head + 1`: The slot has data. CAS `head` to `head + 1` to claim it.
   * If `sequence < head + 1` (as signed difference): The slot is empty. Queue is **empty**.
   * If `sequence > head + 1`: Another consumer claimed this slot. Reload `head` and retry.
5. After claiming: read the value, then store `sequence = head + capacity` (marking "writable again").

```zig
pub fn tryRecv(self: *Self) RecvResult {
    var head = self.head.load(.monotonic);
    while (true) {
        // Bitmask index: 1 cycle vs ~20 for modulo on ARM64
        const idx: usize = @intCast(head & self.buf_mask);
        const slot = &self.slots[idx];
        const seq = slot.sequence.load(.acquire);

        const diff: i64 = @bitCast(seq -% (head +% 1));

        if (diff == 0) {
            // Slot has data -- claim by advancing head
            if (self.head.cmpxchgWeak(head, head +% 1, .acq_rel, .monotonic)) |new_head| {
                head = new_head;
                continue;
            }
            // Claimed! Read value and release slot
            const value = slot.value;
            slot.sequence.store(head +% self.buf_cap, .release);

            // seq_cst: Dekker protocol — pairs with seq_cst store
            // in sendWait(). See design/channel-wakeup-protocol.
            if (self.has_send_waiters.load(.seq_cst)) {
                self.wakeOneSendWaiter();
            }
            return .{ .value = value };
        } else if (diff < 0) {
            if (isClosedBit(self.tail.load(.acquire))) return .closed;
            return .empty;
        } else {
            head = self.head.load(.monotonic);
        }
    }
}
```

## Sequence Number State Machine

The sequence number cycles through states as the slot is used. For a slot at index `i` with channel capacity `C`:

```
                            Producer claims
Writable: seq = i    --->   Has data: seq = tail + 1
    ^                            |
    |                            | Consumer claims
    |                            v
    +--- seq = head + C ---  Read complete
```

Each full cycle advances the sequence by `C`. After `N` complete send/receive cycles, the sequence for slot `i` is `i + N * C`. The wrapping arithmetic ensures this works correctly even when positions overflow u64.

### Visual Walkthrough

Starting state (capacity 4, all slots writable):

```
slot[0].seq=0  slot[1].seq=1  slot[2].seq=2  slot[3].seq=3
head=0, tail=0
```

After `send(A)` at tail=0:

```
slot[0].seq=1  slot[1].seq=1  slot[2].seq=2  slot[3].seq=3
         ^--- has data (seq == old_tail + 1)
head=0, tail=1
```

After `send(B)` at tail=1:

```
slot[0].seq=1  slot[1].seq=2  slot[2].seq=2  slot[3].seq=3
head=0, tail=2
```

After `recv()` at head=0, returns A:

```
slot[0].seq=4  slot[1].seq=2  slot[2].seq=2  slot[3].seq=3
         ^--- writable again (seq = head + capacity = 0 + 4)
head=1, tail=2
```

## ABA Prevention Through Sequence Numbers

The classic ABA problem in lock-free data structures occurs when a CAS succeeds even though the value was changed and then changed back. The Vyukov queue is immune to ABA because sequence numbers monotonically increase.

Consider the scenario:

1. Thread A reads `head=5`, `slot[5 & mask].seq = 6` (data ready, `6 == 5+1`).
2. Thread A is preempted.
3. Thread B receives from head=5, advances head to 6. Slot 5's seq becomes `5 + C`.
4. Thread B sends again, slot wraps, seq becomes `5 + C + 1`.
5. Thread A resumes, tries CAS on head from 5 to 6.

At step 5, `head` is now 6, not 5. Thread A's CAS fails. Even if by some contortion `head` wrapped back to 5, the sequence number would be `5 + C` (writable), not `6` (has data), so Thread A would see `diff != 0` and retry. The sequence counter makes each slot visit unique.

## Cache Line Padding

The `head` and `tail` fields are on separate cache lines to prevent false sharing between producers and consumers:

```zig
head: std.atomic.Value(u64) align(CACHE_LINE_SIZE),
tail: std.atomic.Value(u64) align(CACHE_LINE_SIZE),
```

Without this padding, every CAS on `head` would invalidate the cache line containing `tail` on other cores, and vice versa. Since producers write `tail` and consumers write `head`, false sharing would cause severe performance degradation under contention.

Each slot interleaves its sequence counter and value in a single struct:

```zig
const Slot = struct {
    sequence: std.atomic.Value(u64),
    value: T = undefined,
};
slots: []Slot,  // Interleaved sequence + value (crossbeam layout)
```

This interleaved layout gives spatial locality: the sequence check and value read/write hit the same cache line instead of bouncing between two separate arrays. This matches crossbeam-channel's slot layout.

## Power-of-2 Buffer Size

The internal buffer size is always rounded up to the next power of 2 (minimum 2):

```zig
const buf_cap: u64 = nextPow2(@max(cap, 2));
```

This serves two purposes:

1. **Bitmask indexing**: `tail & buf_mask` (1 cycle on ARM64) replaces `tail % buf_cap` (~20 cycles on ARM64). The mask is simply `buf_cap - 1`.

2. **Minimum 2 slots for correctness**: With a single slot (buf\_cap=1), the sequence number after a send would be `tail + 1 = 1`, and after a receive it would be `head + capacity = 0 + 1 = 1`. The "has data" state and "writable" state would have the same sequence number, making them indistinguishable.

For non-power-of-2 user capacities (e.g., capacity=100 with buf\_cap=128), an explicit capacity check ensures the channel does not exceed the user-requested limit:

```zig
if (self.buf_cap != self.capacity) {
    const head = self.head.load(.acquire);
    if (tail -% head >= self.capacity) return .full;
}
```

This check is skipped when `buf_cap == capacity` (already power-of-2), so the common case of power-of-2 capacities pays no extra cost.

## Waiter Integration for Async Operations

The lock-free ring buffer handles the fast path. When the channel is full (for senders) or empty (for receivers), tasks must wait. This is handled by a separate waiter system that does not touch the lock-free data path:

```
Fast path (lock-free):          Slow path (mutex-protected):
  trySend() / tryRecv()           waiter_mutex
  CAS on head/tail                send_waiters: intrusive list
  sequence number check           recv_waiters: intrusive list
```

The waiter system uses a "flag-before-check" protocol to prevent lost wakeups:

1. Lock `waiter_mutex`.
2. Set the `has_recv_waiters` (or `has_send_waiters`) flag with **seq\_cst**.
3. Re-check the ring buffer under lock (`trySend`/`tryRecv`).
4. If the re-check succeeds, clear the flag and return immediately.
5. If still blocked, add the waiter to the list and release the lock.

After a successful `trySend`, the sender checks `has_recv_waiters` (seq\_cst load) and wakes one receiver if set. After a successful `tryRecv`, the receiver checks `has_send_waiters` and wakes one sender. This ensures no waiter is stranded.

### Why Sequential Consistency?

The waiter flags implement a **Dekker pattern** — the fast path writes
the buffer then reads the flag, while the slow path writes the flag then
re-reads the buffer. These are two separate variables on different cache
lines. With acquire/release, there is no cross-variable ordering
guarantee: on ARM64, both sides can see stale values simultaneously,
causing a lost wakeup that orphans a task forever.

Sequential consistency (`.seq_cst`) provides a total order that prevents
this: at least one side is guaranteed to see the other's write. This is
the same approach used by crossbeam-channel's `SyncWaker::is_empty` flag.
On x86 (TSO), the overhead is negligible (~1 cycle MFENCE on stores).
On ARM64, it adds a DMB barrier that is essential for correctness.

For a detailed walkthrough with step-by-step diagrams, see
[Channel Wakeup Protocol](/v1.0.0-zig0.15.2/design/channel-wakeup-protocol/).

The wakeup avoids use-after-free by copying the waker function pointer and context **before** setting the `status` flag. A lock-free single-waiter fast slot (`fast_recv_waiter`) avoids the mutex entirely for the common case of one blocked consumer:

```zig
fn wakeOneRecvWaiter(self: *Self) void {
    // Fast path: atomic swap on single-waiter slot (avoids mutex)
    const fast_ptr = self.fast_recv_waiter.swap(0, .acq_rel);
    if (fast_ptr != 0) {
        const w: *RecvWaiter = @ptrFromInt(fast_ptr);
        const waker_fn = w.waker;
        const waker_ctx = w.waker_ctx;
        w.status.store(WAITER_COMPLETE, .release); // Owner may destroy after this
        // ... clear flag under mutex if no list waiters remain ...
        if (waker_fn) |wf| if (waker_ctx) |ctx| wf(ctx);
        return;
    }

    // Slow path: pop from linked list under mutex
    self.waiter_mutex.lock();
    const waiter = self.recv_waiters.popFront();
    // ... clear has_recv_waiters flag if no waiters remain ...
    self.waiter_mutex.unlock();

    if (waiter) |w| {
        const waker_fn = w.waker;
        const waker_ctx = w.waker_ctx;
        w.status.store(WAITER_COMPLETE, .release); // Owner may destroy after this
        if (waker_fn) |wf| if (waker_ctx) |ctx| wf(ctx);
    }
}
```

## Comparison with Other Approaches

| Approach | Send | Recv | Bounded | Lock-free |
|----------|------|------|---------|-----------|
| Vyukov bounded (Volt) | O(1) CAS | O(1) CAS | Yes | Yes (data path) |
| Mutex + VecDeque | O(1) + lock | O(1) + lock | Optional | No |
| Michael-Scott queue | O(1) CAS + alloc | O(1) CAS + free | No | Yes |
| LCRQ (linked CRQs) | O(1) CAS | O(1) CAS | No | Yes |

The Vyukov bounded queue is ideal for Volt's use case:

* **Bounded**: provides backpressure (senders wait when full), preventing unbounded memory growth.
* **Lock-free data path**: no mutex on send/receive, only on waiter management.
* **No allocation per operation**: the ring buffer is pre-allocated.
* **Efficient under both low and high contention**: low contention means CAS succeeds on first try; high contention means retry loops converge quickly because successful CAS operations advance the position.

### Source Files

* Channel implementation: `src/channel/Channel.zig`

## References

* Dmitry Vyukov, "Bounded MPMC Queue," *1024cores.net*, 2010. [http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue](http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue)
* crossbeam-channel (Rust), which uses the same Vyukov bounded queue: [https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel)
* Maurice Herlihy and Nir Shavit, *The Art of Multiprocessor Programming*, Morgan Kaufmann, 2012 — Chapter 10 (concurrent queues).

---
title: "Channel Wakeup Protocol"
description: "How Volt's MPMC channel avoids lost wakeups between lock-free producers/consumers and blocking waiters, and why sequential consistency is required."
---

Volt's `Channel(T)` has two layers: a **lock-free Vyukov ring buffer** for
the data path, and a **mutex-protected waiter list** for tasks that must
block when the channel is full or empty. Bridging these two layers without
losing wakeups is the hardest part of the design.

This page explains the wakeup protocol, the subtle ordering bug that
caused intermittent deadlocks on ARM64, and how it was fixed.

## The Two-Layer Architecture

```
                    ┌─────────────────────────────────┐
                    │  Lock-Free Ring Buffer (Vyukov)  │
                    │  trySend() / tryRecv()           │
                    │  CAS on head/tail                │
                    │  Per-slot sequence numbers        │
                    └──────────┬──────────────────────-┘
                               │
                    ┌──────────▼──────────────────────-┐
                    │  Waiter Layer (mutex-protected)   │
                    │  sendWait() / recvWait()          │
                    │  has_send_waiters flag (atomic)   │
                    │  has_recv_waiters flag (atomic)   │
                    │  Intrusive linked lists           │
                    └─────────────────────────────────-┘
```

The fast path (`trySend`/`tryRecv`) never takes the mutex. After
successfully sending or receiving, it checks an atomic boolean flag
(`has_recv_waiters` or `has_send_waiters`) to decide whether to wake
a blocked task. This avoids mutex contention on every operation.

The slow path (`sendWait`/`recvWait`) takes the mutex to add a waiter to
the list. But it must coordinate with the fast path to avoid a window
where both sides miss each other.

## The Lost Wakeup Problem

Consider a producer (P) calling `trySend` while a consumer (C) calls
`recvWait` concurrently on an empty channel:

```
Producer P                          Consumer C
─────────                           ──────────
                                    tryRecv() → .empty

                                    lock(waiter_mutex)
trySend():
  write value to slot
  slot.sequence.store(...)
                                    has_recv_waiters.store(true)
  // Check for waiters
  has_recv_waiters.load() → ???
                                    tryRecv() → gets value (re-check)
                                    unlock(waiter_mutex)
```

If the producer's load of `has_recv_waiters` sees `false` (the old value),
it skips the wakeup. But the consumer's re-check under the lock succeeds
(it gets the value), so no waiter is actually added. In this case, no
wakeup is lost — the consumer got the value from the re-check.

The dangerous scenario is when there **are** other waiters already queued:

```
Producer P                          Consumer C (waiter already queued)
─────────                           ──────────
trySend():
  write value to slot
  slot.sequence.store(...)
                                    (C is blocked, waiting for wakeup)
  has_recv_waiters.load() → false   // STALE! C is waiting!
  // Skip wakeup — C is orphaned forever
```

This happens when the flag store and the flag load are reordered by the
CPU's memory model.

## The Dekker Pattern

The protocol between the fast path and slow path is a classic **Dekker
pattern** — two threads communicate through two separate variables, and
each thread writes one variable then reads the other:

```
Thread A (fast path):              Thread B (slow path):
  write buffer (data)                write has_waiters = true (flag)
  read has_waiters (flag)            read buffer (data)
```

For correctness, at least one thread must see the other's write. If both
threads see stale values (producer sees `has_waiters=false`, consumer sees
`buffer=empty`), the wakeup is lost.

This is the **exact** problem that the Dekker mutual exclusion algorithm
faces, and it has the **exact** same solution: **sequential consistency**.

### Why Acquire/Release Is Not Enough

With acquire/release ordering:
- Thread A's `.release` store to the buffer creates a happens-before edge
  **only** with a corresponding `.acquire` load of the **same** variable.
- Thread B's `.release` store to the flag creates a happens-before edge
  **only** with a corresponding `.acquire` load of the **same** flag.

But there is **no happens-before relationship between the buffer and the
flag**. They are on different cache lines, different variables. The CPU
is free to reorder:

```
ARM64 (weakly ordered):

Producer:                          Consumer:
  store buffer [release]             store flag [release]
  load flag [acquire] → false        load buffer [acquire] → empty
  // Both see stale values! Lost wakeup!
```

On ARM64, the store buffer can delay writes to one cache line while
allowing loads from another cache line to proceed. This means the
producer's buffer write might not be visible to the consumer, AND the
consumer's flag write might not be visible to the producer — simultaneously.

### Why x86 Hides This Bug

On x86 (including Windows x86_64), the hardware provides **Total Store
Ordering (TSO)**. Under TSO:
- All stores become visible to all cores in the order they were executed.
- A load cannot be reordered before an earlier store **from the same thread**.
- The only reordering allowed is a load being moved before an earlier store
  to a **different** address (StoreLoad reordering).

In practice, x86's strong guarantees mean that acquire/release is
nearly equivalent to sequential consistency for the Dekker pattern. The
bug was **latent** on x86 but **observable** on ARM64.

This is why the deadlock only manifested on linux-arm64 CI runners, not
on macOS ARM64 (which also has a weak memory model, but the benchmark's
timing made the race window too small to hit consistently) or Windows
x86_64 (which has TSO).

### The Fix: Sequential Consistency

Sequential consistency (`.seq_cst`) provides a **total order** across all
seq_cst operations on all threads. If the producer's flag load and the
consumer's flag store are both seq_cst, the hardware inserts barriers
that prevent the StoreLoad reordering:

```
Producer:                          Consumer:
  store buffer [release]             store flag [seq_cst]  ← DMB barrier
  load flag [seq_cst]  ← DMB        load buffer [acquire]
  // At least one sees the other's write
```

The seq_cst total order guarantees that either:
1. The producer's load sees `true` (flag was set before load in total order), OR
2. The consumer's re-check sees the value (buffer was written before re-check in total order)

In both cases, no wakeup is lost.

## The Complete Protocol

Here is the full wakeup protocol with the correct orderings:

### Fast Path (trySend)

```zig
pub fn trySend(self: *Self, value: T) SendResult {
    // ... CAS loop to claim slot ...

    // Publish value (acquire/release with slot sequence)
    slot.value = value;
    slot.sequence.store(tail +% 1, .release);

    // Dekker check: seq_cst load pairs with seq_cst store in recvWait
    if (self.has_recv_waiters.load(.seq_cst)) {
        self.wakeOneRecvWaiter();
    }
    return .ok;
}
```

### Slow Path (recvWait)

```zig
pub fn recvWait(self: *Self, waiter: *RecvWaiter) ?T {
    // Fast path attempt (lock-free)
    if (self.tryRecv()) |v| return v;

    // Slow path: must register waiter
    self.waiter_mutex.lock();

    // Dekker write: seq_cst store pairs with seq_cst load in trySend.
    // MUST come BEFORE the re-check to ensure the producer sees the flag
    // if it sends after this point.
    self.has_recv_waiters.store(true, .seq_cst);

    // Re-check: a sender may have added a value between our tryRecv
    // and our flag store. This closes the race window.
    if (self.tryRecv()) |v| {
        // Got value — clear flag if no other waiters
        // (checks both the fast slot and the linked list)
        if (self.noRecvWaiters()) {
            self.has_recv_waiters.store(false, .seq_cst);
        }
        self.waiter_mutex.unlock();
        return v;
    }

    // Still empty — try single-waiter fast slot first, then linked list
    waiter.status.store(WAITER_PENDING, .release);
    if (self.fast_recv_waiter.cmpxchgStrong(0, @intFromPtr(waiter), .release, .monotonic) == null) {
        self.waiter_mutex.unlock();
        return null;
    }
    self.recv_waiters.pushBack(waiter);
    self.waiter_mutex.unlock();
    return null;  // Caller will yield
}
```

### Why the Re-check Matters

The re-check under the lock is essential. Without it:

```
Consumer:                          Producer:
  tryRecv() → empty                  trySend():
  lock()                               write value
  has_recv_waiters.store(true)          has_recv_waiters.load() → true
  // NO re-check                        wakeOneRecvWaiter() → pops waiter
  pushBack(waiter)                    // Waiter was woken before it was added!
  unlock()
```

With the re-check, the consumer sees the value that was sent between the
initial `tryRecv` and the lock acquisition, and returns it directly
instead of adding a waiter.

## Visual Walkthrough: The Race

Here is a step-by-step walkthrough of the race that caused the ARM64
deadlock, and how seq_cst prevents it.

### Before the Fix (acquire/release)

```
Time    Producer (trySend)              Consumer (recvWait)
────    ──────────────────              ───────────────────
 T1     write slot.seq [release]
 T2                                     tryRecv() → empty
 T3                                     lock(mutex)
 T4                                     flag.store(true) [release]
 T5     flag.load() [acquire]
        → false !! (ARM reordered)      tryRecv() → empty
 T6     return .ok                      pushBack(waiter)
 T7     // No wakeup sent               unlock(mutex)
 T8                                     // Waiter stuck forever ☠
```

At T5, the producer's acquire load can see the stale `false` because on
ARM64, the consumer's release store to the flag (T4) has no ordering
relationship with the producer's release store to the slot (T1). They are
independent release/acquire pairs on different variables.

### After the Fix (seq_cst)

```
Time    Producer (trySend)              Consumer (recvWait)
────    ──────────────────              ───────────────────
 T1     write slot.seq [release]
 T2                                     tryRecv() → empty
 T3                                     lock(mutex)
 T4                                     flag.store(true) [seq_cst]
                                        ← DMB ISH barrier
 T5     flag.load() [seq_cst]
        ← DMB ISH barrier
        → true ✓                        tryRecv() → empty
 T6     wakeOneRecvWaiter()             pushBack(waiter)
 T7     // Waiter woken ✓               unlock(mutex)
```

The seq_cst total order forces T4's store to be visible before T5's load
(or T5 comes first in the total order, in which case the consumer's
re-check at T6 would see the value written at T1). Either way, no
wakeup is lost.

## Cross-Platform Behavior

| Architecture | Memory Model | Bug Observable? | seq_cst Overhead |
|-------------|-------------|-----------------|-----------------|
| x86_64 (Intel/AMD) | TSO (strong) | No* | ~1 cycle (MFENCE on stores) |
| ARM64 (Apple M-series) | Weak | Rare (timing-dependent) | DMB ISH barrier |
| ARM64 (Linux/Ampere) | Weak | Yes (reproduced in CI) | DMB ISH barrier |
| ARM64 (Windows/Qualcomm) | Weak | Yes (same ISA) | DMB ISH barrier |
| RISC-V | Weak (RVWMO) | Yes | FENCE instructions |

*x86's TSO prevents the StoreLoad reordering that triggers this bug.
However, the x86 memory model **does** allow StoreLoad reordering in
theory (loads can bypass earlier stores to different addresses). The
MFENCE instruction that seq_cst adds on stores closes this gap.

**Windows note:** Windows on x86_64 is safe due to TSO. Windows on ARM64
(Surface Pro, Snapdragon laptops) would be affected by the same bug.
The seq_cst fix ensures correctness on all platforms.

## Crossbeam-Channel: The Reference

Volt's wakeup protocol directly mirrors crossbeam-channel's `SyncWaker`
in Rust, which solves the same problem with the same approach:

```rust
// crossbeam-channel: src/waker.rs
impl SyncWaker {
    /// Registers a waiter (sets the "someone is waiting" flag)
    pub fn watch(&self, oper: Operation) {
        // ...
        self.is_empty.store(false, Ordering::SeqCst);  // ← seq_cst!
    }

    /// Checks if anyone is waiting
    pub fn notify(&self) {
        if !self.is_empty.load(Ordering::SeqCst) {     // ← seq_cst!
            // ... wake the waiter
        }
    }
}
```

Crossbeam-channel uses `SeqCst` for its `is_empty` flag for exactly the
same reason: it's a Dekker-style protocol between the lock-free data path
(which calls `notify()` after sending) and the blocking path (which calls
`watch()` before blocking). Using `Acquire`/`Release` would risk lost
wakeups on ARM64.

## Summary

| Aspect | Detail |
|--------|--------|
| **Pattern** | Dekker protocol (flag + data on separate variables) |
| **Required ordering** | Sequential consistency (`.seq_cst`) |
| **Affected variables** | `has_recv_waiters`, `has_send_waiters` |
| **Operations upgraded** | 15 total (2 loads in fast path, 13 stores in slow path) |
| **Symptom** | Intermittent deadlock on ARM64 (14/15 MPMC rounds pass, 15th hangs) |
| **Root cause** | `.acquire`/`.release` on separate variables has no cross-variable ordering guarantee |
| **Reference** | crossbeam-channel `SyncWaker::is_empty` uses `SeqCst` for the same reason |
| **Verification** | CI linux-arm64 benchmark now passes consistently |

### Source Files

- Channel implementation: `src/channel/Channel.zig`
- Vyukov algorithm details: [Vyukov MPMC Queue](/algorithms/vyukov-mpmc/)
- Memory ordering reference: [Memory Ordering](/design/memory-ordering/)

### References

- Leslie Lamport, "How to Make a Multiprocessor Computer That Correctly Executes Multiprocess Programs," *IEEE Transactions on Computers*, 1979 — original definition of sequential consistency.
- Edsger W. Dijkstra, "Solution of a Problem in Concurrent Programming Control," *Communications of the ACM*, 1965 — the Dekker/Peterson mutual exclusion problem that motivates seq_cst.
- crossbeam-channel `SyncWaker`: [https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-channel/src/waker.rs](https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-channel/src/waker.rs)
- ARM Architecture Reference Manual, section B2.3 — definition of the ARM64 weak memory model.

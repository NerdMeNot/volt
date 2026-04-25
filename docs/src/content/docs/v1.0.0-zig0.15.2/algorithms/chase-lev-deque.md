---
title: Chase-Lev Work-Stealing Deque
description: The lock-free ring buffer at the heart of Volt's per-worker task
  queue, based on the Chase-Lev deque algorithm with a packed dual-head design.
slug: v1.0.0-zig0.15.2/algorithms/chase-lev-deque
---

Each worker in Volt owns a `WorkStealQueue` -- a 256-slot lock-free ring buffer based on the Chase-Lev work-stealing deque. The owner thread has fast, uncontested access for push and pop operations, while other threads can safely steal half the queue without locks.

## The Chase-Lev Algorithm

The Chase-Lev deque (David Chase and Yossi Lev, 2005) is the standard data structure for work-stealing schedulers. It provides:

* **Push (owner only):** O(1) amortized, wait-free in the common case
* **Pop (owner only):** O(1), lock-free (CAS may retry on steal contention)
* **Steal (any thread):** O(1), lock-free (single CAS to claim tasks)

The key insight is that the owner thread and stealers operate on opposite ends of the deque, so they rarely contend. The owner pushes and pops from the **back** (tail), while stealers take from the **front** (head).

```
            stealers                              owner
               |                                    |
               v                                    v
         +---+---+---+---+---+---+---+---+---+
 head -> | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |   <- tail
         +---+---+---+---+---+---+---+---+---+
         FIFO (steal from front)    LIFO (push/pop at back)
```

In Volt's variant, the local queue is used in **FIFO** order (pop from head) for fairness, with LIFO behavior provided by a separate `lifo_slot` atomic pointer on the `Worker` struct. This separation simplifies the ring buffer logic.

## The Packed Dual-Head Design

The standard Chase-Lev deque uses separate atomic variables for head and tail. Volt packs **two head values** into a single 64-bit atomic to support a three-phase steal protocol:

```
head (u64) = [ steal_head (upper 32 bits) | real_head (lower 32 bits) ]
tail (u32) = separate atomic, only written by owner
```

* **`real_head`**: The true consumer position. The owner pops from here.
* **`steal_head`**: Tracks how far stealers have confirmed their steals. During a steal, `steal_head` lags behind `real_head`.
* When `steal_head == real_head`: No steal in progress.
* When `steal_head != real_head`: A steal is active. The stealer has claimed slots `[steal_head, real_head)` but has not yet finished copying them.

This design enables the three-phase steal protocol without requiring additional synchronization.

```zig
pub const WorkStealQueue = struct {
    pub const CAPACITY: u32 = 256;
    const MASK: u32 = CAPACITY - 1;

    head: std.atomic.Value(u64) align(CACHE_LINE_SIZE),
    tail: std.atomic.Value(u32) align(CACHE_LINE_SIZE),
    buffer: [CAPACITY]std.atomic.Value(?*Header) align(CACHE_LINE_SIZE),

    pub inline fn pack(steal: u32, real: u32) u64 {
        return (@as(u64, steal) << 32) | @as(u64, real);
    }

    pub inline fn unpack(val: u64) struct { steal: u32, real: u32 } {
        return .{
            .steal = @truncate(val >> 32),
            .real = @truncate(val),
        };
    }
};
```

## Owner Operations

### Push (to back)

The owner pushes a new task to the tail of the ring buffer. This is wait-free in the common case (just a store and tail increment). If the buffer is full, the owner overflows half the queue to the global queue.

```zig
pub fn push(self: *Self, task: *Header, global_queue: *GlobalTaskQueue) void {
    var tail = self.tail.load(.monotonic);

    while (true) {
        const head_packed = self.head.load(.acquire);
        const head = unpack(head_packed);

        if (tail -% head.steal < CAPACITY) {
            // Room available -- write and advance tail
            self.buffer[tail & MASK].store(task, .release);
            self.tail.store(tail +% 1, .release);
            return;
        }

        if (head.steal != head.real) {
            // Stealer active -- can't safely overflow, push to global
            global_queue.push(task);
            return;
        }

        // No stealer -- overflow half to global queue, then retry
        const half = CAPACITY / 2;
        const new_head = pack(head.steal +% half, head.real +% half);
        if (self.head.cmpxchgWeak(head_packed, new_head, .release, .acquire)) |_| {
            tail = self.tail.load(.monotonic);
            continue;
        }

        // Push overflowed tasks to global queue
        var i: u32 = 0;
        while (i < half) : (i += 1) {
            const idx = (head.real +% i) & MASK;
            const t = self.buffer[idx].load(.acquire) orelse continue;
            global_queue.push(t);
        }

        self.buffer[tail & MASK].store(task, .release);
        self.tail.store(tail +% 1, .release);
        return;
    }
}
```

**Overflow handling:** When the queue is full and no stealer is active, the owner atomically advances both heads by `CAPACITY / 2 = 128`, effectively discarding the oldest 128 entries from the ring buffer. Those 128 tasks are then pushed to the global queue in a batch. This amortizes the cost of global queue mutex acquisition over 128 tasks.

If a stealer is active (heads differ), the owner cannot safely overflow because the stealer is still reading from those slots. In this case, the single new task goes directly to the global queue.

### Pop (from front, FIFO)

The owner pops from the head for FIFO ordering. This ensures tasks are executed in roughly the order they were enqueued, providing fairness. LIFO behavior (executing the newest task first for cache locality) is handled by the separate `lifo_slot` on the Worker struct.

```zig
pub fn pop(self: *Self) ?*Header {
    var head_packed = self.head.load(.acquire);

    while (true) {
        const head = unpack(head_packed);
        const tail = self.tail.load(.monotonic);

        if (head.real == tail) return null;  // Empty

        const idx = head.real & MASK;
        const next_real = head.real +% 1;
        const new_head = if (head.steal == head.real)
            pack(next_real, next_real)      // No stealer: advance both
        else
            pack(head.steal, next_real);    // Stealer active: only advance real

        if (self.head.cmpxchgWeak(head_packed, new_head, .release, .acquire)) |actual| {
            head_packed = actual;
            continue;  // Lost race with stealer, retry
        }

        return self.buffer[idx].load(.acquire);
    }
}
```

## Stealer Operations

### Three-Phase Steal Protocol

Stealing is the most complex operation. It transfers half of the victim's queue into the stealer's queue. The protocol uses three CAS operations to ensure correctness without locks.

```
Phase 1 (Claim):     Advance victim's real_head, keep steal_head
Phase 2 (Copy):      Copy tasks from victim's buffer to stealer's buffer
Phase 3 (Release):   Advance victim's steal_head to match real_head

Victim's head during steal:

  Before:   [steal=5 | real=5]     (no steal in progress)
  Phase 1:  [steal=5 | real=9]     (claimed 4 tasks: slots 5,6,7,8)
  Phase 2:  (copy slots 5,6,7,8 to stealer's buffer)
  Phase 3:  [steal=9 | real=9]     (steal complete)
```

The full implementation:

```zig
pub fn stealInto(src: *Self, dst: *Self) ?*Header {
    const dst_tail = dst.tail.load(.monotonic);
    const src_head_packed = src.head.load(.acquire);
    const src_head = unpack(src_head_packed);

    // A steal is already in progress
    if (src_head.steal != src_head.real) return null;

    const src_tail = src.tail.load(.acquire);
    const available = src_tail -% src_head.real;
    if (available == 0) return null;

    // Steal half (ceiling), capped to CAPACITY/2
    var to_steal = available -% (available / 2);
    if (to_steal > CAPACITY / 2) to_steal = CAPACITY / 2;

    // Phase 1: CAS to claim -- advance real_head, keep steal_head
    const new_src_head = pack(src_head.steal, src_head.real +% to_steal);
    if (src.head.cmpxchgWeak(src_head_packed, new_src_head, .acq_rel, .acquire)) |_| {
        return null;  // Lost race
    }

    // Phase 2: Copy tasks
    const first_task = src.buffer[src_head.real & MASK].load(.acquire);
    var i: u32 = 1;
    while (i < to_steal) : (i += 1) {
        const src_idx = (src_head.real +% i) & MASK;
        const dst_idx = (dst_tail +% (i - 1)) & MASK;
        const task = src.buffer[src_idx].load(.acquire);
        dst.buffer[dst_idx].store(task, .release);
    }
    if (to_steal > 1) {
        dst.tail.store(dst_tail +% (to_steal - 1), .release);
    }

    // Phase 3: Release -- advance steal_head to match real_head
    var release_head_packed = src.head.load(.acquire);
    while (true) {
        const release_head = unpack(release_head_packed);
        const final_head = pack(release_head.real, release_head.real);
        if (src.head.cmpxchgWeak(
            release_head_packed, final_head, .acq_rel, .acquire
        )) |actual| {
            release_head_packed = actual;
            continue;
        }
        break;
    }

    return first_task;
}
```

**Why three phases?** A single CAS cannot atomically claim, copy, and release. The two-head design allows the claim and release to be separate atomic operations, with the copy happening in between. During the copy phase, the victim's `push` can still proceed as long as the tail does not wrap past `steal_head`. The victim's `pop` CAS may fail and retry, but this is safe.

## Memory Ordering Rationale

Each atomic operation uses the minimum ordering needed for correctness:

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| `tail.load` (owner) | `monotonic` | Only the owner writes tail; reading own writes needs no fence |
| `tail.store` (owner) | `release` | Stealers must see the buffer write before the tail update |
| `head.load` (owner or stealer) | `acquire` | Must see all buffer writes that happened before the head was updated |
| `head.cmpxchgWeak` (claim) | `acq_rel` | Release: publish our claim. Acquire: see current state |
| `buffer[i].store` (push) | `release` | Pair with acquire loads in pop/steal |
| `buffer[i].load` (pop/steal) | `acquire` | See the value written by the pusher |

The `head` and `tail` fields are aligned to separate cache lines (`CACHE_LINE_SIZE`) to prevent false sharing between the owner thread (writing `tail`) and stealer threads (reading/writing `head`).

## The 256-Slot Fixed Ring Buffer

The capacity of 256 is a compile-time constant chosen for several reasons:

1. **Power of two** enables bitwise masking (`idx & 0xFF`) instead of modulo division.
2. **Small enough** to fit in L1/L2 cache (256 pointers = 2KB on 64-bit).
3. **Large enough** to absorb bursts without immediate overflow.
4. **Matches Tokio** which also uses 256 slots, enabling direct performance comparison.

The buffer stores `std.atomic.Value(?*Header)` -- nullable pointers to type-erased task headers. The nullable type allows detecting empty slots.

## Overflow to Global Queue

When the ring buffer is full, the overflow strategy depends on whether a steal is in progress:

```
Queue full, no stealer:
  1. Atomically advance head by CAPACITY/2 (claim 128 tasks)
  2. Push those 128 tasks to global queue (single lock acquisition)
  3. Write new task to buffer, advance tail
  Result: 128 tasks moved to global, buffer half empty

Queue full, stealer active:
  1. Push new task directly to global queue
  Result: Single task to global, no disruption to stealer
```

This design ensures that overflow is always possible and that the global queue absorbs excess work. The batch overflow of 128 tasks amortizes the mutex cost of the global queue, making it negligible per task.

```
Before overflow (full):
+---+---+---+---+---+---+   ...   +---+---+
| 0 | 1 | 2 | 3 | 4 | 5 |        |254|255|
+---+---+---+---+---+---+   ...   +---+---+
  ^                                         ^
  head=0                                    tail=256

After overflow:
+---+---+---+---+---+---+   ...   +---+---+
|   |   |   |128|129|130|        |254|255| NEW
+---+---+---+---+---+---+   ...   +---+---+
              ^                              ^
              head=128                       tail=257

Tasks 0-127 moved to global queue
```

## Integration with the LIFO Slot

The `WorkStealQueue` itself is a FIFO queue. LIFO behavior comes from the separate `lifo_slot` on each `Worker`. When the worker receives a new task (e.g., from a waker callback), the new task goes into the LIFO slot, and any previously held LIFO task is evicted to the ring buffer:

```
Worker receives task C:

Before:  LIFO: [B]    Ring: [... A ...]
After:   LIFO: [C]    Ring: [... A ... B]   (B evicted to ring)
```

This separation means the ring buffer is pure FIFO, stealers always get the oldest tasks (fairest), and the owner always executes the newest task first (best cache locality).

### Source Files

* `WorkStealQueue`: `src/internal/scheduler/Header.zig`
* Worker integration: `src/internal/scheduler/Scheduler.zig`

## References

* David Chase and Yossi Lev, "Dynamic Circular Work-Stealing Deque," *SPAA '05*, 2005. [doi:10.1145/1073970.1073974](https://doi.org/10.1145/1073970.1073974)
* Nhat Minh Lê, Antoniu Pop, Albert Cohen, and Francesco Zappa Nardelli, "Correct and Efficient Work-Stealing for Weak Memory Models," *PPoPP '13*, 2013. [doi:10.1145/2442516.2442524](https://doi.org/10.1145/2442516.2442524)
* Tokio's Chase-Lev implementation: [`tokio/src/runtime/scheduler/multi_thread/queue.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/multi_thread/queue.rs)

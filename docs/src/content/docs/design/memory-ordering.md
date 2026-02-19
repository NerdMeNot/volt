---
title: "Memory Ordering"
description: "Atomic memory orderings used throughout Volt, why each was chosen, and how correctness is verified through loom-style testing."
---

Volt is a concurrent system with multiple worker threads, lock-free
queues, and atomic state machines. Getting memory ordering wrong leads to
bugs that are nearly impossible to reproduce: lost wakeups, stale reads,
torn state, and data corruption under load.

This page documents the ordering choices throughout Volt, explains the
reasoning behind each, and describes how correctness is verified.

## Zig 0.15 Atomic Primitives

Zig 0.15 provides atomics through `std.atomic.Value(T)`:

```zig
const std = @import("std");

var counter = std.atomic.Value(u64).init(0);

// Load with ordering
const val = counter.load(.acquire);

// Store with ordering
counter.store(42, .release);

// Compare-and-swap
const prev = counter.cmpxchgWeak(expected, desired, .acq_rel, .acquire);

// Fetch-and-modify
_ = counter.fetchAdd(1, .acq_rel);
```

### Available Orderings

| Ordering | Zig Name | Guarantees |
|----------|----------|-----------|
| Relaxed | `.monotonic` | Atomicity only. No ordering relative to other operations. |
| Acquire | `.acquire` | Subsequent reads/writes cannot be reordered before this load. |
| Release | `.release` | Preceding reads/writes cannot be reordered after this store. |
| Acquire-Release | `.acq_rel` | Both acquire and release. For read-modify-write operations. |
| Sequentially Consistent | `.seq_cst` | Total order across all SeqCst operations on all threads. |

### No @fence Builtin

Zig 0.15 does not have a `@fence` builtin. When a standalone fence is
needed, the idiom is a no-op atomic read-modify-write with `.seq_cst`:

```zig
// Equivalent to a SeqCst fence
_ = some_atomic.fetchAdd(0, .seq_cst);
```

In practice, Volt avoids standalone fences. Every ordering is attached
to the atomic operation that logically requires it.

---

## The Packed Atomic State Pattern

The most critical atomic in Volt is the task `Header.state` -- a single
64-bit word that packs the entire task state:

```
 63                  40  39       32  31     24  23     16  15      0
+----------------------+-----------+---------+-----------+----------+
|    ref_count (24)    | shield(8) | flags(8)| lifecycle | reserved |
+----------------------+-----------+---------+-----------+----------+
```

### Why Pack Into One Word?

Multiple related fields must be updated atomically. If `lifecycle` and
`notified` were separate atomics, a thread could observe inconsistent state:

```
// BROKEN: separate atomics
Thread A (waker):                   Thread B (scheduler):
  read lifecycle → RUNNING            poll() returns .pending
  set notified = true                 set lifecycle = IDLE
                                      read notified → false  // LOST WAKEUP
```

By packing everything into a single `u64`, a CAS operation updates all
fields atomically:

```zig
pub const State = packed struct(u64) {
    _reserved: u16 = 0,
    lifecycle: Lifecycle = .idle,
    notified: bool = false,
    cancelled: bool = false,
    join_interest: bool = false,
    detached: bool = false,
    _flag_reserved: u4 = 0,
    shield_depth: u8 = 0,
    ref_count: u24 = 1,
};
```

### CAS Loops on the State Word

All state transitions use CAS loops with `acq_rel` success ordering and
`acquire` failure ordering:

```zig
pub fn transitionToScheduled(self: *Header) bool {
    var current = State.fromU64(self.state.load(.acquire));

    while (true) {
        if (current.lifecycle == .complete) return false;

        if (current.lifecycle == .idle) {
            var new = current;
            new.lifecycle = .scheduled;
            new.notified = false;

            if (self.state.cmpxchgWeak(
                current.toU64(),
                new.toU64(),
                .acq_rel,   // success: publish new state
                .acquire,   // failure: acquire latest state
            )) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return true;
        }

        // RUNNING or SCHEDULED: set notified
        var new = current;
        new.notified = true;
        if (self.state.cmpxchgWeak(
            current.toU64(), new.toU64(), .acq_rel, .acquire,
        )) |updated| {
            current = State.fromU64(updated);
            continue;
        }
        return false;
    }
}
```

**Why `acq_rel` on success?**

- The **release** component ensures that all writes done before the
  transition (e.g., storing the future's result) are visible to the thread
  that later reads the state.
- The **acquire** component ensures that the thread sees the latest state
  written by other threads before making its decision.

**Why `acquire` on failure?**

When CAS fails, we need to see the current value written by whoever won
the race. `acquire` ensures we see their writes.

### Reference Counting

Reference count operations use `fetchAdd` and `fetchSub` with `acq_rel`:

```zig
pub fn ref(self: *Header) void {
    _ = self.state.fetchAdd(State.REF_ONE, .acq_rel);
}

pub fn unref(self: *Header) bool {
    const prev = self.state.fetchSub(State.REF_ONE, .acq_rel);
    const prev_state = State.fromU64(prev);
    return prev_state.ref_count == 1;  // Was this the last ref?
}
```

**Why `acq_rel`?**

- **Release**: When decrementing, all prior accesses to the task (writing
  results, modifying state) must be visible to the thread that drops the
  last reference and frees the memory.
- **Acquire**: When the last reference is detected (`ref_count == 1`), the
  thread must see all writes from other threads before calling `drop()`.

This is the standard pattern for atomic reference counting. Using `.monotonic`
would risk a use-after-free where one thread frees memory while another
still has stale cached data from the task.

---

## Release/Acquire Pairs

The most common ordering pattern in Volt is the release/acquire pair,
used whenever one thread publishes data and another thread consumes it.

### Semaphore Permit Handoff

When the semaphore grants permits to a waiter, the granter stores with
`.release` and the waiter loads with `.acquire`:

```zig
// In Semaphore.releaseToWaiters() -- under mutex, granting permits
waiter.state.store(remaining - assign, .release);

// In Waiter.isReady() -- polling from another thread
return self.state.load(.acquire) == 0;
```

**Why this pairing?**

The `.release` store ensures that all modifications to shared state done
before granting the permit (e.g., updating the protected data) are visible
to the waiter when it sees `state == 0`. The `.acquire` load ensures the
waiter sees those prior writes.

### Semaphore tryAcquire (Lock-Free CAS)

The uncontended fast path uses lock-free CAS:

```zig
pub fn tryAcquire(self: *Self, num: usize) bool {
    var current = self.permits.load(.acquire);
    while (true) {
        if (current < num) return false;

        if (self.permits.cmpxchgWeak(
            current,
            current - num,
            .acq_rel,     // success
            .acquire,     // failure
        )) |new_val| {
            current = new_val;
            continue;
        }
        return true;
    }
}
```

**Why `.acquire` on the initial load?**

We need to see the latest permit count published by `release()`. A
`.monotonic` load might see a stale value, causing us to either fail
spuriously (seeing fewer permits than available) or succeed on a stale
value and corrupt the count.

### WorkStealQueue Buffer Operations

The lock-free ring buffer uses release/acquire on buffer slots:

```zig
// Owner writes to tail (producer)
self.buffer[tail & MASK].store(task, .release);
self.tail.store(tail +% 1, .release);

// Stealer reads from head (consumer)
const task = src.buffer[src_head.real & MASK].load(.acquire);
```

The `.release` store on the buffer slot ensures the task data is fully
written before the slot becomes visible. The `.acquire` load ensures the
stealer sees the complete task data.

---

## Sequential Consistency (SeqCst)

SeqCst provides a total order across all SeqCst operations on all threads.
It is the most expensive ordering and is used sparingly, only where the
simpler orderings are insufficient.

### Idle State Coordination

The scheduler's `IdleState` uses SeqCst for all operations on
`num_searching`, `num_parked`, and `parked_bitmap`:

```zig
pub const IdleState = struct {
    num_searching: std.atomic.Value(u32) = ...,
    num_parked: std.atomic.Value(u32) = ...,
    parked_bitmap: std.atomic.Value(u64) = ...,
};

fn notifyShouldWakeup(self: *const IdleState) bool {
    return self.num_searching.load(.seq_cst) == 0 and
        self.num_parked.load(.seq_cst) > 0;
}

pub fn transitionWorkerFromSearching(self: *IdleState) bool {
    const prev = self.num_searching.fetchSub(1, .seq_cst);
    return prev == 1;
}
```

**Why SeqCst here?**

The "last searcher must notify" protocol requires a total order between:

1. A worker decrementing `num_searching` to 0
2. A task producer reading `num_searching == 0` and deciding to wake a worker

With `acq_rel`, these operations on different atomics (`num_searching` and
`parked_bitmap`) have no guaranteed relative ordering. Thread A might
decrement `num_searching` while Thread B reads the old value and decides
not to wake anyone -- a lost wakeup.

SeqCst establishes a total order: if Thread A's decrement happens before
Thread B's load in the SeqCst total order, Thread B sees 0 and wakes a
worker. If Thread B's load happens first, Thread A was still searching
and will find the work.

This is the same pattern Tokio uses for its idle coordination, and for the
same reason.

### Channel Waiter Flags (Dekker Pattern)

The MPMC channel's `has_recv_waiters` and `has_send_waiters` flags use
SeqCst for all operations:

```zig
// Fast path (trySend) — after writing value to ring buffer:
if (self.has_recv_waiters.load(.seq_cst)) {
    self.wakeOneRecvWaiter();
}

// Slow path (recvWait) — under waiter_mutex, before re-checking buffer:
self.has_recv_waiters.store(true, .seq_cst);
const retry = self.tryRecv();  // Re-check under lock
```

**Why SeqCst here?**

This is a **Dekker pattern**: the fast path writes the buffer (variable A)
then reads the flag (variable B), while the slow path writes the flag
(variable B) then reads the buffer (variable A). With acquire/release,
these operations on different variables have no mutual ordering guarantee.
On ARM64, both sides can see stale values simultaneously:

```
Producer: store buffer [release] → load flag [acquire] → false (STALE!)
Consumer: store flag [release]   → load buffer [acquire] → empty (STALE!)
// Both miss each other — lost wakeup, task orphaned forever
```

SeqCst establishes a total order: the flag store and flag load are ordered
relative to each other, guaranteeing that at least one side sees the
other's write. This is the same pattern crossbeam-channel uses for its
`SyncWaker::is_empty` flag.

This bug caused an intermittent deadlock on ARM64 (14 of 15 MPMC benchmark
rounds passed, the 15th hung). On x86 (TSO), the bug was latent because
the hardware provides near-sequential-consistency for stores. See
[Channel Wakeup Protocol](/design/channel-wakeup-protocol/) for the full
analysis.

### Bitmap Operations

The parked worker bitmap uses SeqCst for claim operations:

```zig
pub fn claimParkedWorker(self: *IdleState) ?usize {
    while (true) {
        const bitmap = self.parked_bitmap.load(.seq_cst);
        if (bitmap == 0) return null;

        const worker_id: usize = @ctz(bitmap);  // O(1) lookup
        const mask: u64 = @as(u64, 1) << @intCast(worker_id);

        const prev = self.parked_bitmap.fetchAnd(~mask, .seq_cst);
        if (prev & mask == 0) continue;  // Lost race, retry

        _ = self.num_parked.fetchSub(1, .seq_cst);
        _ = self.num_searching.fetchAdd(1, .seq_cst);

        return worker_id;
    }
}
```

**Why SeqCst for the bitmap?**

The bitmap, `num_parked`, and `num_searching` must be mutually consistent.
A thread that clears a bitmap bit must also see the updated `num_parked`.
SeqCst ensures these operations are visible in a consistent order to all
threads.

---

## Monotonic (Relaxed) Ordering

Monotonic ordering provides atomicity but no ordering guarantees relative
to other operations. It is the cheapest ordering and is used when:

1. The value is only used for statistics/diagnostics
2. The value is protected by a mutex (the mutex provides the ordering)
3. The value is used as a hint, not for correctness

### Statistics Counters

```zig
_ = self.total_spawned.fetchAdd(1, .monotonic);
_ = self.timers_processed.fetchAdd(count, .monotonic);
```

Statistics are approximate by nature. Using SeqCst for a counter that is
only read for debugging would waste cycles.

### Waiter State Under Mutex

When the semaphore modifies a waiter's state under the mutex, it uses
`.monotonic` for the load (since the mutex provides ordering) and
`.release` for the store (since the polling thread reads without the mutex):

```zig
// Under mutex -- only one writer
const remaining = waiter.state.load(.monotonic);

// .release ensures the polling thread's .acquire load sees this
waiter.state.store(remaining - assign, .release);
```

The `.monotonic` load is safe because the mutex's lock acquisition provides
an acquire barrier, and only the mutex holder modifies `waiter.state`.

### WorkStealQueue Tail (Owner-Only)

The tail of the work-steal queue is only modified by the owning worker:

```zig
var tail = self.tail.load(.monotonic);
// ... only the owner writes to tail
```

The owner can use `.monotonic` because no other thread modifies `tail`.
Other threads read `tail` with `.acquire` to see the owner's writes.

---

## Common Patterns

### CAS Loop

The standard pattern for atomic state transitions:

```zig
var current = atomic.load(.acquire);
while (true) {
    // Compute desired state from current
    var new = current;
    new.field = new_value;

    if (atomic.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
        current = State.fromU64(updated);
        continue;  // Retry with latest value
    }
    break;  // Success
}
```

`cmpxchgWeak` is preferred over `cmpxchgStrong` because:

- It can fail spuriously (returning the current value even if it matches
  `expected`), which is fine because we retry anyway.
- On ARM and other weakly-ordered architectures, `cmpxchgWeak` compiles to
  a single LL/SC pair without a retry loop, which is faster.

### Publish/Consume with Release/Acquire

```zig
// Producer: write data, then publish
data.* = computed_value;
flag.store(true, .release);      // Publishes data write

// Consumer: acquire flag, then read data
if (flag.load(.acquire)) {       // Acquires data write
    use(data.*);                 // Guaranteed to see computed_value
}
```

### Lock-Free Emptiness Check

The global task queue uses an atomic length counter for lock-free emptiness
checks, avoiding the mutex when the queue is empty:

```zig
// Push: increment under lock
_ = self.atomic_len.fetchAdd(1, .release);

// Check: lock-free read
pub fn isEmptyFast(self: *const GlobalTaskQueue) bool {
    return self.atomic_len.load(.acquire) == 0;
}
```

The `.release` on push ensures the task data is visible. The `.acquire` on
the emptiness check ensures we see the latest count. If the count is 0,
we avoid taking the mutex entirely.

---

## The Wakeup Protocol: A Case Study

The most subtle ordering requirement in Volt is the wakeup protocol.
Here is the full sequence:

```
Thread A (waker):                     Thread B (scheduler):
                                        poll() returns .pending
  [1] header.transitionToScheduled()
      load state (.acquire)             [2] header.transitionToIdle()
      if lifecycle == RUNNING:              load state (.acquire)
        CAS: set notified (.acq_rel)        CAS: lifecycle=IDLE, notified=false (.acq_rel)
        return false                        return prev_state

                                        [3] if prev.notified:
                                              reschedule task
```

**The race:** Steps [1] and [2] can execute concurrently. The CAS
operations ensure that exactly one of two outcomes occurs:

1. **Thread A's CAS succeeds first:** Thread A sets `notified=true` on the
   RUNNING state. Thread B's CAS sees `notified=true` in the previous
   state and reschedules.

2. **Thread B's CAS succeeds first:** Thread B transitions to IDLE.
   Thread A's CAS now sees `lifecycle=IDLE`, transitions to SCHEDULED,
   and returns `true` (caller must queue the task).

The `acq_rel` ordering on both CAS operations ensures mutual visibility.
If we used `monotonic`, Thread B might not see Thread A's `notified=true`,
causing a lost wakeup.

---

## Verifying Ordering Correctness

### Loom-Style Testing

Volt includes loom-style concurrency tests (in `tests/concurrency/`)
that systematically explore thread interleavings. These tests use a
deterministic scheduler (`Model`) that controls thread execution order:

```zig
test "mutex - two threads contend" {
    const model = Model.default();
    model.check(struct {
        fn run(m: *Model) void {
            var mutex = Mutex.init();

            const t1 = m.spawn(struct {
                fn f(mutex: *Mutex) void {
                    _ = mutex.tryLock();
                    mutex.unlock();
                }
            }, .{&mutex});

            const t2 = m.spawn(struct {
                fn f(mutex: *Mutex) void {
                    _ = mutex.tryLock();
                    mutex.unlock();
                }
            }, .{&mutex});

            t1.join();
            t2.join();
        }
    });
}
```

The model explores different interleavings of `t1` and `t2`, checking that
the mutex invariants hold in every case.

### Test Categories

| Test Suite | Focus | Count |
|-----------|-------|-------|
| `concurrency/mutex_loom_test.zig` | Mutex state transitions | ~8 tests |
| `concurrency/semaphore_loom_test.zig` | Permit accounting | ~8 tests |
| `concurrency/rwlock_loom_test.zig` | Reader/writer exclusion | ~8 tests |
| `concurrency/notify_loom_test.zig` | Permit delivery | ~8 tests |
| `concurrency/channel_loom_test.zig` | Send/recv ordering | ~8 tests |
| `concurrency/barrier_loom_test.zig` | Generation counting | ~5 tests |
| `concurrency/select_loom_test.zig` | Multi-channel selection | ~8 tests |

### Stress Tests

For scenarios too complex for exhaustive enumeration, stress tests run
thousands of iterations with real threads:

```zig
test "semaphore - 100 tasks contend" {
    var sem = Semaphore.init(5);
    var threads: [100]Thread = undefined;

    for (&threads) |*t| {
        t.* = try Thread.spawn(.{}, worker, .{&sem});
    }
    for (&threads) |*t| t.join();

    // Invariant: all permits returned
    assert(sem.availablePermits() == 5);
}
```

### Ordering Bugs Found and Fixed

Several ordering bugs were found during development:

1. **Oneshot `RecvWaiter.closed` data race:** The `closed` field was a
   non-atomic `bool`, causing data races between sender and receiver
   threads. Fixed by changing to `std.atomic.Value(bool)` with proper
   load/store operations.

2. **Mutex use-after-free in unlock:** The unlock path could read from a
   waiter node after another thread had already freed it. Fixed by
   capturing all needed data before the release store that makes the
   waiter visible to the waiting thread.

3. **Scheduler lost wakeup:** Tasks hung when 100+ contended on the same
   mutex. Root cause: a split-brain state where `waiting` and `notified`
   flags created a window where the waker set `notified` but the scheduler
   checked `waiting` first. Fixed by removing the `waiting` flag entirely
   -- only `notified` signals "poll again".

4. **MPMC channel Dekker pattern (ARM64 deadlock):** The
   `has_recv_waiters`/`has_send_waiters` flags used `.acquire`/`.release`,
   but the fast path (buffer write → flag read) and slow path (flag write
   → buffer read) form a Dekker pattern that requires `.seq_cst`. On
   ARM64, both sides could see stale values, causing the producer to skip
   the wakeup while the consumer saw an empty buffer — permanently
   orphaning the task. Fixed by upgrading all 15 flag operations to
   `.seq_cst`. See [Channel Wakeup Protocol](/design/channel-wakeup-protocol/).

These bugs underscore why ordering choices must be deliberate and tested.
A "just use SeqCst everywhere" approach would mask bugs at the cost of
performance, while a "use relaxed wherever possible" approach invites
subtle correctness failures.

---

## Ordering Decision Guide

When adding a new atomic operation to Volt, use this decision tree:

```
Is this a statistics/diagnostics counter?
  YES -> .monotonic

Is this data protected by a mutex?
  YES (load under lock)  -> .monotonic for load
  YES (store under lock) -> .release for store (if read without lock)

Is this a publish/consume pattern?
  YES -> .release on store, .acquire on load

Is this a CAS state transition?
  YES -> .acq_rel on success, .acquire on failure

Does correctness require a total order across multiple atomics?
  YES -> .seq_cst

When in doubt:
  -> Use .acq_rel and add a loom test
```

The goal is to use the weakest ordering that is provably correct, verified
by loom-style testing. Stronger orderings are not "safer" -- they just
hide bugs by making them less likely to manifest, while adding unnecessary
performance overhead on weakly-ordered architectures (ARM, RISC-V).

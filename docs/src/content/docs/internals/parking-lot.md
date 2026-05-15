---
title: Volt parking lot
description: One wait/wake mechanism for every coroutine-level sync primitive.
---

# Volt parking lot — design

## What problem this solves

Volt has many primitives that share one conceptual operation: a coroutine
wants to wait until some condition holds, and another party wants to wake
it. Today each primitive — `WaitGroup`, `Mutex`, `Notify`, `Semaphore`,
`Spsc` channel, `reactor.waitFd` — rolls its own register-then-park dance
with its own waiter slot or wait queue.

This is fragile, duplicative, and untestable in any unified way. We have
already hit one wait/wake race that the per-primitive review missed
(`WaitGroup` use-after-free on Task.join during sustained spawn loops).
Every other primitive has the same algorithmic shape, which means every
other primitive likely has the same class of bug.

The parking lot replaces all of them with **one** wait/wake mechanism.
Primitives become thin shims on top of two functions:
`parkOn(addr, validator)` and `unparkOne(addr)` / `unparkAll(addr)`.

## The shape

```
   Public primitives — Mutex · Notify · Semaphore · Spsc · Task.join · reactor.waitFd
                                  │
                                  ▼
   park.parkOn(addr, validator)
   park.unparkOne(addr) / park.unparkAll(addr)
                                  │
                                  ▼
   Parking lot — sharded hashmap: address → wait queue
                                  │
                                  ▼
   Parker (OS-thread sleep) — already in place
   Darwin __ulock_wait · Linux futex
```

The parking lot handles coroutine-level wait/wake. The `Parker` (worker
OS thread sleep) stays exactly as it is today — futex/ulock is already
correct for blocking an OS thread.

## API

```zig
/// Park the current coroutine until `unparkOne` / `unparkAll` is
/// called for the same address. `validator` is called UNDER the
/// bucket lock; if it returns false, parkOn returns immediately
/// (the condition is already satisfied).
///
/// Caller must guarantee:
///   * `addr`'s lifetime exceeds any concurrent unpark on it
///   * `addr` is keyed on the same logical condition consistently
pub fn parkOn(addr: *const anyopaque, validator: *const fn () bool) void;

/// Wake exactly one coroutine parked on `addr`, FIFO order.
/// Returns true if a waiter was popped (and `runtime.unpark` called).
pub fn unparkOne(addr: *const anyopaque) bool;

/// Wake every coroutine parked on `addr`. Returns the count.
pub fn unparkAll(addr: *const anyopaque) usize;
```

The address is a key — typically `&primitive.state_word`. Different
primitives use different addresses; one primitive uses one address.

## The validator closes the race

The whole class of wait/wake races is closed by one rule:

> **The validator runs under the bucket lock, and it observes the same
> state that the wake side modifies before calling unpark.**

```
parkOn(addr, validator):
  bucket = lot.bucket(addr)
  lock bucket
  if !validator():            // condition already satisfied
    unlock; return            //   → don't park, return immediately
  enqueue self on bucket for addr
  unlock
  runtime.park()              // coroutine suspension
```

```
unparkOne(addr):
  bucket = lot.bucket(addr)
  lock bucket
  coro = bucket.popFirstFor(addr)
  unlock
  if coro: runtime.unpark(coro)
```

The unparker writes its state change **before** calling `unparkOne`.
The parker reads that state **under the same bucket lock** in the
validator. The bucket lock totally orders the two:

- If the parker's validator runs first, it sees old state, enqueues,
  parks. The unparker then locks, pops the parker, wakes it.
- If the unparker's store runs first, the parker's validator sees the
  new state and returns without parking.

There is no register-then-park gap. The validator IS the
register-or-bail decision, and it is atomic with respect to the bucket
lock.

## Data structures

```zig
const BUCKET_COUNT = 256; // power-of-two for cheap masking

const Bucket = struct {
    mutex: PthreadMutex,    // brief — held only across list manipulation
    head: ?*Waiter,         // intrusive singly-linked list
};

const Waiter = struct {
    coro: *Coroutine,       // who to wake
    addr: usize,            // key (may differ from head's next entry — hash collisions)
    next: ?*Waiter,
};

const ParkingLot = struct {
    buckets: [BUCKET_COUNT]Bucket,
};
```

One `ParkingLot` per `Runtime`. Buckets sharded by
`(addr >> 4) & (BUCKET_COUNT - 1)`. The `>> 4` defeats alignment
clustering; addresses are sufficiently spread that no stronger hash is
needed (we can swap in a real mixer if a workload reveals clustering).

The `Waiter` node lives on the **parking coroutine's stack** for the
duration of `parkOn`. No heap allocation. Stackful coroutines preserve
their stack across suspension, so the node remains valid while the
coroutine is parked. When the coroutine wakes and `parkOn` returns,
the stack frame is popped and the node's storage is gone.

## Memory ordering

The bucket mutex sequences everything for a given address. The
happens-before chain from a wake-side state change to a parker
observing it:

```
wake side:         state.store(new_value, .release)
                                ↘
unpark:            lock(bucket) | pop coro | unlock(bucket)
                                                 ↘
unpark:            injection.push(coro)  // release CAS on injection head
                                                       ↘
some worker:       injection.pop(coro)   // acquire CAS
                                                          ↘
some worker:       context.swap(&w.main_ctx, &coro.ctx)  // memory barrier
                                                              ↘
coro resumes:      reads state              // sees the release-stored value
```

Every hop is a paired release/acquire. No torn reads, no missed wakes,
no compiler reorderings of the user's state load past the lock.

The parker's own protocol:

```
park:    lock(bucket) | validator (loads state, .acquire) | enqueue | unlock | runtime.park
                                ↘
unparker: store(state, .release) | lock(bucket) | pop | unlock | unpark
```

The bucket mutex's lock-acquire pairs with the prior unlock-release
between any two threads. So the parker's validator can never observe
state older than what the latest unparker stored before its own lock.

## Driver-thread parking (non-coroutine context)

Most parkers are coroutines. But `Task.join` may be called from the
driver thread (e.g., from `main` after `rt.run` returns — though by
that point the task is already done and join doesn't block). The
existing `thread_waiter` mechanism in `Runtime.run` handles wake-on-
root-completion without going through the parking lot — the dispatch
loop itself stops when the root task is done and returns from
`workerLoopUntilTaskDone`.

We don't need to extend `parkOn` to support non-coroutine callers in
v1. The driver-thread path is handled by the worker dispatch loop's
exit condition, not by `parkOn`.

## Fairness

FIFO by default — enqueue at tail, pop from head. The bucket mutex is
held only across queue-manipulation (pointer writes), never across
`runtime.park` or `runtime.unpark` themselves, so contention is bounded
to ~100ns per critical section.

For primitives that want different policies (Mutex direct handoff,
condition-variable signalling), an extension API can come later:

```zig
pub fn unparkBy(addr, predicate) bool;     // pop first matching waiter
pub fn unparkRequeue(from_addr, to_addr);  // move waiters from one address to another
```

Out of scope for v1.

## Edge cases

| Case | Handling |
|---|---|
| Spurious wakeups | Impossible by construction. Only `unparkOne`/`unparkAll` wake a parked coroutine, and each pops the Waiter. No loop needed inside `parkOn`. |
| Park-then-condition-becomes-true race | Closed by validator-under-lock. Either park or observe new state, never both. |
| Multiple waiters on same address | Bucket holds a linked list; `unparkOne` walks to find matching `addr`; `unparkAll` drains all matching. |
| Hash collisions (different addrs in same bucket) | The list walk filters by `addr`. Extra work is O(N waiters in bucket) but contention is rare with 256 buckets. |
| Lifetime of `addr` | Caller's responsibility. Same constraint as today — primitives must outlive their parkers. |
| Cancellation | Out of scope for v1. When added, a `cancelPark(waiter)` helper re-locks the bucket and unlinks. |

## Performance

| Operation | Approx cost |
|---|---|
| `parkOn` with validator returning false (fast bail) | 1 lock + 1 unlock + validator call ≈ 30ns |
| `parkOn` with park (validator returns true) | + enqueue + `runtime.park` (coro swap-out) ≈ 100ns |
| `unparkOne` matching, single waiter | 1 lock + list walk + 1 unlock + `runtime.unpark` ≈ 80ns |
| `unparkOne` no matching waiter | 1 lock + list walk + 1 unlock ≈ 30ns |

Uncontended primitives never touch the parking lot — they handle their
fast path on their own atomic state. The lot only enters the picture
when a coroutine actually needs to block.

Comparable in cost to today's per-primitive wait queues, but with one
implementation that gets stress-tested instead of N.

## First consumer — Task.join

The first consumer is also the first deletion: `wait_group.zig` goes
away entirely, replaced by a flat `done` flag on Task.

```zig
pub fn Task(comptime T: type) type {
    return struct {
        coro: *Coroutine,
        result_ptr: *T,
        frame_ptr: *anyopaque,
        frame_destroy: FrameDestroyFn,
        allocator: std.mem.Allocator,
        done: std.atomic.Value(u32),

        const NOT_DONE: u32 = 0;
        const DONE: u32 = 1;

        pub fn join(self: *Self) T {
            while (self.done.load(.acquire) == NOT_DONE) {
                park.parkOn(&self.done, struct {
                    fn v(d: *const std.atomic.Value(u32)) bool {
                        return d.load(.acquire) == NOT_DONE;
                    }
                }.v);
            }
            const result = self.result_ptr.*;
            self.frame_destroy(self.frame_ptr, self.allocator);
            self.allocator.destroy(self);
            return result;
        }
    };
}
```

`Coroutine.task_done: ?*std.atomic.Value(u32)` replaces `Coroutine.wg`.
Dispatch's `.done` branch:

```zig
.done => {
    if (c.task_done) |done| {
        done.store(DONE, .release);
        _ = park.unparkOne(done);
    }
    _ = rt.stat_done.fetchAdd(1, .release);
    rt.allocator.free(c.stack);
    rt.allocator.destroy(c);
},
```

The wait/wake races that have been eating us disappear:

- The validator (`d.load(.acquire) == NOT_DONE`) runs under the bucket
  lock. If the wake side already stored `DONE` before our `parkOn`,
  the validator returns false and we don't park.
- If we do park, the wake side will lock our bucket, find us, and wake
  us.
- No separate waiter slot to corrupt. No drain flag. No
  use-after-free.

## Migration plan

1. **Implement `src/park.zig`.** ~300 lines. Sharded bucket array,
   intrusive Waiter list, the two-line `parkOn` / `unparkOne`
   protocols above. Standalone unit tests using fake coroutines
   (just integers + a "signal" closure) to verify the algorithm.

2. **Add `zig build stress`.** A 60-second workload exercising
   spawn+join + Mutex + channel + Notify across worker counts.
   Becomes a pre-merge gate. **This step is non-negotiable** — the
   parking lot must be exercised under sustained load to validate it
   before consumers depend on it.

3. **Replace WaitGroup.** `Task.done` + `parkOn`. Delete
   `wait_group.zig`, `WaitGroupAtomic`, the `wg` field on Coroutine.
   The hot-loop bench (`bench-spawn-hot`) should now pass at
   workers=NumCPU without hanging.

4. **Migrate Mutex.** State word (UNLOCKED / LOCKED / CONTENDED) stays
   the same. The slow path becomes `parkOn(&self.state, isContended)`
   instead of the pthread-mutex + linked-list wait queue. Direct
   handoff is preserved by `unparkOne` waking exactly one waiter at
   the end of `unlock`.

5. **Migrate Notify, Semaphore** the same way.

6. **Migrate Spsc channel** block-on-full / block-on-empty.

7. **Optional — migrate reactor.waitFd** to `parkOn` for API symmetry.
   The actual park inside `waitFd` is on the kqueue/kevent, not on a
   primitive, so this is cosmetic. Defer.

After step 5, the only sync mechanism in the runtime that isn't
parking-lot-based is the reactor and the Parker itself. That's the
target end state.

## Open questions to settle during implementation

- **Bucket count.** 256 is a guess. Adjust after measuring.
- **Hash function.** `addr >> 4` is the floor. Swap in a stronger
  mixer (e.g., MurmurHash) if a real workload shows pathological
  clustering.
- **Waiter allocation: stack vs Coroutine-embedded.** Stack is
  simpler; embedded saves a stack write per park. Pick after
  measurement; the public API doesn't change either way.
- **Adaptive spin before park.** WebKit's parking_lot spins ~10 times
  before the kernel call to absorb very-brief contention. Worth
  adding if measurements show it helps; the kernel call itself is
  already cheap on Darwin (ulock).

## What this design buys us

- **One algorithm to get right.** One stress test to harden every
  primitive.
- **WaitGroup, Mutex's wait queue, Notify, Semaphore, channel block
  paths** all converge to a 5–10 line shim each. Net deletion of
  hundreds of lines of bespoke wait/wake code.
- **The wait/wake races we have been hitting become impossible by
  construction.** Use-after-free, register-then-park, lost wakeup —
  all closed by the validator-under-lock rule.
- **Future primitives** (RwLock, Broadcast, Watch, Oneshot, Barrier,
  OnceCell, Mpmc) get the foundation for free. Each becomes a few
  lines of state-machine code on top of `parkOn`/`unparkOne` instead
  of a fresh handcrafted wait/wake protocol.

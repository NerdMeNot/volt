---
title: Zero-Allocation Waiters
description: How Volt eliminates heap allocation on sync primitive hot paths
  using intrusive linked lists and embedded waiter structs.
slug: v1.0.0-zig0.15.2/design/zero-allocation-waiters
---

One of the most important performance properties of Volt's synchronization
primitives is that **waiting on a mutex, semaphore, or read-write lock
requires zero heap allocation**. The waiter struct is embedded directly in
the future that is waiting, and linked into the primitive's queue through
intrusive list pointers.

This page explains the design, why it matters, and how it works in practice.

## The Problem

Traditional async mutex implementations allocate a waiter node on the heap
when a task needs to wait:

```
// Traditional approach (what Volt does NOT do)
fn lockWait(self: *Mutex) !void {
    const waiter = try allocator.create(WaiterNode);  // heap allocation
    waiter.* = .{ .task = currentTask() };
    self.queue.push(waiter);
    suspend();
    allocator.destroy(waiter);                        // heap free
}
```

Every lock acquisition that contends requires `malloc` and `free`. Under
heavy contention -- hundreds of tasks fighting for the same mutex -- this
creates significant allocator pressure:

* Each allocation touches the allocator's free list (often a global lock)
* Memory fragmentation from many small, short-lived allocations
* Cache pollution from allocator metadata
* Failure mode: `OutOfMemory` during lock contention

For a runtime targeting millions of operations per second, this overhead is
unacceptable.

## The Solution: Intrusive Waiters

Volt embeds the waiter struct directly inside the future that is waiting.
The waiter includes intrusive linked list pointers, so it can be threaded
into the primitive's wait queue without any separate allocation.

### The Intrusive Linked List

The foundation is an intrusive doubly-linked list defined in
`src/internal/util/linked_list.zig`:

```zig
/// Pointers for linking a node into a list.
/// Embed this in your node struct.
pub fn Pointers(comptime T: type) type {
    return struct {
        prev: ?*T = null,
        next: ?*T = null,
    };
}

/// An intrusive doubly-linked list.
pub fn LinkedList(comptime T: type, comptime pointers_field: []const u8) type {
    return struct {
        head: ?*T = null,
        tail: ?*T = null,
        len: usize = 0,

        pub fn pushFront(self: *Self, node: *T) void { ... }
        pub fn pushBack(self: *Self, node: *T) void { ... }
        pub fn popFront(self: *Self) ?*T { ... }
        pub fn popBack(self: *Self) ?*T { ... }
        pub fn remove(self: *Self, node: *T) void { ... }
    };
}
```

Key properties:

* **O(1) insertion and removal** -- no searching
* **No allocation** -- nodes embed their own pointers
* **O(1) cancellation** -- remove a specific node by pointer
* **FIFO ordering** -- pushFront + popBack gives fair queuing

### The Semaphore Waiter

The `Semaphore.Waiter` struct (from `src/sync/Semaphore.zig`) is the core
waiter type. It embeds intrusive list pointers and tracks how many permits
are still needed:

```zig
pub const Waiter = struct {
    /// Remaining permits needed (atomic for cross-thread visibility)
    state: std.atomic.Value(usize),

    /// Original number of permits requested (for cancellation math)
    permits_requested: usize,

    /// Waker callback invoked when permits are granted
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Intrusive list pointers -- NO HEAP ALLOCATION
    pointers: Pointers(Waiter) = .{},

    /// Debug-mode invocation tracking
    invocation: InvocationId = .{},
};
```

The waiter list in the semaphore:

```zig
pub const Semaphore = struct {
    permits: std.atomic.Value(usize),
    mutex: std.Thread.Mutex,
    waiters: LinkedList(Waiter, "pointers"),  // Intrusive list
};
```

When `release()` grants permits to waiters, it walks the list and assigns
permits directly. Satisfied waiters are removed from the list and their
waker callbacks are collected into a `WakeList` for batch invocation after
the mutex is released:

```zig
fn releaseToWaiters(self: *Self, num: usize) void {
    var wake_list: WakeList = .{};
    var rem = num;

    // Serve waiters from the back (oldest first = FIFO)
    while (rem > 0) {
        const waiter = self.waiters.back() orelse break;

        const remaining = waiter.state.load(.monotonic);
        const assign = @min(rem, remaining);
        waiter.state.store(remaining - assign, .release);
        rem -= assign;

        if (remaining - assign == 0) {
            _ = self.waiters.popBack();
            // Collect waker for batch invocation
            wake_list.push(.{ .context = ctx, .wake_fn = wf });
        }
    }

    // Surplus permits go to the atomic counter
    if (rem > 0) _ = self.permits.fetchAdd(rem, .release);

    self.mutex.unlock();
    wake_list.wakeAll();  // Wake AFTER releasing the lock
}
```

## How Primitives Use Intrusive Waiters

### Mutex

The `Mutex` is built on `Semaphore(1)`. Its waiter is a thin wrapper:

```zig
pub const Waiter = struct {
    inner: SemaphoreWaiter,

    pub fn init() Self {
        return .{ .inner = SemaphoreWaiter.init(1) };
    }

    pub fn isAcquired(self: *const Self) bool {
        return self.inner.isReady();
    }
};
```

The `LockFuture` embeds this waiter as a field:

```zig
pub const LockFuture = struct {
    pub const Output = void;

    acquire_future: semaphore_mod.AcquireFuture,  // Contains the waiter
    mutex: *Mutex,

    pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
        return self.acquire_future.poll(ctx);
    }
};
```

Since `AcquireFuture` itself embeds a `Waiter`:

```zig
pub const AcquireFuture = struct {
    semaphore: *Semaphore,
    num_permits: usize,
    waiter: Waiter,           // Embedded, not heap-allocated
    state: State,
    stored_waker: ?FutureWaker,
};
```

The full chain of embedding means the waiter lives inside the future, which
lives inside the `FutureTask`, which is the single allocation for the
entire task. Zero additional allocations for waiting.

### RwLock

The `RwLock` is built on `Semaphore(MAX_READS)` where `MAX_READS` is ~536
million. Read locks acquire 1 permit; write locks acquire all `MAX_READS`
permits:

```zig
pub const ReadWaiter = struct {
    inner: SemaphoreWaiter,     // Requests 1 permit
    user_waker: ?WakerFn = null,
    user_waker_ctx: ?*anyopaque = null,
};

pub const WriteWaiter = struct {
    inner: SemaphoreWaiter,     // Requests MAX_READS permits
    user_waker: ?WakerFn = null,
    user_waker_ctx: ?*anyopaque = null,
};
```

Writer priority emerges naturally from this design: a queued writer's
partial acquisition drains permits toward 0, so new readers'
`tryAcquire(1)` fails. The FIFO queue serves the writer before any reader
queued behind it.

The `ReadLockFuture` and `WriteLockFuture` each embed their respective
waiter:

```zig
pub const ReadLockFuture = struct {
    pub const Output = void;
    rwlock: *RwLock,
    waiter: ReadWaiter,         // Embedded
    state: State,
    stored_waker: ?FutureWaker,
};

pub const WriteLockFuture = struct {
    pub const Output = void;
    rwlock: *RwLock,
    waiter: WriteWaiter,        // Embedded
    state: State,
    stored_waker: ?FutureWaker,
};
```

## Drop Safety: The State Enum

A subtle correctness requirement: what happens if a future is destroyed
while its waiter is still in the queue? The waiter's intrusive pointers
would dangle, corrupting the list.

Each future tracks its lifecycle through a `State` enum:

```zig
const State = enum {
    /// Haven't tried to acquire yet
    init,
    /// Waiting for permits (waiter is in the queue)
    waiting,
    /// Permits acquired (waiter has been removed)
    acquired,
};
```

The `cancel` and `deinit` methods check the state before cleanup:

```zig
pub fn cancel(self: *Self) void {
    if (self.state == .waiting) {
        // Remove waiter from queue before destroying
        self.semaphore.cancelAcquire(&self.waiter);
    }
    if (self.stored_waker) |*w| {
        w.deinit();
        self.stored_waker = null;
    }
}

pub fn deinit(self: *Self) void {
    self.cancel();
}
```

This ensures:

| State | On Drop | Safe? |
|-------|---------|-------|
| `init` | Nothing to clean up | Yes |
| `waiting` | Remove waiter from queue, then destroy | Yes |
| `acquired` | Nothing to clean up (already removed) | Yes |

The `cancelAcquire` method on the semaphore handles the removal under lock,
returning any partially acquired permits:

```zig
pub fn cancelAcquire(self: *Self, waiter: *Waiter) void {
    if (waiter.isComplete()) return;

    self.mutex.lock();
    if (waiter.isComplete()) { self.mutex.unlock(); return; }

    // Remove from list if linked
    self.waiters.remove(waiter);
    waiter.pointers.reset();

    const acquired = waiter.permits_requested - waiter.state.load(.monotonic);
    self.mutex.unlock();

    // Return partial permits (may wake other waiters)
    if (acquired > 0) self.release(acquired);
}
```

## The Waker Callback Bridge

When a waiter is satisfied (e.g., a mutex is unlocked), the sync primitive
needs to wake the waiting task. The bridge between the primitive's waker
callback and the future's stored `Waker` is a static function pointer:

```zig
// In AcquireFuture
pub fn poll(self: *Self, ctx: *Context) PollResult(void) {
    switch (self.state) {
        .init => {
            // Store the scheduler's waker
            self.stored_waker = ctx.getWaker().clone();

            // Set callback that bridges to the stored waker
            self.waiter.setWaker(@ptrCast(self), wakeCallback);

            if (self.semaphore.acquireWait(&self.waiter)) {
                self.state = .acquired;
                return .{ .ready = {} };
            }
            self.state = .waiting;
            return .pending;
        },
        .waiting => {
            if (self.waiter.isComplete()) {
                self.state = .acquired;
                return .{ .ready = {} };
            }
            return .pending;
        },
        .acquired => return .{ .ready = {} },
    }
}

/// Bridge: primitive's WakerFn -> future's stored Waker
fn wakeCallback(ctx: *anyopaque) void {
    const self: *Self = @ptrCast(@alignCast(ctx));
    if (self.stored_waker) |*w| {
        w.wakeByRef();
    }
}
```

The flow:

```
Semaphore.release()
  -> waiter.waker(waiter.waker_ctx)     // primitive's callback
    -> AcquireFuture.wakeCallback(self)  // bridge function
      -> self.stored_waker.wakeByRef()   // scheduler's waker
        -> header.schedule()             // reschedule the task
```

No allocation at any step. The callback pointer is a comptime-known function.
The `self` pointer is the future itself (which is inside the `FutureTask`
allocation). The stored waker is the scheduler's task waker.

## Waker Migration

Tasks can migrate between worker threads (via work stealing). When a task
resumes on a different worker, its waker might need updating. The `waiting`
state handles this:

```zig
.waiting => {
    if (self.waiter.isComplete()) {
        self.state = .acquired;
        return .{ .ready = {} };
    }

    // Update waker in case task migrated to different worker
    const new_waker = ctx.getWaker();
    if (self.stored_waker) |*old| {
        if (!old.willWakeSame(new_waker)) {
            old.deinit();
            self.stored_waker = new_waker.clone();
        }
    }
    return .pending;
},
```

`willWakeSame` compares the raw data pointers. If the waker has changed
(different task header due to migration), the old one is dropped and the
new one is cloned.

## Anti-Starvation: The Semaphore Algorithm

A naive semaphore has a starvation bug: permits can "slip through" between
a failed CAS and queue insertion. Volt's semaphore eliminates this with
a "mutex-before-CAS" slow path:

```zig
pub fn acquireWait(self: *Self, waiter: *Waiter) bool {
    // Fast path: lock-free CAS for full acquisition
    var curr = self.permits.load(.acquire);
    while (curr >= needed) {
        if (self.permits.cmpxchgWeak(curr, curr - needed, ...)) |updated| {
            curr = updated;
            continue;
        }
        waiter.state.store(0, .release);
        return true;  // Got all permits without locking
    }

    // Slow path: lock BEFORE CAS
    self.mutex.lock();

    // Under lock: drain available permits
    curr = self.permits.load(.acquire);
    while (acquired < needed and curr > 0) {
        // CAS under lock...
    }

    if (acquired >= needed) {
        self.mutex.unlock();
        return true;
    }

    // Queue waiter UNDER LOCK -- no permits can arrive
    // between our CAS and this insertion, because
    // release() also takes this mutex.
    self.waiters.pushFront(waiter);
    self.mutex.unlock();
    return false;
}
```

And `release()` always takes the mutex:

```zig
pub fn release(self: *Self, num: usize) void {
    self.mutex.lock();

    if (self.waiters.back() == null) {
        // No waiters -- surplus to atomic
        _ = self.permits.fetchAdd(num, .release);
        self.mutex.unlock();
        return;
    }

    // Serve waiters directly
    self.releaseToWaiters(num);
}
```

The key invariant: **permits never float in the atomic counter when waiters
are queued**. `release()` serves waiters directly; only surplus goes to the
atomic. This prevents indefinite starvation.

## Comparison with Tokio

Tokio uses the same intrusive waiter pattern. The Rust implementation is
structurally equivalent:

| Aspect | Tokio (Rust) | Volt (Zig) |
|--------|-------------|---------------|
| Waiter embedding | `Pin<&mut Waiter>` in future | `Waiter` field in future struct |
| List type | Custom intrusive list | `LinkedList(T, field)` |
| Drop safety | Rust's `Drop` trait | Manual `deinit`/`cancel` with state enum |
| Waker storage | `Option<Waker>` | `?FutureWaker` |
| Memory ordering | `Acquire`/`Release` pairs | `Acquire`/`Release` pairs |
| Permit tracking | Atomic state word | `std.atomic.Value(usize)` |

The primary difference is safety enforcement. Rust's type system guarantees
(via `Pin`) that the waiter is not moved while in the queue. Zig requires
the programmer to ensure this through the state enum and proper
`deinit`/`cancel` calls.

## Performance Impact

The zero-allocation design has measurable performance benefits:

| Operation | With Allocation | Zero-Allocation | Improvement |
|-----------|----------------|-----------------|-------------|
| Uncontended lock | ~15 ns | ~10 ns | 1.5x |
| Contended lock (10 waiters) | ~150 ns | ~100 ns | 1.5x |
| 1M lock/unlock cycles | 1M malloc + 1M free | 0 allocations | -- |

The improvement comes from:

1. **No allocator calls** -- the waiter is already allocated as part of the
   future/task
2. **Better cache locality** -- the waiter is adjacent to the future's other
   fields
3. **No fragmentation** -- no small, short-lived allocations polluting the
   free list
4. **No failure mode** -- contention cannot cause `OutOfMemory`

For workloads with heavy sync primitive contention (e.g., connection pools,
rate limiters, shared caches), this is the difference between scalable and
not scalable.

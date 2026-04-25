---
title: The Future Model
description: How Volt implements poll-based stackless futures, wakers, state
  machines, and composable combinators in Zig 0.15.
slug: v1.0.0-zig0.15.2/design/future-model
---

Volt's async model is built on **stackless futures** -- small state
machines that are polled to completion by the scheduler. This page covers
the core types, the polling protocol, waker mechanics, and how futures
compose.

## Core Types

The future system is defined in `src/future/` and consists of four
fundamental types:

| Type | Size | Purpose |
|------|------|---------|
| `PollResult(T)` | Tagged union | Result of polling: `.pending` or `.ready(T)` |
| `Waker` | 16 bytes | Reschedules a suspended task |
| `Context` | Pointer | Carries the waker into `poll()` |
| `Future` (trait) | Structural | Any type with `poll(*Self, *Context) PollResult(Output)` |

### PollResult

The polling result type is a tagged union with two variants:

```zig
pub fn PollResult(comptime T: type) type {
    return union(enum) {
        pending: void,   // Not ready, waker registered
        ready: T,        // Complete with value
    };
}
```

There is no error variant. Errors are part of `T` itself. A future that can
fail uses `PollResult(MyError!MyData)`. This keeps the poll protocol simple
and orthogonal to error handling.

`PollResult` provides convenience methods: `isReady()`, `isPending()`,
`unwrap()`, `value()`, and `map()` for transforming the ready value.

### Waker

A `Waker` is a 16-byte value type that knows how to reschedule its
associated task:

```zig
pub const Waker = struct {
    raw: RawWaker,
};

pub const RawWaker = struct {
    data: *anyopaque,          // Pointer to task Header
    vtable: *const VTable,     // Type-erased operations

    pub const VTable = struct {
        wake: *const fn (*anyopaque) void,         // Consume and reschedule
        wake_by_ref: *const fn (*anyopaque) void,  // Reschedule without consuming
        clone: *const fn (*anyopaque) RawWaker,    // Duplicate (ref++)
        drop: *const fn (*anyopaque) void,         // Release (ref--)
    };
};
```

The vtable pattern provides type erasure without dynamic dispatch overhead
on the hot path. The scheduler's task waker implementation:

* **`wake`** calls `header.schedule()` then `header.unref()` (consumes the waker)
* **`wake_by_ref`** calls `header.schedule()` without consuming
* **`clone`** calls `header.ref()` and returns a new `RawWaker`
* **`drop`** calls `header.unref()`, freeing the task if it was the last reference

### Context

The `Context` is passed into every `poll()` call. Its only job is to carry
the waker:

```zig
pub const Context = struct {
    waker: *const Waker,

    pub fn getWaker(self: *const Context) *const Waker {
        return self.waker;
    }
};
```

The scheduler creates a context for each poll by wrapping the task's waker:

```zig
const task_waker = header.waker();
var ctx = Context{ .waker = &task_waker };
const result = self.future.poll(&ctx);
```

## The Future Trait

Volt uses Zig's structural typing for the future "trait" -- no
interface or vtable at the user level. Any type that has an `Output`
declaration and a `poll` method with the correct signature is a future:

```zig
pub fn isFuture(comptime T: type) bool {
    return @hasDecl(T, "poll") and
           @hasDecl(T, "Output") and
           @TypeOf(T.poll) matches fn(*T, *Context) PollResult(T.Output);
}
```

A minimal future:

```zig
const CountFuture = struct {
    pub const Output = u32;
    count: u32 = 0,

    pub fn poll(self: *@This(), ctx: *Context) PollResult(u32) {
        self.count += 1;
        if (self.count >= 3) {
            return .{ .ready = self.count };
        }
        // Must register waker before returning pending
        _ = ctx.getWaker();
        return .pending;
    }
};
```

## The Polling Protocol

The poll protocol defines the contract between a future and the scheduler:

1. The scheduler calls `future.poll(&ctx)`.
2. The future attempts to make progress.
3. If the operation completes, it returns `.{ .ready = value }`.
4. If it cannot complete yet, it **must** register the waker from `ctx`
   somewhere that will call `waker.wake()` when progress can be made, then
   return `.pending`.
5. When the waker fires, the scheduler polls the future again.

**The critical invariant**: a future that returns `.pending` must have
arranged for its waker to be called eventually. If it returns `.pending`
without registering the waker, the task is orphaned forever.

### State Machine Transformation

Consider this conceptual async code:

```
read data from socket
process the data
write result to socket
```

As a stackless future, this becomes a state machine with explicit states
for each suspend point:

```zig
const HandleFuture = struct {
    pub const Output = void;

    state: enum { reading, processing, writing, done } = .reading,
    conn: *TcpStream,
    buf: [4096]u8 = undefined,
    result: ?[]const u8 = null,

    pub fn poll(self: *@This(), ctx: *Context) PollResult(void) {
        while (true) {
            switch (self.state) {
                .reading => {
                    const n = self.conn.tryRead(&self.buf) orelse {
                        self.conn.registerWaker(ctx.getWaker());
                        return .pending;
                    };
                    self.result = process(self.buf[0..n]);
                    self.state = .writing;
                },
                .writing => {
                    self.conn.tryWrite(self.result.?) orelse {
                        self.conn.registerWaker(ctx.getWaker());
                        return .pending;
                    };
                    self.state = .done;
                },
                .done => return .{ .ready = {} },
                .processing => unreachable,
            }
        }
    }
};
```

Each local variable that lives across a suspend point becomes a field in the
struct. The `state` enum tracks which suspend point to resume from. The total
size is the struct size -- typically a few hundred bytes instead of a 16 KB
stack.

## FutureTask: Wrapping Futures for the Scheduler

The scheduler operates on type-erased `Header` pointers. `FutureTask(F)`
wraps a concrete future type into a schedulable task:

```
+----------------------------------------------+
| FutureTask(MyFuture)                         |
+----------------------------------------------+
| header: Header          | 64 bytes           |
|   state: atomic(u64)    |   packed state word |
|   next/prev: ?*Header   |   intrusive list    |
|   vtable: *const VTable |   type erasure      |
+-------------------------+--------------------+
| future: MyFuture        | User's state       |
|   state: enum           |   machine          |
|   data: ...             |                    |
+-------------------------+--------------------+
| result: ?Output         | Result storage     |
+-------------------------+--------------------+
| allocator: Allocator    | For cleanup        |
+-------------------------+--------------------+
| scheduler: ?*anyopaque  | Scheduler ref      |
| schedule_fn: ?*fn       | Callback for wake  |
| reschedule_fn: ?*fn     | Callback for       |
|                         | reschedule         |
+----------------------------------------------+
```

Total size: Header (~64 bytes) + Future (user-defined) + overhead (~40
bytes). For a simple future, this is typically 200-400 bytes.

The vtable dispatches to the concrete implementation via `@fieldParentPtr`:

```zig
fn pollImpl(header: *Header) Header.PollResult_ {
    const self: *Self = @fieldParentPtr("header", header);

    if (header.isCancelled()) return .complete;

    const task_waker = header.waker();
    var ctx = Context{ .waker = &task_waker };
    const poll_result = self.future.poll(&ctx);

    if (poll_result.isReady()) {
        if (Output != void) self.result = poll_result.unwrap();
        return .complete;
    }
    return .pending;
}
```

## Built-in Futures

Volt provides several built-in future types. The types below are engine
internals (`volt.future.*`) used by the runtime implementation. Most users
interact with futures through the `io.@"async"` convenience API instead.

### Ready

A future that completes immediately with a value:

```zig
// Engine internal: volt.future.ready(...)
var f = volt.future.ready(@as(i32, 42));
// First poll returns .{ .ready = 42 }
```

### Pending

A future that never completes (useful for testing and `select`):

```zig
// Engine internal: volt.future.pending(...)
var f = volt.future.pending(i32);
// Every poll returns .pending
```

### Lazy

A future that computes its value on the first poll:

```zig
// Engine internal: volt.future.lazy(...)
var f = volt.future.lazy(struct {
    fn compute() i32 { return expensiveCalculation(); }
}.compute);
```

### FnFuture

Wraps a regular function as a single-poll future. This is how
`io.@"async"` converts plain functions into tasks:

```zig
fn processData(data: []const u8) !Result {
    return Result.from(data);
}

// FnFuture(processData) polls once, calling the function
var f = try io.@"async"(processData, .{data});
const result = f.@"await"(io);
```

## Composing Futures

### MapFuture

Transforms a future's output with a comptime function:

```zig
const doubled = MapFuture(MyFuture, i32, struct {
    fn transform(x: i32) i32 { return x * 2; }
}.transform);
```

### AndThenFuture

Chains two futures sequentially (monadic bind). The second future is
created from the first's output:

```zig
// fetch user, then fetch their posts
const chained = AndThenFuture(FetchUserFuture, FetchPostsFuture, struct {
    fn then(user: User) FetchPostsFuture {
        return FetchPostsFuture.init(user.id);
    }
}.then);
```

### Compose (Fluent API)

The `Compose` wrapper provides a fluent combinator interface. This is an
engine internal (`volt.future.compose`) used for building higher-level APIs:

```zig
// Engine internal: volt.future.compose(...)
const result = volt.future.compose(myFuture)
    .map(i32, doubleIt)
    .andThen(NextFuture, startNext);
```

### Task Combinators

At the task level, Volt provides higher-level combinators:

| Combinator | Behavior |
|-----------|----------|
| `joinAll` | Wait for all futures, return tuple of results |
| `tryJoinAll` | Wait for all, collect results and errors |
| `race` | First to complete wins, cancel others |
| `select` | First to complete, keep others running |

```zig
// Run two tasks concurrently, wait for both
const user, const posts = try io.joinAll(.{
    try io.@"async"(fetchUser, .{id}),
    try io.@"async"(fetchPosts, .{id}),
});

// Race: first result wins
const fastest = try io.race(.{
    try io.@"async"(fetchFromPrimary, .{key}),
    try io.@"async"(fetchFromReplica, .{key}),
});
```

### Group (Structured Concurrency)

For fire-and-forget tasks that share a common lifecycle, the `Group` API
provides structured concurrency: spawn multiple tasks, wait for all, and
cancel on scope exit.

```zig
var group = volt.Group.init(io);
_ = group.spawn(fetchUser, .{id});
_ = group.spawn(fetchPosts, .{id});
_ = group.spawn(updateMetrics, .{id});
group.wait();   // Blocks until all three complete
group.cancel(); // Or cancel all outstanding tasks
```

`group.spawn()` returns `bool` (false if the group is full or spawn fails).
Groups track up to 32 concurrent tasks. For larger fan-outs, use
`io.@"async"()` directly with your own tracking.

## The Wakeup Protocol

The interaction between futures, wakers, and the scheduler is the most
critical part of the system. A race between a waker firing and the
scheduler transitioning a task to idle can cause lost wakeups, where a
task is never polled again despite being ready.

Volt prevents lost wakeups through a single `notified` bit in the
task's packed atomic state word:

```
When a waker fires:
  - If task is IDLE:                 transition to SCHEDULED, queue it
  - If task is RUNNING or SCHEDULED: set the notified bit

After poll() returns .pending:
  1. transitionToIdle() atomically:
     - Sets lifecycle to IDLE
     - Clears the notified bit
     - Returns the PREVIOUS state
  2. If prev.notified was true:
     - Immediately reschedule the task
```

This protocol ensures that even if a waker fires during the window between
`poll()` returning `.pending` and the scheduler transitioning the task to
IDLE, the notification is captured and the task is rescheduled. See the
[memory ordering](/v1.0.0-zig0.15.2/design/memory-ordering) page for the atomic details.

## Comparison with Zig's std.Io

Zig's [`std.Io`](https://github.com/ziglang/zig/tree/master/lib/std/Io) (in development, expected in 0.16) is an explicit I/O interface using method calls — not language keywords. There is no Future/Poll/Waker pattern; `await()` blocks the calling fiber/thread until the operation completes.

| Aspect | Volt | Zig `std.Io` |
|--------|------|-------------|
| State machine | Hand-written futures with Poll/Waker | Backend-managed (Threaded, IoUring, Kqueue) |
| Async call | `io.@"async"(func, .{args})` | `io.async(func, .{args})` |
| Await | `handle.@"await"(io)` | `future.await(io)` (blocking) |
| Cancellation | `handle.cancel()` | `future.cancel(io)` (idempotent with await) |
| Scheduler | Work-stealing with LIFO slot | OS threads (Threaded) |

Volt's polling infrastructure, waker mechanism, and scheduler provide the runtime layer that `std.Io` does not include. See [Stackless vs Stackful](/v1.0.0-zig0.15.2/design/stackless-vs-stackful/) for the full comparison.

## Why Manual State Machines Are Acceptable

While writing futures by hand is more verbose than async/await, several
factors make it practical:

1. **Most users write functions, not futures.** `io.@"async"(fn, args)`
   wraps any function into a single-poll future automatically via `FnFuture`.
   Only library authors implementing sync primitives or I/O operations need
   to write multi-state futures.

2. **State is explicit and auditable.** Every field in the future struct is
   visible. There are no hidden allocations or implicit captures. When
   debugging a hang, you can inspect the state enum to see exactly where
   the future is stuck.

3. **Sizes are known at comptime.** The allocator knows exactly how many
   bytes to allocate for each task. No runtime sizing, no growth, no
   fragmentation.

4. **The pattern is mechanical.** Once you understand the protocol (poll,
   check readiness, register waker, return pending), writing futures is
   straightforward. The complexity is bounded.

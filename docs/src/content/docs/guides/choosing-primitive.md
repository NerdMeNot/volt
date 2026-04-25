---
title: Choosing a Primitive
description: Decision tree for selecting the right synchronization primitive or channel type in Volt.
---

Volt provides six sync primitives and four channel types. Picking the wrong one leads to unnecessary contention, complexity, or subtle bugs. This guide helps you choose.

## Decision Tree

Start here and follow the arrows:

```
Do you need to pass DATA between tasks?
├── YES --> Is it a single value (one producer, one consumer)?
│   ├── YES --> Oneshot
│   └── NO --> Do ALL consumers need every message?
│       ├── YES --> BroadcastChannel
│       └── NO --> Do you only care about the LATEST value?
│           ├── YES --> Watch
│           └── NO --> Channel (bounded MPMC)
│
└── NO --> Do you need to PROTECT shared state?
    ├── YES --> Are reads much more frequent than writes?
    │   ├── YES --> RwLock
    │   └── NO --> Is it a counter / resource pool?
    │       ├── YES --> Semaphore
    │       └── NO --> Mutex
    │
    └── NO --> Do you need to COORDINATE timing?
        ├── YES --> Is it one-time initialization?
        │   ├── YES --> OnceCell
        │   └── NO --> Is it a rendezvous point (N tasks must arrive)?
        │       ├── YES --> Barrier
        │       └── NO --> Notify
        │
        └── NO --> You probably don't need a primitive.
```

:::tip[Looking for `volt.Group`?]
`volt.Group` is not a sync primitive -- it's a structured concurrency tool for spawning and managing multiple tasks. Use it when you need to spawn N tasks and wait for all to complete (or cancel all). See [Basic Concepts](/getting-started/basic-concepts/#structured-concurrency-with-groups).
:::

## Sync Primitives at a Glance

| Primitive | Purpose | Allocation | Fairness |
|-----------|---------|------------|----------|
| `Mutex` | Exclusive access to shared state | None | FIFO |
| `RwLock` | Many readers OR one writer | None | Writer priority |
| `Semaphore` | Limit concurrent access (N permits) | None | FIFO |
| `Notify` | Wake one or all waiting tasks | None | FIFO (one) / All |
| `Barrier` | Wait until N tasks arrive | None | All released together |
| `OnceCell` | Lazy one-time initialization | None | First caller initializes |

## Channel Types at a Glance

| Channel | Pattern | Allocation | Blocking Behavior |
|---------|---------|------------|-------------------|
| `Channel(T)` | MPMC bounded queue | Heap (ring buffer) | Senders wait when full |
| `Oneshot(T)` | Single value delivery | None | Receiver waits for value |
| `BroadcastChannel(T)` | Fan-out to all receivers | Heap (ring buffer) | Lagging receivers skip |
| `Watch(T)` | Latest value broadcast | None | Receivers watch for changes |

## When to Use Notify vs Channel

**Use Notify when:**
- You need to signal "something happened" without data.
- Multiple tasks wait for a condition to become true.
- You want `notifyOne()` (wake one) or `notifyAll()` (wake all) semantics.

```zig
var notify = volt.sync.Notify.init();

// Producer signals that data is ready
notify.notifyOne();

// Consumer waits for signal
var waiter = volt.sync.notify.Waiter.init();
notify.waitWith(&waiter);
```

**Use Channel when:**
- You need to transfer actual data values.
- You want backpressure (bounded queue).
- Order of messages matters.

```zig
var ch = try volt.channel.bounded(Task, allocator, 100);

// Producer sends work
_ = ch.trySend(work_item);

// Consumer receives work
switch (ch.tryRecv()) {
    .value => |task| process(task),
    .empty => {},
    .closed => break,
}
```

## Oneshot vs Watch vs Broadcast

These three are the "specialized channels" for specific patterns.

### Oneshot: Request/Response

Exactly one send, exactly one receive. Zero allocation.

```zig
fn requestResponse(io: volt.Runtime, request: Request) !HttpResponse {
    var os = volt.channel.oneshot(HttpResponse);

    // Spawn a task that computes the response
    var f = try io.@"async"(handleRequest, .{request, &os.sender});
    _ = f;

    // Wait for the response
    var waiter = @import("volt").channel.oneshot_mod.Oneshot(HttpResponse).RecvWaiter.init();
    if (!os.receiver.recvWait(&waiter)) {
        // yield until value arrives
    }
    return waiter.value.?;
}
```

### Watch: Config / State Broadcasting

Single value that changes over time. Receivers only see the latest value. No history.

```zig
var config = volt.channel.watch(AppConfig, default_config);

// Updater thread
config.send(new_config);

// Multiple readers
var rx = config.subscribe();
if (rx.hasChanged()) {
    const cfg = rx.borrow();
    applyConfig(cfg);
    rx.markSeen();
}
```

### Broadcast: Event Fan-Out

Every receiver gets every message. Ring buffer with configurable capacity.

```zig
var events = try volt.channel.broadcast(Event, allocator, 256);
defer events.deinit();

var rx1 = events.subscribe();
var rx2 = events.subscribe();

_ = events.send(.{ .kind = .user_login, .user_id = 42 });

// Both rx1 and rx2 receive the event
```

## Channel vs Shared State

A common question: should I use a `Mutex`-protected struct or a `Channel`?

### Prefer Channel When

- Data flows in one direction (producer to consumer).
- You want backpressure (bounded capacity).
- Tasks don't need to read the same data simultaneously.
- You want to decouple producers from consumers.

### Prefer Shared State (Mutex/RwLock) When

- Multiple tasks need to read AND write the same data.
- You need atomic read-modify-write operations.
- The data is too large or complex to copy through a channel.
- Access patterns are mostly reads (`RwLock`).

### Hybrid Pattern

Use both: channel for commands, shared state for the data.

```zig
const Command = union(enum) {
    get: struct { key: []const u8, reply: *volt.channel.oneshot_mod.Oneshot(Value).Sender },
    put: struct { key: []const u8, value: Value },
};

// Single owner task manages the map
fn mapOwner(ch: *volt.channel.Channel(Command)) void {
    var map = std.AutoHashMap([]const u8, Value).init(allocator);
    while (true) {
        switch (ch.tryRecv()) {
            .value => |cmd| switch (cmd) {
                .get => |g| _ = g.reply.send(map.get(g.key).?),
                .put => |p| map.put(p.key, p.value) catch {},
            },
            .empty => {},
            .closed => return,
        }
    }
}
```

## Performance Comparison

Based on benchmark results (M3 Mac, 8 workers). Note: these numbers use 8 worker threads for contended benchmarks. The [Tokio comparison](/testing/comparing-with-tokio/) uses 4 workers to match Tokio's default configuration, so numbers there will differ.

| Primitive | Uncontended | 8-Thread Contended |
|-----------|------------|-------------------|
| Mutex | 10.2ns | 99.1ns |
| RwLock (read) | 11.0ns | 94.4ns |
| RwLock (write) | 10.2ns | 94.4ns |
| Semaphore | 9.7ns | 162.5ns |
| Notify | 9.9ns | -- |
| Barrier | 15.3ns | -- |
| OnceCell (get) | 0.4ns | -- |
| Channel send | 5.3ns | 190.8ns (MPMC) |
| Channel recv | 3.1ns | -- |
| Oneshot | 4.0ns | -- |
| Broadcast | 23.4ns | -- |
| Watch | 13.0ns | -- |

Key takeaways:
- **OnceCell** is nearly free after initialization (0.4ns read).
- **Channel** has the fastest uncontended send/recv (lock-free ring buffer).
- **Mutex/RwLock/Semaphore** are similar uncontended (~10ns). Under contention, RwLock matches Mutex for read-heavy workloads.
- **MPMC Channel** under heavy contention (190ns) is the most expensive. Consider partitioning or using MPSC mode.

## Common Anti-Patterns

### 1. Using Mutex When OnceCell Suffices

```zig
// BAD: Mutex for one-time init
var mutex = volt.sync.Mutex.init();
var initialized = false;
var resource: ?Resource = null;

fn getResource() *Resource {
    if (mutex.tryLock()) {
        defer mutex.unlock();
        if (!initialized) {
            resource = initResource();
            initialized = true;
        }
    }
    return &resource.?;
}

// GOOD: OnceCell
var cell = volt.sync.OnceCell(Resource).init();

fn getResource() *Resource {
    return cell.getOrInit(initResource);
}
```

### 2. Using Channel(1) Instead of Oneshot

```zig
// BAD: Channel for single-value delivery
var ch = try volt.channel.bounded(Result, allocator, 1);
defer ch.deinit();  // unnecessary allocation

// GOOD: Oneshot (zero allocation)
var os = volt.channel.oneshot(Result);
```

### 3. Using Mutex to Protect a Counter

```zig
// BAD: Mutex around a counter
var mutex = volt.sync.Mutex.init();
var count: usize = 0;
fn increment() void {
    if (mutex.tryLock()) {
        defer mutex.unlock();
        count += 1;
    }
}

// GOOD: Semaphore (designed for counting)
var sem = volt.sync.Semaphore.init(0);
fn increment() void { sem.release(1); }
fn getCount() usize { return sem.availablePermits(); }
```

### 4. Broadcasting When Only Latest Value Matters

```zig
// BAD: Broadcast channel for config updates
// Receivers process every intermediate value, even stale ones.
var bc = try volt.channel.broadcast(Config, allocator, 16);

// GOOD: Watch channel
// Receivers only see the latest config.
var watch = volt.channel.watch(Config, default_config);
```

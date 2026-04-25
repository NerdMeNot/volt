---
title: Channels
description: Message passing between async tasks with Channel, Oneshot, Broadcast, and Watch.
---

Channels are the primary mechanism for passing data between tasks in Volt. Four channel types cover the common communication patterns.

## Channel overview

| Type | Pattern | Allocation | Close required |
|------|---------|------------|----------------|
| `Channel(T)` | Bounded MPMC (multi-producer, multi-consumer) | Yes (ring buffer) | `deinit()` |
| `Oneshot(T)` | Single value, one sender, one receiver | No | No |
| `BroadcastChannel(T)` | Fan-out, all receivers get every message | Yes (ring buffer) | `deinit()` |
| `Watch(T)` | Single value with change notification | No | No |

:::danger[Channel and BroadcastChannel require `deinit()`]
**Rule: if you passed an allocator to create it, you must call `deinit()`.**

```zig
// These allocate — ALWAYS defer deinit()
var ch = try volt.channel.bounded(u32, allocator, 100);
defer ch.deinit();

var bc = try volt.channel.broadcast(Event, allocator, 16);
defer bc.deinit();

// These are zero-allocation — no deinit needed
var os = volt.channel.oneshot(u32);
var wt = volt.channel.watch(Config, defaults);
```

Forgetting `deinit()` on `Channel` or `BroadcastChannel` leaks the ring buffer. See [Common Pitfalls](/guides/common-pitfalls/#forgetting-deinit-on-channel-and-broadcastchannel) for more details.
:::

### Return type differences

Each channel type has different return types for send and receive operations. Don't assume they work the same way.

| Channel | `trySend` returns | `tryRecv` returns |
|---------|------------------|------------------|
| `Channel(T)` | `.ok`, `.full`, `.closed` | `.value`, `.empty`, `.closed` |
| `Oneshot(T)` | `bool` (via `sender.send()`) | `?T` (via `receiver.tryRecv()`) |
| `BroadcastChannel(T)` | `.ok(usize)`, `.closed` | `.value`, `.empty`, `.lagged(usize)`, `.closed` |
| `Watch(T)` | `void` (via `send()`) | Borrow via `rx.borrow()`, check `rx.hasChanged()` |

## Channel (bounded MPMC)

A bounded channel backed by a lock-free Vyukov/crossbeam-style ring buffer. Multiple producers and multiple consumers can operate concurrently. When the buffer is full, senders are suspended (not the OS thread); when empty, receivers are suspended.

### Creation

```zig
const volt = @import("volt");

// Using the factory function
var ch = try volt.channel.bounded(u32, allocator, 100);
defer ch.deinit();

// Or directly
var ch2 = try volt.channel.Channel(u32).init(allocator, 100);
defer ch2.deinit();
```

### Non-blocking API

```zig
// Send
switch (ch.trySend(42)) {
    .ok => {},       // Value was placed in the buffer
    .full => {},     // Buffer is full, try again later
    .closed => {},   // Channel was closed
}

// Receive
switch (ch.tryRecv()) {
    .value => |v| processValue(v),
    .empty => {},     // No values available
    .closed => {},    // Channel closed, no more values
}
```

### Async API

```zig
// Send -- yields until a slot is available
ch.send(io, 42);

// Receive -- yields until a value is available (returns null if closed)
if (ch.recv(io)) |value| {
    processValue(value);
} else {
    // Channel closed and drained
}
```

Pass the `io: volt.Runtime` handle so the channel can yield to the scheduler when the buffer is full (send) or empty (recv) and resume the task when the operation can proceed.

### Advanced: sendFuture / recvFuture

For manual future composition or custom schedulers, use the `Future`-returning variants:

```zig
var send_future = ch.sendFuture(42);
// Poll through your scheduler...

var recv_future = ch.recvFuture();
// When future.poll() returns .ready, you have the value.
```

### Closing

Close the channel to signal that no more values will be sent:

```zig
ch.close();
```

After closing, `trySend` returns `.closed`. `tryRecv` continues to drain buffered values, then returns `.closed`.

### Waiter API

For custom scheduler integration:

```zig
// Receive with waiter
var waiter = volt.channel.channel_mod.RecvWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);
ch.recvWait(&waiter);
// Yield to scheduler. When woken, check waiter.status.load(.acquire):
// WAITER_COMPLETE (1) = success, WAITER_CLOSED (2) = channel closed.

// Send with waiter
var send_waiter = volt.channel.channel_mod.SendWaiter.init();
send_waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);
ch.sendWait(value, &send_waiter);
```

---

## Oneshot

A oneshot channel delivers exactly one value from a sender to a receiver. It is zero-allocation and ideal for returning results from spawned tasks.

### Creation

```zig
var os = volt.channel.oneshot(u32);
// os.sender -- use to send one value
// os.receiver -- use to receive the value
```

### Sending

The sender can send exactly one value. `send` returns `true` if the value was delivered to a waiting receiver or stored for later pickup:

```zig
_ = os.sender.send(42);
```

If the sender is dropped without sending, the receiver sees the channel as closed.

### Receiving

```zig
// Non-blocking
if (os.receiver.tryRecv()) |value| {
    // Got the value
} else {
    // Not sent yet (or sender closed without sending)
}
```

### Async receive

```zig
// Yields until the value arrives or the sender closes
switch (os.receiver.recv(io)) {
    .value => |v| {
        // Got the value
        useResult(v);
    },
    .closed => {
        // Sender dropped without sending
    },
}
```

### Advanced: recvFuture (low-level waiter)

For manual future composition:

```zig
var waiter = volt.channel.oneshot_mod.Oneshot(u32).RecvWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);

if (!os.receiver.recvWait(&waiter)) {
    // Yield to scheduler. Woken when value arrives or sender closes.
}

if (waiter.value) |v| {
    // Got the value
} else if (waiter.closed.load(.acquire)) {
    // Sender closed without sending
}
```

### Pattern: task result delivery

```zig
fn deliverResult(io: volt.Runtime) void {
    var os = volt.channel.oneshot(ComputeResult);

    // Spawn computation
    const f = try io.@"async"(struct {
        fn run() void {
            const result = expensiveComputation();
            _ = os.sender.send(result);
        }
    }.run, .{});
    _ = f;

    // ... do other work ...

    // Collect result (async -- yields until value arrives)
    switch (os.receiver.recv(io)) {
        .value => |result| useResult(result),
        .closed => {},
    }
}
```

---

## BroadcastChannel

A broadcast channel delivers every sent message to **all** subscribed receivers. It uses a ring buffer, and slow receivers that fall behind may miss messages.

### Creation

```zig
var bc = try volt.channel.broadcast(Event, allocator, 16);
defer bc.deinit();
```

The capacity (16) is the size of the ring buffer.

### Subscribing

Create receivers by subscribing to the channel:

```zig
var rx1 = bc.subscribe();
var rx2 = bc.subscribe();
```

Each receiver maintains its own read position in the ring buffer.

### Sending

Sends are non-blocking and never fail (unless the channel is closed):

```zig
_ = bc.send(Event{ .kind = .user_joined, .user_id = 42 });
```

If a receiver is too slow and the ring buffer wraps, that receiver's oldest unread messages are overwritten.

### Receiving

```zig
switch (rx1.tryRecv()) {
    .value => |event| handleEvent(event),
    .empty => {},     // No new messages
    .lagged => |n| {
        // Missed n messages due to slow consumption
        // The next tryRecv will return the oldest available message
    },
    .closed => {},    // Channel was closed
}
```

### Async receive

```zig
// Yields until a new message is available
switch (rx1.recv(io)) {
    .value => |event| handleEvent(event),
    .empty => {},      // No messages available yet
    .lagged => |n| {
        // Missed n messages due to slow consumption
    },
    .closed => {},     // Channel was closed
}
```

### Advanced: recvFuture (low-level waiter)

```zig
var waiter = volt.channel.broadcast_mod.RecvWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);
rx1.recvWait(&waiter);
// Yield until a new message arrives.
```

---

## Watch

A watch channel holds a single value and notifies receivers when it changes. Only the latest value is kept -- there is no history. This is perfect for configuration or state that changes over time.

### Creation

```zig
var watch = volt.channel.watch(Config, default_config);
```

Zero-allocation, no `deinit` required.

### Updating the value

```zig
watch.send(Config{
    .max_connections = 200,
    .timeout_ms = 5000,
});
```

### Observing changes

```zig
var rx = watch.subscribe();

// Check if value changed since last read
if (rx.hasChanged()) {
    const config = rx.borrow();
    applyConfig(config.*);
    rx.markSeen();
}
```

### Async: wait for changes

```zig
// Yields until the value is updated
switch (rx.changed(io)) {
    .changed => {
        const config = rx.borrow();
        applyConfig(config.*);
        rx.markSeen();
    },
    .closed => {},
}
```

### Advanced: changedFuture (low-level waiter)

```zig
var waiter = volt.channel.watch_mod.ChangeWaiter.init();
waiter.setWaker(@ptrCast(&my_ctx), myWakeCallback);
rx.waitForChange(&waiter);
// Yield to scheduler. Woken when value is updated.
```

### Pattern: config hot-reload

```zig
var config_watch = volt.channel.watch(AppConfig, default_config);

// Config reloader task
fn reloadLoop() void {
    while (running) {
        const new_config = loadConfigFromFile("config.toml");
        config_watch.send(new_config);
        sleep(Duration.fromSecs(30));
    }
}

// Worker task
fn workerLoop() void {
    var rx = config_watch.subscribe();
    while (running) {
        if (rx.hasChanged()) {
            const cfg = rx.borrow();
            updateWorkerSettings(cfg.*);
            rx.markSeen();
        }
        doWork();
    }
}
```

---

## Backpressure and Slow Consumers

Bounded channels provide natural **backpressure**: when the buffer is full, senders are forced to slow down. Understanding this behavior is essential for building reliable pipelines.

### What happens when the channel is full

| API | Behavior when full |
|-----|-------------------|
| `trySend(val)` | Returns `.full` immediately -- the caller decides what to do |
| `send(io, val)` | Suspends the task until a slot opens (another task calls `tryRecv`/`recv`) |
| `sendFuture(val)` | Returns a future that resolves when the value is accepted |

### Strategies for handling full channels

**Drop oldest (lossy).** If freshness matters more than completeness (e.g., metrics, telemetry), use `trySend` and discard on `.full`:

```zig
switch (ch.trySend(metric)) {
    .ok => {},
    .full => {}, // Drop this metric -- better than blocking the producer
    .closed => return,
}
```

**Exponential backoff.** For producers that should slow down but not block forever:

```zig
var delay = volt.Duration.fromMillis(1);
while (true) {
    switch (ch.trySend(item)) {
        .ok => break,
        .full => {
            // Back off, doubling each time (capped at 1 second)
            const sleep_f = volt.time.sleep(delay);
            _ = sleep_f;
            delay = volt.Duration.fromNanos(@min(
                delay.toNanos() * 2,
                volt.Duration.fromSecs(1).toNanos(),
            ));
        },
        .closed => return,
    }
}
```

**Blocking send.** When every message matters, use `send(io, val)` to let the runtime manage the wait:

```zig
ch.send(io, important_event); // Suspends until space is available
```

### Deadlocks with multiple channels

A classic deadlock occurs when two tasks send to each other through full channels:

```
Task A: ch_x.send(io, val);  // Blocks -- ch_x is full
         ch_y.recv(io);       // Never reached
Task B: ch_y.send(io, val);  // Blocks -- ch_y is full
         ch_x.recv(io);       // Never reached
```

**Prevention:** Ensure at least one direction uses `trySend` with a fallback, or design your pipeline as a DAG (no cycles between channels).

---

## Choosing the right channel

| Need | Channel type |
|------|-------------|
| Work queue with backpressure | `Channel` (bounded MPMC) |
| Return a single result from a task | `Oneshot` |
| Configuration that changes at runtime | `Watch` |
| Event fan-out to multiple consumers | `BroadcastChannel` |
| Multiple producers, single consumer | `Channel` |
| Multiple producers, multiple consumers | `Channel` |
| Fire-and-forget notifications | `BroadcastChannel` |
| Latest-value observation | `Watch` |

### Resource management summary

| Type | Needs allocator | Needs `deinit()` |
|------|:---------------:|:----------------:|
| `Channel(T)` | Yes | Yes |
| `BroadcastChannel(T)` | Yes | Yes |
| `Oneshot(T)` | No | No |
| `Watch(T)` | No | No |

### Performance notes

- `Channel` uses a lock-free ring buffer for `trySend`/`tryRecv`. The waiter queue uses a separate mutex that never touches the data path.
- `Oneshot` is entirely lock-free (atomic state machine).
- `BroadcastChannel` uses atomic sequence numbers per slot for lock-free reads.
- `Watch` uses a version counter with atomic compare-and-swap.

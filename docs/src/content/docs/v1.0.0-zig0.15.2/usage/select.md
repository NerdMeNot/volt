---
title: Select
description: Waiting on multiple futures simultaneously with Select, Join, Race,
  and Timeout combinators.
slug: v1.0.0-zig0.15.2/usage/select
---

When a task needs to wait on multiple async operations and respond to whichever completes first, Volt provides several patterns using `io.@"async"` and `Group`.

## Pattern overview

| Pattern | Description | Use Case |
|---------|-------------|----------|
| Race two `@"async"` tasks | First to complete wins | Accept vs shutdown signal |
| `Group` with multiple spawns | Wait for all to complete | Parallel fetches |
| `io.@"async"` + `isDone()` | Poll multiple futures | Custom select logic |
| `time.Deadline` | Timeout on any operation | Read with timeout |

## Racing two async operations

Race two operations of different types by spawning both as async tasks and checking which completes first:

```zig
const volt = @import("volt");

// Race accept against a shutdown signal
var accept_f = try io.@"async"(acceptConnection, .{listener});
var signal_f = try io.@"async"(waitForSignal, .{&shutdown_handler});

// First to complete wins -- check isDone()
if (accept_f.isDone()) {
    const conn = accept_f.@"await"(io);
    handleConnection(conn);
} else if (signal_f.isDone()) {
    const sig = signal_f.@"await"(io);
    log.info("Shutdown: {}", .{sig});
}
```

## Group: wait for all

Use `Group` to spawn multiple tasks and wait for all to complete:

```zig
var group = volt.Group.init(io);

// Spawn parallel fetches
_ = group.spawn(fetchUser, .{user_id});
_ = group.spawn(fetchPosts, .{user_id});

// Wait for all to finish
group.wait();
```

## Parallel fetch with individual results

Spawn individual futures when you need each result:

```zig
var user_f = try io.@"async"(fetchUser, .{user_id});
var posts_f = try io.@"async"(fetchPosts, .{user_id});

const user = user_f.@"await"(io);
const posts = posts_f.@"await"(io);
renderProfile(user, posts);
```

## Race N homogeneous operations

Race multiple operations of the same type:

```zig
// Spawn all fetches
var futures: [3]@TypeOf(io.@"async"(fetch, .{url1}) catch unreachable) = undefined;
futures[0] = try io.@"async"(fetch, .{url1});
futures[1] = try io.@"async"(fetch, .{url2});
futures[2] = try io.@"async"(fetch, .{url3});

// Check which completes first
for (futures) |*f| {
    if (f.isDone()) {
        const value = f.@"await"(io);
        useFirstResult(value);
        break;
    }
}
```

## Timeout

Use `time.Deadline` to implement timeouts on any async operation:

```zig
const Duration = volt.time.Duration;

var deadline = volt.time.Deadline.init(Duration.fromSecs(5));

// Spawn the operation
var read_f = try io.@"async"(readFromStream, .{stream, &buf});

// Check deadline
if (deadline.isExpired()) {
    return error.TimedOut;
}

const result = read_f.@"await"(io);
handleReadResult(result);
```

## SelectContext (low-level)

For advanced use cases or custom select implementations, `SelectContext` provides the coordination primitive that powers `Select`. It uses atomic operations and a `CancelToken` to ensure exactly one branch wins.

```zig
const SelectContext = volt.sync.select_context.SelectContext;

// Create a context for 3 branches
var ctx = SelectContext.init(3);

// Set the external waker (your task's waker)
ctx.setWaker(@ptrCast(&my_task), myWakeCallback);

// Create branch wakers to register with channels
var waker0 = ctx.branchWaker(0);
var waker1 = ctx.branchWaker(1);
var waker2 = ctx.branchWaker(2);

// Register with channels (pass waker function and context)
channel1.recvWait(&recv_waiter1);
recv_waiter1.setWaker(waker0.wakerCtx(), waker0.wakerFn());

channel2.recvWait(&recv_waiter2);
recv_waiter2.setWaker(waker1.wakerCtx(), waker1.wakerFn());

// When any channel has data, the branch waker fires,
// which calls branchReady(branch_id) on the SelectContext.
// The first branch to call branchReady wins (claims the CancelToken).

// Check the result
if (ctx.isComplete()) {
    const winner = ctx.getWinner().?;
    switch (winner) {
        0 => handleChannel1(channel1.tryRecv()),
        1 => handleChannel2(channel2.tryRecv()),
        2 => handleTimeout(),
        else => unreachable,
    }
}

// Reset for reuse
ctx.reset();
```

### BranchWaker

Each `BranchWaker` wraps a `SelectContext` and a branch ID. When its waker function is called (by a channel, signal handler, or timer), it atomically claims the `CancelToken`. If it wins, the `SelectContext` is marked complete and the external task is woken.

```zig
var bw = ctx.branchWaker(0);

bw.isWinner();     // true if this branch won
bw.isCancelled();  // true if another branch won
```

### Selector (channel-specific)

The `channel.Selector` type provides a higher-level API specifically for selecting across multiple channels:

```zig
const channel = volt.channel;

var ch1 = try channel.bounded(u32, allocator, 10);
var ch2 = try channel.bounded([]const u8, allocator, 10);

var sel = channel.selector();
const branch0 = sel.addRecv(u32, &ch1);
const branch1 = sel.addRecv([]const u8, &ch2);

const result = sel.selectBlocking();
switch (result.branch) {
    branch0 => {
        if (ch1.tryRecv()) |v| processNumber(v.value);
    },
    branch1 => {
        if (ch2.tryRecv()) |v| processString(v.value);
    },
    else => {},
}
```

## Pattern: server with graceful shutdown

The most common pattern races `accept` against a shutdown signal using async tasks:

```zig
fn serverLoop(
    io: volt.Io,
    listener: *volt.net.TcpListener,
    shutdown: *volt.shutdown.Shutdown,
) !void {
    while (!shutdown.isShutdown()) {
        // Race accept against shutdown
        var accept_f = try io.@"async"(acceptConnection, .{listener});
        var shutdown_f = try io.@"async"(waitForShutdown, .{shutdown});

        // Check which completed first
        if (accept_f.isDone()) {
            const conn = accept_f.@"await"(io);
            var guard = shutdown.startWork();
            _ = io.@"async"(handleConn, .{ conn, &guard }) catch {
                guard.complete();
                continue;
            };
        } else if (shutdown_f.isDone()) {
            log.info("Shutdown signal received", .{});
            break;
        }
    }
}
```

## Pattern: channel + timeout

Wait for a channel message with a timeout:

```zig
const Duration = volt.time.Duration;

var deadline = volt.time.Deadline.init(Duration.fromSecs(10));
var recv_f = try io.@"async"(recvFromChannel, .{channel});

if (deadline.isExpired()) {
    handleTimeout();
} else {
    const msg = recv_f.@"await"(io);
    handleMessage(msg);
}
```

## Pattern: first successful fetch

Race multiple fetch operations, use whichever responds first:

```zig
var primary_f = try io.@"async"(fetchFromPrimary, .{key});
var replica_f = try io.@"async"(fetchFromReplica, .{key});

// First to complete wins
if (primary_f.isDone()) {
    return primary_f.@"await"(io);
} else if (replica_f.isDone()) {
    return replica_f.@"await"(io);
}
```

## Limits

`SelectContext` supports up to `MAX_SELECT_BRANCHES` (16) branches. This limit avoids dynamic allocation -- all branch state is stack-allocated.

---
title: Common Pitfalls
description: Footguns, gotchas, and mistakes that trip up developers new to Volt -- with fixes for each.
sidebar:
  order: 0
---

Every issue on this page is something a real developer will hit within their first hour using Volt. Read this before you ship anything, or bookmark it for when something goes wrong.

## The `@"async"` / `@"await"` Syntax

Zig reserves `async` and `await` as keywords. Volt uses Zig's standard identifier quoting syntax to work around this:

```zig
// Read these as "io dot async" and "future dot await"
const future = try io.@"async"(myFunc, .{args});
const result = future.@"await"(io);
```

This is not Volt-specific -- it is standard Zig. Any reserved keyword can be used as an identifier with `@""` quoting. You will see this throughout every Volt example.

:::tip
If your editor highlights `@"async"` oddly, that is an editor issue, not a Volt issue. The code is valid Zig.
:::

---

## Forgetting `deinit()` on Channel and BroadcastChannel

**Rule: if you passed an allocator, you must call `deinit()`.**

`Channel` and `BroadcastChannel` allocate a ring buffer. `Oneshot` and `Watch` are zero-allocation. The consequence of forgetting `deinit()` is a memory leak.

```zig
// BAD: leaks the ring buffer
var ch = try volt.channel.bounded(u32, allocator, 100);
// ... use ch ...
// oops, forgot deinit

// GOOD: defer deinit immediately after creation
var ch = try volt.channel.bounded(u32, allocator, 100);
defer ch.deinit();

// Oneshot and Watch: no deinit needed
var os = volt.channel.oneshot(u32);       // zero allocation
var wt = volt.channel.watch(Config, cfg); // zero allocation
```

| Type | Needs allocator | Needs `deinit()` |
|------|:-:|:-:|
| `Channel(T)` | Yes | Yes |
| `BroadcastChannel(T)` | Yes | Yes |
| `Oneshot(T)` | No | No |
| `Watch(T)` | No | No |

---

## The Three-Way I/O Pattern

Every non-blocking read/accept/recv in Volt returns one of **four** outcomes. If you handle only two (data and error), your server will have bugs.

| Result | Meaning | What to do |
|--------|---------|-----------|
| `n > 0` | Got data | Process it |
| `null` | Would block | Retry later (`orelse continue`) |
| `n == 0` | Peer closed (FIN) | Clean up and return |
| `error` | Connection reset / failure | Log and return |

**Copy-paste template** -- use this pattern for every `tryRead`/`tryAccept`/`tryRecv`:

```zig
while (true) {
    const n = stream.tryRead(&buf) catch |err| {
        // Connection error (RST, broken pipe, etc.)
        log.err("read error: {}", .{err});
        return;
    } orelse continue; // Would block -- retry

    if (n == 0) return; // Peer disconnected (FIN)

    // Process buf[0..n]
    processData(buf[0..n]);
}
```

The idiomatic one-liner:

```zig
const n = stream.tryRead(&buf) catch return orelse continue;
if (n == 0) return;
```

:::caution
The most common bug: treating `null` (would-block) as an error or EOF. It just means "no data right now, try again."
:::

---

## Operations That Block the Thread

Some Volt APIs are **synchronous** -- they block the OS thread, not just the task. If you call them on a worker thread, every other task on that worker stalls.

### Blocking APIs (run on main thread or blocking pool)

| API | What it does | Async alternative |
|-----|-------------|-------------------|
| `volt.run(fn)` | Blocks main thread until runtime exits | N/A (this is intentional) |
| `volt.net.resolve()` | DNS lookup via `getaddrinfo` | Wrap in `io.concurrent()` |
| `volt.fs.readFile()` | Synchronous file read | `io.concurrent(volt.fs.readFile, .{...})` |
| `volt.fs.writeFile()` | Synchronous file write | `io.concurrent(volt.fs.writeFile, .{...})` |
| `volt.fs.File.open()` | Synchronous file open | `io.concurrent(...)` |

### Safe on worker threads

| API | Why it's safe |
|-----|--------------|
| `mutex.lock(io)` | Yields task to scheduler, doesn't block thread |
| `ch.send(io, val)` | Yields task to scheduler |
| `ch.recv(io)` | Yields task to scheduler |
| `sem.acquire(io, n)` | Yields task to scheduler |
| `volt.time.sleep(dur)` | Yields task to scheduler |
| `stream.tryRead()` | Non-blocking (returns null if would block) |
| `stream.tryWrite()` | Non-blocking |
| `mutex.tryLock()` | Non-blocking (returns immediately) |

:::tip[Rule of thumb]
If it takes `io: volt.Io`, it yields the task. If it doesn't take `io`, check whether it does I/O -- if so, wrap it in `io.concurrent()`.
:::

---

## DNS Resolution Is Blocking

`volt.net.resolve()` and `volt.net.resolveFirst()` call the system's `getaddrinfo`, which blocks the calling thread. On a worker thread, this stalls all tasks on that worker.

**Fix: wrap DNS calls in `io.concurrent()`:**

```zig
// BAD: blocks the worker thread during DNS lookup
const addr = try volt.net.resolveFirst(allocator, "example.com", 443);

// GOOD: runs on the blocking pool
const handle = try io.concurrent(struct {
    fn resolve() !volt.net.Address {
        return volt.net.resolveFirst(std.heap.page_allocator, "example.com", 443);
    }
}.resolve, .{});
const addr = try handle.wait();
```

Note: `volt.net.connectHost()` already handles this internally. Prefer it for simple client connections:

```zig
var stream = try volt.net.connectHost(allocator, "example.com", 443);
```

---

## Using `const` Instead of `var` for Futures

Futures are mutated during polling -- `.@"await"(io)` calls `poll()` internally, which updates the future's state machine. Declaring a future as `const` is a compile error.

```zig
// BAD: won't compile -- @"await" mutates the future
const f = try io.@"async"(myFunc, .{});
const result = f.@"await"(io);

// GOOD: use var
var f = try io.@"async"(myFunc, .{});
const result = f.@"await"(io);
```

This also applies to sync primitive futures:

```zig
// BAD
const lock_future = mutex.lockFuture();

// GOOD
var lock_future = mutex.lockFuture();
```

---

## Discarding Futures Silently Loses Panics

When you spawn a task with `io.@"async"()` and discard the returned future, any panic in that task is silently lost. The task runs and panics, but nobody observes it.

```zig
// BAD: if processItem panics, nobody knows
_ = try io.@"async"(processItem, .{item});

// GOOD: await the result (or use a Group)
var f = try io.@"async"(processItem, .{item});
f.@"await"(io); // Panic surfaces here

// GOOD: Group tracks all tasks
var group = volt.Group.init(io);
_ = group.spawn(processItem, .{item1});
_ = group.spawn(processItem, .{item2});
group.wait(); // Panics surface here
```

Discarding futures is fine for fire-and-forget tasks that you are confident won't fail. But if in doubt, keep the handle.

---

## Blocking the Worker Thread

Worker threads run the cooperative scheduler. If you block one, every task assigned to it stops making progress.

**Things that block the worker thread:**

| Bad | Good alternative |
|-----|-----------------|
| `std.Thread.sleep(ns)` | `volt.time.sleep(Duration)` (yields to scheduler) |
| Tight CPU loop | `io.concurrent(fn, args)` (blocking pool) |
| Synchronous file I/O (`std.fs`) | `io.concurrent(fn, args)` or Volt's `fs` module |
| `std.net` blocking calls | Volt's `net` module with `tryX()` APIs |

```zig
// BAD: blocks the worker for 1 second
std.Thread.sleep(1_000_000_000);

// GOOD: yields the task, worker runs other tasks
const sleep_future = volt.time.sleep(volt.Duration.fromSecs(1));
_ = sleep_future; // Poll to completion in your async context
```

---

## Channel API Return Types Differ by Type

Each channel type has different return types for send and receive. Don't assume they're the same.

| Channel | `trySend` returns | `tryRecv` returns |
|---------|------------------|------------------|
| `Channel(T)` | `.ok`, `.full`, `.closed` | `.value`, `.empty`, `.closed` |
| `Oneshot(T)` | `bool` (via `sender.send()`) | `?T` (via `receiver.tryRecv()`) |
| `BroadcastChannel(T)` | `.ok(usize)`, `.closed` | `.value`, `.empty`, `.lagged(usize)`, `.closed` |
| `Watch(T)` | `void` (via `send()`) | Borrow via `rx.borrow()`, check `rx.hasChanged()` |

The async convenience APIs also differ:

| Channel | `recv(io)` returns |
|---------|-------------------|
| `Channel(T)` | `?T` (null if closed) |
| `Oneshot(T)` | `RecvResult`: `.value` or `.closed` |
| `BroadcastChannel(T)` | `RecvResult`: `.value`, `.empty`, `.lagged`, `.closed` |
| `Watch(T)` | `ChangedResult`: `.changed` or `.closed` (via `rx.changed(io)`) |

---

## Summary Checklist

Before shipping, verify:

1. Every `Channel` and `BroadcastChannel` has a matching `defer ch.deinit()`
2. Every `tryRead`/`tryAccept` handles all four outcomes (data, null, zero, error)
3. No `std.Thread.sleep` or raw blocking I/O on worker threads
4. DNS resolution (`net.resolve`) is wrapped in `io.concurrent()`
5. Futures you care about are awaited, not discarded with `_ =`

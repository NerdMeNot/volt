---
title: Common Pitfalls
description: The mistakes that bite every Volt developer at least once. Read before shipping.
---

These are the failure modes that catch people. The runtime panics
loudly for most of them — it'd rather give you a clear error than
silently corrupt state — but understanding *why* they panic helps
you fix them faster.

## Calling Volt code outside `volt.run`

```zig
pub fn main() !void {
    try volt.sleep(volt.Duration.fromSecs(1));   // PANIC
}
```

Every Volt-suspending call panics if it can't find a current
runtime in TLS. The error message tells you exactly what happened:

```
thread panic: volt.sleep called outside a runtime
```

Fix: wrap your entry point in `volt.run`:

```zig
pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, app, .{});
}
fn app() !void {
    try volt.sleep(volt.Duration.fromSecs(1));
}
```

This applies to library code too. If you write a library that uses
Volt internally, your library's users have to call `volt.run`
themselves — there's no way to hide it.

## Forgetting `destroyJob` / `destroyTask`

```zig
const j = try volt.launch(work, .{});
try j.join();
// ← Job handle leaks!
```

The `*Job` handle is heap-allocated. The runtime owns the
coroutine; *you* own the handle. Always pair `launch` / `spawn`
with `destroyJob` / `destroyTask`:

```zig
const j = try volt.launch(work, .{});
defer volt.destroyJob(j);
try j.join();
```

Better: use `volt.scope` and never see a Job handle at all:

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(work, .{});
    }
}.body);
```

## Holding a Mutex across a suspension

```zig
mu.lock();
defer mu.unlock();
const v = try ch.recv();   // suspends WHILE HOLDING THE LOCK
process(v);
```

This works (Volt locks are coroutine-aware), but it usually means
you got the design wrong. Holding a lock across a suspension can
serialize unrelated coroutines that just want the lock, including
the producer that would have sent into `ch`.

The fix is almost always:

```zig
const v = try ch.recv();   // outside the lock
mu.lock();
defer mu.unlock();
process(v);
```

If you genuinely need to hold the lock across the suspension
(e.g., guarded enqueue with notify), that's a Notify pattern, not
a Mutex pattern.

## Spawning instead of scoping

```zig
fn parent() !void {
    _ = try volt.launch(child, .{});   // ← outlives parent
    return;
}
```

`volt.launch` returns a `*Job`. If you don't keep it and join it,
the child outlives the parent — you've created a fire-and-forget
that may still be running when the parent's caller assumes it's
done. Resources the child references can be freed underneath it.

For static-N children: use `volt.scope`. For dynamic-N: use
`volt.JoinSet`. Reach for `volt.launch` only when the child
genuinely needs to outlive the parent (e.g., per-connection
handlers in a TCP server).

## Using `std.Thread.sleep` inside a coroutine

```zig
fn worker() void {
    std.Thread.sleep(1_000_000_000);   // ← blocks the WORKER, not just this coroutine
}
```

`std.Thread.sleep` blocks the OS thread. That OS thread is a Volt
worker; while it's blocked, no other coroutines on that worker
make progress.

Use `volt.sleep`:

```zig
try volt.sleep(volt.Duration.fromSecs(1));
```

Same applies to any blocking call in `std.posix.*` or `std.Thread.*`
— if it blocks, it blocks a worker. Either swap to a Volt-aware
equivalent or wrap the call in `volt.spawnBlocking`.

## Calling `std.posix.read` / `write` directly on a registered fd

```zig
const conn = try listener.accept();
const n = try std.posix.read(conn.fd, &buf);   // ← BAD; bypasses reactor
```

`TcpStream.read` does the non-blocking read + reactor wait dance.
Calling `std.posix.read` directly returns `EWOULDBLOCK` on the
non-blocking fd — your code would have to do the wait itself.
Use `conn.read(&buf)` instead.

## Mutating `args` after `volt.launch`

```zig
var args = MyArgs{ .x = 1 };
const j = try volt.launch(handler, .{&args});
args.x = 2;   // ← did the handler observe x=1 or x=2?
try j.join();
```

`volt.launch` and `volt.spawn` copy the args tuple by value into
the coroutine's stack. Pointer arguments are copied too — the
*pointer* is captured, but it still points at the caller's
mutable storage. So the example above is racy: the handler sees
whatever `args.x` happens to be when it reads.

If you need a snapshot: copy `args.x` into a local before passing
the pointer, or pass the value directly (not via pointer).

## Sending to a closed channel

```zig
ch.close();
try ch.send(42);   // returns error.Closed
```

Not actually a bug per se — `error.Closed` is a normal return
value. But programs sometimes treat `error.Closed` as fatal when
it's actually expected (e.g., the receiver finished early). The
right pattern:

```zig
ch.send(42) catch |err| switch (err) {
    error.Closed => return,    // receiver gone; we're done
    error.Cancelled => return,
};
```

## Multi-Volt-runtime mistakes

```zig
try volt.run(.{ .allocator = a }, outer, .{});

fn outer() !void {
    try volt.run(.{ .allocator = a }, inner, .{});   // ← PANIC
}
```

You can't nest `volt.run`. The runtime uses TLS to track current
coroutine / worker / runtime; the inner `volt.run` would clobber
the outer's TLS. If you need multiple "Volt-like" islands, run
them in separate processes.

## Spawning from a non-coroutine thread

```zig
const t = std.Thread.spawn(.{}, struct {
    fn run() void {
        _ = volt.launch(work, .{});   // PANIC
    }
}.run, .{});
```

`volt.launch` requires a current runtime in TLS, which only exists
on coroutines and Volt workers. If you need to send work into Volt
from outside (e.g., from a callback in another runtime), use a
`Channel(T).trySend` (lock-free, callable anywhere) and have a
Volt coroutine consume from it.

## Forgetting to deinit a Channel / Broadcast / JoinSet

```zig
fn root() !void {
    var ch = try volt.channel.Channel(u32).init(alloc, 64);
    // ... use ch ...
    // ← never deinit'd; ring buffer + waiter list leak
}
```

Always:

```zig
var ch = try volt.channel.Channel(u32).init(alloc, 64);
defer ch.deinit();
```

Same for `Broadcast`, `JoinSet`, `Watch`, `Mutex` — though the
last three are zero-allocation, so their `deinit` is a no-op or
defensive assertion (Watch).

## Cancelling without joining

```zig
j.cancel();
volt.destroyJob(j);   // ← coroutine may still be running!
```

`cancel()` sets a flag and unparks the coroutine. The coroutine
hasn't finished — it's about to wake up and observe the cancel.
Destroying its handle now (and its underlying Coroutine) leads to
use-after-free.

Always:

```zig
j.cancel();
_ = j.join() catch {};
volt.destroyJob(j);
```

## Concurrent Job/Task on the same join_park

The Park inside a coroutine's `join_park` is single-waiter. If two
different coroutines both `j.join()` the same Job concurrently,
the second panics with `concurrent waiter on Park`.

Fix: only one coroutine should `join` a given Job. If you need
multiple consumers to wait for completion, use a `Notify` or
`Oneshot` instead.

## Long-running CPU loops without yield

```zig
fn cpuLoop() void {
    while (true) {
        for (data) |x| heavyComputation(x);
        // never yields, never suspends
    }
}
```

This blocks the worker indefinitely. Other coroutines on that
worker never run. Cancellation can never propagate (no suspension
point to surface `error.Cancelled`).

Fixes:

- Add `try volt.yield();` periodically — gives other coroutines a
  chance and acts as a cancellation point.
- Move the work to `volt.spawnBlocking` — runs on a dedicated
  pool thread, doesn't tie up a worker.

## Using `volt.io.read(fd, buf)` instead of `stream.read(buf)`

```zig
const n = try volt.io.lowlevel.read(conn.fd, &buf);
```

This works but it's the wrong layer. `lowlevel` is for FFI and
custom-fd integrations; `TcpStream.read` is what application code
should use. The difference is documentation more than behavior —
new readers of your code will be confused why you're reaching
through to `.fd`.

## "It works in tests but hangs in production"

Almost always one of:

- A `Mutex` deadlock you didn't trip in single-threaded tests
  because two coroutines never raced on the same lock.
- A `Channel` that's never closed; receivers park forever.
- A `Job.join()` on something that errored out before the join
  could see it (rare; usually a misuse of structured concurrency).

Run with `--workers 1 --deterministic` for tests:

```zig
try volt.run(.{
    .allocator = std.testing.allocator,
    .deterministic = true,
}, test_root, .{});
```

That makes test traces reproducible. Then add multi-worker stress
tests separately to surface concurrency-only bugs.

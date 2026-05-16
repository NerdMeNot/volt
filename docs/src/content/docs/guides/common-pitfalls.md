---
title: Common pitfalls
description: The mistakes that bite every Volt developer at least once. Read before shipping.
---

These are the failure modes that catch people. The runtime panics
loudly for most of them — it'd rather give you a clear error than
silently corrupt state — but understanding *why* helps you fix
them faster.

## Calling Volt code outside `Runtime.run`

```zig
pub fn main() !void {
    volt.sleep(1 * std.time.ns_per_s);   // PANIC
}
```

Every Volt-suspending call panics if it can't find a current
coroutine in TLS. The error is some variant of:

```
thread panic: not in a coroutine
```

Fix: wrap your entry point in `Runtime.run`:

```zig
pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(app, .{}));
}
fn app() !void {
    volt.sleep(1 * std.time.ns_per_s);
}
```

This applies to library code too. A function that uses `volt.spawn`
or `volt.sleep` only works inside a coroutine; document the
constraint at the function level.

## Forgetting to join a Task

```zig
const t = try volt.spawn(work, .{});
// ← never joined: Task struct leaks
```

`volt.spawn` returns `*Task(T)` — heap-allocated. `t.join()`
parks until the coroutine completes **and frees the Task
struct**. If you don't join, the Task struct leaks.

For one child:

```zig
const t = try volt.spawn(work, .{});
_ = t.join();
```

For multiple, the structured pattern is `volt.scope` + explicit
joins inside:

```zig
try volt.scope(struct {
    fn body(c: *volt.Cancel) anyerror!void {
        _ = c;
        const a = try volt.spawn(workA, .{});
        const b = try volt.spawn(workB, .{});
        _ = a.join();
        _ = b.join();
    }
}.body);
```

Volt does not ship a "detach" primitive. Fire-and-forget patterns
genuinely leak Tasks; bound the leak by scoping the spawns
inside a scope that joins them, or by capping concurrent spawns
with a Semaphore.

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

## Calling `Task.join` from outside a coroutine

```zig
pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = a });
    defer rt.deinit();
    const t = try rt.spawn(work, .{});
    _ = t.join();   // PANIC — not in a coroutine
}
```

`Task.join` parks on the parking lot, which only works inside a
coroutine. `Runtime.run` does its internal join from inside its
own context — that's the only way to bridge.

```zig
pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = a });
    defer rt.deinit();
    try (try rt.run(work, .{}));   // run handles the bridge
}
```

If you need to coordinate from outside the runtime, use a
`std.Thread.Mutex` + `std.Thread.Condition`-style pattern in the
non-coroutine code, with a coroutine doing the final notify.

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
volt.sleep(1 * std.time.ns_per_s);
```

Same applies to any blocking syscall on a Volt-registered fd —
if it blocks, it blocks a worker. Either swap to a Volt-aware
equivalent (`volt.net.TcpStream.read` instead of
`std.posix.read`) or bridge via `std.Thread.spawn` + a Mpmc
channel (see [Offloading CPU work](/cookbook/work-offload/)).

## Calling `std.posix.read` / `write` directly on a Volt-managed fd

```zig
const conn = try listener.accept();
const n = try std.posix.system.read(conn.fd, &buf);   // BAD; bypasses reactor
```

`TcpStream.read` does the non-blocking read + reactor wait dance.
Calling the raw syscall directly returns `EAGAIN` on the
non-blocking fd — your code would have to do the wait itself.
Use `conn.read(&buf)`.

## Mutating `args` after `volt.spawn`

```zig
var args = MyArgs{ .x = 1 };
const t = try volt.spawn(handler, .{&args});
args.x = 2;   // ← does the handler observe x=1 or x=2?
_ = t.join();
```

`volt.spawn` copies the args tuple by value into the coroutine's
Frame. Pointer arguments are copied as pointers — the *pointer*
is captured, but it still points at the caller's mutable storage.
So the example above is racy: the handler reads whatever
`args.x` happens to be at read time.

If you need a snapshot: copy `args.x` into a local before passing
the pointer, or pass the value directly (not via pointer). For
larger structs, copy by value into the args tuple itself —
`volt.spawn(handler, .{args})` (no `&`) copies the struct.

## Sending to a closed channel

```zig
ch.close();
try ch.send(42);   // returns error.Closed
```

Not a bug per se — `error.Closed` is a normal return value. But
programs sometimes treat `error.Closed` as fatal when it's
expected (e.g., the receiver finished early). The right pattern
on send paths that might race with close:

```zig
ch.send(42) catch |err| switch (err) {
    error.Closed => return,    // receiver gone; we're done
};
```

`Oneshot.send` similarly returns `error.Closed` on second send
or after close. The `catch {}` idiom is the common form for
race-style fan-out.

## Multi-Runtime mistakes

```zig
var rt = try volt.Runtime.init(...);
try (try rt.run(outer, .{}));

fn outer() !void {
    var rt2 = try volt.Runtime.init(...);   // creates a second Runtime
    try (try rt2.run(inner, .{}));          // ← may panic or deadlock
}
```

Nesting `Runtime.run` calls within the same thread doesn't work —
the threadlocal current-coroutine pointer is shared between
runtimes; the inner runtime can't tell which coroutine is "live"
on the M[0] that's already inside `outer`'s dispatch.

If you need multiple runtimes, run them on **different OS
threads** (each thread becomes its own `M[0]` for its runtime).
For the typical use case (one process = one runtime), just don't
nest.

## Spawning from a non-coroutine thread

```zig
const t = std.Thread.spawn(.{}, struct {
    fn run() void {
        _ = volt.spawn(work, .{});   // PANIC — not in a coroutine
    }
}.run, .{}) catch unreachable;
```

`volt.spawn` requires a current coroutine in TLS — only exists on
Volt workers. From a non-coroutine thread, use `rt.spawn(...)`
which takes a `*Runtime` handle and routes directly through the
slab arena (no per-P pool fast path):

```zig
const t = std.Thread.spawn(.{}, struct {
    fn run(rt: *volt.Runtime) void {
        _ = rt.spawn(work, .{}) catch unreachable;
    }
}.run, .{rt_ptr}) catch unreachable;
```

`rt.spawn` is the cross-thread injection door. Use sparingly.

## Forgetting to deinit a Watch / Broadcast / channel

```zig
fn root() !void {
    var b = volt.Broadcast(Event, 64).init();
    // ... use b ...
    // ← never deinit'd; per-receiver waiter lists leak
}
```

Always:

```zig
var b = volt.Broadcast(Event, 64).init();
defer b.deinit();
```

Same for `Watch`. `Spsc` and `Mpmc` and `Oneshot` are
zero-allocation (their `.{}` init form has no allocation to
clean up), but the convention is to call `deinit` anyway — future
versions might add bookkeeping.

## Cancelling without ensuring children observe

```zig
c.fire();
// ← children may still be running; firing Cancel doesn't wait
```

`Cancel.fire()` flips the flag and unparks every coroutine
parked on a cancel-aware op with the Cancel. But:

1. Children that aren't parked yet (running CPU, between syscalls)
   won't observe the cancel until they hit the next cancel-aware
   op or `checkpoint`.
2. Children parked on **non-cancel-aware** ops (`volt.sleep`,
   any I/O) don't wake from Cancel at all.

The typical pattern: fire the Cancel, then join each child to
wait for unwind:

```zig
c.fire();
_ = t1.join();
_ = t2.join();
```

If children might be stuck on a non-cancel-aware op, you have to
break them out another way (e.g., close their fd).

## Concurrent Task.join on the same Task

The Task's `done` flag has at most one parked waiter slot. If two
coroutines both `t.join()` on the same Task, behaviour is
undefined — likely a panic, possibly a missed wake.

Fix: a given Task is joined by exactly one coroutine. If you
need fan-out of "task is done", have the task signal a `Notify`
that multiple coroutines can `wait` on.

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
point to observe a Cancel at).

Fixes:

- Add `volt.yield()` periodically — gives other coroutines a
  chance.
- Add `try c.checkpoint()` periodically when a `*Cancel` is
  threaded through — makes the loop cancel-aware.
- Move the work to a separate OS thread via `std.Thread.spawn`
  and bridge with a `Mpmc` channel — see [Offloading CPU
  work](/cookbook/work-offload/).

## Assuming `volt.sleep` is cancel-aware

```zig
volt.sleep(60 * std.time.ns_per_s);
try c.checkpoint();   // checked AFTER the 60s sleep
```

`volt.sleep` runs to completion regardless of `Cancel.fire()`.
Today there's no cancel-aware sleep variant. If a watchdog fires
a Cancel during the sleep, the sleeping coroutine won't observe
it until after wake.

Workaround: race the sleep against the Cancel via a Notify, or
use a shorter sleep + checkpoint loop:

```zig
var elapsed_ns: u64 = 0;
while (elapsed_ns < 60 * std.time.ns_per_s) {
    try c.checkpoint();
    volt.sleep(100 * std.time.ns_per_ms);
    elapsed_ns += 100 * std.time.ns_per_ms;
}
```

Coarser; cancellation observed within 100 ms of fire.

## "It works in tests but hangs in production"

Almost always one of:

- A `Mutex` deadlock you didn't trip in single-threaded tests
  because two coroutines never raced on the same lock.
- A channel that's never closed; receivers park forever.
- A `Task.join` on a Task whose coroutine errored out in a way
  the joiner doesn't observe — typically a misuse of structured
  concurrency.

For reproducible test traces, run with `workers = 1`:

```zig
var rt = try volt.Runtime.init(.{
    .allocator = std.heap.smp_allocator,
    .workers = 1,
});
```

This makes the scheduler single-threaded. Then add multi-worker
stress tests separately to surface concurrency-only bugs.

For hang investigation, `rt.dumpState()` prints scheduler atomics
to stderr — useful when you can attach a debugger and call it.

## See also

- [Common pitfalls](/guides/common-pitfalls/) — this page.
- [Error handling](/guides/error-handling/) — the error vocabulary across primitives.
- [Performance tuning](/guides/performance-tuning/) — the perf-side mistakes.
- [Architecture: memory model](/architecture/memory-model/) — what the runtime's atomics actually mean.

---
title: Spawning and joining
description: volt.spawn returns a typed handle; t.join() parks until the child completes and returns its result.
---

`volt.sleep` from the previous page showed one coroutine
suspending and resuming. This page covers what makes the runtime
actually useful: spawning more than one coroutine and collecting
their results.

## The shape

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    const sum = try (try rt.run(parallelSum, .{}));
    std.debug.print("sum = {d}\n", .{sum});
}

fn parallelSum() !u64 {
    const a = try volt.spawn(compute, .{ @as(u64, 1), @as(u64, 100) });
    const b = try volt.spawn(compute, .{ @as(u64, 101), @as(u64, 200) });
    return a.join() + b.join();
}

fn compute(lo: u64, hi: u64) u64 {
    var sum: u64 = 0;
    var i: u64 = lo;
    while (i <= hi) : (i += 1) sum += i;
    return sum;
}
```

```sh
zig build run
# sum = 20100
```

What happened:

1. `parallelSum` is the root coroutine.
2. `volt.spawn(compute, .{1, 100})` allocates a new coroutine, pushes
   it into the LIFO slot of whichever worker `parallelSum` is on, and
   returns `*Task(u64)` (the type is inferred from `compute`'s
   return).
3. Same for the second spawn.
4. `a.join()` parks `parallelSum` until `a`'s coroutine completes.
   If another worker is idle, it steals one of the spawned
   computes; if not, the same worker runs them sequentially after
   `parallelSum` parks. Either way, `a.join()` returns when `a` is
   done.
5. Same for `b.join()`.
6. `parallelSum` returns `sum = 20100`.

## `volt.spawn` returns `*Task(T)`

```zig
const t = try volt.spawn(myFn, .{arg1, arg2});
// t : *volt.Task(return_type_of_myFn)
const result = t.join();
```

`T` is inferred from `myFn`'s return type. If `myFn` returns `!u64`,
`t.join()` returns `!u64`. If it returns `void`, `t.join()` returns
`void`.

`spawn` itself returns `!*Task(T)` because the spawn can fail
(`error.OutOfMemory`, `error.ArenaExhausted`). Handle that with
`try`.

## `join` frees the Task

`t.join()` parks the caller until the spawned coroutine completes.
**It also frees the Task struct.** Don't use `t` after calling
`join`.

If you spawn but don't join, you leak: the coroutine completes but
its Task struct stays allocated. For fire-and-forget patterns:

```zig
_ = try volt.spawn(backgroundWork, .{});
```

This still leaks the Task. For real fire-and-forget — where you
genuinely don't want to track the child — see `volt.scope` below,
or accept the leak. (A future API may add explicit detach; today
there isn't one.)

## Direct handoff

When you `spawn` then immediately `join` on the same worker —
the common case — Volt skips the park/unpark round trip:

```zig
const t = try volt.spawn(work, .{});
const result = t.join();   // same M's lifo_slot → inline dispatch
```

The child is in the worker's LIFO slot. `join` claims it back via a
single CAS, runs it inline on the same stack, and returns its
result. No park, no wake, no scheduler round trip. See [Direct
handoff](/architecture/direct-handoff/) for the architecture.

If the child got stolen between spawn and join, `join` falls through
to the normal park-on-done path.

## Structured concurrency with `volt.scope`

For spawning multiple children and guaranteeing they all complete
or get cancelled before the function returns, use `volt.scope`:

```zig
fn parallelWork() !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const a = try volt.spawn(doA, .{c});
            const b = try volt.spawn(doB, .{c});
            try a.join();
            try b.join();
        }
    }.body);
}
```

`scope` runs `body(&cancel)`. If `body` returns an error, `scope`
**fires the Cancel** before propagating — any child still parked on
a cancel-aware blocking op wakes with `error.Cancelled`.

The body is responsible for joining its own children. `scope` doesn't
auto-await; it only manages the Cancel lifetime. This is intentional:
auto-await without explicit join order can mask error propagation
bugs.

For the cancellation deep dive, see [Cancellation](/usage/structured-concurrency/).

## Join from outside a coroutine

You can't call `t.join()` from the driver thread — `join` parks on
the parking lot, which only works inside a coroutine. The bootstrap
pattern (`rt.run(root, .{})`) handles this: `rt.run` itself does the
join from inside its own internal context.

```zig
// inside main():
const result = try rt.run(rootFn, .{});  // OK — rt.run handles join

// vs:
const t = try rt.spawn(rootFn, .{});
const result = t.join();                  // panic — not in a coroutine
```

The rule is: `volt.spawn`/`Task.join`/`volt.sleep`/etc. must be
called from inside a coroutine. The exception is `Runtime.spawn`,
which can be called from outside (it routes through the arena
directly instead of a P-local pool) — useful for the rare case of
needing to inject work into a running runtime from a non-coroutine
thread.

## Next

- [Talk to the network](/getting-started/io-tutorial/) — `Task` +
  TCP + the reactor.
- [Basic concepts](/getting-started/basic-concepts/) — the model
  underneath spawn / join / scope.

---
title: Spawning
description: volt.spawn returns a typed handle. Task(T).join() parks until completion and returns the result. Direct handoff makes the common case zero-overhead.
---

Volt has one spawn primitive: `volt.spawn(fn, args)`. It allocates
a coroutine, pushes it for the current worker to dispatch, and
returns a typed `*Task(T)` handle. `t.join()` parks the caller
until the coroutine completes and returns its result.

There is no fire-and-forget primitive. Every spawn returns a Task
you're expected to join (or let `volt.scope` join for you).

## The shape

```zig
fn parentFn() !void {
    const t = try volt.spawn(childFn, .{ arg1, arg2 });
    // t : *volt.Task(return_type_of_childFn)
    const result = t.join();
    // result : return_type_of_childFn
}

fn childFn(x: u64, y: u64) u64 {
    return x + y;
}
```

`T` in `*Task(T)` is inferred from `childFn`'s return type. If
`childFn` returns `!u64`, `t.join()` returns `!u64`. If `void`,
`t.join()` returns `void`.

## `volt.spawn` vs `Runtime.spawn`

Two entry points, same allocation path:

| Call | Must be inside coro? | Pool path |
|---|---|---|
| `volt.spawn(fn, args)` | yes | Current P's local pool → arena fallback |
| `rt.spawn(fn, args)` | no | Slab arena directly |

`volt.spawn` is the canonical API. `rt.spawn` exists for the rare
case of injecting work from a non-coroutine thread holding a
`*Runtime`. Inside a coroutine, always use `volt.spawn` — the local
pool is one pointer load on the hot path.

## `Task(T).join()`

```zig
const result = t.join();
```

Behaviour:

- If the spawned coroutine has completed already (`done` flag set),
  `join` reads the result and returns immediately.
- Otherwise, `join` parks the caller on `&t.done` via the parking
  lot. The coroutine's dispatch `.done` branch unparks the join
  waiter atomically with setting `done`.
- After reading the result, `join` frees the `Task` struct (and
  the `Frame` closure beneath it).

**`join` consumes the Task.** Don't use `t` after calling
`t.join()`. Calling `join` twice is undefined behaviour.

**`join` from outside a coroutine panics** while the spawned task
is still running. This is the rule: every blocking op on the
parking lot requires a current coroutine. The exception is the
bootstrap path — `Runtime.run` does its internal join from inside
its own context, so users never write `join` from `main` directly.

## Direct handoff

The common case is `spawn` followed immediately by `join`:

```zig
const t = try volt.spawn(work, .{});
const result = t.join();
```

Without optimization, that's: spawn → push to LIFO slot → park
caller → worker pulls from LIFO slot → run child → child completes
→ unpark caller → caller resumes. Round trip through the parking
lot, two context switches.

Volt's `Task.join` detects this pattern via `tryRemoveLifo` + a
single CAS: if the child is still in this M's LIFO slot, claim it
back, dispatch it inline on the caller's stack, and return its
result. Zero park, zero unpark, zero round trip.

If the child got stolen between spawn and join, the CAS fails and
`join` falls through to the normal park-on-done path. No
correctness impact, just a 4-13% measured win on workloads that
hit the inline path. See [Direct handoff](/architecture/direct-handoff/)
for the design.

## Errors

`volt.spawn` itself can fail:

| Error | When |
|---|---|
| `error.OutOfMemory` | Allocator failed allocating the Frame+Task combined struct or the Coroutine struct. |
| `error.ArenaExhausted` | Slab arena has no free slots — `max_concurrent_stacks` reached. Raise the config. |

`Task.join`'s return type is whatever `user_fn` returns, with no
runtime-level errors injected. If the spawned function panics
mid-execution, the worker panics (process aborts) — Volt does not
intercept panics.

## Multiple children

For more than one child, you have two patterns:

### Explicit pairwise join

```zig
fn fetchBoth() !struct { a: []u8, b: []u8 } {
    const ta = try volt.spawn(fetch, .{ "https://a.example.com" });
    const tb = try volt.spawn(fetch, .{ "https://b.example.com" });
    return .{ .a = try ta.join(), .b = try tb.join() };
}
```

You own each Task; you join each one. Simple and explicit. Right
choice when N is small and known.

### `volt.scope` for error-driven cleanup

```zig
fn fetchBoth() !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const ta = try volt.spawn(fetchCancel, .{ "https://a", c });
            const tb = try volt.spawn(fetchCancel, .{ "https://b", c });
            _ = try ta.join();
            _ = try tb.join();
        }
    }.body);
}
```

If `fetchA` errors before `fetchB` returns, the explicit pattern
above won't cancel `fetchB` — you'll wait for it to finish on its
own. `volt.scope` gives you "if either child errors, fire the
Cancel so the other observes it via cancel-aware blocking ops." See
[Structured Concurrency](/usage/structured-concurrency/).

## Fire-and-forget?

Volt does not have a detach primitive. You always get back a Task.
The closest pattern:

```zig
_ = try volt.spawn(backgroundWork, .{});
```

This still allocates the Task and leaks it (the Task struct is
freed only by `join`). For long-running runtimes where you spawn
once and never want the handle back, the leak is bounded by the
spawn rate. For high-rate spawns, use `scope` so the Tasks get
freed when the scope returns.

## Cooperative yield

```zig
volt.yield();
```

Re-queue the current coroutine to the worker's queue tail (FIFO,
not LIFO slot). The dispatch loop runs every other queued
coroutine first, then comes back. Use cases:

- A CPU-bound loop that should let sibling coroutines on the same
  worker make progress.
- A cancellation checkpoint when combined with `*Cancel` —
  `c.checkpoint() catch return; volt.yield();` is the canonical "I'm
  about to do more work; let cancellation propagate first" idiom.

For I/O-bound code, you don't need `yield` — the next blocking
call already releases the worker.

## See also

- [Structured Concurrency](/usage/structured-concurrency/) — `volt.scope` and `Cancel`.
- [Direct handoff](/architecture/direct-handoff/) — why spawn-then-join is zero-overhead.
- [The Runtime](/usage/runtime/) — `Runtime.spawn` for cross-thread injection.

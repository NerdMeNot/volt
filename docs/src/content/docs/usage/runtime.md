---
title: The Runtime
description: How to bootstrap Volt with volt.run, what Config controls, and when to construct a Runtime by hand.
---

`volt.run` is the entry point. It takes a `Config`, a root function,
and the args tuple for that function:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}

fn root() !void {
    // You're inside the runtime now.
    try volt.sleep(volt.Duration.fromMillis(50));
}
```

The runtime owns the worker pool, the reactor, the global injection
queue, and the per-task stack pool. It tears all of them down when
`volt.run` returns. There is no global state and no `init()` call —
constructing the runtime *is* `volt.run`.

## Config

```zig
pub const Config = struct {
    allocator: std.mem.Allocator,    // required
    workers: ?usize = null,          // null → getCpuCount()
    deterministic: bool = false,     // single worker, fixed seed
    pin_workers: bool = false,       // pthread_setaffinity_np per worker (Linux)
};
```

- **`allocator`** — required. Used for the worker pool, the
  injection queue, the reactor, and per-coroutine stacks. Volt does
  not assume a particular allocator; pass whatever you'd pass to
  `std.Thread.Pool.init`.
- **`workers`** — number of worker threads. `null` (default) means
  `getCpuCount()` with a floor of 1. Set to a small fixed number for
  deterministic tests; set to a higher number than your CPU count
  if your workload is mostly I/O-bound and you want to absorb more
  pending I/O without spawning at every accept.
- **`deterministic`** — forces single-worker mode and uses a
  pseudo-random seed for steal selection. Useful for reproducible
  test traces. Doesn't fully eliminate non-determinism (the OS
  thread scheduler still nudges futex wakes), but eliminates
  Volt's contributions.
- **`pin_workers`** — pin worker `i` to core `i % cpu_count` on
  Linux via `pthread_setaffinity_np`. No-op on Darwin (no clean
  match for QoS classes). Reduces cross-core cache traffic on
  latency-sensitive workloads.

## Common patterns

Default config (most cases):

```zig
try volt.run(.{ .allocator = a }, root, .{});
```

Tuned worker count:

```zig
try volt.run(.{ .allocator = a, .workers = 4 }, root, .{});
```

Deterministic single-threaded for tests:

```zig
try volt.run(.{ .allocator = std.testing.allocator, .deterministic = true }, root, .{});
```

CPU-pinned for latency-critical:

```zig
try volt.run(.{ .allocator = a, .workers = 8, .pin_workers = true }, root, .{});
```

## Return type and error handling

`volt.run` returns the same shape as the root function plus a
runtime-error union:

- Root returns `T` — `volt.run` returns `T`. Init failures panic.
- Root returns `E!T` — `volt.run` returns `(E || RunError)!T`. Init
  failures and runtime errors flow through the error union.

`volt.RunError` is exported so you can build unified application
error sets:

```zig
const AppError = volt.RunError || error{ BadRequest, Forbidden };

fn main() !void {
    const result: AppError!void = volt.run(.{ .allocator = a }, app, .{});
    // ... handle uniformly ...
}
```

## What happens when `root` returns

1. `root` returns its value or error.
2. `volt.run` signals shutdown to the worker pool.
3. Workers drain remaining work — coroutines that were already
   running finish their current tick. Coroutines waiting on I/O are
   cancelled.
4. Workers join.
5. The reactor closes.
6. The stack pool drains and unmaps all reserved address space.
7. `volt.run` returns the root's value (or error).

A coroutine that's parked on something nothing will ever wake (e.g.
a `Channel` whose every sender has been dropped without `close()`)
will be cancelled during shutdown — its current park surfaces
`error.Cancelled`, and the worker reaps it. You should not see
"hangs forever on shutdown" with Volt; if you do, it's a bug.

## Constructing a Runtime by hand

For embedding scenarios — running coroutines from inside a larger
program that owns its own main loop — you can drop one level deeper
to `volt.Runtime`:

```zig
var rt = try volt.Runtime.init(.{ .allocator = a });
defer rt.deinit();
rt.bindWorkers();
const created = try rt.spawnRoot(root, .{});
try rt.start();
rt.runUntilDone(created.coro);
```

This is what `volt.run` does internally. The five-step shape is the
same as `std.Thread.Pool.init` + `Pool.spawn` + `Pool.deinit` —
explicit because the embedding case wants control over each phase.

For 99% of programs, `volt.run` is what you want.

## Single runtime per process

You cannot nest `volt.run` calls. The runtime uses thread-local state
to track the current coroutine, worker, and runtime; nested
bootstraps would panic when the inner one tries to install over the
outer's TLS. If you need to run multiple "Volt-like" islands, use
one runtime with multiple `volt.scope` regions, or run them in
separate OS processes.

## Querying the active runtime

Inside a coroutine:

```zig
const rt: ?*volt.Runtime = volt.currentRuntime();
```

Returns `null` if you're not inside `volt.run`. Used internally by
every primitive that needs to schedule wakes; you'd only reach for
it directly when implementing your own park-based primitive.

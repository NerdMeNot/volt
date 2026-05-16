---
title: The Runtime
description: Runtime.init / Runtime.run / Runtime.deinit — how to bootstrap Volt, what Config controls, and what the runtime owns.
---

`volt.Runtime` is the whole scheduler — workers, reactor, parking
lot, slab arena. You construct one, hand it a root function, and
let it tear itself down on exit.

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    // You're inside the runtime now — volt.spawn / volt.sleep /
    // channels / sync / TCP all work here.
}
```

There is no global runtime. You construct one explicitly; `volt.runtime()`
(callable from inside a coroutine) finds it via the threadlocal
current coroutine. Multiple `Runtime` instances per process are
allowed; coroutines can't migrate between them.

## Config

```zig
pub const Config = struct {
    allocator: std.mem.Allocator,    // required
    workers: ?usize = null,          // null → std.Thread.getCpuCount()
    max_concurrent_stacks: usize = 16 * 1024,
};
```

- **`allocator`** — required. Used for `Coroutine` structs, `Task`
  handles, `Frame` closures, and runtime bookkeeping. The slab
  arena's virtual reservation comes from `mmap` directly, not the
  allocator. `std.heap.smp_allocator` is the recommended default for
  multi-worker runtimes; `std.testing.allocator` works for
  single-worker tests but isn't thread-safe on all paths.

- **`workers`** — number of OS threads to spawn. `null` (default)
  uses `getCpuCount()` with a floor of 1. The thread calling
  `rt.run` becomes worker `M[0]`; the runtime spawns `workers - 1`
  additional threads. Capped at `volt.MAX_WORKERS` (64).

- **`max_concurrent_stacks`** — hard cap on live coroutine stacks.
  The runtime pre-reserves `max_concurrent_stacks × 256 KiB` of
  virtual address space at `init` (PROT_NONE, zero RSS until used)
  and hands slots out from the slab arena. `volt.spawn` returns
  `error.ArenaExhausted` once every slot is in use.

Default `max_concurrent_stacks` (16384) covers the bench-rss
N=10000 test and typical interactive workloads with headroom. Raise
for HTTP servers expecting tens of thousands of concurrent
connections; lower to bound virtual address space on tight budgets.

## Lifecycle

### `Runtime.init(config)`

```zig
var rt = try volt.Runtime.init(.{ .allocator = a });
```

In order:

1. Allocates `n_workers` `M` structs and `n_workers` `P` structs.
2. Initialises the kqueue reactor (one fd per Runtime).
3. Initialises the sharded parking lot (16 buckets, each with its
    own pthread mutex + waiter list).
4. Constructs the slab arena: `mmap` of
    `max_concurrent_stacks × 256 KiB`, all `PROT_NONE` initially.
    Bookkeeping (free-index stack + commit bitmap) is heap-allocated
    via the user allocator.
5. Installs the process-wide `SIGSEGV` / `SIGBUS` handler that
    grows arena stacks on demand (idempotent across multiple
    Runtimes).
6. Spawns `n_workers - 1` pthread workers; each enters
    `workerLoopUntilShutdown`. Worker M[0] is the calling thread
    and joins the loop later, when `rt.run` is called.

Failure modes: `error.OutOfMemory` (allocator), `error.MmapFailed`
(arena), `error.TooManyWorkers` (workers > MAX_WORKERS = 64),
`error.ReactorInitFailed` (kqueue), `error.SystemResources` (pthread
spawn).

### `Runtime.run(user_fn, args)`

```zig
try (try rt.run(root, .{}));
```

The bootstrap entry point. Runs `user_fn` as the root coroutine on
the calling thread (which becomes M[0]). Returns when the root
completes.

Signature:

```zig
pub fn run(
    self: *Runtime,
    comptime user_fn: anytype,
    args: anytype,
) !@typeInfo(@TypeOf(user_fn)).@"fn".return_type.? { ... }
```

Return type is `!T` where `T` is `user_fn`'s return type. If
`user_fn` returns `!U` (an error union), the outer `!` is for
runtime errors (allocation failures, arena exhaustion), the inner
`!` is yours — hence the `try (try ...)` idiom. The runtime tests
use the same pattern.

If `user_fn` returns `void` (no error union), the call site is the
cleaner `try rt.run(root, .{})`.

### `Runtime.deinit()`

```zig
defer rt.deinit();
```

In order:

1. Sets `shutdown = true` and unparks every worker so they observe
    it.
2. Joins all spawned worker threads (M[1..N-1]).
3. Drains each P's local pools (coroutine pool → allocator, stack
    pool → arena).
4. Releases the slab arena (one `munmap` for the whole region).
5. Unregisters from the SIGSEGV handler. Closes the reactor fd.
    Tears down the parking lot.

Calling `deinit` while coroutines are still in flight is undefined
behaviour. Use `volt.scope` or explicit joins to ensure quiescence
before returning from `rt.run`.

## `Runtime.spawn` vs `volt.spawn`

`rt.spawn(fn, args)` is the method form — useable from outside a
coroutine (e.g. injecting work from a non-coroutine thread that
holds a Runtime pointer). It bypasses per-P pools and pulls
directly from the slab arena.

`volt.spawn(fn, args)` is the free-function form — must be called
**from inside a coroutine**. It infers the Runtime via
`volt.current.require()` and routes through the current P's local
pool for cache locality.

The free function is the canonical API; the method form is for the
rare cross-thread injection case.

## Diagnostics

```zig
rt.dumpState();
```

Dumps per-P and global scheduler state (parked workers bitmap,
searching count, reactor poller flag, per-P spawn/done/unpark
counters) to stderr. Safe to call from any thread. Use for
investigating hangs.

## Common patterns

**Default** (most apps):

```zig
var rt = try volt.Runtime.init(.{ .allocator = a });
defer rt.deinit();
try (try rt.run(root, .{}));
```

**Pinned worker count** (deterministic tests):

```zig
var rt = try volt.Runtime.init(.{
    .allocator = a,
    .workers = 1,
});
```

**High concurrency** (TCP server with 50k connections):

```zig
var rt = try volt.Runtime.init(.{
    .allocator = a,
    .max_concurrent_stacks = 65536,
});
```

## See also

- [Spawning](/usage/spawning/) — the spawn / join surface.
- [Structured Concurrency](/usage/structured-concurrency/) — Cancel + scope.
- [The slab arena](/architecture/) — what `max_concurrent_stacks` actually configures.
- [The M:N scheduler](/architecture/mn-scheduler/) — what workers and Ps do.

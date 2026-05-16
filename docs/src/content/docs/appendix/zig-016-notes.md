---
title: Zig 0.16 notes
description: What Zig 0.16 removed from std, what Volt uses instead. Useful for porting or for tracking what depends on libc directly.
---

Volt is pinned to Zig 0.16.0. The version is part of the release
tag (`v1.0.0-zig0.16.0`). When a new Zig minor lands, Volt
re-versions and a new branch tracks the next minor.

Zig 0.16 made substantial cuts to `std`. This page documents
what Volt routes around `std` to do, where in the source it
happens, and why each cut matters for a runtime.

## What's gone in std 0.16

### `std.Thread.Mutex` / `std.Thread.Condition` / `std.Thread.sleep` / `std.Thread.Futex`

All removed. Zig's design direction is "stdlib provides bindings,
not abstractions" for concurrency primitives. Concurrency
abstractions belong in libraries.

**What Volt uses instead:**

- **OS-thread mutexes** — `pthread_mutex_init` / `_lock` /
  `_unlock` / `_destroy` via `@extern`. Used inside the parking
  lot's bucket locks (`src/park.zig`).
- **Spinlocks** for short critical sections — single
  `std.atomic.Value(u32)` with TTAS pattern. Used in
  `src/cancel.zig` (Cancel's waiter list lock) and `src/stack.zig`
  (Arena's free-list lock).
- **Coroutine mutex** — `volt.Mutex`, built on the parking lot.
  Different shape from `std.Thread.Mutex` (parks coroutines, not
  threads). Source: `src/sync.zig`.
- **Sleep** — `volt.sleep` for coroutines (parks on kqueue
  EVFILT_TIMER). For sleeping the calling thread, use
  `std.time.sleep` (which still exists, calling `nanosleep`).
- **Futex-equivalent** — `__ulock_wait` / `__ulock_wake` on
  Darwin, `futex` syscall on Linux (when ported). Used in
  `src/parker.zig`.

### `std.posix.{socket, bind, listen, accept, connect, fcntl, read, write, close}` mid-level

The medium-level `std.posix` wrappers around libc syscalls were
mostly removed in 0.16. Low-level `std.posix.system.*` still
exists for raw syscalls; the `std.posix.X` ergonomic wrappers
that handled errno and retry are gone.

**What Volt uses instead:** direct `@extern` bindings to libc
functions in `src/net.zig`. Each Volt entry-point handles errno
and parks on EAGAIN.

```zig
// src/net.zig — example
const accept_fn = @extern(
    *const fn (fd: c_int, addr: ?*anyopaque, addrlen: ?*c_int) callconv(.c) c_int,
    .{ .name = "accept" },
);

pub fn accept(self: *TcpListener) !TcpStream {
    while (true) {
        const fd = accept_fn(self.fd, null, null);
        if (fd >= 0) return TcpStream{ .fd = fd };
        const e = std.posix.errno();
        if (e != .AGAIN) return acceptError(e);
        runtime().reactor.waitRead(self.fd);
    }
}
```

### `std.posix.{mmap, munmap, mprotect}` mid-level

Same story — removed in 0.16. Volt uses `@extern`:

```zig
// src/stack.zig
const mmap_fn = @extern(
    *const fn (
        addr: ?*anyopaque, len: usize, prot: c_int, flags: c_int,
        fd: c_int, offset: i64,
    ) callconv(.c) ?*anyopaque,
    .{ .name = "mmap" },
);
```

These are the load-bearing syscalls for the [slab
arena](/architecture/slab-arena/) and [stack
growth](/architecture/stack-growth/).

### `std.fs.cwd()`, `std.fs.Dir`, `std.fs.File`

Moved under `std.Io`. Volt doesn't use them — sockets are bare fds,
and file I/O isn't in core scope. Anything in `volt-fs` (future)
would route through `std.Io`.

### `async` / `await` keywords

Zig removed `async` / `await` in late-2024 (before 0.16). Volt
predates this in design — it was always stackful, no language-
level async. The keyword removal didn't affect us; if anything,
it made the design call obvious. See [Stackful by
design](/architecture/stackful-design/).

## What's still in std

- `std.atomic.Value(T)` — atomics. Used everywhere in Volt.
- `std.heap.smp_allocator` — thread-safe allocator. The
  recommended allocator for multi-worker runtimes.
- `std.heap.DebugAllocator` — leak-detecting allocator. Use for
  tests; not thread-safe enough for multi-worker.
- `std.testing.allocator` — `GeneralPurposeAllocator{.safety =
  true}`. Use for single-worker tests.
- `std.posix.system.*` — raw syscall bindings. Volt sometimes
  uses these instead of `@extern`.
- `std.posix.errno()` — errno read helper. Still present.
- `std.time.{ns_per_ms, ns_per_s, ns_per_min, ...}` — time
  constants. Use these instead of magic numbers.
- `std.Thread.getCpuCount()` — used in `Runtime.init` to default
  worker count.

## Gotchas

### Module-level comptime asm on x86_64-linux ELF

`comptime { asm(...) }` at module scope doesn't always emit
symbols on x86_64-linux ELF in current Zig. Workaround: use a
separate `.S` file linked via `build.zig`.

This bites Volt's x86_64 context-switch port (not yet shipping).
The arm64 port uses inline asm inside a function (`callconv(.naked)`
`fn`), which works fine. The Linux x86_64 port will need the
`.S` workaround.

Documented in [CLAUDE.md](https://github.com/NerdMeNot/volt/blob/main/CLAUDE.md)
under "Zig 0.16 API notes".

### `std.heap.smp_allocator` is the right default

`std.testing.allocator` (the leak-detecting GPA) is **not
thread-safe on all paths** — specifically its stack-trace
capture for double-free detection uses a process-global hash
map. Single-worker tests work; multi-worker tests crash with
`EXC_BAD_ACCESS` in `array_hash_map.ensureTotalCapacityContext`.

Volt's tests use `std.heap.smp_allocator` instead. Documented in
the test discovery commit.

### `pthread_mutex_init` on Darwin needs an attribute argument

The libc declaration takes a `?*const PthreadMutexAttr`. Passing
`null` works (default attributes — recursive on Linux, non-
recursive on Darwin). Most Volt code passes `null`.

## What Volt routes via libc

A complete list of libc functions Volt binds via `@extern`:

| Source | Function | Why |
|---|---|---|
| `src/stack.zig` | `mmap`, `munmap`, `mprotect` | Slab arena allocation |
| `src/signal.zig` | `sigaction`, `mprotect` | SIGSEGV handler for stack grow |
| `src/parker.zig` | `__ulock_wait`, `__ulock_wake` (Darwin) | Per-worker OS-level park |
| `src/park.zig` | `pthread_mutex_*` | Parking lot bucket locks |
| `src/reactor_kqueue.zig` | `kqueue`, `kevent` | Reactor |
| `src/net.zig` | `socket`, `bind`, `listen`, `accept`, `connect`, `setsockopt`, `getsockname`, `fcntl`, `read`, `write`, `close` | TCP |
| `src/worker.zig` | `pthread_create`, `pthread_join` | Worker threads |

When the Linux backend lands, parallel files
(`src/reactor_epoll.zig`, etc.) will use the same `@extern`
pattern with Linux-specific syscalls.

## Why not `zig-async-helper` or similar wrappers

We considered using existing Zig concurrency libs (like
zig-async / circuit / coro-zig). The decision was to build the
runtime in-tree because:

1. **The architectural choices matter.** A coroutine runtime is
   defined by its scheduler / parking / reactor design, not by
   API surface. Wrapping someone else's runtime means inheriting
   their tradeoffs.
2. **External deps drift.** Zig is at 0.16; libraries pinned to
   0.13 or 0.14 need port work anyway.
3. **The runtime is small.** ~5 KLoC including tests. The
   maintenance burden is real but bounded.

When the Zig ecosystem matures and someone publishes a
production-grade coroutine runtime with the right tradeoffs,
Volt may consume it. Until then, the design is in-tree.

## Forward compatibility

Volt's pin policy: one Zig minor per release. When Zig 0.17 lands:

- Tag a `v1.0.0-zig0.17.0` after porting.
- The 0.16 branch stays as a maintenance branch; bug fixes only.
- New features land on the latest-Zig branch.

The two-track model keeps Volt usable for users on stable Zig
versions while not blocking feature work on the latest.

## Further reading

- Zig 0.16.0 release notes — official what-changed list.
- Volt's `CLAUDE.md` — the source-of-truth doc for project-level
  Zig-version policy and gotchas.
- [The slab arena](/architecture/slab-arena/) — the load-bearing
  use of direct libc binding.

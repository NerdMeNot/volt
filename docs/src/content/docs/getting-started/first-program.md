---
title: Your first program
description: A 30-second tour of what happens when a Volt program runs — bootstrap, suspend, resume.
---

If you ran the verify program from [Installation](/getting-started/installation/),
you've already written your first Volt program. This page is the
30-second tour of what actually happened.

## The program

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(hello, .{}));
}

fn hello() !void {
    volt.sleep(50 * std.time.ns_per_ms);
    std.debug.print("hello from a coroutine\n", .{});
}
```

Six lines of executable code. Let's walk them.

## What `Runtime.init` does

```zig
var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
```

This single call:

1. **Reserves the slab arena** — one `mmap` of `max_concurrent_stacks
   × 256 KiB` virtual address space (default 16384 × 256 KiB = 4 GiB
   virtual, zero RSS — every page is PROT_NONE until touched).
2. **Spawns `getCpuCount() - 1` worker threads**, each with its own
   work-stealing deque, mailbox, and parker. The thread you're on
   (the one running `main`) becomes worker `M[0]` when `run` enters.
3. **Initialises the reactor** (kqueue / epoll / io_uring / IOCP,
   picked at comptime), the sharded parking lot, the per-worker
   coroutine pool. Installs the SIGSEGV handler that grows stacks
   on demand.

Default config is reasonable for most apps. `Config` only requires
`allocator`; `workers` defaults to `NumCPU`, `max_concurrent_stacks`
to 16384.

## What `rt.run` does

```zig
try (try rt.run(hello, .{}));
```

`run` is the bootstrap entry. It:

1. Allocates a coroutine on the arena, with `hello` as the entry point.
2. Pushes it into worker M[0]'s queue.
3. Calls into M[0]'s dispatch loop. **The calling thread *becomes* a
   worker.** The dispatch loop steals, polls the reactor, parks
   when idle. It only returns when the root coroutine completes.

`hello` returns `!void`, so `rt.run(hello, .{})` returns `!!void` —
the outer `!` is from `run` itself (allocation failures, etc.), the
inner `!` is `hello`'s. The `try` at the call site collapses one
level. If `hello` returns an error you handle it in `main`.

## What `volt.sleep` does

```zig
volt.sleep(50 * std.time.ns_per_ms);
```

`sleep` does not block the worker thread. It:

1. Registers a timer with the reactor (`EVFILT_TIMER` on Darwin
   kqueue, `timerfd` on epoll, `IORING_OP_TIMEOUT` on io_uring,
   `CreateThreadpoolTimer` on Windows IOCP) with the current
   coroutine as the wake identity.
2. Marks the coroutine `pending = .park` and calls `context.swap`
   back to the worker's dispatch loop. The worker can now run
   something else; the coroutine's stack is paused, registers saved.
3. When the kernel delivers the timer, the reactor unparks the
   coroutine — pushes it back to a worker's queue.
4. The worker (possibly a different one than was running `hello`
   before) dispatches the coroutine, which `context.swap`s back
   into the saved stack, and `sleep` returns.

You see one line of code. The runtime did six syscalls' worth of
bookkeeping.

## What `defer rt.deinit()` does

```zig
defer rt.deinit();
```

When `main` returns (after `rt.run` finishes), `deinit`:

1. Sets the shutdown flag, unparks every worker, joins each thread.
2. Drains every per-P pool back to the arena.
3. Tears down the parking lot, reactor, and SIGSEGV registration.
4. Munmaps the arena slab (one syscall releases all 4 GiB virtual).

The slab arena means a single munmap on shutdown regardless of how
many coroutines ran. See [The slab arena](/architecture/) for the why.

## Coroutines are not threads

A coroutine in Volt is **not** an OS thread. The runtime multiplexes
N coroutines across M OS threads (the workers), where N can be
much larger than M. The 16 KiB resident stack per coroutine is
real memory but the kernel only sees `getCpuCount()` threads.

`getCpuCount()` on an 11-core Mac → 11 workers. You can spawn
10,000 coroutines without spawning 10,000 threads.

## Try it

Spawn two coroutines that each sleep different amounts and see they
interleave:

```zig
fn hello() !void {
    _ = try volt.spawn(say, .{ "first",  10 * std.time.ns_per_ms });
    _ = try volt.spawn(say, .{ "second",  5 * std.time.ns_per_ms });
    volt.sleep(50 * std.time.ns_per_ms); // wait for them
}

fn say(name: []const u8, ns: u64) void {
    volt.sleep(ns);
    std.debug.print("{s}\n", .{name});
}
```

Output:

```
second
first
```

`second` sleeps shorter, wakes first, prints first. Two coroutines,
one worker thread (probably), no race, no thread overhead.

(We're leaking the spawned coroutines' Task handles in this example.
That's fine for a sleep-and-exit demo; the next page covers join.)

## Next

- [Spawning and joining](/getting-started/spawn-join/) — `volt.spawn`
  returns a `*Task(T)`; `t.join()` parks until it completes and
  returns the result.
- [Basic concepts](/getting-started/basic-concepts/) — the model
  underneath all of this.

---
title: Debugging Volt programs
description: How to use Runtime.dumpState, the Observer hook, gdb/lldb, and common deadlock patterns. From "my server hangs" to root cause in under five minutes.
---

Volt is cooperative + stackful, which changes the shape of debugging
from what you're used to in either single-threaded Zig or
preemptive-multithreaded Zig. A coroutine that doesn't yield pins
its worker; a coroutine parked on the wrong primitive can hang
silently; a long sync call can starve the reactor. This page covers
the tools and patterns for finding those bugs fast.

## `Runtime.dumpState()` — the first thing to try

When the runtime seems hung, call `rt.dumpState()` from any thread
that has a `*Runtime` (usually the driver, but the OS-thread parker
in `runDetached` works too). It prints a snapshot to stderr:

```
=== Runtime.dumpState ===
  shutdown:           false
  parked_workers:     0b1111110     ← 6 of 8 workers parked
  num_searching:      0             ← no worker is in find-work phase
  reactor_poller:     true          ← someone's in poll(true)
  reactor_pending:    1             ← one coro is parked on IO
  total_spawned:      245
  total_done:         244           ← one coro hasn't completed
  spawned - done:     1             ← ← non-zero = lost coroutines
  m[0]/p[0]: lifo_set=false, local_empty=true, mailbox_empty=true, parker_state=2
  m[1]/p[1]: lifo_set=false, local_empty=true, mailbox_empty=true, parker_state=2
  ...
=========================
```

The high-signal fields:

- **`spawned - done > 0`** — coroutines were spawned but never
  completed. The most common cause: a coro parked on something
  (channel, mutex, IO) that will never fire. Cross-reference with
  `reactor_pending` to see if it's stuck in the reactor specifically.
- **`reactor_pending > 0` + every worker parked** — the polling
  worker (if any) is in a syscall; the rest are parked on
  `m.parker`. Usually means "we have one coro waiting on IO; that
  IO won't arrive." Common when you forgot to send a request before
  reading a response.
- **`parker_state = 2`** for every M — every worker is in
  `m.parker.park()`. Combined with `reactor_pending = 0`, this is
  a true deadlock: no work, no IO, nothing to wake anyone.

## `Runtime.Config.observer` — instrument when dumpState isn't enough

For continuous instrumentation (rather than a one-shot snapshot),
set `Config.observer` to a `*Observer` with the callbacks you care
about:

```zig
var spawn_count: std.atomic.Value(u32) = .init(0);
var done_count: std.atomic.Value(u32) = .init(0);

fn onSpawn(c: *volt.internal.Coroutine) void {
    _ = c;
    _ = spawn_count.fetchAdd(1, .acq_rel);
}
fn onComplete(c: *volt.internal.Coroutine) void {
    _ = c;
    _ = done_count.fetchAdd(1, .acq_rel);
}

var observer = volt.Observer{
    .onSpawn = &onSpawn,
    .onComplete = &onComplete,
};
var rt = try volt.Runtime.init(.{
    .allocator = gpa,
    .observer = &observer,
});
```

Four callbacks — `onSpawn`, `onPark`, `onUnpark`, `onComplete`.
Each receives a `*Coroutine` you can inspect (read fields like
`pending`, `park_state`, `task_done`) but shouldn't mutate. Hot
path: keep callbacks fast — they fire per-coroutine on the
scheduler's critical path.

**When `observer == null` (the default), every hook site folds to a
dead branch at optimisation time.** Zero runtime cost. Add an
observer when you need it; remove the field when you don't.

The hooks are intentionally low-level. v1 ships no built-in
emitter (no tracing crate, no OpenTelemetry, no StatsD) — write the
emitter you actually want and connect it to the Observer surface.

## gdb / lldb — when you need a real stack trace

A parked coroutine isn't actively running, so a thread-level
backtrace won't show its frames. To inspect parked-coroutine state:

1. Pause the process (`gdb -p <pid>` or `lldb -p <pid>`).
2. Look at every worker thread's backtrace (`thread apply all bt`).
   Most will be in `parker.park` → futex wait. The polling worker
   is in `reactor.poll` → kevent/epoll_wait/io_uring_enter/GetQueued-
   CompletionStatusEx.
3. To inspect a specific coroutine's saved registers + stack: find
   the `*Coroutine` (most easily via `dumpState`'s spawned-but-not-
   done count tracking) and read its `ctx` field — that's the saved
   register state from the last `context.swap`. Manually reconstruct
   the call chain by unwinding from that saved PC + SP.

In practice you almost never need (3). The combination of
`dumpState` + Observer covers ~95% of the hangs people actually hit.

## Common deadlock patterns

### "All workers parked, no IO pending"

```
parked_workers:  0b11111111   (all)
reactor_pending: 0
spawned - done:  1
```

A coroutine is parked on a sync primitive that nothing will signal.
Usually one of:

- `Notify.wait()` with no `Notify.notify()` ever called
- `Mutex.lock()` on a mutex held by a coroutine that crashed/exited
  without unlocking
- `Spsc.recv()` on a channel with no live producer + not closed

**Fix**: add a `closed` signal to the missing-producer side, or use
`Cancel` + `recvCancel` to bound the wait.

### "One coro stuck in poll forever"

```
parked_workers:  0b11111110   (all but one)
reactor_pending: 1
spawned - done:  1
```

One coroutine is parked in the reactor on an IO event that won't
arrive. Common: `TcpStream.read` on a connection the peer never
writes to, or a `waitReadable` on an fd that was closed by another
coro without a wakeup.

**Fix**: wrap the read in `withTimeout`, or use `*Cancel` + the
cancel-aware reactor variants and fire the cancel on shutdown.

### "Worker pinned in a sync syscall"

The signature: `dumpState` looks healthy (no `spawned - done` gap)
but throughput on other coros is poor or zero. The culprit is a
coroutine running a long sync call (a 5-second DB query, a gzip on
a megabyte) that doesn't yield.

**Fix**: move the sync call to `volt.spawnBlocking` — runs on an
OS thread, leaves the coroutine worker free to dispatch others.

## Pitfalls Volt-specific debuggers should know

1. **Coroutine stacks are at stable VAs**, but a stack pointer
   captured from a coroutine that completed is dangling — the slot
   has been returned to the per-P pool.
2. **The Observer's `*Coroutine` argument** is valid only for the
   duration of the callback. After return, the runtime may free or
   recycle the slot.
3. **`std.debug.print` from inside a coroutine** is fine
   functionally, but synchronizes on a process-global stderr mutex
   — it can mask scheduler-timing bugs by serializing prints.
   Prefer the Observer hook for instrumentation that needs timing
   fidelity.
4. **`std.testing.allocator`** corrupts under multi-worker Volt
   tests (its stack-trace capture races with worker stack writes).
   Use [`volt.testing.allocator`](../../api-reference/) instead —
   same leak detection, no stack-trace race.

## Related

- [`volt.testing` helpers](/api-reference/#testing) — `expect` and
  `spawnRace` for assertion-friendly async tests
- [Common pitfalls](/guides/common-pitfalls/) — broader gotchas
- [Cancellation internals](/architecture/cancellation-internals/) —
  the model that makes `Cancel`-based cleanup work

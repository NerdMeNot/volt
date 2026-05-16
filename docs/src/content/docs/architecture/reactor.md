---
title: The kqueue reactor
description: One reactor per Runtime, single-poller claim, EV_ONESHOT registration, udata = coroutine pointer. How non-blocking I/O parks and resumes coroutines.
---

A stackful coroutine runtime needs **one place where I/O parks
and one place where I/O wakes**. On Darwin, that's `kqueue`. The
runtime owns one kqueue fd, every blocking I/O op registers itself,
and a single worker at a time calls `kevent` to harvest readiness
events back to coroutine queues.

This page covers the design. The Linux `epoll` and Windows `IOCP`
backends are roadmapped but not yet shipping.

## Mental model

> The reactor is a **postbox at the back of the post office**.
> When a coroutine does `read()` and the kernel says "no data
> yet", the runtime drops a slip into the postbox: "deliver to
> coroutine X when fd Y is readable". A single worker at a time
> reads the postbox and routes the delivered slips back to the
> coroutines. Other workers run regular work in parallel.

The slip is a kqueue event: a tuple of `(fd, filter, flags,
udata)`. We set `udata` to the coroutine pointer; when the kernel
delivers the event, we know exactly who to wake.

## Layout

```
                Runtime
                   │
                   ▼
       ┌───────────────────────┐
       │      Reactor          │
       │                       │
       │  kqueue fd            │
       │  poller_taken: bool   │   single-claim flag
       │  pending_count: u32   │   registered-but-not-yet-fired
       └───────────────────────┘
              ▲          │
              │          │
   register   │          │ kevent() returns events
   (from any  │          │
    blocking  │          ▼
    op)       │     ┌──────────────────────┐
              │     │   one worker at a    │
              │     │   time claims the    │
              │     │   poll via CAS       │
              │     └──────────────────────┘
              │            │
              │            │ for each event: unpark coro
              │            │ → push to P's run queue
              │            ▼
              │     ┌──────────────────────┐
              │     │   other workers      │
              │     │   pick up unparked   │
              │     │   coros via work     │
              │     │   stealing           │
              │     └──────────────────────┘
              │
              └── coroutine A parked on `read()`
                  coroutine B parked on `accept()`
                  coroutine C parked on `sleep()`  (EVFILT_TIMER)
```

Reference: `src/reactor_kqueue.zig` (the Reactor struct, register,
poll, unpark paths) and `src/runtime.zig` (the dispatch loop's
"poll reactor when idle" branch).

## Single-poller claim

The reactor's `kevent` syscall is sequential — multiple workers
calling it concurrently would all block, wasting threads. Instead,
one worker at a time claims the poll via a single CAS:

```zig
// Pseudocode of the dispatch loop's reactor branch.
if (rt.reactor_poller_taken.cmpxchgWeak(false, true, ...) == null) {
    // Claimed.
    const events = reactor.poll(timeout);
    for (events) |ev| {
        const c: *Coroutine = @ptrFromInt(ev.udata);
        unparkAndDispatch(c);
    }
    rt.reactor_poller_taken.store(false, .release);
}
```

Reference: `src/runtime.zig:430-470` (the dispatch loop's reactor
poll path).

The CAS-claim is critical because multiple workers reach the
"nothing in my queue, nothing to steal" state independently. If
they all tried to poll, they'd serialize on the kqueue lock
internally. The single-claim ensures one worker pays the kqueue
syscall cost while the others stay free to run unparked work.

A worker that fails the claim falls through to the parker — sleeps
on its own `__ulock_wait` until either work appears in its
mailbox or the poller wakes it.

## Per-event registration: EV_ONESHOT

Each kqueue event is registered with the `EV_ONESHOT` flag. Effect:
the event fires **once**, then auto-deletes from the kqueue.

Why: each blocking read/write is one-shot from the coroutine's
POV. `coroutine A` does `read()`, parks on `EVFILT_READ` for its
fd, the read happens, returns. A different read on the same fd is
a new registration. No need for "edge-triggered vs level-
triggered" cleverness — every event registers fresh.

Tradeoff: a few cycles per re-registration vs the simpler
"register and forget" model. Volt picks the simpler model
because:

1. The kqueue add-event cost is ~50 ns; spawn-hot adds many of
   these, but the dominant cost is elsewhere.
2. Edge-triggered semantics with persistent registrations
   require the coroutine to drain the fd in a loop (read until
   EAGAIN), which is harder to write correctly.
3. With ONESHOT we always know "kqueue has at most one pending
   event for this (fd, filter) pair". Cleanup on coroutine
   cancellation is trivial.

Reference: `src/reactor_kqueue.zig` — the `waitRead` / `waitWrite`
helpers that register the kevent.

## `udata` = coroutine pointer

The `udata` field of a kevent is a `void *` the kernel returns
unchanged when the event fires. We use it to carry the coroutine
pointer:

```c
struct kevent {
    uintptr_t ident;   // fd
    short     filter;  // EVFILT_READ / EVFILT_WRITE / EVFILT_TIMER
    u_short   flags;   // EV_ADD | EV_ONESHOT
    u_int     fflags;
    intptr_t  data;
    void     *udata;   // ← coroutine pointer
};
```

When the poll loop pulls an event out, it casts `udata` back to
`*Coroutine` and unparks it via the parking lot:

```zig
for (events[0..n]) |ev| {
    const c: *Coroutine = @ptrCast(@alignCast(ev.udata));
    runtime.unpark(c);
}
```

The parking lot's `unpark` enqueues `c` onto a P's mailbox (or
direct-handoffs if the unparker is on the same M as a parked
joiner). Reference: `src/runtime.zig:108-145` (`unpark`).

## Reactor as a wakeup source for timers

`volt.sleep(ns)` parks on the reactor too — but on `EVFILT_TIMER`
instead of `EVFILT_READ`:

```zig
pub fn waitTimer(self: *Reactor, ns: u64) void {
    const c = current.require();
    // Register EVFILT_TIMER for `ns` nanoseconds; udata = c.
    // ... kevent() with EV_ADD | EV_ONESHOT ...
    park.parkOn(...);
}
```

The kernel's timer heap handles delivery; we just register and
park. This is what lets `sleep` scale — no per-sleep thread, no
busy-wait loop, just one kqueue registration per sleeping
coroutine. The kernel can hold thousands of timers in its internal
heap at trivial cost.

## EAGAIN → reactor park

A typical I/O op looks like:

```zig
pub fn read(self: *TcpStream, buf: []u8) !usize {
    while (true) {
        const n = read_syscall(self.fd, buf.ptr, buf.len);
        if (n >= 0) return @intCast(n);

        const errno = std.posix.errno();
        if (errno != .AGAIN) return readError(errno);

        // EAGAIN — kernel says "not ready". Park on the reactor.
        runtime().reactor.waitRead(self.fd);
        // ... when waitRead returns, we know the fd is readable.
        // Loop back and try the syscall again.
    }
}
```

The `EAGAIN` retry loop is intentional. When `waitRead` returns,
the fd is "readable" by the kernel's notification but might already
be drained by the time our coroutine actually runs (a different
worker reads first, or a peer sent a 0-byte packet). The retry
handles all of these: read again, see EAGAIN again, park again.
In practice the first retry succeeds.

Reference: `src/net.zig` — the read/writeAll/connect/accept
patterns all follow this shape.

## Tried & rejected: per-coroutine kqueue fd

We could give each coroutine its own kqueue and let it manage its
own readiness events. Simpler conceptually — no shared registry.

The problem: kqueue fds are kernel resources, and `kevent` per
coroutine is just as serialized as `kevent` per worker (the
kernel's per-fd lock). Plus we'd burn one fd per coroutine — at
16k coroutines, that's 16k fds, hitting `RLIMIT_NOFILE` on most
systems. Sharing one kqueue across all coroutines is unambiguously
better.

## Tried & rejected: dedicated poller thread

A common pattern in async runtimes: dedicate one OS thread to
calling `kevent` in a loop, route events to worker threads via
some queue.

Why we didn't: that thread is idle while there are no events
(burning OS scheduling overhead), or oversubscribes when many
events fire (the dedicated thread is the bottleneck). The
single-claim design lets **any worker** be the poller when idle,
and the workers self-balance via work stealing once events fire.
Better resource utilization for typical workloads.

## What's not yet built

- **`io_uring` backend.** Linux 5.1+ supports `io_uring`, which
  bundles I/O issue + completion into one ring with no syscalls
  in the steady state. Volt's reactor abstraction could front this
  but the implementation isn't written. The kqueue and io_uring
  shapes differ enough that they're separate backends, not a
  shared layer.
- **`epoll` backend.** The fallback Linux path. Conceptually a
  straight port of the kqueue path (epoll has similar
  semantics — `EPOLLIN` ≈ `EVFILT_READ`, etc.) but not yet
  written.
- **`IOCP` backend.** Windows. Completion-based instead of
  readiness-based. Substantially different shape; the reactor
  interface needs an `iocp.wait_completion` flavour. Not yet
  written.
- **Cancellation-aware I/O ops.** Today `read` / `writeAll` /
  `accept` run to completion or error from the syscall. To
  cancel a parked I/O op, you `close()` the fd from another
  coroutine; the kernel returns an error to the parked syscall.
  Direct cancel-aware variants (`readCancel`) need to register
  with a `*Cancel` waiter list alongside the kqueue
  registration. Roadmapped.

## Further reading

- [Networking](/usage/networking/) — the user-facing TCP API that uses the reactor.
- [Parking lot](/architecture/parking-lot/) — the wait/wake substrate the reactor unparks through.
- Darwin `kqueue(2)` man page — official semantics.
- Tokio's I/O driver — Rust's equivalent reactor; same single-poller-claim idea, different waker mechanism (since Rust async needs to plumb wakers through `Future::poll`, not direct unpark).
- `libuv` design notes — Node.js's event loop, also kqueue/epoll/IOCP shaped.

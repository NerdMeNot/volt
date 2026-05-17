---
title: The reactor
description: One reactor per Runtime, single-poller claim, one-shot registration, *Coroutine as the wake identity. Backend-agnostic mental model — the per-platform syscall details (kqueue / epoll / io_uring / IOCP) live next door.
---

A stackful coroutine runtime needs **one place where I/O parks
and one place where I/O wakes**. The runtime owns one reactor per
process, every blocking I/O op registers itself, and a single
worker at a time harvests readiness events back to coroutine
queues.

The interface is the same on every platform — `init`, `deinit`,
`waitReadable`, `waitWritable`, `waitTimer`, `poll`, `pendingCount`.
The syscall plumbing differs (kqueue on Darwin, epoll or io_uring
on Linux, IOCP on Windows); the dispatcher integration does not.
This page is the shared mental model. The per-platform mechanics
are in [The reactor backends](/architecture/reactor-backends/).

## Mental model

> The reactor is a **postbox at the back of the post office**.
> When a coroutine does `read()` and the kernel says "no data
> yet", the runtime drops a slip into the postbox: "deliver to
> coroutine X when fd Y is readable". A single worker at a time
> reads the postbox and routes the delivered slips back to the
> coroutines. Other workers run regular work in parallel.

The slip is a kernel event with one user-data field. We set that
field to the coroutine pointer; when the kernel delivers the
event, we know exactly who to wake. The encoding differs per
platform — kqueue's `udata`, epoll's `data.ptr`, io_uring's
`user_data`, IOCP's `OVERLAPPED.hEvent` — but the convention is
the same. One cache line of state per parked op.

## Layout

```
                Runtime
                   │
                   ▼
       ┌───────────────────────┐
       │      Reactor          │
       │                       │
       │  backend fd/handle    │  kqueue / epoll / io_uring ring / IOCP
       │  poller_taken: bool   │  single-claim flag
       │  pending: u32         │  registered-but-not-yet-fired
       └───────────────────────┘
              ▲          │
              │          │
   register   │          │ poll() returns ready set
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
                  coroutine C parked on `sleep()`  (timer)
```

Reference: `src/reactor.zig` (the per-platform dispatch shim),
`src/reactor_kqueue.zig` / `src/reactor_epoll.zig` /
`src/reactor_io_uring.zig` / `src/reactor_iocp.zig` (the four
backends), and `src/runtime.zig` (the dispatch loop's "poll
reactor when idle" branch).

## Single-poller claim

Each backend's harvest syscall is sequential — multiple workers
calling it concurrently would all block, wasting threads. Instead,
one worker at a time claims the poll via a single CAS:

```zig
// Pseudocode of the dispatch loop's reactor branch.
if (rt.reactor_poller_taken.cmpxchgWeak(false, true, ...) == null) {
    // Claimed.
    const events = reactor.poll(blocking);
    for (events) |ev| {
        const c: *Coroutine = @ptrFromInt(ev.user_data);
        unparkAndDispatch(c);
    }
    rt.reactor_poller_taken.store(false, .release);
}
```

Reference: `src/runtime.zig:430-470` (the dispatch loop's reactor
poll path).

The CAS-claim is critical because multiple workers reach the
"nothing in my queue, nothing to steal" state independently. If
they all tried to poll, they'd serialize on the kernel's per-fd
or per-ring lock internally. The single-claim ensures one worker
pays the syscall cost while the others stay free to run unparked
work.

A worker that fails the claim falls through to the parker — sleeps
on its own `__ulock_wait` / `futex` / `WaitOnAddress` until either
work appears in its mailbox or the poller wakes it.

## Per-event registration: one-shot

Each registration fires **once**, then auto-deletes from the
backend's interest set. kqueue calls this `EV_ONESHOT`; epoll
calls it `EPOLLONESHOT`; IOCP gets it for free (overlapped ops
are inherently one-shot); io_uring's `IORING_OP_POLL_ADD` defaults
to one-shot.

Why: each blocking read/write is one-shot from the coroutine's
POV. `coroutine A` does `read()`, parks for read-readiness on its
fd, the read happens, returns. A different read on the same fd is
a new registration. No need for edge-triggered drain-until-EAGAIN
loops — every event registers fresh.

Tradeoff: a few cycles per re-registration vs the simpler
"register and forget" model. Volt picks the simpler model
because:

1. The register cost is ~50–200 ns depending on backend; spawn-hot
   shapes add many of these, but the dominant cost is elsewhere.
2. Edge-triggered semantics with persistent registrations require
   the coroutine to drain the fd in a loop (read until EAGAIN),
   which is harder to write correctly and pushes complexity into
   every I/O helper.
3. With one-shot we always know "the backend has at most one
   pending event for this (fd, direction) pair". Cleanup on
   coroutine cancellation is trivial.

Reference: each backend's `waitReadable` / `waitWritable` helpers
in `src/reactor_*.zig`.

## `*Coroutine` as the wake identity

Every backend exposes some "user data" field per registration that
the kernel returns unchanged on completion. We use it to carry
the coroutine pointer:

| Backend | Carrier field |
|---|---|
| kqueue | `kevent.udata` (`u64`) |
| epoll | `epoll_event.data.ptr` (`void*`) |
| io_uring | `io_uring_sqe.user_data` (`u64`) |
| IOCP | `OVERLAPPED.hEvent` (`HANDLE`) |

When the poll loop pulls an event out, it casts the carrier back
to `*Coroutine` and unparks via the runtime's `unpark` (which
enqueues onto a P's mailbox, or direct-handoffs if the unparker is
on the same M as a parked joiner). Reference:
`src/runtime.zig:108-145` (`unpark`).

## Reactor as a wakeup source for timers

`volt.sleep(ns)` parks on the reactor too. The kernel timer
mechanism varies by platform:

| Backend | Timer mechanism |
|---|---|
| kqueue | `EVFILT_TIMER` (native kqueue filter) |
| epoll | `timerfd_create` + register as `EPOLLIN` |
| io_uring | `IORING_OP_TIMEOUT` |
| IOCP | `CreateThreadpoolTimer` → `PostQueuedCompletionStatus` |

The kernel's timer heap handles delivery; we just register and
park. This is what lets `sleep` scale — no per-sleep thread, no
busy-wait loop, just one reactor registration per sleeping
coroutine. The kernel can hold thousands of timers in its internal
heap at trivial cost.

## EAGAIN → reactor park

A typical I/O op looks like:

```zig
pub fn read(self: *TcpStream, buf: []u8) !usize {
    while (true) {
        const n = read_syscall(self.fd, buf.ptr, buf.len);
        if (n >= 0) return @intCast(n);

        if (!isAgain(errno())) return readError();

        // EAGAIN — kernel says "not ready". Park on the reactor.
        runtime().reactor.waitReadable(self.fd);
        // ... when waitReadable returns, we know the fd is readable.
        // Loop back and try the syscall again.
    }
}
```

The retry loop is intentional. When `waitReadable` returns, the
fd is "readable" by the kernel's notification but might already
be drained by the time our coroutine actually runs (a different
worker reads first, or a peer sent a 0-byte packet). The retry
handles all of these: read again, see EAGAIN again, park again.
In practice the first retry succeeds.

On Windows, the same shape holds — except IOCP is natively
completion-based, so Volt's IOCP backend submits a zero-byte
`WSARecv` / `WSASend` as a readiness probe. The completion fires
when the socket would be readable/writable, the coroutine wakes,
and the same retry loop issues the real `recv` / `send`. See the
[Windows IOCP polyfill](/architecture/reactor-backends/#windows-iocp-polyfilled-as-readiness)
section for the trade-offs.

Reference: `src/net.zig` — the read/writeAll/connect/accept
patterns all follow this shape; the helpers (`readAsync`,
`writeAsync`, `readFull`, `writeAll`, `setNonblock`) live per-
backend in `src/reactor_*.zig`.

## Tried & rejected: per-coroutine reactor handle

We could give each coroutine its own kqueue / epoll fd / io_uring
ring and let it manage its own readiness events. Simpler
conceptually — no shared registry.

The problem: kernel notification handles are kernel resources, and
the harvest syscall per coroutine is just as serialized as per
worker (the kernel's per-fd or per-ring lock). Plus we'd burn one
fd per coroutine — at 16k coroutines, that's 16k fds, hitting
`RLIMIT_NOFILE` on most systems. Sharing one reactor across all
coroutines is unambiguously better.

## Tried & rejected: dedicated poller thread

A common pattern in async runtimes: dedicate one OS thread to
the harvest syscall in a loop, route events to worker threads via
some queue.

Why we didn't: that thread is idle while there are no events
(burning OS scheduling overhead), or oversubscribes when many
events fire (the dedicated thread is the bottleneck). The
single-claim design lets **any worker** be the poller when idle,
and the workers self-balance via work stealing once events fire.
Better resource utilization for typical workloads.

## Cross-platform status

| Platform | Backend | Status |
|---|---|---|
| Darwin / BSD | kqueue | **Working** — primary dev platform; full bench suite green |
| Linux | epoll | **Working** — cross-compile + epoll-specific tests green; bench parity expected |
| Linux | io_uring | **Working (poll mode)** — `Runtime.Config.io_backend = .io_uring`; `IORING_OP_POLL_ADD` shape, not yet kernel-DMA-into-app-buffer |
| Windows | IOCP | **Cross-compiles** — implemented as readiness polyfill (zero-byte `WSARecv`/`WSASend`); runtime validation deferred to a Windows VM/CI pass |

The Linux `.io_backend` field on `Runtime.Config` defaults to
`.auto` (= epoll today; io_uring detection on supported kernels
will be added once io_uring proves out in real workloads). On
Darwin and Windows the field is accepted but ignored — kqueue
or IOCP is fixed.

## Cancellation-aware I/O

Today `read` / `writeAll` / `accept` run to completion or error
from the syscall. To cancel a parked I/O op, you `close()` the
fd from another coroutine; the kernel returns an error to the
parked syscall.

Direct cancel-aware variants (`readCancel`, etc.) would register
with a `*Cancel` waiter list alongside the reactor registration —
a small parking-lot integration on the cancel side. Roadmapped;
not in `src/` today.

## Further reading

- [The reactor backends](/architecture/reactor-backends/) — per-platform syscall walkthroughs (kqueue / epoll / io_uring / IOCP).
- [Networking](/usage/networking/) — the user-facing TCP API that uses the reactor.
- [Parking lot](/architecture/parking-lot/) — the wait/wake substrate the reactor unparks through.
- Tokio's I/O driver — Rust's equivalent reactor; same single-poller-claim idea, different waker mechanism (since Rust async needs to plumb wakers through `Future::poll`, not direct unpark).
- `libuv` design notes — Node.js's event loop, also kqueue/epoll/IOCP shaped.

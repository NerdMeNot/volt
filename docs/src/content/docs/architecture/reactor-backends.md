---
title: The reactor backends
description: Per-platform reactor implementation deep-dive. kqueue on Darwin/BSD, epoll on Linux, io_uring on Linux 5.10+ (poll mode), IOCP on Windows polyfilled as readiness via zero-byte WSARecv/WSASend.
---

The [reactor](/architecture/reactor/) page gives the shared
mental model: one reactor per Runtime, single-poller claim,
one-shot registration, `*Coroutine` carried as the wake identity,
a dispatcher that pulls events and calls `runtime.unpark`. This
page covers the four implementations of that interface — what
syscalls each one makes, where the perf wins and losses come
from, and the trade-offs that were rejected during design.

## Shared interface

Every backend exports the same struct:

```zig
pub const Reactor = struct {
    pub fn init() !Reactor;
    pub fn deinit(self: *Reactor) void;

    pub fn waitReadable(self: *Reactor, fd: i32) void;
    pub fn waitWritable(self: *Reactor, fd: i32) void;
    pub fn waitTimer(self: *Reactor, ns: u64) void;

    pub fn poll(self: *Reactor, blocking: bool) usize;
    pub fn pendingCount(self: *const Reactor) u32;
};
```

Plus free helpers — `setNonblock`, `readAsync`, `writeAsync`,
`readFull`, `writeAll` — that handle the platform-specific
syscall family (`fcntl(O_NONBLOCK)` vs `ioctlsocket(FIONBIO)`,
`read`/`write` vs `recv`/`send`, `EAGAIN` vs `WSAEWOULDBLOCK`).
The dispatch shim at `src/reactor.zig` picks the implementation
at comptime based on `builtin.os.tag`.

## kqueue — Darwin / BSD

Primary dev platform. Full bench suite green; the reference
implementation the other three are ported from.

**Files:** `src/reactor_kqueue.zig`.

**Syscalls used:**

| Syscall | Purpose |
|---|---|
| `kqueue()` | Create the kqueue fd at `Reactor.init`. |
| `kevent(kq, changes, ...)` | Register a `(fd, filter)` with `EV_ADD \| EV_ONESHOT` from `waitReadable`/`waitWritable`. The single syscall both submits the change and (optionally) harvests events. |
| `kevent(kq, NULL, 0, events, ...)` | Pure-harvest call from `poll`. Blocks until at least one event fires when `blocking=true`. |
| `close(kq)` | Released at `Reactor.deinit`. |

**Per-event wire shape:**

```c
struct kevent {
    uintptr_t ident;   // fd (or coroutine ptr for timers)
    short     filter;  // EVFILT_READ / EVFILT_WRITE / EVFILT_TIMER
    u_short   flags;   // EV_ADD | EV_ONESHOT
    u_int     fflags;
    intptr_t  data;
    void     *udata;   // ← coroutine pointer
};
```

`udata` survives unchanged through the kernel; the poll loop
casts it back to `*Coroutine` and hands to `runtime.unpark`.

**Timers:** `EVFILT_TIMER` is native — `kevent` takes a duration,
fires once after that elapses. No timerfd, no thread pool. Kernel
timer resolution is bounded below by ~1 µs on Darwin arm64;
shorter sleeps round up.

**Why one-shot:** kqueue defaults to level-triggered, persistent.
`EV_ONESHOT` makes each registration fire-once-then-auto-delete,
matching the "one wake per `waitReadable` call" contract that
the rest of Volt assumes.

## epoll — Linux

Selected when `Runtime.Config.io_backend = .epoll`, or as the
fallback on kernels older than 5.10 (where io_uring isn't
available in the shape we need).

**Files:** `src/reactor_epoll.zig`.

**Syscalls used:**

| Syscall | Purpose |
|---|---|
| `epoll_create1(EPOLL_CLOEXEC)` | Create the epoll fd at `Reactor.init`. |
| `epoll_ctl(epfd, EPOLL_CTL_ADD, fd, ev)` | Register an fd with `EPOLLIN \| EPOLLONESHOT` or `EPOLLOUT \| EPOLLONESHOT`. One syscall per registration — this is the main place epoll pays vs io_uring. |
| `epoll_wait(epfd, events, max, timeout_ms)` | Pure-harvest call from `poll`. |
| `timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC)` + `timerfd_settime` | Per-sleep timer fd. Registered into the same epoll fd with `EPOLLIN \| EPOLLONESHOT`; closed on wake. |
| `close()` | Per-timer cleanup; per-reactor at `deinit`. |

**Per-event wire shape:**

```c
struct epoll_event {
    uint32_t events;
    epoll_data_t data;   // union { void*; int; uint32_t; uint64_t }
};
```

Volt uses `data.ptr` to carry the coroutine pointer — exact analog
of kqueue's `udata` pattern.

**Why level-triggered + `EPOLLONESHOT`, not `EPOLLET`:** mio (the
Rust async I/O lib) defaults to edge-triggered, which forces the
user code to drain the fd in a loop until `EAGAIN`. Volt's I/O
helpers already issue one syscall per loop iteration and re-
register cleanly on each `EAGAIN`; the simpler level-triggered +
one-shot shape matches the rest of the runtime's wake contract.

**Timer cost:** `timerfd_create` + `timerfd_settime` + `close`
adds ~1 µs per sleep on Linux. For sleeps of any meaningful
duration that's a rounding error. A per-P timerfd pool is the
natural optimisation if profiling shows it matters at high sleep
rates — deferred until measured.

**Deliberately not used:** `signalfd` (Volt's user-facing signal
handling lives outside the runtime), `eventfd` (the parking lot
owns inter-worker coordination), `EPOLLEXCLUSIVE` (the single-
poller-claim in `runtime.zig` already serialises poll calls so
thundering-herd protection isn't needed).

## io_uring — Linux 5.10+ (poll mode)

Selected when `Runtime.Config.io_backend = .io_uring`, or
auto-probed on Linux ≥ 5.10. Uses `IORING_OP_POLL_ADD` for fd
readiness and `IORING_OP_TIMEOUT` for sleeps, keeping the same
readiness contract as kqueue / epoll. The buffer-ownership ops
(`IORING_OP_RECV` / `_SEND`) that give io_uring its full perf
advantage are deliberately not used — they'd force a parallel
completion-based path in `net.zig`, which is a future v1.x
evolution if perf data justifies it.

**Files:** `src/reactor_io_uring.zig`. Uses `std.os.linux.IoUring`
from Zig's std for ring setup, SQE prep, and CQE consumption —
saved ~200 lines vs hand-rolling.

**Syscalls used:**

| Syscall | Purpose |
|---|---|
| `io_uring_setup(entries, params)` | Create the SQ + CQ rings at `Reactor.init`. Ring size 256 (matches `KEV_BATCH=32` × overhead headroom). |
| `io_uring_enter(fd, to_submit, min_complete, flags, ...)` | The single workhorse: `submit_and_wait` submits all queued SQEs and waits for ≥ 1 completion. Called from `poll(blocking=true)`. |
| `munmap()` + `close()` | Per-reactor at `deinit`. |

**Per-event wire shape:**

```c
struct io_uring_sqe {
    __u8  opcode;        // IORING_OP_POLL_ADD or IORING_OP_TIMEOUT
    __u32 fd;
    __u32 poll_events;   // POLLIN / POLLOUT (matches epoll's events)
    __u64 user_data;     // ← coroutine pointer
    // ... other fields irrelevant for poll mode
};

struct io_uring_cqe {
    __u64 user_data;     // ← coroutine pointer, returned unchanged
    __s32 res;
    __u32 flags;
};
```

**The perf win over epoll:**

1. **Batched submission.** Each `waitReadable` / `waitTimer`
   enqueues an SQE *without invoking a syscall*. Only when the
   dispatcher calls `poll(blocking=true)` do we
   `submit_and_wait` — that single syscall submits the entire
   pending batch and waits for at least one completion. epoll
   needs one `epoll_ctl` per registration plus one `epoll_wait`
   to harvest.
2. **Userspace ring polling.** The SQ/CQ rings are mmap'd shared
   memory; producers and consumers communicate via the rings
   without crossing the syscall boundary at all in the steady
   state.

Expected ~10–20% TCP throughput improvement on loopback;
real-workload numbers vary.

**SQE production is single-threaded.** `IoUring`'s SQ tail is
shared mutable state and not thread-safe for concurrent
producers. Volt's multi-worker model means any worker can queue
an SQE; a spinlock serialises the produce path. Submission and
CQE consumption stay on the single poller-claimed worker so no
additional sync there.

**`flags = 0` for ring setup.** `IORING_SETUP_SINGLE_ISSUER` +
`IORING_SETUP_DEFER_TASKRUN` are available on kernel ≥ 6.0 and
reduce kernel coordination but require single-issuer semantics
(one thread submitting). Deferred until we have benchmark data
showing it matters.

## Windows IOCP — polyfilled as readiness

IOCP is fundamentally completion-based: you submit an I/O
operation with a buffer, the kernel completes it asynchronously,
and your code reads the result. Volt's `net.zig` is
readiness-based — coroutines do their own `read` / `write` after
a wake. To bridge the two without forking `net.zig` per platform,
we use Go's `netpoll_windows.go` pattern: a zero-byte overlapped
op as a readiness probe.

**Files:** `src/reactor_iocp.zig`.

**Syscalls used:**

| Syscall | Purpose |
|---|---|
| `WSAStartup(MAKEWORD(2,2), ...)` | One-shot per process at first `Reactor.init`. Idempotent via process-static flag. |
| `CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0)` | Create the IOCP handle at `Reactor.init`. `NumberOfConcurrentThreads = 0` lets the system pick (= NumCPU). |
| `CreateIoCompletionPort(socket, iocp, key, 0)` | Associate a socket with the IOCP after creation. CompletionKey = socket handle. |
| `WSARecv(socket, buffers, 1, ..., overlapped, NULL)` | Submit a zero-byte read as a read-readiness probe. Completion fires when the socket becomes readable. |
| `WSASend(socket, buffers, 1, ..., overlapped, NULL)` | Same shape for write-readiness. |
| `GetQueuedCompletionStatusEx(iocp, entries, 32, ...)` | Batched harvest (Vista+). Same shape as kqueue's `kevent`-with-batch or epoll's `epoll_wait`. |
| `CreateThreadpoolTimer` + `SetThreadpoolTimer` + `WaitForThreadpoolTimerCallbacks` + `CloseThreadpoolTimer` | Per-sleep timer via the Windows thread pool. The callback fires on a pool thread and `PostQueuedCompletionStatus`'s a wake into the IOCP with `OVERLAPPED = NULL` (distinguishes timer wakes from I/O completions). |
| `CloseHandle(iocp)` | At `Reactor.deinit`. |

**Per-completion wire shape:**

```c
typedef struct _OVERLAPPED_ENTRY {
    ULONG_PTR  lpCompletionKey;       // socket handle (or coroutine ptr for timer)
    LPOVERLAPPED lpOverlapped;        // NULL for timer wakes
    ULONG_PTR  Internal;
    DWORD      dwNumberOfBytesTransferred;
} OVERLAPPED_ENTRY;
```

For I/O completions, `lpOverlapped` is non-null and Volt stashes
the coroutine pointer in `OVERLAPPED.hEvent` (a field IOCP itself
doesn't touch). For timer wakes, `lpOverlapped` is null and the
coroutine pointer rides in `lpCompletionKey` directly. The poll
loop discriminates on which field is set.

**OVERLAPPED is stack-allocated.** Volt's stackful coroutines have
stable mmap'd stacks, so the `OVERLAPPED` pointer is valid until
the coroutine resumes. No heap pool needed — same lifetime
guarantee as kqueue's `udata` or epoll's `data.ptr`.

**The trade-off: extra syscall per real I/O op.** The zero-byte
polyfill loses IOCP's main perf win — kernel-initiated DMA into
application buffers. We pay an extra `recv` / `send` syscall per
real I/O op vs a "native" IOCP design that owns the buffer.
Justification:

1. `net.zig` stays uniform across all platforms (one code path).
2. Go shipped this design for 15+ years; it works.
3. A buffer-ownership Windows path can be added later without
   disrupting POSIX if perf data ever justifies it.

**Runtime verification status.** Volt's primary dev platform is
Darwin; the IOCP implementation is best-effort based on Win32 /
Winsock documentation and Go's `netpoll_windows.go` reference.
The cross-compile gate (`zig build-lib src/lib.zig -target
x86_64-windows-gnu -lc -fno-emit-bin`) exits 0. Full end-to-end
Windows runtime support additionally needs:

1. A `WaitOnAddress` / `WakeByAddressSingle` backend in
   `src/parker.zig` (separate landing).
2. A Windows VM or CI runner.
3. Running the full bench gate + stress test.
4. Any bug fixes that surface (especially around `OVERLAPPED`
   lifetime, `WSAStartup` ordering, and the threadpool timer
   cleanup path).

Anyone with a Windows dev environment is welcome to contribute
the runtime validation pass.

## Comparing the four

| Property | kqueue | epoll | io_uring (poll) | IOCP |
|---|---|---|---|---|
| Model | readiness | readiness | readiness | completion (polyfilled) |
| Register syscall | bundled w/ harvest | one per register | none (deferred) | one per register |
| Harvest syscall | `kevent` | `epoll_wait` | `io_uring_enter` | `GetQueuedCompletionStatusEx` |
| User-data field | `udata` (`u64`) | `data.ptr` (`void*`) | `user_data` (`u64`) | `OVERLAPPED.hEvent` |
| Timer mechanism | `EVFILT_TIMER` (native) | `timerfd` + epoll | `IORING_OP_TIMEOUT` | thread-pool timer → `PostQueuedCompletionStatus` |
| One-shot shape | `EV_ONESHOT` | `EPOLLONESHOT` | inherent | inherent (per overlapped op) |
| Cross-platform unity | reference | port | port | polyfill |

The interface above the dispatch shim is identical; the four
implementations differ only in syscall plumbing. That's the whole
point of the abstraction — adding a fifth backend (Solaris event
ports? io_uring native completion mode?) is a self-contained file
plus an entry in `src/reactor.zig`'s comptime switch.

## Cross-platform bench

`zig build bench-reactor-throughput` runs a single-connection
TCP ping-pong with a 1-byte payload, one worker, tight loop. The
register + park + kernel-deliver + unpark + dispatch cycle
dominates (no payload-copy or concurrency masking). Useful as a
"did the backend pull its weight" receipt when comparing across
platforms — kqueue / epoll / io_uring / IOCP numbers should track
within ~2× of each other on similarly-clocked hardware; outliers
point at a backend bug.

Reference number from the primary dev platform (Darwin arm64,
ReleaseFast): median ~10 µs per reactor wake. Linux / Windows
baselines will be added as those platforms get real CI runs.

## Further reading

- [The reactor](/architecture/reactor/) — the shared mental model and dispatcher integration.
- [Networking](/usage/networking/) — the user-facing TCP API.
- Darwin `kqueue(2)` man page — official semantics.
- Linux `epoll(7)` and `io_uring(7)` man pages — both well-written.
- Microsoft Learn: [I/O Completion Ports](https://learn.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports) — the canonical IOCP reference.
- Go's `runtime/netpoll_*.go` — same four-backend shape, same zero-byte-WSARecv polyfill on Windows.
- `libuv` `src/{unix,win}/*.c` — Node.js's reactor across the same four platforms.

# Reactor restructure proposal

**Status:** Proposed, 2026-05-24
**Owner:** Reactor hardening branch (`reactor-hardening`)
**Driver:** Race-after-race pattern in `closeFd` / `cancelCoro` across
all four backends, culminating in the io_uring SQ-lock deadlock found
2026-05-24. Root cause is architectural, not local. This document
proposes the replacement architecture and the migration order.

This document is grounded in two reference implementations read
end-to-end for this proposal: **Go's `runtime/netpoll.go`** (Go 1.23
HEAD) and **mio's `src/sys/windows/{selector.rs, afd.rs}`** (mio
1.0). Quoted line numbers refer to those sources. The `internal/poll`
ordering was confirmed by web-fetching the canonical
`go.dev/src/internal/poll/{fd_unix.go, fd_windows.go,
fd_poll_runtime.go}`.

## 1. The diagnosis

Every race we have fixed in the last three weeks has the same shape:
two coroutines, two kernel registrations, and the wake protocol
threads through both user-space *and* the kernel. The fixes are
band-aids on a coupling that should not exist:

| Commit | Fix | Underlying coupling |
|---|---|---|
| `024ab3a` | submit poll_add before cancel SQE | wake requires kernel arbitration of two SQEs in submission order |
| `24c31b3` | insertWaiter before kernel ADD | close requires side-table lookup *plus* kernel deregister |
| `aabf1a8` | release SQ lock before blocking enter | wake requires the firer to push an SQE through a lock the poller is holding |

Go does not have these races. Not because Go is more careful — because
Go's architecture **does not put the kernel on the wake path.** A
parked goroutine is woken by a user-space CAS in a state machine. The
kernel registration is set up once when the fd is opened and torn
down once when the fd is closed; it does not participate in any
individual wake/cancel/close decision.

## 2. What Go actually does

The crown jewel is `poll_runtime_pollUnblock` (`runtime/netpoll.go:451`).
This is what runs when a socket is closed while a goroutine is parked
in `Read` or `Write`:

```go
func poll_runtime_pollUnblock(pd *pollDesc) {
    lock(&pd.lock)
    pd.closing = true       // (1) flip user-space flag
    pd.rseq++; pd.wseq++    // (2) invalidate stale deadline timers
    pd.publishInfo()        // (3) publish closing to lock-free readers
    rg := netpollunblock(pd, 'r', false, &delta)  // (4) pop parked G via state machine
    wg := netpollunblock(pd, 'w', false, &delta)
    // ... stop deadline timers ...
    unlock(&pd.lock)
    if rg != nil { netpollgoready(rg, 3) }  // (5) wake outside lock
    if wg != nil { netpollgoready(wg, 3) }
}
```

**Nothing here touches the kernel.** The kernel registration is left
exactly as it was; pending events from the kernel will arrive and be
silently dropped via the `fdseq` filter (`runtime/netpoll_epoll.go:169`).

The state machine on `rg` / `wg` (`runtime/netpoll.go:51-68`) has four
states:

- `pdNil` — no one waiting, no event pending
- `pdReady` — event pending, no one waiting yet
- `pdWait` — goroutine is about to park (transient, between
  intent-to-park and actual park-commit)
- G pointer — goroutine is parked

Every wake source (I/O, deadline, close, cancel) goes through the
same `netpollunblock` (`netpoll.go:591`):

```go
func netpollunblock(pd *pollDesc, mode int32, ioready bool, delta *int32) *g {
    // ... CAS the state to pdReady (for I/O) or pdNil (for close/timeout) ...
    // returns the G that was parked, if any
}
```

Three things make this work:

**(a) Persistent fd registration.** `netpollopen` is called once when
the fd is created (`netpoll_epoll.go:49`): `EPOLL_CTL_ADD` with
`EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET`. Both readability and
writability are registered together with edge-triggered semantics.
The fd is then in epoll forever. There is no `EPOLL_CTL_ADD` per
read or per write. On kqueue (`netpoll_kqueue.go:32-66`), the same:
register `EVFILT_READ` and `EVFILT_WRITE` with `EV_CLEAR` once.
`netpollclose` on kqueue is even a literal no-op — closing the fd
auto-removes the kevents.

**(b) Stale-event filter via `fdseq`.** Every kernel event carries a
`taggedPointer(pd, fdseq)`. When the event fires, `netpoll` checks
`pd.fdseq.Load() == tag` (`netpoll_epoll.go:169-172`). If the
pollDesc has been freed and reused (different `fdseq`), the event
is dropped. This is how Go handles the kernel still delivering
events for a closed fd.

**(c) Close ordering, enforced by a refcount + semaphore.** The
`internal/poll.FD.Close` path goes:

```
FD.Close():
  fdmu.increfAndClose()         # bump refcount, mark for close
  pd.evict() = pollUnblock      # wake all waiters (no syscall)
  decref()                      # waiters return, drop their refs
  Semacquire(csema)             # wait until refcount == 0
  → destroy():
      pd.close() = pollClose    # kernel deregister
      CloseFunc(Sysfd)          # actual close(2) / closesocket()
```

The semaphore guarantees no in-flight syscall references the
`Sysfd` when the close syscall happens. The kernel deregister
precedes the close on Unix; on Windows it is a no-op because
closing the socket implicitly aborts any pending OVERLAPPED ops via
IOCP (Go does NOT call `CancelIoEx` for sockets — only for pipes;
`fd_windows.go:1289-1303`).

## 3. What mio does on Windows (for comparison)

mio takes a different route on Windows. Rather than completion-based
I/O like Go, it uses the **AFD driver** — the kernel driver that
backs Winsock — and issues `IOCTL_AFD_POLL` operations through it to
get readiness-style notifications (`mio/src/sys/windows/afd.rs:15`,
`selector.rs:142`). This is the same trick libuv and wepoll use. It
is undocumented but stable in practice.

Pros: keeps the user-facing API identical across platforms
(everything is readiness; the caller does WSARecv/WSASend itself
after a wake).

Cons: `\Device\Afd\Mio` is not a documented Win32 surface;
Microsoft could break it. The state machine inside mio's `SockState`
(`selector.rs:114-258`) is non-trivial because AFD_POLL is
single-interest — changing the registered events requires cancel-
and-resubmit.

**We will not follow mio's Windows approach.** Reason: Go's
completion-based approach is the officially supported high-perf I/O
model on Windows; it's how every long-lived Windows-native server
works (IIS, SQL Server, etc.). We accept that the read/write path
diverges slightly on Windows (issue OVERLAPPED op, wait for
completion) — that's fine; the close semantics are what we're
unifying, and those are uniform under Go's design.

## 4. Decisions

These are made; no further input needed before code starts.

**D1. Kernel minimum: Linux ≥ 5.15, recommended ≥ 6.1.**
CLAUDE.md's current claim of ≥ 5.1 is wrong — we already require
`IORING_OP_TIMEOUT` (5.4) and `IORING_OP_ASYNC_CANCEL` (5.5).
Bumping to 5.15 (Ubuntu 22.04 LTS, GitHub CI runs 6.8+) enables
`IORING_POLL_ADD_MULTI` (5.13) which is the load-bearing feature
for the new io_uring path. ≥ 6.1 additionally enables
`IORING_SETUP_SINGLE_ISSUER` + `IORING_SETUP_DEFER_TASKRUN` for
better perf; we will gate those at runtime via `io_uring_get_probe`
and document RHEL 9 (kernel 5.14) as "works, degraded" rather than
"unsupported".

**D2. Per-fd `PollDesc` struct, owned by the socket type.**
Allocated when the socket is created, freed when the socket is
fully closed. Holds the state machine, `closing` flag, `fdseq`
counter, and a small mutex for slow-path state changes.

**D3. Persistent kernel registration per fd, registered once at
socket creation:**
- **kqueue**: `EV_ADD | EV_CLEAR` for `EVFILT_READ` and `EVFILT_WRITE`. `netpollclose` is a no-op (auto-removed on `close(fd)`).
- **epoll**: `EPOLL_CTL_ADD` with `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET`. Deregister via `EPOLL_CTL_DEL` on close (best-effort; ignore EBADF).
- **io_uring**: `IORING_OP_POLL_ADD` with the `IORING_POLL_ADD_MULTI` flag. Deregister via `IORING_OP_POLL_REMOVE` on close.
- **IOCP**: `CreateIoCompletionPort` to associate the socket. No deregister API — close implicitly removes.

**D4. State machine on `rg`/`wg`, four states, Go-style.**
`pdNil` / `pdReady` / `pdWait` / `coro ptr`. All wake sources
(I/O CQE/kevent, cancel.fire, deadline, close) route through one
`unparkWaiter(pd, mode)` function.

**D5. `closeFd` is pure user-space.** Pops parked coros via the
state machine, wakes them via `runtime.unpark`. No kernel syscall.
The kernel deregister and the actual `close(fd)` happen later,
gated by a per-PollDesc refcount + semaphore (Go's `fdmu` + `csema`
pattern).

**D6. IOCP becomes truly completion-based.** Read/Write issue
`WSARecv`/`WSASend` with an OVERLAPPED tied to the PollDesc. Close
just calls `closesocket()` — pending OVERLAPPED ops complete with
`WSA_OPERATION_ABORTED` via IOCP. We drop the current
CancelIoEx-on-close pattern (it was modelled on Boost.Asio's, which
is correct for Boost's invariants but not for ours).

**D7. Stale-event filter via `fdseq` tag.** `taggedPointer(pd, seq)`
goes into kernel event `udata`/`data`. Poll loop checks seq match
before unparking.

**D8. The `Cancel` primitive's `cancelCoro` becomes a state-machine
CAS.** No reactor syscall. It iterates its waiter list and CAS-pops
each coro from the PollDesc state.

**D9. Eliminate the side-table** (`fd → coro` map in each reactor).
Replaced by the PollDesc field on the socket struct.

**D10. Eliminate the io_uring SQ producer lock for the wait path.**
The only SQ producers are socket creation (one POLL_ADD per fd) and
socket close (one POLL_REMOVE per fd). Both are rare. A small lock
is fine; the deadlock scenario from `aabf1a8` cannot occur because
no wake operation needs to push an SQE.

## 5. The new architecture, file by file

```
src/
├── poll_desc.zig         NEW. PollDesc struct, state machine,
│                              netpollunblock equivalent, fdseq, refcount.
├── reactor_kqueue.zig    REWRITTEN. open(fd, pd) registers EV_ADD|EV_CLEAR
│                              for both filters once. poll() drains kevents,
│                              uses fdseq filter, calls unparkWaiter(pd, mode).
│                              No side-table. No per-wait kevent.
├── reactor_epoll.zig     REWRITTEN. Same pattern: register-once with EPOLLET.
├── reactor_io_uring.zig  REWRITTEN. POLL_ADD_MULTI per fd, single SQE.
│                              Drop SQ producer lock for wait path.
│                              Drop interrupt eventfd (replace with
│                              io_uring's native wake — POST a CQE via cancel
│                              on a sentinel user_data, or use eventfd via a
│                              persistent POLL_ADD).
├── reactor_iocp.zig      REWRITTEN. Completion-based Read/Write.
│                              No CancelIoEx in close path. PollDesc state
│                              machine for cancel.
├── net/
│   ├── tcp.zig           UPDATED. Hold a PollDesc on Stream/Listener.
│   │                              Read/Write use pd.wait(mode).
│   ├── udp.zig           UPDATED. Same.
│   └── ...
└── cancel.zig            UPDATED. cancelCoro becomes pd.wakeCancel(coro)
                                via the state machine — no reactor syscall.
```

## 6. The PollDesc API (concrete)

```zig
pub const PollDesc = struct {
    // Hot fields — accessed lock-free by waiters and the poller.
    rg: std.atomic.Value(usize),  // pdNil/pdReady/pdWait/coro_ptr
    wg: std.atomic.Value(usize),
    info: std.atomic.Value(u32),  // closing flag, fdseq, error bits

    // Slow-path fields — under lock.
    lock: Mutex,
    closing: bool,
    rd: i64,                       // read deadline (or -1 expired)
    wd: i64,
    fdseq: std.atomic.Value(u32),

    // Lifetime.
    refcount: std.atomic.Value(u32),
    close_sema: Parker,
    fd: i32,
    reactor: *Reactor,

    // ─── Lifecycle ────────────────────────────────────────────────
    pub fn init(self: *PollDesc, reactor: *Reactor, fd: i32) !void;
    pub fn deinit(self: *PollDesc) void;

    // ─── User-facing wait ────────────────────────────────────────
    /// Wait for `mode` ('r' or 'w'). Returns:
    ///   .ready    → fd is ready, retry the syscall
    ///   .closed   → fd is closing/closed, return error
    ///   .timeout  → deadline expired, return error
    ///   .cancelled → cancel fired, return error
    pub fn wait(self: *PollDesc, mode: u8) WaitResult;
    pub fn waitCancel(self: *PollDesc, mode: u8, c: *Cancel) WaitResult;

    // ─── Close (user-space wake) ─────────────────────────────────
    /// Mark closing, wake all parked waiters via the state machine.
    /// Does NOT call the kernel. Caller follows up with close(fd).
    pub fn evict(self: *PollDesc) void;

    // ─── Reactor callbacks ───────────────────────────────────────
    /// Called from the poller when the kernel reports readiness.
    /// Transitions rg/wg from pdWait/coro → pdReady, returns the
    /// coro to unpark.
    pub fn deliverReady(self: *PollDesc, mode: u8) ?*Coroutine;

    /// Called from Cancel.fire to wake a parked coro on this fd.
    /// State machine CAS, no syscall.
    pub fn deliverCancel(self: *PollDesc, mode: u8) ?*Coroutine;
};
```

## 7. Migration order

Each step keeps the full test suite + bench suite green before the
next. No "follow up later" — per CLAUDE.md's phase-landing protocol.

**Step 1: `poll_desc.zig` + tests.** No callers yet. Unit tests
exercise: state-machine transitions, the register-then-park race
window, close races stress test (just on the state machine, no
reactor), fdseq stale-event detection. ~500 LoC + ~400 LoC tests.
Must land green before any backend changes.

**Step 2: Migrate kqueue.** Smallest delta because `EV_CLEAR` makes
persistent registration straightforward and kqueue's `netpollclose`
is a no-op. Reorganize `reactor_kqueue.zig` to: register at socket
creation, poll loop dispatches events to PollDesc.deliverReady,
deregister-on-close is no-op. Delete the side-table. Delete per-wait
kevents. macOS CI green.

**Step 3: Migrate epoll.** Similar shape; deregister via
`EPOLL_CTL_DEL` on close. Linux x86_64 + arm64 CI green.

**Step 4: Migrate io_uring.** Adopt POLL_ADD_MULTI. Drop the SQ
producer lock for the wait path. Reactor-internal wake (was:
interrupt eventfd) → can stay as is or switch to a dedicated
sentinel CQE. Bump CLAUDE.md kernel-version claim. Linux io_uring CI
green.

**Step 5: Migrate IOCP.** This is the biggest delta — flip
Read/Write to completion-based. Drop CancelIoEx on close. Should fix
the Windows UDP IPv6 hang at test 121 because the close path no
longer tries to coordinate with an in-flight op via cancellation.
Windows CI green.

**Step 6: Cleanup.** Delete dead code: side-table types, per-wait
SQE/kevent helpers, the cancelCoro reactor paths, the
register-then-fire and close-vs-register race-tracking comments.
Bench full suite, capture new baselines, document the
syscall-per-RTT reduction.

Each step is a separate PR onto `reactor-hardening`.

## 8. What this kills

- close-vs-register race (no kernel register at wait time → nothing to race)
- cancel-vs-register race (same)
- io_uring SQ-lock-across-blocking-syscall deadlock (no wake-path SQEs)
- io_uring submit-before-cancel SQE batching race (no per-wait SQEs to cancel)
- ENOENT arbitration heuristics in cancelCoro (no kernel deregister to fail)
- the side-table `fd → coro` lookup and its lock
- one `EPOLL_CTL_ADD` / one `kevent ADD` / one `poll_add SQE` per coro park

## 9. What it costs

- One `PollDesc` allocation per socket creation (~96 bytes incl. atomics + mutex)
- One extra struct field on TcpStream / UdpSocket / TcpListener
- IOCP backend grows a completion-based Read/Write path (currently
  uses a readiness-emulation shim)
- A bench refresh; numbers will move, mostly favorably on Linux
  (one fewer syscall per RTT) but the io_uring multishot path needs
  to be re-measured under contention

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| POLL_ADD_MULTI quirks (e.g., overflow semantics under heavy traffic) | Step 4 has stress tests at 10k+ events/sec; fall back to one-shot rearm if multishot drops events |
| IOCP rewrite breaks AcceptEx/ConnectEx flows that already work | Step 5 ports those first, validates, then changes Read/Write |
| Refcount/semaphore ordering bugs in the close path | Step 1 has explicit tests for: close-during-wait, close-during-cancel, close-during-deadline-fire, close-during-close (idempotent) |
| Bench regression on tcp-echo | Step 6 has a full bench gate; revert any backend that loses >5% on the canonical shapes |
| Migration takes 2-3 weeks of focused work | This is the cost of doing it right; the band-aid path has already cost three weeks and we'd be back here in another two |

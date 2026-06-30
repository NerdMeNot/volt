---
title: Reactor audit (2026-05-23)
description: Cross-platform consistency audit of the four reactor backends (kqueue, epoll, io_uring, IOCP). Codifies known gaps as a driver for the reactor-hardening branch.
---

This page is a point-in-time audit of the four reactor backends.
Each finding is paired with the file:line where it lives, a short
explanation of the failure mode, and the fix task on the
`reactor-hardening` branch.

Audit date: **2026-05-23**. Re-run from scratch any time the
backend surfaces or close/cancel paths change materially.

## What the audit covers

- The public surface of every backend (kqueue, epoll, io_uring,
  IOCP) — does every backend expose the same set of methods with
  compatible signatures?
- Cancel ordering — does every `waitXCancel` register with the
  kernel BEFORE adding the coroutine to the `Cancel` waiter list,
  closing the cancel-then-register race fixed in `0af6a23`?
- fd-close paths — what happens to a parked coroutine when the
  fd it's waiting on is closed by another coroutine?
- Error mapping — same kernel error → same Volt error variant on
  every platform?
- Timer handling — `ns=0`, very large `ns`, negative inputs cast
  through `u64`?
- Cross-platform branch points in `src/net/*` and `src/fs/*` —
  each `builtin.os.tag` switch is a place a future change can
  silently orphan a platform.

## Status summary

| Area | Status | Notes |
|---|---|---|
| Public surface | Consistent | 9 core methods + 3 cancel variants + Windows 4-method extension. No signature drift across backends. |
| Cancel ordering | **Consistent** | Verified across all 10 `waitXCancel` variants (kqueue×2, epoll×2, io_uring×2, IOCP×4). Commit `0af6a23` closed the race. |
| fd-close-while-parked | **Gap** | No backend exposes `closeFd(fd)`. Net types call libc `close()` directly and orphan the kernel registration. Tests `SkipZigTest`'d. |
| Error mapping | Mostly consistent | Shared `ReactorWaitError` / `IoError` taxonomy. ENOTSOCK handled inconsistently across backends; numeric ECONNRESET differs Darwin vs Linux but handled via per-OS Errno table. |
| Timer `ns=0` | **Divergence** | POSIX backends (kqueue/epoll/io_uring) all guarded by `bc4685a`. IOCP `waitTimer` has no guard. |
| Timer overflow / negative | **Unchecked** | All four backends do `@intCast(ns)` with no bounds check. Negative input wraps; very-large input overflows silently. No regression test. |
| Net/fs platform branching | 62 sites | Branching is correct everywhere it occurs, but the surface is large; a single missing case orphans a platform. |

## Finding 1 — `waitTimer(0)` guard missing on three sites

**Severity:** medium (silent divergence; only fires with `sleep(0)`).
**Backends affected:** IOCP (both variants), epoll (cancel variant only).
**Fix:** Phase 3a (`P3a Fix IOCP waitTimer(0) divergence`) — broadened
in scope to cover all three missing sites.

Audit of `if (ns == 0) return;` across all `waitTimer` + `waitTimerCancel`:

| Backend | `waitTimer` | `waitTimerCancel` |
|---|---|---|
| `src/reactor_kqueue.zig` | line 125 ✓ | line 309 ✓ |
| `src/reactor_epoll.zig` | line 241 ✓ | **line 378 — MISSING** |
| `src/reactor_io_uring.zig` | line 170 ✓ | line 300 ✓ |
| `src/reactor_iocp.zig` | **line 642 — MISSING** | **line 676 — MISSING** |

The `bc4685a` commit landed the guard at `waitTimer` on all three
POSIX backends, but missed:

1. **epoll `waitTimerCancel`** (line 378). Same `timerfd_settime`
   semantics: all-zero `it_value` = disarm. A coroutine calling
   `sleep(0)` through a cancellable wrapper would hang forever
   on Linux.

2. **IOCP `waitTimer`** (line 642). With `ns=0`,
   `ticks_100ns = 0`, `SetThreadpoolTimer(timer, &due_time, 0, 0)`
   sets an expired timer that fires before the park returns —
   ambiguous behaviour, not a documented contract.

3. **IOCP `waitTimerCancel`** (line 676). Same issue.

Fix: add `if (ns == 0) return;` at all three sites, immediately
after the `try c.checkpoint();` line on the cancel variants (so
a pre-fired cancel still surfaces `error.Cancelled` rather than
silently no-op'ing).

Conformance test `sleep(0) returns immediately` exists but only
exercises `waitTimer`, not `waitTimerCancel`. Phase 3a adds a
cancel-variant counterpart.

## Finding 2 — Timer inputs are unchecked

**Severity:** medium (untested boundary).
**Backends affected:** all four.
**Fix:** Phase 3b (`P3b Add timeout bounds checking (all 4 backends)`).

Every backend casts user-supplied `ns: u64` into a smaller or
signed kernel type without bounds checking:

| Backend | Cast site | Failure mode |
|---|---|---|
| kqueue | `src/reactor_kqueue.zig:133` `kev.data = @intCast(ns)` | EVFILT_TIMER `data` is `intptr_t` (i64 on 64-bit); negative wraps to huge timeout. |
| epoll | `src/reactor_epoll.zig:256` `itimerspec.it_value.sec = @intCast(ns / ns_per_s)` | `time_t` is i64 on Linux; negative input still wraps before the divide. |
| io_uring | `src/reactor_io_uring.zig:174` `kernel_timespec.sec = @intCast(...)` | `__kernel_timespec.tv_sec` is i64; same wrap. |
| IOCP | `src/reactor_iocp.zig:655` `ticks_100ns: i64 = -@as(i64, @intCast(ns / 100))` | Cast before negation; `ns > i64::MAX * 100` overflows the intermediate. |

The contract Volt should publish: `waitTimer(ns: u64)` accepts
`0 ≤ ns ≤ u63_max`. Inputs above that bound return
`error.TimeoutOutOfRange` rather than wrap silently.

Regression test coverage to add (Phase 3b): `0`, `1`,
`1_000_000_000` (one second), `u63_max`, `u64_max` (must error).

## Finding 3 — `closeFd(fd)` missing on every backend

**Severity:** high (parked coroutine hangs indefinitely).
**Backends affected:** all four.
**Fix:** Phase 3d (`P3d Implement closeFd(fd) per backend + un-skip close-while-parked tests`).

When one coroutine closes a fd that another coroutine is parked
on (`accept` / `recvFrom` / `read`), the parked coroutine must
unblock with some categorical error. Today it hangs forever
because:

- **kqueue** silently drops the kevent registration when the fd
  closes; no event ever fires.
- **epoll** orphans the `EPOLL_CTL_DEL` if `close()` runs before
  it; no event ever fires.
- **io_uring** the pending `POLL_ADD` SQE never completes if the
  fd vanishes outside the ring's knowledge.
- **IOCP** the overlapped operation may complete with
  `ERROR_OPERATION_ABORTED`, but only if `CancelIoEx` is called
  first — `closesocket()` alone is not enough.

Sites that close registered fds today:

| File:line | Close path |
|---|---|
| `src/net.zig:199` | `TcpListener.close` |
| `src/net.zig:319` | `TcpStream.close` |
| `src/net/udp.zig:~150` | `UdpSocket.close` |
| `src/net/unix.zig` | `UnixListener.close`, `UnixStream.close` |
| `src/fs/file.zig` | `File.close` (if registered for async I/O — applies on IOCP, future io_uring fs ops) |

Documented today by two `SkipZigTest` cases in
`src/reactor_conformance_test.zig`:
- `conformance: TcpListener.close while accept() is parked — no hang`
- `conformance: UdpSocket.close while recvFrom() is parked — no hang`

Phase 3d's fix: each backend gains a `closeFd(fd: i32) void`
(or `closeHandle(h: HANDLE)` on Windows) that:
1. Looks up any parked waiter for `fd` via a new fd→coro
   side-table.
2. Deregisters the kernel side (`EV_DELETE` / `EPOLL_CTL_DEL` /
   io_uring `CANCEL` / `CancelIoEx`).
3. Dispatches the waiter to be unparked with
   `error.BadDescriptor` (or `error.Closed` — choice deferred to
   Finding 5 below).
4. Calls libc `close()` (or `closesocket` on Windows) last.

The side-table sizing: a fixed-size open-addressing hash table
sized to the runtime's `max_pending_io` config (defaults to a
few thousand) is enough — fd reuse means stale entries get
overwritten naturally on next registration.

## Finding 4 — 62 `builtin.os.tag` branch sites in `src/net/*` and `src/fs/*`

**Severity:** low individually, high in aggregate (surface area).
**Fix:** ongoing audit; no single Phase 3 task. Periodic review.

The audit counted 62 occurrences of `builtin.os.tag` switching
in `src/net/*` and `src/fs/*` combined. Per-file:

| File | Count | Notes |
|---|---|---|
| `src/net/options.zig` | 19 | setsockopt/getsockopt dispatch, per-OS socket constants. Necessary divergence. |
| `src/net/udp.zig` | 18 | AF_INET6, multicast constants (IP_MULTICAST_IF differs Linux vs BSD vs Windows). |
| `src/net/resolver.zig` | 1 | Linux `AI_NUMERICSERV` flag. |
| `src/fs/*` | 24 (combined) | statx vs stat, FSEvents vs inotify vs ReadDirectoryChangesW. |

Risk: 62 sites = 62 places a typo or missing case silently
orphans a platform. Mitigation: each branch must have all four
cases (`.linux`, `.macos`, `.windows`, `else`) and the `else`
must `@compileError` rather than fall through.

Phase 1 deliverable does not refactor these; the Phase 3 ports
from reference libraries (Phase 2 / Phase 3e) will surface any
that are buggy via test failures.

## Finding 5 — Error mapping inconsistencies

**Severity:** low (only matters for callers that want to branch
on error kind).
**Fix:** Phase 3d (decide one consistent close-error variant) +
follow-up.

Shared error definitions in `src/reactor.zig`:
- `ReactorSetupError`: `OutOfDescriptors`, `SystemResources`, `Unexpected`
- `ReactorWaitError`: `BadDescriptor` + `ReactorSetupError`
- `IoError`: `ConnectionReset`, `BrokenPipe`, `ConnectionAborted`,
  `NotConnected`, `ConnectionRefused`, `Timeout` + `ReactorWaitError`

Per-backend errno→error mapping comparison:

| Errno | kqueue | epoll | io_uring | IOCP |
|---|---|---|---|---|
| EBADF / WSAEBADF | `BadDescriptor` | `BadDescriptor` | (kernel CQE) | `BadDescriptor` |
| EMFILE / WSAEMFILE | `OutOfDescriptors` | `OutOfDescriptors` | `SystemResources` | `OutOfDescriptors` |
| ENOMEM / WSAENOBUFS | `SystemResources` | `SystemResources` | `SystemResources` | `SystemResources` |
| ENOTSOCK | `BadDescriptor` | `BadDescriptor` | (not surfaced) | (not surfaced — uses WSAEBADF) |

Two known inconsistencies:

1. **ENOTSOCK**: kqueue/epoll map it to `BadDescriptor`;
   io_uring and IOCP don't surface it as a distinct path. Low
   impact (it implies a programmer error — using a non-socket
   where a socket is expected), but inconsistent.

2. **Close path error**: when Phase 3d's `closeFd` dispatches a
   parked waiter, it must pick ONE error variant. Options:
   `error.BadDescriptor` (matches the eventual EBADF the
   coroutine would have seen) or `error.Closed` (a new variant;
   more semantically precise). Decision deferred to Phase 3d;
   recommendation: `error.BadDescriptor` to avoid adding a new
   error to `ReactorWaitError` that every existing caller would
   need to handle.

## What the audit deliberately does NOT cover

- **Performance** — runtime/scheduler perf and the bench-runner
  harness are tracked separately.
- **Future backends** — Per-P reactors (issue #7), io_uring for
  fs ops (issue #3), Watcher native backends (issue #4) are
  out of scope. This audit is about the four current backends.
- **API ergonomics** — the public `volt.net` / `volt.fs` surface
  is a separate concern.

## Re-running this audit

When a future change touches close/cancel paths, a backend's
public surface, or adds a new platform branch in `src/net/*` or
`src/fs/*`, re-run the audit:

1. Read all four `reactor_*.zig` files end-to-end.
2. Check each `waitXCancel` for kernel-register-before-cancel-list
   ordering.
3. Walk every `close()` site in `src/net/*` and `src/fs/*` and
   confirm it routes through the reactor's `closeFd` (post Phase
   3d).
4. Count `builtin.os.tag` branches in `src/net/*` and `src/fs/*`;
   if it's grown since the last audit, the new sites need their
   own platform-coverage check.
5. Update this document with the new date and any new findings.

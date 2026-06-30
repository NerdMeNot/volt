---
title: Reactor reference map (2026-05-23)
description: Cross-reference of high-value regression tests from libuv, mio, Go runtime netpoll, and Boost.Asio against Volt's current and planned reactor coverage. Drives test-port decisions for Phase 3e.
---

This page maps regression tests from four mature platform-IO
projects to Volt's reactor coverage. Each row is a specific
invariant — file lifecycle, race, timer corner case, etc. —
that the upstream project codified after hitting a real bug.

The goal is to learn from their bug history rather than rediscover
the same bugs ourselves.

Reference projects cloned under `.refs/` (gitignored, sparse where
the full repo is huge):

- `libuv` — node.js's cross-platform IO. Mature test suite under
  `test/test-*.c`, 1299-line test-list manifest.
- `mio` — Tokio's substrate. Tests under `tests/*.rs`, with an
  explicit `regressions.rs` file for named bug fixes.
- Go runtime — `src/runtime/netpoll*.go` (kqueue / epoll / IOCP),
  closest-shape analogue to Volt since both pair a stackful
  coroutine scheduler with a cross-platform reactor.
- Boost.Asio — `include/boost/asio/detail/impl/*` for the IOCP and
  POSIX reactor implementations.

## Status legend

- **port-3d** — port as un-skip of the close-while-parked work in
  Phase 3d (`closeFd(fd)` per backend).
- **port-3e** — port as a new conformance test after Phase 3d
  lands.
- **covered** — Volt's existing test or conformance suite already
  pins the invariant. Audit-confirmed by checking the named test
  exists, not just by inference.
- **N/A** — Volt's design doesn't have the failure mode. Reason
  given in the notes column.

## libuv

| Test | Invariant pinned | Volt coverage | Action |
|---|---|---|---|
| `test-tcp-close-accept.c` | Close listener while `accept()` pending must not orphan parked acceptor; stale kevent on closed fd must not deliver to the new fd if the slot is reused | gap (`SkipZigTest`) | **port-3d** |
| `test-udp-recv-cb-close-pollerr.c` | Close UDP socket while `recvFrom()` pending unblocks the receiver with a categorical error, not a hang | gap (`SkipZigTest`) | **port-3d** |
| `test-close-fd.c` | External `close()` on a fd registered with the reactor; subsequent reads see EOF, not a hang | gap | **port-3e** |
| `test-poll-close-doesnt-corrupt-stack.c` | Windows-specific: closing a socket while a poll is pending must not corrupt the stack from the IOCP callback | N/A — Volt uses Zig-safe coroutine dispatch, not raw IOCP callbacks into user stacks | N/A |
| `test-tcp-close-while-connecting.c` | `close()` during in-flight `connect()` must cancel rather than hang; timer cleanup must run | covered — `net.test.reactor: waitReadableCancel wakes a parked accept on Cancel.fire` is analogous; verify post-3d that connect cancel works the same way | port-3e (verify) |
| `test-tcp-close-after-read-timeout.c` | Read-timeout fires, then close runs; read callback fires before close callback | N/A — Volt's coroutine model returns from the read with `error.Cancelled`; ordering is implicit via stack unwind | N/A |
| `test-tcp-close-reset.c` (5 variants) | Forced RST via `shutdown(RST)` semantics; not the same as graceful close | N/A — Volt's `close()` is graceful only; explicit RST is a future API decision | N/A |
| `test-iouring-pollhup.c` | io_uring receives POLLHUP for a hung-up peer; the poll completion must mark the fd as readable so the next read returns EOF | covered — `conformance: TCP half-close — reader observes clean EOF` (post-bc4685a) pins this on every backend | covered |

## mio

| Test | Invariant pinned | Volt coverage | Action |
|---|---|---|---|
| `regressions.rs::issue_776` | Deregister a handle during `poll()` — the deregister must not race with the in-flight event dispatch | needs audit — Volt's `cancelCoro` was the related fix in `0af6a23`; need to verify deregister-during-poll separately | **port-3e** |
| `regressions.rs::issue_1205` | Waker registered on a deregistered source must not produce a stale event | N/A — Volt's parker uses per-coroutine wake state, not a shared waker registered on the reactor | N/A |
| `close_on_drop.rs` | Dropping a registered fd cleans up its reactor registration; no panic on next poll | gap — Volt's net types call `close()` from a destructor pattern (`defer s.close()`), but the underlying gap is the same as `test-tcp-close-accept` | **port-3d** (covered by closeFd work) |
| `poll.rs::add_then_drop` | Register fd, drop immediately; poll doesn't crash | partially covered via existing `defer close()` patterns in net tests; explicit test missing | **port-3e** |
| `poll.rs::register_during_poll` (cross-thread) | Register a fd from one thread while another thread is blocked in poll; the new fd's events get delivered | covered — Volt's serialised submission lock (kqueue) / kernel-side registration (epoll/io_uring/IOCP) handles this | covered |
| `poll.rs::poll_ok_after_cancelling_pending_ops` | Re-register a fd whose previous IOCP/epoll op was cancelled; the new interest takes effect | needs audit on IOCP — the PR #14 lazy-IOCP-association fix is in the same area | **port-3e** post-3c |

## Go runtime netpoll

| Site | Pattern pinned | Volt coverage | Action |
|---|---|---|---|
| `netpoll.go` `pollDesc.fdseq` + tagged pointer in `pollDesc.userData` | Stale event filtering — when an fd is closed and the file descriptor reused, the bumped seq number lets the netpoller reject the stale completion rather than delivering it to the new fd's owner | gap on epoll / IOCP — Volt's kqueue path uses `udata` with a coroutine pointer; closure-while-parked could deliver to a new owner if fd is reused before deregister | **port-3e** (consider seq-tag extension) |
| `netpoll_kqueue.go` `netpollopen` / `netpollclose` | Notes kqueue auto-deregisters on `close()`; no explicit `EV_DELETE` needed in the normal path | informational — Volt's kqueue backend relies on the same auto-deregister, but the close-while-parked gap is about the parked coroutine not knowing the fd is gone | informs port-3d design |
| `netpoll_epoll.go` `netpollopen` (edge-triggered + auto-cleanup on close) | Edge-triggered epoll; the kernel auto-cleans `EPOLL_CTL_DEL` on the last fd close, but the user-space parked-waiter table needs explicit cleanup | informs port-3d design — Volt's epoll closeFd needs to do `EPOLL_CTL_DEL` BEFORE close() because the kernel auto-cleanup races with new fd allocations on the same number | informs port-3d design |
| `netpoll_windows.go` `pollDesc` packed key into IOCP `lpCompletionKey` | Tagged completion key so a stale IOCP completion (from a closed handle that's been reused) is rejected rather than delivered to the new owner | gap on Volt IOCP — Volt's IOCP backend uses raw coroutine pointer as completion key; same stale-completion risk | **port-3e** (Volt IOCP needs seq-tag too if Windows handle reuse becomes a real failure mode) |

## Boost.Asio

| Site | Pattern pinned | Volt coverage | Action |
|---|---|---|---|
| `epoll_reactor.ipp::deregister_descriptor` | Comment notes "the kernel auto-removes the registration on fd close" but the implementation still calls `EPOLL_CTL_DEL` explicitly to abort pending ops — confirming the race we identified | informs port-3d design | informs port-3d design |
| `epoll_reactor.ipp::cancel_ops` | Pending op queue drained on deregister; double-completion prevented by op state machine | covered — Volt's `cancelCoro` + deferred dispatch queue handles this | covered |
| `win_iocp_socket_service_base.ipp::close` | Calls `CancelIoEx(handle, NULL)` BEFORE `closesocket()` to abort pending IOCP ops; otherwise the completion can fire on a freed handle | gap on Volt IOCP — Volt's UdpSocket.close calls closesocket directly; PR #14 adds lazy association but not cancellation | **port-3d** (Volt IOCP closeFd needs CancelIoEx before closesocket) |
| `win_iocp_io_context.ipp` timer integration | Threadpool timer associated with the IOCP via the same `lpCompletionKey` tag scheme; bounds-checked timer values | informs Phase 3b (timeout bounds) | informs port-3b design |

## Summary by Phase 3 task

**Phase 3a (waitTimer(0) gaps)** — no reference test ports. Volt's
own `conformance: sleep(0) returns immediately` already pins the
invariant; Phase 3a just extends it to the cancel variants and the
two IOCP sites identified in `reactor-audit.md`.

**Phase 3b (timeout bounds)** — informed by Boost.Asio's bounds
checking pattern; no direct port. New regression tests for `0`,
`1`, `1_000_000_000`, `u63_max`, `u64_max` per `reactor-audit.md`.

**Phase 3c (Windows UDP ABI)** — informed by mio's
`poll_ok_after_cancelling_pending_ops` for the post-fix verification.

**Phase 3d (closeFd per backend) — high-priority ports:**
1. libuv `test-tcp-close-accept` → un-skip Volt's `TcpListener.close
   while accept() is parked`
2. libuv `test-udp-recv-cb-close-pollerr` → un-skip Volt's
   `UdpSocket.close while recvFrom() is parked`
3. mio `close_on_drop` → covered by the un-skip work since Volt's
   net types' `defer close()` is the same pattern
4. Boost.Asio's `CancelIoEx` before `closesocket` → design
   constraint for Volt's IOCP `closeFd`

**Phase 3e (post-3d regression ports):**
1. libuv `test-close-fd` → external close on registered fd
2. libuv `test-tcp-close-while-connecting` → close during in-flight
   connect (verify Phase 3d covers it)
3. mio `regressions.rs::issue_776` → deregister-during-poll
4. mio `poll.rs::add_then_drop` → register-then-drop without an
   intervening park
5. mio `poll.rs::poll_ok_after_cancelling_pending_ops` (post-3c)
6. Go runtime / Asio stale-event filtering — open question whether
   Volt needs seq-tag (kqueue and epoll on Linux/Darwin reuse fd
   numbers quickly; IOCP handle reuse is more conservative)

## Re-running this map

The references at `.refs/` are pinned to whatever HEAD was at
clone time. Re-clone or pull periodically to catch newly-added
regression tests, especially in mio's `regressions.rs` (its
file grows by ~1-2 tests per year per past commits).

When updating: re-walk each project's test directory for newly
added close/cancel/lifecycle tests. The bar for inclusion is
"would a Volt user file a bug if this invariant broke?"

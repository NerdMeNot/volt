---
title: Backend parity & production tiers
description: Which reactor backends Volt supports and what tier each is at.
---

Volt is built around a single **readiness-based reactor protocol**. Every
backend exposes the same surface — `init`, `deinit`, `pendingCount`,
`tickle`, `registerWait`, `unregisterWait`, `registerTimer`,
`unregisterTimer`, `poll` — and the comptime conformance check in
`src/io/reactor.zig` rejects builds where a backend has drifted from
that signature.

The point of this page is to set honest expectations about *which*
backend is at *which* tier of validation, so you know what you're
adopting when you depend on Volt.

## Tier definitions

| Tier | What it means |
|---|---|
| **Production** | Native CI runs the full test suite on every PR. Cancellation, leak-tracking, and reactor-conformance suites all green. Used by the maintainer's own consumers (NerdMeNot libs). |
| **Production-candidate** | Cross-compile validated; structurally complete; all conformance tests pass on the platforms where they can be run. Native CI either gated on missing pieces or recently-added (<4 weeks of green). Tier-bump to *Production* requires ≥4 weeks of CI greenness. |
| **Beta** | Code is in the tree and compiles, but lacks runtime validation. Typically a structural skeleton missing one or more dependent layers (e.g. AFD reactor with no syscall layer below it). |

## Current matrix (v1.1)

| Platform / Backend | Tier | Default? | Notes |
|---|---|---|---|
| **Darwin / kqueue** (`reactor_kqueue.zig`) | Production | ✅ | Volt's primary dev platform. All v1.0 cancel-leak audits + recent reactor-conformance suite green here. |
| **Linux / epoll** (`reactor_epoll.zig`) | Production | ✅ | Default Linux backend. Cross-compile + native CI green. |
| **Linux / io_uring** (`reactor_iouring.zig`) | Production-candidate | Opt-in (`-Dreactor=iouring`) | Phase 1 of the v1.1 plan: tracked-registration model with `IORING_OP_ASYNC_CANCEL` + generation-counter UAF prevention (ported from tokio-uring). Currently `allow_failure: true` in CI pending the unrelated `linux.fstat` fix that blocks `zig build test -Dtarget=linux-gnu` for *any* backend. |
| **Windows / IOCP+AFD** (`reactor_iocp.zig`) | Beta | — (dispatcher gated) | Phase 2a-2b of v1.1: AFD-based readiness reactor (mio/wepoll approach via `IOCTL_AFD_POLL`) + SEH stack-overflow handler. Cross-compiles cleanly. Tier-bump requires Phase 2c-2g (syscall layer arms, fs/Dir, fs/Metadata, process/Command, signal, file watcher, CI runner). |

## What can a consumer adopt today?

If you're shipping a Zig library on Volt:

- **Darwin + Linux x86_64 + Linux arm64**: production-ready substrate. Adopt freely.
- **Linux io_uring**: works structurally; opt-in via `-Dreactor=iouring`. Treat as evaluation-grade until the upstream `fstat` blocker clears and CI runs ≥4 weeks green.
- **Windows**: not yet runtime-default. The reactor + stack handler are ready; the I/O syscall layer + fs surface still need Windows arms. Track Phase 2c-2g in the v1.1 plan.

## Architectural choices, in case you're wondering

- **Why AFD on Windows, not IOCP completion-style?** Volt's protocol everywhere else is readiness-based: a coroutine calls a non-blocking syscall, parks on `WouldBlock`, the reactor wakes it when ready. Plain IOCP is a *completion* model — the kernel does the read/write itself. Bridging the two means either two distinct I/O paths or a per-call buffer copy on Windows. Solution (used by mio, wepoll, Tokio): `IOCTL_AFD_POLL` against an AFD device handle gives readiness semantics on top of IOCP. See `src/io/reactor_iocp.zig` for the implementation; `src/internal/win32/ntdll.zig` for the bindings.
- **Why tracked registrations on io_uring?** io_uring's cancel model differs from kqueue/epoll: cancelling a poll/timer submits an `IORING_OP_ASYNC_CANCEL` SQE; the original CQE still arrives, just with `-ECANCELED`. The naive scheme of packing `*Park` into `user_data` is unsafe — the cancelled Park's stack frame is gone by the time the CQE lands. We use a slab-backed registration table; `user_data` packs `(slot, generation, kind)`. Cancel bumps the generation; CQEs with stale generations drop silently. Ported from tokio-uring's `Op` registry.
- **Why a comptime conformance check?** Rust's mio uses trait bounds; Boost.Asio uses templates; libuv uses a vtable struct. Each of these forces backend uniformity at compile time. Volt's `comptime` block in `reactor.zig` does the same — a backend that drops or renames a method fails to build with a localized error referencing the conformance assertion, not at the consumer site.

## Reference implementations

The architecture deliberately mirrors well-known async runtimes for the parts where their solutions are battle-tested:

| Volt piece | Reference | License |
|---|---|---|
| Cross-platform readiness reactor shape | mio (Rust) | MIT |
| Windows readiness via AFD.sys | mio's `sys/windows` + wepoll | MIT |
| io_uring tracked cancellation | tokio-uring (Rust) | MIT |
| Stackful scheduler ergonomics | may (Rust) | MIT/Apache-2.0 |
| SEH stack-overflow handler (Windows) | libuv `src/win/error.c` | MIT |

We port architecture, not code; Zig rewrites only.

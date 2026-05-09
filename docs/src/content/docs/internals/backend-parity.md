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

## Current matrix (v1.0)

| Platform / Backend | Tier | Default? | Notes |
|---|---|---|---|
| **Darwin arm64 / kqueue** (`reactor_kqueue.zig`) | Production | ✅ | Volt's primary dev platform. Native CI green + N=200 nightly stress. |
| **Darwin x86_64 (Intel) / kqueue** | Cross-compile only | — | Apple Silicon is the v1 macOS target. Native runtime returns in v1.x. |
| **Linux x86_64 + arm64 / epoll** (`reactor_epoll.zig`) | Production | ✅ | Default Linux backend. Native CI green + N=200 nightly stress. |
| **Linux x86_64 + arm64 / io_uring** (`reactor_iouring.zig`) | Production | Opt-in (`-Dreactor=iouring`) | Tracked-registration model with `IORING_OP_ASYNC_CANCEL` + generation-counter UAF prevention (ported from tokio-uring). Native CI green + N=200 nightly stress. Pending tier-bump to default after ≥4 weeks of consecutive nightly green. |
| **Windows x86_64 + arm64 / IOCP+AFD** (`reactor_iocp.zig`) | Cross-compile only | — | Reactor (AFD-based readiness via `IOCTL_AFD_POLL`) + SEH stack-overflow handler are landed and cross-compile-clean. Native runtime is blocked on three Zig 0.16 stdlib bugs (see *Windows runtime status* below). Targets v1.x once upstream Zig fixes land. |

## What can a consumer adopt today?

If you're shipping a Zig library on Volt:

- **Darwin arm64 + Linux x86_64 + Linux arm64**: production-ready substrate. Adopt freely.
- **Linux io_uring**: opt-in via `-Dreactor=iouring`. Native CI green + N=200 nightly stress; safe to ship on. Default-flip lands once it's been green for ≥4 weeks of consecutive nightlies.
- **Darwin x86_64 (Intel)**: cross-compile-validated only at v1; runtime returns in v1.x. Apple Silicon is the v1 macOS target.
- **Windows**: not runtime-supported at v1. The reactor + stack handler are ready and cross-compile cleanly; runtime is blocked on Zig 0.16 stdlib bugs in code Volt's tests exercise.

## Windows runtime status

Three Zig 0.16 stdlib bugs surface when running `zig build test -Dtarget=x86_64-windows-gnu`:

1. **`std.Io.Writer.zig:1803`** — invalid format string `'d'` for type `*anyopaque`. Hits any `std.fmt`-using path that prints opaque pointers; Volt's diagnostic logging triggers it.
2. **`std.c.zig:4767`** — `os.windows.ws2_32` has no member `addrinfo`. Volt's DNS path references it.
3. **`std.c.zig:10659`** — `mmap` parameter of type `void` not allowed under `x86_64_win` calling convention. Volt's `fs/Mmap.zig` Windows arm hits it.

These are upstream blockers — fixing them requires patching Zig stdlib. Volt v1 ships with Windows as cross-compile-only; native runtime tier-bumps in v1.x once one of:

- Upstream Zig releases a 0.16.x fix.
- Volt replaces the affected stdlib calls with internal bindings (substantial scope; tracked in W2-W7 of the original Windows port plan).

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

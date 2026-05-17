---
title: Roadmap
description: What Volt doesn't have yet, and what intentionally lives outside the core runtime.
---

Volt is **not 1.0** yet. The runtime works for what it claims (see
the bench gate + the 45-second stress test), but several pieces are
still in flight.

## In-flight

| Platform | Status |
|---|---|
| Darwin arm64 (kqueue) | **Working** — primary dev platform; full bench suite + 45 s stress green |
| Linux arm64 (epoll) | **Working** — cross-compile + epoll-specific tests green; runtime CI pass pending |
| Linux arm64 (io_uring, poll mode) | **Working** — `Runtime.Config.io_backend = .io_uring`, kernel ≥ 5.10; `IORING_OP_POLL_ADD` shape |
| Windows arm64 (IOCP, readiness polyfill) | **Cross-compiles cleanly** — implemented via zero-byte `WSARecv`/`WSASend`; runtime validation deferred to a Windows VM/CI pass; needs `WaitOnAddress` parker backend |
| Linux x86_64 / Windows x86_64 | **Cross-compile only** — runtime needs an x86_64 context switch (#149); ARM64 ctx switch is the only one shipping today |

## Out of scope (Volt core)

The runtime stays small on purpose. The following live in downstream
libraries, not Volt itself:

| Capability | Lives in |
|---|---|
| File I/O | [NerdMeNot](https://github.com/NerdMeNot) `volt-fs` (planned) |
| DNS resolution | `volt-net` extensions (planned) |
| TLS | `volt-tls` (planned, C-interop opt-in) |
| HTTP client / server | `volt-http` (planned) |
| S3, gRPC, PostgreSQL drivers | Application-specific libraries |
| UDP / IPv6 / Unix sockets | `volt-net` extensions (planned) |
| Subprocess management | Application-specific (use `std.process` directly) |
| Signal handling (user-facing) | Application-specific (Volt only installs `SIGSEGV` internally for stack-growth) |
| Metrics / tracing surfaces | Application-specific |

The boundary is documented in [Volt design boundaries](https://github.com/NerdMeNot/volt/blob/main/CLAUDE.md): pure
Zig by default; `asm` / raw syscalls justified case-by-case; C
interop only for opt-in extensions like `volt-tls`.

## Why this scope

A stackful coroutine runtime that tries to also be a kitchen-sink
async library has a maintenance fan-out that doesn't end. Volt's
mission is to be the substrate — fast spawn, fast wait/wake,
correct under multi-worker — and let libraries on top compose
their domain-specific concerns. The same architectural call Tokio
made (`tokio::net`, `tokio::fs` separate from runtime core) and
Go didn't (everything in `runtime`).

The cost of this choice: if you want a complete HTTP server today,
you can't grab it from Volt alone. You wait for `volt-http`, or
you compose `volt.net.TcpListener` + your own HTTP parser. The
upside: Volt itself stays explainable in ~5 KLoC and the bench
gate stays tight.

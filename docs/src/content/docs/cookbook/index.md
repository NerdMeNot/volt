---
title: Cookbook
description: Concrete recipes for common patterns — each one is a complete, runnable Volt program built from primitives that actually ship.
---

The recipes in this section are **complete programs**. Copy-paste,
adapt, run. Each one solves a specific problem using primitives
from the current Volt surface (`volt.Runtime`, `volt.spawn`,
`volt.Cancel`, `volt.scope`, the channel family, `volt.net.*`,
`volt.sync.*`).

If a recipe references an API you haven't seen, jump to the
corresponding [API Reference page](/usage/runtime/) — they explain
the primitives in detail.

## Network

- [TCP Echo Server](/cookbook/echo-server/) — one coroutine per
  connection.
- [Connection Pool](/cookbook/connection-pool/) — `Semaphore` +
  free list for reusable connections.

## Coordination

- [Fan Out, Take First Answer](/cookbook/fan-out-first-wins/) —
  race N tasks, take the first result, cancel the losers via
  `volt.scope` + `Oneshot`.
- [Graceful Drain on Shutdown](/cookbook/graceful-drain/) — let
  active requests finish before exit.
- [Rate Limiter](/cookbook/rate-limiter/) — bound concurrency
  with `Semaphore`; build rate-per-second with `Semaphore` +
  refill.
- [Timeout with Retry](/cookbook/timeout-retry/) — `volt.scope`
  + watchdog as the timeout primitive.

## Channels

- [Pub/Sub Fan-Out](/cookbook/pub-sub/) — `Broadcast` for events
  every subscriber should see.
- [Config Hot-Reload](/cookbook/config-hot-reload/) — `Watch`
  for values that change at runtime.

## Bridges to non-coroutine code

- [Offloading CPU Work](/cookbook/work-offload/) — `std.Thread`
  + `volt.Mpmc` for synchronous CPU-heavy code that shouldn't
  block a worker.

## How to read these

Each recipe starts with the full code, then explains the *why* —
which primitive does what, and what the structural guarantees are.
The aim is to leave you with enough understanding to adapt the
pattern, not just retype it.

## What's NOT in here yet

Some patterns aren't in Volt core today:

- **HTTP server / client** — Volt is a runtime, not a framework.
  Build on `volt.net.TcpListener`; a `volt-http` library is
  planned. See [Roadmap](/appendix/roadmap/).
- **Database client** — same shape as connection pool plus
  protocol-specific framing. The connection pool recipe is the
  foundation; the framing is up to you (or a future
  `volt-pg`/`volt-mysql` library).
- **Signal handling (SIGINT / SIGTERM for graceful shutdown)** —
  Volt's signal handler is internal (used for SIGSEGV stack-growth).
  User-facing signal handling lives outside Volt core. The
  graceful-drain recipe uses `volt.Cancel` directly; pair it with
  `std.posix.sigaction` or your platform's signal API to wire to
  Ctrl-C.

If you find yourself wanting a pattern that isn't here, the
[Choosing a Primitive](/guides/choosing-primitive/) guide is
designed to help you reason about which existing primitive to
reach for.

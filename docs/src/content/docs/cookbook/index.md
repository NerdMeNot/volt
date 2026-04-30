---
title: Cookbook
description: Concrete recipes for common patterns — each one is a complete, runnable Volt program.
---

The recipes in this section are **complete programs**. Copy-paste,
adapt, run. Each one solves a specific problem and explains why the
pattern works.

The four pages below have a runnable counterpart in
[`examples/`](https://github.com/NerdMeNot/volt/tree/main/examples)
that you can `zig build run-*` immediately:

- [TCP Echo Server](/cookbook/echo-server/) — `zig build run-echo`
- [Fan Out, Take First Answer](/cookbook/fan-out-first-wins/) — `zig build run-fan-out`
- [Offloading CPU Work](/cookbook/work-offload/) — `zig build run-work-offload`
- [Timeout with Retry](/cookbook/timeout-retry/) — `zig build run-timeout-retry`

The rest are written-out patterns you'd build on top:

- [Graceful Drain on Shutdown](/cookbook/graceful-drain/) — let
  active requests finish before exit.
- [Rate Limiter](/cookbook/rate-limiter/) — bound concurrency or
  rate-per-second.
- [Pub/Sub Fan-Out](/cookbook/pub-sub/) — Broadcast for events
  every subscriber must see.
- [Config Hot-Reload](/cookbook/config-hot-reload/) — Watch for
  values that change at runtime.
- [Connection Pool](/cookbook/connection-pool/) — Semaphore + free
  list for reusable resources.

## How to read these

Each recipe starts with the full code, then explains the
*why* — which primitive does what, and what the structural
guarantees are. The aim is to leave you with enough understanding
to adapt the pattern, not just retype it.

If a recipe references an API you haven't seen, jump to the
corresponding [Usage page](/usage/runtime/) — they explain the
primitives in detail.

## What's NOT in here yet

Some patterns from the pre-stackful tree don't have stackful-shape
recipes yet:

- HTTP server — Volt is a runtime, not a framework. Build on
  `volt.io.TcpListener`; a separate `volt-http` library is
  planned.
- Database client — same shape as connection pool plus
  protocol-specific framing. The connection pool recipe is the
  foundation; the framing is up to you (or a `volt-pg` /
  `volt-mysql` library).
- Streaming file pipeline — needs better filesystem primitives
  than v1.0 ships. Use `volt.spawnBlocking` with `std.fs` for now.

If you find yourself wanting a pattern that isn't here, the
[Choosing a Primitive](/guides/choosing-a-primitive/) guide is
designed to help you reason about which existing primitive to
reach for.

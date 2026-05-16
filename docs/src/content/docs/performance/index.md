---
title: Performance
description: How Volt measures itself, the receipts behind every claim, and the postmortems for designs that didn't survive contact with the bench gate.
---

Volt's performance posture is **honesty over advocacy**. Go is the
scale reference — not the target. The numbers exist to tell us
whether the runtime is in a sensible range, not to claim victory.

The pages in this chapter answer three questions:

- **How do we measure?** [Benchmark methodology + receipts](/performance/benchmarks/) — what each bench does, what numbers it produces, what the Go counterpart looks like.
- **What did profiling teach us?** [Multi-worker scheduler profile](/performance/multi-worker-profile/) — `samply`-driven tunings, the three wins that closed the 11× gap to 2.7×.
- **What didn't survive contact with reality?** Postmortems:
  - [Phase 4 (mprotect per spawn)](/performance/phase-4-postmortem/) — why grow-on-demand stacks couldn't ship with per-spawn `mprotect`.
  - [Slab arena (`POOL_CAP=64` cliff)](/performance/slab-arena-postmortem/) — how a benchmark-tuned cap blew up under realistic workloads, and the structural fix that replaced it.

## Bench gate

Every architectural change that touches the scheduler, coroutine
struct, allocation path, or sync primitives runs the full suite
green before landing. The protocol — and the past landings that
violated it — is in [Contributing](/appendix/contributing/).

```sh
zig build bench-yield
zig build bench-spsc
zig build bench-mpmc
zig build bench-mutex
zig build bench-spawn-hot
zig build bench-fanout-scaling
zig build bench-scaling
zig build bench-parallel-compute
zig build bench-tcp-echo
zig build bench-rss
zig build stress    # 3 runs, 45s each
```

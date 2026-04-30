---
title: Performance Tuning
description: Where Volt's overhead lives, what's worth measuring, and the knobs you can turn.
---

This guide is for the case where Volt is in your hot path and
you've decided you need to make it faster. Most programs won't
need any of it.

## Measure first

Volt ships `zig build bench`, which runs core benchmarks in
`ReleaseFast`:

```sh
zig build bench
```

Output is a small table — spawn cost, yield cost, channel SPSC
throughput, mutex lock/unlock cost on the host. Treat the numbers
as your **baseline**: if your application's costs are within 2-3×
of the bench numbers, the runtime isn't the bottleneck — your
work is.

If you're well above bench numbers, profile. Volt's source is
small enough that walking through the dispatch path with a
profiler usually surfaces the cause.

## The knobs

### Worker count

```zig
try volt.run(.{ .allocator = a, .workers = 4 }, root, .{});
```

Default is `getCpuCount()`. Tune up or down based on:

- **CPU-bound workloads**: workers = physical cores. Hyperthreads
  rarely help with stackful coroutines because the dispatch
  overhead doesn't parallelize well across SMT.
- **I/O-bound workloads**: workers = somewhere between 1 and
  CPU count. More workers = more reactor-claim contention; fewer
  workers = fewer steal targets.
- **Latency-sensitive**: workers = CPU count, plus `pin_workers
  = true` to keep each worker on its own core.

For most workloads, the default is fine. Tuning matters mainly when
you've measured a clear bottleneck.

### Worker pinning

```zig
try volt.run(.{ .allocator = a, .pin_workers = true }, root, .{});
```

Linux only. Pins worker `i` to CPU `i % cpu_count` via
`pthread_setaffinity_np`. Reduces cross-core cache traffic for
hot data; increases tail latency if a high-priority task arrives
on a busy worker (no migration possible).

Pin if your workload has clearly hot per-coroutine data and
near-zero load imbalance. Don't pin if you have bursty traffic
where load imbalance is the bigger problem.

### Stack size

Volt commits 1 page (4-16 KiB) up front per coroutine and grows in
place to 8 MiB on guard-page faults. The cap is a compile-time
constant (`default_reserved` in `src/coroutine/stack.zig`). If
you're handling truly recursive workloads (regex backtracking,
recursive-descent parsers) and hitting `error.StackOverflow`,
raise it.

For most workloads, the 1-page initial commit is what you want —
zero physical memory until the coroutine actually uses stack.

## What's actually expensive

In rough order, biggest first:

1. **`mmap` / `mprotect` syscalls on coroutine spawn.** Per spawn
   we reserve 8 MiB virtual address space + commit 1 page +
   protect a guard page. That's 2-3 syscalls per spawn. The slab
   pool eliminates this for steady-state workloads (recycles
   stacks across spawns), but the first N spawns of the runtime
   pay the syscall cost.

   *Mitigation*: pre-warm the pool by spawning + finishing N
   coroutines before your hot path. Subsequent spawns are
   essentially free.

2. **Context switches.** ~10-15 ns on Apple Silicon for the
   assembly switch. Plus park/unpark bookkeeping (a few atomic
   ops). The dominant cost in a "ping-pong between two
   coroutines" benchmark.

   *Mitigation*: batch work. If you have an iteration that does
   1 ns of compute and then yields, you're paying 10ns+ of
   overhead per item. Restructure to do hundreds of items per
   suspend.

3. **Mutex contention under high core counts.** Volt's Mutex is
   FIFO-fair; on a 64-core machine with all 64 contending, the
   waiter-list overhead dominates. Std's `std.Thread.Mutex` is
   fair-ish via parking_lot internals; for small uncontended
   sections it can be faster.

   *Mitigation*: shard. Per-core counters with a periodic
   aggregator. Per-key locks instead of one global lock. The
   classic "shard your hash table" pattern.

4. **Channel ring contention.** `Channel(T)` is a Vyukov MPMC
   ring; its fast path is a single CAS, but contention on the
   tail counter under heavy multi-producer load shows up.

   *Mitigation*: shard channels. N producers each writing to
   their own channel that one consumer multiplexes is faster
   than N producers contending on one channel.

5. **Reactor poll dispatch.** Every wake event causes a
   single-poller-claim dance: try to claim, poll, dispatch. With
   thousands of events per second this is fine; with millions
   you start seeing the claim contention on the bitmap.

   *Mitigation*: io_uring's batched submission helps if your I/O
   is on Linux and you can flip the reactor backend (currently a
   one-line edit in `reactor.zig` until the runtime
   `Config.reactor_backend` ships).

## Measuring per-operation cost

Wrap a hot path with `volt.tracing.span`:

```zig
const result = try volt.tracing.span(.{
    .name = "process_request",
}, struct {
    fn body() !Response { ... }
}.body);
```

The default sink emits OTel-shaped JSON Lines to stderr with
nanosecond start/end timestamps. Pipe to a collector for
aggregation, or set a custom sink:

```zig
fn mySink(e: volt.tracing.Event) void { /* aggregate */ }
volt.tracing.setSink(&mySink);
```

For lower-level "what are workers doing" questions:

```zig
const m = try volt.observability.metrics(allocator, rt);
defer m.deinit(allocator);
for (m.workers) |w| {
    std.log.debug("w{d}: pushed={d} stolen={d} parked={d} ctx_sw={d}", .{
        w.id, w.pushes, w.steals, w.parks, w.context_switches,
    });
}
```

High `parks` count means workers are idle a lot — your work
isn't keeping them busy. High `steals` means good load
distribution. High `context_switches` per second is normal under
I/O load; it's only a problem if it's coming with high yields
from CPU code that should be batching.

## Snapshot the live runtime

```zig
const snaps = try volt.observability.snapshot(alloc, rt);
defer alloc.free(snaps);
```

Each `TaskSnapshot` has the task's name, spawn site, current
state, and accumulated CPU time. Useful in production for "what
are my long-running tasks doing right now" — pipe to your
operational dashboard.

## When the runtime *isn't* the bottleneck

Most performance problems aren't Volt's. Common ones:

- **Allocator pressure**. `std.heap.DebugAllocator` is for
  development; use `std.heap.smp_allocator` or arena-per-request
  in production.
- **Logging in the hot path**. `std.debug.print` synchronously
  formats and writes; in a request handler that's measurable
  overhead.
- **Heavy computation on the worker thread**. Move it to
  `volt.spawnBlocking`.
- **JSON parsing per request**. Use stack-allocated parser state
  + reuse buffers across requests.

Profile, don't guess. Volt's source is ~10K lines and readable —
if you see Volt's code dominating a profile, file an issue with
the trace; we want to know.

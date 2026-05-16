---
title: Performance tuning
description: Where Volt's overhead lives, what's worth measuring, and the knobs you can turn.
---

This guide is for when Volt is in your hot path and you've decided
you need to make it faster. Most programs don't need any of it —
the defaults are good enough for typical interactive workloads.

## Measure first

Run the bench gate from a fresh checkout to establish what
"normal" looks like on your hardware:

```sh
zig build bench-yield        # ~9 ns/op
zig build bench-spsc         # ~12 ns/op
zig build bench-mpmc         # ~54-157 ns/op
zig build bench-mutex        # ~15 ns/op
zig build bench-tcp-echo     # ~8,500 ns/RTT
zig build bench-spawn-hot    # ~100-575 ns/op (workers=1 .. 11)
zig build bench-rss          # ~16.6 KiB / coroutine at N=10000
```

Treat these as your **baseline**: if your application's costs are
within 2-3× of the bench numbers for the matching shape, the
runtime isn't the bottleneck. Your work is.

If you're well above bench numbers, profile. `samply record` on
Darwin is the recommended tool; the
[multi-worker profile](/performance/multi-worker-profile/) doc
walks through the methodology with real-data examples.

## The knobs (Config)

```zig
pub const Config = struct {
    allocator: std.mem.Allocator,    // required
    workers: ?usize = null,          // null → getCpuCount()
    max_concurrent_stacks: usize = 16 * 1024,
};
```

### `workers`

Default is `getCpuCount()` (with a floor of 1). Tune based on
workload:

- **CPU-bound**: `workers = physical_cores`. Hyperthreads rarely
  help with stackful coroutines because dispatch overhead
  doesn't parallelise across SMT.
- **I/O-bound**: `workers = getCpuCount()` is usually fine.
  Fewer workers leaves cores idle if many coroutines are runnable
  at once; more workers mean more reactor-claim contention.
- **Latency-sensitive single-purpose**: `workers = 1` eliminates
  cross-worker coordination entirely. Use for serial pipelines
  that don't benefit from parallelism.

For most workloads, the default is correct. Tune only when
you've measured a clear bottleneck and have a hypothesis about
which direction.

### `max_concurrent_stacks`

Default 16384. Sizes the slab arena: `n_slots × 256 KiB` of
virtual address space reserved at runtime init. Slots have zero
RSS until they're allocated; raising this is cheap on 64-bit
hosts.

Raise if:

- You expect tens of thousands of concurrent connections / coros.
- `volt.spawn` is returning `error.ArenaExhausted`.

Lower if:

- You want a hard cap on memory footprint (back-pressure: spawn
  fails when arena is full, instead of growing without bound).
- You're on a tight virtual-address budget (rare on 64-bit).

## What Volt does NOT have

Some knobs that exist in other runtimes don't exist here:

- **`pin_workers`**: no built-in CPU pinning. Wire
  `pthread_setaffinity_np` directly from your code if needed.
- **`deterministic`**: no built-in deterministic mode. Run
  `workers = 1` for reproducible test traces; full determinism
  also requires controlling the OS scheduler (sched_setaffinity
  + nice + RT priority) — out of Volt's scope.
- **`reactor_backend`**: kqueue is the only shipping backend;
  there's nothing to switch to. Linux backends are roadmapped.

## What's expensive (rough cost ordering)

1. **`mprotect` syscalls on slot first-use.** Each slot's top
   page is mprotect'd RW the first time it's allocated. After
   warmup, the slot stays committed; no further mprotect for
   that slot. Cost: ~1 µs per syscall, fires at most `n_slots`
   times over the runtime's life.

   *Mitigation*: warm the arena by spawning + completing N
   coroutines at startup. After that, the steady-state spawn
   path is allocation-only with zero syscalls.

2. **Context switches.** ~9 ns/op for the bare swap on arm64
   (`bench-yield`). Plus a few atomic ops for park-state
   transitions. The dominant cost in a "ping-pong between two
   coroutines" benchmark.

   *Mitigation*: batch work. If your loop does 1 ns of compute
   then yields, you're paying 9 ns+ of overhead per item.
   Restructure to do hundreds of items per suspend.

3. **Mutex contention under high core counts.** Volt's contended
   path goes through the parking lot — a sharded mutex per
   bucket. Under heavy contention on the same address, all
   contenders converge on one bucket, and the bucket's pthread
   mutex serializes them. Still 15 ns/op under bench-mutex's 8x
   contention; scales sublinearly past that.

   *Mitigation*: shard. Per-core counters with a periodic
   aggregator. Per-key locks instead of one global lock. The
   classic "shard your hash map" pattern.

4. **Mpmc ring contention.** Vyukov's MPMC is wait-free under
   non-pathological load, but `enqueue_pos` is a single counter
   that all producers CAS-bump. Under heavy multi-producer load
   it's the contention point.

   *Mitigation*: shard channels. N producers each writing to
   their own Spsc that one consumer multiplexes is faster than
   N producers contending on one Mpmc.

5. **Cross-P stealing on idle workers.** Workers without local
   work randomly choose a sibling P and try to steal. The CAS-
   based steal is wait-free, but the search can be wasted work
   if no one has anything stealable.

   *Mitigation*: rarely worth tuning. If steal contention shows
   up high in a profile, the underlying issue is usually load
   imbalance — fix the work shape, not the scheduler.

## Stack size

Volt commits 16 KiB (1 Darwin page) per slot at first use, growing
in 16 KiB increments via SIGSEGV. Total reservation per slot is
256 KiB. The body and reservation sizes are compile-time
constants in `src/stack.zig`.

If you're hitting deep recursion that overflows the 256 KiB
reservation:

- Refactor to remove the recursion (iterative or trampoline-style).
- Raise `RESERVATION_SIZE` in `src/stack.zig` for the whole
  runtime (no per-coroutine override today).

For most workloads, the 16 KiB initial commit is correct — zero
physical memory until the coroutine actually uses stack.

## Diagnostics in production

```zig
rt.dumpState();
```

Writes to stderr: parked-workers bitmap, num_searching count,
reactor poller flag, pending reactor events, per-P stat counters
(spawned / done / fairness hits / cross-thread unparks).

Safe to call from any thread. Useful for "what is the runtime
doing right now" investigations during hangs.

```sh
samply record -- zig-out/bin/my-app
```

`samply` works well on Darwin. The architecture chapter has
real examples of profiles in [Multi-worker
profile](/performance/multi-worker-profile/).

## When the runtime *isn't* the bottleneck

Most performance problems aren't Volt's. Common ones:

- **Allocator pressure**. `std.testing.allocator` is for tests;
  use `std.heap.smp_allocator` (the recommended default) or
  arena-per-request in production. Volt does not provide a
  bespoke allocator — that's `std`'s job.
- **Logging in the hot path**. `std.debug.print` synchronously
  formats and writes; in a request handler that's measurable
  overhead. Buffer and flush async.
- **Heavy computation on a worker thread**. Move it to a real
  OS thread via `std.Thread.spawn` + a Mpmc bridge (see
  [Offloading CPU work](/cookbook/work-offload/)).
- **Mutex held across a suspension**. See
  [Common pitfalls](/guides/common-pitfalls/) — the lock
  serialises unrelated coroutines.

Profile, don't guess. Volt's source is ~5 KLoC and readable — if
you see Volt's own code dominating a profile, file an issue with
the trace; we want to know.

## Phase-landing protocol applies to user code too

If you measure a change of 20% on a key bench, you've done
something real. If it moves by less, it's noise — discard. The
bench gate's discipline (5-run medians, fixed system load) is the
right discipline for your own perf work.

## See also

- [Benchmarks](/performance/benchmarks/) — methodology and baselines.
- [Multi-worker profile](/performance/multi-worker-profile/) — profiling receipts.
- [Slab arena postmortem](/performance/slab-arena-postmortem/) — what a 30× regression looked like.
- [Architecture: M:N scheduler](/architecture/mn-scheduler/) — where the cycles go.

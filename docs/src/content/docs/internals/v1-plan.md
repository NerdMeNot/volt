---
title: v1 plan — the best stackful coroutine runtime in Zig
description: Pre-ship plan to incorporate per-worker arenas, structured concurrency, io_uring-default, deadlock detection, and Go-class spawn cost into v1.0.0-zig0.16.0 before tagging.
---

:::caution
**Historical (pre-v2 flattening).** This plan was written against
the v0.x src tree (`src/api/`, `src/coroutine/`, `src/scheduler/`,
`src/sync/`) which no longer exists. Many of the items here were
implemented, some were retired, some are still open. The current
state of the tree is documented in `architecture.md` and the
multi-worker / direct-handoff docs. Kept for historical context;
do not use as a roadmap.
:::

Volt's v1 hasn't shipped. This plan replaces the prior "ship the safe v1, defer the rest to v1.x" framing — we have one shot at the v1 mental model and we should make it the right one.

The bar: **the best stackful coroutine runtime in Zig**, with all the Zig advantages (no GC, explicit allocators, comptime specialization) and Go-class performance on the spawn / context-switch / channel hot paths.

## What we're committing to

**Single scheduler.** Work-stealing, like Go and Tokio. No comptime "thread-per-core vs work-stealing" knob — that's a deployment pattern users build on top, not a runtime config. One mental model, one bug surface, one library compatibility story.

**Per-worker arenas.** Each Worker owns its memory. Stack pool, coroutine struct allocator, closure allocator — all per-worker, lock-free for the owning worker, no syscalls on spawn hot path. Replaces today's global stack pool + user-passed allocator everywhere.

**Structured concurrency as primary API.** `volt.scope` owns child lifetimes. Spawn-into-scope. Errors and cancellation propagate by construction. `volt.spawn` survives as the escape hatch.

**io_uring default on Linux.** epoll only as a legacy fallback for kernels < 5.1.

**Deadlock detection in Debug.** Park records the wait-for graph; cycles panic with a readable trace.

**Bench-as-design-driver.** Bench gate ceilings become public commitments. Identical workloads run against a Go reference. Every perf-sensitive PR shows before/after.

## What stays

- Stackful with AAPCS64 / SysV asm context switch (10 ns one-way).
- Park primitive (single-atomic state encoding).
- Growable virtual stacks via mmap PROT_NONE + guard-page promotion.
- Chase-Lev work-stealing deque.
- kqueue (Darwin), epoll/io_uring (Linux), IOCP+AFD (Windows) reactor backends.
- Vyukov-style channel.
- parking_lot-style sync primitives.

## What we're dropping

- The "user passes an allocator everywhere" model — `volt.spawn(allocator, fn, args)` becomes `volt.spawn(fn, args)`.
- The 8 MB virtual stack reservation — drops to 1 MB; reduces TLB pressure.
- `volt.launch` as a separate concept — merges with `volt.spawn`; the variant that returns `*Job` vs `*Task(T)` is determined by the function's return type.
- The `bench/bench_core.zig` use of `std.heap.page_allocator` — switches to `std.heap.smp_allocator`.

## Phases

Strictly sequential because each unblocks the next. Estimated ~3 weeks of focused work to ship.

---

### Phase A — Allocator architecture (week 1)

Foundation work. Everything else depends on this.

- **A1. Per-worker stack arena.** Each Worker owns a 64 MB mmap'd region carved into 1 MB stack slots. LIFO freelist per worker — single-owner, lock-free push/pop. Pre-warm on `Runtime.init` (mmap once, not per-spawn). Cross-worker steal of stacks is rare and goes through a fallback global pool.

- **A2. Per-worker bump arena for spawn metadata.** Coroutine struct, Closure, args, Task/Job — all bump-allocated from the owning worker's arena. No `allocator.create(...)` per spawn. Free list per slot size for reuse.

- **A3. Single-allocator API.** `Runtime.init` takes one allocator for runtime-state (Worker[], Reactor, etc.). Every other API drops the allocator parameter. `volt.spawn(fn, args)` reads the current worker's arena.

- **A4. Shrink default stack reservation 8 MB → 1 MB.** Override available via `Runtime.Config.stack_reserved`.

**Acceptance:** spawn+join bench drops from ~20 µs (today, page_allocator) to ≤ 1 µs on Linux x86. Yield stays at 20 ns. mmap traffic during the spawn hot loop is **zero** (verified via strace on bench).

**Critical files:**
- `src/scheduler/worker.zig` — Worker grows arena fields.
- `src/coroutine/stack_pool.zig` — rewritten as per-worker LIFO; old global pool becomes a cold-path fallback.
- `src/runtime.zig` — Runtime.createCoroutine routes to current worker's arenas.
- `src/api/spawn.zig`, `launch.zig` — drop allocator param.
- `src/coroutine/stack.zig` — `default_reserved` 8 MB → 1 MB.
- All callers of `volt.spawn(allocator, ...)` — port to no-allocator form.

---

### Phase B — API surface (week 2)

- **B1. `volt.scope(body)` as primary API.** Trio / Kotlin `coroutineScope` semantics. Body is `fn (*Scope) !void`. `scope.spawn(fn, args)` returns a handle. On scope exit: wait for all children; propagate first error; cancel siblings on first failure; reraise. Errors out of scope.body propagate after children unwind.

- **B2. Merge `volt.launch` and `volt.spawn`.** Single function. Return type inferred from user function: `void` → `*Job`, `T` → `*Task(T)`. Reduces public surface area.

- **B3. io_uring default on Linux ≥ 5.1.** Probe kernel version at `Runtime.init`. Default → io_uring; fall back to epoll on older kernels. `-Dreactor=epoll` becomes opt-out (was opt-in to `=iouring`). Update CI: io_uring becomes the headline backend.

**Acceptance:** all examples + tests use `volt.scope` as the entry pattern. `volt.spawn` exists but is positioned as escape hatch in docs. CI runs both backends green; iouring is the default.

**Critical files:**
- `src/sync/Scope.zig` — already exists, expand to be primary.
- `src/api/spawn.zig` + `launch.zig` — merge.
- `src/io/reactor.zig` — flip default backend selection.
- `examples/*.zig` — port to `volt.scope` style.
- `docs/` — new "getting started" walks through scope-first.

---

### Phase C — Correctness hardening (week 3)

- **C1. Deadlock detection in Debug builds.** `Park.parkCurrent` (Debug only) records `(parker_id, parked_on_id)` in a runtime-owned graph. Cycle check on every parkCurrent. On cycle: panic with the wait-for path printed. Zero release-mode cost (comptime-gated).

- **C2. Complete IRIW audit.** From the original v1 plan R3. Every (signaller, parker) pair: Mutex (done), Channel send/recv/close, Notify/Semaphore/RwLock unlock, Sleep timer fire/cancel, Job.join + child Done. New `scripts/audit_iriw.sh` flags `flag.store + ptr.load` patterns not using seq_cst. Document in `docs/internals/cancellation-contract.md`.

- **C3. Bench-as-design-driver infrastructure.** New `bench/bench_vs_go/` with matched Go binaries running the same workloads (TCP echo, fan-out/fan-in, channel pipeline). CI uploads numbers to a tracked dashboard. PR template includes "perf delta" section. Bench gate ceilings become public commitments (yield ≤ 30 ns, spawn ≤ 1 µs, channel ≤ 200 ns, mutex ≤ 300 ns).

- **C4. Stress at N=500 across 3 backends.** kqueue (Darwin), io_uring (Linux), epoll (Linux). 1500 iterations all green. Hard `pendingCount == 0` assert on Runtime.deinit (no print-warning fallback).

**Acceptance:** All N=500 stress runs green. Deadlock detection catches a hand-crafted cycle in < 1 dispatch round-trip. Bench-vs-Go shows Volt within 2× of Go on every workload (ideally beats Go on switch-heavy paths).

**Critical files:**
- `src/scheduler/park.zig` — Debug-only graph machinery.
- `scripts/audit_iriw.sh` — new.
- `bench/bench_vs_go/` — new directory with matched harnesses.
- `src/runtime.zig` — flip pendingCount to hard assert.

---

### Ship gate (S)

- **S1. All v1 plan items A1-C4 landed and stable for ≥ 3 consecutive CI runs.**
- **S2. README + docs reframed to v1 messaging** — drop "v0.x" placeholder language. Stackful coroutine runtime, 10ns switch, ~500ns spawn, no GC, structured concurrency, io_uring native.
- **S3. Tag `v1.0.0-zig0.16.0` on `main`.**

## Out of scope for v1 (explicitly v1.x)

- Windows native runtime (W3-W7) — needs upstream Zig stdlib fixes + ~6 days work.
- macOS x86_64 native runtime — needs SEGV diagnosis (in flight via CI re-add).
- `volt-top` live introspection CLI.
- Built-in OpenTelemetry tracing export.
- Stack growth via copy-and-fixup (the Zig-no-stackmaps blocker — accept fixed reservation in v1).
- Performance benchmark dashboard (we'll have raw numbers in CI logs at v1; the dashboard is v1.x).

## What "best stackful runtime in Zig" looks like at v1.0 ship

```zig
// Hello world
try volt.run(allocator, struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(handler, .{conn});
        try s.spawn(handler, .{conn});
        // scope.deinit waits, propagates errors, cancels on failure.
    }
}.body, .{});
```

- Spawn cost: ~500 ns (Go: ~200–500 ns).
- Yield: 10 ns one-way (Go: ~50–150 ns).
- Channel SPSC: ~150 ns (Go: ~250 ns).
- Mutex contended: ~200 ns (Go: ~250 ns).
- Memory per coroutine (live, after grow to a few KB): ~16 KB resident, 1 MB virtual.
- IO: io_uring on Linux ≥ 5.1, kqueue on Darwin.
- Concurrency model: structured. Cancellation works by default.
- Allocator: explicit at runtime init, hidden everywhere else.

That's the Volt the Zig ecosystem copies into other projects.

## Risks

- **A3 (drop allocator-passed-everywhere) is a breaking API change.** All examples + tests + the few external users we have need updating. The blast radius is the whole repo's user-facing surface. Worth it.
- **B1 (volt.scope as primary)** is a public-API recentering. Once landed, hard to reverse. We need the scope semantics to be RIGHT first time. Reference Trio + Kotlin closely.
- **C2 (IRIW audit)** may surface 1–2 more races. We've found 2 so far (Mutex unlock dual-store, TLS caching). The third would be a v1 ship blocker if it's load-bearing. Plan extra week of headroom.

## Why this is worth the 3-week delay

We've already proven correctness this session — the v1 we'd ship today is *correct*. But it's not *fast on spawn* and the API has unnecessary surface area (allocator everywhere, separate launch/spawn). Tagging v1 with those rough edges means every adopter starts with a runtime that's competitive on switches but mediocre on spawns and ergonomically heavier than Go.

The runtime that becomes the reference doesn't get a second chance at a first impression.

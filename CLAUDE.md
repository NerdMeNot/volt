# CLAUDE.md — Volt

## What is Volt?

A **stackful coroutine runtime** for Zig. Kotlin-style ergonomics, native
io_uring (Linux ≥ 5.15, recommended ≥ 6.1) / kqueue (Darwin) / IOCP
(Windows) reactors, no GC. Designed as the substrate for NerdMeNot's
async/IO libs (S3 client, HTTP client, PG pool, DataFrame I/O).

The Linux minimum is set by the io_uring features the reactor uses:
`IORING_OP_POLL_ADD` (5.1), `IORING_OP_TIMEOUT` (5.4),
`IORING_OP_ASYNC_CANCEL` (5.5), and post-restructure
`IORING_POLL_ADD_MULTI` (5.13). 5.15 is Ubuntu 22.04 LTS's stock
kernel — that's the floor. 6.1 unlocks `IORING_SETUP_SINGLE_ISSUER`
+ `IORING_SETUP_DEFER_TASKRUN`, which we runtime-detect and opt into.

The pre-stackful tree (Future/Poll state machines) is preserved at git tag
`pre-stackful-pivot`. An earlier stackful attempt was POC-validated against
Go in `spike/`, then rewritten — the resulting tree is what `src/` contains
today. There is just Volt.

## Build

**Zig 0.16.0** required.

```sh
zig build                    # Build the volt module
zig build test               # Run unit tests
zig build docs               # Generate API documentation
zig build bench-spawn-hot    # (and bench-yield, bench-spsc, bench-mpmc,
zig build bench-mutex        #  bench-tcp-echo, bench-parallel-compute,
                             #  bench-rss, bench-scaling, bench-fanout-scaling)
zig build spike-A            # POC validations (A, B, C, D, F, G, H)
```

## Source tree

Convention: a public subsystem is a `foo.zig` facade + a `foo/`
subdir of its parts. `reactor/`, `fs/`, `net/`, `time/` follow it.
Single-file modules (the scheduler core, primitives) stay flat.

```
src/
├── lib.zig                # Public surface + composition: spawn, scope,
│                          # joinAll/joinFirst, withTimeout, sleep, re-exports
├── runtime.zig            # Runtime: workers + injection + reactor; run/spawn/
│                          # dispatch/parkWorker/wakeOneParked
├── runtime_fs_ring.zig    # Per-P io_uring fs-ring glue (extracted from runtime)
├── runtime_signal.zig     # SIGINT graceful-shutdown plumbing for runWithSignals
├── worker.zig             # M (OS thread) + Mailbox (per-P MPMC queue)
├── p.zig                  # P: WSQ + lifo_slot + mailbox + per-P pools
├── work_steal_queue.zig   # Fixed-256 lock-free WSQ (Tokio-style)
├── coroutine.zig          # Coroutine struct, Frame factory, park_state machine
├── context.zig            # ctx-switch dispatch → context_arm64 / context_x86_64
├── context_arm64.zig      # AAPCS64 wide ctx switch + trampoline (asm)
├── context_x86_64.zig     # System V x86-64 ctx switch (.S linked via build.zig)
├── current.zig            # threadlocal current coroutine pointer
├── parker.zig             # __ulock_wait (Darwin) / FUTEX_WAIT (Linux) Parker
├── park.zig               # Parking lot — sharded bucket-locked waiter queues
├── poll_desc.zig          # Go-netpoll-style per-fd readiness state machine
├── pd_handle.zig          # Lazy per-fd PollDesc slot (atomic ensure/release)
├── cancel.zig             # Cancel token + cancel-aware waiter registration
├── select.zig             # select over multiple channel/sync ops
├── race.zig               # race() — first-of-N coroutine composition
├── signal.zig             # Public POSIX signal API (sigaction, ignoreSigpipe)
├── blocking_pool.zig      # spawnBlocking thread pool
├── stack.zig              # Coroutine stack slab arena
├── task.zig               # Task(T) typed handle (atomic-counter join)
├── channel.zig            # Spsc / Mpmc / Oneshot / Watch / Broadcast
├── streams.zig            # Pull-based async Stream(T) + operators
├── sync.zig               # Mutex, RwLock, OnceCell, Barrier, Notify, Semaphore
├── testing.zig            # Leak-detecting multi-worker-safe test allocator
├── reactor.zig            # Reactor facade — comptime-picks the backend
├── reactor/               # kqueue, epoll, io_uring, linux, posix (shared
│                          # helpers), fs (spawnBlocking proxy), conformance_test
├── fs.zig + fs/           # File, Dir, Metadata, mmap, path, syscall, watcher,
│                          # error, ring (per-P io_uring fs ring)
├── net.zig + net/         # Tcp*, address, options, resolver, udp, unix
└── time.zig + time/       # Duration/clock + after, ticker, timer

bench/                     # Perf benches — each one a standalone executable
spike/                     # POC validations (kept as historical reference)
docs/                      # Astro-Starlight site
```

Windows/IOCP was dropped 2026-06-16; its reactor backend lives on
branch `feat/windows-fs` (issue #6), not in this tree.

## Benchmarks (Darwin arm64, 11 cores, ReleaseFast)

Go 1.26.0 is the **scale reference**, not a competitive benchmark.
Go has been optimised over 15+ years; expecting Volt to beat it
everywhere would be naive. When Volt is faster, it's usually because
Go pays a cost we don't (GC write barriers, function colouring),
not because we out-engineered them. The numbers exist to know
whether we're in a sensible range. See `BENCHMARKS.md` for full
methodology + receipts.

| Workload | Volt | Go | Volt/Go |
|---|---|---|---|
| yield (one-way ctx switch) | 9 ns | 42 ns | 0.21× |
| Mutex contended (8 × 50k) | 10 ns | 81 ns | 0.12× |
| Spsc send+recv (cap=16) | 12 ns | 33 ns | 0.36× |
| TCP echo (64 × 16 RTT × 1 KB) | ~9,500 ns | 9,050 ns | ~1.0× |
| spawn+join workers=1 | 78 ns | 136 ns | 0.57× |
| fan-out scaling workers=4 | 64 ns | — | — |
| fan-out scaling workers=11 | 113 ns | 107 ns | 1.06× |
| parallel-compute (8 workers) | 6.5× speedup | — | near-ideal |
| stress (mixed, 45 s) | ~850 M ops | — | green |
| spawn+join workers=11 (synthetic) | 421 ns | 213 ns | 1.97× |

Numbers reflect the 2026-05 perf cycle (driver-poll-wakeup fix,
spin-before-park, log-N stealing cap, runtime cache-line padding,
Coroutine extern-struct field reorder for cache locality). Range
on spawn-hot tightened from ±32 % variance to ±10 % at a 25 % lower
median. The synthetic-shape Volt/Go gap dropped under 2× for the
first time. See `git log` for the commit trail.

The single workers=11 outlier is a synthetic spawn-heavy shape (one
driver feeding 11 workers trivial tasks) where adding workers can
only hurt. Real-work multi-worker shapes land near or below the Go
reference. See `docs/internals/multi-worker-profile.md` for the
investigation.

## Phase-landing protocol

Every architectural change runs the full bench suite green before
merge. Phase 4 was reverted on 2026-05-15 because it skipped this and
shipped a SIGSEGV in `bench-spawn-hot`. The 2026-05-16 stack guard-page
landing also skipped this — it regressed `bench-spawn-hot` ~30× via a
POOL_CAP=64 cliff under BATCH=1000 spawn-bursts; root-caused and fixed
later that day with the slab-arena rewrite. See
`docs/internals/phase-4-postmortem.md`.

Required bench suite for any landing that touches the scheduler,
coroutine struct, allocation path, or sync primitives. Each must be
green vs its prior measured baseline — no "follow up later" on
regressions.

- `zig build bench-yield`
- `zig build bench-spsc`
- `zig build bench-mpmc`
- `zig build bench-mutex`
- `zig build bench-spawn-hot` — canonical: single Notify barrier per 1000-batch (matches Go's `wg.Wait`)
- `zig build bench-fanout-scaling` — N drivers × N workers, real parallel work
- `zig build bench-scaling` — single-driver curve (false-parallelism receipt)
- `zig build bench-parallel-compute`
- `zig build bench-tcp-echo`
- `zig build bench-reactor-fanout` — high-fd-pressure oversubscribed TCP; guards the kqueue persistent-registration close-churn livelock (regressed silently once because tcp-echo's 64 clients stayed under the threshold)
- `zig build bench-rss` — RSS per idle coroutine
- `zig build stress` (3 runs)

Variance is high on the spawn benches — take 5-run medians, not single
runs. `bench-rss` and `bench-scaling` are receipt benches; their numbers
must move with the changes they're claimed to enable.

### Memory-leak gate

All `src/*.zig` unit tests use `std.testing.allocator` (the leak-
detecting `GeneralPurposeAllocator{ .safety = true }`). A test that
leaks memory fails `zig build test`. Adding a new allocation path
to the runtime requires a test that exercises both alloc and free
of that path so the leak detector validates it.

## Measurement discipline

All benchmark claims are made against:

1. An **absolute Go reference** measured on the same hardware
   (see `bench/go/`). No "% improvement" without an absolute Go
   number in the same table.
2. A **matched bench shape**. Volt's `bench-spawn-hot` and Go's
   `wg.Wait` benchmark are *not* the same shape — the matched
   version is `bench-spawn-hot (Notify barrier shape — was `bench-spawn-hot-waitall` pre-rename)`. If we publish a comparison,
   it uses matched shapes.
3. **5-run medians**, not single runs, for any spawn-heavy bench.
   Run-to-run variance can be 30-50 %.

## Zig 0.16 API notes

- `std.atomic.Value(T)` for atomics
- `std.Thread.{Mutex,Condition,sleep,Futex}` are gone — Volt builds its own
  on `__ulock_wait` (Darwin) / `futex` (Linux). See `src/parker.zig`.
- `std.posix.{socket,pipe,fcntl,kqueue,read,write,...}` medium-level
  functions are gone — Volt uses `@extern` to libc directly. See
  `src/net.zig`, `src/reactor_kqueue.zig`.
- `std.fs.cwd()`, `std.fs.Dir`, `std.fs.File` moved into `std.Io`; Volt
  doesn't use them — sockets are bare fds.
- Module-level comptime asm on x86_64-linux ELF doesn't always emit
  symbols; use a separate `.S` file linked via `build.zig`. (Documented
  for when #149 adds the x86_64 ctx switch.)
- No `async` / `await` keywords; the runtime exposes synchronous-looking
  coroutine APIs that suspend at I/O / channel / sync points.

## Memory model

All shared state in `src/` is documented in
`docs/src/content/docs/internals/memory-model.md` — who reads, who writes,
which orderings, what happens-before. If you add or change shared state,
update that doc or your change isn't done.

Critical recent invariants:

- **`Coroutine.park_state`** closes the register-then-park race. A
  primitive that registers a waiter and calls `runtime.park()` is safe
  from cross-worker double-dispatch because the transition to PARKED
  happens *after* the swap-back, inside `dispatch`.
- **`Mutex.lock`** slow path inspects the swap-CONTENDED return value —
  unlock's fast path doesn't take the inner mutex, so it can race past
  the slow-path swap; we detect that race and take ownership inline.
- **No global pending counter.** Termination is observed via the root
  Task's `WaitGroup.thread_waiter`, which `Runtime.run` sets to the
  driver's Parker.

## Git conventions

- **No Co-Authored-By** lines in commit messages.
- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `bench:`, `refactor:`.
- Concise messages.

## Naming conventions

| Kind | Convention | Example |
|---|---|---|
| Types | PascalCase | `Mutex`, `Coroutine`, `Spsc` |
| Functions | camelCase | `tryLock`, `tryAccept`, `writeAll` |
| Variables | snake_case | `parked_workers`, `wait_next` |
| Files exporting a type | PascalCase.zig (rare in current tree) | — |
| Namespace files | snake_case.zig | `lib.zig`, `runtime.zig`, `sync.zig` |

## Code principles

1. **Correctness first** — port well-known algorithms (Tokio WSQ,
   parking_lot mutex, Vyukov ring) rather than invent.
2. **Comments explain WHY, not WHAT** — ordering constraints, hidden
   races, platform behaviour.
3. **Atomic ops need ordering justification** — every `.acq_rel` /
   `.release` / `.acquire` should be paired against a matching read or
   write in the memory-model doc.
4. **No thread-identity state across yield points** — a coroutine may
   resume on a different worker thread, so anything that depends on
   *which OS thread you're on* is invalid after a yield/park: TLS
   reads, thread-locals, cached `pthread_self()`, per-thread runtime
   state, pointers returned from thread-local lookups. Stack
   pointers into your *own* (or another live) coroutine's frame are
   fine — slots are at stable VAs for the coroutine's lifetime.
   (Known instance worked around: x86_64 Linux LLVM cached `%fs:0`
   in r12 across yield; fix was `@call(.never_inline)` around TLS
   reads.)
5. **Stackful means stack contents preserved across suspension** — heap
   pointers stashed on a coroutine's stack live as long as the coroutine.
6. **Explicit allocators** — Zig idiom; the runtime takes one allocator
   in `Runtime.Config` and uses it for every per-coroutine allocation.

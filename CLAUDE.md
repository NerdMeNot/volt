# CLAUDE.md — Volt

## What is Volt?

A **stackful coroutine runtime** for Zig. Kotlin-style ergonomics, native
io_uring (Linux ≥ 5.1) / kqueue (Darwin) / IOCP (Windows) reactors, no GC.
Designed as the substrate for NerdMeNot's async/IO libs (S3 client, HTTP
client, PG pool, DataFrame I/O).

The pre-stackful tree (Future/Poll state machines) is preserved at git tag
`pre-stackful-pivot`. The first stackful attempt (v0.x) was POC-validated
against Go in `spike/`, then rewritten and retired in favour of the v2
tree, which is now `src/`. There is no "v2" in the codebase — there is
just Volt.

## Build

**Zig 0.16.0** required.

```sh
zig build                    # Build the volt module
zig build test               # Run unit tests
zig build docs               # Generate API documentation
zig build bench-spawn-join   # (and bench-yield, bench-spsc, bench-tcp-echo,
zig build bench-mutex        #  bench-parallel-compute) — perf benches
zig build spike-A            # POC validations (A, B, C, D, F, G, H)
```

## Source tree

```
src/
├── lib.zig                # Public surface: Runtime, Task, yield, channel,
│                          # net, sync, reactor, current
├── runtime.zig            # Runtime: workers + injection + reactor; run/spawn/
│                          # dispatch/parkWorker/wakeOneParked
├── worker.zig             # M (OS thread) + Mailbox (per-P MPMC queue)
├── p.zig                  # P: WSQ + lifo_slot + mailbox + per-P pools
├── work_steal_queue.zig   # Fixed-256 lock-free WSQ (Tokio-style)
├── coroutine.zig          # Coroutine struct, Frame factory, park_state machine
├── context_arm64.zig      # AAPCS64 wide ctx switch + trampoline (asm)
├── current.zig            # threadlocal current coroutine pointer
├── parker.zig             # __ulock_wait (Darwin) / FUTEX_WAIT (Linux) Parker
├── park.zig               # Parking lot — sharded bucket-locked waiter queues
├── reactor_kqueue.zig     # kqueue reactor + non-blocking IO helpers
├── task.zig               # Task(T) typed handle (atomic-counter join)
├── channel.zig            # Spsc(T, cap) — comptime-specialized SPSC ring
├── sync.zig               # Mutex, Notify, Semaphore (parking-lot based)
└── net.zig                # TcpListener, TcpStream, Address (IPv4 loopback)

bench/                     # Perf benches — each one a standalone executable
spike/                     # POC validations (kept as historical reference)
docs/                      # Astro-Starlight site
```

## Benchmarks (Darwin arm64, 11 cores, ReleaseFast)

Numbers from 2026-05-15 vs Go 1.26.0 on the same hardware. See
`docs/internals/multi-worker-profile.md` for the investigation that
produced these and `BENCHMARKS.md` for the full table.

### Where Volt wins

| Bench | Volt | Go | Ratio |
|---|---|---|---|
| yield (one-way ctx switch) | 9 ns | 42 ns | **4.7× faster** |
| Spsc send+recv (cap=16) | 12 ns | 33 ns | **2.8× faster** |
| spawn+join **workers=1** (waitall) | 106 ns | 137 ns | **1.3× faster** |
| fan-out scaling **workers=1** | 73 ns | 107 ns | **1.5× faster** |
| TCP echo 64×16 RTT 1 KB | ~7,000 ns | 9,050 ns | **1.3× faster** |
| fan-out scaling workers=11 | 117 ns | 106 ns | 1.10× — parity |
| parallel-compute (8 workers) | 6.62× speedup | n/a | near-ideal |
| stress test (mixed, 45 s) | ~140 M ops | n/a | green |

### Where Volt is behind

| Bench | Volt | Go | Ratio |
|---|---|---|---|
| spawn+join workers=11 (waitall) | 490 ns | 172 ns | 2.84× behind |
| Mutex contended workers=NumCPU | ~640 ns | 81 ns | ~8× behind |

The multi-worker spawn+join gap is concentrated on synthetic
spawn-heavy patterns (one driver spawning into many workers with
trivial per-task work). On real-work multi-worker shapes (fan-out,
TCP, parallel-compute), we're at parity or better. Closing the
remaining gap further needs deeper structural work (smaller
Coroutine struct, per-fn-type Combined pool) — see the profile doc.

The Mutex gap is its own design problem; see #168.

## Phase-landing protocol

Every architectural change runs the full bench suite green before
merge. Phase 4 was reverted on 2026-05-15 because it skipped this and
shipped a SIGSEGV in `bench-spawn-hot`. See
`docs/internals/phase-4-postmortem.md`.

Required bench suite for any landing that touches the scheduler,
coroutine struct, allocation path, or sync primitives. Each must be
green vs its prior measured baseline — no "follow up later" on
regressions.

- `zig build bench-yield`
- `zig build bench-spsc`
- `zig build bench-mutex`
- `zig build bench-spawn-hot` — canonical: single Notify barrier per batch (matches Go's `wg.Wait`)
- `zig build bench-spawn-hot-individual` — Volt-specific: 1000 individual `task.join` per batch
- `zig build bench-spawn-join`
- `zig build bench-fanout-scaling` — N drivers × N workers, real parallel work
- `zig build bench-scaling` — single-driver curve (false-parallelism receipt)
- `zig build bench-parallel-compute`
- `zig build bench-tcp-echo`
- `zig build bench-rss` — RSS per idle coroutine (Zig-native metric)
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
4. **No raw pointers across yield points** — a coroutine may resume on a
   different worker thread.
5. **Stackful means stack contents preserved across suspension** — heap
   pointers stashed on a coroutine's stack live as long as the coroutine.
6. **Explicit allocators** — Zig idiom; the runtime takes one allocator
   in `Runtime.Config` and uses it for every per-coroutine allocation.

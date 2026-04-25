# CLAUDE.md - Volt

## What is Volt?

A **stackful coroutine runtime** for Zig — work-in-progress rebuild from a
prior stackless attempt. Inspired by Kotlin coroutines for ergonomics, with
the perf characteristics targeting "lighter than Go on memory and spawn cost,
no GC, native io_uring." Designed as the foundation for NerdMeNot's lightweight
networking and file I/O libs (S3 client, HTTP client, PG pool, DataFrame I/O).

The pre-stackful tree (Future/Poll state machines) was stripped 2026-04-26.
The pre-strip state is preserved at git tag `pre-stackful-pivot` (commit
`44ec2e3`).

## Build

**Zig 0.16.0** required.

```sh
zig build              # Build library
zig build test         # Run unit tests on the kept platform internals
zig build docs         # Generate API documentation
```

The grand `test-all` / `test-stress` / `test-concurrency` / `bench` targets
were removed with the stackless tree. They come back as the new core lands.

## Current state (rebuilding)

```
src/
├── lib.zig                  # Public API entry — minimal scaffold
├── time.zig                 # Duration, Instant types (model-agnostic)
└── internal/
    ├── thread/              # Mutex, Condition, Futex, sleep
    │                        # Volt's own thread primitives — std.Thread.*
    │                        # equivalents moved to std.Io in 0.16
    ├── syscall.zig          # Raw syscall wrappers — replaces removed
    │                        # medium-level std.posix.* functions
    └── util/
        ├── linked_list.zig  # Intrusive doubly-linked list
        ├── slab.zig         # Slab allocator
        ├── pool.zig         # Object pool (lock-free)
        ├── stack_guard.zig  # Magic-number stack overflow detection
        ├── cacheline.zig    # Cache-line alignment
        ├── bit.zig          # Bit manipulation
        ├── invocation_id.zig # ABA-protection IDs
        └── signal.zig       # Signal handling helpers

spike/coroutines/             # Stackful spike — REFERENCE, will be deleted
                              # once the production rewrite lands
attic/                        # Archived for reference, will be deleted at v1
├── concurrency-tests/        # Loom-style harness from prior tree
└── vyukov_channel_reference.zig  # The optimized MPMC algorithm
```

## Spike validation (Darwin-ARM64, ReleaseFast)

| Metric | Volt | Go reference | Target |
|---|---|---|---|
| Context switch (one-way) | **10ns** | ~150ns | ≤200ns |
| Spawn (gpa) | **622ns** | ~3µs | ≤1000ns |

Numbers from `spike/coroutines/bench_switch.zig`. Stack size currently 64KB
(target 4KB once guard pages + slab pool land).

## Roadmap (toward v1.0.0-zig0.16.0)

| Version | Scope |
|---|---|
| v0.1 | Scheduler + run/launch/yield/scope (working coroutines, no I/O) |
| v0.2 | I/O integration (kqueue/epoll/io_uring → coroutine wake) |
| v0.3 | Channels + select |
| v0.4 | Sync primitives (Mutex, Semaphore, Notify) |
| v0.5 | Structured concurrency (scope, supervisor, cancellation) |
| v0.6 | Streams/flows |
| v0.7 | Timers (sleep, interval, withTimeout) |
| v0.8 | Blocking pool, dispatcher abstraction |
| v0.9 | Hardening (multi-arch asm, guard pages, slab pool) |
| v1.0 | Ship |

Currently at v0.1 starting line.

## Zig 0.16.x API Notes

- `std.atomic.Value(T)` for atomics
- `std.Thread.Futex` is gone; use `volt.internal.thread.Futex`
- `std.Thread.{Mutex,Condition,sleep}` are gone; use `volt.internal.thread.*`
- `std.posix.{socket,pipe,fcntl,kqueue,read,write,...}` medium-level functions
  are gone; use `volt.internal.syscall.*`
- `std.fs.cwd()`, `std.fs.Dir`, `std.fs.File` moved into `std.Io`; we don't
  use them
- `std.time.nanoTimestamp` moved; use `volt.time.nanoTimestamp`
- `@Type(.{...})` replaced by `@Struct`, `@Union`, `@Enum`, `@Tuple`, `@Int`
- No async/await keywords; the runtime provides synchronous-looking
  coroutine APIs that suspend at I/O / channel / sync points

## Git Conventions

- **No Co-Authored-By** lines in commit messages
- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `bench:`, `refactor:`
- Keep messages concise

## Naming Conventions

| Kind | Convention | Example |
|------|-----------|---------|
| Types | PascalCase | `Mutex`, `Coroutine`, `Channel` |
| Functions | camelCase | `tryLock`, `tryAccept`, `writeAll` |
| Variables | snake_case | `current_runtime`, `wait_queue` |
| Files exporting a type | PascalCase.zig | `Mutex.zig`, `Coroutine.zig` |
| Namespace files | snake_case.zig | `lib.zig`, `time.zig`, `syscall.zig` |

## Code Principles

1. **Correctness first** — port well-known algorithms (Vyukov MPMC, parking_lot
   mutex, libdill structured concurrency) rather than invent
2. **Comments explain WHY, not WHAT** — non-obvious platform behavior, ordering
   constraints
3. **Atomic ops need ordering justification** — document why each ordering was
   chosen
4. **No raw pointers across yield points** — coroutine may resume on different
   worker thread
5. **Stackful means stack contents preserved across suspension** — but heap
   pointers stashed on the coroutine's stack live as long as the coroutine
6. **Explicit allocators** — Zig idiom; coroutines take or share allocators
   from their parent scope

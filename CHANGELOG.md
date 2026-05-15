# Changelog

All notable changes to Volt are documented here.

## Unreleased

Volt was rewritten on a stackful coroutine substrate after the
Future/Poll machine and an earlier stackful follow-on both proved
architecturally limited. `src/` is the current tree; earlier trees
are preserved only via git tags (`pre-stackful-pivot`,
`v1.0.0-zig0.15.2`, `v1.1.0-zig0.15.2`). The decision and the POC
numbers that drove the rewrite are recorded in `spike/SYNTHESIS.md`;
the per-area design docs are under `docs/internals/`.

### Added

- Stackful coroutine runtime with AAPCS64 context switch (`src/context_arm64.zig`).
- M:N work-stealing scheduler — N OS threads, per-P fixed-256 WSQ,
  LIFO slot, per-P mailbox (`src/runtime.zig`, `src/p.zig`,
  `src/work_steal_queue.zig`).
- Direct handoff in `Task.join` when the joinee is in the same M's
  LIFO slot — skips the park/unpark round trip for the common
  spawn-then-await pattern.
- Parking lot (sharded buckets, validator-under-lock) backing all
  sync primitives (`src/park.zig`).
- Parker built on `__ulock_wait` (Darwin) / `futex` (Linux planned)
  — `std.Thread.Mutex` / `Condition` are gone in Zig 0.16, so the
  runtime provides its own.
- Sync primitives — `Mutex`, `Notify`, `Semaphore` on the parking lot.
- `Spsc(T, cap)` channel — comptime-specialised SPSC ring.
- kqueue reactor for Darwin (single-poller claim, non-blocking sockets).
- TCP networking on Darwin — `TcpListener`, `TcpStream`, `Address`.
- Typed `Task(T)` handle with `join()`.
- Stack guard pages — overflow now SIGSEGVs instead of corrupting heap.
  (See note below — the heap-alloc + per-spawn mprotect variant
  regressed multi-worker and was reverted; redesigned mmap-once-pool
  landing tracked.)
- Bench harness with Go side-by-side comparison (`bench/go/`).
- Stress test at `zig build stress` — 45 s mixed-primitive harness,
  parity-checks ~140 M ops on Darwin arm64.

### Performance (Darwin arm64, ReleaseFast, vs Go 1.26.0)

Where Volt wins:

| Workload | Volt | Go | Ratio |
|---|---|---|---|
| yield (one-way ctx switch) | 9 ns | 42 ns | 4.7× faster |
| Spsc send+recv (cap=16) | 12 ns | 33 ns | 2.8× faster |
| spawn+join workers=1 | 106 ns | 137 ns | 1.3× faster |
| TCP echo 64×16×1 KB | ~7,000 ns | 9,050 ns | 1.3× faster |
| fan-out workers=11 (real work) | 117 ns | 106 ns | 1.10× — parity |
| parallel-compute (8 workers) | 6.62× speedup | n/a | near-ideal |

Where Volt is behind:

| Workload | Volt | Go | Ratio |
|---|---|---|---|
| spawn+join workers=11 (synthetic) | 490 ns | 172 ns | 2.84× behind |
| Mutex contended workers=NumCPU | ~640 ns | 81 ns | ~8× behind |

The multi-worker spawn+join gap is concentrated on synthetic
spawn-heavy patterns; real-work multi-worker shapes are at parity or
better. Mutex contention is tracked separately. See `BENCHMARKS.md`
and `docs/internals/multi-worker-profile.md`.

### Removed

The Future/Poll runtime in its entirety. Specifically:

- `Future`, `Poll`, manual state machines.
- Linux backends (epoll, io_uring) and Windows (IOCP) — to be
  re-landed on the stackful substrate in libraries, not core.
- File I/O (`File`, `AsyncFile`, `MappedFile`), DNS, UDP,
  Unix sockets, processes (`Command`, `Child`), signals, buffered
  readers/writers, timers (`sleep`/`interval`/`timeout`/`deadline`).
- Combinators (`joinAll`, `tryJoinAll`, `race`, `select`),
  cancellation, broadcast/watch/oneshot/MPMC channels.
- The earlier race-correctness + cancellation-contract ship work —
  superseded by the rewrite.

These belong in libraries on top of Volt, not the core runtime. The
core stays small.

### Known gaps

| | Status |
|---|---|
| Darwin arm64 kqueue | Working — primary dev platform |
| Linux x86_64 / arm64 | Not yet — epoll backend planned |
| Windows | Not yet — IOCP backend planned |
| Cancellation | Not implemented; design retired with v1 |
| File I/O / DNS / TLS | Library territory |
| Mutex throughput | Real but slow on contended micro-bench |
| Stack guard pages | Reverted; redesigned mmap-once-pool landing tracked |

### Phase landings still open

- mmap-backed stack slab with mprotect-once guard pages.
- Mutex redesign closing the 8× Go gap.
- `Mpmc(T, cap)` channel.

See `docs/internals/phase-4-postmortem.md` for the guard-page design
trail and `docs/internals/multi-worker-profile.md` for the scheduler
investigation.

---

## v1.1.0-zig0.15.2 (legacy — Future/Poll runtime)

### Added

- **Memory-mapped files** (`MappedFile`, `mmapFile`, `mmapHandle`) — zero-copy file access via `mmap`. Supports read-only (`MAP_PRIVATE`) and read-write (`MAP_SHARED`) protection, sequential/random/populate hints, `sync()` (`msync`), `advise()` (`madvise`), and `unmap()`.
- **Advisory file hints** (`File.advise`, `File.adviseRange`) — tell the kernel about expected access patterns. Uses `posix_fadvise` on Linux and `fcntl(F_RDAHEAD)` on macOS.
- **`fs.fileSize`** — get file size in bytes without opening a handle.
- **`File.readFull`** — fill a buffer as much as possible, returning the byte count instead of erroring on EOF. Useful for streaming parsers that process partial chunks.
- Cooperative budgeting (128 polls/tick), matching Tokio's model.
- `JoinHandle` waker fix for correct task resumption.
- Concurrent `race`/`select` combinators for task coordination.
- io_uring `close` support on Linux.
- Integration test suite (25 async tests).
- Apache 2.0 license.

### Fixed

- Scheduler shutdown reliability on slow machines.
- Notify integration tests wait for waiters before firing.
- Work stealing test determinism.
- Windows compatibility: replaced `yield` with `sleep` in busy-wait loops.
- Test suite thread oversubscription when running all suites.

### CI

- Added `timeout-minutes` to all CI and nightly jobs.
- Integration tests added to CI and nightly workflows.
- macOS Intel runner for nightly builds.

## v1.0.0-zig0.15.2 (legacy — Future/Poll runtime)

Initial release. High-performance async I/O runtime for Zig 0.15.2 with:

- Work-stealing scheduler (Tokio-style, O(1) worker waking via bitmap)
- Future/Poll model with manual state machines
- Sync primitives: Mutex, RwLock, Semaphore, Barrier, Notify, OnceCell, CancelToken
- Channels: bounded MPMC (Vyukov lock-free), Oneshot, Broadcast, Watch
- Task spawning with JoinHandle (join, cancel, detach)
- Combinators: joinAll, tryJoinAll, race, select
- TCP, UDP, Unix socket networking
- Filesystem: sync File, async AsyncFile (io_uring on Linux, blocking pool elsewhere)
- Process management: Command builder, Child process
- Timers: sleep, interval, timeout, deadline
- Signal handling and graceful shutdown
- Buffered I/O: BufReader, BufWriter, line iterator
- 588+ unit tests, 83 concurrency tests, 35+ robustness tests

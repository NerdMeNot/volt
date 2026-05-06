# Changelog

All notable changes to Volt are documented in this file.

## Unreleased — v1.1.0-zig0.16.0 (Phase 0: risk-mitigation foundation)

Phase 0 of the v1.1 cycle. No new features — this release locks
contracts and seals leaks before the deep net/fs/mmap/streaming work
in P1–P4. The repositioning **"Volt is the async standard library
for Zig — runtime + net + fs + mmap"** starts here.

### Breaking — public error surface

Public types in `volt.io.{io,wait,net}` and `volt.fs` no longer leak
`syscall.*Error` unions. They are re-typed against the new
`volt.io.errors.IoError` master closed set and per-operation subsets
(`ReadError`, `WriteError`, `ConnectError`, `AcceptError`, `BindError`,
`ListenError`, `ShutdownError`, `SocketError`, `SendError`, `RecvError`,
`OpenError`, `StatError`, `FcntlError`, `GetSockOptError`, `SyncError`,
`SeekError`, `WaitError`).

The taxonomy is platform-neutral: kqueue's `EventNotFound` and
epoll's `EpollCtlFailed` / `TimerfdSettimeFailed` collapse to the
single `error.WaitRegistrationFailed` so the public surface no longer
reveals which reactor backend is active.

`syscall.zig` standardises on `error.AccessDenied` everywhere it used
to emit `error.PermissionDenied` — the duplicate name is gone.

**Migration:** every error tag a v1.0 user could `catch |err| switch
(err)` against still exists in the new sub-sets (verified by
`src/test/error_taxonomy_test.zig`). The breakage is the concrete
*type*, not the names — `error.BrokenPipe`, `error.ConnectionRefused`,
`error.WouldBlock`, etc. all survive. No compat shim ships; no
external consumers exist yet.

### Added

- `volt.io.errors` namespace, `volt.io.IoError`, `volt.io.fromErrno`
  (errno → IoError translator).
- `src/test/error_taxonomy_test.zig` — verifies `fromErrno` covers
  every errno of interest and contains a historical-name compile-fence
  that refuses to merge any change deleting a v1.0-era error name.
- `bench/bench_io_baseline.zig` — pipe-throughput baseline for
  `volt.io.lowlevel.read`. P1's BufReader-via-trait benchmark must
  land within 10% of this number to merge. Wired as
  `zig build bench-io-baseline`.

### Contracts locked (stub only — implementations in P3 / P4)

- `volt.fs.Mmap` — full API surface (`mapFile`, `anonymous`, `slice`,
  `advise`, **`prefault`**, `lock`, `unlock`, `flush`, `protect`,
  `remap`, `deinit`) with `@compileError` bodies. The page-fault
  contract (Risk #3) and `remap`-returns-new-slice contract (Risk #4)
  are documented in the file header and embedded in the type
  signature respectively.
- `volt.fs.Walker` — `WalkOptions { max_depth = 4096, follow_symlinks,
  skip_hidden }`, `Walker.next` / `Walker.skipSubtree` / `Walker.deinit`,
  `error.MaxDepthExceeded`. Risk #5 mitigation is in the API shape —
  bodies fill in P3.

### Risks status

| Risk | Mitigation | Status |
|---|---|---|
| #1 Error taxonomy break | Volt-owned `IoError` + sub-sets + name preservation | **Mitigated** in this release |
| #2 Vtable cost on hot reads | Baseline bench + 10% gate documented | **Gate landed**; trait-side bench in P1 |
| #3 Mmap page faults | `prefault` API + honest doc header | **Contract locked**; impl in P4 |
| #4 `remap` address move | Signature returns new slice | **Contract locked**; impl in P4 |
| #5 Walker stack depth | `max_depth` cap + `skipSubtree` | **Contract locked**; impl in P3 |

## v1.0.0-zig0.16.0

First release on the **stackful coroutine** architecture. The previous
v1.0.0-zig0.15.2 / v1.1.0-zig0.15.2 entries below were a different
runtime — Future/Poll state machines — preserved at git tag
`pre-stackful-pivot`. This release is a ground-up rewrite.

### Architecture

- Stackful coroutines: each task owns a growable virtual stack
  (1 page committed, grows in-place via `mprotect` /
  `VirtualAlloc(MEM_COMMIT)` on guard-page hit; 8 MiB reservation
  cap surfaced as `error.StackOverflow`). No compiler stackmaps.
- Multi-worker work-stealing scheduler: per-worker Chase-Lev deque,
  LIFO slot, global injection queue, EV_USER reactor wakeup.
- Park-based primitives: `Coroutine.current_park` makes every
  suspension cancellable from anywhere — cancel propagates into
  sleep / I/O / channel / sync waits and surfaces `error.Cancelled`
  promptly.
- Slab-pooled stacks: `Done.subscribe` returns the stack to a per-
  runtime pool on coroutine completion (cap=256, with miss/hit
  counters in `RuntimeMetrics`).

### Public surface

- Bootstrap: `volt.run(allocator, fn, args)`.
- Spawning: `volt.launch` (fire-and-forget → `*Job`), `volt.spawn`
  (value-returning → `*Task(T)`), `volt.spawnBlocking` (off-loop
  thread pool).
- Channels: `Channel`, `Oneshot`, `Watch`, `Broadcast`, `select`.
- Sync: `Mutex`, `RwLock`, `Semaphore`, `Notify`, `Barrier`,
  `OnceCell`.
- Structured concurrency: `volt.scope`, `Scope`, `JoinSet`,
  `CancellationToken`.
- Time: `volt.sleep`, `volt.withTimeout`, `Interval`.
- I/O: `volt.io.TcpListener`, `TcpStream`, `Address`, `read`,
  `write`, `writeAll`.
- Filesystem: `volt.fs` (read/write/open).
- Process: `volt.process.Command` (fork/execve/waitpid).
- Signals: `volt.signal.shutdown`, `ctrlC`.
- Observability: `volt.observability.snapshot`, `count`, `metrics`;
  `volt.tracing.span` with OTel-shaped JSON sink.
- Streams: `volt.stream.Stream` (async iterator + operators).

### Platform support

| Target                        | Backend       | Status                        |
|-------------------------------|---------------|-------------------------------|
| macOS arm64 / x86_64          | kqueue        | runtime + CI                  |
| Linux x86_64 / arm64          | epoll         | runtime + CI                  |
| Linux x86_64 / arm64          | io_uring      | parallel backend; cross-comp. |
| Windows x86_64 / arm64 (IOCP) | reactor_iocp  | cross-compile only — see      |
|                               |               | `src/io/reactor.zig`          |

### Notes / known limitations

- `volt.select` over multiple channels is currently lossy on
  simultaneous publish (forwarder-based v1 design); lossless
  arm/disarm select is on the v1.x plan.
- Async preemption asm path is present (M8 watchdog scaffolding) but
  not wired in by default — the SIGUSR1 / context-restore path SEGVs
  in tight CPU loops we couldn't isolate without a kernel debugger.
  Cooperative preemption (yield at every park / I/O / channel point)
  is the v1.0 default; cooperative-only matches Go pre-1.14 and
  exceeds `may`.
- Windows runtime support: IOCP backend, VirtualAlloc stacks,
  WaitOnAddress futex, QueryPerformanceCounter time, and Sleep are
  all in place. Remaining: ioctlsocket / WriteFile / CreateProcess
  arms in `io/net.zig`, `io/io.zig`, `process/Command.zig`, plus a
  Windows CI runner.

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

# Changelog

All notable changes to Volt are documented in this file.

## v1.1.0-zig0.15.2

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

## v1.0.0-zig0.15.2

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

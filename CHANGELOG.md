# Changelog

All notable changes to Volt are documented in this file.

## Unreleased — v1.1.0-zig0.16.0

The repositioning of Volt from "stackful coroutine runtime" to
**"the async standard library for Zig — runtime + net + fs + mmap"**.
Phase 0 was risk mitigation; Phase 1 the I/O trait surface and
adapters; Phase 2 the networking depth.

### Phase 4 — memory mapping (`volt.fs.Mmap`)

The user-flagged high-priority slot from the original conversation.
P0 locked the API contract; P4 fills the bodies.

- **`Mmap.mapFile(file, opts)`** — file-backed mmap. Dups the fd
  internally so the caller's `File` can close independently. fstat
  for length when `opts.len` is null. `MapOptions` carries
  `mode` / `perms` / `len` / `populate` / `huge_pages` / `locked` /
  `offset`.
- **`Mmap.anonymous(len, opts)`** — `MAP_ANON` with no fd. Useful
  for huge scratch buffers without going through the heap allocator.
- **`Mmap.advise` / `lock` / `unlock` / `flush` / `protect`** —
  one-call libc wrappers (madvise / mlock / munlock / msync /
  mprotect). All take a `Range { offset, length }` within the
  mapping.
- **`Mmap.prefault(range)`** — **Risk #3 mitigation**. Runs on the
  blocking pool: madvise(MADV_WILLNEED) + walk every page with
  volatile reads to force-fault each one. The calling coroutine
  parks; when it returns, the range is RAM-resident. Use before a
  hot read loop over a file-backed map.
- **`Mmap.remap(new_len)`** — **Risk #4 contract**. Linux:
  `mremap(MREMAP_MAYMOVE)`; Darwin: unmap + remap. Returns the new
  slice; signature forces callers to rebind any cached pointer.
- **Trait surface** — `Mmap.reader()` and `Mmap.readerAt()`
  populate `as_bytes` so `volt.io.copy(dst, mmap.reader())`
  takes the byte-slice fast path: writes the whole region in one
  `writeAll` then advances the cursor via `discard`. The third
  `copy()` dispatch arm (P1.C) is now wired.
- **`MapOptions.populate`** — Linux: `MAP_POPULATE` flag at mmap
  time. Darwin: emulated by walking + volatile-touching every
  page after mmap (no native equivalent).
- **Honest doc header** — the file's opening paragraph is the
  page-fault contract, exactly as locked in P0. mmap pages are
  fundamentally synchronous on first touch; `prefault` is the
  Volt tool that controls *where* the synchronous wait happens
  (blocking pool, not worker thread).

#### Deferred from P4 → v1.2

- `huge_pages` request — returns `error.HugePagesUnsupported` for
  now. Linux: needs MAP_HUGETLB + page-size encoding. Darwin:
  needs `VM_FLAGS_SUPERPAGE_SIZE_2MB` via `mach_vm_map` (not
  std.c.mmap). Both require platform-specific work; the API is
  locked.

### Phase 3 — filesystem depth + streaming (`volt.fs.*`)

- **`File`** — async file handle implementing all six `volt.io`
  traits (`Reader`, `Writer`, `Seeker`, `Closer`, `ReaderAt`,
  `WriterAt`). All blocking I/O routes through the blocking pool;
  the calling coroutine parks while the pool thread does the
  syscall. `as_fd` populated, so `volt.io.copy(socket.writer(),
  file.reader())` is positioned for kernel zero-copy in v1.2.
- **`OpenOptions`** — `{ read, write, append, create, exclusive,
  truncate, mode }` builder. POSIX `O_*` flag mapping in `toPosix`.
- **`Metadata`** — typed `fstat` result: size, mode, kind (file /
  directory / symlink / device / fifo / socket / unknown),
  atime, mtime, ctime, optional btime.
- **`Dir`** — dirfd-rooted directory handle. `cwd()` for `AT_FDCWD`
  without keeping a dirfd. `openDir` / `openFile` use `*at`-rooted
  syscalls (TOCTOU-safe). Iterator yields `DirEntry { name, kind,
  inode }`.
- **`Walker`** — recursive iterator (real implementation, replaces
  the P0 stub). Allocator-backed stack of frames so deep trees
  don't blow the OS stack. `WalkOptions { max_depth = 4096,
  follow_symlinks, skip_hidden }`. `skipSubtree()` cancels descent
  into the previously yielded directory. Symlink-loop detection
  is a v1.2 follow-up; default-false ships now.
- **`tree`** — `makeDir`, `makeDirAll`, `removeFile`, `removeDir`,
  `removeTree`, `rename`, `symlink`, `readlinkAlloc`, `copy`.
  `removeTree` walks pre-order then iterates in reverse for
  post-order removal. Path-relative ops route through the
  blocking pool. `copy` is read+write today; kernel accelerators
  (`copy_file_range`, `clonefile`/`fcopyfile`) are tracked for
  v1.2 alongside the underlying syscall wrappers.
- **`path`** — re-exports of `std.fs.path` (join, dirname,
  basename, extension, isAbsolute) under `volt.fs.path` so users
  don't cross namespaces.
- **`temp`** — `createTemp` / `mkdtemp` with random alphanumeric
  suffix (~5e18 unique paths, time XOR pid seeded — collision-
  resistant for temp files, not cryptographic). `TempDir.deinit()`
  removes the tree.
- **`fs.zig` facade** — `readFile` / `writeFile` rewritten on top
  of `File` (no longer bypassing via raw syscalls). Pre-allocates
  by size, loops on partial reads.
- Integration tests for File round-trip + positional I/O + trait
  composition; Dir.iterate + Walker (incl. skipSubtree); tree ops
  round-trip; rename + copy + symlink + readlinkAlloc.

#### Deferred from P3 → v1.2 / later

- **Kernel zero-copy fills** in `volt.io.copy` (`sendfile` Darwin /
  Linux, `splice` Linux, `copy_file_range` Linux,
  `clonefile`/`fcopyfile` Darwin). The dispatch shape is in
  place (P1.C); the platform-specific syscall wrappers and
  fallback handling are a focused workstream of their own.
- **`statx` integration** for `Metadata.btime` on Linux — today
  btime is null on systems where it's not free.
- **Symlink-loop detection** in `Walker` when
  `follow_symlinks = true`. The contract is locked; the inode
  tracking is the v1.2 fill.

### Phase 2 — networking depth (`volt.net.*`)

- `src/io/net.zig` promoted to its own folder `src/net/`. Tokio shape:
  `volt.io` is the abstract trait surface, `volt.net` is one of the
  concrete libraries built on it. `volt.io.{TcpListener,TcpStream,
  Address}` remain as deprecated aliases for one minor cycle, removed
  in v1.2.
- **`Address`** — RFC 4291 IPv6 parser. Handles zero-compression
  (`::`, `::1`, `fe80::1`, `2001:db8::1`), full eight-group form,
  rejects multi-`::`, oversized groups, RFC 4291 violations.
  IPv4-mapped (`::ffff:1.2.3.4`) and scope IDs (`fe80::1%en0`)
  documented as deferred.
- **`TcpStream`** — `shutdown(.read|.write|.both)` half-close;
  `readv`/`writev` vectored I/O; convenience setters for
  `setNoDelay`, `setKeepAlive[Params]`, `setLinger`, recv/send
  buf sizes.
- **`sockopt`** — typed setters reusable across stream types
  (TcpStream, UdpSocket, Unix sockets).
- **`UdpSocket`** — `bind`/`connect`/`sendTo`/`recvFrom`/`send`/`recv`,
  `joinMulticast`/`leaveMulticast`, `setMulticastTtl`,
  `setMulticastLoopback`, `setBroadcast`. IPv4 + IPv6 multicast.
- **Unix sockets** — `UnixAddress`, `UnixListener`, `UnixStream`,
  `UnixDatagram`. Trait surface (`reader`/`writer`/`closer` with
  `as_fd`) on `UnixStream`. SCM_RIGHTS fd-passing deferred to v1.2
  (needs `sendmsg`/`recvmsg` syscall wrappers and platform cmsg
  layout dispatch).
- **`dns`** — `lookupHost(allocator, name, port) ![]Address` and
  `lookupHostFirst`. Honest about the architecture: `getaddrinfo`
  on the blocking pool, not async DNS. Tokio, Trio, .NET all do
  the same — universally available system resolver is blocking-only.
- Integration tests for UDP loopback, Unix stream/datagram loopback,
  DNS numeric + localhost resolution.

### Phase 1 — I/O trait surface (`volt.io.*`)

The keystone — without it, every later type would reinvent
`read`/`write`/`close` and nothing would compose.

- **6 traits** under `src/io/traits/` — `Reader`, `Writer`,
  `Seeker`, `Closer`, `ReaderAt`, `WriterAt`. Vtable-based, blocking-
  shaped (Go's `io.Reader` model) since stackful Volt suspends
  transparently — no poll-based dance needed.
- **Sentinel markers** — `as_fd` (kernel-level zero-copy hook) and
  `as_bytes` (memory-direct hook) are nullable function pointers
  on the Reader/Writer vtables. Vtable size capped ≤ 64 bytes via
  `comptime` assertion. Marker-policy contract documented in
  `traits.zig` — hard cap until v2.0; future hooks need a tagged-
  union redesign.
- **6 adapters** under `src/io/adapters/` — `BufReader` (with
  `readUntil`, `readUntilAlloc`, `peek`), `BufWriter`, `LimitReader`,
  `TeeReader` (best-effort mirror, capture errors via
  `mirrorError()`), `lineIterator`, `chunked`.
- **`copy(dst, src)`** in `src/io/copy.zig` — comptime+runtime
  dispatch shape with `as_fd` / `as_bytes` fast-path queries. Kernel
  fast-paths (sendfile/splice/copy_file_range) deferred to P3 where
  `fs.File` provides the canonical use case.
- `IoError` gains `EndOfStream` and `StreamTooLong`.
- `volt.io.Fd` — generic non-blocking-fd-as-trait wrapper for FFI
  and arbitrary-fd cases.
- `TcpStream` retrofitted onto traits (`reader`/`writer`/`closer`
  with `as_fd` populated).
- Bench gate: `bench/bench_io_traits.zig` measures BufReader-via-
  trait pipe throughput. ≤10% overhead vs.
  `bench/bench_io_baseline.zig` is the merge gate (gate enforcement
  pending Darwin throughput stabilisation; both benches inherit a
  preexisting kqueue ping-pong flakiness under load).

### Phase 0 — risk-mitigation foundation

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

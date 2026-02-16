# CLAUDE.md - Volt

## What is Volt?

High-performance async I/O runtime for Zig, modeled after Tokio. Provides work-stealing scheduler, async sync primitives, channels, networking, filesystem, process management, timers, and signal handling. Designed to complement [Blitz](https://github.com/srini-code/blitz) (CPU parallelism) the same way Tokio complements Rayon.

## Build

**Zig 0.15.2** required (minimum 0.15.0).

```sh
zig build              # Build library
zig build test         # Unit tests (588+ tests)
zig build test-concurrency  # Loom-style concurrency tests (83 tests)
zig build test-robustness   # Edge case tests (35+ tests)
zig build test-stress       # Stress tests with real threads
zig build test-all          # All of the above
zig build bench             # Benchmarks
zig build compare           # Volt vs Tokio comparison
zig build docs              # Generate documentation
```

## Zig 0.15.x API Notes

- `std.atomic.Value(T)` for atomics (not `std.atomic.Atomic`)
- `std.Thread.Futex` for futex operations
- `std.posix` for POSIX syscalls (not `std.os`)
- No `@fence` builtin -- use `fetchAdd(0, .seq_cst)` for fences
- No async/await -- manual state machines (Future/Poll pattern)
- `std.ArrayList(T)` uses `.empty` init + pass allocator to methods
- `std.AutoHashMap(K, V)` uses `.init(allocator)` + stores allocator internally

## Git Conventions

- **No Co-Authored-By** lines in commit messages
- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `bench:`, `refactor:`
- Keep messages concise

## Project Structure

```
src/
├── lib.zig              # Public API entry point
├── Io.zig               # Explicit runtime handle (like Allocator for async)
├── runtime.zig          # Runtime lifecycle (init, run, shutdown)
├── shutdown.zig         # Graceful shutdown (Shutdown, WorkGuard)
├── signal.zig           # Signal handling (AsyncSignal)
├── stream.zig           # Reader/Writer traits
├── async.zig            # Async operations namespace
│
├── sync/                # Sync primitives (Tokio-style, zero-alloc waiters)
│   ├── Mutex.zig        # Async mutex with LockFuture
│   ├── RwLock.zig       # Read-write lock
│   ├── Semaphore.zig    # Counting semaphore
│   ├── Barrier.zig      # Barrier synchronization
│   ├── Notify.zig       # Task notification
│   ├── OnceCell.zig     # Lazy initialization
│   ├── CancelToken.zig  # Cancellation tokens
│   └── SelectContext.zig # Select coordination
│
├── channel/             # Message passing
│   ├── Channel.zig      # Bounded MPMC (Vyukov lock-free ring buffer)
│   ├── Oneshot.zig      # Single-value channel
│   ├── Broadcast.zig    # Broadcast channel
│   ├── Watch.zig        # Watch channel (single-value, latest wins)
│   └── select.zig       # Select across channels
│
├── task/                # Task spawning and coordination
│   ├── spawn.zig        # spawn, spawnBlocking
│   ├── JoinHandle.zig   # Task handle (join, cancel, detach)
│   ├── combinators.zig  # joinAll, tryJoinAll, race, select
│   └── sleep.zig        # Async sleep
│
├── future/              # Future/Poll model
│   ├── Future.zig       # Core Future trait
│   ├── Poll.zig         # Poll result type
│   ├── Waker.zig        # Waker for rescheduling
│   ├── Compose.zig      # Fluent combinator API
│   ├── MapFuture.zig    # .map() combinator
│   ├── AndThenFuture.zig # .andThen() combinator
│   └── FnFuture.zig     # Function-to-Future wrapper
│
├── net/                 # Networking
│   ├── address.zig      # Address resolution
│   ├── tcp.zig          # TCP namespace
│   ├── tcp/             # TcpListener, TcpStream, TcpSocket, split
│   ├── udp.zig          # UdpSocket
│   └── unix/            # UnixStream, UnixListener, UnixDatagram
│
├── fs/                  # Filesystem
│   ├── file.zig         # File handle (read, write, seek, sync)
│   ├── async_file.zig   # Async file operations
│   ├── dir.zig          # Directory iteration (readDir)
│   ├── ops.zig          # Convenience (readFile, writeFile, rename, copy)
│   ├── metadata.zig     # File metadata
│   └── open_options.zig # File open configuration
│
├── process/             # Subprocess management
│   ├── command.zig      # Command builder (new, arg, env, stdio)
│   └── child.zig        # Child process (wait, kill, stdin/stdout/stderr)
│
├── time/                # Timers
│   ├── driver.zig       # Timer driver
│   ├── sleep.zig        # sleep, sleepUntil
│   ├── interval.zig     # Repeating interval
│   └── timeout.zig      # Timeout combinator, Deadline
│
├── io/                  # Stream utilities
│   ├── buf_reader.zig   # BufReader (buffered reads)
│   ├── buf_writer.zig   # BufWriter (buffered writes)
│   ├── lines.zig        # Line iterator
│   ├── copy.zig         # Stream copy utilities
│   └── util.zig         # FixedBufferReader, CountingWriter, etc.
│
└── internal/            # Runtime internals (not public API)
    ├── scheduler/       # Work-stealing scheduler (Tokio-style)
    │   ├── Scheduler.zig, Runtime.zig, Header.zig
    │   ├── Deque.zig    # Chase-Lev work-stealing deque
    │   └── TimerWheel.zig
    ├── backend/         # Platform I/O backends
    │   ├── kqueue.zig, epoll.zig, io_uring.zig, iocp.zig, poll.zig
    │   └── scheduled_io.zig  # Per-resource state machine
    ├── blocking.zig     # Blocking thread pool
    ├── executor.zig     # Task execution
    ├── io_driver.zig    # I/O driver integration
    ├── util/            # Slab, intrusive list, cacheline, bit packing
    └── test/            # Test utilities (atomic_log, concurrency model)

tests/
├── common/              # Shared test utilities, model, assertions
├── concurrency/         # Loom-style exhaustive concurrency tests
├── robustness/          # Edge case tests
└── stress/              # Real-thread stress tests

bench/
├── volt_bench.zig       # Main benchmark suite (JSON output, B/op tracking)
├── compare.zig          # Volt vs Tokio comparison table
├── async_bench.zig      # Async integration benchmarks
├── internal_bench.zig   # Low-level primitive benchmarks
└── rust_bench/          # Tokio benchmarks (Rust, for comparison)

docs/                    # Starlight documentation site
├── src/content/docs/    # All documentation pages
│   ├── cookbook/         # 15 practical recipes
│   ├── usage/           # API usage guides
│   ├── api/             # API reference
│   ├── algorithms/      # Algorithm deep-dives
│   ├── design/          # Design documents
│   ├── internals/       # Architecture internals
│   └── testing/         # Test and benchmark guides
└── sidebar.mjs          # Sidebar configuration

```

## Key Architecture Patterns

- **Tokio-style state machine**: Single 64-bit packed atomic with CAS transitions (Header.zig)
- **Zero-allocation waiters**: Waiter structs embedded in LockFuture/AcquireFuture (no heap alloc per wait)
- **Lock-free MPMC channel**: Vyukov ring buffer with per-slot sequence numbers
- **Work-stealing scheduler**: LIFO slot + local FIFO queue + global injection queue
- **O(1) worker waking**: Bitmap with `@ctz` for finding idle workers
- **Cooperative budgeting**: 128 polls per tick (matches Tokio)
- **Future/Poll model**: Manual state machines since Zig 0.15 has no async/await
- **Explicit Io parameter**: Runtime handle (`Io`) passed explicitly like `Allocator` -- no thread-locals, no hidden global state

## Naming Conventions

| Kind | Convention | Example |
|------|-----------|---------|
| Types | PascalCase | `Mutex`, `TcpStream`, `JoinHandle` |
| Functions | camelCase | `tryLock`, `tryAccept`, `writeAll` |
| Variables | snake_case | `current_runtime`, `wait_queue` |
| Files exporting a type | PascalCase.zig | `Mutex.zig`, `Channel.zig` |
| Namespace files | snake_case.zig | `sync.zig`, `lib.zig`, `tcp.zig` |

## Code Principles

1. **Correctness first** -- port Tokio's battle-tested logic, study edge cases before implementing
2. **Comments explain WHY, not WHAT** -- non-obvious platform behavior, ordering constraints
3. **Atomic ops need ordering justification** -- document why each ordering was chosen
4. **Lock ordering documented** -- prevent deadlocks in multi-lock paths
5. **No raw pointers across yield points** -- task may resume on different worker thread

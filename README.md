<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/src/assets/logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/src/assets/logo-light.png">
    <img alt="Volt" src="docs/src/assets/logo-dark.png" height="80">
  </picture>
</p>

<p align="center">
  <strong>Async I/O runtime for Zig. Work-stealing scheduler, zero-alloc sync primitives, lock-free channels.</strong>
</p>

<p align="center">
  <a href="https://github.com/NerdMeNot/volt/actions/workflows/ci.yml"><img src="https://github.com/NerdMeNot/volt/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://volt.nerdmenot.in"><img src="https://img.shields.io/badge/docs-volt.nerdmenot.in-blue" alt="Docs"></a>
</p>

Volt is an async I/O runtime for Zig. It provides a work-stealing task scheduler, synchronization primitives, channels, networking, filesystem, timers, and process management -- everything you need to build concurrent servers and services.

Volt follows the architecture of [Tokio](https://tokio.rs/), adapted for Zig's value semantics and comptime specialization. It is the I/O counterpart to [Blitz](https://github.com/NerdMeNot/blitz) (CPU parallelism), the same way Tokio complements Rayon.

- **Work-stealing scheduler**: LIFO slot + local ring buffer + global injection, cooperative budgeting (128 polls/tick), O(1) bitmap worker waking
- **Cross-platform I/O**: io_uring (Linux), kqueue (macOS), IOCP (Windows), epoll (fallback) -- auto-detected at startup
- **Zero-allocation primitives**: Mutex, RwLock, Semaphore, Barrier, Notify, OnceCell -- intrusive waiters embedded in futures, no heap per wait
- **Lock-free channels**: MPSC/MPMC (Vyukov ring buffer), Oneshot, Broadcast, Watch, Select -- 284 B/op total vs 1,868 B/op (Tokio) across all benchmarks

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(serve);
}

fn serve(io: volt.Io) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();
    while (listener.tryAccept() catch null) |conn| {
        _ = io.spawn(echo, .{conn.stream}) catch continue;
    }
}

fn echo(stream: volt.net.TcpStream) void {
    var s = stream;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.tryRead(&buf) catch return orelse continue;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [Modules](#modules)
  - [Task Spawning](#task-spawning)
  - [Synchronization](#synchronization)
  - [Channels](#channels)
  - [Networking](#networking)
  - [Time](#time)
- [Performance](#performance)
- [Architecture](#architecture)
- [Best Practices](#best-practices)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [Documentation](#documentation)
- [Acknowledgments](#acknowledgments)

## Installation

### Requirements

- Zig 0.15.0 or later
- Linux, macOS, or Windows

### Using Zig Package Manager

Add to your `build.zig.zon`:

```zig
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.15.0",

    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/tags/v1.0.0-zig0.15.2.tar.gz",
            .hash = "volt-1.0.0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            // Run `zig build` to get the correct hash
        },
    },

    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
    },
}
```

Add to your `build.zig`:

```zig
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Get Volt dependency
    const volt_dep = b.dependency("volt", .{
        .target = target,
        .optimize = optimize,
    });

    // Create your executable
    const exe = b.addExecutable(.{
        .name = "my_app",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add Volt module
    exe.root_module.addImport("volt", volt_dep.module("volt"));

    b.installArtifact(exe);
}
```

### Building from Source

```bash
git clone https://github.com/NerdMeNot/volt.git
cd volt
zig build              # Build library
zig build test         # Run unit tests (588+ tests)
zig build test-stress  # Run stress tests
zig build test-all     # Run all test suites
```

## Quick Start

### TCP Echo Server

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(serve);
}

fn serve(io: volt.Io) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    while (listener.tryAccept() catch null) |result| {
        _ = io.spawn(handleClient, .{result.stream}) catch continue;
    }
}

fn handleClient(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.tryRead(&buf) catch return orelse continue;
        if (n == 0) return;
        stream.writeAll(buf[0..n]) catch return;
    }
}
```

### Concurrent Tasks

```zig
const volt = @import("volt");

pub fn main() !void {
    try volt.run(fetchAll);
}

fn fetchAll(io: volt.Io) !void {
    // Spawn concurrent tasks
    const user_h = try io.spawn(fetchUser, .{42});
    const posts_h = try io.spawn(fetchPosts, .{42});

    // Wait for both
    const user, const posts = try io.joinAll(.{ user_h, posts_h });
    _ = user;
    _ = posts;
}
```

### Graceful Shutdown

```zig
const volt = @import("volt");

pub fn main() !void {
    var shutdown = try volt.shutdown.Shutdown.init();
    defer shutdown.deinit();

    var listener = try volt.net.listen("0.0.0.0:8080");
    defer listener.close();

    while (!shutdown.isShutdown()) {
        if (listener.tryAccept() catch null) |result| {
            var work = shutdown.startWork();
            defer work.deinit();
            handleConnection(result.stream);
        }
    }

    shutdown.waitForPending();
}
```

## API Reference

### Modules

| Module | Description |
|--------|-------------|
| `volt.Io` | Runtime handle passed to tasks that need to spawn (Tier 2 API) |
| `volt.sync` | Synchronization: Mutex, RwLock, Semaphore, Notify, Barrier, OnceCell |
| `volt.channel` | Message passing: Channel, Oneshot, Broadcast, Watch, Select |
| `volt.net` | Networking: TCP, UDP, Unix sockets, DNS resolution |
| `volt.fs` | Filesystem operations |
| `volt.stream` | I/O streams: Reader, Writer, buffered I/O |
| `volt.time` | Duration, Instant, Sleep, Timeout, Interval |
| `volt.signal` | Signal handling (SIGINT, SIGTERM) |
| `volt.process` | Process spawning and piped I/O |
| `volt.shutdown` | Graceful shutdown coordination |
| `volt.async_ops` | Future combinators: Timeout, Select, Join, Race |
| `volt.future` | Low-level future types: Poll, Waker, Context |

### Task Spawning

These functions require a `volt.Io` handle (received as a parameter by functions passed to `volt.run()`).

| Function | Description |
|----------|-------------|
| `io.spawn(fn, args)` | Spawn async task, returns `JoinHandle` |
| `io.spawnFuture(future)` | Spawn a Future directly |
| `io.spawnBlocking(fn, args)` | Run on dedicated blocking pool thread |
| `io.sleep(duration)` | Async-aware sleep |
| `io.yield()` | Yield to scheduler |
| `io.joinAll(handles)` | Wait for all tasks, return results tuple |
| `io.tryJoinAll(handles)` | Wait for all, collect results and errors |
| `io.race(handles)` | First to complete wins, cancel others |
| `io.select(handles)` | First to complete, keep others running |

### Synchronization

All primitives are zero-allocation and require no `deinit()`. Each provides two tiers: `tryX()` (non-blocking, returns immediately) and `x()` (returns a Future for the scheduler).

| Primitive | Use Case | API |
|-----------|----------|-----|
| `Mutex` | Protect shared state | `tryLock()`, `lock()` -> Future |
| `RwLock` | Many readers, one writer | `tryReadLock()`, `readLock()` / `writeLock()` |
| `Semaphore` | Limit concurrent access | `tryAcquire(n)`, `acquire(n)` -> Future |
| `Notify` | Task wake-up signal | `notifyOne()`, `notifyAll()`, `wait()` |
| `Barrier` | Wait for N tasks | `wait()` -> leader/follower result |
| `OnceCell` | Lazy one-time init | `get()`, `getOrInit(fn)` |

### Channels

| Channel | Pattern | Allocation | Description |
|---------|---------|------------|-------------|
| `Channel(T)` | MPSC / MPMC | Heap (bounded) | Bounded work queue with backpressure |
| `Oneshot(T)` | 1:1 | None | Single-value delivery |
| `BroadcastChannel(T)` | 1:N | Heap (bounded) | All receivers get all messages |
| `Watch(T)` | 1:N | None | Single value with change notification |

Each channel provides `trySend()`/`tryRecv()` (non-blocking) and `send()`/`recv()` (Future-based).

```zig
// Bounded channel
var ch = try volt.channel.bounded(Task, allocator, 100);
defer ch.deinit();

switch (ch.trySend(task)) {
    .ok => {},
    .full => {},     // backpressure
    .closed => {},   // receiver dropped
}

// Oneshot
var os = volt.channel.oneshot(Result);
os.sender.send(computeResult());
if (os.receiver.tryRecv()) |result| { ... }

// Watch
var config = volt.channel.watch(Config, default_config);
config.send(new_config);
```

### Networking

| Type | Description |
|------|-------------|
| `TcpListener` | Accept incoming TCP connections |
| `TcpStream` | Bidirectional TCP connection |
| `TcpSocket` | Socket builder for custom configuration |
| `UdpSocket` | Connectionless datagram socket |
| `UnixStream` | Unix domain stream socket |
| `UnixListener` | Unix domain socket listener |
| `UnixDatagram` | Unix domain datagram socket |
| `Address` | IPv4/IPv6 socket address |

Convenience functions: `volt.net.listen(addr)`, `volt.net.connect(addr)`, `volt.net.listenPort(port)`, `volt.net.resolve(host, port)`, `volt.net.connectHost(host, port)`.

### Time

| Type / Function | Description |
|-----------------|-------------|
| `Duration` | Span of time (nanosecond precision) |
| `Instant` | Point in time (monotonic clock) |
| `Sleep` | Async-aware sleep future |
| `Interval` | Recurring timer |
| `Deadline` | Timeout tracking |

```zig
const dur = volt.time.Duration.fromSecs(5);
const start = volt.time.Instant.now();
io.sleep(dur);  // requires volt.Io handle
const elapsed = start.elapsed();
```

## Performance

Benchmarks on Apple M3 Pro, comparing Volt to Tokio (Rust). All measurements: median of 10 iterations, 5 warmup discarded, 4 worker threads. Run `zig build compare` to reproduce.

### Synchronization

| Benchmark | Volt | Tokio | Winner |
|-----------|------|-------|--------|
| Mutex (uncontended) | 31.8 ns | 28.2 ns | Tokio +1.1x |
| RwLock read (uncontended) | 25.3 ns | 27.1 ns | Volt +1.1x |
| RwLock write (uncontended) | 20.7 ns | 25.2 ns | Volt +1.2x |
| Semaphore (uncontended) | 22.7 ns | 33.7 ns | Volt +1.5x |
| Mutex (4 tasks) | 91.7 ns | 207.9 ns | Volt +2.3x |
| RwLock (4R + 2W) | 149.5 ns | 247.4 ns | Volt +1.7x |
| Semaphore (8T, 2 permits) | 139.4 ns | 323.0 ns | Volt +2.3x |

### Channels

| Benchmark | Volt | Tokio | Winner |
|-----------|------|-------|--------|
| Channel send | 11.1 ns | 16.3 ns | Volt +1.5x |
| Channel recv | 11.4 ns | 22.3 ns | Volt +2.0x |
| Channel roundtrip | 23.1 ns | 37.8 ns | Volt +1.6x |
| MPMC (4P + 4C) | 73.3 ns | 132.8 ns | Volt +1.8x |
| Oneshot | 27.1 ns | 51.5 ns | Volt +1.9x |
| Broadcast (4 recv) | 95.2 ns | 143.8 ns | Volt +1.5x |
| Watch | 45.7 ns | 145.4 ns | Volt +3.2x |

### Coordination and Scheduling

| Benchmark | Volt | Tokio | Winner |
|-----------|------|-------|--------|
| OnceCell get | 2.0 ns | 1.0 ns | Tokio +2.0x |
| OnceCell set | 41.8 ns | 90.8 ns | Volt +2.2x |
| Barrier | 50.9 ns | 1,312.8 ns | Volt +25.8x |
| Notify | 15.6 ns | 18.9 ns | Volt +1.2x |
| Spawn + await | 29,597 ns | 18,946 ns | Tokio +1.6x |
| Spawn batch (per task) | 601.0 ns | 611.4 ns | Tie |
| Blocking spawn | 25,037 ns | 12,483 ns | Tokio +2.0x |

### Summary

**Volt wins 16/21 benchmarks, Tokio wins 4/21, 1 tie.**

Memory: Volt **284 B/op** total vs Tokio's **1,868 B/op** -- 6.6x less allocation overhead. See [BENCHMARKS.md](BENCHMARKS.md) for full analysis.

## Architecture

```
+-----------------------------------------------------------------+
|                       User Application                          |
|        volt.run(fn) / Io.init() + io.run()                      |
+-----------------------------------------------------------------+
                              |
+-----------------------------------------------------------------+
|                        Public API                               |
|  volt.Io      volt.sync    volt.channel   volt.net   volt.fs   |
|  volt.time    volt.signal  volt.process   volt.stream           |
|  volt.shutdown             volt.async_ops                       |
+-----------------------------------------------------------------+
                              |
+-----------------------------------------------------------------+
|                     Runtime Core                                |
|  +------------------+  +---------------+  +------------------+  |
|  |    Scheduler     |  |   I/O Driver  |  |   Timer Wheel    |  |
|  |                  |  |               |  |                  |  |
|  | - LIFO slot      |  | - Submit ops  |  | - Hierarchical   |  |
|  | - Local queue    |  | - Poll events |  | - Tick-based     |  |
|  |   (256-slot ring)|  | - Complete    |  | - Cancel support |  |
|  | - Global inject  |  |               |  |                  |  |
|  | - Work stealing  |  |               |  |                  |  |
|  | - Coop budget    |  |               |  |                  |  |
|  |   (128 polls)    |  |               |  |                  |  |
|  +------------------+  +---------------+  +------------------+  |
|                              |                                  |
|  +----------------------------------------------------------+  |
|  |                   Blocking Pool                           |  |
|  |  - Dedicated threads for CPU-heavy / blocking work        |  |
|  |  - Auto-scaling up to 512 threads                         |  |
|  |  - 10s idle timeout                                       |  |
|  +----------------------------------------------------------+  |
+-----------------------------------------------------------------+
                              |
+-----------------------------------------------------------------+
|                    Platform Backend                              |
|  +----------+  +----------+  +----------+  +----------+        |
|  | io_uring |  |  kqueue  |  |   IOCP   |  |  epoll   |        |
|  | (Linux)  |  | (macOS)  |  | (Windows)|  |(fallback)|        |
|  +----------+  +----------+  +----------+  +----------+        |
+-----------------------------------------------------------------+
```

### Key Design Decisions

1. **Stackless futures**: Each task is a state machine at ~256-512 bytes, not a coroutine stack (16-64 KB). Predictable memory, cache-friendly layout, millions of concurrent tasks. Same tradeoff Tokio made.

2. **Tokio-style state machine**: Single 64-bit packed atomic with CAS transitions for task state. The `notified` bit is cleared atomically in `transitionToIdle`, returning previous state to detect missed wakeups.

3. **Work-stealing scheduler**: LIFO slot for hot-path locality, local FIFO ring buffer (256 slots), global injection queue with mutex. O(1) worker waking via bitmap with `@ctz`.

4. **Cooperative budgeting**: 128 polls per tick (matching Tokio). Prevents a single task from starving others.

5. **Zero-allocation waiters**: Waiter structs are embedded directly in `LockFuture`/`AcquireFuture` types. No heap allocation per contended wait.

6. **Two-tier API**: `tryX()` methods (non-blocking) work without a runtime. `x(io)` methods (async, Future-based) require a `volt.Io` handle from `volt.run()`. The type system enforces this at compile time.

## Best Practices

### Do

```zig
// DO: Use volt.run() for simple programs
pub fn main() !void {
    try volt.run(myApp);
}

// DO: Wrap operations with timeouts
var timeout_future = volt.async_ops.Timeout(MyFuture).init(
    my_future,
    volt.time.Duration.fromSecs(5),
);

// DO: Use spawnBlocking for CPU-intensive work (requires volt.Io)
const hash = try io.spawnBlocking(computeExpensiveHash, .{data});

// DO: Use tryX() when you can handle failure immediately (no Io needed)
if (mutex.tryLock()) {
    defer mutex.unlock();
    // critical section
}

// DO: Track in-flight work for graceful shutdown
var work = shutdown.startWork();
defer work.deinit();
handleRequest(conn);

// DO: Use bounded channels for backpressure
var ch = try volt.channel.bounded(Job, allocator, 1000);
```

### Don't

```zig
// DON'T: Block the I/O thread with CPU-heavy work
fn handleRequest(io: volt.Io, conn: volt.net.TcpStream) void {
    // BAD: This blocks a scheduler worker thread
    const hash = expensiveSha256(huge_payload);
    // GOOD: Offload to blocking pool
    const hash = try io.spawnBlocking(expensiveSha256, .{huge_payload});
}

// DON'T: Use OS blocking primitives inside async tasks
fn badTask(io: volt.Io) void {
    std.Thread.sleep(1_000_000_000);  // BAD: blocks worker thread
    io.sleep(volt.time.Duration.fromSecs(1));  // GOOD: yields to scheduler
}

// DON'T: Forget to close resources
fn leaky() void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    // BAD: listener never closed!
    // GOOD: defer listener.close();
}

// DON'T: Ignore channel backpressure
switch (ch.trySend(item)) {
    .ok => {},
    .full => {},     // BAD: silently dropping work
    .closed => {},
}
```

## Limitations

### Current Limitations

1. **No coroutine/async-await syntax**: Zig 0.15.x does not have async/await. All async operations use manual state machines and the Future/Poll interface. This will improve when Zig adds language-level async support.

2. **No TLS/SSL**: Volt provides raw TCP/UDP/Unix sockets. TLS must be handled by a separate library layered on top.

3. **DNS resolution is blocking**: `volt.net.resolve()` and `volt.net.connectHost()` use blocking OS calls. Use `io.spawnBlocking()` to avoid stalling I/O workers.

4. **No HTTP protocol**: Volt is a runtime, not a web framework. HTTP servers/clients should be built on top of `volt.net`.

5. **Single runtime per process**: The runtime uses thread-local state. Running multiple `Runtime` instances concurrently is not supported.

6. **Task scheduling overhead**: Tokio's spawn + await (1.6x) and blocking pool spawn (2.0x) are faster due to years of `RawTask` optimization and thread pool tuning respectively.

### Platform-Specific Notes

- **Linux**: Full support. io_uring (kernel 5.1+) preferred, epoll fallback for older kernels.
- **macOS**: Full support via kqueue. Tested on both x86_64 and Apple Silicon.
- **Windows**: IOCP backend. Requires Windows 10+.

## Documentation

- **Website**: [volt.nerdmenot.in](https://volt.nerdmenot.in) -- guides, API reference, cookbook, architecture internals
- **Benchmarks**: [BENCHMARKS.md](BENCHMARKS.md) -- full Volt vs Tokio comparison with analysis
- **Tests**: `zig build test-all` runs all test suites (588+ unit, 83 concurrency, 35+ robustness)

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide. Here's the short version:

1. Fork and create a branch from `main`
2. Make your changes following the [code style guidelines](CONTRIBUTING.md#code-style)
3. Run the tests:
   ```bash
   zig build test              # Unit tests (588+)
   zig build test-concurrency  # Loom-style concurrency tests (83)
   zig build test-all          # Everything
   ```
4. Commit with [Conventional Commits](https://www.conventionalcommits.org/) style (`feat:`, `fix:`, `docs:`, etc.)
5. Open a pull request with a clear description

For concurrency or lock-free contributions, add tests in `tests/concurrency/` that exercise interleavings systematically. See [CONTRIBUTING.md](CONTRIBUTING.md) for details on test filtering, intensity configuration, and bug reporting.

## License

Apache 2.0. See [LICENSE](LICENSE).

## Acknowledgments

Volt would not exist without the work of the teams and individuals behind these projects:

- [Tokio](https://github.com/tokio-rs/tokio) -- Volt's architecture is inspired by Tokio's. The work-stealing scheduler, the sync primitive designs, cooperative budgeting, and the task state protocol were all informed by studying Tokio's source code and documentation. Where Volt is fast, it's because Tokio's team figured out the right architecture first and we applied it in a language with different tradeoffs. Where Tokio is faster (task scheduling, blocking pool), it's because of years of production tuning we haven't done yet. Thank you to the Tokio team for building the runtime that showed us how async I/O should work.
- [Mio](https://github.com/tokio-rs/mio) -- Platform I/O abstraction that informed our backend implementations.
- [Crossbeam](https://github.com/crossbeam-rs/crossbeam) -- Lock-free channel designs and the epoch-based memory reclamation patterns.
- [parking_lot](https://github.com/Amanieu/parking_lot) -- Adaptive locking strategies that inspired our contention handling.
- [Blitz](https://github.com/NerdMeNot/blitz) -- Sibling project for CPU-bound parallelism. Volt handles I/O; Blitz handles compute.
- The Zig community for building a language that makes systems programming accessible without sacrificing control.

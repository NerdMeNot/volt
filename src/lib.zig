//! # Volt — Stackful coroutine runtime for Zig
//!
//! Code reads like blocking I/O — no `async`/`await`, no Future state
//! machines. Each task owns a growable virtual stack; the scheduler
//! suspends at every I/O / channel / sleep / sync wait point and
//! resumes on whichever worker the reactor wakes first.
//!
//! ## Bootstrap
//!
//! ```zig
//! pub fn main() !void {
//!     try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
//! }
//! ```
//!
//! ## Public surface (highlights)
//!
//! - `volt.run`, `volt.launch`, `volt.spawn`, `volt.spawnBlocking`,
//!   `volt.yield` — bootstrap and spawning.
//! - `volt.scope`, `volt.Scope`, `volt.JoinSet`, `volt.CancellationToken`
//!   — structured concurrency.
//! - `volt.sleep`, `volt.withTimeout`, `volt.Interval` — time.
//! - `volt.channel.{Channel, Oneshot, Watch, Broadcast}`, `volt.select`
//!   — message passing.
//! - `volt.sync.{Mutex, RwLock, Semaphore, Notify, Barrier, OnceCell}`
//!   — synchronization.
//! - `volt.io.{TcpListener, TcpStream, Address}`, `volt.fs`,
//!   `volt.process`, `volt.signal` — I/O surface.
//!
//! See `CHANGELOG.md` for status notes and platform support.

const std = @import("std");

// ─────────────────────────────────────────────────────────────────────────────
// Public API — the user-facing surface
// ─────────────────────────────────────────────────────────────────────────────

pub const run = @import("api/run.zig").run;
pub const RunError = @import("api/run.zig").RunError;
pub const launch = @import("api/launch.zig").launch;
pub const destroyJob = @import("api/launch.zig").destroyJob;
pub const spawn = @import("api/spawn.zig").spawn;
pub const destroyTask = @import("api/spawn.zig").destroyTask;
pub const yield = @import("api/yield.zig").yield;
pub const spawnBlocking = @import("api/spawn_blocking.zig").spawnBlocking;

pub const Runtime = @import("runtime.zig").Runtime;
pub const Config = @import("runtime.zig").Config;
pub const Job = @import("task/job.zig").Job;
pub const Task = @import("task/task.zig").Task;

/// The currently-active runtime, if any. Returns null if not inside `volt.run`.
pub const currentRuntime = @import("runtime.zig").currentRuntime;

// ─────────────────────────────────────────────────────────────────────────────
// Time primitives — model-agnostic types
// ─────────────────────────────────────────────────────────────────────────────

pub const time = @import("time.zig");
pub const Duration = time.Duration;
pub const Instant = time.Instant;
pub const sleep = @import("time/Sleep.zig").sleep;
pub const withTimeout = @import("time/Timeout.zig").withTimeout;
pub const Interval = @import("time/Interval.zig").Interval;

pub const stream = @import("stream/Stream.zig");
pub const Stream = stream.Stream;

pub const fs = struct {
    pub const readFile = @import("fs/fs.zig").readFile;
    pub const writeFile = @import("fs/fs.zig").writeFile;

    /// P0 contract artifacts — type signatures for P3 (`Walker`) and
    /// P4 (`Mmap`) are locked here so feature work can't renegotiate
    /// them. Calling any method `@compileError`s with a pointer to the
    /// implementing phase. See the file headers for the contracts.
    pub const Mmap = @import("fs/Mmap.zig").Mmap;
    pub const MmapError = @import("fs/Mmap.zig").MmapError;
    pub const MapOptions = @import("fs/Mmap.zig").MapOptions;
    pub const AnonOptions = @import("fs/Mmap.zig").AnonOptions;
    pub const Walker = @import("fs/Walker.zig").Walker;
    pub const WalkOptions = @import("fs/Walker.zig").WalkOptions;
    pub const WalkError = @import("fs/Walker.zig").WalkError;
};

pub const process = struct {
    const cmd_mod = @import("process/Command.zig");
    pub const Command = cmd_mod.Command;
    pub const Output = cmd_mod.Output;
    pub const Term = cmd_mod.Term;
};

pub const testing = struct {
    pub const assertNoLeaks = @import("testing/leak.zig").assertNoLeaks;
};

pub const observability = struct {
    pub const snapshot = @import("observability/snapshot.zig").snapshot;
    pub const count = @import("observability/snapshot.zig").count;
    pub const liveCount = @import("observability/snapshot.zig").liveCount;
    pub const TaskSnapshot = @import("observability/snapshot.zig").TaskSnapshot;
    pub const TaskState = @import("observability/snapshot.zig").TaskState;
    pub const metrics = @import("observability/metrics.zig").collect;
    pub const RuntimeMetrics = @import("observability/metrics.zig").RuntimeMetrics;
    pub const WorkerMetrics = @import("observability/metrics.zig").WorkerMetrics;
};

pub const tracing = struct {
    pub const span = @import("observability/tracing.zig").span;
    pub const SpanOptions = @import("observability/tracing.zig").SpanOptions;
    pub const setSink = @import("observability/tracing.zig").setSink;
};

pub const signal = struct {
    const sd = @import("signal/Shutdown.zig");
    pub const Signal = sd.Signal;
    pub const SignalSet = sd.SignalSet;
    pub const SignalListener = sd.SignalListener;
    pub const shutdown = sd.shutdown;
    pub const ctrlC = sd.ctrlC;
};

// ─────────────────────────────────────────────────────────────────────────────
// Internal building blocks — exposed for benchmarks/tests, not for users
// ─────────────────────────────────────────────────────────────────────────────

pub const coroutine = struct {
    pub const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
    pub const ClosureBase = @import("coroutine/coroutine.zig").ClosureBase;
    pub const EventSource = @import("coroutine/event_source.zig").EventSource;
    pub const event_source = @import("coroutine/event_source.zig");
    pub const stack = @import("coroutine/stack.zig");
    pub const stack_pool = @import("coroutine/stack_pool.zig");
    pub const context = @import("coroutine/context.zig");
    pub const spawn_helper = @import("coroutine/spawn.zig");

    /// Annotate the *currently-running* coroutine with a name. Equivalent
    /// to `Job.setName` but reads the active coroutine from TLS — only
    /// callable from inside that coroutine itself.
    ///
    /// Prefer `Job.setName` / `Task.setName` when you have a handle.
    pub fn setCurrentName(name: []const u8) void {
        const current = @import("scheduler/current.zig");
        if (current.currentCoroutine()) |coro| coro.name = name;
    }

    /// Annotate the *currently-running* coroutine with a spawn-site
    /// source location (typically `@src()`). Equivalent to
    /// `Job.setSpawnSite` but reads the active coroutine from TLS —
    /// only callable from inside that coroutine itself.
    pub fn setCurrentSpawnSite(src: std.builtin.SourceLocation) void {
        const current = @import("scheduler/current.zig");
        if (current.currentCoroutine()) |coro| coro.spawn_site = src;
    }
};

pub const scheduler = struct {
    pub const current = @import("scheduler/current.zig");
    pub const Worker = @import("scheduler/worker.zig").Worker;
    pub const Injection = @import("scheduler/injection.zig").Injection;
    pub const park = @import("scheduler/park.zig");
    pub const Deque = @import("scheduler/deque.zig").Deque;
};

pub const channel = struct {
    pub const Channel = @import("channel/Channel.zig").Channel;
    pub const Oneshot = @import("channel/Oneshot.zig").Oneshot;
    pub const Watch = @import("channel/Watch.zig").Watch;
    pub const Broadcast = @import("channel/Broadcast.zig").Broadcast;
    pub const select = @import("channel/select.zig").select;
    pub const OnRecv = @import("channel/select.zig").OnRecv;

    /// Unified error vocabulary — every channel-family operation uses
    /// these so a single `catch` can cover the whole module.
    pub const SendError = @import("channel/errors.zig").SendError;
    pub const RecvError = @import("channel/errors.zig").RecvError;
};

/// Synchronization primitives — locks, semaphores, condition-variables,
/// barriers, lazy init. Held under `volt.sync.*` so the top-level
/// namespace stays focused on structured concurrency and the rest of
/// the user-facing surface.
pub const sync = struct {
    pub const Mutex = @import("sync/Mutex.zig").Mutex;
    pub const Semaphore = @import("sync/Semaphore.zig").Semaphore;
    pub const Notify = @import("sync/Notify.zig").Notify;
    pub const RwLock = @import("sync/RwLock.zig").RwLock;
    pub const Barrier = @import("sync/Barrier.zig").Barrier;
    pub const OnceCell = @import("sync/OnceCell.zig").OnceCell;
};

// ─────────────────────────────────────────────────────────────────────────────
// Structured concurrency — top-level because it's the headline feature.
// (Trio nurseries / Kotlin coroutineScope analogue.)
// ─────────────────────────────────────────────────────────────────────────────

pub const scope = @import("sync/Scope.zig").scope;
pub const Scope = @import("sync/Scope.zig").Scope;
pub const JoinSet = @import("sync/JoinSet.zig").JoinSet;
pub const CancellationToken = @import("sync/CancellationToken.zig").CancellationToken;

/// `volt.select(.{ .a = OnRecv(T){...}, ... })` — wait for the first of
/// several channel operations to complete. Park-based; cancels the
/// losing branches when one wins.
pub const select = @import("channel/select.zig").select;

pub const io = struct {
    pub const TcpListener = @import("io/net.zig").TcpListener;
    pub const TcpStream = @import("io/net.zig").TcpStream;
    pub const Address = @import("io/net.zig").Address;

    /// v1.1 error taxonomy. `IoError` is the master closed set; the
    /// per-operation aliases below (`ReadError`, `WriteError`, …) are
    /// curated subsets. Replaces the leaky `syscall.*Error` unions
    /// from v1.0; see CHANGELOG for migration notes.
    pub const errors = @import("io/errors.zig");
    pub const IoError = errors.IoError;
    pub const fromErrno = errors.fromErrno;

    /// v1.1 trait surface — vtable-based byte-level Reader / Writer /
    /// Seeker / Closer / ReaderAt / WriterAt. Stackful Volt suspends
    /// transparently at I/O sites, so these are blocking-shaped (Go's
    /// `io.Reader` model) rather than poll-based. Adapters (BufReader,
    /// copy, …) compose over the traits without knowing the concrete
    /// source/sink type.
    pub const traits = @import("io/traits/traits.zig");
    pub const Reader = traits.Reader;
    pub const Writer = traits.Writer;
    pub const Seeker = traits.Seeker;
    pub const Closer = traits.Closer;
    pub const ReaderAt = traits.ReaderAt;
    pub const WriterAt = traits.WriterAt;

    /// Composable trait adapters. `BufReader` / `BufWriter` add
    /// buffering. `LimitReader` caps total bytes (frame parsing).
    /// `TeeReader` mirrors reads to a side writer (hashing, audit).
    /// `lineIterator` and `chunked` are streaming iterators over
    /// a `Reader`.
    pub const BufReader = @import("io/adapters/BufReader.zig").BufReader;
    pub const BufWriter = @import("io/adapters/BufWriter.zig").BufWriter;
    pub const LimitReader = @import("io/adapters/LimitReader.zig").LimitReader;
    pub const TeeReader = @import("io/adapters/TeeReader.zig").TeeReader;
    pub const lineIterator = @import("io/adapters/lines.zig").lineIterator;
    pub const LineIterator = @import("io/adapters/lines.zig").LineIterator;
    pub const chunked = @import("io/adapters/chunked.zig").chunked;
    pub const ChunkIterator = @import("io/adapters/chunked.zig").ChunkIterator;

    /// Reactor types — exposed for users who want to register raw fds
    /// (e.g., custom OS resources, FFI integrations). Most code should
    /// reach for `TcpStream` / `TcpListener` instead.
    pub const Reactor = @import("io/reactor.zig").Reactor;
    pub const EventKind = @import("io/reactor.zig").EventKind;

    /// Low-level fd-taking primitives. These are the building blocks
    /// behind `TcpStream.read`/`writeAll`/etc. — exposed for FFI and
    /// custom-fd integrations. Most application code should not use
    /// them directly; reach for the typed wrappers in `volt.io.Tcp*`.
    pub const lowlevel = struct {
        pub const read = @import("io/io.zig").read;
        pub const write = @import("io/io.zig").write;
        pub const writeAll = @import("io/io.zig").writeAll;
        pub const setNonblock = @import("io/io.zig").setNonblock;
        pub const waitReadable = @import("io/wait.zig").waitReadable;
        pub const waitWritable = @import("io/wait.zig").waitWritable;
    };
};

pub const internal = struct {
    pub const thread = @import("internal/thread.zig");
    pub const syscall = @import("internal/syscall.zig");
    pub const util = struct {
        pub const linked_list = @import("internal/util/linked_list.zig");
        pub const slab = @import("internal/util/slab.zig");
        pub const pool = @import("internal/util/pool.zig");
        pub const stack_guard = @import("internal/util/stack_guard.zig");
        pub const cacheline = @import("internal/util/cacheline.zig");
        pub const bit = @import("internal/util/bit.zig");
        pub const invocation_id = @import("internal/util/invocation_id.zig");
        pub const signal = @import("internal/util/signal.zig");
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Version
// ─────────────────────────────────────────────────────────────────────────────

pub const version = struct {
    pub const major = 1;
    pub const minor = 0;
    pub const patch = 0;
    pub const string = "1.0.0-zig0.16.0";
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pull in inline tests from every module so `zig build test` covers
// the whole tree.
// ─────────────────────────────────────────────────────────────────────────────

test {
    _ = time;
    _ = coroutine.Coroutine;
    _ = coroutine.stack;
    _ = coroutine.stack_pool;
    _ = coroutine.context;
    _ = coroutine.spawn_helper;
    _ = coroutine.event_source;
    _ = scheduler.current;
    _ = scheduler.Worker;
    _ = scheduler.Injection;
    _ = scheduler.park;
    _ = @import("scheduler/deque.zig");
    _ = @import("scheduler/injection.zig");
    _ = io.Reactor;
    // Cross-compile-only: ensure the alternative io_uring backend
    // builds cleanly on Linux. Not loaded into the dispatcher.
    if (comptime @import("builtin").os.tag == .linux) {
        _ = @import("io/reactor_iouring.zig").Reactor;
    }
    // Cross-compile-only: ensure the IOCP backend builds cleanly on
    // Windows. Not loaded into the dispatcher (Windows isn't a v1.0
    // default — see reactor.zig dispatch table).
    if (comptime @import("builtin").os.tag == .windows) {
        _ = @import("io/reactor_iocp.zig").Reactor;
    }
    _ = @import("io/wait.zig");
    _ = @import("io/io.zig");
    _ = @import("io/net.zig");
    _ = @import("io/traits/traits.zig");
    _ = @import("io/adapters/BufReader.zig");
    _ = @import("io/adapters/BufWriter.zig");
    _ = @import("io/adapters/LimitReader.zig");
    _ = @import("io/adapters/TeeReader.zig");
    _ = @import("io/adapters/lines.zig");
    _ = @import("io/adapters/chunked.zig");
    _ = Runtime;
    _ = Job;
    _ = @import("task/task.zig");
    _ = @import("api/yield.zig");
    _ = @import("api/launch.zig");
    _ = @import("api/spawn.zig");
    _ = @import("api/run.zig");
    _ = @import("test/integration_test.zig");
    _ = @import("test/io_integration_test.zig");
    _ = @import("test/tcp_integration_test.zig");
    _ = @import("test/multi_worker_test.zig");
    _ = @import("test/scheduler_stress_test.zig");
    _ = @import("test/park_stress_test.zig");
    _ = @import("test/scheduler_fuzz_test.zig");
    _ = @import("test/error_taxonomy_test.zig");
    _ = @import("channel/Channel.zig");
    _ = @import("channel/Oneshot.zig");
    _ = @import("channel/Watch.zig");
    _ = @import("channel/Broadcast.zig");
    _ = @import("channel/select.zig");
    _ = @import("sync/Mutex.zig");
    _ = @import("sync/Semaphore.zig");
    _ = @import("sync/Notify.zig");
    _ = @import("sync/RwLock.zig");
    _ = @import("sync/Barrier.zig");
    _ = @import("sync/OnceCell.zig");
    _ = @import("sync/Scope.zig");
    _ = @import("sync/JoinSet.zig");
    _ = @import("sync/CancellationToken.zig");
    _ = @import("time/Sleep.zig");
    _ = @import("time/Timeout.zig");
    _ = @import("time/Interval.zig");
    _ = @import("stream/Stream.zig");
    _ = @import("blocking/Pool.zig");
    _ = @import("api/spawn_blocking.zig");
    _ = @import("fs/fs.zig");
    _ = @import("process/Command.zig");
    _ = @import("observability/snapshot.zig");
    _ = @import("observability/metrics.zig");
    _ = @import("observability/tracing.zig");
    _ = @import("testing/leak.zig");
    _ = @import("test/channel_integration_test.zig");
    _ = @import("test/stack_overflow_test.zig");
    _ = internal.thread;
    _ = internal.syscall;
    _ = internal.util.linked_list;
    _ = internal.util.slab;
    _ = internal.util.pool;
    _ = internal.util.stack_guard;
    _ = internal.util.cacheline;
    _ = internal.util.bit;
    _ = internal.util.invocation_id;
    _ = internal.util.signal;
}

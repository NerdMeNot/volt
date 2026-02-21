//! # Volt: High-Performance Async I/O for Zig
//!
//! A production-grade async I/O runtime with:
//! - Work-stealing task scheduler
//! - Platform-optimized backends (io_uring, kqueue, epoll, IOCP)
//! - Structured concurrency primitives
//! - Graceful shutdown with signal handling
//!
//! ## Quick Start
//!
//! ```zig
//! const std = @import("std");
//! const volt = @import("volt");
//!
//! pub fn main() !void {
//!     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!
//!     var io = try volt.Io.init(gpa.allocator(), .{});
//!     defer io.deinit();
//!
//!     try io.run(server);
//! }
//!
//! fn server(io: volt.Io) !void {
//!     var f = try io.@"async"(compute, .{42});
//!     const result = f.@"await"(io);
//!     _ = result;
//! }
//! ```
//!
//! **Convenience shorthand** — zero-config, uses `page_allocator`:
//!
//! ```zig
//! pub fn main() !void {
//!     try volt.run(server);
//! }
//! ```
//!
//! ## Module Organization
//!
//! | Module | Description |
//! |--------|-------------|
//! | `volt.Io` | Runtime handle — init/deinit/run/async (like Allocator) |
//! | `volt.Future` | Task result handle — await/cancel |
//! | `volt.sync` | Synchronization: Mutex, RwLock, Semaphore, Notify, Barrier |
//! | `volt.channel` | Message passing: Channel, Oneshot, Broadcast, Watch |
//! | `volt.net` | Networking: TCP, UDP, Unix sockets, DNS |
//! | `volt.fs` | Filesystem operations |
//! | `volt.stream` | I/O streams: Reader, Writer, buffered I/O |
//! | `volt.time` | Duration, Instant, timers |
//! | `volt.signal` | Signal handling |
//! | `volt.process` | Process spawning |
//! | `volt.shutdown` | Graceful shutdown |

const std = @import("std");
const builtin = @import("builtin");
const is_windows = builtin.os.tag == .windows;

// ═══════════════════════════════════════════════════════════════════════════════
// Namespaced Modules — Primary API
// ═══════════════════════════════════════════════════════════════════════════════

/// Task spawning and concurrency (internal — use Io methods instead).
const task_internal = @import("task.zig");

/// Synchronization primitives for async tasks.
pub const sync = @import("sync.zig");

/// Message passing channels.
pub const channel = @import("channel.zig");

/// Stackless future primitives (engine internal — for custom Futures only).
pub const future = @import("future.zig");

/// I/O streams and utilities.
pub const stream = @import("stream.zig");

/// Networking (TCP, UDP, Unix sockets, DNS).
pub const net = @import("net.zig");

/// Filesystem operations (POSIX only — not yet available on Windows).
pub const fs = if (!is_windows) @import("fs.zig") else struct {};

/// Time primitives.
pub const time = @import("time.zig");

/// Signal handling.
pub const signal = @import("signal.zig");

/// Process spawning and management (POSIX only — not yet available on Windows).
pub const process = if (!is_windows) @import("process.zig") else struct {};

/// Graceful shutdown handler for async servers.
pub const shutdown = @import("shutdown.zig");

/// Async combinators (engine internal — prefer io.@"async" + Group).
pub const async_ops = @import("async.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Runtime Lifecycle
// ═══════════════════════════════════════════════════════════════════════════════

const runtime_mod = @import("runtime.zig");

/// The primary entry point for Volt.
///
/// Create with `Io.init(allocator, config)`, clean up with `io.deinit()`,
/// and run your async root function with `io.run(myApp)`.
///
/// Pass `Io` through function signatures like an `Allocator` — if a function
/// needs to spawn tasks or use async runtime features, it takes `io: volt.Io`.
///
/// Tier 1 operations (`tryLock`, `trySend`, `tryRecv`) do NOT need Io.
/// Tier 2 operations (`@"async"`, `concurrent`) do.
pub const Io = @import("Io.zig");

/// Handle to an async task's result. Obtain via `io.@"async"()`.
///
/// ```zig
/// var f = try io.@"async"(compute, .{42});
/// const result = f.@"await"(io);
/// ```
pub const Future = Io.IoFuture;

/// Structured concurrency — spawn tasks, wait for all, cancel on scope exit.
///
/// ```zig
/// var group = volt.Group.init(io);
/// _ = group.spawn(fetchUser, .{id});
/// _ = group.spawn(fetchPosts, .{id});
/// group.wait();
/// ```
pub const Group = @import("io/Group.zig").Group;

/// Runtime configuration.
pub const Config = runtime_mod.Config;

// ─────────────────────────────────────────────────────────────────────────────
// Engine internals — NOT part of the stable public API.
// Exposed for benchmarks, custom Futures, and advanced integration.
// ─────────────────────────────────────────────────────────────────────────────

/// Internal re-export for benchmark and advanced use. Prefer `volt.Io`.
pub const Runtime = runtime_mod.Runtime;

/// Internal re-export for benchmark and advanced use. Prefer `volt.Future`.
pub const JoinHandle = runtime_mod.JoinHandle;

/// Zero-config entry point — run an async function with default settings.
///
/// Uses `page_allocator` and default `Config`. For control over the
/// allocator and configuration, use `Io.init` directly:
///
/// ```zig
/// // Explicit (recommended for production):
/// var io = try volt.Io.init(allocator, .{ .num_workers = 4 });
/// defer io.deinit();
/// try io.run(myApp);
///
/// // Convenience shorthand:
/// try volt.run(myApp);
/// ```
pub fn run(comptime func: anytype) anyerror!PayloadType(@TypeOf(func)) {
    return runWith(std.heap.page_allocator, .{}, func);
}

/// Convenience entry point with custom allocator and configuration.
///
/// Equivalent to `Io.init` + `io.run` + `io.deinit`:
/// ```zig
/// try volt.runWith(allocator, .{ .num_workers = 4 }, myApp);
/// ```
pub fn runWith(
    allocator: std.mem.Allocator,
    config: Config,
    comptime func: anytype,
) anyerror!PayloadType(@TypeOf(func)) {
    var io = try Io.init(allocator, config);
    defer io.deinit();
    return io.run(func);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Common Re-exports
// ═══════════════════════════════════════════════════════════════════════════════

/// Duration of time (nanosecond precision).
pub const Duration = time.Duration;

/// Point in time (monotonic clock).
pub const Instant = time.Instant;

// ═══════════════════════════════════════════════════════════════════════════════
// Internal — For Advanced Users
// ═══════════════════════════════════════════════════════════════════════════════

/// Internal implementation details (engine internal — not part of stable API).
pub const internal = @import("internal.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Version
// ═══════════════════════════════════════════════════════════════════════════════

pub const version = struct {
    pub const major = 1;
    pub const minor = 0;
    pub const patch = 0;
    pub const string = "1.0.0";
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn PayloadType(comptime Func: type) type {
    const info = @typeInfo(Func);
    const fn_info = switch (info) {
        .pointer => |p| @typeInfo(p.child).@"fn",
        .@"fn" => info.@"fn",
        else => @compileError("expected function type"),
    };
    const raw = fn_info.return_type.?;
    return switch (@typeInfo(raw)) {
        .error_union => |eu| eu.payload,
        else => raw,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    // Run all module tests
    _ = @import("Io.zig");
    _ = @import("time.zig");
    _ = @import("internal/blocking.zig");
    _ = @import("runtime.zig");
    _ = @import("internal/backend.zig");
    _ = @import("internal/executor.zig");
    _ = @import("net.zig");
    _ = @import("stream.zig");
    _ = @import("sync.zig");
    _ = @import("channel.zig");
    _ = @import("task.zig");
    _ = @import("internal/util/signal.zig");
    _ = @import("signal.zig");
    _ = @import("async.zig");
    _ = @import("shutdown.zig");
    _ = @import("io/Future.zig");
    _ = @import("io/Group.zig");

    // POSIX-only modules (filesystem, process)
    if (!is_windows) {
        _ = @import("fs.zig");
        _ = @import("process.zig");
    }

    // Scheduler tests
    _ = @import("internal/scheduler/Header.zig");
    _ = @import("internal/scheduler/Scheduler.zig");
    _ = @import("internal/scheduler/Runtime.zig");
    _ = @import("internal/scheduler/TimerWheel.zig");

    // Future tests
    _ = @import("future.zig");

    // Utility tests
    _ = @import("internal/util/pool.zig");

    // Internal module tests
    _ = @import("internal.zig");
}

test "0 - unit test suite header" {
    std.debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Unit Tests (inline)                                         │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  sync       channel     net         fs          time         │
        \\│  task       future      stream      signal      process      │
        \\│  scheduler  backend     runtime     shutdown    internal     │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

test "version" {
    try std.testing.expect(version.major >= 0);
    try std.testing.expect(version.string.len > 0);
}

test "run function compiles" {
    _ = run;
    _ = runWith;
}

test "namespace structure" {
    // Core types
    _ = Io;
    _ = Future;
    _ = Group;
    _ = Config;

    // Sync primitives
    _ = sync.Mutex;
    _ = sync.RwLock;
    _ = sync.Semaphore;
    _ = sync.Notify;
    _ = sync.Barrier;

    // Channels
    _ = channel.Channel;
    _ = channel.Oneshot;
    _ = channel.BroadcastChannel;
    _ = channel.Watch;

    // I/O
    _ = stream.Reader;
    _ = stream.Writer;
    _ = net.TcpListener;
    _ = time.Duration;
    _ = signal.AsyncSignal;
    if (!is_windows) {
        _ = fs.File;
        _ = process.Command;
    }

    // Shutdown
    _ = shutdown.Shutdown;
    _ = shutdown.ShutdownFuture;
    _ = shutdown.WorkGuard;

    // Internal modules still compile (for engine use)
    _ = future;
    _ = async_ops;
}

test "z - unit test suite complete" {
    std.debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  All unit tests passed                                       │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

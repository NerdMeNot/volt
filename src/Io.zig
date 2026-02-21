//! The async I/O runtime handle.
//!
//! `Io` is the primary entry point for Volt. Create one explicitly with
//! `init`/`deinit` like an `Allocator`, then pass it through your function
//! signatures — if a function needs async capabilities, it takes `io: volt.Io`.
//!
//! ## Two-Tier API
//!
//! - **Tier 1** (`tryLock`, `trySend`, `tryRecv`): No `Io` needed.
//! - **Tier 2** (`@"async"`, `concurrent`): Requires `Io`.
//!
//! ## Usage
//!
//! ```zig
//! const volt = @import("volt");
//!
//! pub fn main() !void {
//!     var io = try volt.Io.init(allocator, .{});
//!     defer io.deinit();
//!     try io.run(server);
//! }
//!
//! fn server(io: volt.Io) !void {
//!     var f = try io.@"async"(compute, .{42});
//!     const result = f.@"await"(io);
//! }
//! ```
//!
//! ## Convenience Shorthand
//!
//! ```zig
//! pub fn main() !void {
//!     try volt.run(server); // uses page_allocator, default config
//! }
//! ```

const std = @import("std");
const runtime_mod = @import("runtime.zig");
const fn_future_mod = @import("future/FnFuture.zig");
const FnFuture = fn_future_mod.FnFuture;
const FnReturnType = fn_future_mod.FnReturnType;
const blocking_mod = @import("internal/blocking.zig");
const combinators = @import("task/combinators.zig");
pub const IoFuture = @import("io/Future.zig").Future;

/// Lightweight future wrapping a `*BlockingHandle(T)`.
///
/// Uses a two-phase wait strategy: spin (1024 iterations) → futex.
/// The spin catches tasks completed by a spinning blocking thread
/// (~1µs on ARM), avoiding both kernel round-trips entirely. For
/// tasks still in-flight, the futex provides efficient kernel-mediated
/// sleep with precise wakeup.
pub fn ConcurrentFuture(comptime T: type) type {
    return struct {
        handle: *blocking_mod.BlockingHandle(T),

        /// Wait for the blocking task to complete.
        /// Uses spin → futex to minimize latency.
        pub fn await(self: *@This(), _: Io) anyerror!T {
            const h = self.handle;
            const Handle = blocking_mod.BlockingHandle(T);

            // Spin long enough to overlap with a spinning blocking thread's
            // task execution (~300-500ns). 1024 iters ≈ 1µs on ARM, ~10µs
            // on x86. If the blocking thread is spinning on task_available,
            // it catches our submit almost instantly and completes within
            // this window — avoiding BOTH kernel round-trips.
            var spin: u32 = 0;
            while (spin < 1024) : (spin += 1) {
                if (h.state.load(.acquire) == Handle.COMPLETED) return h.wait();
                std.atomic.spinLoopHint();
            }

            // Futex wait — blocks until woken by Futex.wake in complete().
            return h.wait();
        }

        /// Check if the blocking task has completed (non-blocking).
        pub fn isDone(self: *const @This()) bool {
            return self.handle.isComplete();
        }
    };
}

const Io = @This();

runtime: *runtime_mod.Runtime,
/// Set when this Io owns the runtime (created via `init`).
/// `null` for non-owning handles created internally.
allocator: ?std.mem.Allocator = null,

// ═══════════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

/// Create an Io handle with its own runtime.
///
/// The runtime is heap-allocated using the provided allocator and fully
/// owned by this handle. Call `deinit()` to shut down and free it.
///
/// ```zig
/// var io = try volt.Io.init(allocator, .{ .num_workers = 4 });
/// defer io.deinit();
/// try io.run(myApp);
/// ```
pub fn init(allocator: std.mem.Allocator, config: runtime_mod.Config) !Io {
    const rt = try allocator.create(runtime_mod.Runtime);
    errdefer allocator.destroy(rt);
    rt.* = try runtime_mod.Runtime.init(allocator, config);
    return .{ .runtime = rt, .allocator = allocator };
}

/// Shut down the runtime and free resources.
///
/// For owning handles (from `init`), this deinits the runtime and frees
/// the heap allocation. For non-owning handles, this only shuts down
/// the runtime.
pub fn deinit(self: *Io) void {
    self.runtime.deinit();
    if (self.allocator) |alloc| {
        alloc.destroy(self.runtime);
    }
}

/// Run a function as the root task on the scheduler.
///
/// The function receives this `Io` handle as its first parameter,
/// providing explicit access to the runtime for spawning tasks.
/// Blocks the calling thread until the function completes.
///
/// ```zig
/// var io = try volt.Io.init(allocator, .{});
/// defer io.deinit();
/// try io.run(myApp);
///
/// fn myApp(io: volt.Io) void { ... }
/// ```
pub fn run(self: Io, comptime func: anytype) anyerror!fn_future_mod.FnPayload(@TypeOf(func)) {
    return self.runtime.runWithIo(self, func);
}

// ═══════════════════════════════════════════════════════════════════════════
// Task Spawning
// ═══════════════════════════════════════════════════════════════════════════

/// Spawn a function as a concurrent async task, returning a `Future(T)`.
///
/// This is the primary API for concurrent work. The function is scheduled
/// on the work-stealing runtime. Use `.@"await"(io)` to get the result.
///
/// ## Example
///
/// ```zig
/// var f = try io.@"async"(fetchUser, .{user_id});
/// const user = f.@"await"(io);
/// ```
pub fn async(self: Io, comptime func: anytype, args: anytype) !IoFuture(FnReturnType(@TypeOf(func))) {
    const F = FnFuture(func, @TypeOf(args));
    const handle = try self.runtime.spawn(F, F.init(args));
    return .{ ._handle = handle };
}

/// Spawn a plain function as a concurrent async task.
///
/// Lower-level API — returns a JoinHandle for direct poll/join access.
/// Prefer `io.@"async"()` for most use cases.
///
/// ## Example
///
/// ```zig
/// const handle = try io.spawn(fetchUser, .{user_id});
/// const user = handle.join();
/// ```
pub fn spawn(self: Io, comptime func: anytype, args: anytype) !runtime_mod.JoinHandle(FnReturnType(@TypeOf(func))) {
    const F = FnFuture(func, @TypeOf(args));
    return self.runtime.spawn(F, F.init(args));
}

/// Spawn an existing Future value as a concurrent async task.
///
/// Lower-level API — returns a JoinHandle for direct poll/join access.
///
/// ## Example
///
/// ```zig
/// const handle = try io.spawnFuture(mutex.lock());
/// ```
pub fn spawnFuture(self: Io, future: anytype) !runtime_mod.JoinHandle(@TypeOf(future).Output) {
    const F = @TypeOf(future);
    return self.runtime.spawn(F, future);
}

/// Spawn an existing Future value, returning an `IoFuture` handle.
///
/// Internal convenience used by sync/channel convenience methods.
pub fn awaitFuture(self: Io, future: anytype) !IoFuture(@TypeOf(future).Output) {
    const F = @TypeOf(future);
    const handle = try self.runtime.spawn(F, future);
    return .{ ._handle = handle };
}

/// Run a function on the blocking thread pool, returning a `ConcurrentFuture`.
///
/// The function executes on a dedicated blocking thread. The returned future
/// uses spin-wait + yield + futex to efficiently wait for completion.
///
/// ## Example
///
/// ```zig
/// var f = try io.concurrent(computeHash, .{data});
/// const hash = try f.await(io);
/// ```
pub fn concurrent(
    self: Io,
    comptime func: anytype,
    args: anytype,
) !ConcurrentFuture(blocking_mod.ResultType(@TypeOf(func))) {
    const handle = try self.runtime.spawnBlocking(func, args);
    return .{ .handle = handle };
}

/// Blocking variant — returns raw handle with `.wait()` (for sync callers).
///
/// Prefer `concurrent()` for async code. This method blocks the calling
/// worker thread with a futex until the blocking work completes.
pub fn concurrentBlocking(
    self: Io,
    comptime func: anytype,
    args: anytype,
) !*blocking_mod.BlockingHandle(blocking_mod.ResultType(@TypeOf(func))) {
    return self.runtime.spawnBlocking(func, args);
}

/// Alias for `concurrentBlocking` — the Tokio-equivalent name.
/// Fully supported; use whichever name fits your codebase.
pub const spawnBlocking = concurrentBlocking;

// ═══════════════════════════════════════════════════════════════════════════
// Task Coordination
// ═══════════════════════════════════════════════════════════════════════════

/// Wait for all tasks to complete. Returns tuple of results.
/// Fails on first error (remaining tasks continue running).
pub fn joinAll(_: Io, handles: anytype) combinators.JoinAllResult(@TypeOf(handles)) {
    return combinators.joinAll(handles);
}

/// Wait for all tasks, collecting both successes and errors.
pub fn tryJoinAll(_: Io, handles: anytype) combinators.TryJoinAllResult(@TypeOf(handles)) {
    return combinators.tryJoinAll(handles);
}

/// First task to complete wins. Cancels all other tasks.
pub fn race(_: Io, handles: anytype) combinators.RaceResult(@TypeOf(handles)) {
    return combinators.race(handles);
}

/// First task to complete wins. Other tasks keep running.
pub fn select(_: Io, handles: anytype) combinators.SelectResult(@TypeOf(handles)) {
    return combinators.select(handles);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Io - init and deinit lifecycle" {
    var io = try Io.init(std.testing.allocator, .{});
    // Should have an owning allocator
    std.debug.assert(io.allocator != null);
    io.deinit();
}

test "Io - init with custom config" {
    var io = try Io.init(std.testing.allocator, .{
        .num_workers = 2,
        .max_blocking_threads = 64,
    });
    defer io.deinit();
    try std.testing.expect(io.allocator != null);
}

test "Io - method signatures exist" {
    // Verify all public methods exist at comptime
    comptime {
        _ = Io.init;
        _ = Io.deinit;
        _ = Io.run;
        _ = Io.async;
        _ = Io.spawn;
        _ = Io.spawnFuture;
        _ = Io.awaitFuture;
        _ = Io.concurrent;
        _ = Io.spawnBlocking;
        _ = Io.joinAll;
        _ = Io.tryJoinAll;
        _ = Io.race;
        _ = Io.select;
    }
}

test "Io - IoFuture type is accessible" {
    comptime {
        _ = IoFuture(u32);
        _ = IoFuture(void);
        _ = IoFuture([]const u8);
    }
}

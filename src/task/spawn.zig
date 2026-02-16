//! # Task Spawning
//!
//! Ergonomic spawn API for concurrent task execution.
//!
//! Two entry points:
//! - `spawn(func, args)` — Spawn a plain function as a task (most common)
//! - `spawnFuture(future)` — Spawn an existing Future value
//!
//! ## Examples
//!
//! ```zig
//! // Spawn a plain function
//! const handle = try io.spawn(fetchUser, .{user_id});
//! const user = handle.join();
//!
//! // Spawn a Future value (e.g., from compose chain or sync primitive)
//! const handle2 = try io.spawnFuture(mutex.lock());
//! ```

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const future_mod = @import("../future.zig");
const FnFuture = @import("../future/FnFuture.zig").FnFuture;
const FnReturnType = @import("../future/FnFuture.zig").FnReturnType;

// ═══════════════════════════════════════════════════════════════════════════════
// Core Spawn Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Spawn a plain function as a concurrent async task.
///
/// The function is wrapped in a single-poll Future and scheduled on the
/// work-stealing runtime. Returns a JoinHandle to await the result.
///
/// ## Example
///
/// ```zig
/// fn add(a: i32, b: i32) i32 { return a + b; }
///
/// const handle = try spawn(add, .{ 1, 2 });
/// const result = handle.join(); // 3
/// ```
///
/// ## Panics
///
/// Panics if called outside of a volt runtime context.
pub fn spawn(comptime func: anytype, args: anytype) !runtime_mod.JoinHandle(FnReturnType(@TypeOf(func))) {
    const rt = runtime_mod.runtime();
    const F = FnFuture(func, @TypeOf(args));
    return rt.spawn(F, F.init(args));
}

/// Spawn an existing Future value as a concurrent async task.
///
/// Use this for:
/// - Futures from sync primitives: `spawnFuture(mutex.lock())`
/// - Composed futures: `spawnFuture(compose(f).map(g).withTimeout(d))`
/// - Custom poll-based futures
///
/// ## Example
///
/// ```zig
/// const handle = try spawnFuture(future.ready(@as(i32, 42)));
/// const result = try handle.join(); // 42
/// ```
///
/// ## Panics
///
/// Panics if called outside of a volt runtime context.
pub fn spawnFuture(f: anytype) !runtime_mod.JoinHandle(@TypeOf(f).Output) {
    const F = @TypeOf(f);
    const rt = runtime_mod.runtime();
    return rt.spawn(F, f);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Blocking Spawn
// ═══════════════════════════════════════════════════════════════════════════════

const blocking_mod = @import("../internal/blocking.zig");

/// Spawn a blocking function on the blocking thread pool.
///
/// Use this for CPU-intensive or blocking I/O work that would starve
/// the async I/O workers. Returns a handle that can be waited on.
///
/// ## Example
///
/// ```zig
/// const hash = try io.spawnBlocking(computeExpensiveHash, .{data});
/// const result = try hash.wait();
/// ```
///
/// ## Panics
///
/// Panics if called outside of a volt runtime context.
pub fn spawnBlocking(
    comptime func: anytype,
    args: anytype,
) !*blocking_mod.BlockingHandle(blocking_mod.ResultType(@TypeOf(func))) {
    const rt = runtime_mod.runtime();
    return rt.spawnBlocking(func, args);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "spawn function exists" {
    _ = &spawn;
}

test "spawnFuture function exists" {
    _ = &spawnFuture;
}

test "FnFuture integration - type compiles" {
    const F = FnFuture(struct {
        fn add(a: i32, b: i32) i32 {
            return a + b;
        }
    }.add, struct { i32, i32 });

    // Verify it satisfies the Future trait
    try std.testing.expect(future_mod.isFuture(F));
    try std.testing.expect(F.Output == i32);
}

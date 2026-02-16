//! # Task Module - Task Spawning and Concurrency
//!
//! This module provides the public API for spawning and managing async tasks.
//! It is the primary interface for concurrent programming in volt.
//!
//! ## Quick Reference
//!
//! Task spawning is done via `volt.Io`:
//!
//! | Method | Description |
//! |--------|-------------|
//! | `io.spawn(fn, args)` | Spawn async task, returns `JoinHandle` |
//! | `io.spawnFuture(f)` | Spawn a Future value as a task |
//! | `io.spawnBlocking(fn, args)` | CPU-intensive work on blocking pool |
//!
//! Task coordination (no runtime needed):
//!
//! | Function | Description |
//! |----------|-------------|
//! | `joinAll` | Wait for all tasks, return tuple of results |
//! | `tryJoinAll` | Wait for all, collect results and errors |
//! | `race` | First to complete wins, cancel others |
//! | `select` | First to complete, keep others running |
//!
//! ## Example
//!
//! ```zig
//! const volt = @import("volt");
//!
//! fn fetchAll(io: volt.Io, id: u64) !void {
//!     const user_h = try io.spawn(fetchUser, .{id});
//!     const posts_h = try io.spawn(fetchPosts, .{id});
//!
//!     // Wait for both
//!     const user, const posts = try volt.task.joinAll(.{user_h, posts_h});
//! }
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Re-exports from task/ directory
// ═══════════════════════════════════════════════════════════════════════════════

// JoinHandle type
const join_handle_mod = @import("task/JoinHandle.zig");
pub const JoinHandle = join_handle_mod.JoinHandle;
pub const CoroJoinHandle = join_handle_mod.CoroJoinHandle;

// Duration type (for timeout specifications)
const sleep_mod = @import("task/sleep.zig");
pub const Duration = sleep_mod.Duration;

// Multi-task coordination helpers
const helpers_mod = @import("task/combinators.zig");
pub const joinAll = helpers_mod.joinAll;
pub const tryJoinAll = helpers_mod.tryJoinAll;
pub const race = helpers_mod.race;
pub const select = helpers_mod.select;
pub const TaskResult = helpers_mod.TaskResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    // Run all sub-module tests
    _ = @import("task/spawn.zig");
    _ = @import("task/JoinHandle.zig");
    _ = @import("task/sleep.zig");
    _ = @import("task/combinators.zig");
}

test "task module exports" {
    // Verify all exports are available
    _ = JoinHandle;
    _ = Duration;
    _ = joinAll;
    _ = tryJoinAll;
    _ = race;
    _ = select;
}

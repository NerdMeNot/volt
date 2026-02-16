//! # JoinHandle - Task Result Handle
//!
//! JoinHandle is returned by `spawn` and provides methods to:
//! - `join()` - Block until complete, return result
//! - `tryJoin()` - Non-blocking check (null if not ready)
//! - `cancel()` - Cancel the task
//! - `isFinished()` - Check if task has completed
//! - `deinit()` - Detach from the task (fire-and-forget)
//!
//! ## Example
//!
//! ```zig
//! const handle = try io.spawn(compute, .{data});
//!
//! // Option 1: Block and wait
//! const result = try handle.join();
//!
//! // Option 2: Non-blocking poll
//! if (handle.tryJoin()) |result| {
//!     // Task completed
//! }
//!
//! // Option 3: Cancel
//! handle.cancel();
//!
//! // Option 4: Fire and forget
//! handle.deinit();
//! ```

const std = @import("std");
const runtime_mod = @import("../runtime.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// JoinHandle Type
// ═══════════════════════════════════════════════════════════════════════════════

/// Handle for awaiting spawned task results.
///
/// ## Methods
///
/// - `join()` - Block until complete, return result
/// - `tryJoin()` - Non-blocking check (null if not ready)
/// - `cancel()` - Cancel the task
/// - `isFinished()` - Check if task has completed
/// - `deinit()` - Detach (fire and forget)
pub fn JoinHandle(comptime T: type) type {
    return runtime_mod.JoinHandle(T);
}

/// Internal JoinHandle from the coroutine runtime.
/// For advanced users who need direct coroutine access.
pub const CoroJoinHandle = @import("../internal/scheduler/Runtime.zig").JoinHandle;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "JoinHandle type" {
    _ = JoinHandle(u32);
    _ = JoinHandle(void);
    _ = JoinHandle([]const u8);
}

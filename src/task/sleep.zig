//! # Sleep and Yield Operations
//!
//! Async sleep and yield operations for the task scheduler.
//!
//! ## Stackless Architecture
//!
//! In the stackless model, "yield" means returning Poll::Pending from
//! a future. For blocking sleep, use `std.Thread.sleep()` directly.
//!
//! ## Async Sleep
//!
//! For async sleep that integrates with the runtime, use SleepFuture.
//! This allows the runtime to efficiently schedule other tasks while
//! waiting.

const std = @import("std");
const time_mod = @import("../time.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Duration type for sleep operations.
pub const Duration = time_mod.Duration;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Duration type available" {
    _ = Duration;
}

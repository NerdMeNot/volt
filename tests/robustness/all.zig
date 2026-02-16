//! Robustness Tests - Edge cases and boundary conditions
//!
//! Run: zig build test-robustness
//!
//! Tests cover:
//! - Zero/empty/boundary values
//! - Single-element scenarios
//! - Double operations (double lock, double release, etc.)
//! - State transitions and recovery
//! - Unusual interleaving patterns

const std = @import("std");
const debug = std.debug;

// Pull in all robustness test files
comptime {
    _ = @import("edge_cases_test.zig");
}

test "0 - robustness suite header" {
    debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Robustness Tests (edge cases)                               │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Boundary conditions, unusual sequences, and error paths     │
        \\│  for all sync primitives and channel types.                  │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Mutex       Semaphore    RwLock       Barrier               │
        \\│  Notify      OnceCell     Channel      Oneshot               │
        \\│  Watch       Broadcast                                       │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

test "z - robustness suite complete" {
    debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  All robustness tests passed                                 │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

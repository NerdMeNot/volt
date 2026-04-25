//! Concurrency Tests - Loom-style systematic testing
//!
//! Run: zig build test-concurrency
//!
//! Environment:
//!   VOLT_TEST_INTENSITY=smoke|standard|stress|exhaustive

const std = @import("std");
const testing = std.testing;

const common = @import("common");
const config = common.config;

// ═══════════════════════════════════════════════════════════════════════════════
// Loom Tests
// ═══════════════════════════════════════════════════════════════════════════════

const mutex_tests = @import("mutex_loom_test.zig");
const rwlock_tests = @import("rwlock_loom_test.zig");
const semaphore_tests = @import("semaphore_loom_test.zig");
const notify_tests = @import("notify_loom_test.zig");
const barrier_tests = @import("barrier_loom_test.zig");
const oncecell_tests = @import("oncecell_loom_test.zig");
const channel_tests = @import("channel_loom_test.zig");
const oneshot_tests = @import("oneshot_loom_test.zig");
const broadcast_tests = @import("broadcast_loom_test.zig");
const watch_tests = @import("watch_loom_test.zig");
const select_tests = @import("select_loom_test.zig");

// Force test discovery
comptime {
    _ = mutex_tests;
    _ = rwlock_tests;
    _ = semaphore_tests;
    _ = notify_tests;
    _ = barrier_tests;
    _ = oncecell_tests;
    _ = channel_tests;
    _ = oneshot_tests;
    _ = broadcast_tests;
    _ = watch_tests;
    _ = select_tests;
}

// This runs first (alphabetically "0" comes before other test names)
test "0 - loom test suite header" {
    const cfg = config.autoConfig();
    const name = config.intensityName(cfg);

    std.debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Concurrency Tests (loom-style)                              │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Systematic interleaving of concurrent operations across      │
        \\│  all sync primitives and channel types.                       │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Intensity:  {s:<48}│
        \\│  Threads:    {d:<48}│
        \\│  Iterations: {d:<48}│
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{ name, cfg.effectiveThreadCount(), cfg.loom_iterations });
}

// This runs last (alphabetically "z" comes after other test names)
test "z - loom test suite complete" {
    std.debug.print(
        \\
        \\  Modules tested:
        \\    Mutex        RwLock       Semaphore    Notify
        \\    Barrier      OnceCell     Channel      Oneshot
        \\    Broadcast    Watch        Select
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  All concurrency tests passed                                │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

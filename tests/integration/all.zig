//! Async Integration Tests
//!
//! Run: zig build test-integration
//!
//! Tests the real async Future/Poll/Waker protocol through the Volt
//! scheduler under contention. Catches bugs like lost wakeups, spurious
//! wake hangs, and semaphore starvation that spin-loop stress tests miss.

const std = @import("std");
const debug = std.debug;

// Force test discovery for all integration test modules
comptime {
    _ = @import("async_mutex_test.zig");
    _ = @import("async_channel_test.zig");
    _ = @import("async_semaphore_test.zig");
    _ = @import("async_coordination_test.zig");
    _ = @import("scheduler_test.zig");
    _ = @import("combinators_test.zig");
    _ = @import("blocking_pool_test.zig");
}

test "0 - integration suite header" {
    debug.print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Async Integration Tests                                     │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Real scheduler + Future/Poll/Waker protocol under           │
        \\│  contention. Each test creates its own runtime.              │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{});
}

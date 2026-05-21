//! Volt's test allocator — leak-detecting, safe under multi-worker.
//!
//! `std.testing.allocator` wraps `DebugAllocator` with the default
//! `stack_trace_frames = 6`. That trace capture walks the live stack
//! via DWARF unwinding, which races with stack writes from other
//! workers when Volt spawns work across threads. Tests that touch
//! `Runtime` with `workers >= 2` corrupt under it.
//!
//! `volt.testing.allocator` is a `DebugAllocator` with
//! `stack_trace_frames = 0` and `thread_safe = true`. Same leak
//! detection, no unwinding race. Drop-in replacement for
//! `std.testing.allocator` in any test that constructs a multi-worker
//! Runtime.
//!
//! Usage:
//!
//! ```zig
//! const volt = @import("volt");
//! const allocator = volt.testing.allocator;
//!
//! test "spawn cleanup" {
//!     var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = 2 });
//!     defer rt.deinit();
//!     // ...
//! }
//! ```
//!
//! Leaks are detected at process exit via std's test runner
//! (`instance.detectLeaks()`); for finer-grained reporting, tests
//! can call `volt.testing.instance.detectLeaks()` explicitly.

const std = @import("std");

/// Module-global DebugAllocator instance. Same lifecycle as
/// `std.testing.allocator_instance` — initialised at module load,
/// leak-checked at process exit by the test runner.
pub var instance: std.heap.DebugAllocator(.{
    .stack_trace_frames = 0,
    .thread_safe = true,
    .safety = true,
}) = .init;

/// Leak-detecting `std.mem.Allocator` that's safe across multi-worker
/// Volt runtimes. See module docstring for why this exists.
pub const allocator: std.mem.Allocator = instance.allocator();

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

// ─────────────────────────────────────────────────────────────────────
// Async test helpers
// ─────────────────────────────────────────────────────────────────────
//
// `volt.testing.expect(budget, body, args)` and
// `volt.testing.spawnRace(c, tasks, .expected_winner)` make async
// tests pleasant. Both wrap existing public primitives (`withTimeout`,
// `joinFirst`) with assertion-friendly return types — instead of
// returning `error.Timeout` or some opaque "wrong winner" payload,
// they fail the test directly via `error.TestExceededBudget` /
// `error.TestUnexpectedWinner`.

const lib = @import("lib.zig");

/// Run `body(args..., *Cancel)` with a budget. Fails the test if the
/// body exceeds `budget`. Returns the body's result if it completed
/// in time.
///
/// Drop-in replacement for `volt.withTimeout` in tests where a
/// timeout is a *failure*, not a normal outcome.
///
/// Must be called from inside a coroutine.
pub fn expect(
    budget: lib.Duration,
    comptime body: anytype,
    args: anytype,
) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
    const result = lib.withTimeout(budget, body, args);
    if (result) |v| return v else |e| {
        // Translate Timeout into the test-flavored name. Other body
        // errors propagate as-is so the test author sees the real
        // failure category.
        if (e == error.Timeout) return error.TestExceededBudget;
        return e;
    }
}

/// Spawn-then-race assertion. Calls `joinFirst(c, tasks)` and asserts
/// the winner is the variant named by `expected_winner`. Fails the
/// test if a different task won.
///
/// `expected_winner` is an enum literal that must match a field name
/// of the `tasks` struct (e.g. if `tasks = .{ .fast = t1, .slow = t2 }`,
/// `expected_winner` must be `.fast` or `.slow`).
pub fn spawnRace(
    c: *lib.Cancel,
    tasks: anytype,
    comptime expected_winner: anytype,
) !void {
    const result = try lib.joinFirst(c, tasks);
    const actual = @tagName(std.meta.activeTag(result));
    const expected = @tagName(expected_winner);
    if (!std.mem.eql(u8, actual, expected)) return error.TestUnexpectedWinner;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

fn quickBody(c: *lib.Cancel) !u32 {
    _ = c;
    return 7;
}

fn expectRoot() !void {
    const v = try expect(lib.Duration.fromMillis(100), quickBody, .{});
    try std.testing.expectEqual(@as(u32, 7), v);
}

test "volt.testing.expect: body within budget returns result" {
    var rt = try lib.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(expectRoot, .{}));
}

fn slowBody(c: *lib.Cancel) !u32 {
    _ = c;
    try lib.sleep(lib.Duration.fromSecs(10));
    return 0;
}

fn expectTimeoutRoot() !void {
    const r = expect(lib.Duration.fromMillis(5), slowBody, .{});
    try std.testing.expectError(error.TestExceededBudget, r);
}

test "volt.testing.expect: body over budget fails with TestExceededBudget" {
    var rt = try lib.Runtime.init(.{ .allocator = allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(expectTimeoutRoot, .{}));
}

fn winnerFast(c: *lib.Cancel) u32 {
    _ = c;
    return 1;
}

fn loserSlow(c: *lib.Cancel) u32 {
    lib.sleepCancel(lib.Duration.fromSecs(10), c) catch return 0;
    return 0;
}

fn spawnRaceRoot(c: *lib.Cancel) !void {
    const t_w = try lib.spawn(winnerFast, .{c});
    const t_l = try lib.spawn(loserSlow, .{c});
    try spawnRace(c, .{ .fast = t_w, .slow = t_l }, .fast);
}

fn spawnRaceOuter() !void {
    try lib.scope(spawnRaceRoot);
}

test "volt.testing.spawnRace: asserts expected winner" {
    var rt = try lib.Runtime.init(.{ .allocator = allocator, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(spawnRaceOuter, .{}));
}

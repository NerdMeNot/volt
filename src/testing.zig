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

/// Wall-clock deadline guard for tests that risk hanging the runner
/// (e.g. signal-handling tests, deadlock-detection tests). Uses
/// `setitimer(ITIMER_REAL)` + `SIGALRM` — the kernel manages the
/// timer; no watchdog OS thread is created. That matters because a
/// helper thread can change signal-mask politics on POSIX and break
/// tests that rely on specific signal delivery (e.g. raise(SIGINT)).
///
/// Usage:
///
/// ```zig
/// test "hang-prone test" {
///     const guard = volt.testing.deadlineGuard(15, "my test");
///     defer guard.disarm();
///     // ... test body that should complete in under 15s ...
/// }
/// ```
///
/// If the test exceeds the deadline, SIGALRM fires, our handler
/// prints a clear label, and `abort()` kills the process. The Zig
/// test runner reports the test as crashed (signal ABRT) — which
/// is the entire point: hangs become VISIBLE failures.
///
/// Limitation: tests that themselves install a SIGALRM handler will
/// conflict. None of Volt's tests do today; if one ever does,
/// switch its deadline mechanism.
pub const DeadlineGuard = struct {
    pub fn disarm(_: DeadlineGuard) void {
        // Clear the timer + restore default SIGALRM handler.
        var off = std.mem.zeroes(itimerval);
        _ = c_setitimer(ITIMER_REAL, &off, null);
        deadline_label = null;
    }
};

const itimerval = extern struct {
    it_interval: std.c.timeval = .{ .sec = 0, .usec = 0 },
    it_value: std.c.timeval = .{ .sec = 0, .usec = 0 },
};

const ITIMER_REAL: c_int = 0;
const SIGALRM: c_int = 14;

const c_setitimer = @extern(
    *const fn (c_int, *const itimerval, ?*itimerval) callconv(.c) c_int,
    .{ .name = "setitimer" },
);

var deadline_label: ?[]const u8 = null;

// Match SigactionFn shape from signal.zig — the 3-arg form.
fn deadlineSigalrmHandler(_: c_int, _: *anyopaque, _: ?*anyopaque) callconv(.c) void {
    const label = deadline_label orelse "(unlabeled)";
    const prefix = "\n!!! test exceeded deadline: ";
    _ = c_write_fd2(2, prefix.ptr, prefix.len);
    _ = c_write_fd2(2, label.ptr, label.len);
    _ = c_write_fd2(2, "\n", 1);
    @import("std").process.abort();
}

const c_write_fd2 = @extern(
    *const fn (c_int, [*]const u8, usize) callconv(.c) isize,
    .{ .name = "write" },
);

pub fn deadlineGuard(seconds: u32, comptime label: []const u8) DeadlineGuard {
    deadline_label = label;
    const signal_mod = @import("signal.zig");
    var act: signal_mod.Sigaction = std.mem.zeroes(signal_mod.Sigaction);
    act.sa_sigaction = @ptrCast(&deadlineSigalrmHandler);
    _ = signal_mod.sigaction_fn(SIGALRM, &act, null);

    var it: itimerval = .{};
    it.it_value.sec = @intCast(seconds);
    _ = c_setitimer(ITIMER_REAL, &it, null);
    return .{};
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

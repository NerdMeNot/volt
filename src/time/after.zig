//! `afterFunc(d, fn, args)` — spawn a side coroutine that sleeps
//! `d` then calls `fn(args)`. Returns the `*Task` so the caller can
//! cancel or wait. Volt's equivalent of Go's `time.AfterFunc`.
//!
//! ```zig
//! _ = try volt.time.afterFunc(volt.Duration.fromSecs(30), saveState, .{&app});
//! ```
//!
//! Cancel-aware variant accepts an explicit `*Cancel`; firing the
//! cancel before the deadline aborts the fire and the spawned task
//! returns without invoking `fn`.

const std = @import("std");
const lib = @import("../lib.zig");
const cancel_mod = @import("../cancel.zig");
const SpawnError = @import("../runtime.zig").SpawnError;

/// Spawn a coroutine that sleeps `d` and then calls `f(args)`.
/// Returns the spawned `*Task(void)`. The caller can `.join()` if
/// they want to wait for the fire, or forget the handle if they
/// don't care. Errors in `f` are dropped (catch internally) —
/// callers that need to propagate should use a side channel.
pub fn afterFunc(
    d: lib.Duration,
    comptime f: anytype,
    args: anytype,
) SpawnError!*lib.Task(void) {
    const ArgsT = @TypeOf(args);
    const Inner = struct {
        fn run(dur: lib.Duration, a: ArgsT) void {
            lib.sleep(dur) catch return;
            _ = @call(.auto, f, a);
        }
    };
    return lib.spawn(Inner.run, .{ d, args });
}

/// Cancel-aware variant. Firing `c` before the deadline aborts the
/// fire; the spawned task returns without invoking `f`.
///
/// ```zig
/// var c = volt.Cancel.init(rt);
/// defer c.deinit();
/// const t = try volt.time.afterFuncCancel(d, &c, fireOnce, .{});
/// // ...later, before deadline:
/// c.fire();  // fireOnce never runs
/// _ = t.join();
/// ```
pub fn afterFuncCancel(
    d: lib.Duration,
    c: *cancel_mod.Cancel,
    comptime f: anytype,
    args: anytype,
) SpawnError!*lib.Task(void) {
    const ArgsT = @TypeOf(args);
    const Inner = struct {
        fn run(dur: lib.Duration, cn: *cancel_mod.Cancel, a: ArgsT) void {
            lib.sleepCancel(dur, cn) catch return;
            _ = @call(.auto, f, a);
        }
    };
    return lib.spawn(Inner.run, .{ d, c, args });
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;

// Process-global so the spawned coro can touch state the test
// inspects afterwards.
var test_after_fired: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

fn testAfterFireOnce() void {
    _ = test_after_fired.fetchAdd(1, .acq_rel);
}

fn afterFuncRoot() !void {
    test_after_fired.store(0, .release);
    const t = try afterFunc(lib.Duration.fromMillis(10), testAfterFireOnce, .{});
    _ = t.join();
    try testing.expectEqual(@as(u32, 1), test_after_fired.load(.acquire));
}

test "afterFunc fires once after the deadline" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(afterFuncRoot, .{}));
}

fn afterFuncCancelRoot() !void {
    test_after_fired.store(0, .release);
    var c = cancel_mod.Cancel.init(lib.runtime());
    defer c.deinit();
    const t = try afterFuncCancel(lib.Duration.fromSecs(10), &c, testAfterFireOnce, .{});
    // Yield a few times to let the spawned coro park on sleepCancel.
    var i: u32 = 0;
    while (i < 50) : (i += 1) lib.yield();
    c.fire();
    _ = t.join();
    try testing.expectEqual(@as(u32, 0), test_after_fired.load(.acquire));
}

test "afterFuncCancel — cancel before deadline aborts the fire" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(afterFuncCancelRoot, .{}));
}

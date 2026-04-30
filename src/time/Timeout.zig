//! `volt.withTimeout` — race a task against a deadline.
//!
//! Park-based race (v1.0). One Park unparked by EITHER the task's
//! Done.subscribe (via the task-watcher coro) OR the reactor timer
//! (via the sleep-watcher coro). Cancellable-from-anywhere parks
//! mean cancelling the task wakes it from sleep / channel / I/O
//! waits promptly, so `error.Timeout` returns at the deadline even
//! when the task is parked on uncooperative I/O.

const std = @import("std");
const Duration = @import("../time.zig").Duration;
const Park = @import("../scheduler/park.zig").Park;
const sleep = @import("Sleep.zig").sleep;
const launch = @import("../api/launch.zig").launch;
const destroyJob = @import("../api/launch.zig").destroyJob;
const Task = @import("../task/task.zig").Task;
const api_spawn = @import("../api/spawn.zig").spawn;
const destroyTask = @import("../api/spawn.zig").destroyTask;
const runtime_mod = @import("../runtime.zig");

pub const TimeoutError = error{Timeout};

const TimeoutCtx = struct {
    parker: Park = .{},
    /// CAS true on first unpark to dedupe (parker is single-shot).
    fired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn signal(self: *TimeoutCtx) void {
        if (self.fired.cmpxchgStrong(false, true, .acq_rel, .monotonic) == null) {
            self.parker.unpark();
        }
    }
};

pub fn withTimeout(
    duration: Duration,
    comptime user_fn: anytype,
    args: anytype,
) !@import("../coroutine/spawn.zig").PayloadOf(@TypeOf(user_fn)) {
    const t = try api_spawn(user_fn, args);
    defer destroyTask(t);

    const TaskT = @TypeOf(t.*);

    // Stack-local ctx — alive for the entire function, including the
    // join-watchers cleanup at the end. Watchers carry a `*TimeoutCtx`
    // reference but never outlive our scope (we always join them).
    var ctx_storage: TimeoutCtx = .{};
    const ctx = &ctx_storage;

    const TaskWatcher = struct {
        fn body(c: *TimeoutCtx, task_ptr: *TaskT) void {
            _ = task_ptr.join() catch {};
            c.signal();
        }
    };
    const SleepWatcher = struct {
        fn body(c: *TimeoutCtx, d: Duration) void {
            sleep(d) catch {};
            c.signal();
        }
    };

    const tw = try launch(TaskWatcher.body, .{ ctx, t });
    const sw = try launch(SleepWatcher.body, .{ ctx, duration });

    // Park until either watcher signals.
    ctx.parker.parkCurrent() catch |err| {
        // Cancelled by an outer scope — clean up before bubbling.
        t.cancel();
        tw.cancel();
        sw.cancel();
        _ = tw.join() catch {};
        _ = sw.join() catch {};
        destroyJob(tw);
        destroyJob(sw);
        return err;
    };

    // Determine outcome and tear down the watchers cleanly. Cancellable
    // parks mean cancel() wakes them from any active wait.
    var timed_out = false;
    if (!t.isCompleted()) {
        t.cancel();
        timed_out = true;
    }

    // Tear down task-watcher BEFORE we attempt to read t's result —
    // task-watcher is parked on `t.coro.join_park`; subscribing main
    // there too would violate the single-waiter invariant.
    tw.cancel();
    _ = tw.join() catch {};
    sw.cancel();
    _ = sw.join() catch {};
    destroyJob(tw);
    destroyJob(sw);

    if (timed_out) return error.Timeout;
    return try t.join();
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn quickReturn() u32 {
    return 42;
}

fn quickRoot() !u32 {
    return try withTimeout(Duration.fromSecs(1), quickReturn, .{});
}

test "withTimeout: fast task returns its value" {
    const v = try volt.run(.{ .allocator = std.testing.allocator }, quickRoot, .{});
    try std.testing.expectEqual(@as(u32, 42), v);
}

fn slowSleep() u32 {
    sleep(Duration.fromMillis(500)) catch {};
    return 99;
}

fn slowRoot() !u32 {
    return try withTimeout(Duration.fromMillis(50), slowSleep, .{});
}

test "withTimeout: slow task returns error.Timeout" {
    const result = volt.run(.{ .allocator = std.testing.allocator }, slowRoot, .{});
    try std.testing.expectError(error.Timeout, result);
}

fn moderateSleep() u32 {
    sleep(Duration.fromMillis(20)) catch {};
    return 7;
}

fn moderateRoot() !u32 {
    return try withTimeout(Duration.fromMillis(500), moderateSleep, .{});
}

test "withTimeout: 20ms task with 500ms timeout returns value" {
    const v = try volt.run(.{ .allocator = std.testing.allocator }, moderateRoot, .{});
    try std.testing.expectEqual(@as(u32, 7), v);
}

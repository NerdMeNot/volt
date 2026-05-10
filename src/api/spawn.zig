//! `volt.spawn(fn, args)` — value-returning spawn, returns a `*Task(T)`.
//!
//! Use Task.join() to retrieve the value. The caller owns the Task; calls
//! `volt.destroyTask(task)` to free the handle. The coroutine itself is
//! owned by the runtime.

const std = @import("std");
const TaskMod = @import("../task/task.zig");
const Job = @import("../task/job.zig").Job;
const runtime_mod = @import("../runtime.zig");

pub fn spawn(
    comptime user_fn: anytype,
    args: anytype,
) !*TaskMod.Task(@TypeOf(user_fn)) {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.spawn called outside a runtime — use volt.run(...) first");

    const created = try rt.createCoroutine(user_fn, args);

    const T = TaskMod.Task(@TypeOf(user_fn));
    const task = try rt.allocator.create(T);
    task.* = .{
        .job = .{ .coro = created.coro },
        .result = created.result_ptr,
    };
    return task;
}

pub fn destroyTask(task: anytype) void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.destroyTask called outside a runtime");
    const coro = task.job.coro;
    rt.allocator.destroy(task);
    const coro_mod = @import("../coroutine/coroutine.zig");
    coro_mod.lifecycleRelease(rt.allocator, coro);
}

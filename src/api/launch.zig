//! `volt.launch(fn, args)` — fire-and-forget spawn, returns a `*Job` handle.
//!
//! The Job is heap-allocated; the caller owns it (calls `job.deinit()` to
//! release). The underlying coroutine is owned by the runtime and reaped at
//! runtime deinit. Caller can drop the Job — the coroutine continues to
//! completion.

const std = @import("std");
const Job = @import("../task/job.zig").Job;
const runtime_mod = @import("../runtime.zig");

pub fn launch(comptime user_fn: anytype, args: anytype) !*Job {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.launch called outside a runtime — use volt.run(...) first");

    const created = try rt.createCoroutine(user_fn, args);

    const job = try rt.allocator.create(Job);
    job.* = .{ .coro = created.coro };
    return job;
}

/// Free a Job's heap allocation (does not affect the coroutine itself).
pub fn destroyJob(job: *Job) void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.destroyJob called outside a runtime");
    rt.allocator.destroy(job);
}

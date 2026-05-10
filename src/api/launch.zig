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

/// Free a Job's heap allocation AND signal the coroutine's lifecycle
/// rendezvous. If Done.subscribe has already fired, the coroutine is
/// freed here; otherwise the eventual Done.subscribe is the second
/// arrival and frees there. Either way, the coroutine struct + extras
/// are reclaimed deterministically — no accumulation in
/// `Worker.spawned[]` until runtime shutdown.
pub fn destroyJob(job: *Job) void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.destroyJob called outside a runtime");
    const coro = job.coro;
    rt.allocator.destroy(job);
    const coro_mod = @import("../coroutine/coroutine.zig");
    coro_mod.lifecycleRelease(rt.allocator, coro);
}

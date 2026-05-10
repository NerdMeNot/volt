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
    return launchWith(rt.allocator, user_fn, args);
}

/// Variant of `launch` that allocates the Job + coroutine Frame from
/// the given allocator instead of `rt.allocator`. Used by `volt.scope`
/// to allocate children from the scope's inline arena (no heap calls
/// in the spawn hot path for scope-bounded coroutines).
///
/// The allocator must outlive the coroutine (or at least until the
/// coroutine reaches `.done` and is joined / destroyed). Scope ensures
/// this by joining all children before its body returns.
pub fn launchWith(allocator: std.mem.Allocator, comptime user_fn: anytype, args: anytype) !*Job {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.launchWith called outside a runtime — use volt.run(...) first");

    // Allocate the Job wrapper FIRST. If that fails we haven't
    // launched a coroutine, so there's nothing to clean up. Doing
    // createCoroutine first would leak the coroutine on Job-alloc
    // failure — the coro is already running on a worker but no
    // handle exists, so neither destroyJob nor join can fire the
    // lifecycle rendezvous.
    const job = try allocator.create(Job);
    errdefer allocator.destroy(job);

    const created = try rt.createCoroutineWith(allocator, user_fn, args);
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

/// `destroyJob` variant for jobs allocated via `launchWith` — uses the
/// caller-supplied allocator (e.g. Scope's FixedBufferAllocator over
/// the inline arena). For FBA, both `allocator.destroy` and the
/// frame's `destroy_extras_fn` no-op; memory is reclaimed when the
/// arena itself goes out of scope.
pub fn destroyJobWith(allocator: std.mem.Allocator, job: *Job) void {
    const coro = job.coro;
    allocator.destroy(job);
    const coro_mod = @import("../coroutine/coroutine.zig");
    coro_mod.lifecycleRelease(allocator, coro);
}

//! EventSource — the dispatch contract.
//!
//! When a coroutine yields, it sets `Coroutine.pending_event` to an
//! `EventSource`. The worker reads that pointer after swap-back and calls
//! `subscribe(coro)`, transferring ownership of the *Coroutine to the
//! EventSource. Yield, Park, and Done are all just EventSources.
//!
//! See `docs/design/scheduler-protocol.md` for the full protocol.

const std = @import("std");
const Coroutine = @import("coroutine.zig").Coroutine;
const current = @import("../scheduler/current.zig");
const runtime_mod = @import("../runtime.zig");

/// `subscribe_fn(self_opaque, coro)` takes ownership of `coro`. The opaque
/// pointer is the EventSource's own address (the implementation casts it
/// as needed via `@fieldParentPtr` for embedded EventSources, or ignores
/// it for singletons).
pub const SubscribeFn = *const fn (*anyopaque, *Coroutine) void;

pub const EventSource = struct {
    subscribe_fn: SubscribeFn,
};

// ─────────────────────────────────────────────────────────────────────────────
// Yield — re-pushes the coroutine onto the calling worker's local deque.
// ─────────────────────────────────────────────────────────────────────────────

pub const yield_singleton: EventSource = .{ .subscribe_fn = &yieldSubscribe };

fn yieldSubscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
    _ = opaque_self;
    // Forward-declared Worker to avoid a hard import cycle. The opaque
    // *Worker is recovered from TLS and we call its `pushLocal` method.
    const Worker = @import("../scheduler/worker.zig").Worker;
    const raw = current.currentWorkerRaw() orelse
        @panic("Yield.subscribe: no current worker (yield outside a worker thread?)");
    const w: *Worker = @ptrCast(@alignCast(raw));
    w.pushLocal(coro);
}

// ─────────────────────────────────────────────────────────────────────────────
// Done — terminal event source.
//
// The trampoline (in coroutine/spawn.zig) sets `pending_event = &done_singleton`
// before its final swap to the scheduler. The worker calls `subscribe`, which:
//   1. Sets `coro.done_flag` (visible to handles via Job.isCompleted etc.)
//   2. Unparks any joiner waiting on `coro.join_park` (the unpark routes
//      via `Runtime.schedule` and `wakeOneSibling`, so a sibling worker
//      gets nudged for steal naturally).
//   3. If this is the root coroutine, wakes worker 0 specifically so the
//      bootstrap loop in `Runtime.runUntilDone` observes the completion.
//
// We deliberately do NOT broadcast a wake to all workers here. Earlier
// designs called `notifyAllWorkers` on every Done — the textbook
// thundering herd. With many short-lived coroutines that's N parker.unpark
// calls per completion, most of them no-ops because the woken worker
// finds nothing to do and re-parks immediately. Tokio (`num_searching`)
// and Go (`nmspinning`) both avoid this with a "one searcher is enough"
// invariant; we adopt the same shape via `Runtime.num_searching` and a
// targeted bootstrap wake.
// ─────────────────────────────────────────────────────────────────────────────

pub const done_singleton: EventSource = .{ .subscribe_fn = &doneSubscribe };

fn doneSubscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
    _ = opaque_self;
    coro.done_flag.store(true, .release);

    // Recycle the coroutine's stack into the runtime's stack pool
    // BEFORE unparking the joiner. If we unparked first, the joiner
    // could resume on a different worker, return from join(), and
    // immediately spawn a new coroutine that calls `pool.acquire()` —
    // racing the recycle. Pool would miss → mmap → munmap on
    // overflow. Recycling first guarantees the next acquire hits.
    //
    // We DON'T recycle the root coroutine's stack — the bootstrap
    // thread runs on it and keeps polling `until_done` from worker.run
    // after this returns; if we recycled, a fresh spawn could overwrite
    // pages worker 0 is about to read.
    if (!coro.is_root) {
        if (runtime_mod.currentRuntime()) |rt| {
            const stack_mod = @import("stack.zig");
            const base: usize = @intFromPtr(coro.stack.ptr);
            const reserved = coro.stack.len;
            const cb = coro.stack_committed_bottom.load(.acquire);
            const page = stack_mod.pageSize();
            const gs: stack_mod.GrowableStack = .{
                .base = base,
                .top = base + reserved,
                .reserved_size = reserved,
                .committed_bottom = cb,
                .floor = base + page,
            };
            coro.stack_pooled.store(true, .release);
            rt.stack_pool.release(gs);
        }
    }

    coro.join_park.unpark();

    if (coro.is_root) {
        // The bootstrap thread in `runUntilDone` polls `until_done.isDone()`
        // and may currently be parked. Wake worker 0 specifically — this
        // is the ONE thread we know has to observe the root's completion.
        if (runtime_mod.currentRuntime()) |rt| rt.workers[0].unpark();
        // Root is special: `volt.run` reads `result_ptr` AFTER
        // `runUntilDone` returns and releases the coroutine itself.
        // Don't fire the rendezvous here — `volt.run` does it.
        return;
    }

    // Non-root coroutine: rendezvous with the Job/Task-handle release.
    // If `destroyJob`/`destroyTask` already fired, we're the second
    // arrival and free here. If not, the eventual handle-release is
    // the second arrival.
    if (runtime_mod.currentRuntime()) |rt| {
        const coro_mod = @import("coroutine.zig");
        coro_mod.lifecycleRelease(rt.allocator, coro);
    }
}

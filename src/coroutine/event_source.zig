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
const tls = @import("../scheduler/tls.zig");
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
    const raw = tls.currentWorkerRaw() orelse
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
//   2. Unparks any joiner waiting on `coro.join_park`.
//
// The coroutine's memory remains valid (owned by the spawning worker's
// `spawned` list) until runtime teardown. After Done runs, no one holds a
// schedulable reference to the coro.
// ─────────────────────────────────────────────────────────────────────────────

pub const done_singleton: EventSource = .{ .subscribe_fn = &doneSubscribe };

fn doneSubscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
    _ = opaque_self;
    coro.done_flag.store(true, .release);
    coro.join_park.unpark();
    // Notify ALL workers. The bootstrap thread polls `until_done.isDone()`
    // in its run loop's `shouldStop`, and only observes the completion
    // when it (re)checks. notifyOneWorker may pick a non-bootstrap worker
    // as its victim and the bootstrap could race into parker.park without
    // any unpark_pending set. Notifying everyone is cheap (N atomic stores
    // in the no-op case for running workers) and correct.
    if (runtime_mod.currentRuntime()) |rt| rt.notifyAllWorkers();
}

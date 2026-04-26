//! Job: handle to a fire-and-forget coroutine.
//!
//! Returned by `volt.launch(fn, args)`. Lets you cancel the coroutine, query
//! its state, or join (wait for it to finish).
//!
//! Lifetime: the Job is heap-allocated alongside the coroutine. The caller
//! owns the Job pointer; the underlying Coroutine is owned by the runtime.
//! `volt.destroyJob(job)` releases the Job; the coroutine itself is reaped
//! at runtime deinit.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const tls = @import("../scheduler/tls.zig");
const park = @import("../scheduler/park.zig");

pub const Job = struct {
    coro: *Coroutine,

    pub fn cancel(self: *Job) void {
        self.coro.cancel();
    }

    pub fn isActive(self: *const Job) bool {
        return self.coro.loadState() != .done;
    }

    pub fn isCompleted(self: *const Job) bool {
        return self.coro.loadState() == .done;
    }

    pub fn isCancelled(self: *const Job) bool {
        return self.coro.isCancelled();
    }

    /// Wait until the coroutine is done. Parks the calling coroutine on
    /// `self.coro.waiter` — the worker that completes the child unparks us.
    ///
    /// Race-free under multi-worker:
    /// 1. CAS-attach `me` into `self.coro.waiter` (null → me).
    /// 2. Re-check `state == .done` AFTER the CAS — if the child completed
    ///    before our CAS landed, the done-handler already saw waiter=null
    ///    and didn't wake us, so we must wake ourselves by detaching and
    ///    returning. This is the standard "publish-then-recheck" pattern.
    /// 3. Otherwise park; the done-handler will swap waiter→null and wake us.
    pub fn join(self: *Job) error{Cancelled}!void {
        if (self.coro.loadState() == .done) {
            if (self.coro.isCancelled()) return error.Cancelled;
            return;
        }

        const me = tls.currentCoroutine() orelse
            @panic("Job.join called outside a coroutine");

        if (std.debug.runtime_safety and me == self.coro) {
            @panic("Job.join: coroutine cannot join itself");
        }

        // Try to attach as the (single) waiter.
        if (self.coro.waiter.cmpxchgStrong(null, me, .acq_rel, .acquire)) |existing| {
            // Either someone else is already joining (v0.4 will allow this
            // via a queue), or the slot got cleared by a fast-completing
            // child — in which case we re-check state below.
            if (existing != null) {
                @panic("Job.join: another coroutine is already joining this job (v0.4 will allow multiple)");
            }
        }

        // Re-check after attaching. If the child completed between our
        // initial check and the CAS above, the done-handler may have run
        // before our CAS — then it swapped null→null (waiter was null then)
        // and didn't wake us. Detect by re-reading state.
        if (self.coro.loadState() == .done) {
            // Detach — done-handler already ran; nobody's going to wake us.
            // Use a fetchExchange-style swap that returns the old value to
            // detect whether the done-handler beat us to detaching.
            _ = self.coro.waiter.swap(null, .acq_rel);
            if (self.coro.isCancelled()) return error.Cancelled;
            return;
        }

        try park.parkCurrent();

        if (self.coro.isCancelled()) return error.Cancelled;
    }
};

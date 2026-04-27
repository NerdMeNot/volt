//! Job: handle to a fire-and-forget coroutine.
//!
//! Returned by `volt.launch(fn, args)`. Lets you cancel the coroutine,
//! query its state, or join (wait for it to finish).
//!
//! Lifetime: the Job is heap-allocated alongside the coroutine. The caller
//! owns the Job pointer; the underlying Coroutine is owned by the runtime.
//! `volt.destroyJob(job)` releases the Job; the coroutine is reaped at
//! runtime deinit.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;

pub const Job = struct {
    coro: *Coroutine,

    pub fn cancel(self: *Job) void {
        self.coro.cancel();
    }

    pub fn isActive(self: *const Job) bool {
        return !self.coro.isDone();
    }

    pub fn isCompleted(self: *const Job) bool {
        return self.coro.isDone();
    }

    pub fn isCancelled(self: *const Job) bool {
        return self.coro.isCancelled();
    }

    /// Wait until the coroutine is done. Parks the calling coroutine on
    /// the child's `join_park`. Done.subscribe (in the trampoline's swap-
    /// out path) unparks us when the child completes.
    ///
    /// The Park primitive's atomic protocol absorbs all interleavings of
    /// "I attempt to park" vs "child completes" vs "child has already
    /// completed before I joined." See `docs/design/scheduler-protocol.md`.
    pub fn join(self: *Job) error{Cancelled}!void {
        // Fast path: already done.
        if (self.coro.isDone()) {
            if (self.coro.isCancelled()) return error.Cancelled;
            return;
        }

        // Park on the child's join slot. The Park's pre-park drainNotify
        // and the worker's subscribe-then-recheck handle the race where
        // the child completes between the check above and our park.
        try self.coro.join_park.parkCurrent();

        if (self.coro.isCancelled()) return error.Cancelled;
    }
};

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
        return self.coro.state != .done;
    }

    pub fn isCompleted(self: *const Job) bool {
        return self.coro.state == .done;
    }

    pub fn isCancelled(self: *const Job) bool {
        return self.coro.isCancelled();
    }

    /// Wait until the coroutine is done. Parks the calling coroutine on
    /// `self.coro.waiter` — the scheduler unparks us when the child
    /// transitions to `.done`.
    ///
    /// Single-threaded v0.2 invariant: there's no race between checking
    /// `.done` and setting `.waiter`, because we don't yield between those
    /// statements. v0.9 multi-threaded will need atomic compare-and-set.
    pub fn join(self: *Job) error{Cancelled}!void {
        if (self.coro.state == .done) {
            if (self.coro.isCancelled()) return error.Cancelled;
            return;
        }

        const me = tls.currentCoroutine() orelse
            @panic("Job.join called outside a coroutine");

        // Self-join would silently hang — parent waiting for itself, no one
        // wakes it. Catch in debug.
        if (std.debug.runtime_safety and me == self.coro) {
            @panic("Job.join: coroutine cannot join itself");
        }

        // Single-waiter for v0.2 — we'd otherwise clobber an existing one.
        // Two coroutines waiting on the same Job is a v0.4 concern.
        std.debug.assert(self.coro.waiter == null);
        self.coro.waiter = me;

        try park.parkCurrent();

        if (self.coro.isCancelled()) return error.Cancelled;
    }
};

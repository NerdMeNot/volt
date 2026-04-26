//! Job: handle to a fire-and-forget coroutine.
//!
//! Returned by `volt.launch(fn, args)`. Lets you cancel the coroutine, query
//! its state, or join (wait for it to finish).
//!
//! Lifetime: the Job is heap-allocated alongside the coroutine. The caller
//! owns the Job pointer; the underlying Coroutine is owned by the runtime.
//! Calling `Job.deinit()` releases the Job's hold on the coroutine but
//! doesn't free the coroutine itself (the runtime does that at deinit).

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;

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

    /// Wait until the coroutine is done. v0.1 implementation is a yield loop —
    /// not optimal but correct. v0.4 (sync primitives) gives us proper parking.
    pub fn join(self: *Job) error{Cancelled}!void {
        const yield_mod = @import("../api/yield.zig");
        while (self.coro.state != .done) {
            // Yield gives other coroutines a chance to make progress;
            // when we resume, we re-check.
            try yield_mod.yield();
        }
        if (self.coro.isCancelled()) return error.Cancelled;
    }
};

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

/// Coarse-grained state of a Job. Use `Job.state()` for `switch`-style
/// dispatch; `isActive` / `isCompleted` / `isCancelled` / `isOverflowed`
/// are sugar that read off this.
pub const State = enum { running, completed, cancelled, overflowed };

pub const Job = struct {
    coro: *Coroutine,

    pub fn cancel(self: *Job) void {
        self.coro.cancel();
    }

    /// Single-shot state query. Prefer this over the boolean predicates
    /// when you have multi-way logic.
    pub fn state(self: *const Job) State {
        if (!self.coro.isDone()) return .running;
        if (self.coro.overflow_flag.load(.acquire)) return .overflowed;
        if (self.coro.isCancelled()) return .cancelled;
        return .completed;
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

    pub fn isOverflowed(self: *const Job) bool {
        return self.coro.overflow_flag.load(.acquire);
    }

    /// Annotate the coroutine with a human-readable name. Surfaces in
    /// task snapshots, OTel spans, and panic messages.
    pub fn setName(self: *Job, name: []const u8) void {
        self.coro.name = name;
    }

    /// Annotate the coroutine with the spawn-site source location
    /// (typically `@src()`). Surfaces in task snapshots and panic
    /// messages.
    pub fn setSpawnSite(self: *Job, src: std.builtin.SourceLocation) void {
        self.coro.spawn_site = src;
    }

    /// Wait until the coroutine is done. Parks the calling coroutine on
    /// the child's `join_park`. Done.subscribe (in the trampoline's swap-
    /// out path) unparks us when the child completes.
    ///
    /// Returns:
    ///   - `error.StackOverflow` if the coroutine exhausted its reserved
    ///     stack range (terminal overflow caught by the SIGSEGV handler).
    ///   - `error.Cancelled` if the coroutine was cancelled.
    ///   - `void` on normal completion.
    pub fn join(self: *Job) error{ Cancelled, StackOverflow }!void {
        // Fast path: already done.
        if (self.coro.isDone()) {
            if (self.coro.overflow_flag.load(.acquire)) return error.StackOverflow;
            if (self.coro.isCancelled()) return error.Cancelled;
            return;
        }

        // Park on the child's join slot. The Park's pre-park drainNotify
        // and the worker's subscribe-then-recheck handle the race where
        // the child completes between the check above and our park.
        self.coro.join_park.parkCurrent() catch |err| switch (err) {
            error.Cancelled => return error.Cancelled,
        };

        if (self.coro.overflow_flag.load(.acquire)) return error.StackOverflow;
        if (self.coro.isCancelled()) return error.Cancelled;
    }
};

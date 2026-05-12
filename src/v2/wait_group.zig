//! v2 WaitGroup — atomic counter join (Go style).
//!
//! Replaces the v0.x per-coroutine `Park` primitive. Spawning a
//! coroutine increments the counter; coroutine terminal-swap decrements
//! it. The waiter polls the counter to 0.
//!
//! For single-worker runtimes (POC-C shape), `wait` is a busy-poll
//! since the waiter IS the worker — once the queue is empty AND the
//! counter is 0, we're done. There's no separate "waiter thread" to
//! park, so this is the right model.
//!
//! For multi-worker runtimes (later), `wait` will park the waiter on a
//! single ulock/futex and the last `done()` will unpark. That comes
//! when we add multi-worker; for now, single-worker is the foundation.

const std = @import("std");

pub const WaitGroup = extern struct {
    counter: std.atomic.Value(u32),

    pub fn init(n: u32) WaitGroup {
        return .{ .counter = std.atomic.Value(u32).init(n) };
    }

    /// Increment the counter (called BEFORE spawning N coroutines).
    pub fn add(self: *WaitGroup, n: u32) void {
        _ = self.counter.fetchAdd(n, .acq_rel);
    }

    /// Decrement the counter. Called by the scheduler on terminal
    /// swap-back of an attached coroutine. Manual `done` is rarely
    /// needed in v2 — the runtime auto-decrements.
    pub fn done(self: *WaitGroup) void {
        _ = self.counter.fetchSub(1, .acq_rel);
    }

    /// Check current count. Returns 0 if all attached coroutines have
    /// completed.
    pub fn count(self: *const WaitGroup) u32 {
        return self.counter.load(.acquire);
    }
};

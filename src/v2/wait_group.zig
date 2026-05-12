//! v2 WaitGroup — atomic counter join (Go style) with optional waiter.
//!
//! Spawning a coroutine increments the counter; the runtime
//! auto-decrements via `done()` on terminal swap-back. `wait()` parks
//! the current coroutine until the counter hits 0; the last `done()`
//! unparks the waiter.
//!
//! Single-worker v2: only ONE waiter is supported. Multi-waiter
//! semantics arrive with multi-worker (Phase 3.3) — the waiter slot
//! becomes a queue.
//!
//! Layout note: the FIRST field is `counter: std.atomic.Value(u32)`,
//! matching `coroutine.WaitGroupAtomic` (the type-erased view the
//! Coroutine uses for its `wg` field). DO NOT reorder.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");

pub const WaitGroup = extern struct {
    counter: std.atomic.Value(u32),
    waiter: std.atomic.Value(?*coroutine.Coroutine),

    pub fn init(n: u32) WaitGroup {
        return .{
            .counter = std.atomic.Value(u32).init(n),
            .waiter = std.atomic.Value(?*coroutine.Coroutine).init(null),
        };
    }

    pub fn add(self: *WaitGroup, n: u32) void {
        _ = self.counter.fetchAdd(n, .acq_rel);
    }

    /// Decrement counter. If we just brought it to 0 AND there's a
    /// parked waiter, unpark it.
    pub fn done(self: *WaitGroup) void {
        const prev = self.counter.fetchSub(1, .acq_rel);
        if (prev == 1) {
            if (self.waiter.swap(null, .acq_rel)) |w| {
                runtime.unpark(w);
            }
        }
    }

    pub fn count(self: *const WaitGroup) u32 {
        return self.counter.load(.acquire);
    }

    /// Park the current coroutine until the counter hits 0.
    /// Returns immediately if already 0.
    pub fn wait(self: *WaitGroup) void {
        while (true) {
            if (self.counter.load(.acquire) == 0) return;
            const me = current.require();
            self.waiter.store(me, .release);
            // Re-check counter after registering, in case it hit 0
            // between our first load and the store. Otherwise we'd
            // wait forever for a `done()` that already happened.
            if (self.counter.load(.acquire) == 0) {
                _ = self.waiter.swap(null, .acq_rel);
                return;
            }
            runtime.park();
            // Resume: loop and re-check.
        }
    }
};

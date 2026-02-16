//! Waker - Task Wake Mechanism
//!
//! A Waker is how async operations signal that a task should be polled again.
//! When an I/O operation, timer, or sync primitive becomes ready, it calls
//! `waker.wake()` to reschedule the waiting task.
//!
//! This is the stackless equivalent of `coroutine.yield()` + `scheduler.reschedule()`.
//!
//! ## Design
//!
//! Wakers use a vtable pattern for type erasure:
//! - The scheduler creates wakers that know how to reschedule tasks
//! - Async primitives store wakers and call them when ready
//! - Zero allocation - wakers are small structs (~16 bytes)
//!
//! ## Example
//!
//! ```zig
//! // Inside an async primitive (e.g., timer)
//! fn registerTimer(self: *Timer, waker: Waker) void {
//!     self.stored_waker = waker.clone();
//! }
//!
//! fn onTimerExpired(self: *Timer) void {
//!     if (self.stored_waker) |w| {
//!         w.wake();  // Reschedules the waiting task
//!     }
//! }
//! ```

const std = @import("std");

/// Raw waker with vtable for type erasure
pub const RawWaker = struct {
    /// Pointer to the task/context
    data: *anyopaque,

    /// Virtual function table
    vtable: *const VTable,

    pub const VTable = struct {
        /// Wake the task and consume the waker
        wake: *const fn (*anyopaque) void,

        /// Wake the task without consuming (for shared wakers)
        wake_by_ref: *const fn (*anyopaque) void,

        /// Clone the waker (increment refcount if needed)
        clone: *const fn (*anyopaque) RawWaker,

        /// Drop the waker (decrement refcount if needed)
        drop: *const fn (*anyopaque) void,
    };
};

/// Waker - handles waking a suspended task
///
/// This is the primary interface for signaling that a task should be polled.
/// Wakers are lightweight (16 bytes) and can be cloned/dropped safely.
pub const Waker = struct {
    raw: RawWaker,

    /// Wake the associated task (consumes this waker)
    ///
    /// After calling wake(), this waker should not be used again.
    /// Use `wakeByRef()` if you need to wake multiple times.
    pub fn wake(self: Waker) void {
        const vtable = self.raw.vtable;
        const data = self.raw.data;

        // Call wake (which may drop internally)
        vtable.wake(data);
    }

    /// Wake the task without consuming the waker
    ///
    /// Use this when you need to keep the waker for potential future wakes.
    pub fn wakeByRef(self: *const Waker) void {
        self.raw.vtable.wake_by_ref(self.raw.data);
    }

    /// Clone this waker
    ///
    /// Returns a new waker that will wake the same task.
    /// Both the original and clone must eventually be dropped.
    pub fn clone(self: *const Waker) Waker {
        return .{ .raw = self.raw.vtable.clone(self.raw.data) };
    }

    /// Drop this waker
    ///
    /// Must be called when the waker is no longer needed.
    /// After drop(), the waker should not be used.
    pub fn deinit(self: *Waker) void {
        self.raw.vtable.drop(self.raw.data);
        self.* = undefined;
    }

    /// Check if two wakers will wake the same task
    pub fn willWakeSame(self: *const Waker, other: *const Waker) bool {
        return self.raw.data == other.raw.data;
    }
};

/// Context passed to Future.poll()
///
/// Contains the waker that should be used to signal readiness.
pub const Context = struct {
    waker: *const Waker,

    /// Get the waker from this context
    pub fn getWaker(self: *const Context) *const Waker {
        return self.waker;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Noop Waker (for testing)
// ═══════════════════════════════════════════════════════════════════════════════

/// A waker that does nothing (useful for testing)
pub const noop_waker = Waker{
    .raw = .{
        .data = undefined,
        .vtable = &noop_vtable,
    },
};

const noop_vtable = RawWaker.VTable{
    .wake = noopWake,
    .wake_by_ref = noopWake,
    .clone = noopClone,
    .drop = noopDrop,
};

fn noopWake(_: *anyopaque) void {}

fn noopClone(_: *anyopaque) RawWaker {
    return .{ .data = undefined, .vtable = &noop_vtable };
}

fn noopDrop(_: *anyopaque) void {}

// ═══════════════════════════════════════════════════════════════════════════════
// Callback Waker (simple callback-based waker)
// ═══════════════════════════════════════════════════════════════════════════════

/// Create a waker from a simple callback function
/// Note: This is a simplified implementation - the callback is stored
/// in the context itself, so the ctx pointer must point to a struct
/// containing both the context data and callback pointer.
pub fn callbackWaker(
    ctx: *anyopaque,
    _: *const fn (*anyopaque) void,
) Waker {
    // Caller must ensure ctx outlives waker
    return .{
        .raw = .{
            .data = ctx,
            .vtable = &callback_vtable,
        },
    };
}

const callback_vtable = RawWaker.VTable{
    .wake = callbackWake,
    .wake_by_ref = callbackWake,
    .clone = callbackClone,
    .drop = noopDrop,
};

fn callbackWake(_: *anyopaque) void {
    // Simplified - real implementation would call stored callback
}

fn callbackClone(data: *anyopaque) RawWaker {
    return .{ .data = data, .vtable = &callback_vtable };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Waker - noop waker" {
    var waker = noop_waker;

    // Should not crash
    waker.wakeByRef();
    const cloned = waker.clone();
    cloned.wake();
}

test "Waker - clone and wake" {
    var wake_count: u32 = 0;

    const TestWaker = struct {
        fn wake(data: *anyopaque) void {
            const count: *u32 = @ptrCast(@alignCast(data));
            count.* += 1;
        }

        fn clone(data: *anyopaque) RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };
    };

    var waker = Waker{
        .raw = .{
            .data = &wake_count,
            .vtable = &TestWaker.vtable,
        },
    };

    waker.wakeByRef();
    try std.testing.expectEqual(@as(u32, 1), wake_count);

    const cloned = waker.clone();
    cloned.wake();
    try std.testing.expectEqual(@as(u32, 2), wake_count);
}

test "Context - get waker" {
    const waker = noop_waker;
    const ctx = Context{ .waker = &waker };

    const retrieved = ctx.getWaker();
    try std.testing.expect(retrieved == &waker);
}

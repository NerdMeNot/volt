//! Task(T) — typed handle returned by `volt.spawn`.
//!
//! Owns the Frame allocation. `join()` waits for the coroutine to
//! complete via a one-shot `done` flag + parking lot, then returns
//! the stored result.
//!
//! Lifecycle:
//!   * `spawn` allocates Frame + Task, returns `*Task(T)`. Task's
//!     `done` flag starts at NOT_DONE; the Coroutine carries a
//!     pointer to it in `task_done`.
//!   * Coroutine runs, trampoline stores result in Frame, terminal
//!     swap-back.
//!   * Dispatch `.done` branch: stores `done = DONE` and calls
//!     `parking_lot.unparkOne(&done)`. Frame is left alone for the
//!     joiner (Task owns it).
//!   * Caller calls `task.join()` → parks on `&done` until DONE,
//!     reads Frame.result, frees Frame + Task.
//!
//! The use-after-free that bit the old WaitGroup design can't
//! happen here: the parking-lot's validator-under-lock guarantees
//! that either we observe `done == DONE` (the wake side stored it
//! BEFORE calling unparkOne) and bail without parking, or we
//! enqueue under the bucket lock and are atomically visible to
//! the wake side's `unparkOne`.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const current = @import("current.zig");
const park = @import("park.zig");
const runtime = @import("runtime.zig");

pub const NOT_DONE: u32 = 0;
pub const DONE: u32 = 1;

fn isNotDone(addr: *const anyopaque) bool {
    const d: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    return d.load(.acquire) == NOT_DONE;
}

pub fn Task(comptime T: type) type {
    return struct {
        const Self = @This();

        coro: *coroutine.Coroutine,
        /// Pointer to the Frame's `result` field. Set by spawn.
        result_ptr: *T,
        /// Pointer to the Frame (for destroy on join).
        frame_ptr: *anyopaque,
        frame_destroy: coroutine.FrameDestroyFn,
        allocator: std.mem.Allocator,
        /// One-shot completion flag. Dispatch sets to DONE on
        /// terminal swap-back; `join()` waits for it.
        done: std.atomic.Value(u32),

        /// Wait for the coroutine to complete, return its result,
        /// free Frame + Task.
        ///
        /// Must be called from inside a coroutine while the task
        /// might still be running. Calling from a non-coroutine
        /// thread is only valid when the caller has another
        /// guarantee that `done == DONE` (e.g. `Runtime.run`'s
        /// implicit join after the worker loop has exited).
        pub fn join(self: *Self) T {
            // Direct-handoff fast path: if we're in a coroutine and the
            // spawned coro is still in our P's lifo_slot, dispatch it
            // inline. Skips parkOn → futex_wake → re-dispatch round-trip
            // when the spawner-then-joiner pattern is on a single M.
            // On miss (target stolen / evicted), falls through silently.
            if (self.done.load(.acquire) == NOT_DONE and current.get() != null) {
                _ = runtime.tryDispatchInline(self.coro);
            }
            while (self.done.load(.acquire) == NOT_DONE) {
                if (current.get() == null) {
                    @panic("Task.join called outside a coroutine while task is still running");
                }
                park.parkOn(&self.done, isNotDone);
            }
            // Copy result to a stack local — `self` lives inside the
            // Combined{Frame, Task} allocation that `frame_destroy`
            // is about to free. After the destroy call, `self` is a
            // dangling pointer; we must not touch it (no separate
            // `allocator.destroy(self)` like the old layout had).
            const result = self.result_ptr.*;
            self.frame_destroy(self.frame_ptr, self.allocator);
            return result;
        }

        /// Check completion without consuming the Task.
        pub fn isDone(self: *const Self) bool {
            return self.done.load(.acquire) == DONE;
        }
    };
}

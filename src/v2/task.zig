//! Task(T) — typed handle returned by `volt.spawn`.
//!
//! Owns the Frame allocation. `join()` waits for the coroutine to
//! complete (via WaitGroup) then returns the stored result.
//!
//! Lifecycle:
//!   * `spawn` allocates Frame + Task, returns *Task.
//!   * Coroutine runs, trampoline stores result in Frame.
//!   * On terminal swap-back, worker decrements WG, leaves Frame alone
//!     (because Task owns it).
//!   * Caller calls `task.join()` → reads Frame.result, frees Frame + Task.
//!
//! `join()` is cooperative: if called from inside a coroutine, it
//! yields until the task is done. If called from the runtime driver
//! (main thread, after `rt.run()` returned), the task is already done
//! and join completes immediately.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const wait_group = @import("wait_group.zig");
const current = @import("current.zig");
const runtime = @import("runtime.zig");

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
        /// WaitGroup with count 1; decremented on coroutine completion.
        wg: wait_group.WaitGroup,

        /// Wait for the coroutine to complete, return its result,
        /// free Frame + Task.
        ///
        /// If called from inside a coroutine, parks on the
        /// WaitGroup. The coroutine's `done` decrement unparks the
        /// waiter. If called from outside (the runtime driver,
        /// post-`rt.run()`), the wg is already 0 and wait() returns
        /// immediately.
        pub fn join(self: *Self) T {
            if (self.wg.count() != 0) {
                if (current.get() == null) {
                    @panic("Task.join called outside a coroutine and before rt.run()");
                }
                self.wg.wait();
            }
            const result = self.result_ptr.*;
            self.frame_destroy(self.frame_ptr, self.allocator);
            self.allocator.destroy(self);
            return result;
        }

        /// Check completion without consuming the Task.
        pub fn isDone(self: *const Self) bool {
            return self.wg.count() == 0;
        }
    };
}

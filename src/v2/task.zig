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
//! Task is NOT thread-safe — it's expected to be joined on the same
//! worker that spawned it (single-worker v2 makes this trivial; multi-
//! worker v2 will need to enforce single-joiner).

const std = @import("std");
const coroutine = @import("coroutine.zig");
const wait_group = @import("wait_group.zig");

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

        /// Spin until the coroutine completes, return its result, free
        /// Frame + Task. Single-worker semantics: caller is presumed
        /// to be the runtime driver (call `rt.run()` before `task.join()`).
        ///
        /// For now `join` ASSUMES the WaitGroup has been driven to 0
        /// by `rt.run()`. A future multi-worker version will park the
        /// joiner on the WG.
        pub fn join(self: *Self) T {
            std.debug.assert(self.wg.count() == 0);
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

//! POC-G — minimum Task primitive.
//!
//! A Task is just a function pointer + arg. We don't need coroutines
//! to test the scheduler architecture choice; the architecture question
//! is "what does a worker do when its queue is empty?", which is
//! orthogonal to whether the unit-of-work is a function or a coroutine.
//!
//! Bench: 10 K spawn-and-complete pairs. Each task decrements an atomic
//! counter; main waits for counter == 0. Measures wall / 10 K.

const std = @import("std");

pub const RunFn = *const fn (*Task) void;

pub const Task = struct {
    run_fn: RunFn,
    next: ?*Task = null,
    // No actual user payload — POC bench tasks just decrement an
    // atomic counter that the runner threads through.
};

// ─────────────────────────────────────────────────────────────────────
// Lock-free MPMC queue (Treiber stack, LIFO — fine for POC).
// ─────────────────────────────────────────────────────────────────────
pub const TaskQueue = struct {
    head: std.atomic.Value(?*Task) = std.atomic.Value(?*Task).init(null),

    pub fn push(self: *TaskQueue, t: *Task) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            t.next = cur;
            if (self.head.cmpxchgWeak(cur, t, .release, .monotonic)) |observed| {
                cur = observed;
            } else {
                return;
            }
        }
    }

    pub fn pop(self: *TaskQueue) ?*Task {
        var cur = self.head.load(.acquire);
        while (cur) |t| {
            const next = t.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else {
                return t;
            }
        }
        return null;
    }

    pub fn peek(self: *const TaskQueue) bool {
        return self.head.load(.acquire) != null;
    }
};

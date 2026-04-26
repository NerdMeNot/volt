//! FIFO ready queue for the v0.1 single-threaded scheduler.
//!
//! Wraps std.array_list.Managed for now; v0.9 swaps this for a Chase-Lev
//! work-stealing deque per worker. The interface (push, pop) stays stable.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;

pub const ReadyQueue = struct {
    items: std.array_list.Managed(*Coroutine),

    pub fn init(allocator: std.mem.Allocator) ReadyQueue {
        return .{ .items = std.array_list.Managed(*Coroutine).init(allocator) };
    }

    pub fn deinit(self: *ReadyQueue) void {
        self.items.deinit();
    }

    /// Add a coroutine to the back of the queue. Caller-owned pointer.
    pub fn push(self: *ReadyQueue, coro: *Coroutine) !void {
        try self.items.append(coro);
    }

    /// Remove and return the front coroutine, or null if empty.
    pub fn pop(self: *ReadyQueue) ?*Coroutine {
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    pub fn isEmpty(self: *const ReadyQueue) bool {
        return self.items.items.len == 0;
    }

    pub fn len(self: *const ReadyQueue) usize {
        return self.items.items.len;
    }
};

test "ready_queue: push/pop FIFO order" {
    var q = ReadyQueue.init(std.testing.allocator);
    defer q.deinit();

    var c1: Coroutine = undefined;
    var c2: Coroutine = undefined;
    var c3: Coroutine = undefined;

    try q.push(&c1);
    try q.push(&c2);
    try q.push(&c3);
    try std.testing.expectEqual(@as(usize, 3), q.len());

    try std.testing.expectEqual(@as(?*Coroutine, &c1), q.pop());
    try std.testing.expectEqual(@as(?*Coroutine, &c2), q.pop());
    try std.testing.expectEqual(@as(?*Coroutine, &c3), q.pop());
    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(?*Coroutine, null), q.pop());
}

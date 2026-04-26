//! Global injection queue — fallback path for cross-thread scheduling.
//!
//! Used when:
//!   - The scheduler is invoked from a non-worker thread (rare in v0.3 —
//!     mostly defensive)
//!   - A worker spawns a coro but the local deque is full (deque grow
//!     handles this today; this is a future-proof escape valve)
//!
//! Mutex-protected linked list of `*Coroutine`. Workers check this after
//! their local deque and before stealing. This is not on the hot path —
//! contention is fine. v1.0+ may swap in a lock-free MPMC queue if profiles
//! show this is a bottleneck.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Mutex = @import("../internal/thread.zig").Mutex;

pub const Injection = struct {
    mutex: Mutex = .{},
    items: std.array_list.Managed(*Coroutine),

    pub fn init(allocator: std.mem.Allocator) Injection {
        return .{ .items = std.array_list.Managed(*Coroutine).init(allocator) };
    }

    pub fn deinit(self: *Injection) void {
        self.items.deinit();
    }

    pub fn push(self: *Injection, coro: *Coroutine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.items.append(coro);
    }

    pub fn tryPop(self: *Injection) ?*Coroutine {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    pub fn isEmpty(self: *Injection) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.items.items.len == 0;
    }
};

test "injection: push/pop FIFO" {
    var inj = Injection.init(std.testing.allocator);
    defer inj.deinit();

    var c1: Coroutine = undefined;
    var c2: Coroutine = undefined;
    try inj.push(&c1);
    try inj.push(&c2);
    try std.testing.expectEqual(@as(?*Coroutine, &c1), inj.tryPop());
    try std.testing.expectEqual(@as(?*Coroutine, &c2), inj.tryPop());
    try std.testing.expectEqual(@as(?*Coroutine, null), inj.tryPop());
}

//! Wake List - Batch Waker Invocation
//!
//! Collects wakers to be invoked in a batch after releasing locks.
//! This is critical for avoiding lock contention - we don't want to
//! hold a lock while invoking wakers (which may do arbitrary work).
//!
//! Pattern:
//! 1. Hold lock
//! 2. Collect wakers into WakeList (no allocation, stack buffer)
//! 3. Release lock
//! 4. Invoke all wakers
//!

const std = @import("std");

/// A fixed-capacity list of wakers to be invoked in batch.
///
/// Designed to be stack-allocated and used transiently.
/// If capacity is exceeded, wakers are invoked immediately.
pub fn WakeList(comptime capacity: usize) type {
    return struct {
        /// Stack-allocated buffer of wakers
        wakers: [capacity]Waker = undefined,
        /// Number of wakers currently stored
        len: usize = 0,

        const Self = @This();

        /// A waker that can wake a task.
        /// In Zig, this is typically a function pointer + context.
        pub const Waker = struct {
            context: *anyopaque,
            wake_fn: *const fn (*anyopaque) void,

            pub fn wake(self: Waker) void {
                self.wake_fn(self.context);
            }
        };

        /// Check if there's room for more wakers.
        pub fn canPush(self: *const Self) bool {
            return self.len < capacity;
        }

        /// Check if the list is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.len == 0;
        }

        /// Add a waker to the list.
        /// If full, invokes the waker immediately (does not store it).
        pub fn push(self: *Self, waker: Waker) void {
            if (self.len < capacity) {
                self.wakers[self.len] = waker;
                self.len += 1;
            } else {
                // Overflow: wake immediately
                waker.wake();
            }
        }

        /// Wake all stored wakers and clear the list.
        pub fn wakeAll(self: *Self) void {
            for (self.wakers[0..self.len]) |waker| {
                waker.wake();
            }
            self.len = 0;
        }

        /// Clear without waking.
        pub fn clear(self: *Self) void {
            self.len = 0;
        }
    };
}

/// Default wake list with reasonable capacity.
/// 32 wakers is enough for most cases without being too large for the stack.
pub const DefaultWakeList = WakeList(32);

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "WakeList - basic push and wake" {
    var counter: usize = 0;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };

    var list: WakeList(8) = .{};

    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });
    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });
    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });

    try std.testing.expectEqual(@as(usize, 3), list.len);
    try std.testing.expectEqual(@as(usize, 0), counter);

    list.wakeAll();

    try std.testing.expectEqual(@as(usize, 0), list.len);
    try std.testing.expectEqual(@as(usize, 3), counter);
}

test "WakeList - overflow wakes immediately" {
    var counter: usize = 0;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };

    var list: WakeList(2) = .{};

    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });
    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });
    try std.testing.expectEqual(@as(usize, 0), counter); // Not yet woken

    // This one overflows, should wake immediately
    list.push(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake });
    try std.testing.expectEqual(@as(usize, 1), counter); // Overflow waker invoked

    // List still has 2
    try std.testing.expectEqual(@as(usize, 2), list.len);

    list.wakeAll();
    try std.testing.expectEqual(@as(usize, 3), counter);
}

test "WakeList - canPush" {
    var list: WakeList(2) = .{};
    const dummy = WakeList(2).Waker{ .context = undefined, .wake_fn = undefined };

    try std.testing.expect(list.canPush());
    list.wakers[0] = dummy;
    list.len = 1;
    try std.testing.expect(list.canPush());
    list.len = 2;
    try std.testing.expect(!list.canPush());
}

test "WakeList - clear" {
    var list: WakeList(4) = .{};
    list.len = 3;
    try std.testing.expect(!list.isEmpty());

    list.clear();
    try std.testing.expect(list.isEmpty());
}

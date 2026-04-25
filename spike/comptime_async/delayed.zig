//! A Future that suspends exactly once before becoming ready.
//!
//! Lets us verify state machines handle the pending→ready transition
//! correctly. Without something like this, "everything's ready on first
//! poll" tests would pass even if the state machine is broken.

const std = @import("std");

const proto = @import("proto.zig");
pub const PollResult = proto.PollResult;
pub const Context = proto.Context;

/// A Future that becomes ready on its second poll.
/// First poll: registers waker (no-op for the spike), returns .pending.
/// Second poll: returns .{ .ready = value }.
pub fn Delayed(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = T;

        value: T,
        polled_once: bool = false,

        pub fn init(value: T) Self {
            return .{ .value = value };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(T) {
            _ = ctx; // spike: not actually registering with a real waker
            if (!self.polled_once) {
                self.polled_once = true;
                return .pending;
            }
            return .{ .ready = self.value };
        }
    };
}

pub fn delayed(value: anytype) Delayed(@TypeOf(value)) {
    return Delayed(@TypeOf(value)).init(value);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Delayed - pends once, then ready" {
    var f = delayed(@as(i32, 42));
    var ctx = Context{ .waker = &proto.noop_waker };

    const r1 = f.poll(&ctx);
    try std.testing.expect(r1.isPending());

    const r2 = f.poll(&ctx);
    try std.testing.expect(r2.isReady());
    try std.testing.expectEqual(@as(i32, 42), r2.unwrap());
}

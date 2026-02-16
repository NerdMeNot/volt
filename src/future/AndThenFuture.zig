//! AndThenFuture - Chain Futures Sequentially
//!
//! Two-phase state machine:
//! 1. Poll inner Future F until ready
//! 2. Call `next_fn(result)` to produce Future G, poll G until ready
//!
//! This is the monadic bind (>>=) for Futures — enables sequential
//! composition where the second operation depends on the first's result.
//!
//! ## Example
//!
//! ```zig
//! // Parse a message, then validate it (both async)
//! const chained = AndThenFuture(RecvFuture, struct {
//!     fn then(msg: Message) ValidateFuture {
//!         return ValidateFuture.init(msg);
//!     }
//! }.then).init(channel.recv());
//! ```

const std = @import("std");
const PollResult = @import("Poll.zig").PollResult;
const Context = @import("Waker.zig").Context;
const isFuture = @import("Future.zig").isFuture;

/// Chain two Futures: poll F, then call next_fn on the result to get G, poll G.
///
/// Given:
/// - Future F with Output T
/// - `next_fn: fn(T) G` where G is a Future with Output U
///
/// Produces a Future with Output U.
pub fn AndThenFuture(comptime F: type, comptime next_fn: anytype) type {
    const NextFnType = @TypeOf(next_fn);
    const NextFnInfo = @typeInfo(NextFnType);
    const fn_info = switch (NextFnInfo) {
        .@"fn" => NextFnInfo.@"fn",
        .pointer => |p| @typeInfo(p.child).@"fn",
        else => @compileError("AndThenFuture: next_fn must be a function"),
    };
    const G = fn_info.return_type.?;
    const U = G.Output;

    return struct {
        const Self = @This();
        pub const Output = U;

        phase: Phase,

        const Phase = union(enum) {
            /// Phase 1: polling the first future
            first: F,
            /// Phase 2: polling the chained future
            second: G,
        };

        pub fn init(inner: F) Self {
            return .{ .phase = .{ .first = inner } };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(U) {
            switch (self.phase) {
                .first => |*first| {
                    const result = first.poll(ctx);
                    if (result.isReady()) {
                        // Phase 1 done — produce Phase 2 future
                        const next = @call(.auto, next_fn, .{result.unwrap()});
                        self.phase = .{ .second = next };
                        // Immediately poll Phase 2
                        return self.phase.second.poll(ctx);
                    }
                    return .pending;
                },
                .second => |*second| {
                    return second.poll(ctx);
                },
            }
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const noop_waker = @import("Waker.zig").noop_waker;
const Ready = @import("Future.zig").Ready;

test "AndThenFuture - chain two ready futures" {
    const First = Ready(i32);
    const Second = Ready(bool);

    const Chained = AndThenFuture(First, struct {
        fn then(x: i32) Second {
            return Second.init(x > 0);
        }
    }.then);

    var fut = Chained.init(First.init(42));
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.unwrap());
}

test "AndThenFuture - chain transforms value" {
    const First = Ready(i32);
    const Second = Ready(i32);

    const Chained = AndThenFuture(First, struct {
        fn then(x: i32) Second {
            return Second.init(x * 2);
        }
    }.then);

    var fut = Chained.init(First.init(21));
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "AndThenFuture - pending first stays pending" {
    const Pending = @import("Future.zig").Pending;
    const First = Pending(i32);
    const Second = Ready(i32);

    const Chained = AndThenFuture(First, struct {
        fn then(x: i32) Second {
            return Second.init(x * 2);
        }
    }.then);

    var first: First = .{};
    var fut = Chained.init(first);
    _ = &first;
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isPending());
}

test "AndThenFuture - isFuture compatible" {
    const First = Ready(i32);
    const Second = Ready(bool);

    const Chained = AndThenFuture(First, struct {
        fn then(x: i32) Second {
            _ = x;
            return Second.init(true);
        }
    }.then);

    try std.testing.expect(isFuture(Chained));
    try std.testing.expect(Chained.Output == bool);
}

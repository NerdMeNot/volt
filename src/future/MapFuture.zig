//! MapFuture - Transform a Future's Output
//!
//! Polls an inner Future, then applies a comptime transform function
//! to the ready result. The transform is baked into the type at comptime.
//!
//! ## Example
//!
//! ```zig
//! const doubled = MapFuture(MyFuture, struct {
//!     fn f(x: i32) i32 { return x * 2; }
//! }.f).init(my_future);
//! ```

const std = @import("std");
const PollResult = @import("Poll.zig").PollResult;
const Context = @import("Waker.zig").Context;
const isFuture = @import("Future.zig").isFuture;

/// Transform a Future's output with a comptime function.
///
/// Given a Future F with Output T, and a transform `fn(T) U`,
/// produces a Future with Output U.
pub fn MapFuture(comptime F: type, comptime transform: anytype) type {
    const TransformType = @TypeOf(transform);
    const TransformInfo = @typeInfo(TransformType);
    const fn_info = switch (TransformInfo) {
        .@"fn" => TransformInfo.@"fn",
        .pointer => |p| @typeInfo(p.child).@"fn",
        else => @compileError("MapFuture: transform must be a function"),
    };
    const U = fn_info.return_type.?;

    return struct {
        const Self = @This();
        pub const Output = U;

        inner: F,

        pub fn init(inner: F) Self {
            return .{ .inner = inner };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(U) {
            const result = self.inner.poll(ctx);
            if (result.isReady()) {
                return .{ .ready = @call(.auto, transform, .{result.unwrap()}) };
            }
            return .pending;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const noop_waker = @import("Waker.zig").noop_waker;
const Ready = @import("Future.zig").Ready;

test "MapFuture - transform ready value" {
    const Inner = Ready(i32);
    const Mapped = MapFuture(Inner, struct {
        fn double(x: i32) i32 {
            return x * 2;
        }
    }.double);

    var fut = Mapped.init(Inner.init(21));
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "MapFuture - transform type change" {
    const Inner = Ready(i32);
    const Mapped = MapFuture(Inner, struct {
        fn toBool(x: i32) bool {
            return x > 0;
        }
    }.toBool);

    var fut = Mapped.init(Inner.init(42));
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.unwrap());
}

test "MapFuture - pending stays pending" {
    const Pending = @import("Future.zig").Pending;
    const Inner = Pending(i32);
    const Mapped = MapFuture(Inner, struct {
        fn double(x: i32) i32 {
            return x * 2;
        }
    }.double);

    var inner: Inner = .{};
    var fut = Mapped.init(inner);
    _ = &inner;
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isPending());
}

test "MapFuture - isFuture compatible" {
    const Inner = Ready(i32);
    const Mapped = MapFuture(Inner, struct {
        fn double(x: i32) i32 {
            return x * 2;
        }
    }.double);

    try std.testing.expect(isFuture(Mapped));
    try std.testing.expect(Mapped.Output == i32);
}

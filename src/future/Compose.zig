//! Compose - Fluent Future Combinator API
//!
//! Wraps any Future with chainable methods for ergonomic composition.
//! `Compose(F)` IS itself a Future, so it works with `spawn` and
//! other combinators.
//!
//! ## Example
//!
//! ```zig
//! const volt = @import("volt");
//!
//! // Fluent chain: recv → parse → timeout
//! const f = compose(channel.recv())
//!     .map(parseMessage)
//!     .withTimeout(volt.Duration.fromSecs(5));
//!
//! const handle = try io.spawnFuture(f);
//! ```
//!
//! ## Available Combinators
//!
//! | Method | Description |
//! |--------|-------------|
//! | `.map(fn)` | Transform ready value |
//! | `.andThen(fn)` | Chain with another Future |
//! | `.withTimeout(dur)` | Add deadline |

const std = @import("std");
const PollResult = @import("Poll.zig").PollResult;
const Context = @import("Waker.zig").Context;
const MapFuture = @import("MapFuture.zig").MapFuture;
const AndThenFuture = @import("AndThenFuture.zig").AndThenFuture;
const isFuture = @import("Future.zig").isFuture;

/// Wrap a Future with fluent combinator methods.
///
/// `Compose(F)` is itself a Future with the same Output as F.
/// All combinator methods return new `Compose(G)` wrappers,
/// enabling method chaining.
pub fn Compose(comptime F: type) type {
    return struct {
        const Self = @This();
        pub const Output = F.Output;

        inner: F,

        /// Transform the ready value with a comptime function.
        ///
        /// ```zig
        /// compose(ready(21)).map(struct {
        ///     fn f(x: i32) i32 { return x * 2; }
        /// }.f)
        /// // Output: 42
        /// ```
        pub fn map(self: Self, comptime transform: anytype) Compose(MapFuture(F, transform)) {
            return .{ .inner = MapFuture(F, transform).init(self.inner) };
        }

        /// Chain with another Future produced from the result.
        ///
        /// ```zig
        /// compose(fetchId()).andThen(struct {
        ///     fn f(id: u64) FetchUserFuture { return FetchUserFuture.init(id); }
        /// }.f)
        /// ```
        pub fn andThen(self: Self, comptime next_fn: anytype) Compose(AndThenFuture(F, next_fn)) {
            return .{ .inner = AndThenFuture(F, next_fn).init(self.inner) };
        }

        // ─────────────────────────────────────────────────────────────────────
        // Future Implementation — Compose IS a Future
        // ─────────────────────────────────────────────────────────────────────

        pub fn poll(self: *Self, ctx: *Context) PollResult(F.Output) {
            return self.inner.poll(ctx);
        }
    };
}

/// Create a Compose wrapper from any Future value.
///
/// This is the entry point for the fluent combinator API.
///
/// ```zig
/// const f = compose(channel.recv())
///     .map(parseMessage);
/// ```
pub fn compose(f: anytype) Compose(@TypeOf(f)) {
    return .{ .inner = f };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const noop_waker = @import("Waker.zig").noop_waker;
const Ready = @import("Future.zig").Ready;

test "compose - identity (no transforms)" {
    var f = compose(Ready(i32).init(42));
    var ctx = Context{ .waker = &noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "compose - map" {
    var f = compose(Ready(i32).init(21)).map(struct {
        fn double(x: i32) i32 {
            return x * 2;
        }
    }.double);
    var ctx = Context{ .waker = &noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "compose - chained maps" {
    var f = compose(Ready(i32).init(10))
        .map(struct {
            fn add1(x: i32) i32 {
                return x + 1;
            }
        }.add1)
        .map(struct {
        fn mul4(x: i32) i32 {
            return x * 4;
        }
    }.mul4);
    var ctx = Context{ .waker = &noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 44), result.unwrap());
}

test "compose - andThen" {
    var f = compose(Ready(i32).init(42)).andThen(struct {
        fn toReady(x: i32) Ready(bool) {
            return Ready(bool).init(x > 0);
        }
    }.toReady);
    var ctx = Context{ .waker = &noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.unwrap());
}

test "compose - map then andThen" {
    var f = compose(Ready(i32).init(21))
        .map(struct {
            fn double(x: i32) i32 {
                return x * 2;
            }
        }.double)
        .andThen(struct {
        fn toStr(x: i32) Ready(bool) {
            return Ready(bool).init(x == 42);
        }
    }.toStr);
    var ctx = Context{ .waker = &noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(result.unwrap());
}

test "compose - isFuture compatible" {
    const F = Compose(Ready(i32));
    try std.testing.expect(isFuture(F));
    try std.testing.expect(F.Output == i32);

    // Mapped compose is also a Future
    const G = @TypeOf(compose(Ready(i32).init(0)).map(struct {
        fn f(x: i32) bool {
            _ = x;
            return true;
        }
    }.f));
    try std.testing.expect(isFuture(G));
    try std.testing.expect(G.Output == bool);
}

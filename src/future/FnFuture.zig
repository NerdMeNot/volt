//! FnFuture - Wrap a Plain Function as a Future
//!
//! Wraps a comptime-known function as a single-poll Future. The function
//! pointer is baked into the type (not stored at runtime), so only the
//! captured `args` occupy memory.
//!
//! ## Example
//!
//! ```zig
//! fn add(a: i32, b: i32) i32 { return a + b; }
//!
//! const F = FnFuture(add, @TypeOf(.{ @as(i32, 1), @as(i32, 2) }));
//! var fut = F.init(.{ 1, 2 });
//!
//! const waker = noop_waker;
//! var ctx = Context{ .waker = &waker };
//! const result = fut.poll(&ctx);
//! // result.unwrap() == 3
//! ```
//!
//! ## Error Handling
//!
//! If the function returns `E!T`, FnFuture.Output is `E!T` (the full
//! error union). This propagates naturally through JoinHandle.

const std = @import("std");
const PollResult = @import("Poll.zig").PollResult;
const Context = @import("Waker.zig").Context;

// ═══════════════════════════════════════════════════════════════════════════════
// FnFuture Type Constructor
// ═══════════════════════════════════════════════════════════════════════════════

/// Wrap a comptime function as a single-poll Future.
///
/// The function is baked into the type at comptime — zero runtime storage
/// for the function pointer. Only `args` are stored.
///
/// Works with:
/// - `fn() void`
/// - `fn(i32) i32`
/// - `fn(a: i32, b: i32) !Result`
pub fn FnFuture(comptime func: anytype, comptime Args: type) type {
    const RawReturn = FnReturnType(@TypeOf(func));

    return struct {
        const Self = @This();

        /// Output type matches the function's return type exactly.
        /// If the function returns `E!T`, Output is `E!T`.
        pub const Output = RawReturn;

        /// Captured arguments (the only runtime storage).
        args: Args,

        pub fn init(args: Args) Self {
            return .{ .args = args };
        }

        /// Single-poll: calls the function and returns .ready immediately.
        pub fn poll(self: *Self, _: *Context) PollResult(RawReturn) {
            return .{ .ready = @call(.auto, func, self.args) };
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract the return type from a function or function pointer type.
pub fn FnReturnType(comptime Func: type) type {
    const info = @typeInfo(Func);
    return switch (info) {
        .@"fn" => |f| f.return_type.?,
        .pointer => |p| @typeInfo(p.child).@"fn".return_type.?,
        else => @compileError("FnReturnType: expected function or function pointer, got " ++ @typeName(Func)),
    };
}

/// Extract the innermost payload type from a function's return type.
/// Strips error unions: `fn() !i32` → `i32`, `fn() i32` → `i32`.
pub fn FnPayload(comptime Func: type) type {
    const raw = FnReturnType(Func);
    return switch (@typeInfo(raw)) {
        .error_union => |eu| eu.payload,
        else => raw,
    };
}

/// Compute the spawn output type for a function or Future type.
/// - Future type F → F.Output
/// - Function type → FnReturnType
pub fn SpawnOutput(comptime T: type) type {
    if (@hasDecl(T, "Output") and @hasDecl(T, "poll")) {
        return T.Output;
    }
    return FnReturnType(T);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const noop_waker = @import("Waker.zig").noop_waker;

test "FnFuture - void function" {
    var called = false;
    const F = FnFuture(struct {
        fn run(flag: *bool) void {
            flag.* = true;
        }
    }.run, @TypeOf(.{@as(*bool, &called)}));

    var fut = F.init(.{&called});
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expect(called);
}

test "FnFuture - returning value" {
    const F = FnFuture(struct {
        fn add(a: i32, b: i32) i32 {
            return a + b;
        }
    }.add, struct { i32, i32 });

    var fut = F.init(.{ 10, 32 });
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "FnFuture - no args" {
    const F = FnFuture(struct {
        fn answer() i32 {
            return 42;
        }
    }.answer, @TypeOf(.{}));

    var fut = F.init(.{});
    var ctx = Context{ .waker = &noop_waker };
    const result = fut.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "FnFuture - isFuture compatible" {
    const isFuture = @import("Future.zig").isFuture;

    const F = FnFuture(struct {
        fn add(a: i32, b: i32) i32 {
            return a + b;
        }
    }.add, struct { i32, i32 });

    try std.testing.expect(isFuture(F));
}

test "FnReturnType - plain function" {
    const F = fn (i32, i32) i32;
    try std.testing.expect(FnReturnType(F) == i32);
}

test "FnReturnType - function pointer" {
    const F = *const fn (i32) bool;
    try std.testing.expect(FnReturnType(F) == bool);
}

test "FnPayload - non-error" {
    try std.testing.expect(FnPayload(fn () i32) == i32);
}

test "FnPayload - void" {
    try std.testing.expect(FnPayload(fn () void) == void);
}

//! Future - Async Operation Trait
//!
//! A Future represents an asynchronous computation that may not have completed yet.
//! Unlike stackful coroutines that suspend the entire call stack, futures are
//! state machines that explicitly track their progress.
//!
//! ## The Future Protocol
//!
//! 1. Call `poll(ctx)` to attempt progress
//! 2. If `.ready`, the future is complete - get the result
//! 3. If `.pending`, the future registered with `ctx.waker` and will wake when ready
//! 4. When woken, call `poll()` again
//!
//! ## Creating Futures
//!
//! Futures are created by implementing the poll function:
//!
//! ```zig
//! const MyFuture = struct {
//!     state: enum { init, waiting, done },
//!     result: ?i32 = null,
//!
//!     pub fn poll(self: *@This(), ctx: *Context) PollResult(i32) {
//!         switch (self.state) {
//!             .init => {
//!                 // Start async operation, register waker
//!                 startAsyncOp(ctx.getWaker());
//!                 self.state = .waiting;
//!                 return .pending;
//!             },
//!             .waiting => {
//!                 if (checkComplete()) |result| {
//!                     self.result = result;
//!                     self.state = .done;
//!                     return .{ .ready = result };
//!                 }
//!                 return .pending;
//!             },
//!             .done => return .{ .ready = self.result.? },
//!         }
//!     }
//! };
//! ```
//!
//! ## Memory Model
//!
//! - Futures are value types (can be stack-allocated or embedded)
//! - The caller owns the future and must keep it alive until complete
//! - Futures should be small (~64-512 bytes) for cache efficiency

const std = @import("std");
const Poll = @import("Poll.zig").Poll;
const PollResult = @import("Poll.zig").PollResult;
const Waker = @import("Waker.zig").Waker;
const Context = @import("Waker.zig").Context;

// ═══════════════════════════════════════════════════════════════════════════════
// Future Trait (compile-time interface)
// ═══════════════════════════════════════════════════════════════════════════════

/// Check if a type implements the Future interface
pub fn isFuture(comptime T: type) bool {
    const has_poll = @hasDecl(T, "poll");
    const has_output = @hasDecl(T, "Output");

    if (!has_poll or !has_output) return false;

    // Verify poll signature
    const poll_info = @typeInfo(@TypeOf(T.poll));
    if (poll_info != .@"fn") return false;

    const params = poll_info.@"fn".params;
    if (params.len != 2) return false;

    // First param should be *T
    if (params[0].type != *T) return false;

    // Second param should be *Context
    if (params[1].type != *Context) return false;

    return true;
}

/// Get the output type of a Future
pub fn FutureOutput(comptime F: type) type {
    return F.Output;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Ready Future - Immediately complete
// ═══════════════════════════════════════════════════════════════════════════════

/// A future that is immediately ready with a value
pub fn Ready(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = T;

        value: T,

        pub fn init(value: T) Self {
            return .{ .value = value };
        }

        pub fn poll(self: *Self, _: *Context) PollResult(T) {
            return .{ .ready = self.value };
        }
    };
}

/// Create a future that is immediately ready
pub fn ready(value: anytype) Ready(@TypeOf(value)) {
    return Ready(@TypeOf(value)).init(value);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Pending Future - Never completes
// ═══════════════════════════════════════════════════════════════════════════════

/// A future that never completes (always pending)
pub fn Pending(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = T;

        pub fn poll(_: *Self, _: *Context) PollResult(T) {
            return .pending;
        }
    };
}

/// Create a future that never completes
pub fn pending(comptime T: type) Pending(T) {
    return .{};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lazy Future - Computes value on first poll
// ═══════════════════════════════════════════════════════════════════════════════

/// A future that computes its value lazily on first poll
pub fn Lazy(comptime F: type) type {
    const info = @typeInfo(F);
    const fn_info = if (info == .pointer) @typeInfo(info.pointer.child) else info;
    const Output = fn_info.@"fn".return_type.?;

    return struct {
        const Self = @This();
        pub const OutputType = Output;

        func: F,
        computed: bool = false,
        result: ?Output = null,

        pub fn init(func: F) Self {
            return .{ .func = func };
        }

        pub fn poll(self: *Self, _: *Context) PollResult(Output) {
            if (!self.computed) {
                self.result = self.func();
                self.computed = true;
            }
            return .{ .ready = self.result.? };
        }
    };
}

/// Create a lazy future from a function
pub fn lazy(func: anytype) Lazy(@TypeOf(func)) {
    return Lazy(@TypeOf(func)).init(func);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Poll Function Wrapper (for type-erased futures)
// ═══════════════════════════════════════════════════════════════════════════════

/// Type-erased future poll function
pub const PollFn = *const fn (*anyopaque, *Context) Poll;

/// Type-erased future with vtable
pub const DynFuture = struct {
    ptr: *anyopaque,
    poll_fn: PollFn,
    drop_fn: *const fn (*anyopaque) void,

    pub fn poll(self: *DynFuture, ctx: *Context) Poll {
        return self.poll_fn(self.ptr, ctx);
    }

    pub fn deinit(self: *DynFuture) void {
        self.drop_fn(self.ptr);
    }
};

/// Erase a concrete future type
pub fn erase(future: anytype) DynFuture {
    const T = @TypeOf(future);
    const Ptr = *T;

    return .{
        .ptr = @ptrCast(future),
        .poll_fn = struct {
            fn poll(ptr: *anyopaque, ctx: *Context) Poll {
                const f: Ptr = @ptrCast(@alignCast(ptr));
                const result = f.poll(ctx);
                return if (result.isReady()) .ready else .pending;
            }
        }.poll,
        .drop_fn = struct {
            fn drop(_: *anyopaque) void {
                // No-op for stack-allocated futures
            }
        }.drop,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Ready future" {
    var future = ready(@as(i32, 42));
    const waker = @import("Waker.zig").noop_waker;
    var ctx = Context{ .waker = &waker };

    const result = future.poll(&ctx);
    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(i32, 42), result.unwrap());
}

test "Pending future" {
    var future = pending(i32);
    const waker = @import("Waker.zig").noop_waker;
    var ctx = Context{ .waker = &waker };

    const result = future.poll(&ctx);
    try std.testing.expect(result.isPending());
}

test "Lazy future" {
    var call_count: u32 = 0;

    var future = Lazy(*const fn () i32).init(struct {
        fn compute() i32 {
            return 42;
        }
    }.compute);
    _ = &call_count;

    const waker = @import("Waker.zig").noop_waker;
    var ctx = Context{ .waker = &waker };

    // First poll computes
    const result1 = future.poll(&ctx);
    try std.testing.expect(result1.isReady());
    try std.testing.expectEqual(@as(i32, 42), result1.unwrap());

    // Second poll returns cached
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
}

test "isFuture" {
    const ValidFuture = struct {
        pub const Output = i32;
        pub fn poll(_: *@This(), _: *Context) PollResult(i32) {
            return .{ .ready = 42 };
        }
    };

    const InvalidNoOutput = struct {
        pub fn poll(_: *@This(), _: *Context) PollResult(i32) {
            return .{ .ready = 42 };
        }
    };

    const InvalidNoPoll = struct {
        pub const Output = i32;
    };

    try std.testing.expect(isFuture(ValidFuture));
    try std.testing.expect(!isFuture(InvalidNoOutput));
    try std.testing.expect(!isFuture(InvalidNoPoll));
}

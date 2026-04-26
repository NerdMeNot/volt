//! Task(T): typed handle for a value-returning coroutine.
//!
//! Returned by `volt.spawn(fn, args)`. Extends Job with typed `join()` that
//! returns the user function's value.
//!
//! Lifetime: heap-allocated alongside the coroutine. Calling `Task.deinit()`
//! is equivalent to dropping it — the underlying coroutine is owned by the
//! runtime and reaped at runtime deinit.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("../coroutine/spawn.zig");
const Job = @import("job.zig").Job;

/// Build the join() return type:
///   user fn returns T   -> error{Cancelled}!T
///   user fn returns !T  -> (error{Cancelled} || error_set_of_user_fn)!T
fn JoinReturnType(comptime UserFn: type) type {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    switch (@typeInfo(RT)) {
        .error_union => |eu| {
            return (eu.error_set || error{Cancelled})!eu.payload;
        },
        else => return error{Cancelled}!RT,
    }
}

pub fn Task(comptime UserFn: type) type {
    const Payload = spawn_mod.PayloadOf(UserFn);
    const JoinT = JoinReturnType(UserFn);

    return struct {
        const Self = @This();
        pub const Output = Payload;

        job: Job,
        result: *spawn_mod.ResultSlot(Payload),

        pub fn cancel(self: *Self) void {
            self.job.cancel();
        }

        pub fn isActive(self: *const Self) bool {
            return self.job.isActive();
        }

        pub fn isCompleted(self: *const Self) bool {
            return self.job.isCompleted();
        }

        /// Wait until the coroutine is done; return its value (or an error
        /// if it threw, or error.Cancelled if it was cancelled).
        pub fn join(self: *Self) JoinT {
            const yield_mod = @import("../api/yield.zig");
            while (self.job.coro.state != .done) {
                // v0.1: yield-and-recheck. v0.4 will use proper parking.
                yield_mod.yield() catch |e| return e;
            }

            return switch (self.result.tag) {
                .pending => unreachable, // .done implies tag != .pending
                .ok => self.result.value,
                .err => @errorCast(self.result.err),
                .cancelled => error.Cancelled,
            };
        }
    };
}

//! Task(T): typed handle for a value-returning coroutine.
//!
//! Returned by `volt.spawn(fn, args)`. Extends Job with typed `join()` that
//! returns the user function's value.
//!
//! Lifetime: heap-allocated alongside the coroutine. `volt.destroyTask(task)`
//! releases the Task handle; the underlying coroutine is owned by the
//! runtime and reaped at runtime deinit.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("../coroutine/spawn.zig");
const Job = @import("job.zig").Job;

/// Build the join() return type:
///   user fn returns T   -> error{Cancelled, StackOverflow}!T
///   user fn returns !T  -> (error{Cancelled, StackOverflow} || error_set_of_user_fn)!T
fn JoinReturnType(comptime UserFn: type) type {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    const VoltErrors = error{ Cancelled, StackOverflow };
    switch (@typeInfo(RT)) {
        .error_union => |eu| {
            return (eu.error_set || VoltErrors)!eu.payload;
        },
        else => return VoltErrors!RT,
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

        pub fn state(self: *const Self) @import("job.zig").State {
            return self.job.state();
        }

        pub fn isActive(self: *const Self) bool {
            return self.job.isActive();
        }

        pub fn isCompleted(self: *const Self) bool {
            return self.job.isCompleted();
        }

        pub fn setName(self: *Self, name: []const u8) void {
            self.job.setName(name);
        }

        pub fn setSpawnSite(self: *Self, src: std.builtin.SourceLocation) void {
            self.job.setSpawnSite(src);
        }

        /// Wait until the coroutine is done; return its value, or an
        /// error: `error.Cancelled`, `error.StackOverflow`, or whatever
        /// the user fn threw.
        pub fn join(self: *Self) JoinT {
            self.job.join() catch |e| return e;

            return switch (self.result.tag) {
                .pending => unreachable, // .done implies tag != .pending
                .ok => self.result.value,
                .err => @errorCast(self.result.err),
                .cancelled => error.Cancelled,
                .stack_overflow => error.StackOverflow,
            };
        }
    };
}

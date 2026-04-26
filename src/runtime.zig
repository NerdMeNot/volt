//! Runtime: top-level handle to the Volt scheduler.
//!
//! Wraps the Scheduler with configuration (default stack size) and the
//! caller's allocator. Users construct a Runtime via `volt.run(allocator, fn)`
//! — they don't usually touch this type directly.
//!
//! In v0.1 the Runtime is single-threaded with one Scheduler. v0.9 will
//! generalize to N workers each with their own scheduler instance.

const std = @import("std");
const ctx = @import("coroutine/context_arm64.zig");
const Scheduler = @import("scheduler/scheduler.zig").Scheduler;
const tls = @import("scheduler/tls.zig");
const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("coroutine/spawn.zig");
const stack_mod = @import("coroutine/stack.zig");

pub const Config = struct {
    /// Default stack size for spawned coroutines.
    default_stack_size: usize = stack_mod.default_size,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: Config,
    scheduler: Scheduler,

    pub fn init(allocator: std.mem.Allocator, config: Config) Runtime {
        return .{
            .allocator = allocator,
            .config = config,
            .scheduler = Scheduler.init(allocator),
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.scheduler.deinit();
    }

    /// Spawn a coroutine for `user_fn(args)` and add it to the ready queue.
    /// Returns the Coroutine pointer + result slot pointer (handles wrap this).
    pub fn createCoroutine(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !spawn_mod.Created(@TypeOf(user_fn)) {
        const created = try spawn_mod.create(
            self.allocator,
            self.config.default_stack_size,
            user_fn,
            args,
        );
        try self.scheduler.enqueue(created.coro);
        return created;
    }

    /// Drain the scheduler — run until the ready queue is empty.
    pub fn run(self: *Runtime) void {
        self.scheduler.run(@ptrCast(self));
    }

    /// Drive until the given coroutine reaches `.done`. Used by the bootstrap
    /// to drain enough work for the root coroutine to complete.
    pub fn runUntilDone(self: *Runtime, coro: *Coroutine) void {
        self.scheduler.runUntilDone(coro, @ptrCast(self));
    }
};

/// Recover a *Runtime from the type-erased TLS slot.
pub fn currentRuntime() ?*Runtime {
    const raw = tls.currentRuntimeRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

test "runtime: init/deinit cycle" {
    var rt = Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    try std.testing.expectEqual(stack_mod.default_size, rt.config.default_stack_size);
}

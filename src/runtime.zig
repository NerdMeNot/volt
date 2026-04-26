//! Runtime: top-level handle to the Volt scheduler + reactor.
//!
//! Wraps the Scheduler with configuration (default stack size) and the
//! caller's allocator. Users construct a Runtime via `volt.run(allocator, fn)`
//! — they don't usually touch this type directly.
//!
//! In v0.1 the Runtime was scheduler-only. v0.2 adds the I/O reactor, and
//! the master dispatch loop moves here so it can interleave coroutine
//! execution with reactor polling.
//!
//! Multi-worker work-stealing arrives in v0.9 — the public surface stays.

const std = @import("std");
const ctx = @import("coroutine/context_arm64.zig");
const Scheduler = @import("scheduler/scheduler.zig").Scheduler;
const tls = @import("scheduler/tls.zig");
const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
const spawn_mod = @import("coroutine/spawn.zig");
const stack_mod = @import("coroutine/stack.zig");
const reactor_mod = @import("io/reactor.zig");

pub const Config = struct {
    /// Default stack size for spawned coroutines.
    default_stack_size: usize = stack_mod.default_size,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: Config,
    scheduler: Scheduler,
    reactor: reactor_mod.Reactor,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Runtime {
        return .{
            .allocator = allocator,
            .config = config,
            .scheduler = Scheduler.init(allocator),
            .reactor = try reactor_mod.Reactor.init(allocator),
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.scheduler.deinit();
        self.reactor.deinit();
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

    /// Drive the dispatch loop until `until_done.state == .done`. Used by the
    /// bootstrap (`volt.run`) — we drain enough work for the root coroutine
    /// to complete; orphan coros are reaped at deinit.
    pub fn runUntilDone(self: *Runtime, until_done: *Coroutine) void {
        while (until_done.loadState() != .done) {
            if (self.scheduler.tryDispatch(@ptrCast(self))) continue;

            // Ready queue is empty. If there are I/O-parked coros, block on
            // the reactor; otherwise we're genuinely idle and can stop.
            if (self.reactor.pendingCount() == 0) break;

            _ = self.reactor.poll(null, @ptrCast(self), reactorWake) catch |err| {
                std.debug.panic("volt runtime: reactor.poll failed: {}", .{err});
            };
        }
    }

    /// Reactor wake callback: an event fired, mark the coro runnable and
    /// re-enqueue it. Signature matches reactor.Reactor.poll's wakeFn param.
    fn reactorWake(opaque_self: *anyopaque, coro: *Coroutine) anyerror!void {
        const self: *Runtime = @ptrCast(@alignCast(opaque_self));
        // CAS .parked → .runnable. If the coroutine isn't parked here, it
        // was already woken by another path (e.g., cancellation racing the
        // I/O event in multi-worker mode) — drop the duplicate wake.
        if (coro.casState(.parked, .runnable) != null) return;
        try self.scheduler.requeue(coro);
    }
};

/// Recover a *Runtime from the type-erased TLS slot.
pub fn currentRuntime() ?*Runtime {
    const raw = tls.currentRuntimeRaw() orelse return null;
    return @ptrCast(@alignCast(raw));
}

test "runtime: init/deinit cycle" {
    var rt = try Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    try std.testing.expectEqual(stack_mod.default_size, rt.config.default_stack_size);
}

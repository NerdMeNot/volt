//! `volt.run(allocator, root_fn, args)` — bootstrap entry point.
//!
//! Initializes a Runtime, starts the worker pool (worker 0 is THIS thread;
//! workers 1..N-1 are dedicated threads), spawns the root coroutine on
//! worker 0, drives the dispatch loop until the root completes, then
//! signals shutdown and joins the worker threads. Returns the root
//! function's value (or error).

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const spawn_mod = @import("../coroutine/spawn.zig");

/// Build the return type of `volt.run`:
///   root_fn returns T  -> T              (init/spawn/cancel errors panic)
///   root_fn returns E!T -> (E || RuntimeBootstrapError)!eu.payload
const RuntimeBootstrapError = error{
    OutOfMemory,
    Cancelled,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    InvalidWorkerCount,
    Unexpected,
    // From kqueue init / EV_USER registration:
    AccessDenied,
    EventNotFound,
    SystemResources,
} || std.Thread.SpawnError;

fn RunReturnType(comptime UserFn: type) type {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    return switch (@typeInfo(RT)) {
        .error_union => |eu| (eu.error_set || RuntimeBootstrapError)!eu.payload,
        else => RT,
    };
}

pub fn run(
    allocator: std.mem.Allocator,
    comptime root_fn: anytype,
    args: anytype,
) RunReturnType(@TypeOf(root_fn)) {
    var rt = runtime_mod.Runtime.init(allocator, .{}) catch |err| {
        const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;
        if (@typeInfo(RT) == .error_union) return err;
        std.debug.panic("volt.run: failed to initialize runtime: {}", .{err});
    };
    defer rt.deinit();
    rt.bindWorkers();

    // Spawn the root coroutine on worker 0 BEFORE workers 1..N-1 start so
    // the root is on a deque the moment any worker can steal it.
    const created = rt.spawnRoot(root_fn, args) catch |err| {
        const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;
        if (@typeInfo(RT) == .error_union) return err;
        std.debug.panic("volt.run: failed to spawn root coroutine: {}", .{err});
    };

    // Spawn the rest of the worker pool. They start work-stealing immediately.
    rt.start() catch |err| {
        const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;
        if (@typeInfo(RT) == .error_union) return err;
        std.debug.panic("volt.run: failed to start workers: {}", .{err});
    };

    // Bootstrap thread runs as worker 0 until the root is done.
    rt.runUntilDone(created.coro);

    // Translate the root's result.
    const result = created.result_ptr;
    const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;

    return switch (result.tag) {
        .pending => std.debug.panic(
            "volt.run: dispatch loop exited with the root coroutine still .pending. " ++
                "This means the root parked on something nothing will ever wake — typically " ++
                "a join on a coroutine that was never spawned, or a wait on an fd that was " ++
                "registered without a corresponding I/O event. Audit the root fn for an " ++
                "unmatched park/wait.",
            .{},
        ),
        .ok => result.value,
        .err => blk: {
            if (@typeInfo(RT) == .error_union) {
                break :blk @errorCast(result.err);
            }
            std.debug.panic("volt.run: root coroutine errored but root fn signature has no error union: {}", .{result.err});
        },
        .cancelled => blk: {
            if (@typeInfo(RT) == .error_union) {
                break :blk error.Cancelled;
            }
            std.debug.panic("volt.run: root coroutine was cancelled but root fn signature has no error union", .{});
        },
    };
}

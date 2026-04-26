//! `volt.run(allocator, root_fn, args)` — bootstrap entry point.
//!
//! Initializes a Runtime, sets TLS so all subsequent `volt.*` calls find it,
//! spawns the root coroutine, drives the scheduler until the root completes,
//! tears down. Returns the root function's value (or error).

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const spawn_mod = @import("../coroutine/spawn.zig");

/// Build the return type of `volt.run`:
///   root_fn returns T  -> T              (init/spawn/cancel errors panic)
///   root_fn returns E!T -> (E || RuntimeBootstrapError)!eu.payload
///
/// We widen the error set on error-returning root fns because the bootstrap
/// itself can fail (kqueue init, OOM during spawn, root-cancellation propagation).
const RuntimeBootstrapError = error{
    OutOfMemory,
    Cancelled,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    Unexpected,
};

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

    tls.setRuntime(@ptrCast(&rt));
    defer tls.clearRuntime();

    // Spawn the root coroutine; we hold onto the result slot to read after
    // the scheduler drains.
    const created = rt.createCoroutine(root_fn, args) catch |err| {
        // If we can't even spawn, propagate as-is for error-returning fns;
        // for void-returning fns, panic since there's no return path.
        const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;
        if (@typeInfo(RT) == .error_union) return err;
        std.debug.panic("volt.run: failed to spawn root coroutine: {}", .{err});
    };

    rt.runUntilDone(created.coro);

    // At this point root coroutine is done. Translate its result.
    const result = created.result_ptr;
    const RT = @typeInfo(@TypeOf(root_fn)).@"fn".return_type.?;

    return switch (result.tag) {
        .pending => unreachable, // .done implies tag != .pending
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

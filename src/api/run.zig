//! `volt.run(config, root_fn, args)` — bootstrap entry point.
//!
//! Initializes a Runtime, starts the worker pool (worker 0 is THIS thread;
//! workers 1..N-1 are dedicated threads), spawns the root coroutine on
//! worker 0, drives the dispatch loop until the root completes, then
//! signals shutdown and joins the worker threads. Returns the root
//! function's value (or error).
//!
//! ```zig
//! try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
//!
//! // With overrides:
//! try volt.run(.{
//!     .allocator = gpa.allocator(),
//!     .workers = 4,
//!     .pin_workers = true,
//! }, serve, .{});
//! ```

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const spawn_mod = @import("../coroutine/spawn.zig");

/// All errors `volt.run` can surface besides the root function's own.
/// Exported so applications can build unified error sets:
///
/// ```zig
/// const AppError = volt.RunError || error{BadRequest, Forbidden};
/// ```
pub const RunError = error{
    OutOfMemory,
    Cancelled,
    StackOverflow,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    InvalidWorkerCount,
    TooManyWorkers,
    Unexpected,
    ReservedTooSmall,
    InitialCommitTooSmall,
    InitialCommitTooLarge,
    // From kqueue init / EV_USER registration:
    AccessDenied,
    EventNotFound,
    SystemResources,
    // From epoll init / eventfd registration (Linux):
    EpollCreateFailed,
    EpollCtlFailed,
    EventfdCreateFailed,
    // From IOCP CreateIoCompletionPort (Windows):
    CreateIoCompletionPortFailed,
    AssociateFailed,
    WindowsTimerNotImplemented,
    // From mmap stack allocation with guard pages:
    PermissionDenied,
    MemoryMappingNotSupported,
    MappingAlreadyExists,
    LockedMemoryLimitExceeded,
    GuardPageProtectionFailed,
} || std.Thread.SpawnError;

fn RunReturnType(comptime UserFn: type) type {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    return switch (@typeInfo(RT)) {
        .error_union => |eu| (eu.error_set || RunError)!eu.payload,
        else => RT,
    };
}

/// Bootstrap a Volt runtime, run `root_fn(args)` to completion, and
/// return its result.
///
/// `config.allocator` is required; everything else has sane defaults.
/// If `root_fn` returns an error union, runtime-bootstrap errors join
/// it (`(E || RunError)!Payload`). If `root_fn` returns a plain value
/// type, init failures surface as a panic — wrap your root in a
/// fallible signature (`fn root() !T`) if you'd rather catch them.
pub fn run(
    config: runtime_mod.Config,
    comptime root_fn: anytype,
    args: anytype,
) RunReturnType(@TypeOf(root_fn)) {
    var rt = runtime_mod.Runtime.init(config) catch |err| {
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
        .pending => blk: {
            // .pending here can mean either an unmatched park (root
            // parked on something nothing will wake) OR a stack-overflow
            // path where Done fired without the trampoline reaching its
            // final tag write. Disambiguate via overflow_flag.
            if (created.coro.overflow_flag.load(.acquire)) {
                if (@typeInfo(RT) == .error_union) {
                    break :blk error.StackOverflow;
                }
                std.debug.panic("volt.run: root coroutine overflowed its stack but root fn signature has no error union", .{});
            }
            std.debug.panic(
                "volt.run: dispatch loop exited with the root coroutine still .pending. " ++
                    "This means the root parked on something nothing will ever wake — typically " ++
                    "a join on a coroutine that was never spawned, or a wait on an fd that was " ++
                    "registered without a corresponding I/O event. Audit the root fn for an " ++
                    "unmatched park/wait.",
                .{},
            );
        },
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
        .stack_overflow => blk: {
            if (@typeInfo(RT) == .error_union) {
                break :blk error.StackOverflow;
            }
            std.debug.panic("volt.run: root coroutine overflowed its stack but root fn signature has no error union", .{});
        },
    };
}

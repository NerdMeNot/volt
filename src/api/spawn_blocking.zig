//! `volt.spawnBlocking(fn, args)` — run a synchronous function on the
//! blocking-thread pool, suspending the calling coroutine until done.
//!
//! Use for:
//! - Blocking I/O syscalls without an async equivalent (e.g. file I/O
//!   on platforms without io_uring).
//! - CPU-bound computation that would otherwise monopolize a worker
//!   thread.
//! - Calling third-party C libraries that block.
//!
//! ## Usage
//!
//! ```zig
//! const result = try volt.spawnBlocking(heavyDbQuery, .{ &request });
//! ```
//!
//! Behavior: the calling coroutine parks on a `Park` while the pool
//! thread runs `fn(args)`. When the fn returns, the pool thread
//! `park.unpark()`s the coroutine (cross-thread, routed via the
//! injection queue). The coroutine then reads the result from a stack-
//! local slot and returns it.

const std = @import("std");
const Park = @import("../scheduler/park.zig").Park;
const Pool = @import("../blocking/Pool.zig").Pool;
const PoolJob = @import("../blocking/Pool.zig").Job;
const runtime_mod = @import("../runtime.zig");
const current = @import("../scheduler/current.zig");
const Futex = @import("../internal/thread.zig").Futex;

const PayloadOf = @import("../coroutine/spawn.zig").PayloadOf;
const CanError = @import("../coroutine/spawn.zig").CanError;

pub fn spawnBlocking(
    comptime user_fn: anytype,
    args: anytype,
) !PayloadOf(@TypeOf(user_fn)) {
    const UserFn = @TypeOf(user_fn);
    const Payload = PayloadOf(UserFn);
    const RT = @typeInfo(UserFn).@"fn".return_type.?;

    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.spawnBlocking called outside a runtime");
    const coro = current.currentCoroutine() orelse
        @panic("volt.spawnBlocking called outside a coroutine");

    if (coro.isCancelled()) return error.Cancelled;

    const pool = try rt.blockingPool();

    // Closure on the calling coroutine's stack — pool thread reads
    // these while we're parked. Stable until we wake AND the pool
    // thread is done writing.
    //
    // ## Cancellation safety
    //
    // If our coroutine is cancelled mid-park, the pool thread is
    // still running and WILL write to this closure (`result_value`
    // / `result_err` / `tag`). If we returned immediately on cancel,
    // the closure would be freed (stack unwind) and the pool thread
    // would write to freed memory — UAF, possibly silent corruption.
    //
    // Fix: on Cancelled, drop down to OS-level futex wait on `tag`
    // until the pool thread sets it to non-zero. The pool thread
    // also `Futex.wake`s `tag` after writing, so this returns
    // promptly. We then surface the original Cancelled.
    //
    // (Tag is `u32` instead of `u8` so it's futex-compatible.)
    const Closure = struct {
        args_storage: @TypeOf(args),
        park: Park = .{},
        result_value: Payload = undefined,
        result_err: anyerror = undefined,
        /// 0 = pending, 1 = ok, 2 = err. u32 for Futex compatibility.
        tag: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        job: PoolJob = undefined,

        fn run(opaque_ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(opaque_ctx));
            if (comptime @typeInfo(RT) == .error_union) {
                if (@call(.auto, user_fn, self.args_storage)) |val| {
                    self.result_value = val;
                    self.tag.store(1, .release);
                } else |e| {
                    self.result_err = e;
                    self.tag.store(2, .release);
                }
            } else {
                self.result_value = @call(.auto, user_fn, self.args_storage);
                self.tag.store(1, .release);
            }
            // Wake both kinds of waiter: the parked coroutine (normal
            // case) AND any thread-level futex waiter (cancellation
            // case). park.unpark is a no-op if not parked; Futex.wake
            // is a no-op if no thread is waiting.
            Futex.wake(&self.tag, 1);
            self.park.unpark();
        }
    };

    var closure = Closure{ .args_storage = args };
    closure.job = .{
        .run_fn = &Closure.run,
        .ctx = @ptrCast(&closure),
    };

    pool.submit(&closure.job);

    closure.park.parkCurrent() catch |err| switch (err) {
        error.Cancelled => {
            // Pool thread may still be writing to `closure`. Block at
            // OS level (futex) until tag becomes non-zero — coroutine
            // park would re-fire Cancelled and not actually wait.
            while (closure.tag.load(.acquire) == 0) {
                Futex.wait(&closure.tag, 0);
            }
            // Pool thread is done; closure is safe to free.
            return error.Cancelled;
        },
    };

    const tag = closure.tag.load(.acquire);
    return switch (tag) {
        1 => closure.result_value,
        2 => blk: {
            if (comptime @typeInfo(RT) == .error_union) {
                break :blk @errorCast(closure.result_err);
            }
            // Non-erroring user fn can't have set tag=2.
            unreachable;
        },
        else => unreachable, // park returned without pool setting tag
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn cpuWork(n: u64) u64 {
    var sum: u64 = 0;
    var i: u64 = 0;
    while (i < n) : (i += 1) sum +%= i;
    return sum;
}

fn cpuWorkRoot() !u64 {
    return try spawnBlocking(cpuWork, .{@as(u64, 1000)});
}

test "spawnBlocking: cpu work returns correct sum" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, cpuWorkRoot, .{});
    try std.testing.expectEqual(@as(u64, 499500), sum);
}

fn slowWork(ms: u32) u32 {
    @import("../internal/thread.zig").sleep(@as(u64, ms) * std.time.ns_per_ms);
    return ms * 2;
}

const ConcurrentBlockingCtx = struct {
    sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn slowChild(ctx: *ConcurrentBlockingCtx, n: u32) !void {
    const v = try spawnBlocking(slowWork, .{n});
    _ = ctx.sum.fetchAdd(v, .monotonic);
}

fn concurrentBlockingRoot() !u64 {
    var ctx = ConcurrentBlockingCtx{};
    const N: u32 = 8;
    var jobs: [N]*volt.Job = undefined;
    for (&jobs, 0..) |*j, i| j.* = try volt.launch(slowChild, .{ &ctx, @as(u32, 5) + @as(u32, @intCast(i)) });
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return ctx.sum.load(.acquire);
}

test "spawnBlocking: 8 concurrent blocking calls finish — pool services them in parallel" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, concurrentBlockingRoot, .{});
    // 5..12 (each *2) summed: (5+6+...+12)*2 = 68*2 = 136
    try std.testing.expectEqual(@as(u64, 136), sum);
}

fn fallibleWork(should_fail: bool) error{IntentionalFailure}!u32 {
    if (should_fail) return error.IntentionalFailure;
    return 1;
}

fn fallibleErrRoot() !u32 {
    return try spawnBlocking(fallibleWork, .{true});
}

fn fallibleOkRoot() !u32 {
    return try spawnBlocking(fallibleWork, .{false});
}

test "spawnBlocking: error from user fn propagates" {
    const result = volt.run(.{ .allocator = std.testing.allocator }, fallibleErrRoot, .{});
    try std.testing.expectError(error.IntentionalFailure, result);
}

test "spawnBlocking: ok from user fn returns value" {
    const v = try volt.run(.{ .allocator = std.testing.allocator }, fallibleOkRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), v);
}

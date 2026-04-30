//! `volt.sleep` — suspend the calling coroutine for a duration.
//!
//! Built on the reactor's `registerTimer` (kqueue EVFILT_TIMER on
//! Darwin/BSD; epoll's timerfd or io_uring's IORING_OP_TIMEOUT on
//! Linux when those backends arrive). One-shot — the timer is auto-
//! removed by the kernel when it fires.
//!
//! ## Usage
//!
//! ```zig
//! try volt.sleep(volt.Duration.fromMillis(100));
//! try volt.sleep(volt.Duration.fromSecs(1));
//! ```

const std = @import("std");
const Duration = @import("../time.zig").Duration;
const runtime_mod = @import("../runtime.zig");
const tls = @import("../scheduler/tls.zig");
const Park = @import("../scheduler/park.zig").Park;

/// Union of errors `Reactor.registerTimer` can return on any backend
/// (kqueue: KeventError; epoll: EpollCtlFailed/TimerfdCreateFailed/
/// TimerfdSettimeFailed). Plus `Cancelled` from `parkCurrent`.
pub const SleepError = error{
    Cancelled,
    OutOfMemory,
    // kqueue paths
    AccessDenied,
    EventNotFound,
    SystemResources,
    Unexpected,
    // epoll paths
    EpollCtlFailed,
    TimerfdCreateFailed,
    TimerfdSettimeFailed,
};

/// Suspend the calling coroutine for `duration`. Returns
/// `error.Cancelled` if the coroutine is cancelled (either before
/// the sleep starts or via spurious wake during the sleep).
pub fn sleep(duration: Duration) SleepError!void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.sleep called outside a runtime");
    const coro = tls.currentCoroutine() orelse
        @panic("volt.sleep called outside a coroutine");

    if (coro.isCancelled()) return error.Cancelled;
    const ns = duration.asNanos();
    if (ns == 0) return; // zero-duration sleep is a no-op

    // Park on the calling coroutine's stack. Stable for the duration
    // of the sleep — the coroutine can't return while parked.
    var park: Park = .{};
    try rt.reactor.registerTimer(ns, @ptrCast(&park));
    try park.parkCurrent();
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");
const time = @import("../time.zig");

fn shortSleepRoot() !u64 {
    const t0 = time.nanoTimestamp();
    try sleep(Duration.fromMillis(20));
    return @intCast(time.nanoTimestamp() - t0);
}

test "sleep: 20ms returns after at least 18ms (allows 10% kernel slack)" {
    const elapsed_ns = try volt.run(.{ .allocator = std.testing.allocator }, shortSleepRoot, .{});
    // kqueue can fire a few ms early on macOS — accept ≥ 18ms (90% of target).
    try std.testing.expect(elapsed_ns >= 18 * std.time.ns_per_ms);
    // And not absurdly late — guard against scheduler hangs.
    try std.testing.expect(elapsed_ns < 200 * std.time.ns_per_ms);
}

const ConcurrentSleepCtx = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn sleepyChild(ctx: *ConcurrentSleepCtx) !void {
    try sleep(Duration.fromMillis(10));
    _ = ctx.counter.fetchAdd(1, .monotonic);
}

fn concurrentSleepRoot() !u32 {
    var ctx = ConcurrentSleepCtx{};
    const N: u32 = 16;
    var jobs: [N]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(sleepyChild, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return ctx.counter.load(.acquire);
}

test "sleep: 16 concurrent sleepers all wake (no missed timers)" {
    const woke = try volt.run(.{ .allocator = std.testing.allocator }, concurrentSleepRoot, .{});
    try std.testing.expectEqual(@as(u32, 16), woke);
}

fn zeroSleepRoot() !void {
    try sleep(Duration.fromNanos(0));
}

test "sleep: zero duration is a no-op" {
    try volt.run(.{ .allocator = std.testing.allocator }, zeroSleepRoot, .{});
}

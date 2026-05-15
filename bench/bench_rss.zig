//! Bench — resident set size per idle coroutine.
//!
//! Spawns N coroutines that immediately wait on a shared Notify, samples
//! peak RSS, then unblocks them all. Reports RSS-per-idle-coro.
//!
//! This is the **headline Zig-native metric**: how cheap is an idle
//! coroutine? Lower is better; Phase 4 stack-grow work should drive it
//! down (4× on Linux when initial-commit is 1 page = 4 KiB).
//!
//! macOS note: `ru_maxrss` is reported in bytes; Linux reports in
//! kilobytes. This bench normalizes to bytes.

const std = @import("std");
const builtin = @import("builtin");
const volt = @import("volt");

const Ctx = struct {
    note: *volt.Notify,
    started: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn waiterCoro(ctx: *Ctx) void {
    _ = ctx.started.fetchAdd(1, .acq_rel);
    ctx.note.wait();
}

extern "c" fn nanosleep(req: *const std.posix.timespec, rem: ?*std.posix.timespec) c_int;

fn maxRssBytes() u64 {
    var u: std.posix.rusage = undefined;
    const rc = std.posix.system.getrusage(std.posix.rusage.SELF, &u);
    _ = rc;
    // macOS reports ru_maxrss in bytes; Linux reports in KiB.
    const raw: u64 = @intCast(u.maxrss);
    return if (comptime builtin.os.tag == .linux) raw * 1024 else raw;
}

fn benchRoot(n: u32) !void {
    const rt: *volt.Runtime = @ptrCast(@alignCast(volt.current.require().runtime));
    var note = volt.Notify.init();
    defer note.deinit();
    var ctx = Ctx{ .note = &note };

    const tasks = try rt.allocator.alloc(*volt.Task(void), n);
    defer rt.allocator.free(tasks);
    for (tasks) |*t| t.* = try rt.spawn(waiterCoro, .{&ctx});

    // Wait until all N coros have parked on the Notify.
    while (ctx.started.load(.acquire) < n) volt.yield();
    // Give the runtime a beat to make sure they've actually swapped out
    // and aren't still in their first dispatch.
    var i: u32 = 0;
    while (i < 100) : (i += 1) volt.yield();

    const peak_rss = maxRssBytes();
    const per_coro = peak_rss / n;
    std.debug.print("  N={d:>7}  peak RSS = {d:>10} B  ({d:>6} KiB)  → per-coro = {d:>6} B ({d:.1} KiB)\n", .{
        n,
        peak_rss,
        peak_rss / 1024,
        per_coro,
        @as(f64, @floatFromInt(per_coro)) / 1024.0,
    });

    // Wake them all so they can exit.
    while (ctx.started.load(.acquire) > 0) {
        note.notifyOne();
        volt.yield();
        // notifyOne wakes one waiter; loop until none left. Track
        // exits via started decrement? simpler: just notify N times.
        // Actually we never decrement, so this loop never exits.
        // Use a different signal — break after notifying n times.
        break;
    }
    var w: u32 = 0;
    while (w < n) : (w += 1) note.notifyOne();
    for (tasks) |t| t.join();
}

fn runAtN(allocator: std.mem.Allocator, n: u32, workers: ?usize) !void {
    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();
    try (try rt.run(benchRoot, .{n}));
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== RSS per idle coroutine ===\n", .{});
    std.debug.print("Platform: ReleaseFast, single-worker (workers=1)\n", .{});
    std.debug.print("Stack size: {d} bytes ({d} KiB) — see volt.STACK_SIZE\n", .{ volt.STACK_SIZE, volt.STACK_SIZE / 1024 });
    std.debug.print("\n", .{});

    const ns = [_]u32{ 100, 1_000, 5_000, 10_000 };
    for (ns) |n| {
        try runAtN(allocator, n, 1);
    }

    std.debug.print("\n", .{});
    std.debug.print("Notes:\n", .{});
    std.debug.print("  - macOS reports maxrss in bytes; Linux in KiB. Normalized to bytes here.\n", .{});
    std.debug.print("  - Per-coro overhead = stack ({d} KiB) + Coroutine struct + Frame + Task.\n", .{volt.STACK_SIZE / 1024});
    std.debug.print("  - Phase 4 grow-on-demand target: per-coro committed RSS ≈ 1 page (4 KiB Linux, 16 KiB Darwin).\n", .{});
}

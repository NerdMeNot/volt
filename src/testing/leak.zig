//! `volt.testing.assertNoLeaks` — verify a runtime body left no
//! orphaned coroutines or pending I/O at exit.
//!
//! Snapshots the runtime's coroutine count and reactor pending count
//! before and after running `body`. If anything increased without a
//! matching cleanup, returns `error.LeakDetected`.

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const snapshot_mod = @import("../observability/snapshot.zig");

pub const LeakError = error{LeakDetected};

pub fn assertNoLeaks(comptime body: anytype) !void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.testing.assertNoLeaks called outside a runtime");

    const live_before = snapshot_mod.liveCount(rt);
    const reactor_before = rt.reactor.pendingCount();

    try body();

    const live_after = snapshot_mod.liveCount(rt);
    const reactor_after = rt.reactor.pendingCount();

    if (live_after > live_before) {
        std.log.err(
            "leak: {} live coroutines before, {} after (delta {})",
            .{ live_before, live_after, live_after - live_before },
        );
        return error.LeakDetected;
    }
    if (reactor_after > reactor_before) {
        std.log.err(
            "leak: {} reactor waits before, {} after (delta {})",
            .{ reactor_before, reactor_after, reactor_after - reactor_before },
        );
        return error.LeakDetected;
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const CleanCtx = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn cleanChild(ctx: *CleanCtx) void {
    _ = ctx.counter.fetchAdd(1, .monotonic);
}

threadlocal var clean_ctx: ?*CleanCtx = null;

fn cleanBody() !void {
    var jobs: [4]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(cleanChild, .{clean_ctx.?});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
}

fn cleanRoot() !void {
    var ctx = CleanCtx{};
    clean_ctx = &ctx;
    defer clean_ctx = null;

    try assertNoLeaks(cleanBody);
}

test "leak: clean spawn-and-join body passes assertNoLeaks" {
    try volt.run(.{ .allocator = std.testing.allocator }, cleanRoot, .{});
}

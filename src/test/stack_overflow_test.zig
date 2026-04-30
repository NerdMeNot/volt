//! v0.3.x acceptance tests for growable stacks + graceful overflow.
//!
//! 1. **Transparent growth.** A coroutine that uses much more stack than
//!    the initial committed region should run to completion without
//!    intervention — the SIGSEGV handler grows on demand.
//!
//! 2. **Graceful terminal overflow.** A coroutine that genuinely
//!    exhausts its reserved range surfaces `error.StackOverflow` from
//!    `join`. The runtime — and other coroutines — keep running.

const std = @import("std");
const volt = @import("../lib.zig");

// ─────────────────────────────────────────────────────────────────────
// 1. Deep but bounded recursion that grows the stack.
//
//    With initial commit = 1 page (4 KiB on Linux/Intel, 16 KiB on
//    Apple Silicon) and reservation = 1 MiB, we should be able to do
//    deep-but-finite recursion that uses far more than the initial
//    page without crashing.
// ─────────────────────────────────────────────────────────────────────

fn deepRecurse(depth: u32) u64 {
    // Each frame eats some stack via a local array. With ~512 bytes per
    // frame and depth = 200, total stack usage is ~100 KiB — well past
    // the 4-16 KiB initial commit.
    var scratch: [128]u64 = undefined;
    for (&scratch, 0..) |*x, i| x.* = @as(u64, @intCast(i)) +% depth;

    if (depth == 0) {
        var sum: u64 = 0;
        for (scratch) |x| sum +%= x;
        return sum;
    }
    return deepRecurse(depth - 1) +% scratch[0];
}

fn growsRoot() u64 {
    return deepRecurse(200);
}

test "v0.3.x stack: 200-deep recursion grows the stack transparently" {
    const r = volt.run(.{ .allocator = std.testing.allocator }, growsRoot, .{});
    // Just exercising — the test passes if it doesn't crash. The exact
    // sum value isn't important; that we got here is.
    try std.testing.expect(r != 0);
}

// ─────────────────────────────────────────────────────────────────────
// 2. Sibling coroutines keep running when one overflows.
//
//    A coroutine doing infinite recursion eventually exhausts the
//    reserved 1 MiB range and is killed via `error.StackOverflow`.
//    Meanwhile, other coroutines must continue producing results.
// ─────────────────────────────────────────────────────────────────────

fn infiniteRecurse(_: u64) u64 {
    // Grows forever — eventually hits the floor and is killed.
    var pad: [128]u64 = undefined;
    for (&pad, 0..) |*x, i| x.* = i;
    // Use pad to defeat the optimizer.
    return infiniteRecurse(pad[0]) +% pad[1];
}

fn safeWorker(out: *u64) !void {
    // Modest sleep-equivalent: yield a few times so we run alongside
    // the offending coroutine and the join below.
    var i: u32 = 0;
    while (i < 8) : (i += 1) try volt.yield();
    out.* = 0xC0FFEE;
}

const OverflowOutcome = struct {
    sibling_finished: u64,
    overflowed_join: bool,
};

fn overflowRoot() !OverflowOutcome {
    var sibling_out: u64 = 0;
    var sibling = try volt.spawn(safeWorker, .{&sibling_out});
    defer volt.destroyTask(sibling);

    var doomed = try volt.spawn(infiniteRecurse, .{@as(u64, 0)});
    defer volt.destroyTask(doomed);

    try sibling.join();

    // Joining the doomed coroutine must yield error.StackOverflow.
    const got_overflow = blk: {
        if (doomed.join()) |_| {
            break :blk false;
        } else |e| switch (e) {
            error.StackOverflow => break :blk true,
            else => break :blk false,
        }
    };

    return .{ .sibling_finished = sibling_out, .overflowed_join = got_overflow };
}

test "v0.3.x stack: terminal overflow → error.StackOverflow; siblings keep running" {
    const r = try volt.run(.{ .allocator = std.testing.allocator }, overflowRoot, .{});
    try std.testing.expectEqual(@as(u64, 0xC0FFEE), r.sibling_finished);
    try std.testing.expect(r.overflowed_join);
}

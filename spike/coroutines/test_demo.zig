//! End-to-end demo: spawn coroutines, yield, interleave, complete.
//! This is what stackful Volt's user-facing code aims to feel like.

const std = @import("std");
const rt_mod = @import("runtime.zig");

var trace: std.array_list.Managed(u32) = undefined;

fn worker(id: u32, yields: u32) void {
    trace.append(id * 100 + 0) catch unreachable; // start marker
    var i: u32 = 0;
    while (i < yields) : (i += 1) {
        rt_mod.yield();
        trace.append(id * 100 + i + 1) catch unreachable;
    }
    trace.append(id * 100 + 99) catch unreachable; // end marker
}

test "demo: three coroutines interleave on one worker" {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    trace = std.array_list.Managed(u32).init(alloc);
    defer trace.deinit();

    var rt = rt_mod.Runtime.init(alloc, .{});
    defer rt.deinit();

    try rt.spawn(worker, .{ @as(u32, 1), @as(u32, 3) });
    try rt.spawn(worker, .{ @as(u32, 2), @as(u32, 3) });
    try rt.spawn(worker, .{ @as(u32, 3), @as(u32, 3) });

    rt.run();

    // Each coro emits: start (id*100), then yields*1 events for each yield, then 99 end.
    // 3 coros × (1 start + 3 mid + 1 end) = 15 events
    try std.testing.expectEqual(@as(usize, 15), trace.items.len);

    // Verify interleaving — first three events must be "start" of all three coros
    // (since FIFO ready queue picks them in spawn order, and each yields once
    // before printing more).
    try std.testing.expectEqual(@as(u32, 100), trace.items[0]); // coro 1 start
    try std.testing.expectEqual(@as(u32, 200), trace.items[1]); // coro 2 start
    try std.testing.expectEqual(@as(u32, 300), trace.items[2]); // coro 3 start

    // After first yield, coros 1, 2, 3 print 101, 201, 301 (after yield 1)
    try std.testing.expectEqual(@as(u32, 101), trace.items[3]);
    try std.testing.expectEqual(@as(u32, 201), trace.items[4]);
    try std.testing.expectEqual(@as(u32, 301), trace.items[5]);

    std.debug.print("\n  trace: ", .{});
    for (trace.items) |v| std.debug.print("{} ", .{v});
    std.debug.print("\n", .{});
}

test "demo: many coroutines complete cleanly" {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    trace = std.array_list.Managed(u32).init(alloc);
    defer trace.deinit();

    var rt = rt_mod.Runtime.init(alloc, .{});
    defer rt.deinit();

    const N: u32 = 100;
    var i: u32 = 0;
    while (i < N) : (i += 1) {
        try rt.spawn(worker, .{ i, @as(u32, 5) });
    }

    rt.run();

    // 100 coros × 7 events each = 700
    try std.testing.expectEqual(@as(usize, N * 7), trace.items.len);
    std.debug.print("  100 coroutines, each yielding 5 times: {} events\n", .{trace.items.len});
}

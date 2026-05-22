//! Fan-out + fan-in — process N items in parallel via `volt.collect`,
//! then compose a final result via `volt.joinAll` over heterogeneous
//! tasks.
//!
//! Run: zig build run-fan-out
//!
//! Output:
//!   collect: 8 items in parallel → sum = 36
//!   joinAll: producer + consumer composed → 42

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    // 1. collect: parallel map of `slowDouble` over 8 inputs.
    const inputs = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var outputs: [8]u32 = undefined;
    try volt.collect(&inputs, slowDouble, &outputs);

    var sum: u32 = 0;
    for (outputs) |v| sum += v;
    std.debug.print("collect: 8 items in parallel → sum = {d}\n", .{sum});

    // 2. joinAll: heterogeneous tasks (different return types) joined
    //    into a typed struct. The result fields carry their actual
    //    payload types — `produce`'s u32 and `label`'s []const u8.
    const t_num = try volt.spawn(produce, .{});
    const t_str = try volt.spawn(label, .{});
    const r = volt.joinAll(.{ .answer = t_num, .name = t_str });
    std.debug.print("joinAll: {s} = {d}\n", .{ r.name, r.answer });
}

fn slowDouble(x: u32) u32 {
    // Yield once to simulate IO or compute that benefits from
    // dispatching across workers. Without a yield this would just
    // run serially on whichever worker picked us up first.
    volt.yield();
    return x * 2;
}

fn produce() u32 {
    return 42;
}

fn label() []const u8 {
    return "answer";
}

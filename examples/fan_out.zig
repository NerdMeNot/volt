//! Cookbook: fan out a request to N backends, return whichever
//! responds first; cancel the losers.
//!
//! Pattern: structured-concurrency `Scope` + `Oneshot` for the
//! first-result race.
//!
//! Each "backend" here is simulated with a sleep of varying duration;
//! the fastest finishes first, wins, and the slower siblings are
//! cancelled by scope cleanup.

const std = @import("std");
const volt = @import("volt");

const SimResult = struct {
    backend_id: u32,
    result: u32,
};

const Ctx = struct {
    winner: *volt.channel.Oneshot(SimResult),
};

fn backend(ctx: *Ctx, id: u32, latency_ms: u32) void {
    volt.coroutine.setCurrentName("backend-worker");
    volt.sleep(volt.Duration.fromMillis(latency_ms)) catch return;
    // First send wins; subsequent senders get error.Closed.
    _ = ctx.winner.send(.{ .backend_id = id, .result = id * 100 }) catch {};
}

fn raceBody(scope: *volt.Scope) !void {
    // Pull ctx from scope-set ambient (in this minimal demo we just
    // reach into thread-local for clarity; production code would pass
    // it via a proper context struct).
    const ctx = race_ctx.?;
    try scope.spawn(backend, .{ ctx, @as(u32, 1), @as(u32, 80) });
    try scope.spawn(backend, .{ ctx, @as(u32, 2), @as(u32, 30) });
    try scope.spawn(backend, .{ ctx, @as(u32, 3), @as(u32, 120) });

    // Wait for the first send into the oneshot.
    const winner = try ctx.winner.recv();
    std.debug.print("winner: backend {d} → {d}\n", .{ winner.backend_id, winner.result });
    // Returning from raceBody → scope joins remaining children. They
    // observe the oneshot is closed (next send → error.Closed) and
    // exit cleanly.
    ctx.winner.close();
}

threadlocal var race_ctx: ?*Ctx = null;

fn root() !void {
    var oneshot = volt.channel.Oneshot(SimResult){};
    var ctx = Ctx{ .winner = &oneshot };
    race_ctx = &ctx;
    defer race_ctx = null;
    try volt.scope(raceBody);
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}

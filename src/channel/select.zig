//! `volt.select` — wait for the first of several channel ops to complete.
//!
//! ## Implementation (v1.0)
//!
//! Park-based, forwarder-style. For each branch, `select` spawns a
//! forwarder coroutine that does the blocking `recv` on its channel.
//! The first forwarder to receive a value sends it into a shared
//! `Oneshot`; main parks on `Oneshot.recv`. After main wakes, it
//! cancels the losing forwarders — cancellable-from-anywhere parks
//! propagate the cancel into their channel-recv waits, so they exit
//! promptly.
//!
//! ## Lossiness caveat (v1.0)
//!
//! If two channels publish a value AT THE SAME TIME and both
//! forwarders consume their respective values before main cancels,
//! ONE of those values is "lost" — the loser's value was consumed
//! from its channel but never delivered to main (the loser's
//! Oneshot.send sees `error.Closed` and discards). Lossless
//! select with two-phase claim-before-consume is a planned add-on
//! tracked under the model-checker work.
//!
//! ## Usage
//!
//! ```zig
//! const winner = try volt.select(.{
//!     .msg = OnRecv(u32){ .ch = &ch1 },
//!     .ctrl = OnRecv(Cmd){ .ch = &ch2 },
//! });
//! switch (winner) {
//!     .msg => |v| handle(v),
//!     .ctrl => |c| handleCtrl(c),
//! }
//! ```

const std = @import("std");
const Channel = @import("Channel.zig").Channel;
const Oneshot = @import("Oneshot.zig").Oneshot;
const launch_mod = @import("../api/launch.zig");

pub fn OnRecv(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Output = T;
        ch: *Channel(T),

        pub fn tryPoll(self: *Self) ?T {
            return switch (self.ch.tryRecv()) {
                .value => |v| v,
                else => null,
            };
        }
    };
}

/// Build the union(enum) result type for a branches struct. Each field's
/// variant payload is the field type's `Output` (the T in OnRecv(T)).
fn ResultUnion(comptime Branches: type) type {
    const fields = @typeInfo(Branches).@"struct".fields;
    const TagInt = std.math.IntFittingRange(0, fields.len - 1);
    var field_names: [fields.len][]const u8 = undefined;
    var field_types: [fields.len]type = undefined;
    var enum_values: [fields.len]TagInt = undefined;
    inline for (fields, 0..) |f, i| {
        field_names[i] = f.name;
        field_types[i] = f.type.Output;
        enum_values[i] = @intCast(i);
    }
    const Tag = @Enum(TagInt, .exhaustive, &field_names, &enum_values);
    return @Union(.auto, Tag, &field_names, &field_types, &@splat(.{}));
}

/// Wait for the first of `branches` to complete. Returns a tagged union
/// whose variant matches the field name of the winning branch.
pub fn select(branches: anytype) !ResultUnion(@TypeOf(branches)) {
    const Branches = @TypeOf(branches);
    const Result = ResultUnion(Branches);
    const fields = @typeInfo(Branches).@"struct".fields;
    if (fields.len == 0) @compileError("volt.select requires at least one branch");
    if (fields.len > 16) @compileError("volt.select supports at most 16 branches");

    var oneshot = Oneshot(Result){};

    // Spawn one forwarder per branch. Each does the blocking recv;
    // the first to succeed sends to the oneshot. Losers are cancelled
    // after main wakes; cancellable-from-anywhere parks propagate the
    // cancel into their channel-recv waits.
    var jobs: [fields.len]*@import("../task/job.zig").Job = undefined;
    inline for (fields, 0..) |f, i| {
        const T = f.type.Output;
        const variant = f.name;
        const Wrapper = struct {
            fn body(ch: *Channel(T), os: *Oneshot(Result)) void {
                const v = ch.recv() catch return;
                _ = os.send(@unionInit(Result, variant, v)) catch {};
            }
        };
        const ch_ptr = @field(branches, f.name).ch;
        jobs[i] = try launch_mod.launch(Wrapper.body, .{ ch_ptr, &oneshot });
    }

    // Park until first forwarder sends.
    const result = oneshot.recv() catch |err| {
        // Cancelled or the oneshot was somehow closed externally.
        for (jobs) |j| {
            j.cancel();
            _ = j.join() catch {};
            launch_mod.destroyJob(j);
        }
        return err;
    };

    // Cancel losers and reap.
    for (jobs) |j| {
        j.cancel();
        _ = j.join() catch {};
        launch_mod.destroyJob(j);
    }

    return result;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const TwoChCtx = struct {
    ch1: *Channel(u32),
    ch2: *Channel(u32),
    winner_a: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    winner_b: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn selectTwoFn(ctx: *TwoChCtx) !void {
    const winner = try select(.{
        .a = OnRecv(u32){ .ch = ctx.ch1 },
        .b = OnRecv(u32){ .ch = ctx.ch2 },
    });
    switch (winner) {
        .a => |v| _ = ctx.winner_a.store(v, .release),
        .b => |v| _ = ctx.winner_b.store(v, .release),
    }
}

fn aSender(ch: *Channel(u32)) !void {
    try ch.send(101);
}

fn selectSendARoot() !TwoChCtx {
    var ch1 = try Channel(u32).init(std.testing.allocator, 4);
    defer ch1.deinit();
    var ch2 = try Channel(u32).init(std.testing.allocator, 4);
    defer ch2.deinit();
    var ctx = TwoChCtx{ .ch1 = &ch1, .ch2 = &ch2 };

    var sel = try volt.spawn(selectTwoFn, .{&ctx});
    defer volt.destroyTask(sel);
    var snd = try volt.spawn(aSender, .{&ch1});
    defer volt.destroyTask(snd);

    try sel.join();
    try snd.join();
    return ctx;
}

test "select: 2 channels, sender on a fires .a winner" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, selectSendARoot, .{});
    try std.testing.expectEqual(@as(u32, 101), ctx.winner_a.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), ctx.winner_b.load(.acquire));
}

fn bSender(ch: *Channel(u32)) !void {
    try ch.send(202);
}

fn selectSendBRoot() !TwoChCtx {
    var ch1 = try Channel(u32).init(std.testing.allocator, 4);
    defer ch1.deinit();
    var ch2 = try Channel(u32).init(std.testing.allocator, 4);
    defer ch2.deinit();
    var ctx = TwoChCtx{ .ch1 = &ch1, .ch2 = &ch2 };

    var sel = try volt.spawn(selectTwoFn, .{&ctx});
    defer volt.destroyTask(sel);
    var snd = try volt.spawn(bSender, .{&ch2});
    defer volt.destroyTask(snd);

    try sel.join();
    try snd.join();
    return ctx;
}

test "select: 2 channels, sender on b fires .b winner" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, selectSendBRoot, .{});
    try std.testing.expectEqual(@as(u32, 0), ctx.winner_a.load(.acquire));
    try std.testing.expectEqual(@as(u32, 202), ctx.winner_b.load(.acquire));
}

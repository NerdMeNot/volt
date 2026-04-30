//! Stream(T) — async iterator with a small operator vocabulary.
//!
//! v0.6 first cut: enough to express the common "iterate-with-async-source"
//! pattern (channel as stream, map, filter, take, collect). Operator
//! coverage grows over v0.7+.
//!
//! ## Design
//!
//! `Stream(T)` is a thin vtable: a `*anyopaque` context and a function
//! pointer that returns `?T` (null on stream end). Concrete sources
//! (channel, range, etc.) implement the next-fn; operators (map,
//! filter) wrap a stream and produce a new stream.
//!
//! ## Usage
//!
//! ```zig
//! var ch = try Channel(u32).init(allocator, 16);
//! defer ch.deinit();
//!
//! var stream = channelStream(u32, &ch);
//! var doubled = map(u32, u32, &stream, doubleFn);
//! const sum = try fold(u32, u64, &doubled.asStream(), 0, addFn);
//! ```

const std = @import("std");

pub const StreamError = error{Cancelled};

pub fn Stream(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Item = T;

        ctx: *anyopaque,
        next_fn: *const fn (*anyopaque) StreamError!?T,

        pub fn next(self: *Self) StreamError!?T {
            return self.next_fn(self.ctx);
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Sources
// ─────────────────────────────────────────────────────────────────────

/// Adapt a `Channel(T)` to a `Stream(T)`. The stream returns `null`
/// when the channel is closed AND drained.
pub fn channelStream(comptime T: type, ch: anytype) Stream(T) {
    return .{
        .ctx = @ptrCast(ch),
        .next_fn = struct {
            fn next(c: *anyopaque) StreamError!?T {
                const ChT = @TypeOf(ch.*);
                const channel: *ChT = @ptrCast(@alignCast(c));
                const result = channel.recv() catch |err| switch (err) {
                    error.Closed => return null,
                    error.Cancelled => return error.Cancelled,
                };
                return result;
            }
        }.next,
    };
}

/// Stream over an integer range `[start, end)`. Useful for tests and
/// canonical pipelines.
pub fn Range(comptime T: type) type {
    return struct {
        const Self = @This();
        cursor: T,
        end: T,

        pub fn init(start: T, end: T) Self {
            return .{ .cursor = start, .end = end };
        }

        pub fn asStream(self: *Self) Stream(T) {
            return .{
                .ctx = @ptrCast(self),
                .next_fn = next,
            };
        }

        fn next(c: *anyopaque) StreamError!?T {
            const self: *Self = @ptrCast(@alignCast(c));
            if (self.cursor >= self.end) return null;
            const v = self.cursor;
            self.cursor += 1;
            return v;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Operators
// ─────────────────────────────────────────────────────────────────────

pub fn Map(comptime In: type, comptime Out: type, comptime map_fn: fn (In) Out) type {
    return struct {
        const Self = @This();
        upstream: *Stream(In),

        pub fn init(upstream: *Stream(In)) Self {
            return .{ .upstream = upstream };
        }

        pub fn asStream(self: *Self) Stream(Out) {
            return .{
                .ctx = @ptrCast(self),
                .next_fn = next,
            };
        }

        fn next(c: *anyopaque) StreamError!?Out {
            const self: *Self = @ptrCast(@alignCast(c));
            const v = (try self.upstream.next()) orelse return null;
            return map_fn(v);
        }
    };
}

pub fn Filter(comptime T: type, comptime pred_fn: fn (T) bool) type {
    return struct {
        const Self = @This();
        upstream: *Stream(T),

        pub fn init(upstream: *Stream(T)) Self {
            return .{ .upstream = upstream };
        }

        pub fn asStream(self: *Self) Stream(T) {
            return .{
                .ctx = @ptrCast(self),
                .next_fn = next,
            };
        }

        fn next(c: *anyopaque) StreamError!?T {
            const self: *Self = @ptrCast(@alignCast(c));
            while (true) {
                const v = (try self.upstream.next()) orelse return null;
                if (pred_fn(v)) return v;
            }
        }
    };
}

pub fn Take(comptime T: type) type {
    return struct {
        const Self = @This();
        upstream: *Stream(T),
        remaining: usize,

        pub fn init(upstream: *Stream(T), n: usize) Self {
            return .{ .upstream = upstream, .remaining = n };
        }

        pub fn asStream(self: *Self) Stream(T) {
            return .{
                .ctx = @ptrCast(self),
                .next_fn = next,
            };
        }

        fn next(c: *anyopaque) StreamError!?T {
            const self: *Self = @ptrCast(@alignCast(c));
            if (self.remaining == 0) return null;
            const v = (try self.upstream.next()) orelse return null;
            self.remaining -= 1;
            return v;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Terminal operators
// ─────────────────────────────────────────────────────────────────────

/// Fold the stream with an accumulator and a binary fn.
pub fn fold(
    comptime T: type,
    comptime A: type,
    s: *Stream(T),
    init_acc: A,
    comptime fn_ptr: fn (A, T) A,
) !A {
    var acc = init_acc;
    while (try s.next()) |v| acc = fn_ptr(acc, v);
    return acc;
}

/// Count items in the stream (drains it).
pub fn count(comptime T: type, s: *Stream(T)) !usize {
    var n: usize = 0;
    while (try s.next()) |_| n += 1;
    return n;
}

/// Collect into a slice. Caller owns the returned slice.
pub fn toList(comptime T: type, allocator: std.mem.Allocator, s: *Stream(T)) ![]T {
    var list = std.array_list.Managed(T).init(allocator);
    errdefer list.deinit();
    while (try s.next()) |v| try list.append(v);
    return list.toOwnedSlice();
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn doubleIt(x: u32) u32 {
    return x * 2;
}

fn isEven(x: u32) bool {
    return (x & 1) == 0;
}

fn add(acc: u64, x: u32) u64 {
    return acc + x;
}

test "stream: range + map + fold" {
    var range = Range(u32).init(0, 10);
    var range_stream = range.asStream();
    var doubled = Map(u32, u32, doubleIt).init(&range_stream);
    var doubled_stream = doubled.asStream();
    const sum = try fold(u32, u64, &doubled_stream, 0, add);
    // 0..10 doubled = 0,2,4,...,18. Sum = 90.
    try std.testing.expectEqual(@as(u64, 90), sum);
}

test "stream: range + filter + count" {
    var range = Range(u32).init(0, 100);
    var range_stream = range.asStream();
    var even = Filter(u32, isEven).init(&range_stream);
    var even_stream = even.asStream();
    const n = try count(u32, &even_stream);
    try std.testing.expectEqual(@as(usize, 50), n);
}

test "stream: take limits the stream" {
    var range = Range(u32).init(0, 1000);
    var range_stream = range.asStream();
    var taken = Take(u32).init(&range_stream, 5);
    var taken_stream = taken.asStream();
    const list = try toList(u32, std.testing.allocator, &taken_stream);
    defer std.testing.allocator.free(list);
    try std.testing.expectEqual(@as(usize, 5), list.len);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 4 }, list);
}

const ChCtx = struct {
    ch: *@import("../channel/Channel.zig").Channel(u32),
};

fn producer(ctx: *ChCtx) !void {
    var i: u32 = 1;
    while (i <= 5) : (i += 1) try ctx.ch.send(i);
    ctx.ch.close();
}

fn channelStreamRoot() !u64 {
    const Channel = @import("../channel/Channel.zig").Channel;
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();
    var ctx = ChCtx{ .ch = &ch };

    var prod = try volt.spawn(producer, .{&ctx});
    defer volt.destroyTask(prod);

    var s = channelStream(u32, &ch);
    const sum = try fold(u32, u64, &s, 0, add);
    try prod.join();
    return sum;
}

test "stream: channel as source — fold drains until close" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, channelStreamRoot, .{});
    // 1+2+3+4+5 = 15
    try std.testing.expectEqual(@as(u64, 15), sum);
}

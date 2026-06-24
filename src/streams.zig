//! `volt.Stream(T)` — pull-based async streams.
//!
//! A `Stream(T)` is a type-erased, single-pass async iterator: `next()`
//! suspends if the source needs I/O and returns `null` at end. It's the
//! substrate for streaming I/O in the consumer libs (S3 list pagination,
//! HTTP chunked bodies, PG cursors, DataFrame batches).
//!
//! Design: `docs/design/streams.md`. Pull (not push/`emit`) because Zig
//! has no lambdas and I/O sources are pull-shaped; type-erased (not
//! comptime-composed) so a lib can write `fn listObjects(...) Stream(Object)`
//! — one concrete return type that escapes the function; allocator-owned
//! (each operator boxes its state once at construction, freed by `deinit`).
//!
//! This is Slice 1: the core type, the three sources (`fromSlice`,
//! `fromChannel`, `generate`), and the terminals. Lazy operators
//! (`map`/`filter`/`take`/…) and the concurrency operators land in later
//! slices; cancellation (`cancel: ?*Cancel` threaded into `recvCancel`)
//! lands in Slice 3.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// A pull-based async stream of `T`. Construct via a source
/// (`fromSlice` / `fromChannel` / `generate`), drive via a terminal
/// (`forEach` / `toList` / `count` / `first` / `fold`), release via
/// `deinit` (which walks + frees the whole pipeline).
pub fn Stream(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Item = T;

        ctx: *anyopaque,
        vtable: *const VTable,
        /// Carried from the source; operators reuse it for their boxes.
        allocator: Allocator,

        pub const VTable = struct {
            /// Suspends if the source needs I/O. `null` = end of stream.
            /// Errors (I/O, OOM, later Cancelled) flow through the union.
            next: *const fn (ctx: *anyopaque) anyerror!?T,
            /// Frees this stage's box and recursively deinits upstream.
            deinit: *const fn (ctx: *anyopaque, a: Allocator) void,
        };

        pub fn next(self: *Self) anyerror!?T {
            return self.vtable.next(self.ctx);
        }

        pub fn deinit(self: *Self) void {
            self.vtable.deinit(self.ctx, self.allocator);
        }

        // ─── Terminals (drive the stream to completion) ──────────────

        /// Call `f` on each item. `f` may be `fn (T) void` or
        /// `fn (T) anyerror!void`; the latter's error ends the stream.
        pub fn forEach(self: *Self, comptime f: anytype) anyerror!void {
            while (try self.next()) |v| {
                const R = @typeInfo(@TypeOf(f)).@"fn".return_type.?;
                if (comptime @typeInfo(R) == .error_union) try f(v) else f(v);
            }
        }

        /// Collect every item into an owned slice (caller frees with `a`).
        pub fn toList(self: *Self, a: Allocator) anyerror![]T {
            var list = std.array_list.Managed(T).init(a);
            errdefer list.deinit();
            while (try self.next()) |v| try list.append(v);
            return list.toOwnedSlice();
        }

        /// Number of items the stream yields.
        pub fn count(self: *Self) anyerror!usize {
            var n: usize = 0;
            while (try self.next()) |_| n += 1;
            return n;
        }

        /// First item, or `null` if the stream is empty. Does not
        /// `deinit` — the caller still owns the stream.
        pub fn first(self: *Self) anyerror!?T {
            return self.next();
        }

        /// Left-fold over the items.
        pub fn fold(
            self: *Self,
            comptime Acc: type,
            initial: Acc,
            comptime f: fn (Acc, T) Acc,
        ) anyerror!Acc {
            var acc = initial;
            while (try self.next()) |v| acc = f(acc, v);
            return acc;
        }
    };
}

// ─── Sources ─────────────────────────────────────────────────────────

/// Stream over a borrowed slice. The slice must outlive the stream
/// (no copy is made). Mainly for tests + small in-memory sequences.
pub fn fromSlice(comptime T: type, allocator: Allocator, items: []const T) Allocator.Error!Stream(T) {
    const Box = struct { items: []const T, i: usize = 0 };
    const box = try allocator.create(Box);
    box.* = .{ .items = items };
    const Impl = struct {
        fn next(ctx: *anyopaque) anyerror!?T {
            const b: *Box = @ptrCast(@alignCast(ctx));
            if (b.i >= b.items.len) return null;
            defer b.i += 1;
            return b.items[b.i];
        }
        fn deinit(ctx: *anyopaque, a: Allocator) void {
            a.destroy(@as(*Box, @ptrCast(@alignCast(ctx))));
        }
    };
    return .{
        .ctx = box,
        .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
        .allocator = allocator,
    };
}

/// Stream over a channel: `next()` is `ch.recv()`, ending (`null`) when
/// the channel is closed. `ch` is a `*Spsc`/`*Mpmc`/… — any channel with
/// `recv() error{Closed}!T`. The channel must outlive the stream.
pub fn fromChannel(allocator: Allocator, ch: anytype) Allocator.Error!Stream(ChanItem(@TypeOf(ch))) {
    const T = ChanItem(@TypeOf(ch));
    const ChPtr = @TypeOf(ch);
    const Box = struct { ch: ChPtr };
    const box = try allocator.create(Box);
    box.* = .{ .ch = ch };
    const Impl = struct {
        fn next(ctx: *anyopaque) anyerror!?T {
            const b: *Box = @ptrCast(@alignCast(ctx));
            // recv() only errors `Closed` → treat as end-of-stream.
            return b.ch.recv() catch return null;
        }
        fn deinit(ctx: *anyopaque, a: Allocator) void {
            a.destroy(@as(*Box, @ptrCast(@alignCast(ctx))));
        }
    };
    return .{
        .ctx = box,
        .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
        .allocator = allocator,
    };
}

/// The general source hook: `gen(ctx)` produces the next item, `null` at
/// end, or an error. This is what the I/O-source libs build on — `gen`
/// does the page-fetch / socket-read / cursor-advance. `ctx` is a
/// caller-owned pointer (not freed by the stream); `gen` must return
/// `!?T` (an error union of optional). `T` is inferred from `gen`.
pub fn generate(allocator: Allocator, ctx: anytype, comptime gen: anytype) Allocator.Error!Stream(GenItem(@TypeOf(gen))) {
    const T = GenItem(@TypeOf(gen));
    const Ctx = @TypeOf(ctx);
    const Box = struct { ctx: Ctx };
    const box = try allocator.create(Box);
    box.* = .{ .ctx = ctx };
    const Impl = struct {
        fn next(c: *anyopaque) anyerror!?T {
            const b: *Box = @ptrCast(@alignCast(c));
            return gen(b.ctx);
        }
        fn deinit(c: *anyopaque, a: Allocator) void {
            a.destroy(@as(*Box, @ptrCast(@alignCast(c))));
        }
    };
    return .{
        .ctx = box,
        .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
        .allocator = allocator,
    };
}

// ─── comptime type inference helpers ─────────────────────────────────

/// `T` from a channel pointer type whose `recv` returns `error{...}!T`.
fn ChanItem(comptime ChPtr: type) type {
    const Chan = std.meta.Child(ChPtr);
    const ret = @typeInfo(@TypeOf(Chan.recv)).@"fn".return_type.?;
    return @typeInfo(ret).error_union.payload;
}

/// `T` from a generator fn whose return type is `error{...}!?T`.
fn GenItem(comptime GenFn: type) type {
    const ret = @typeInfo(GenFn).@"fn".return_type.?;
    const opt = @typeInfo(ret).error_union.payload;
    return @typeInfo(opt).optional.child;
}

// ─── Tests ───────────────────────────────────────────────────────────

const testing = std.testing;

test "fromSlice: toList round-trips" {
    const items = [_]u32{ 1, 2, 3, 4 };
    var s = try fromSlice(u32, testing.allocator, &items);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &items, list);
}

test "fromSlice: count / first / fold" {
    const items = [_]u32{ 10, 20, 30 };

    var s1 = try fromSlice(u32, testing.allocator, &items);
    defer s1.deinit();
    try testing.expectEqual(@as(usize, 3), try s1.count());

    var s2 = try fromSlice(u32, testing.allocator, &items);
    defer s2.deinit();
    try testing.expectEqual(@as(?u32, 10), try s2.first());

    var s3 = try fromSlice(u32, testing.allocator, &items);
    defer s3.deinit();
    const sum = try s3.fold(u32, 0, struct {
        fn add(acc: u32, v: u32) u32 {
            return acc + v;
        }
    }.add);
    try testing.expectEqual(@as(u32, 60), sum);
}

test "fromSlice: empty stream" {
    const items = [_]u32{};
    var s = try fromSlice(u32, testing.allocator, &items);
    defer s.deinit();
    try testing.expectEqual(@as(?u32, null), try s.next());
    try testing.expectEqual(@as(?u32, null), try s.first());
}

var forEach_sum: u32 = 0;

test "forEach: visits every item" {
    forEach_sum = 0;
    const items = [_]u32{ 1, 2, 3 };
    var s = try fromSlice(u32, testing.allocator, &items);
    defer s.deinit();
    try s.forEach(struct {
        fn f(v: u32) void {
            forEach_sum += v;
        }
    }.f);
    try testing.expectEqual(@as(u32, 6), forEach_sum);
}

test "generate: finite counter sequence" {
    const Counter = struct { n: u32, limit: u32 };
    var c = Counter{ .n = 0, .limit = 5 };
    var s = try generate(testing.allocator, &c, struct {
        fn gen(ctx: *Counter) anyerror!?u32 {
            if (ctx.n >= ctx.limit) return null;
            defer ctx.n += 1;
            return ctx.n;
        }
    }.gen);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 2, 3, 4 }, list);
}

test "generate: a failing generator surfaces the error" {
    const Ctx = struct { n: u32 };
    var c = Ctx{ .n = 0 };
    var s = try generate(testing.allocator, &c, struct {
        fn gen(ctx: *Ctx) anyerror!?u32 {
            ctx.n += 1;
            if (ctx.n == 3) return error.Boom;
            return ctx.n;
        }
    }.gen);
    defer s.deinit();
    try testing.expectEqual(@as(?u32, 1), try s.next());
    try testing.expectEqual(@as(?u32, 2), try s.next());
    try testing.expectError(error.Boom, s.next());
}

// fromChannel needs a live producer → run inside a Runtime.
const lib = @import("lib.zig");
const channel = @import("channel.zig");

const ChanTestState = struct {
    ch: *channel.Spsc(u32, 8),
    ok: bool = false,
};

fn channelProducer(ch: *channel.Spsc(u32, 8)) void {
    var i: u32 = 0;
    while (i < 5) : (i += 1) ch.send(i) catch return;
    ch.close();
}

fn channelStreamBody(state: *ChanTestState) !void {
    var producer = try lib.spawn(channelProducer, .{state.ch});
    defer producer.join();

    var s = try fromChannel(lib.testing.allocator, state.ch);
    defer s.deinit();
    const list = try s.toList(lib.testing.allocator);
    defer lib.testing.allocator.free(list);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 2, 3, 4 }, list);
    state.ok = true;
}

test "fromChannel: drains a producer then ends on close" {
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var ch = channel.Spsc(u32, 8){};
    var state = ChanTestState{ .ch = &ch };
    try (try rt.run(channelStreamBody, .{&state}));
    try testing.expect(state.ok);
}

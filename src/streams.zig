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
const cancel_mod = @import("cancel.zig");
const lib = @import("lib.zig");
const channel = @import("channel.zig");

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

        // ─── Lazy operators ──────────────────────────────────────────
        //
        // Each boxes its state once (via the carried allocator) and
        // returns a new Stream wrapping `self`. They **consume the
        // receiver**: don't use or `deinit` a stream after passing it to
        // an operator — `deinit` only the final stream, which walks +
        // frees the whole chain. (Rust-iterator move semantics.)
        // `f`/`pred` are comptime → monomorphized into the stage's
        // `next` (inlined; the only indirection is the one vtable hop).

        /// Transform each item with an infallible `f: fn (T) U`.
        pub fn map(self: Self, comptime f: anytype) Allocator.Error!Stream(MapU(@TypeOf(f))) {
            const U = MapU(@TypeOf(f));
            const Box = struct { up: Self };
            const box = try self.allocator.create(Box);
            box.* = .{ .up = self };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?U {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    const v = (try b.up.next()) orelse return null;
                    return f(v);
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    b.up.deinit();
                    a.destroy(b);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Transform each item with a fallible `f: fn (T) !U`; the first
        /// error ends the stream (surfaces from the terminal).
        pub fn mapTry(self: Self, comptime f: anytype) Allocator.Error!Stream(MapTryU(@TypeOf(f))) {
            const U = MapTryU(@TypeOf(f));
            const Box = struct { up: Self };
            const box = try self.allocator.create(Box);
            box.* = .{ .up = self };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?U {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    const v = (try b.up.next()) orelse return null;
                    return try f(v);
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    b.up.deinit();
                    a.destroy(b);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Keep only items where `pred: fn (T) bool` holds.
        pub fn filter(self: Self, comptime pred: fn (T) bool) Allocator.Error!Self {
            const Box = struct { up: Self };
            const box = try self.allocator.create(Box);
            box.* = .{ .up = self };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?T {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    while (try b.up.next()) |v| {
                        if (pred(v)) return v;
                    }
                    return null;
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    b.up.deinit();
                    a.destroy(b);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Yield at most the first `n` items, then end.
        pub fn take(self: Self, n: usize) Allocator.Error!Self {
            const Box = struct { up: Self, remaining: usize };
            const box = try self.allocator.create(Box);
            box.* = .{ .up = self, .remaining = n };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?T {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    if (b.remaining == 0) return null;
                    const v = (try b.up.next()) orelse {
                        b.remaining = 0;
                        return null;
                    };
                    b.remaining -= 1;
                    return v;
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    b.up.deinit();
                    a.destroy(b);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Skip the first `n` items, yield the rest.
        pub fn drop(self: Self, n: usize) Allocator.Error!Self {
            const Box = struct { up: Self, to_drop: usize };
            const box = try self.allocator.create(Box);
            box.* = .{ .up = self, .to_drop = n };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?T {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    while (b.to_drop > 0) {
                        _ = (try b.up.next()) orelse {
                            b.to_drop = 0;
                            return null;
                        };
                        b.to_drop -= 1;
                    }
                    return b.up.next();
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const b: *Box = @ptrCast(@alignCast(ctx));
                    b.up.deinit();
                    a.destroy(b);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        // ─── Slice 4: combine / concurrency ──────────────────────────

        /// Pairwise-combine with `other` via `f: fn (T, B) C`, ending when
        /// *either* stream ends. Sequential — no coroutine spawn; pulls
        /// one item from each per output. Consumes both streams.
        pub fn zip(self: Self, other: anytype, comptime f: anytype) Allocator.Error!Stream(MapU(@TypeOf(f))) {
            const C = MapU(@TypeOf(f));
            const Other = @TypeOf(other);
            const Box = struct { a: Self, b: Other };
            const box = try self.allocator.create(Box);
            box.* = .{ .a = self, .b = other };
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?C {
                    const bx: *Box = @ptrCast(@alignCast(ctx));
                    const av = (try bx.a.next()) orelse return null;
                    const bv = (try bx.b.next()) orelse return null;
                    return f(av, bv);
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const bx: *Box = @ptrCast(@alignCast(ctx));
                    bx.a.deinit();
                    bx.b.deinit();
                    a.destroy(bx);
                }
            };
            return .{
                .ctx = box,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Prefetch up to `n` items ahead on a background coroutine, so a
        /// slow consumer doesn't stall a fast (or latency-bursty) source.
        /// Spawns one producer + an SPSC buffer — **call from inside a
        /// coroutine.**
        ///
        /// Teardown contract: drain to completion (until `next()` returns
        /// `null`/error) before `deinit`, or ensure the upstream is
        /// finite. `deinit` closes the buffer (waking a producer parked on
        /// `send`) then joins the producer; it can only hang if you
        /// abandon early while the producer is parked pulling an *infinite*
        /// upstream — cancel that upstream's source first.
        pub fn buffered(self: Self, comptime n: usize) (lib.SpawnError || Allocator.Error)!Self {
            const State = struct {
                up: Self,
                ch: channel.Spsc(T, n) = .{},
                producer: ?*lib.Task(void) = null,
                err: ?anyerror = null,
            };
            const st = try self.allocator.create(State);
            st.* = .{ .up = self };
            errdefer {
                st.up.deinit();
                self.allocator.destroy(st);
            }
            const Worker = struct {
                fn run(s: *State) void {
                    defer s.up.deinit();
                    defer s.ch.close();
                    while (true) {
                        const item = s.up.next() catch |e| {
                            s.err = e; // published before close (happens-before via channel)
                            return;
                        };
                        const v = item orelse return;
                        s.ch.send(v) catch return; // consumer closed the buffer
                    }
                }
            };
            st.producer = try lib.spawn(Worker.run, .{st});
            const Impl = struct {
                fn next(ctx: *anyopaque) anyerror!?T {
                    const s: *State = @ptrCast(@alignCast(ctx));
                    const v = s.ch.recv() catch {
                        if (s.err) |e| {
                            s.err = null;
                            return e;
                        }
                        return null;
                    };
                    return v;
                }
                fn deinit(ctx: *anyopaque, a: Allocator) void {
                    const s: *State = @ptrCast(@alignCast(ctx));
                    s.ch.close(); // wake the producer if parked on send
                    if (s.producer) |p| _ = p.join();
                    a.destroy(s);
                }
            };
            return .{
                .ctx = st,
                .vtable = &.{ .next = Impl.next, .deinit = Impl.deinit },
                .allocator = self.allocator,
            };
        }

        /// Drain this stream into `ch` on a background coroutine, closing
        /// `ch` when the stream ends or errors. Returns the producer
        /// `*Task(anyerror!void)` — `join()` it to observe completion + any
        /// stream error. The producer owns + `deinit`s the stream. **Call
        /// from inside a coroutine.** Fan-out / decouple: lets `select`
        /// and other consumers work on the channel side.
        pub fn intoChannel(self: Self, ch: anytype) lib.SpawnError!*lib.Task(anyerror!void) {
            const ChPtr = @TypeOf(ch);
            const Worker = struct {
                fn run(s: Self, c: ChPtr) anyerror!void {
                    var stream = s;
                    defer stream.deinit(); // runs after c.close()
                    defer c.close();
                    while (try stream.next()) |v| {
                        c.send(v) catch return; // consumer closed the channel
                    }
                }
            };
            return lib.spawn(Worker.run, .{ self, ch });
        }
    };
}

/// `U` from an infallible map fn `fn (T) U`.
fn MapU(comptime F: type) type {
    return @typeInfo(F).@"fn".return_type.?;
}

/// `U` from a fallible map fn `fn (T) E!U`.
fn MapTryU(comptime F: type) type {
    return @typeInfo(@typeInfo(F).@"fn".return_type.?).error_union.payload;
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

/// Like `fromChannel`, but parks on `recvCancel(c)` so a fired cancel
/// surfaces as `error.Cancelled` from `next()` — and propagates up
/// through any operators automatically (they all `try upstream.next()`).
/// The channel + cancel must outlive the stream. This is the cancellable
/// counterpart for the built-in channel source; `generate`-based sources
/// thread their own cancel via `ctx` (call `readCancel(ctx.cancel)` etc.
/// inside the generator).
pub fn fromChannelCancel(
    allocator: Allocator,
    ch: anytype,
    c: *cancel_mod.Cancel,
) Allocator.Error!Stream(ChanItem(@TypeOf(ch))) {
    const T = ChanItem(@TypeOf(ch));
    const ChPtr = @TypeOf(ch);
    const Box = struct { ch: ChPtr, c: *cancel_mod.Cancel };
    const box = try allocator.create(Box);
    box.* = .{ .ch = ch, .c = c };
    const Impl = struct {
        fn next(ctx: *anyopaque) anyerror!?T {
            const b: *Box = @ptrCast(@alignCast(ctx));
            const v = b.ch.recvCancel(b.c) catch |e| {
                if (e == error.Closed) return null; // end of stream
                return e; // error.Cancelled propagates to the caller
            };
            return v;
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

/// Fan-in: concurrently pull `inputs` and interleave them into one
/// stream. One producer coroutine per input feeds a shared MPMC buffer
/// (capacity `cap`); the merged `next()` recv's from it. Output order is
/// nondeterministic. The first error from any input ends the merge and
/// surfaces from `next()`. Consumes the input streams (don't `deinit`
/// them yourself — the producers do). **Call from inside a coroutine.**
///
/// Same teardown contract as `buffered`: drain to completion or use
/// finite inputs; `deinit` closes the buffer (waking producers parked on
/// `send`) and joins every producer.
pub fn merge(
    comptime T: type,
    comptime cap: usize,
    allocator: Allocator,
    inputs: []const Stream(T),
) (lib.SpawnError || Allocator.Error)!Stream(T) {
    const State = struct {
        ch: channel.Mpmc(T, cap) = .{},
        inputs: []Stream(T),
        tasks: []?*lib.Task(void),
        remaining: std.atomic.Value(usize),
        err_set: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        err: ?anyerror = null,
    };

    const st = try allocator.create(State);
    errdefer allocator.destroy(st);
    const inputs_copy = try allocator.alloc(Stream(T), inputs.len);
    errdefer allocator.free(inputs_copy);
    @memcpy(inputs_copy, inputs);
    const tasks = try allocator.alloc(?*lib.Task(void), inputs.len);
    errdefer allocator.free(tasks);
    @memset(tasks, null);
    st.* = .{
        .inputs = inputs_copy,
        .tasks = tasks,
        .remaining = std.atomic.Value(usize).init(inputs.len),
    };

    const Producer = struct {
        fn run(s: *State, idx: usize) void {
            defer s.inputs[idx].deinit();
            while (true) {
                const item = s.inputs[idx].next() catch |e| {
                    // First error wins; publish before close (happens-before).
                    if (s.err_set.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) s.err = e;
                    s.ch.close();
                    return;
                };
                const v = item orelse break;
                s.ch.send(v) catch return; // buffer closed (consumer done / sibling errored)
            }
            // Input exhausted cleanly; the last producer closes the buffer.
            if (s.remaining.fetchSub(1, .acq_rel) == 1) s.ch.close();
        }
    };

    var i: usize = 0;
    while (i < inputs_copy.len) : (i += 1) {
        st.tasks[i] = lib.spawn(Producer.run, .{ st, i }) catch |e| {
            // Spawn failed mid-way: shut down what's running, deinit the
            // not-yet-spawned inputs, then let the errdefers free memory.
            st.ch.close();
            var j: usize = 0;
            while (j < i) : (j += 1) {
                if (st.tasks[j]) |t| _ = t.join();
            }
            var k: usize = i;
            while (k < inputs_copy.len) : (k += 1) inputs_copy[k].deinit();
            return e;
        };
    }
    // No inputs → an immediately-closed (empty) stream, else next() hangs.
    if (inputs_copy.len == 0) st.ch.close();

    const Impl = struct {
        fn next(ctx: *anyopaque) anyerror!?T {
            const s: *State = @ptrCast(@alignCast(ctx));
            const v = s.ch.recv() catch {
                if (s.err) |e| {
                    s.err = null;
                    return e;
                }
                return null;
            };
            return v;
        }
        fn deinit(ctx: *anyopaque, a: Allocator) void {
            const s: *State = @ptrCast(@alignCast(ctx));
            s.ch.close(); // wake producers parked on send
            for (s.tasks) |t| {
                if (t) |task| _ = task.join();
            }
            a.free(s.inputs);
            a.free(s.tasks);
            a.destroy(s);
        }
    };
    return .{
        .ctx = st,
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

// ─── Slice 2: lazy operators ─────────────────────────────────────────

test "map: transforms each item (incl. changing type)" {
    const items = [_]u32{ 1, 2, 3 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.map(struct {
        fn f(v: u32) u64 {
            return @as(u64, v) * 100;
        }
    }.f);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u64, &[_]u64{ 100, 200, 300 }, list);
}

test "filter: keeps matching items" {
    const items = [_]u32{ 1, 2, 3, 4, 5, 6 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.filter(struct {
        fn even(v: u32) bool {
            return v % 2 == 0;
        }
    }.even);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &[_]u32{ 2, 4, 6 }, list);
}

test "take: yields at most n" {
    const items = [_]u32{ 1, 2, 3, 4, 5 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.take(3);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3 }, list);
}

test "take: more than available is fine" {
    const items = [_]u32{ 1, 2 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.take(10);
    defer s.deinit();
    try testing.expectEqual(@as(usize, 2), try s.count());
}

test "drop: skips first n" {
    const items = [_]u32{ 1, 2, 3, 4, 5 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.drop(2);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &[_]u32{ 3, 4, 5 }, list);
}

test "mapTry: first error ends the stream" {
    const items = [_]u32{ 1, 2, 3 };
    const src = try fromSlice(u32, testing.allocator, &items);
    var s = try src.mapTry(struct {
        fn f(v: u32) anyerror!u32 {
            if (v == 2) return error.Bad;
            return v * 10;
        }
    }.f);
    defer s.deinit();
    try testing.expectEqual(@as(?u32, 10), try s.next());
    try testing.expectError(error.Bad, s.next());
}

test "chain: filter -> map -> take frees the whole pipeline" {
    const items = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const src = try fromSlice(u32, testing.allocator, &items);
    const evens = try src.filter(struct {
        fn even(v: u32) bool {
            return v % 2 == 0;
        }
    }.even);
    const scaled = try evens.map(struct {
        fn f(v: u32) u32 {
            return v * 10;
        }
    }.f);
    var s = try scaled.take(2);
    defer s.deinit(); // walks take -> map -> filter -> fromSlice
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u32, &[_]u32{ 20, 40 }, list);
}

// ─── Slice 3: cancellation ───────────────────────────────────────────

const CancelState = struct {
    ch: *channel.Spsc(u32, 8),
    c: *lib.Cancel,
    got_cancelled: bool = false,
};

fn cancelReader(state: *CancelState) void {
    // map on top of the cancellable source: proves Cancelled propagates
    // up through an operator, not just out of the bare source.
    const src = fromChannelCancel(lib.testing.allocator, state.ch, state.c) catch return;
    var s = src.map(struct {
        fn f(v: u32) u32 {
            return v + 1;
        }
    }.f) catch return;
    defer s.deinit();
    // The channel is empty with no producer, so next() parks on
    // recvCancel until the sibling fires the cancel.
    _ = s.next() catch |e| {
        if (e == error.Cancelled) state.got_cancelled = true;
        return;
    };
}

fn cancelFirer(state: *CancelState) void {
    var i: u32 = 0;
    while (i < 128) : (i += 1) lib.yield();
    state.c.fire();
}

fn cancelBody(state: *CancelState) !void {
    var reader = try lib.spawn(cancelReader, .{state});
    var firer = try lib.spawn(cancelFirer, .{state});
    _ = reader.join();
    _ = firer.join();
}

test "fromChannelCancel: cancel while parked propagates Cancelled through map" {
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var ch = channel.Spsc(u32, 8){};
    var cancel = lib.Cancel.init(rt);
    defer cancel.deinit();
    var state = CancelState{ .ch = &ch, .c = &cancel };
    try (try rt.run(cancelBody, .{&state}));
    try testing.expect(state.got_cancelled);
}

// ─── Slice 4: combine / concurrency ──────────────────────────────────

test "zip: pairwise combine, ends at the shorter stream" {
    const a = [_]u32{ 1, 2, 3 };
    const b = [_]u64{ 10, 20, 30, 40 };
    const sa = try fromSlice(u32, testing.allocator, &a);
    const sb = try fromSlice(u64, testing.allocator, &b);
    var s = try sa.zip(sb, struct {
        fn f(x: u32, y: u64) u64 {
            return @as(u64, x) + y;
        }
    }.f);
    defer s.deinit();
    const list = try s.toList(testing.allocator);
    defer testing.allocator.free(list);
    try testing.expectEqualSlices(u64, &[_]u64{ 11, 22, 33 }, list);
}

const BufState = struct { ok: bool = false };

fn bufferedDrainBody(state: *BufState) !void {
    const items = [_]u32{ 1, 2, 3, 4, 5, 6 };
    const src = try fromSlice(u32, lib.testing.allocator, &items);
    var s = try src.buffered(2);
    defer s.deinit();
    const list = try s.toList(lib.testing.allocator);
    defer lib.testing.allocator.free(list);
    try std.testing.expectEqualSlices(u32, &items, list);
    state.ok = true;
}

test "buffered: prefetches + drains in order" {
    const guard = lib.testing.deadlineGuard(15, "buffered drain");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var state = BufState{};
    try (try rt.run(bufferedDrainBody, .{&state}));
    try testing.expect(state.ok);
}

fn bufferedAbandonBody(state: *BufState) !void {
    const items = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const src = try fromSlice(u32, lib.testing.allocator, &items);
    var s = try src.buffered(2);
    // Take only two, then abandon (deinit without draining). Finite
    // upstream → the producer is parked on `send` (buffer full); deinit's
    // close + join must tear down without hang or leak.
    _ = try s.next();
    _ = try s.next();
    s.deinit();
    state.ok = true;
}

test "buffered: early abandon of a finite upstream tears down cleanly" {
    const guard = lib.testing.deadlineGuard(15, "buffered abandon");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var state = BufState{};
    try (try rt.run(bufferedAbandonBody, .{&state}));
    try testing.expect(state.ok);
}

const IntoState = struct { sum: u32 = 0, ok: bool = false };

fn intoChannelBody(state: *IntoState) !void {
    var ch = channel.Spsc(u32, 4){};
    const items = [_]u32{ 1, 2, 3, 4, 5 };
    const src = try fromSlice(u32, lib.testing.allocator, &items);
    var task = try src.intoChannel(&ch);
    while (true) {
        const v = ch.recv() catch break; // Closed when the producer ends
        state.sum += v;
    }
    _ = task.join() catch {}; // observe completion; no error expected
    state.ok = true;
}

test "intoChannel: drains a stream into a channel" {
    const guard = lib.testing.deadlineGuard(15, "intoChannel");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var state = IntoState{};
    try (try rt.run(intoChannelBody, .{&state}));
    try testing.expectEqual(@as(u32, 15), state.sum);
    try testing.expect(state.ok);
}

const MergeState = struct { sum: u32 = 0, n: usize = 0, ok: bool = false };

fn mergeFanInBody(state: *MergeState) !void {
    const a = [_]u32{ 1, 2, 3 };
    const b = [_]u32{ 10, 20 };
    const c = [_]u32{100};
    var inputs = [_]Stream(u32){
        try fromSlice(u32, lib.testing.allocator, &a),
        try fromSlice(u32, lib.testing.allocator, &b),
        try fromSlice(u32, lib.testing.allocator, &c),
    };
    var s = try merge(u32, 8, lib.testing.allocator, &inputs);
    defer s.deinit();
    while (try s.next()) |v| {
        state.sum += v;
        state.n += 1;
    }
    state.ok = true;
}

test "merge: fans in N streams (order-independent)" {
    const guard = lib.testing.deadlineGuard(15, "merge fan-in");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 3 });
    defer rt.deinit();
    var state = MergeState{};
    try (try rt.run(mergeFanInBody, .{&state}));
    try testing.expectEqual(@as(usize, 6), state.n);
    try testing.expectEqual(@as(u32, 136), state.sum); // 1+2+3+10+20+100
}

fn mergeErrBody(state: *MergeState) !void {
    const a = [_]u32{ 1, 2, 3, 4 };
    const Ctx = struct { n: u32 };
    var ec = Ctx{ .n = 0 };
    var inputs = [_]Stream(u32){
        try fromSlice(u32, lib.testing.allocator, &a),
        try generate(lib.testing.allocator, &ec, struct {
            fn gen(c: *Ctx) anyerror!?u32 {
                c.n += 1;
                if (c.n == 2) return error.Boom;
                return 50;
            }
        }.gen),
    };
    var s = try merge(u32, 8, lib.testing.allocator, &inputs);
    defer s.deinit();
    var saw_err = false;
    while (true) {
        const item = s.next() catch |e| {
            if (e == error.Boom) saw_err = true;
            break;
        };
        if (item == null) break;
    }
    state.ok = saw_err;
}

test "merge: first input error ends the merge" {
    const guard = lib.testing.deadlineGuard(15, "merge error");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 3 });
    defer rt.deinit();
    var state = MergeState{};
    try (try rt.run(mergeErrBody, .{&state}));
    try testing.expect(state.ok);
}

fn mergeEmptyBody(state: *MergeState) !void {
    var inputs = [_]Stream(u32){};
    var s = try merge(u32, 4, lib.testing.allocator, &inputs);
    defer s.deinit();
    try std.testing.expectEqual(@as(?u32, null), try s.next());
    state.ok = true;
}

test "merge: empty input set yields an empty stream" {
    const guard = lib.testing.deadlineGuard(15, "merge empty");
    defer guard.disarm();
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 2 });
    defer rt.deinit();
    var state = MergeState{};
    try (try rt.run(mergeEmptyBody, .{&state}));
    try testing.expect(state.ok);
}

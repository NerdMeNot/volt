//! `select(.{...})` — first-of-N event multiplexer over channels
//! and timers.
//!
//! Composes existing `*Cancel`-suffixed primitives
//! (`recvCancel` / `sendCancel` / `sleepCancel`). Implementation is
//! "spawn one child coroutine per branch, share a Cancel" — the
//! first child to complete its op CAS-claims the winner slot and
//! fires the Cancel; the other children's `*Cancel` ops return
//! `error.Cancelled` and exit cleanly. No new channel surface, no
//! shadow threads — children dispatch on the existing work-stealing
//! fabric.
//!
//! ## Cost
//!
//! Spawn + join per branch per `select` call (~2 µs / branch on
//! Darwin arm64). Cheap enough for HTTP-server-style "request OR
//! shutdown" loops where select fires once per request, expensive
//! for tight loops that select-then-loop. A future optimization
//! could comptime-generate a single coroutine that parks on all N
//! parking keys simultaneously; deferred until profiling motivates.
//!
//! ## API shape
//!
//! ```zig
//! const r = try volt.select(.{
//!     .data    = volt.selectRecv(&ch),
//!     .timeout = volt.selectTimer(Duration.fromMillis(100)),
//! });
//! switch (r) {
//!     .data => |v| ...,
//!     .timeout => ...,
//! }
//! ```
//!
//! Each field is a *branch builder* — `selectRecv`, `selectSend`,
//! `selectTimer` — that returns a stack-allocated value carrying
//! the channel pointer / duration / value. `select` extracts the
//! payload type from each builder via its `Payload` field and
//! generates the result tagged union accordingly.

const std = @import("std");
const builtin = @import("builtin");
const cancel_mod = @import("cancel.zig");
const lib = @import("lib.zig");
const current = @import("current.zig");
const runtime_mod = @import("runtime.zig");

// ─── Branch builders ─────────────────────────────────────────────

/// Branch: receive from `ch`. Payload is the channel's element
/// type (`Ch.Payload`). Used inside a `select(.{...})` field.
pub fn selectRecv(ch: anytype) RecvBranch(@typeInfo(@TypeOf(ch)).pointer.child) {
    return .{ .ch = ch };
}

/// Branch: send `v` to `ch`. Payload is `void` — success means
/// the value was handed off.
pub fn selectSend(ch: anytype, v: anytype) SendBranch(@typeInfo(@TypeOf(ch)).pointer.child) {
    return .{ .ch = ch, .v = v };
}

/// Branch: timer expiration after `d`. Payload is `void`.
pub fn selectTimer(d: lib.Duration) TimerBranch {
    return .{ .dur = d };
}

pub fn RecvBranch(comptime Ch: type) type {
    return struct {
        ch: *Ch,
        pub const Payload = Ch.Payload;
        pub fn run(self: @This(), c: *cancel_mod.Cancel) !Payload {
            return self.ch.recvCancel(c);
        }
    };
}

pub fn SendBranch(comptime Ch: type) type {
    return struct {
        ch: *Ch,
        v: Ch.Payload,
        pub const Payload = void;
        pub fn run(self: @This(), c: *cancel_mod.Cancel) !void {
            return self.ch.sendCancel(self.v, c);
        }
    };
}

pub const TimerBranch = struct {
    dur: lib.Duration,
    pub const Payload = void;
    pub fn run(self: @This(), c: *cancel_mod.Cancel) !void {
        return lib.sleepCancel(self.dur, c);
    }
};

// ─── select() ────────────────────────────────────────────────────

/// First-of-N branches. Returns the winning branch's variant of
/// the result tagged union. See module docs.
///
/// Errors propagate from `spawn` (resource exhaustion) and from
/// the winning branch's run function (e.g. `error.Closed` if a
/// channel was closed while the branch was parked). `error.Cancelled`
/// from a losing branch is consumed internally — it's the signal
/// "another branch won," not a real error.
pub fn select(branches: anytype) !SelectResult(@TypeOf(branches)) {
    const BranchesT = @TypeOf(branches);
    const fields = @typeInfo(BranchesT).@"struct".fields;
    if (comptime fields.len == 0) @compileError("select needs at least one branch");

    const Result = SelectResult(BranchesT);

    // Per-branch result slot + a winner index. The first child to
    // succeed CAS-claims the winner slot (atomically) and writes
    // its result; the others' ops return error.Cancelled and exit
    // without touching anything.
    // Only the winning child writes its slot; the rest remain
    // undefined. We never read a non-winner slot because the
    // post-join switch indexes by `winner`.
    const SlotsT = Slots(BranchesT);
    var slots: SlotsT = undefined;
    var winner = std.atomic.Value(i32).init(-1);

    var c = cancel_mod.Cancel.init(lib.runtime());
    defer c.deinit();

    // Spawn one child per branch. Children all share `&c` + `&slots`
    // + `&winner`.
    var children: [fields.len]*lib.Task(void) = undefined;
    inline for (fields, 0..) |f, i| {
        const branch = @field(branches, f.name);
        const ChildCtx = struct {
            br: f.type,
            slots: *SlotsT,
            winner: *std.atomic.Value(i32),
            cancel: *cancel_mod.Cancel,

            fn entry(self: *@This()) void {
                const r = self.br.run(self.cancel);
                // CAS-claim the winner slot. The winner index
                // doubles as the publish-fence for the result.
                if (self.winner.cmpxchgStrong(-1, @as(i32, @intCast(i)), .acq_rel, .acquire) == null) {
                    @field(self.slots, f.name) = r;
                    self.cancel.fire();
                }
            }
        };
        var ctx = ChildCtx{
            .br = branch,
            .slots = &slots,
            .winner = &winner,
            .cancel = &c,
        };
        children[i] = try lib.spawn(ChildCtx.entry, .{&ctx});
    }

    // Wait for all children to settle. The winner already fired
    // the cancel, so the losers will return promptly.
    for (children) |child| _ = child.join();

    // Read the winner and construct the typed union.
    const winning_idx = winner.load(.acquire);
    std.debug.assert(winning_idx >= 0);

    var r: Result = undefined;
    inline for (fields, 0..) |f, i| {
        if (winning_idx == @as(i32, @intCast(i))) {
            const slot_value = try @field(slots, f.name);
            r = @unionInit(Result, f.name, slot_value);
        }
    }
    return r;
}

// ─── Comptime type machinery ─────────────────────────────────────

/// Result tagged union for `select(branches)`. One variant per
/// field of `branches`, payload = that branch's `Payload`.
pub fn SelectResult(comptime BranchesT: type) type {
    const fields = @typeInfo(BranchesT).@"struct".fields;
    comptime var names: [fields.len][]const u8 = undefined;
    comptime var tag_vals: [fields.len]std.math.IntFittingRange(0, fields.len) = undefined;
    comptime var union_types: [fields.len]type = undefined;
    comptime var union_attrs: [fields.len]std.builtin.Type.UnionField.Attributes = undefined;
    inline for (fields, 0..) |f, i| {
        names[i] = f.name;
        tag_vals[i] = @intCast(i);
        union_types[i] = f.type.Payload;
        union_attrs[i] = .{};
    }
    const Tag = @Enum(
        std.math.IntFittingRange(0, fields.len),
        .exhaustive,
        &names,
        &tag_vals,
    );
    return @Union(
        .auto,
        Tag,
        &names,
        &union_types,
        &union_attrs,
    );
}

/// Per-branch result-slot struct, used internally to publish the
/// winning branch's result. Each field's type is `anyerror!Payload`
/// so a winning branch's error (e.g. `error.Closed` on a recv from
/// a closed channel) propagates out of `select`; the user's switch
/// only ever sees the success payload variant.
fn Slots(comptime BranchesT: type) type {
    const fields = @typeInfo(BranchesT).@"struct".fields;
    comptime var names: [fields.len][]const u8 = undefined;
    comptime var types: [fields.len]type = undefined;
    comptime var attrs: [fields.len]std.builtin.Type.StructField.Attributes = undefined;
    inline for (fields, 0..) |f, i| {
        names[i] = f.name;
        types[i] = anyerror!f.type.Payload;
        attrs[i] = .{};
    }
    return @Struct(.auto, null, &names, &types, &attrs);
}

// ─── Tests ───────────────────────────────────────────────────────

const channel = @import("channel.zig");

const SelectRecvTimerCtx = struct {
    ch: *channel.Spsc(u32, 4),
    result_tag: u8 = 0xFF, // 0=recv, 1=timeout, 0xFF=unset
    result_val: u32 = 0,
};

fn selectRecvTimerProducer(ch: *channel.Spsc(u32, 4)) void {
    // Wait a bit, then send.
    lib.sleep(lib.Duration.fromMillis(2)) catch return;
    ch.send(0xDEAD) catch return;
}

fn selectRecvTimerRoot(ctx: *SelectRecvTimerCtx) !void {
    var prod = try lib.spawn(selectRecvTimerProducer, .{ctx.ch});

    const r = try select(.{
        .recv = selectRecv(ctx.ch),
        .timeout = selectTimer(lib.Duration.fromMillis(200)),
    });

    switch (r) {
        .recv => |v| {
            ctx.result_tag = 0;
            ctx.result_val = v;
        },
        .timeout => ctx.result_tag = 1,
    }
    _ = prod.join();
}

test "select: recv wins against a longer timeout" {
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 1 });
    defer rt.deinit();
    var ch = channel.Spsc(u32, 4){};
    var ctx = SelectRecvTimerCtx{ .ch = &ch };
    try (try rt.run(selectRecvTimerRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u8, 0), ctx.result_tag);
    try std.testing.expectEqual(@as(u32, 0xDEAD), ctx.result_val);
}

const SelectTimeoutCtx = struct { fired_tag: u8 = 0xFF };

fn selectTimeoutRoot(ctx: *SelectTimeoutCtx) !void {
    // Empty channel + short timer → timer should win.
    var ch = channel.Spsc(u32, 4){};
    const r = try select(.{
        .recv = selectRecv(&ch),
        .timeout = selectTimer(lib.Duration.fromMillis(5)),
    });
    ctx.fired_tag = switch (r) {
        .recv => 0,
        .timeout => 1,
    };
}

test "select: timer wins when no channel ever fires" {
    var rt = try lib.Runtime.init(.{ .allocator = lib.testing.allocator, .workers = 1 });
    defer rt.deinit();
    var ctx = SelectTimeoutCtx{};
    try (try rt.run(selectTimeoutRoot, .{&ctx}));
    try std.testing.expectEqual(@as(u8, 1), ctx.fired_tag);
}

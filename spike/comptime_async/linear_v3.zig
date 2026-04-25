//! Spike Phase 2.5: linear with async stack traces.
//!
//! When a Future is pending, the user (or a debugger, or a `tokio-console`-
//! style tool) can ask the Future where it is. The returned info is:
//!   - The Spec name
//!   - The step name (e.g., "step2")
//!   - Optionally, the source file:line of the step declaration
//!
//! Why this matters: Rust async fn lowers to `impl Future` — opaque.
//! `tokio-console` can show you "task is pending" but not "at line 42 of
//! my fetch fn waiting on stream.read". Zig comptime keeps the structure
//! visible, so we can do better than Tokio at debugging.
//!
//! This builds on linear_v2's error propagation — same Spec shape, just
//! adds an introspection method on the generated Future type.

const std = @import("std");
const proto = @import("proto.zig");
const PollResult = proto.PollResult;
const Context = proto.Context;

// Reuse the same step analysis from linear_v2.
const StepKind = struct {
    step_returns_error: bool,
    inner_is_future: bool,
    future_yields_error: bool,
    value_type: type,
    inner_type: type,
};

fn analyzeStepReturn(comptime Ret: type) StepKind {
    comptime {
        var kind = StepKind{
            .step_returns_error = false,
            .inner_is_future = false,
            .future_yields_error = false,
            .value_type = Ret,
            .inner_type = Ret,
        };
        var T = Ret;
        if (@typeInfo(T) == .error_union) {
            kind.step_returns_error = true;
            T = @typeInfo(T).error_union.payload;
            kind.inner_type = T;
        }
        if (proto.isFuture(T)) {
            kind.inner_is_future = true;
            const Out = T.Output;
            if (@typeInfo(Out) == .error_union) {
                kind.future_yields_error = true;
                kind.value_type = @typeInfo(Out).error_union.payload;
            } else {
                kind.value_type = Out;
            }
        } else {
            kind.value_type = T;
        }
        return kind;
    }
}

fn collectSteps(comptime Spec: type) []const []const u8 {
    comptime {
        var names: []const []const u8 = &.{};
        var i: usize = 1;
        while (true) : (i += 1) {
            const name = std.fmt.comptimePrint("step{d}", .{i});
            if (!@hasDecl(Spec, name)) break;
            names = names ++ [_][]const u8{name};
        }
        if (names.len == 0) @compileError("linear: expected at least one step");
        return names;
    }
}

fn StepReturn(comptime Spec: type, comptime step_name: []const u8) type {
    return @typeInfo(@TypeOf(@field(Spec, step_name))).@"fn".return_type.?;
}

// ─────────────────────────────────────────────────────────────────────────────
// Async stack trace info
// ─────────────────────────────────────────────────────────────────────────────

/// Describes where a Future is currently paused. Available even when the
/// future is pending — comptime preserves the chain structure.
pub const StackFrame = struct {
    spec_name: []const u8,
    step_name: []const u8,
    /// Position in the chain: 0 = before step1, N = currently in stepN.
    /// 0 means not started yet; chain.len + 1 means done.
    step_index: usize,
    /// Total step count for context.
    total_steps: usize,
};

// ─────────────────────────────────────────────────────────────────────────────
// State machine generator (linear_v3 — adds whereAmI())
// ─────────────────────────────────────────────────────────────────────────────

pub fn linear(comptime Spec: type) type {
    const step_names = comptime collectSteps(Spec);
    const ChainOutput = Spec.Output;
    const SpecName = @typeName(Spec);

    return struct {
        const Self = @This();
        pub const Output = ChainOutput;

        spec: Spec,
        state: State,

        const State = blk: {
            var future_step_count: usize = 0;
            for (step_names) |sn| {
                const kind = analyzeStepReturn(StepReturn(Spec, sn));
                if (kind.inner_is_future) future_step_count += 1;
            }
            const N: usize = 1 + future_step_count + 1;

            var names_arr: [N][]const u8 = undefined;
            var types_arr: [N]type = undefined;
            var write: usize = 0;

            names_arr[write] = "start";
            types_arr[write] = void;
            write += 1;

            for (step_names, 0..) |sn, idx| {
                const kind = analyzeStepReturn(StepReturn(Spec, sn));
                if (kind.inner_is_future) {
                    names_arr[write] = std.fmt.comptimePrint("await{d}", .{idx + 1});
                    types_arr[write] = kind.inner_type;
                    write += 1;
                }
            }

            names_arr[write] = "done";
            types_arr[write] = void;

            const TagInt = std.math.IntFittingRange(0, N -| 1);
            var values_arr: [N]TagInt = undefined;
            for (0..N) |i| values_arr[i] = @intCast(i);
            const attrs_arr: [N]std.builtin.Type.UnionField.Attributes = @splat(.{});
            const Tag = @Enum(TagInt, .exhaustive, &names_arr, &values_arr);
            break :blk @Union(.auto, Tag, &names_arr, &types_arr, &attrs_arr);
        };

        pub fn init(spec: Spec) Self {
            return .{ .spec = spec, .state = .{ .start = {} } };
        }

        /// Async stack frame — answers "where is this Future paused?"
        /// Available at any point: before start, between steps, after done.
        pub fn whereAmI(self: *const Self) StackFrame {
            // Comptime-known: spec name + step count
            // Runtime: which state tag is active
            inline for (step_names, 0..) |name, idx| {
                const tag_name = comptime std.fmt.comptimePrint("await{d}", .{idx + 1});
                if (@hasField(@TypeOf(self.state), tag_name) and
                    @as(@typeInfo(State).@"union".tag_type.?, self.state) ==
                        @field(@typeInfo(State).@"union".tag_type.?, tag_name))
                {
                    return .{
                        .spec_name = SpecName,
                        .step_name = name,
                        .step_index = idx + 1,
                        .total_steps = step_names.len,
                    };
                }
            }
            if (self.state == .start) {
                return .{
                    .spec_name = SpecName,
                    .step_name = "(not started)",
                    .step_index = 0,
                    .total_steps = step_names.len,
                };
            }
            return .{
                .spec_name = SpecName,
                .step_name = "(done)",
                .step_index = step_names.len + 1,
                .total_steps = step_names.len,
            };
        }

        pub fn poll(self: *Self, ctx: *Context) PollResult(Output) {
            while (true) {
                if (self.state == .start) {
                    const k = comptime analyzeStepReturn(StepReturn(Spec, "step1"));
                    const result = if (comptime k.step_returns_error)
                        self.spec.step1() catch |err| {
                            self.state = .{ .done = {} };
                            return .{ .ready = err };
                        }
                    else
                        self.spec.step1();

                    if (comptime k.inner_is_future) {
                        self.state = @unionInit(State, "await1", result);
                        continue;
                    } else {
                        self.state = .{ .done = {} };
                        return .{ .ready = result };
                    }
                }
                if (self.state == .done) @panic("linear: polled after ready");

                const drive = pollAwait(self, ctx) catch return .pending;
                if (drive) |out| return .{ .ready = out };
            }
        }

        fn pollAwait(self: *Self, ctx: *Context) error{Pending}!?Output {
            inline for (step_names, 0..) |name, idx| {
                const k = comptime analyzeStepReturn(StepReturn(Spec, name));
                if (comptime k.inner_is_future) {
                    const tag_name = comptime std.fmt.comptimePrint("await{d}", .{idx + 1});
                    if (@as(@typeInfo(State).@"union".tag_type.?, self.state) ==
                        @field(@typeInfo(State).@"union".tag_type.?, tag_name))
                    {
                        const inner_ptr = &@field(self.state, tag_name);
                        const r = inner_ptr.poll(ctx);
                        if (r.isPending()) return error.Pending;
                        const future_out = r.unwrap();

                        const value = if (comptime k.future_yields_error)
                            future_out catch |err| {
                                self.state = .{ .done = {} };
                                return @as(Output, err);
                            }
                        else
                            future_out;

                        const next_idx = idx + 1;
                        if (next_idx >= step_names.len) {
                            self.state = .{ .done = {} };
                            return value;
                        }

                        const next_name = comptime step_names[next_idx];
                        const next_k = comptime analyzeStepReturn(StepReturn(Spec, next_name));
                        const next_method = @field(Spec, next_name);

                        const next_result = if (comptime next_k.step_returns_error)
                            next_method(&self.spec, value) catch |err| {
                                self.state = .{ .done = {} };
                                return @as(Output, err);
                            }
                        else
                            next_method(&self.spec, value);

                        if (comptime next_k.inner_is_future) {
                            const next_tag = comptime std.fmt.comptimePrint("await{d}", .{next_idx + 1});
                            self.state = @unionInit(State, next_tag, next_result);
                            return null;
                        } else {
                            self.state = .{ .done = {} };
                            return next_result;
                        }
                    }
                }
            }
            unreachable;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — async stack traces
// ─────────────────────────────────────────────────────────────────────────────

const Delayed = @import("delayed.zig").Delayed;

test "v3 - whereAmI before start" {
    const Spec = struct {
        a: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a);
        }
    };

    const F = linear(Spec);
    const f = F.init(.{ .a = 42 });

    const where = f.whereAmI();
    try std.testing.expectEqualStrings("(not started)", where.step_name);
    try std.testing.expectEqual(@as(usize, 0), where.step_index);
    try std.testing.expectEqual(@as(usize, 1), where.total_steps);
}

test "v3 - whereAmI mid-chain points at active step" {
    const Spec = struct {
        a: i32,
        b: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a);
        }
        pub fn step2(self: *@This(), prev: i32) Delayed(i32) {
            return Delayed(i32).init(prev + self.b);
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = 10, .b = 5 });
    var ctx = Context{ .waker = &proto.noop_waker };

    // Poll 1: step1 starts, inner Delayed pends
    try std.testing.expect(f.poll(&ctx).isPending());

    // We're now paused inside step1 (its Delayed is pending)
    var where = f.whereAmI();
    try std.testing.expectEqualStrings("step1", where.step_name);
    try std.testing.expectEqual(@as(usize, 1), where.step_index);

    // Poll 2: step1's Delayed becomes ready, step2 is invoked, its Delayed pends
    try std.testing.expect(f.poll(&ctx).isPending());

    // Now paused inside step2
    where = f.whereAmI();
    try std.testing.expectEqualStrings("step2", where.step_name);
    try std.testing.expectEqual(@as(usize, 2), where.step_index);

    // Poll 3: complete
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 15), r.unwrap());

    // Now done
    where = f.whereAmI();
    try std.testing.expectEqualStrings("(done)", where.step_name);
}

test "v3 - whereAmI works for a 5-step chain" {
    const Spec = struct {
        x: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) { return Delayed(i32).init(self.x + 1); }
        pub fn step2(self: *@This(), v: i32) Delayed(i32) { _ = self; return Delayed(i32).init(v + 2); }
        pub fn step3(self: *@This(), v: i32) Delayed(i32) { _ = self; return Delayed(i32).init(v + 3); }
        pub fn step4(self: *@This(), v: i32) Delayed(i32) { _ = self; return Delayed(i32).init(v + 4); }
        pub fn step5(self: *@This(), v: i32) i32 { _ = self; return v + 5; }
    };

    const F = linear(Spec);
    var f = F.init(.{ .x = 0 });
    var ctx = Context{ .waker = &proto.noop_waker };

    // Each Delayed pends once before being ready, so we get 4 pendings.
    var observed_steps: [4][]const u8 = undefined;
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        try std.testing.expect(f.poll(&ctx).isPending());
        observed_steps[i] = f.whereAmI().step_name;
    }
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 15), r.unwrap()); // 0+1+2+3+4+5

    // We should have observed step1, step2, step3, step4 in order.
    try std.testing.expectEqualStrings("step1", observed_steps[0]);
    try std.testing.expectEqualStrings("step2", observed_steps[1]);
    try std.testing.expectEqualStrings("step3", observed_steps[2]);
    try std.testing.expectEqualStrings("step4", observed_steps[3]);
}

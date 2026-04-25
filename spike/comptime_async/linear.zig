//! Spike: comptime-generated state machine for linear async code.
//!
//! User writes a Spec struct with:
//!   - Args/captures as struct fields
//!   - `pub const Output = T` for the terminal type
//!   - `step1`, `step2`, ... methods in order:
//!       - step1: `(self: *Spec) Future` — starts the chain
//!       - stepN (N>1): `(self: *Spec, prev: T) Future_or_Output`
//!     The last step must return `Output` (terminal). Earlier steps return Futures.
//!
//! `linear(Spec)` returns a Future that runs the steps in order, threading the
//! result of each Future-returning step into the next step. Captures (Spec
//! struct fields) are accessible via `self.*` in every step.
//!
//! What this is testing:
//!   - Can comptime introspect step methods and their signatures? (yes, expected)
//!   - Can comptime generate a state union over per-step inner futures? (yes, expected)
//!   - Is the resulting code semantically equivalent to a hand-rolled SM? (this test)
//!   - Is the generated code's runtime cost zero (vs. hand-rolled)? (separate bench)

const std = @import("std");
const proto = @import("proto.zig");
const PollResult = proto.PollResult;
const Context = proto.Context;

// ─────────────────────────────────────────────────────────────────────────────
// Comptime spec introspection
// ─────────────────────────────────────────────────────────────────────────────

/// Find the steps in declaration order. Returns an array of step names that
/// exist on Spec, in step1, step2, ..., stepN order.
fn collectSteps(comptime Spec: type) []const []const u8 {
    comptime {
        var names: []const []const u8 = &.{};
        var i: usize = 1;
        while (true) : (i += 1) {
            const name = std.fmt.comptimePrint("step{d}", .{i});
            if (!@hasDecl(Spec, name)) break;
            names = names ++ [_][]const u8{name};
        }
        if (names.len == 0) {
            @compileError("linear(Spec): expected at least one step method (step1, step2, ...)");
        }
        return names;
    }
}

/// Return type of a Spec step method.
fn StepReturn(comptime Spec: type, comptime step_name: []const u8) type {
    const method = @field(Spec, step_name);
    const info = @typeInfo(@TypeOf(method)).@"fn";
    return info.return_type.?;
}

/// Sanity-check the step chain at comptime. Errors are surfaced now
/// (not at first runtime poll), with messages pointing at the offending step.
fn validateChain(comptime Spec: type, comptime step_names: []const []const u8) void {
    comptime {
        if (!@hasDecl(Spec, "Output")) {
            @compileError("linear(Spec): Spec must declare `pub const Output = T`");
        }

        for (step_names, 0..) |name, idx| {
            const Ret = StepReturn(Spec, name);
            const is_last = idx == step_names.len - 1;
            const is_future = proto.isFuture(Ret);
            const is_output = Ret == Spec.Output;

            if (is_last) {
                if (!is_future and !is_output) {
                    @compileError("linear(Spec): final step `" ++ name ++
                        "` must return either `Output` or a Future. Got: " ++ @typeName(Ret));
                }
            } else {
                if (!is_future) {
                    @compileError("linear(Spec): non-final step `" ++ name ++
                        "` must return a Future. Got: " ++ @typeName(Ret));
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// State machine generator
// ─────────────────────────────────────────────────────────────────────────────

pub fn linear(comptime Spec: type) type {
    const step_names = comptime collectSteps(Spec);
    comptime validateChain(Spec, step_names);

    return struct {
        const Self = @This();
        pub const Output = Spec.Output;

        spec: Spec,
        state: State,

        // Build the state union at comptime.
        //   .start        — about to call step1
        //   .await{N}     — polling the inner Future returned by step{N}
        //   .done         — terminal
        const State = blk: {
            // Count Future-returning steps once to size the arrays.
            var future_step_count: usize = 0;
            for (step_names) |sn| {
                if (proto.isFuture(StepReturn(Spec, sn))) future_step_count += 1;
            }
            const N: usize = 1 + future_step_count + 1; // start + awaits + done

            var names_arr: [N][]const u8 = undefined;
            var types_arr: [N]type = undefined;
            var write: usize = 0;

            names_arr[write] = "start";
            types_arr[write] = void;
            write += 1;

            for (step_names, 0..) |sn, idx| {
                const Ret = StepReturn(Spec, sn);
                if (proto.isFuture(Ret)) {
                    names_arr[write] = std.fmt.comptimePrint("await{d}", .{idx + 1});
                    types_arr[write] = Ret;
                    write += 1;
                }
            }

            names_arr[write] = "done";
            types_arr[write] = void;
            write += 1;

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

        pub fn poll(self: *Self, ctx: *Context) PollResult(Output) {
            while (true) {
                // .start: call step1, transition to .await1 OR (if step1 is
                // terminal Output) finish immediately.
                if (self.state == .start) {
                    const RetType = comptime StepReturn(Spec, "step1");
                    const result = self.spec.step1();
                    if (comptime proto.isFuture(RetType)) {
                        self.state = @unionInit(State, "await1", result);
                        continue;
                    } else {
                        self.state = .{ .done = {} };
                        return .{ .ready = result };
                    }
                }
                if (self.state == .done) @panic("linear: polled after ready");

                // Some .awaitN — drive it.
                const drive = pollAwait(self, ctx) catch return .pending;
                if (drive) |out| return .{ .ready = out };
                // null: state was advanced to next .awaitN, keep looping.
            }
        }

        /// Dispatch among the .awaitN states.
        ///   - returns `error.Pending` if inner future pends
        ///   - returns `null` if state advanced (continue outer loop)
        ///   - returns `Output` if chain terminated
        fn pollAwait(self: *Self, ctx: *Context) error{Pending}!?Output {
            inline for (step_names, 0..) |name, idx| {
                const RetType = comptime StepReturn(Spec, name);
                if (comptime proto.isFuture(RetType)) {
                    const tag_name = comptime std.fmt.comptimePrint("await{d}", .{idx + 1});
                    if (@as(@typeInfo(State).@"union".tag_type.?, self.state) ==
                        @field(@typeInfo(State).@"union".tag_type.?, tag_name))
                    {
                        const inner_ptr = &@field(self.state, tag_name);
                        const r = inner_ptr.poll(ctx);
                        if (r.isPending()) return error.Pending;
                        const value = r.unwrap();

                        // Advance to next step
                        const next_idx = idx + 1;
                        if (next_idx >= step_names.len) {
                            // The last step's Future just produced Output.
                            self.state = .{ .done = {} };
                            return value;
                        }

                        const next_name = comptime step_names[next_idx];
                        const NextRet = comptime StepReturn(Spec, next_name);
                        const next_method = @field(Spec, next_name);
                        const next_result = next_method(&self.spec, value);

                        if (comptime proto.isFuture(NextRet)) {
                            const next_tag = comptime std.fmt.comptimePrint("await{d}", .{next_idx + 1});
                            self.state = @unionInit(State, next_tag, next_result);
                            return null; // outer loop continues
                        } else {
                            // Next step is terminal Output
                            self.state = .{ .done = {} };
                            return next_result;
                        }
                    }
                }
            }
            unreachable; // some .awaitN must match
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const Delayed = @import("delayed.zig").Delayed;

test "linear - one step (terminal Output, no suspension)" {
    const Spec = struct {
        x: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) i32 {
            return self.x * 2;
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .x = 21 });
    var ctx = Context{ .waker = &proto.noop_waker };

    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 42), r.unwrap());
}

test "linear - one step returning a Future (one suspension)" {
    const Spec = struct {
        x: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.x * 2);
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .x = 21 });
    var ctx = Context{ .waker = &proto.noop_waker };

    // Delayed pends once
    try std.testing.expect(f.poll(&ctx).isPending());
    // Then ready
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 42), r.unwrap());
}

test "linear - two suspensions with captured local" {
    // The flagship test: equivalent to the hand-written DoubleAndAddFuture.
    //
    //   doubled = await delayedDouble(a)        // step1
    //   sum = doubled + b   <-- captured `b`
    //   final = await delayedDouble(sum)        // step2 (terminal Future)
    const Spec = struct {
        a: i32,
        b: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a * 2);
        }
        pub fn step2(self: *@This(), doubled: i32) Delayed(i32) {
            return Delayed(i32).init((doubled + self.b) * 2);
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = 3, .b = 5 });
    var ctx = Context{ .waker = &proto.noop_waker };

    // Poll 1: state .start → calls step1 → enters .await1 → polls inner1 (pends)
    try std.testing.expect(f.poll(&ctx).isPending());
    // Poll 2: inner1 ready (6) → step2(6) using b=5 → sum=11, returns Delayed(22)
    //         → enters .await2 → polls inner2 (pends)
    try std.testing.expect(f.poll(&ctx).isPending());
    // Poll 3: inner2 ready (22) → terminal → ready 22
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 22), r.unwrap());
}

test "linear - three steps with mixed terminal" {
    // step1: Future(i32)
    // step2: Future(i32)  (uses captured b)
    // step3: i32 (terminal, value)  — formats the result somehow
    const Spec = struct {
        a: i32,
        b: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a * 2);
        }
        pub fn step2(self: *@This(), doubled: i32) Delayed(i32) {
            return Delayed(i32).init(doubled + self.b);
        }
        pub fn step3(self: *@This(), sum: i32) i32 {
            _ = self;
            return sum * 10;
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = 3, .b = 5 });
    var ctx = Context{ .waker = &proto.noop_waker };

    try std.testing.expect(f.poll(&ctx).isPending());
    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    // (3*2 + 5) * 10 = 110
    try std.testing.expectEqual(@as(i32, 110), r.unwrap());
}

test "linear - intermediate value stashed on self across multiple steps" {
    // The pattern: a value computed in step1 is needed in step3 but NOT step2.
    // User stashes it onto self.*. This is the workaround for "locals that span
    // more than one suspension" without needing tuple-return types.
    //
    // Equivalent to:
    //   const a_doubled = await delayedDouble(a);   // step1: produces a_doubled
    //   const b_plus_one = await delayedDouble(b);  // step2: needs `b`, NOT a_doubled
    //   return a_doubled + b_plus_one;              // step3: needs BOTH
    const Spec = struct {
        a: i32,
        b: i32,
        // Slot for the stashed intermediate
        stash_a_doubled: i32 = undefined,

        pub const Output = i32;

        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a * 2);
        }
        pub fn step2(self: *@This(), a_doubled: i32) Delayed(i32) {
            self.stash_a_doubled = a_doubled; // stash for step3
            return Delayed(i32).init(self.b + 1);
        }
        pub fn step3(self: *@This(), b_plus_one: i32) i32 {
            return self.stash_a_doubled + b_plus_one;
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = 7, .b = 9 });
    var ctx = Context{ .waker = &proto.noop_waker };

    try std.testing.expect(f.poll(&ctx).isPending());
    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    // a_doubled = 14, b_plus_one = 10, sum = 24
    try std.testing.expectEqual(@as(i32, 24), r.unwrap());
}

test "linear - shape sanity" {
    const Spec = struct {
        a: i32,
        pub const Output = i32;
        pub fn step1(self: *@This()) i32 {
            return self.a;
        }
    };
    const F = linear(Spec);
    try std.testing.expect(proto.isFuture(F));
    try std.testing.expect(F.Output == i32);
}

//! Spike Phase 2: linear with error propagation.
//!
//! Extends `linear.zig` to handle real-world async code that returns `!T`.
//! Any step in the chain can:
//!   - Return a Future (success path)
//!   - Return `!Future` (step itself can fail synchronously, e.g., connect())
//!   - Return a Future whose Output is `!T` (future can fail asynchronously, e.g., read())
//!   - Return `Output` directly (terminal value)
//!   - Return `!Output` (terminal value or error)
//!
//! When ANY step produces an error (either by returning error or via Future Output),
//! the chain's overall Output resolves to that error. The Spec's declared Output
//! must be `!T` if any step can produce errors.
//!
//! User-facing example:
//!   const Spec = struct {
//!       url: []const u8,
//!       pub const Output = ![]const u8;
//!
//!       pub fn step1(self: *@This()) !ConnectFuture {
//!           // can fail synchronously (DNS resolution, etc.)
//!           return try ConnectFuture.init(self.url);
//!       }
//!       pub fn step2(self: *@This(), conn: Connection) ReadFuture {
//!           // ReadFuture.Output = ![]const u8 (can fail asynchronously)
//!           return ReadFuture.init(conn);
//!       }
//!       pub fn step3(self: *@This(), data: []const u8) ![]const u8 {
//!           // terminal — can fail synchronously (parse error, etc.)
//!           return try parseAndValidate(data);
//!       }
//!   };

const std = @import("std");
const proto = @import("proto.zig");
const PollResult = proto.PollResult;
const Context = proto.Context;

// ─────────────────────────────────────────────────────────────────────────────
// Error-aware type analysis
// ─────────────────────────────────────────────────────────────────────────────

/// Information about a step's return type, decomposed into:
///   - whether the step itself can fail (return `!T` from the call)
///   - the inner type after stripping the error union
///   - whether the inner type is a Future
///   - whether the Future's Output is itself an error union
const StepKind = struct {
    /// True if the step's return type is `error{...}!T`
    step_returns_error: bool,
    /// True if (after stripping any step-error) the type is a Future
    inner_is_future: bool,
    /// True if the inner type is a Future and its Output is `error{...}!T`
    future_yields_error: bool,
    /// The "value" the chain ultimately threads to the next step (T after all
    /// error-stripping). For a terminal step this is the chain's success type.
    value_type: type,
    /// The full inner type (Future or value) — what we'd actually store in state
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

// ─────────────────────────────────────────────────────────────────────────────
// Comptime spec introspection (same as linear v1)
// ─────────────────────────────────────────────────────────────────────────────

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
            @compileError("linear(Spec): expected at least one step method");
        }
        return names;
    }
}

fn StepReturn(comptime Spec: type, comptime step_name: []const u8) type {
    const method = @field(Spec, step_name);
    const info = @typeInfo(@TypeOf(method)).@"fn";
    return info.return_type.?;
}

// ─────────────────────────────────────────────────────────────────────────────
// State machine generator
// ─────────────────────────────────────────────────────────────────────────────

pub fn linear(comptime Spec: type) type {
    const step_names = comptime collectSteps(Spec);

    // The chain's overall Output. If any step yields errors, the overall Output
    // is `!T` (we use Spec.Output as declared by the user — they're responsible
    // for declaring it correctly. If they declare `T` but a step has errors,
    // we'll get a compile error at the .ready return below — clear signal).
    const ChainOutput = Spec.Output;

    return struct {
        const Self = @This();
        pub const Output = ChainOutput;

        spec: Spec,
        state: State,

        const State = blk: {
            // Count Future-returning steps for state-union sizing.
            var future_step_count: usize = 0;
            for (step_names) |sn| {
                const kind = analyzeStepReturn(StepReturn(Spec, sn));
                if (kind.inner_is_future) future_step_count += 1;
            }
            const N: usize = 1 + future_step_count + 1; // start + awaits + done

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
                    // The state stores the unwrapped Future (no error union;
                    // step-level errors are already resolved synchronously).
                    types_arr[write] = kind.inner_type;
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
                if (self.state == .start) {
                    const k = comptime analyzeStepReturn(StepReturn(Spec, "step1"));
                    // Call step1, handling possible step-level error
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

                // Some .awaitN — drive it.
                const drive = pollAwait(self, ctx) catch return .pending;
                if (drive) |out| return .{ .ready = out };
                // null: state advanced, keep looping.
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

                        // If the future yields !T, unwrap that error union here.
                        const value = if (comptime k.future_yields_error)
                            future_out catch |err| {
                                self.state = .{ .done = {} };
                                return @as(Output, err);
                            }
                        else
                            future_out;

                        // Advance to next step
                        const next_idx = idx + 1;
                        if (next_idx >= step_names.len) {
                            // The last step's Future just produced its value.
                            self.state = .{ .done = {} };
                            return value;
                        }

                        const next_name = comptime step_names[next_idx];
                        const next_k = comptime analyzeStepReturn(StepReturn(Spec, next_name));
                        const next_method = @field(Spec, next_name);

                        // Call next step, handling possible step-level error
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
                            // Terminal value
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
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const Delayed = @import("delayed.zig").Delayed;

test "v2 - chain without errors still works (regression vs v1)" {
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

    try std.testing.expect(f.poll(&ctx).isPending());
    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectEqual(@as(i32, 22), r.unwrap());
}

test "v2 - terminal step returns !Output, success path" {
    const E = error{Bad};
    const Spec = struct {
        a: i32,
        pub const Output = E!i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a * 2);
        }
        pub fn step2(self: *@This(), doubled: i32) E!i32 {
            _ = self;
            if (doubled < 0) return error.Bad;
            return doubled + 1;
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = 10 });
    var ctx = Context{ .waker = &proto.noop_waker };

    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    const v = try r.unwrap();
    try std.testing.expectEqual(@as(i32, 21), v);
}

test "v2 - terminal step returns !Output, error path" {
    const E = error{Bad};
    const Spec = struct {
        a: i32,
        pub const Output = E!i32;
        pub fn step1(self: *@This()) Delayed(i32) {
            return Delayed(i32).init(self.a * 2);
        }
        pub fn step2(self: *@This(), doubled: i32) E!i32 {
            _ = self;
            if (doubled < 0) return error.Bad;
            return doubled + 1;
        }
    };

    const F = linear(Spec);
    var f = F.init(.{ .a = -10 }); // produces -20, triggers error.Bad
    var ctx = Context{ .waker = &proto.noop_waker };

    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectError(error.Bad, r.unwrap());
}

test "v2 - non-terminal step returns !Future, sync error" {
    const E = error{ConnectFailed};
    const Spec = struct {
        should_fail: bool,
        pub const Output = E!i32;
        pub fn step1(self: *@This()) E!Delayed(i32) {
            if (self.should_fail) return error.ConnectFailed;
            return Delayed(i32).init(42);
        }
        pub fn step2(self: *@This(), v: i32) i32 {
            _ = self;
            return v + 1;
        }
    };

    const F = linear(Spec);

    // Failure path
    {
        var f = F.init(.{ .should_fail = true });
        var ctx = Context{ .waker = &proto.noop_waker };
        const r = f.poll(&ctx);
        try std.testing.expect(r.isReady());
        try std.testing.expectError(error.ConnectFailed, r.unwrap());
    }

    // Success path
    {
        var f = F.init(.{ .should_fail = false });
        var ctx = Context{ .waker = &proto.noop_waker };
        try std.testing.expect(f.poll(&ctx).isPending()); // Delayed pends once
        const r = f.poll(&ctx);
        try std.testing.expect(r.isReady());
        try std.testing.expectEqual(@as(i32, 43), try r.unwrap());
    }
}

test "v2 - Future yields !T, async error path" {
    const E = error{ReadFailed};
    const Spec = struct {
        should_fail: bool,
        pub const Output = E!i32;
        pub fn step1(self: *@This()) Delayed(E!i32) {
            return Delayed(E!i32).init(if (self.should_fail) error.ReadFailed else 42);
        }
        pub fn step2(self: *@This(), v: i32) i32 {
            _ = self;
            return v + 1;
        }
    };

    const F = linear(Spec);

    // Failure path
    {
        var f = F.init(.{ .should_fail = true });
        var ctx = Context{ .waker = &proto.noop_waker };
        try std.testing.expect(f.poll(&ctx).isPending());
        const r = f.poll(&ctx);
        try std.testing.expect(r.isReady());
        try std.testing.expectError(error.ReadFailed, r.unwrap());
    }

    // Success path
    {
        var f = F.init(.{ .should_fail = false });
        var ctx = Context{ .waker = &proto.noop_waker };
        try std.testing.expect(f.poll(&ctx).isPending());
        const r = f.poll(&ctx);
        try std.testing.expect(r.isReady());
        try std.testing.expectEqual(@as(i32, 43), try r.unwrap());
    }
}

test "v2 - error short-circuits remaining steps" {
    const E = error{Bad};
    const Spec = struct {
        triggered_step3: *bool,
        pub const Output = E!i32;
        pub fn step1(self: *@This()) Delayed(E!i32) {
            _ = self;
            return Delayed(E!i32).init(error.Bad);
        }
        pub fn step2(self: *@This(), v: i32) i32 {
            self.triggered_step3.* = true; // shouldn't be reached
            return v + 1;
        }
        pub fn step3(self: *@This(), v: i32) i32 {
            self.triggered_step3.* = true; // shouldn't be reached
            return v + 1;
        }
    };

    var triggered = false;
    const F = linear(Spec);
    var f = F.init(.{ .triggered_step3 = &triggered });
    var ctx = Context{ .waker = &proto.noop_waker };

    try std.testing.expect(f.poll(&ctx).isPending());
    const r = f.poll(&ctx);
    try std.testing.expect(r.isReady());
    try std.testing.expectError(error.Bad, r.unwrap());
    try std.testing.expect(!triggered); // proves later steps did NOT run
}

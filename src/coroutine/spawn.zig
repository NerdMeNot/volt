//! Spawn: create a Coroutine + closure + result storage for a user function.
//!
//! The pieces:
//!   - Closure: extern struct, run_fn at offset 0 (asm trampoline reads it)
//!   - Args storage: heap-allocated tuple (tuples can't live inline in extern)
//!   - Result storage: heap-allocated struct holding state + value + error
//!   - Stack: 16-byte aligned heap allocation (size from `stack.default_size`)
//!   - Coroutine: ties everything together with lifecycle state
//!
//! Each (user_fn, Args) pair produces a unique Closure type at comptime.
//! That gives us a per-call-site specialized run function with no runtime
//! dispatch — calls go directly to the user fn.

const std = @import("std");
const ctx = @import("context_arm64.zig");
const stack_mod = @import("stack.zig");
const co = @import("coroutine.zig");
const Coroutine = co.Coroutine;

/// Compute the payload type of a function's return type.
///   fn() T   -> T
///   fn() !T  -> T
pub fn PayloadOf(comptime UserFn: type) type {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    return switch (@typeInfo(RT)) {
        .error_union => |eu| eu.payload,
        else => RT,
    };
}

/// True if the function returns an error union.
pub fn CanError(comptime UserFn: type) bool {
    const RT = @typeInfo(UserFn).@"fn".return_type.?;
    return @typeInfo(RT) == .error_union;
}

/// Per-payload result storage. Held on the heap so a `*ResultSlot(T)` is a
/// stable pointer the Task handle can hold across the coroutine's lifetime.
pub fn ResultSlot(comptime Payload: type) type {
    return struct {
        const Self = @This();
        pub const Tag = enum(u8) { pending, ok, err, cancelled };

        tag: Tag = .pending,
        value: Payload = undefined,
        err: anyerror = undefined,

        pub fn isReady(self: *const Self) bool {
            return self.tag != .pending;
        }
    };
}

/// Per-(user_fn, Args) Closure type with extern layout. The first field is
/// run_fn at offset 0, which the asm trampoline reads to invoke the closure.
pub fn Closure(comptime user_fn: anytype, comptime Args: type) type {
    const UserFn = @TypeOf(user_fn);
    const Payload = PayloadOf(UserFn);
    const can_error = CanError(UserFn);

    return extern struct {
        const Self = @This();
        run_fn: *const fn (*anyopaque) callconv(.c) void,
        coro: *Coroutine,
        args_ptr: *Args,
        result_ptr: *ResultSlot(Payload),

        /// Trampoline body: invoked via the asm trampoline (which reads
        /// run_fn from offset 0 and calls run_fn(closure_ptr)). Runs the
        /// user function, captures its result/error/cancellation, marks
        /// the coroutine done, and yields permanently to the scheduler.
        pub fn run(opaque_ptr: *anyopaque) callconv(.c) void {
            const self: *Self = @ptrCast(@alignCast(opaque_ptr));

            // Cancel-before-start: if cancellation arrived before we ran
            // (between spawn and first dispatch), short-circuit.
            if (self.coro.isCancelled()) {
                self.result_ptr.tag = .cancelled;
            } else {
                if (comptime can_error) {
                    const r = @call(.auto, user_fn, self.args_ptr.*);
                    if (r) |val| {
                        self.result_ptr.value = val;
                        self.result_ptr.tag = .ok;
                    } else |err| {
                        if (err == error.Cancelled) {
                            self.result_ptr.tag = .cancelled;
                        } else {
                            self.result_ptr.err = err;
                            self.result_ptr.tag = .err;
                        }
                    }
                } else {
                    const val = @call(.auto, user_fn, self.args_ptr.*);
                    self.result_ptr.value = val;
                    self.result_ptr.tag = .ok;
                }
            }

            self.coro.state = .done;
            ctx.swap(&self.coro.ctx, self.coro.scheduler_ctx);
            // Scheduler observes .done and won't resume us. Anything past
            // here is dead code — `unreachable` keeps the compiler happy.
            unreachable;
        }

        pub fn destroyExtras(allocator: std.mem.Allocator, coro: *Coroutine) void {
            const closure: *Self = @ptrCast(@alignCast(coro.closure_ptr));
            const args: *Args = @ptrCast(@alignCast(coro.args_ptr));
            allocator.destroy(closure.result_ptr);
            allocator.destroy(args);
            allocator.destroy(closure);
        }
    };
}

/// Pair returned by `create()` — the Coroutine pointer plus a typed pointer
/// to the result slot. Defined as a named generic struct so callers (Runtime,
/// public spawn API) can declare matching return types instead of producing
/// distinct anonymous structs at each call site.
pub fn Created(comptime UserFn: type) type {
    return struct {
        coro: *Coroutine,
        result_ptr: *ResultSlot(PayloadOf(UserFn)),
    };
}

/// Allocate a fresh coroutine for `user_fn(args)`. Returns the Coroutine
/// pointer and a typed pointer to its result slot (used by Task to read the
/// outcome later). The Coroutine is NOT yet enqueued — caller does that.
pub fn create(
    allocator: std.mem.Allocator,
    stack_size: usize,
    comptime user_fn: anytype,
    args: anytype,
) !Created(@TypeOf(user_fn)) {
    const UserFn = @TypeOf(user_fn);
    const Args = @TypeOf(args);
    const Payload = PayloadOf(UserFn);
    const Cl = Closure(user_fn, Args);

    const coro = try allocator.create(Coroutine);
    errdefer allocator.destroy(coro);

    const closure = try allocator.create(Cl);
    errdefer allocator.destroy(closure);

    const args_storage = try allocator.create(Args);
    errdefer allocator.destroy(args_storage);
    args_storage.* = args;

    const result = try allocator.create(ResultSlot(Payload));
    errdefer allocator.destroy(result);
    result.* = .{};

    const stack = try stack_mod.alloc(allocator, stack_size);
    errdefer stack_mod.free(allocator, stack);

    closure.* = .{
        .run_fn = &Cl.run,
        .coro = coro,
        .args_ptr = args_storage,
        .result_ptr = result,
    };

    coro.* = .{
        // scheduler_ctx is set when the scheduler dispatches us for the
        // first time. Until then, leave it pointing at our own ctx (safe
        // sentinel — we'll never use it before dispatch).
        .scheduler_ctx = &coro.ctx,
        .state = .runnable,
        .stack = stack,
        .destroy_extras_fn = &Cl.destroyExtras,
        .closure_ptr = @ptrCast(closure),
        .args_ptr = @ptrCast(args_storage),
    };

    ctx.initContext(&coro.ctx, stack_mod.topOf(stack), closure);

    return .{ .coro = coro, .result_ptr = result };
}

test "spawn: PayloadOf strips error union" {
    const fnA = struct {
        fn x() i32 {
            return 0;
        }
    }.x;
    const fnB = struct {
        fn x() !i32 {
            return 0;
        }
    }.x;
    try std.testing.expectEqual(i32, PayloadOf(@TypeOf(fnA)));
    try std.testing.expectEqual(i32, PayloadOf(@TypeOf(fnB)));
}

test "spawn: CanError detects error unions" {
    const fnA = struct {
        fn x() i32 {
            return 0;
        }
    }.x;
    const fnB = struct {
        fn x() !i32 {
            return 0;
        }
    }.x;
    try std.testing.expect(!CanError(@TypeOf(fnA)));
    try std.testing.expect(CanError(@TypeOf(fnB)));
}

test "spawn: ResultSlot starts pending" {
    const Slot = ResultSlot(i32);
    const s = Slot{};
    try std.testing.expectEqual(Slot.Tag.pending, s.tag);
    try std.testing.expect(!s.isReady());
}

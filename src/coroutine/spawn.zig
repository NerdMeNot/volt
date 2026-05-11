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
const ctx = @import("context.zig");
const stack_mod = @import("stack.zig");
const co = @import("coroutine.zig");
const Coroutine = co.Coroutine;
const event_source = @import("event_source.zig");

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
        pub const Tag = enum(u8) { pending, ok, err, cancelled, stack_overflow };

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

            // Hand the coroutine to the Done EventSource. The worker
            // post-swap reads `pending_event` and calls Done.subscribe,
            // which sets `done_flag` and unparks any joiner.
            self.coro.pending_event = &event_source.done_singleton;
            ctx.swap(&self.coro.ctx, self.coro.scheduler_ctx);
            // The worker observes Done and never re-dispatches us. Anything
            // past here is dead code.
            unreachable;
        }
    };
}

/// Single-allocation frame holding everything a coroutine needs:
/// the Coroutine struct, the Closure, the args, and the result slot.
/// One `allocator.create(Frame)` per spawn instead of four separate
/// allocations — reduces allocator traffic 4×.
///
/// Layout note: `coro` is FIRST so `&frame.coro` is the same address as
/// the Frame itself. That lets `lifecycleRelease(coro)` recover the
/// Frame address with `@fieldParentPtr` — it's the same as the
/// Coroutine pointer the user holds onto.
pub fn Frame(comptime user_fn: anytype, comptime Args: type) type {
    const UserFn = @TypeOf(user_fn);
    const Payload = PayloadOf(UserFn);
    return struct {
        const Self = @This();
        coro: Coroutine,
        closure: Closure(user_fn, Args),
        args: Args,
        result: ResultSlot(Payload),

        /// Free the entire frame. Called by lifecycleRelease via the
        /// `destroy_extras_fn` slot in Coroutine.
        ///
        /// Two paths:
        /// - `from_pool == true`: Frame came from the originating
        ///   worker's `FramePool` slab. Return to that pool's freelist
        ///   via lock-free push (safe cross-worker).
        /// - `from_pool == false`: Frame came from the user allocator
        ///   (pool exhaustion fallback). Standard allocator.destroy.
        pub fn destroyFrame(allocator: std.mem.Allocator, coro: *Coroutine) void {
            const frame: *Self = @fieldParentPtr("coro", coro);
            if (coro.from_pool) {
                // Route to the originating worker's FramePool freelist.
                const runtime_mod = @import("../runtime.zig");
                if (runtime_mod.currentRuntime()) |rt| {
                    if (coro.owning_worker_id < rt.workers.len) {
                        rt.workers[coro.owning_worker_id].frame_pool.free(Self, frame);
                        return;
                    }
                }
                // No runtime context (e.g. test teardown) — fall through
                // to allocator.destroy as a safe-but-leaky fallback. The
                // pool's munmap will reclaim the memory on Worker.deinit.
                // We deliberately don't try to recover the worker here.
            }
            allocator.destroy(frame);
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

pub const StackOptions = struct {
    /// Total reserved virtual size. The runtime grows committed pages
    /// on demand up to this limit; overflow past it surfaces as
    /// `error.StackOverflow`.
    reserved: usize,
    /// Initial committed bytes at the top. `null` = one system page
    /// (default). `0` = fully lazy: the very first stack write traps
    /// into the SIGSEGV handler and commits one page on demand.
    initial_commit: ?usize = null,
};

/// Allocate a fresh coroutine for `user_fn(args)`. Returns the Coroutine
/// pointer and a typed pointer to its result slot (used by Task to read the
/// outcome later). The Coroutine is NOT yet enqueued — caller does that.
///
/// `recycled_stack`: if non-null, reuses that GrowableStack instead of
/// calling `allocGrowable`. Used by Runtime to recycle stacks via the
/// stack pool. Pass null on first-spawn paths or when no pool is wired.
/// Per-worker frame pool handle. Optional argument to `create` that
/// lets the spawn path bypass the user allocator entirely. `null` =>
/// fall back to user allocator.
pub const FrameSource = struct {
    pool: ?*@import("frame_pool.zig").FramePool = null,
    pool_worker_id: u16 = 0,
};

pub fn create(
    allocator: std.mem.Allocator,
    stack_opts: StackOptions,
    recycled_stack: ?stack_mod.GrowableStack,
    comptime user_fn: anytype,
    args: anytype,
) !Created(@TypeOf(user_fn)) {
    return createWithFrameSource(allocator, .{}, stack_opts, recycled_stack, user_fn, args);
}

pub fn createWithFrameSource(
    allocator: std.mem.Allocator,
    frame_source: FrameSource,
    stack_opts: StackOptions,
    recycled_stack: ?stack_mod.GrowableStack,
    comptime user_fn: anytype,
    args: anytype,
) !Created(@TypeOf(user_fn)) {
    const Args = @TypeOf(args);
    const Cl = Closure(user_fn, Args);
    const F = Frame(user_fn, Args);

    // Try the per-worker FramePool first (fast path — comptime-resolved
    // size class, no allocator-vtable call). On miss (pool exhausted or
    // Frame too large for any size class) fall back to user allocator.
    var from_pool = false;
    var pool_worker_id: u16 = 0;
    const frame: *F = blk: {
        if (frame_source.pool) |pool| {
            if (pool.alloc(F)) |p| {
                from_pool = true;
                pool_worker_id = frame_source.pool_worker_id;
                break :blk p;
            }
        }
        break :blk try allocator.create(F);
    };
    errdefer if (from_pool)
        (frame_source.pool.?).free(F, frame)
    else
        allocator.destroy(frame);

    frame.args = args;
    frame.result = .{};

    const gs = if (recycled_stack) |rs| rs else blk: {
        const initial_commit = stack_opts.initial_commit orelse stack_mod.defaultInitialCommit();
        break :blk try stack_mod.allocGrowable(initial_commit, stack_opts.reserved);
    };
    errdefer if (recycled_stack == null) stack_mod.freeGrowable(gs);

    const stack_ptr: [*]align(16) u8 = @ptrFromInt(gs.base);
    const stack: []align(16) u8 = stack_ptr[0..gs.reserved_size];

    frame.closure = .{
        .run_fn = &Cl.run,
        .coro = &frame.coro,
        .args_ptr = &frame.args,
        .result_ptr = &frame.result,
    };

    frame.coro = .{
        // scheduler_ctx is set on every dispatch. Until then, leave it
        // pointing at our own ctx (safe sentinel — we never use it before
        // dispatch).
        .scheduler_ctx = &frame.coro.ctx,
        // pending_event, done_flag, join_park, cancel_flag all default to
        // their zero/initial values via the struct's field defaults.
        .stack = stack,
        .stack_committed_bottom = std.atomic.Value(usize).init(gs.committed_bottom),
        .destroy_extras_fn = &F.destroyFrame,
        .closure_ptr = @ptrCast(&frame.closure),
        .args_ptr = @ptrCast(&frame.args),
        .from_pool = from_pool,
        .owning_worker_id = pool_worker_id,
    };

    ctx.initContext(&frame.coro.ctx, stack_mod.topOf(stack), &frame.closure);

    return .{ .coro = &frame.coro, .result_ptr = &frame.result };
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

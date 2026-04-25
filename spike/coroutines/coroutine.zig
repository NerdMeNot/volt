//! Coroutine: a Context + an owned stack + lifecycle state.
//!
//! Stackful: each coroutine has its own ~4KB stack (configurable). When
//! suspended, all stack contents are preserved in place, callee-saved
//! registers in Context. Resume restores everything — code looks
//! synchronous.

const std = @import("std");
const ctx = @import("context_arm64.zig");

pub const State = enum(u8) {
    runnable,
    running,
    parked,
    done,
};

/// First field of every closure must match this — the asm trampoline reads
/// `run_fn` from offset 0 and calls run_fn(closure_ptr).
pub const ClosureBase = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
};

pub const Coroutine = struct {
    ctx: ctx.Context = .{},
    scheduler_ctx: *ctx.Context,
    state: State,
    stack: []align(16) u8,
    /// Per-type destroy callback — knows how to free closure + args correctly.
    /// Set at spawn time.
    destroy_extras_fn: *const fn (std.mem.Allocator, *Coroutine) void,
    /// Type-erased closure pointer (heap-allocated, fixed extern layout).
    closure_ptr: *anyopaque,
    /// Type-erased args pointer (heap-allocated tuple).
    args_ptr: *anyopaque,
};

pub const stack_alignment: std.mem.Alignment = .@"16";

/// Per-(user_fn, Args) closure. extern struct guarantees layout —
/// the trampoline reads run_fn from offset 0. Args go via a separate
/// heap allocation referenced through an extern-safe pointer.
fn Closure(comptime user_fn: anytype, comptime Args: type) type {
    return extern struct {
        const Self = @This();
        run_fn: *const fn (*anyopaque) callconv(.c) void, // offset 0
        coro: *Coroutine,
        args_ptr: *Args,

        fn run(opaque_ptr: *anyopaque) callconv(.c) void {
            const self: *Self = @ptrCast(@alignCast(opaque_ptr));
            _ = @call(.auto, user_fn, self.args_ptr.*);
            self.coro.state = .done;
            ctx.swap(&self.coro.ctx, self.coro.scheduler_ctx);
            unreachable;
        }

        fn destroyExtras(allocator: std.mem.Allocator, coro: *Coroutine) void {
            const c: *Self = @ptrCast(@alignCast(coro.closure_ptr));
            const a: *Args = @ptrCast(@alignCast(coro.args_ptr));
            allocator.destroy(a);
            allocator.destroy(c);
        }
    };
}

pub fn spawn(
    allocator: std.mem.Allocator,
    scheduler_ctx: *ctx.Context,
    stack_size: usize,
    comptime user_fn: anytype,
    args: anytype,
) !*Coroutine {
    const Args = @TypeOf(args);
    const Cl = Closure(user_fn, Args);

    const coro = try allocator.create(Coroutine);
    errdefer allocator.destroy(coro);

    // Closure (extern, fixed layout) — heap allocated.
    const closure = try allocator.create(Cl);
    errdefer allocator.destroy(closure);

    // Args (regular tuple) — heap allocated, referenced by ptr from closure.
    const args_storage = try allocator.create(Args);
    errdefer allocator.destroy(args_storage);
    args_storage.* = args;

    const stack = try allocator.alignedAlloc(u8, stack_alignment, stack_size);
    errdefer allocator.free(stack);

    closure.* = .{
        .run_fn = &Cl.run,
        .coro = coro,
        .args_ptr = args_storage,
    };

    coro.* = .{
        .ctx = .{},
        .scheduler_ctx = scheduler_ctx,
        .state = .runnable,
        .stack = stack,
        .destroy_extras_fn = &Cl.destroyExtras,
        .closure_ptr = @ptrCast(closure),
        .args_ptr = @ptrCast(args_storage),
    };

    const stack_top: [*]u8 = stack.ptr + stack_size;
    ctx.initContext(&coro.ctx, stack_top, closure);

    return coro;
}

pub fn destroy(allocator: std.mem.Allocator, coro: *Coroutine) void {
    coro.destroy_extras_fn(allocator, coro);
    allocator.free(coro.stack);
    allocator.destroy(coro);
}

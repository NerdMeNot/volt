//! v2 Coroutine — minimum stackful unit of work.
//!
//! Each Coroutine carries:
//!   - Its own callee-save register context (ctx)
//!   - A pointer back to its scheduler's main_ctx (where we swap to on
//!     yield/exit)
//!   - Its stack (allocator-managed)
//!   - A pointer to its Frame (comptime-specialized closure)
//!   - A destroy function for the Frame (set by the Frame factory)
//!   - An intrusive next pointer for the run queue (Treiber stack)
//!
//! No `pending_event`, `join_park`, `done_flag`, `lifecycle_count` —
//! POC-B / POC-I / POC-C synthesis. Completion signaled via WaitGroup
//! (Go style); see `wait_group.zig`.

const std = @import("std");
const context = @import("context_arm64.zig");

/// User functions passed to `spawn` get monomorphized into a Frame
/// (comptime-generated). The frame's FIRST field is always a
/// `*const fn(*anyopaque) callconv(.c) void` (the trampoline that
/// `voltCoroEntry` calls). After the trampoline runs the user fn, it
/// must `context.swap(&coro.ctx, coro.main_ctx)`.
pub const TrampolineFn = *const fn (*anyopaque) callconv(.c) void;

/// Destroy a Frame allocation. Erased to *anyopaque so the Coroutine
/// doesn't carry a generic type parameter. Set by the spawn site to
/// the Frame type's destroy function.
pub const FrameDestroyFn = *const fn (frame_ptr: *anyopaque, allocator: std.mem.Allocator) void;

/// What does a swap-back from a coroutine mean?
///   `done`  — trampoline returned. Worker frees stack + Coroutine
///             (and Frame, if no Task owns it).
///   `yield` — coroutine called `yield()`. Worker re-pushes to queue.
///   `park`  — coroutine called `park()` for sync/IO. Worker leaves it
///             alone; some other event (reactor, sync) will re-push.
pub const PendingKind = enum(u8) {
    done,
    yield,
    park,
};

pub const Coroutine = struct {
    ctx: context.Context = .{},
    /// Worker's main context.
    main_ctx: *context.Context = undefined,
    /// Heap-alloc'd 16-byte-aligned stack slice.
    stack: []align(16) u8 = undefined,
    /// Pointer to the Frame (comptime-generated).
    frame_ptr: *anyopaque = undefined,
    /// Frees the Frame allocation.
    frame_destroy: ?FrameDestroyFn = null,
    /// Why did the last swap-back happen? Worker reads this after the
    /// swap-back returns from `ctx.swap`. The trampoline sets it to
    /// `.done` before the terminal swap. `yield()` and `park()`
    /// helpers set it to `.yield` / `.park` before swapping.
    pending: PendingKind = .done,
    /// Intrusive next pointer (Treiber stack).
    next: ?*Coroutine = null,
    /// Optional WaitGroup. Auto-decremented on terminal swap-back.
    wg: ?*WaitGroupAtomic = null,
    /// Non-null when a Task handle exists.
    has_task: bool = false,
};

/// Forward declaration; structural match to WaitGroup's first field.
pub const WaitGroupAtomic = extern struct {
    counter: std.atomic.Value(u32),
};

// ─────────────────────────────────────────────────────────────────────
// Frame factory — comptime-monomorphized per (user_fn, ArgsT, RetT)
// ─────────────────────────────────────────────────────────────────────

/// The Frame is the per-spawn closure: it stores run_fn (trampoline
/// entry), back-pointer to its Coroutine, user fn's args, and a slot
/// for the result.
///
/// Layout: run_fn at offset 0 (voltCoroEntry reads it). Followed by
/// args / result / coro back-pointer.
pub fn Frame(comptime user_fn: anytype, comptime ArgsT: type) type {
    const FnT = @TypeOf(user_fn);
    const fn_info = @typeInfo(FnT).@"fn";
    const Ret = fn_info.return_type.?;

    return struct {
        const Self = @This();
        // MUST be first field — voltCoroEntry reads *(x19) as run_fn.
        run_fn: TrampolineFn = &trampoline,
        coro: ?*Coroutine = null,
        args: ArgsT,
        result: Ret = undefined,

        pub const Result = Ret;

        fn trampoline(opaque_ptr: *anyopaque) callconv(.c) void {
            const self: *Self = @ptrCast(@alignCast(opaque_ptr));
            self.result = @call(.auto, user_fn, self.args);
            // Terminal swap. Worker will see pending == .done.
            const c = self.coro.?;
            c.pending = .done;
            context.swap(&c.ctx, c.main_ctx);
            unreachable;
        }

        pub fn destroy(frame_ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *Self = @ptrCast(@alignCast(frame_ptr));
            allocator.destroy(self);
        }
    };
}

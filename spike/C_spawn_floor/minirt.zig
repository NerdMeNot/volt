//! POC-C — bare-floor stackful spawn+join runtime.
//!
//! Combines:
//!   * Wide-save ctx switch (POC-A: 6 ns/swap, narrow wins nothing)
//!   * 16 KiB heap-alloc'd fixed stack (POC-E candidate — simplest)
//!   * Single-worker spin scheduler (POC-G: 22 ns/task)
//!   * Atomic-counter join (no per-coro Park primitive — Go WaitGroup style)
//!
//! Goal: prove that stackful spawn+join can hit ≤ 200 ns/op with the
//! combined primitives. If yes, the gap between current Volt (4,163 ns)
//! and Go (149 ns) is entirely v1 architectural waste, and a rewrite
//! on this foundation can match Go.

const std = @import("std");
const ctx_mod = @import("ctx.zig");

pub const STACK_SIZE: usize = 16 * 1024;

pub const Coroutine = struct {
    ctx: ctx_mod.Context = .{},
    main_ctx: *ctx_mod.Context = undefined,
    stack: []align(16) u8 = undefined,
    closure: extern struct {
        run_fn: *const fn (*anyopaque) callconv(.c) void,
        user_fn: *const fn () callconv(.c) void,
        coro: ?*Coroutine = null,
    } = .{ .run_fn = &trampoline, .user_fn = undefined },
    next: ?*Coroutine = null,
};

fn trampoline(opaque_ptr: *anyopaque) callconv(.c) void {
    const closure: *@TypeOf(@as(Coroutine, undefined).closure) = @ptrCast(@alignCast(opaque_ptr));
    const coro = closure.coro.?;
    closure.user_fn();
    // Swap back to scheduler. The scheduler observes this is the
    // "done" exit and frees the coro.
    ctx_mod.swap(&coro.ctx, coro.main_ctx);
    unreachable;
}

// ─────────────────────────────────────────────────────────────────────
// Lock-free MPMC stack of coros to run.
// ─────────────────────────────────────────────────────────────────────
pub const RunQueue = struct {
    head: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    pub fn push(self: *RunQueue, c: *Coroutine) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            c.next = cur;
            if (self.head.cmpxchgWeak(cur, c, .release, .monotonic)) |observed| {
                cur = observed;
            } else {
                return;
            }
        }
    }

    pub fn pop(self: *RunQueue) ?*Coroutine {
        var cur = self.head.load(.acquire);
        while (cur) |c| {
            const next = c.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else {
                return c;
            }
        }
        return null;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Runtime
// ─────────────────────────────────────────────────────────────────────
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    queue: RunQueue = .{},
    main_ctx: ctx_mod.Context = .{},
    /// Atomic counter for "remaining work." Goes to 0 when all spawned
    /// coros have completed; main `join` polls this.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init(allocator: std.mem.Allocator) Runtime {
        return .{ .allocator = allocator };
    }

    pub fn spawn(self: *Runtime, user_fn: *const fn () callconv(.c) void) !void {
        const coro = try self.allocator.create(Coroutine);
        const stack = try self.allocator.alignedAlloc(u8, .@"16", STACK_SIZE);
        coro.* = .{
            .stack = stack,
            .closure = .{
                .run_fn = &trampoline,
                .user_fn = user_fn,
                .coro = coro,
            },
        };
        const stack_top: [*]u8 = stack.ptr + STACK_SIZE;
        ctx_mod.initContext(&coro.ctx, stack_top, &coro.closure);
        coro.main_ctx = &self.main_ctx;
        _ = self.pending.fetchAdd(1, .acq_rel);
        self.queue.push(coro);
    }

    /// Run the scheduler on the calling thread until `pending == 0`.
    /// Single-worker model.
    pub fn run(self: *Runtime) void {
        while (self.pending.load(.acquire) > 0) {
            if (self.queue.pop()) |coro| {
                ctx_mod.swap(&self.main_ctx, &coro.ctx);
                // Coroutine returned (no yield, just terminal swap).
                // Free its stack + struct, decrement pending.
                self.allocator.free(coro.stack);
                self.allocator.destroy(coro);
                _ = self.pending.fetchSub(1, .acq_rel);
            } else {
                std.atomic.spinLoopHint();
            }
        }
    }
};

//! v2 Runtime — single-worker spin scheduler.
//!
//! Single OS thread runs the scheduler loop. Coroutines spawned via
//! `runtime.spawn(fn, args)` go onto a lock-free Treiber-stack run queue.
//! The worker pops, swaps in, swaps back on trampoline return,
//! decrements the attached WaitGroup, frees stack + Coroutine struct
//! (Frame is owned by Task, freed on `task.join()`).
//!
//! No work-stealing, no parker, no multi-worker — those come later.
//! POC-C proved this floor hits 93 ns/op (1.6× faster than Go).

const std = @import("std");
const context = @import("context_arm64.zig");
const coroutine = @import("coroutine.zig");
const wait_group = @import("wait_group.zig");
const task_mod = @import("task.zig");
const current = @import("current.zig");

pub const Coroutine = coroutine.Coroutine;
pub const Frame = coroutine.Frame;
pub const PendingKind = coroutine.PendingKind;
pub const WaitGroup = wait_group.WaitGroup;
pub const Task = task_mod.Task;
pub const STACK_SIZE: usize = 16 * 1024;

/// Cooperative yield. Re-queues the current coroutine after letting
/// any other ready coroutines run. Must be called from inside a
/// coroutine spawned via `Runtime.spawn`.
pub fn yield() void {
    const c = current.require();
    c.pending = .yield;
    context.swap(&c.ctx, c.main_ctx);
    // After resume: pending is reset to .done by the worker before
    // re-dispatching us (so the next swap-back defaults to terminal).
}

pub const RunQueue = struct {
    head: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    pub fn push(self: *RunQueue, c: *Coroutine) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            c.next = cur;
            if (self.head.cmpxchgWeak(cur, c, .release, .monotonic)) |observed| {
                cur = observed;
            } else return;
        }
    }

    pub fn pop(self: *RunQueue) ?*Coroutine {
        var cur = self.head.load(.acquire);
        while (cur) |c| {
            const next = c.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else return c;
        }
        return null;
    }
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    queue: RunQueue = .{},
    main_ctx: context.Context = .{},
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init(allocator: std.mem.Allocator) Runtime {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Runtime) void {
        std.debug.assert(self.pending.load(.acquire) == 0);
    }

    /// Comptime-typed spawn. Returns a *Task(T) where T is the return
    /// type of `user_fn`. Caller must `task.join()` to retrieve result
    /// and free the Frame.
    ///
    /// For "fire and forget" spawns where you don't need the result,
    /// pass a wg via `spawnTracked` instead.
    pub fn spawn(
        self: *Runtime,
        comptime user_fn: anytype,
        args: anytype,
    ) !*Task(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
        const F = Frame(user_fn, @TypeOf(args));
        const T = F.Result;

        const frame = try self.allocator.create(F);
        errdefer self.allocator.destroy(frame);

        const c = try self.allocator.create(Coroutine);
        errdefer self.allocator.destroy(c);

        const stack = try self.allocator.alignedAlloc(u8, .@"16", STACK_SIZE);
        errdefer self.allocator.free(stack);

        const task = try self.allocator.create(Task(T));
        errdefer self.allocator.destroy(task);

        frame.* = .{ .args = args, .coro = c };
        c.* = .{
            .stack = stack,
            .frame_ptr = frame,
            .frame_destroy = &F.destroy,
            .main_ctx = &self.main_ctx,
            .has_task = true,
            .wg = null, // Task carries its own WG
        };
        const stack_top: [*]u8 = stack.ptr + STACK_SIZE;
        context.initContext(&c.ctx, stack_top, frame);

        task.* = .{
            .coro = c,
            .result_ptr = &frame.result,
            .frame_ptr = frame,
            .frame_destroy = &F.destroy,
            .allocator = self.allocator,
            .wg = WaitGroup.init(1),
        };
        // The Coroutine's wg points at the Task's WG so the worker
        // can decrement it on terminal swap-back.
        c.wg = @ptrCast(&task.wg);

        _ = self.pending.fetchAdd(1, .acq_rel);
        self.queue.push(c);
        return task;
    }

    /// Drive the scheduler until `pending == 0`.
    pub fn run(self: *Runtime) void {
        while (self.pending.load(.acquire) > 0) {
            if (self.queue.pop()) |c| {
                // Reset pending to .done before swap; the trampoline /
                // yield / park will set it to the appropriate kind.
                c.pending = .done;
                current.set(c);
                context.swap(&self.main_ctx, &c.ctx);
                current.clear();
                switch (c.pending) {
                    .yield => {
                        // Cooperative re-queue. Coroutine isn't done.
                        self.queue.push(c);
                    },
                    .park => {
                        // Coroutine is waiting on external event.
                        // Some other path (reactor, sync primitive)
                        // will push it back when ready. Do nothing here.
                    },
                    .done => {
                        if (c.wg) |w| {
                            const wg_typed: *WaitGroup = @ptrCast(w);
                            wg_typed.done();
                        }
                        if (!c.has_task) {
                            if (c.frame_destroy) |destroy_fn| {
                                destroy_fn(c.frame_ptr, self.allocator);
                            }
                        }
                        self.allocator.free(c.stack);
                        self.allocator.destroy(c);
                        _ = self.pending.fetchSub(1, .acq_rel);
                    },
                }
                continue;
            }
            std.atomic.spinLoopHint();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;

fn returnInt(x: u32) u32 {
    return x * 2;
}

fn returnString() []const u8 {
    return "hello from coro";
}

fn noReturn(counter: *std.atomic.Value(u32)) void {
    _ = counter.fetchAdd(1, .acq_rel);
}

test "runtime: spawn typed fn returning u32" {
    var rt = Runtime.init(test_allocator);
    defer rt.deinit();

    var task = try rt.spawn(returnInt, .{@as(u32, 21)});
    rt.run();
    const result = task.join();

    try std.testing.expectEqual(@as(u32, 42), result);
}

test "runtime: spawn typed fn returning slice" {
    var rt = Runtime.init(test_allocator);
    defer rt.deinit();

    var task = try rt.spawn(returnString, .{});
    rt.run();
    const result = task.join();

    try std.testing.expectEqualStrings("hello from coro", result);
}

test "runtime: spawn 100 typed fns with pointer arg" {
    var rt = Runtime.init(test_allocator);
    defer rt.deinit();

    var counter = std.atomic.Value(u32).init(0);
    var tasks: [100]*Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(noReturn, .{&counter});
    rt.run();
    for (&tasks) |t| t.join();

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
}

fn yieldingWorker(counter: *std.atomic.Value(u32), n: u32) void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        _ = counter.fetchAdd(1, .acq_rel);
        yield();
    }
}

test "runtime: yield re-queues coroutine" {
    var rt = Runtime.init(test_allocator);
    defer rt.deinit();

    var counter = std.atomic.Value(u32).init(0);
    var task = try rt.spawn(yieldingWorker, .{ &counter, @as(u32, 5) });
    rt.run();
    task.join();

    try std.testing.expectEqual(@as(u32, 5), counter.load(.acquire));
}

test "runtime: two yielding coroutines interleave" {
    var rt = Runtime.init(test_allocator);
    defer rt.deinit();

    var counter = std.atomic.Value(u32).init(0);
    var t1 = try rt.spawn(yieldingWorker, .{ &counter, @as(u32, 10) });
    var t2 = try rt.spawn(yieldingWorker, .{ &counter, @as(u32, 10) });
    rt.run();
    t1.join();
    t2.join();

    try std.testing.expectEqual(@as(u32, 20), counter.load(.acquire));
}

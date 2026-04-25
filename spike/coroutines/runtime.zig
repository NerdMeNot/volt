//! Minimal runtime + scheduler for stackful coroutines.
//!
//! Single-threaded for the spike. One worker, one ready queue (FIFO).
//! Coroutines yield via `yield()` which swaps back to the scheduler's main
//! context; scheduler picks the next .runnable coroutine and swaps to it.
//! When all coroutines are done, run() returns.
//!
//! TLS (per-thread): pointer to the currently-executing coroutine, so any
//! suspending operation can find "myself" without explicit threading.

const std = @import("std");
const ctx = @import("context_arm64.zig");
const Coroutine = @import("coroutine.zig").Coroutine;
const State = @import("coroutine.zig").State;
const cspawn = @import("coroutine.zig").spawn;
const cdestroy = @import("coroutine.zig").destroy;

threadlocal var current_coro: ?*Coroutine = null;
threadlocal var current_runtime: ?*Runtime = null;

pub const Config = struct {
    /// Default coroutine stack size. 4KB is the long-term target with guard
    /// pages + growing stacks; for the spike we use 64KB to avoid stack
    /// overflows in code that calls into the allocator.
    default_stack_size: usize = 64 * 1024,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: Config,
    /// Scheduler's main context — coroutines yield back here.
    main_ctx: ctx.Context = .{},
    /// FIFO ready queue.
    ready: std.array_list.Managed(*Coroutine),
    /// Coroutines waiting to be reaped (they're .done).
    done: std.array_list.Managed(*Coroutine),

    pub fn init(allocator: std.mem.Allocator, config: Config) Runtime {
        return .{
            .allocator = allocator,
            .config = config,
            .ready = std.array_list.Managed(*Coroutine).init(allocator),
            .done = std.array_list.Managed(*Coroutine).init(allocator),
        };
    }

    pub fn deinit(self: *Runtime) void {
        // Reap any leftover coroutines.
        for (self.done.items) |c| cdestroy(self.allocator, c);
        for (self.ready.items) |c| cdestroy(self.allocator, c);
        self.ready.deinit();
        self.done.deinit();
    }

    /// Spawn a new coroutine. Caller doesn't get a handle for now — fire and forget.
    pub fn spawn(self: *Runtime, comptime user_fn: anytype, args: anytype) !void {
        const c = try cspawn(
            self.allocator,
            &self.main_ctx,
            self.config.default_stack_size,
            user_fn,
            args,
        );
        try self.ready.append(c);
    }

    /// Drive the scheduler until ready queue is empty.
    pub fn run(self: *Runtime) void {
        const prev_runtime = current_runtime;
        current_runtime = self;
        defer current_runtime = prev_runtime;

        while (self.ready.items.len > 0) {
            const c = self.ready.orderedRemove(0);
            c.state = .running;
            current_coro = c;

            ctx.swap(&self.main_ctx, &c.ctx);
            // We come back here when the coro yields (or finishes).

            current_coro = null;

            switch (c.state) {
                .running => {
                    // Yielded but not done — re-enqueue.
                    c.state = .runnable;
                    self.ready.append(c) catch @panic("OOM during reschedule");
                },
                .done => {
                    self.done.append(c) catch @panic("OOM during done queue");
                },
                .parked => {
                    // Suspended on something else (e.g., a channel). Don't
                    // re-enqueue; whoever owns the parking will wake it.
                    // Not exercised in this spike.
                },
                .runnable => unreachable, // shouldn't be in this state mid-yield
            }
        }

        // Reap done coroutines.
        for (self.done.items) |c| cdestroy(self.allocator, c);
        self.done.clearRetainingCapacity();
    }
};

/// Yield: pause current coroutine, swap back to scheduler.
/// Scheduler will see state==.running, mark .runnable, re-enqueue.
pub fn yield() void {
    const c = current_coro orelse @panic("yield called outside a coroutine");
    ctx.swap(&c.ctx, c.scheduler_ctx);
}

/// Get the currently-executing coroutine, or null if not in one.
pub fn current() ?*Coroutine {
    return current_coro;
}

/// Get the currently-running runtime.
pub fn currentRuntime() ?*Runtime {
    return current_runtime;
}

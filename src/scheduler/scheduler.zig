//! Single-threaded round-robin scheduler — the queue and dispatch primitive.
//!
//! In v0.1 this also drove the master loop. As of v0.2, with the reactor in
//! the picture, the master loop moved up to `Runtime` so it can interleave:
//!
//!   while (work_left) {
//!       if (scheduler.tryDispatch()) continue;            // ran a ready coro
//!       if (reactor.has_pending) reactor.poll(forever);   // wake on I/O
//!       else break;                                       // genuinely idle
//!   }
//!
//! The scheduler stays small: a queue + a swap. The reactor stays small: a
//! kqueue + a wake callback. Runtime is the orchestrator.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const ctx = @import("../coroutine/context_arm64.zig");
const tls = @import("tls.zig");
const ReadyQueue = @import("ready_queue.zig").ReadyQueue;

pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    /// Scheduler's own context — coroutines yield back here.
    main_ctx: ctx.Context = .{},
    ready: ReadyQueue,
    /// All coroutines we've created. We own them and free them at deinit.
    /// (v0.5 will tighten this with structured-concurrency lifetimes; v0.1
    /// just owns everything at the runtime level.)
    spawned: std.array_list.Managed(*Coroutine),

    pub fn init(allocator: std.mem.Allocator) Scheduler {
        return .{
            .allocator = allocator,
            .ready = ReadyQueue.init(allocator),
            .spawned = std.array_list.Managed(*Coroutine).init(allocator),
        };
    }

    pub fn deinit(self: *Scheduler) void {
        // Free any coroutines we still own (whether they ran or not).
        for (self.spawned.items) |coro| {
            coro.destroy_extras_fn(self.allocator, coro);
            self.allocator.free(coro.stack);
            self.allocator.destroy(coro);
        }
        self.spawned.deinit();
        self.ready.deinit();
    }

    /// Register a freshly-created coroutine with the scheduler — track it
    /// for ownership and put it on the ready queue.
    pub fn enqueue(self: *Scheduler, coro: *Coroutine) !void {
        try self.spawned.append(coro);
        try self.ready.push(coro);
    }

    /// Re-enqueue an already-tracked coroutine (e.g., after an unpark).
    /// Does NOT add to `spawned` — only `enqueue` does that.
    pub fn requeue(self: *Scheduler, coro: *Coroutine) !void {
        try self.ready.push(coro);
    }

    /// Run one ready coroutine if any. Returns true if a coro was dispatched,
    /// false if the ready queue was empty.
    pub fn tryDispatch(self: *Scheduler, runtime_ptr: *anyopaque) bool {
        const coro = self.ready.pop() orelse return false;
        self.dispatchOne(coro, runtime_ptr);
        return true;
    }

    fn dispatchOne(self: *Scheduler, coro: *Coroutine, runtime_ptr: *anyopaque) void {
        coro.state = .running;
        tls.setCurrent(coro, runtime_ptr);
        // Yield-back target for this coroutine is our own main_ctx.
        coro.scheduler_ctx = &self.main_ctx;

        ctx.swap(&self.main_ctx, &coro.ctx);
        // We're back. Coroutine has yielded, parked, or finished.

        tls.clearCurrent();

        switch (coro.state) {
            .running => {
                // Explicitly yielded — back into the ready queue.
                coro.state = .runnable;
                self.ready.push(coro) catch @panic("OOM during reschedule");
            },
            .done => {
                // Finished. If there's a waiter (Task.join / Job.join),
                // wake it. Leave the coro in `spawned` for deinit; handles
                // can still observe state == .done.
                if (coro.waiter) |waiter| {
                    coro.waiter = null;
                    std.debug.assert(waiter.state == .parked);
                    waiter.state = .runnable;
                    self.ready.push(waiter) catch @panic("OOM during waiter wake");
                }
            },
            .parked => {
                // Suspended on something else. Whoever owns the parking
                // (reactor, channel, mutex, child join) re-enqueues when ready.
            },
            .runnable => unreachable, // shouldn't be reachable mid-yield
        }
    }
};

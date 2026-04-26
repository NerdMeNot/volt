//! Single-threaded round-robin scheduler.
//!
//! The dispatch loop:
//!   1. Pop a runnable coroutine from the ready queue
//!   2. Set TLS so anyone calling `volt.currentCoroutine()` finds it
//!   3. swap into it (registers + sp restored, lr was set by initContext)
//!   4. Coroutine runs until it yields, parks, or finishes
//!   5. swap returns; observe state; re-enqueue (runnable), reap (done),
//!      or leave alone (parked — wakeup is someone else's job)
//!   6. repeat until predicate says stop
//!
//! v0.1 is single-threaded and FIFO. Multi-worker work-stealing comes in
//! v0.9 — the interface stays stable.

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

    /// Register a freshly-created coroutine with the scheduler.
    pub fn enqueue(self: *Scheduler, coro: *Coroutine) !void {
        try self.spawned.append(coro);
        try self.ready.push(coro);
    }

    /// Drive the scheduler until the ready queue is empty.
    /// `runtime_ptr` is what `volt.currentRuntime()` returns from inside coros.
    pub fn run(self: *Scheduler, runtime_ptr: *anyopaque) void {
        while (self.ready.pop()) |coro| {
            self.dispatchOne(coro, runtime_ptr);
        }
    }

    /// Drive the scheduler until `until_done.state == .done`. Used by the
    /// bootstrap (`volt.run`) — we only need to drain enough work for the
    /// root coroutine to complete; orphan tasks are reaped at deinit.
    pub fn runUntilDone(self: *Scheduler, until_done: *Coroutine, runtime_ptr: *anyopaque) void {
        while (until_done.state != .done) {
            const coro = self.ready.pop() orelse break;
            self.dispatchOne(coro, runtime_ptr);
        }
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
                // Finished. Leave in `spawned` for deinit; handles (Job/Task)
                // can still observe state == .done.
            },
            .parked => {
                // Suspended on something else. Whoever owns the parking
                // (channel, mutex, child join) re-enqueues when ready.
            },
            .runnable => unreachable, // shouldn't be reachable mid-yield
        }
    }
};

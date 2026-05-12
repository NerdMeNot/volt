//! v2 Worker — one OS thread, one local work-steal queue, one LIFO slot.
//!
//! Each Worker owns:
//!   * A pthread on which the dispatch loop runs
//!   * A fixed-256 lock-free WorkStealQueue (LocalQueue) — owner FIFO pop,
//!     stealers steal half. Overflow spills to the global InjectionQueue.
//!   * A single LIFO slot (`lifo_slot`) — most-recently-pushed continuation.
//!     Checked first on dispatch for cache-warmth on spawn chains.
//!   * A pthread-cond Parker (cross-thread sleep on idle)
//!   * Its own `main_ctx` to swap to from coroutines
//!
//! See `work_steal_queue.zig` for the IO-geared SPMC ring design.
//! See `runtime.zig` for the dispatch loop priority + steal/park logic.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const parker_mod = @import("parker.zig");
const context = @import("context_arm64.zig");
const wsq_mod = @import("work_steal_queue.zig");

pub const Coroutine = coroutine.Coroutine;
pub const Parker = parker_mod.Parker;
pub const LocalQueue = wsq_mod.WorkStealQueue;

/// Lock-free MPMC global injection queue. Treiber stack threaded
/// through `Coroutine.next`. Used for:
///   * Cross-thread spawns from non-worker contexts (driver before
///     entering rt.run, sync primitive unpark, reactor wake)
///   * Overflow from a worker's LocalQueue when push hits CAPACITY
///   * Coroutines that get unparked while no worker is on-CPU
pub const InjectionQueue = struct {
    head: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    pub fn push(self: *InjectionQueue, c: *Coroutine) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            c.next = cur;
            if (self.head.cmpxchgWeak(cur, c, .release, .monotonic)) |observed| {
                cur = observed;
            } else return;
        }
    }

    pub fn pop(self: *InjectionQueue) ?*Coroutine {
        var cur = self.head.load(.acquire);
        while (cur) |c| {
            const next = c.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else {
                c.next = null;
                return c;
            }
        }
        return null;
    }

    pub fn isEmpty(self: *const InjectionQueue) bool {
        return self.head.load(.acquire) == null;
    }
};

threadlocal var current_worker: ?*anyopaque = null;

pub fn currentWorker() ?*anyopaque {
    return current_worker;
}

pub fn currentWorkerSet(w: ?*anyopaque) void {
    current_worker = w;
}

pub const Worker = struct {
    id: usize,
    runtime: *anyopaque, // *Runtime; opaque to avoid cycle
    main_ctx: context.Context = .{},
    local: LocalQueue,
    /// Single-slot LIFO cache for the just-pushed continuation. Spawn
    /// uses this to make the next-spawned coro run before older
    /// queued items on the same worker — pure cache-warmth
    /// optimization. Stored as an atomic so the worker can claim it
    /// without locks; a sibling steal does NOT touch the lifo_slot,
    /// keeping the warm continuation owner-local.
    lifo_slot: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),
    parker: Parker = .{},
    thread: std.Thread = undefined,
    rng: std.Random.DefaultPrng = undefined,

    pub fn init(self: *Worker, id: usize, runtime: *anyopaque) void {
        self.* = .{
            .id = id,
            .runtime = runtime,
            .local = LocalQueue.init(),
            .rng = std.Random.DefaultPrng.init(@intCast(id +% 0xDEADBEEF)),
        };
        self.parker.init();
    }

    pub fn deinit(self: *Worker) void {
        self.parker.deinit();
    }

    /// Owner-side push that uses the LIFO slot first. If the slot
    /// already holds a coro, evict it to the main queue and put the
    /// new one in the slot. Spawn-on-this-worker uses this so the
    /// just-spawned continuation has the lowest dispatch latency.
    pub fn pushLifo(self: *Worker, c: *Coroutine, global: *InjectionQueue) void {
        const evicted = self.lifo_slot.swap(c, .acq_rel);
        if (evicted) |e| self.local.push(e, global);
    }

    /// Owner-side push that goes straight to the main queue. Used for
    /// yield (re-queue a yielding coro at the tail for fairness — we
    /// don't want yield to bounce back to the front via lifo_slot).
    pub fn pushQueue(self: *Worker, c: *Coroutine, global: *InjectionQueue) void {
        self.local.push(c, global);
    }

    /// Owner-side pop in dispatch priority: lifo_slot first, then queue.
    pub fn popLocal(self: *Worker) ?*Coroutine {
        if (self.lifo_slot.swap(null, .acq_rel)) |c| return c;
        return self.local.pop();
    }
};

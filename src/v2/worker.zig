//! v2 Worker — one OS thread, one local run queue.
//!
//! Each Worker owns:
//!   - A pthread on which the dispatch loop runs
//!   - A local FIFO run queue (mutex-protected for steal access)
//!   - A Parker (for cross-thread sleep when no work)
//!   - Its own `main_ctx` to swap to from coroutines
//!
//! Dispatch loop priority:
//!   1. Local queue (FIFO, owner-favored)
//!   2. Global injection queue (cross-thread spawns + unparks land here)
//!   3. Steal from random sibling (FIFO from victim's queue front)
//!   4. Try claim reactor-poller role; if claimed, poll with timeout
//!   5. Park
//!
//! Cross-thread wake protocol:
//!   - `wakeOne()` finds a parked worker via the runtime's bitmap and
//!     unparks it. Called whenever new work is pushed (spawn, unpark,
//!     reactor delivery).

const std = @import("std");
const coroutine = @import("coroutine.zig");
const parker_mod = @import("parker.zig");
const context = @import("context_arm64.zig");
const current = @import("current.zig");
const wait_group = @import("wait_group.zig");
const deque_mod = @import("deque.zig");

pub const Coroutine = coroutine.Coroutine;
pub const Parker = parker_mod.Parker;

/// Per-worker Chase-Lev lock-free work-stealing deque.
/// Owner pushes/pops bottom (LIFO from owner). Stealers take from
/// top (FIFO of stealable items). Cache-line padded between top and
/// bottom to avoid false sharing.
///
/// Initial capacity 256; doubles on overflow via the deque's grow path.
pub const LocalQueue = deque_mod.Deque(*Coroutine);
/// 16 K slots × 8 B = 128 KiB per worker. Generous enough that the
/// deque's `grow` path doesn't fire on typical workloads (the grow
/// path immediately frees the old buffer, which races with in-flight
/// steals on different threads — same caveat as blitz's reference
/// implementation, fix tracked as a perf/correctness follow-up).
pub const LOCAL_INITIAL_CAP: usize = 16384;

/// Lock-free MPMC injection queue (Treiber stack). Used for
/// cross-thread spawns from non-worker threads, and as a fallback
/// when a worker's local queue is empty AND siblings have nothing.
///
/// LIFO is fine here: workers consume from it in parallel; ordering
/// fairness is handled by per-worker FIFO above.
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

/// Get the current worker for the calling thread (returns null if not
/// on a Worker pthread — e.g. the runtime driver thread before it
/// has parked).
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
    parker: Parker = .{},
    thread: std.Thread = undefined,
    rng: std.Random.DefaultPrng = undefined,

    pub fn init(self: *Worker, id: usize, runtime: *anyopaque, allocator: std.mem.Allocator) !void {
        self.* = .{
            .id = id,
            .runtime = runtime,
            .local = try LocalQueue.init(allocator, LOCAL_INITIAL_CAP),
            .rng = std.Random.DefaultPrng.init(@intCast(id +% 0xDEADBEEF)),
        };
        self.parker.init();
    }

    pub fn deinit(self: *Worker) void {
        self.parker.deinit();
        self.local.deinit();
    }
};

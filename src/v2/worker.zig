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

pub const Coroutine = coroutine.Coroutine;
pub const Parker = parker_mod.Parker;

const PthreadMutex = extern struct { _opaque: [8]u64 = @splat(0) };
extern "c" fn pthread_mutex_init(m: *PthreadMutex, attr: ?*anyopaque) c_int;
extern "c" fn pthread_mutex_destroy(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_lock(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_unlock(m: *PthreadMutex) c_int;

/// Per-worker FIFO queue. Owner enqueues/dequeues; siblings only
/// steal (which competes for the same mutex). Mutex-protected for
/// simplicity; Chase-Lev deque can replace this later for higher
/// throughput at high worker counts.
pub const LocalQueue = struct {
    mutex: PthreadMutex = .{},
    head: ?*Coroutine = null,
    tail: ?*Coroutine = null,
    len: usize = 0,

    pub fn init(self: *LocalQueue) void {
        _ = pthread_mutex_init(&self.mutex, null);
    }

    pub fn deinit(self: *LocalQueue) void {
        _ = pthread_mutex_destroy(&self.mutex);
    }

    pub fn push(self: *LocalQueue, c: *Coroutine) void {
        _ = pthread_mutex_lock(&self.mutex);
        defer _ = pthread_mutex_unlock(&self.mutex);
        c.next = null;
        if (self.tail) |t| {
            t.next = c;
            self.tail = c;
        } else {
            self.head = c;
            self.tail = c;
        }
        self.len += 1;
    }

    pub fn pop(self: *LocalQueue) ?*Coroutine {
        _ = pthread_mutex_lock(&self.mutex);
        defer _ = pthread_mutex_unlock(&self.mutex);
        const h = self.head orelse return null;
        self.head = h.next;
        if (self.head == null) self.tail = null;
        h.next = null;
        self.len -= 1;
        return h;
    }

    /// Steal from the front of the queue (oldest item, fair).
    pub fn steal(self: *LocalQueue) ?*Coroutine {
        return self.pop();
    }

    pub fn isEmpty(self: *const LocalQueue) bool {
        return @atomicLoad(?*Coroutine, &self.head, .acquire) == null;
    }
};

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
    local: LocalQueue = .{},
    parker: Parker = .{},
    thread: std.Thread = undefined,
    rng: std.Random.DefaultPrng = undefined,

    pub fn init(self: *Worker, id: usize, runtime: *anyopaque) void {
        self.* = .{
            .id = id,
            .runtime = runtime,
            .rng = std.Random.DefaultPrng.init(@intCast(id +% 0xDEADBEEF)),
        };
        self.local.init();
        self.parker.init();
    }

    pub fn deinit(self: *Worker) void {
        self.parker.deinit();
        self.local.deinit();
    }
};

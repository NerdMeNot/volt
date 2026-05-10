//! Coroutine: the runtime entity for one stackful task.
//!
//! Composition:
//!   - Context (callee-saved register state, swapped on suspension)
//!   - Stack (mmap'd with PROT_NONE guard page at the bottom)
//!   - `pending_event`: where the worker hands the coroutine after each yield
//!   - `done_flag`: terminal state, set by Done.subscribe
//!   - `join_park`: where joiners park to wait for completion
//!   - `cancel_flag`: cancellation, observed at suspension points
//!   - Closure pointers (the comptime-generated user-fn wrapper)
//!
//! There is NO atomic lifecycle enum on the coroutine. Possession of the
//! `*Coroutine` pointer IS the state — see `docs/design/scheduler-protocol.md`.

const std = @import("std");
const ctx = @import("context.zig");
const event_source = @import("event_source.zig");
const stack_mod = @import("stack.zig");
const Park = @import("../scheduler/park.zig").Park;

pub const EventSource = event_source.EventSource;

/// First field of every closure must match this layout — the asm trampoline
/// reads `run_fn` from offset 0 and calls run_fn(closure_ptr).
pub const ClosureBase = extern struct {
    run_fn: *const fn (*anyopaque) callconv(.c) void,
};

pub const Coroutine = struct {
    /// Saved register state for context switching.
    ctx: ctx.Context = .{},

    /// Where to yield back to. Set by the worker on every dispatch (a
    /// coroutine may resume on a different worker than it last suspended on).
    scheduler_ctx: *ctx.Context,

    /// EventSource the coroutine handed itself off to. Set by the
    /// coroutine before each swap-to-scheduler; read by the worker after
    /// swap-back. The worker calls `subscribe(coro)` which transfers
    /// ownership.
    ///
    /// Initialized to `&yield_singleton` so the field is always valid —
    /// every yield/park overwrites it before the swap.
    pending_event: *const EventSource = &event_source.yield_singleton,

    /// Terminal flag — set by Done.subscribe when the trampoline finishes.
    /// Observable by handles via Job.isCompleted, and by `until_done` checks
    /// in the bootstrap loop.
    done_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Parking slot for `Job.join` / `Task.join`. Joiners parkCurrent on
    /// this; Done.subscribe unparks it when the coroutine completes.
    join_park: Park = .{},

    /// Cancellation flag. Set by Job.cancel(); checked at suspension
    /// points (volt.yield, Park.parkCurrent) which return error.Cancelled.
    cancel_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Currently-parked-on Park, set by `Park.parkCurrent` on entry and
    /// cleared on resume. `cancel()` reads this and `unpark()`s the
    /// Park, so a parked coroutine wakes promptly on cancellation
    /// (it then observes `cancel_flag` in parkCurrent's post-resume
    /// check and returns `error.Cancelled`).
    ///
    /// `usize` (not `?*Park`) for atomic access without needing
    /// optional-pointer atomic support.
    current_park: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Set by the SIGSEGV handler when this coroutine writes past the
    /// permanent floor of its reserved stack range. When true, the
    /// coroutine's result is reported as `error.StackOverflow`; the
    /// process keeps running. (The growable stack handles small
    /// overflows transparently — this flag is set only when the entire
    /// reserved range is exhausted.)
    overflow_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// True iff this is the root coroutine watched by `Runtime.runUntilDone`.
    /// `Done.subscribe` performs a targeted wake of worker 0 when set,
    /// avoiding a `notifyAllWorkers` broadcast on every completion.
    is_root: bool = false,

    /// Optional human-readable name set by `volt.coroutine.setCurrentName`
    /// or by spawn-site annotation. Surfaces in `TaskSnapshot.name`.
    name: []const u8 = "",

    /// Optional source-location of the spawn site (set via
    /// `volt.spawnAt` / `volt.launchAt`). Surfaces in
    /// `TaskSnapshot.spawn_site` for async backtraces.
    spawn_site: ?std.builtin.SourceLocation = null,

    /// `id` of the coroutine that called `volt.spawn`/`volt.launch`
    /// to create this one. Forms an ancestry chain for async
    /// backtraces. `0` for the root coroutine (or any coro spawned
    /// from a non-coroutine context).
    parent_id: usize = 0,

    /// True after `Done.subscribe` has handed this coroutine's stack
    /// region back to the runtime stack pool. `Worker.deinit` skips
    /// freeing the stack again — the pool now owns it.
    stack_pooled: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Lifecycle rendezvous counter. Both `Done.subscribe` (terminal
    /// event) and `destroyJob` / `destroyTask` (handle release) call
    /// `lifecycle_count.fetchAdd(1, .acq_rel)`. Whichever side observes
    /// the value `1` (i.e. they are the second arrival) is responsible
    /// for freeing the Coroutine struct + its extras.
    ///
    /// This replaces the prior "free everything at Worker.deinit"
    /// model — `Worker.spawned[]` accumulated forever for long-running
    /// services, leaking ~100-200 bytes per spawn until shutdown.
    /// With the rendezvous, struct + extras are reclaimed as soon as
    /// both sides have signaled.
    ///
    /// Root coroutine is excluded — `volt.run` reads its result after
    /// `runUntilDone` returns, so the result-side increment fires
    /// from `volt.run` (not Done.subscribe). See `is_root` checks.
    lifecycle_count: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),

    /// Intrusive doubly-linked list pointers for the per-runtime
    /// `live_coroutines` registry. Manipulated only under
    /// `Runtime.live_mutex` — observers (snapshot) hold the same
    /// mutex while walking. Cleared on lifecycle release.
    prev_alive: ?*Coroutine = null,
    next_alive: ?*Coroutine = null,

    /// Debug-only: address of the Mutex this coroutine is currently
    /// waiting to acquire (0 if not waiting). Used by Mutex.lock's
    /// deadlock-cycle detector to walk the holder→waiting chain.
    /// In ReleaseFast/ReleaseSafe this field is still present (avoids
    /// per-build struct-size differences that would confuse asm
    /// offsets), but no code reads or writes it. v1.x can elide it
    /// completely via a comptime-conditional struct shape.
    waiting_on_mutex: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Owned stack reservation (whole virtual range, mmap'd PROT_NONE
    /// with a small initial committed region at the top). The runtime
    /// grows the committed portion on demand via the SIGSEGV handler.
    stack: []align(16) u8,

    /// Lowest currently-committed address within `stack`. The page
    /// below this is the guard. Updated atomically by the SIGSEGV
    /// handler when it commits a new page on overflow. Initial value
    /// is `stack.ptr + stack.len - initial_commit`.
    stack_committed_bottom: std.atomic.Value(usize) =
        std.atomic.Value(usize).init(0),

    /// Per-(user_fn, args) destroy callback — knows the type, can free
    /// closure + args correctly.
    destroy_extras_fn: *const fn (std.mem.Allocator, *Coroutine) void,

    /// Type-erased pointer to the closure (heap-allocated, stable).
    closure_ptr: *anyopaque,

    /// Type-erased pointer to the args storage. Carried separately
    /// because args are tuples and tuples can't live inline in extern.
    args_ptr: *anyopaque,

    pub fn isCancelled(self: *const Coroutine) bool {
        return self.cancel_flag.load(.acquire);
    }

    pub fn cancel(self: *Coroutine) void {
        // Race-correctness rationale: cancel() and Park.parkCurrent()
        // are two halves of an IRIW-style ordering problem.
        //
        //   cancel():        store cancel_flag → load current_park
        //   parkCurrent():   store current_park → load cancel_flag (post-store recheck)
        //
        // For the cancel signal to be observed correctly in every
        // interleaving, the two stores must form a global total order
        // — otherwise the canceller can read current_park=0 (because
        // parkCurrent hasn't stored yet) AND parkCurrent can read
        // cancel_flag=false (because it ran before the canceller
        // stored it). Both observations are individually consistent
        // with release/acquire semantics, but the coro then commits
        // to ctx_swap with cancel_flag set and no unpark scheduled —
        // a permanent leak (parked coroutine never wakes).
        //
        // Solution: seq_cst on the cancel_flag store + current_park
        // load here, paired with seq_cst on the matching ops in
        // parkCurrent. Costs a memory fence per cancel; cancellation
        // is rare so the cost is invisible.
        self.cancel_flag.store(true, .seq_cst);
        const park_addr = self.current_park.load(.seq_cst);
        if (park_addr != 0) {
            const park: *Park = @ptrFromInt(park_addr);
            park.unpark();
        }
    }

    pub fn isDone(self: *const Coroutine) bool {
        return self.done_flag.load(.acquire);
    }
};

test "coroutine: initial flags are clear" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    try std.testing.expect(!coro.isCancelled());
    try std.testing.expect(!coro.isDone());
    try std.testing.expectEqual(&event_source.yield_singleton, coro.pending_event);
}

/// Increment the lifecycle rendezvous counter; if we are the second
/// arrival (we observed the count was 1), free the Coroutine and its
/// extras (closure, args, result_ptr, stack).
///
/// Safe to call from both terminal sides:
///   - `Done.subscribe` (after a coroutine reaches its terminal swap).
///   - `destroyJob` / `destroyTask` (when the user releases the
///     handle).
///
/// The first arrival just bumps the count to 1 and returns. The
/// second arrival sees `old == 1` and is responsible for the free.
/// `acq_rel` ordering: the freer needs to observe everything the
/// other side wrote before its fetchAdd (e.g. Done.subscribe's
/// `done_flag.store`). Without acq_rel, the free could publish stale
/// state.
pub fn lifecycleRelease(allocator: std.mem.Allocator, coro: *Coroutine) void {
    const old = coro.lifecycle_count.fetchAdd(1, .acq_rel);
    if (old != 1) return; // first arrival
    // Second arrival — own the free.
    if (@import("../runtime.zig").currentRuntime()) |rt| rt.unregisterLive(coro);
    if (!coro.stack_pooled.load(.acquire)) {
        stack_mod.free(allocator, coro.stack);
    }
    // destroy_extras_fn frees the whole frame (Coroutine + Closure + args
    // + result slot, all in one allocation since the spawn-frame rework).
    // Stack memory is independent and freed above. After this call `coro`
    // is invalid memory — nothing else may touch it.
    coro.destroy_extras_fn(allocator, coro);
}

/// Unconditional release for the root coroutine. `volt.run` calls
/// this exactly once after `runUntilDone` returns and after it has
/// snapshotted `result_ptr`. The root's `Done.subscribe` deliberately
/// skips the rendezvous (it sees `is_root == true`), so this is the
/// only release path for the root's struct + extras.
///
/// `runUntilDone` has already returned, which means the bootstrap
/// thread's TLS-based `currentRuntime()` has been cleared by
/// `Worker.run`'s defer. We accept the `runtime` explicitly so we
/// can still unregister.
pub fn lifecycleReleaseRoot(rt: anytype, allocator: std.mem.Allocator, coro: *Coroutine) void {
    std.debug.assert(coro.is_root);
    rt.unregisterLive(coro);
    if (!coro.stack_pooled.load(.acquire)) {
        stack_mod.free(allocator, coro.stack);
    }
    // Single destroy: the frame allocation includes the Coroutine.
    coro.destroy_extras_fn(allocator, coro);
}

test "coroutine: cancel flag round-trip" {
    var coro_ctx: ctx.Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &coro_ctx,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };
    coro.cancel();
    try std.testing.expect(coro.isCancelled());
}

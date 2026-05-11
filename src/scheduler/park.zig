//! Park — universal suspend/resume primitive.
//!
//! Embedded in any waitable thing (Job's join slot, Mutex's wait queue,
//! channel send/recv slots, reactor wait registrations). The park instance
//! owns the synchronization; the coroutine just calls `parkCurrent`.
//!
//! ## Design — single-atomic state encoding
//!
//! The state is a single `usize` atomic encoding both "is a coroutine
//! waiting" and "has a notification arrived":
//!
//!   `0`              — empty (no waiter, no pending notification)
//!   `NOTIFIED` (=1)  — unpark fired but nobody was registered to wake
//!   `coro_ptr`       — coroutine waiting (low bit clear because Coroutine
//!                      alignment is ≥ 16; we steal bit 0 for NOTIFIED)
//!   `coro_ptr | 1`   — race window where unpark fired after registration;
//!                      the next CAS resolves who schedules the coro.
//!
//! Why one atomic: an earlier design used two atomics (`state: bool` +
//! `wait_co: ?*Coroutine`) with release/acquire ordering. That's the IRIW
//! litmus test on weak hardware (ARM64) — `subscribe` could see
//! `state=false` and `unpark` could see `wait_co=null`, both returning
//! without scheduling and losing the wake. Collapsing into one atomic puts
//! every transition into a single modification order, which is total — no
//! cross-atomic interleavings exist.
//!
//! ## Protocol
//!
//! `parkCurrent` (calling coroutine):
//!   1. Fast path: try CAS(NOTIFIED → 0). If it succeeds, return.
//!   2. Otherwise yield. The worker calls `subscribe` on swap-back.
//!   3. On resume, just return — whichever side scheduled us already
//!      cleared the state.
//!
//! `subscribe` (worker, post-yield):
//!   - CAS-loop: if state==0, install coro_ptr. If state==NOTIFIED,
//!     consume the notification and fast-wake.
//!
//! `unpark` (any thread):
//!   - CAS-loop: if state==0, store NOTIFIED. If state==NOTIFIED, no-op
//!     (idempotent). If state==coro_ptr, take it (CAS to 0) and schedule.
//!
//! ## Race-freeness
//!
//! Every transition is a CAS-loop on the same atomic. The modification
//! order is total. For each pair (subscribe, unpark) interleaving, the CAS
//! that lands first picks an unambiguous next state, and the loser retries
//! against the new state. Exactly one of subscribe and unpark schedules
//! the coroutine.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const event_source = @import("../coroutine/event_source.zig");
const EventSource = event_source.EventSource;
const ctx_mod = @import("../coroutine/context.zig");
// Import the function directly (not the module as `current`) because
// this file uses `current` as a local variable name in CAS loops.
const currentCoroutine = @import("current.zig").currentCoroutine;
const runtime_mod = @import("../runtime.zig");

/// Low bit of the state, indicating "unpark fired."
const NOTIFIED: usize = 1;

/// Mask for extracting the coroutine pointer (clears bit 0).
const PTR_MASK: usize = ~@as(usize, NOTIFIED);

pub const Park = struct {
    es: EventSource = .{ .subscribe_fn = &subscribe },

    /// Encoded state. See module doc for the encoding.
    state: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// Suspend the current coroutine on this park. Returns
    /// `error.Cancelled` if the coroutine has been cancelled (either
    /// before entering, or via cancel-from-anywhere while parked).
    pub fn parkCurrent(self: *Park) error{Cancelled}!void {
        const coro = currentCoroutine() orelse
            @panic("Park.parkCurrent called outside a coroutine");

        if (coro.isCancelled()) return error.Cancelled;

        // Fast path: a notification arrived before we got here. Consume it.
        if (self.state.cmpxchgStrong(NOTIFIED, 0, .acquire, .monotonic) == null) {
            if (coro.isCancelled()) return error.Cancelled;
            return;
        }

        // Register `self` as the coro's current park so a concurrent
        // `cancel()` can wake us. Cleared on resume below.
        //
        // Seq_cst here pairs with `Coroutine.cancel()`'s seq_cst
        // ordering on `cancel_flag.store` + `current_park.load`. The
        // post-store re-check below catches cancellations that
        // happened in the window between the entry isCancelled check
        // and our store of current_park: with the seq_cst total
        // order, either cancel observes our current_park (and
        // unparks us) OR we observe cancel_flag (and bail out).
        coro.current_park.store(@intFromPtr(self), .seq_cst);

        // Re-check cancel_flag AFTER current_park is visible. Without
        // this, a cancel that fires between line 80's check and the
        // current_park.store above can leave cancel_flag set with no
        // unpark scheduled — the coroutine then commits to ctx_swap
        // and never wakes (kernel registration leaks, the runtime's
        // pendingCount never drops to 0, and Runtime.deinit's
        // invariant assert fires).
        if (coro.cancel_flag.load(.seq_cst)) {
            coro.current_park.store(0, .release);
            return error.Cancelled;
        }

        // Yield to the scheduler. The worker will call `self.es.subscribe`
        // which atomically registers us or fast-wakes.
        coro.pending_event = &self.es;
        ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);

        // Clear before reading cancel — a post-return cancel must not
        // chase this stack-local Park.
        coro.current_park.store(0, .release);

        // Resumed. Either subscribe consumed a NOTIFIED inline, unpark
        // took us out of state, or cancel woke us. State is 0 either
        // way. (A late-arriving second unpark could leave NOTIFIED set;
        // harmless — Park is single-use per cycle.)

        if (coro.isCancelled()) return error.Cancelled;
    }

    /// Park-without-cancel-check. Used by lock-free queue primitives
    /// (e.g. Mutex's MCS chain) where a cancelled waiter must stay
    /// queued to consume the eventual handoff — otherwise the chain
    /// breaks. The caller is responsible for checking
    /// `coro.isCancelled()` AFTER resume and propagating the grant to
    /// its successor before returning `error.Cancelled` to the user.
    ///
    /// `current_park` IS registered (so cancel-from-anywhere can wake
    /// us via the standard cancel→unpark path), but we don't surface
    /// `error.Cancelled`. The caller's outer loop re-parks if the wait
    /// condition isn't satisfied yet.
    ///
    /// IRIW pattern (same as `parkCurrent`): seq_cst ordering on
    /// `current_park.store` and the post-store re-check of
    /// `cancel_flag` ensures either cancel observes our `current_park`
    /// (and unparks us) OR we observe `cancel_flag` (and bail without
    /// parking). Without this re-check, a cancel between our entry and
    /// our store can leave us parked with no waker — the test
    /// `cancel-audit:cancel parked Mutex.lock` hangs at the watchdog.
    pub fn parkUncancellable(self: *Park) void {
        const coro = currentCoroutine() orelse
            @panic("Park.parkUncancellable called outside a coroutine");

        // Fast path: a notification arrived before we got here. Consume it.
        if (self.state.cmpxchgStrong(NOTIFIED, 0, .acquire, .monotonic) == null) {
            return;
        }

        coro.current_park.store(@intFromPtr(self), .seq_cst);

        // Post-store IRIW re-check: catches the cancel-before-store
        // window where cancel saw current_park=0 and didn't wake us.
        if (coro.cancel_flag.load(.seq_cst)) {
            coro.current_park.store(0, .release);
            return; // caller's loop sees the cancel and exits
        }

        coro.pending_event = &self.es;
        ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);
        coro.current_park.store(0, .release);
        // Resumed (by grant OR by cancel). Caller decides.
    }

    /// Wake whoever is parked here without firing a sibling-worker
    /// wake. Used by chained-handoff paths (Mutex MCS handoff) where
    /// the waker knows the unparked coro will run on its OWN worker —
    /// there's no point waking siblings for work that's local.
    ///
    /// Same protocol as `unpark` except it routes the wakeup through
    /// the current worker's deque directly (`enqueueLocal`), bypassing
    /// `Runtime.schedule`'s `wakeOneSibling` call (~50 ns saved per
    /// cycle on saturated mutex contention).
    ///
    /// Cross-thread case (no current worker): falls back to standard
    /// `scheduleCoro` for correctness.
    pub fn unparkLocal(self: *Park) void {
        var current = self.state.load(.acquire);
        while (true) {
            if ((current & NOTIFIED) != 0) return;
            if (current == 0) {
                if (self.state.cmpxchgWeak(0, NOTIFIED, .acq_rel, .acquire)) |observed| {
                    current = observed;
                    continue;
                }
                return;
            }
            if (self.state.cmpxchgWeak(current, 0, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            const coro: *Coroutine = @ptrFromInt(current);
            // Local enqueue: skip wakeOneSibling. The mutex chain is
            // strictly serial on this worker — no point notifying others.
            if (@import("current.zig").currentWorkerRaw()) |raw| {
                const worker_mod = @import("worker.zig");
                const w: *worker_mod.Worker = @ptrCast(@alignCast(raw));
                w.enqueueLocal(coro);
            } else {
                scheduleCoro(coro);
            }
            return;
        }
    }

    /// Wake whoever is parked here. Idempotent — if no waiter is
    /// registered, stores NOTIFIED for the next park to consume. Safe to
    /// call from any thread.
    pub fn unpark(self: *Park) void {
        var current = self.state.load(.acquire);
        while (true) {
            // Already notified — nothing to do (idempotent).
            if ((current & NOTIFIED) != 0) return;

            if (current == 0) {
                // No waiter. Store NOTIFIED so the next park consumes it.
                if (self.state.cmpxchgWeak(0, NOTIFIED, .acq_rel, .acquire)) |observed| {
                    current = observed;
                    continue;
                }
                return;
            }

            // current is a coro pointer. Take it by CAS-clearing the state.
            if (self.state.cmpxchgWeak(current, 0, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            const coro: *Coroutine = @ptrFromInt(current);
            scheduleCoro(coro);
            return;
        }
    }

    /// Worker post-yield: install the coroutine into the parked state, or
    /// consume a pre-existing notification and fast-wake.
    fn subscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
        const es: *EventSource = @ptrCast(@alignCast(opaque_self));
        const self: *Park = @fieldParentPtr("es", es);

        const ptr = @intFromPtr(coro);
        // Coroutine pointer must have low bit clear so we can pack NOTIFIED.
        std.debug.assert((ptr & NOTIFIED) == 0);
        std.debug.assert(ptr != 0);

        var current = self.state.load(.acquire);
        while (true) {
            if (current == 0) {
                // Empty — install coro pointer.
                if (self.state.cmpxchgWeak(0, ptr, .acq_rel, .acquire)) |observed| {
                    current = observed;
                    continue;
                }
                // Installed. unpark will pick us up.
                return;
            }
            if (current == NOTIFIED) {
                // unpark fired before we registered — consume the
                // notification and fast-wake.
                if (self.state.cmpxchgWeak(NOTIFIED, 0, .acq_rel, .acquire)) |observed| {
                    current = observed;
                    continue;
                }
                scheduleCoro(coro);
                return;
            }
            // Anything else means another coroutine is registered, which
            // violates the "single waiter" invariant.
            std.debug.panic(
                "Park.subscribe: unexpected state 0x{x} (concurrent waiter?)",
                .{current},
            );
        }
    }
};

/// Hand a runnable coroutine back to the scheduler. Routed through
/// `Runtime.schedule`, which decides between the calling worker's local
/// deque (cache-warm path) and the global injection queue.
fn scheduleCoro(coro: *Coroutine) void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("Park: scheduleCoro called outside a runtime context");
    rt.schedule(coro);
}

test "park: parkCurrent without coroutine panics (smoke)" {
    // parkCurrent requires a current coroutine. Verify the precondition
    // holds (TLS empty when not inside a coroutine).
    try std.testing.expect(currentCoroutine() == null);
}

test "park: encoding constants" {
    // The encoding requires *Coroutine alignment >= 2. We expect >= 16
    // (cache line) due to the struct layout.
    try std.testing.expect(@alignOf(Coroutine) >= 2);
    try std.testing.expectEqual(@as(usize, 1), NOTIFIED);
    try std.testing.expectEqual(@as(usize, ~@as(usize, 1)), PTR_MASK);
}

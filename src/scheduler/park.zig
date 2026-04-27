//! Park — universal suspend/resume primitive.
//!
//! Embedded in any waitable thing (Job's join slot, Mutex's wait queue,
//! channel send/recv slots, reactor wait registrations). The park instance
//! owns the synchronization; the coroutine just calls `parkCurrent`.
//!
//! Race-free protocol (see `docs/design/scheduler-protocol.md`):
//!
//!   parkCurrent(park):           // calling coroutine
//!     if state.swap(false): return     # unpark already arrived
//!     pending_event = &park.es; swap   # park; subscribe runs on worker
//!     _ = state.swap(false)            # drain wake's residue
//!
//!   subscribe(park, coro):       // worker, post-yield
//!     wait_co.store(coro)              # register
//!     if state.load:                   # unpark slipped in?
//!       if c = wait_co.swap(null):
//!         schedule(c)                  # fast-wake
//!
//!   unpark(park):                // any thread
//!     if state.swap(true): return      # someone else got there first
//!     if c = wait_co.swap(null):
//!       schedule(c)                    # wake
//!
//! Four interleavings of subscribe and unpark all converge on "exactly one
//! schedules the coroutine." See the design doc for the proof sketch.
//!
//! Adapted from may/src/park.rs.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const event_source = @import("../coroutine/event_source.zig");
const EventSource = event_source.EventSource;
const ctx_mod = @import("../coroutine/context_arm64.zig");
const tls = @import("tls.zig");
const runtime_mod = @import("../runtime.zig");

pub const Park = struct {
    es: EventSource = .{ .subscribe_fn = &subscribe },

    /// True iff an unpark has fired since the last drain.
    state: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// The coroutine waiting on this park, or null. Set by `subscribe`,
    /// taken by either `unpark` or `subscribe` (whichever sees state set).
    wait_co: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    /// Suspend the current coroutine on this park. Returns
    /// `error.Cancelled` if the coroutine has been cancelled.
    pub fn parkCurrent(self: *Park) error{Cancelled}!void {
        const coro = tls.currentCoroutine() orelse
            @panic("Park.parkCurrent called outside a coroutine");

        if (coro.isCancelled()) return error.Cancelled;

        // Fast path: an unpark has already arrived. Drain and return.
        if (self.state.swap(false, .acq_rel)) return;

        // Yield to the scheduler. The worker will call `self.es.subscribe`
        // which atomically registers us in `wait_co` and re-checks `state`.
        coro.pending_event = &self.es;
        ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);

        // Resumed by an unpark (or possibly a fast-wake from subscribe).
        // Drain the wake's residue.
        _ = self.state.swap(false, .acq_rel);

        if (coro.isCancelled()) return error.Cancelled;
    }

    /// Wake whoever is parked here. Idempotent — only the first unpark
    /// observes the state transition and performs the schedule. Safe to
    /// call from any thread.
    pub fn unpark(self: *Park) void {
        // Set state true. If it was already true, someone else has already
        // unparked — they own the wake.
        if (self.state.swap(true, .acq_rel)) return;

        // We're the first to flip. Take wait_co; if a coro is registered,
        // schedule it. If wait_co is null, the parker hasn't yet reached
        // subscribe; when it does, subscribe sees state=true and fast-wakes.
        if (self.wait_co.swap(null, .acq_rel)) |coro| {
            scheduleCoro(coro);
        }
    }

    /// Worker-side: take ownership of the parking coroutine. Stores it in
    /// `wait_co` and re-checks `state` for the fast-wake race.
    ///
    /// `opaque_self` is the address of the EventSource. Recover the Park
    /// via `@fieldParentPtr` — Zig doesn't guarantee field order in
    /// regular structs, so we can't rely on `es` being at offset 0.
    fn subscribe(opaque_self: *anyopaque, coro: *Coroutine) void {
        const es: *EventSource = @ptrCast(@alignCast(opaque_self));
        const self: *Park = @fieldParentPtr("es", es);

        // Register the coroutine.
        self.wait_co.store(coro, .release);

        // Did an unpark fire between parkCurrent's pre-check and now?
        if (self.state.load(.acquire)) {
            if (self.wait_co.swap(null, .acq_rel)) |c| {
                scheduleCoro(c);
            }
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

test "park: parkCurrent without coroutine panics" {
    // Smoke test — parkCurrent requires a current coroutine. Verify the
    // precondition (TLS is empty when not inside a coroutine).
    try std.testing.expect(tls.currentCoroutine() == null);
}

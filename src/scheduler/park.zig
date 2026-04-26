//! Park — suspend the current coroutine until something else resumes it.
//!
//! `parkCurrent()` transitions the running coroutine to `.parked` and swaps
//! back to the scheduler. The coroutine stays out of the ready queue until
//! someone (the reactor, a sibling coroutine completing, etc.) marks it
//! runnable and pushes it back via `Scheduler.requeue`.
//!
//! This is the building block for:
//!   - I/O readiness wake-ups (reactor parks on EAGAIN, unparks via wakeFn)
//!   - Job.join / Task.join (parent parks until child .done)
//!   - Channel send/recv (slot-empty / queue-empty parking — v0.3)
//!   - Mutex / Semaphore acquire (queued contention — v0.4)
//!
//! Cancellation: `parkCurrent()` is a cancellation point. It returns
//! `error.Cancelled` if the cancel flag is set when the function is called
//! or after the coroutine is resumed.
//!
//! There is no `unpark` helper — each parking primitive owns its own wake
//! protocol. The reactor unparks via its wakeFn callback; the scheduler
//! unparks join-waiters when a coroutine transitions to `.done`. Channels
//! and locks will follow suit. Centralizing "unpark" here would only invite
//! ownership confusion (which list is the coro tracked in? was it already
//! re-enqueued?). Keep `parkCurrent()` symmetric with `swap`: a single
//! suspend-and-resume primitive, leaving wake-up policy to each primitive.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const ctx_mod = @import("../coroutine/context_arm64.zig");
const tls = @import("tls.zig");

pub fn parkCurrent() error{Cancelled}!void {
    const coro = tls.currentCoroutine() orelse
        @panic("park.parkCurrent called outside a coroutine");

    // Pre-park cancellation check — no point parking if we're already
    // cancelled; return immediately and let the caller unwind.
    if (coro.isCancelled()) return error.Cancelled;

    coro.storeState(.parked);
    ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);
    // We're back. Re-check cancellation in case it arrived while parked.
    if (coro.isCancelled()) return error.Cancelled;
}

test "park: parkCurrent without coroutine panics" {
    // Smoke test — we can't catch the panic here without a child process,
    // so just assert the precondition (TLS is empty when not in a coro).
    try std.testing.expect(tls.currentCoroutine() == null);
}

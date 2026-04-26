//! `volt.yield()` — explicitly hand control back to the scheduler.
//!
//! The current coroutine swaps back to the scheduler context; the scheduler
//! re-enqueues it (state remains .running, observed as "yielded" by the
//! dispatch loop) and picks the next runnable coroutine.
//!
//! Yield is also a cancellation point: if the coroutine has been cancelled,
//! yield returns error.Cancelled (giving user code a chance to clean up via
//! defer and return up the call stack).

const ctx_mod = @import("../coroutine/context_arm64.zig");
const tls = @import("../scheduler/tls.zig");

pub fn yield() error{Cancelled}!void {
    const coro = tls.currentCoroutine() orelse @panic("volt.yield called outside a coroutine");

    // Cancellation point — observe the flag before parking.
    if (coro.isCancelled()) return error.Cancelled;

    // Swap back to the scheduler. We come back here when re-dispatched.
    ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);

    // After resume, check again — cancellation may have arrived while parked.
    if (coro.isCancelled()) return error.Cancelled;
}

//! `volt.yield()` — explicit reschedule + cancellation point.
//!
//! Implemented as: set `pending_event = &yield_singleton`, swap to scheduler.
//! The worker reads `pending_event` and calls `Yield.subscribe(coro)` which
//! pushes the coroutine back onto the local deque.

const ctx_mod = @import("../coroutine/context_arm64.zig");
const tls = @import("../scheduler/tls.zig");
const event_source = @import("../coroutine/event_source.zig");

pub fn yield() error{Cancelled}!void {
    const coro = tls.currentCoroutine() orelse
        @panic("volt.yield called outside a coroutine");

    if (coro.isCancelled()) return error.Cancelled;

    // Be explicit even though `pending_event` defaults to yield_singleton —
    // a Park.parkCurrent that returned just before yield could have left it
    // pointing at the Park.
    coro.pending_event = &event_source.yield_singleton;
    ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);

    if (coro.isCancelled()) return error.Cancelled;
}

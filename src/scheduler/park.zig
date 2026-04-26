//! Park / unpark — the primitive that lets a coroutine suspend and be
//! resumed externally.
//!
//! `parkCurrent()` is called from inside a coroutine; it transitions the
//! coroutine to `.parked` and swaps back to the scheduler. The coroutine
//! stays out of the ready queue until someone calls `unpark(coro)` to
//! re-enqueue it.
//!
//! This is the building block for:
//!   - Job.join / Task.join (parent parks until child .done)
//!   - I/O readiness wake-ups (reactor parks on EAGAIN, unparks on kqueue)
//!   - Channel send/recv (slot-empty / queue-empty parking)
//!   - Mutex / Semaphore acquire (queued behind contended primitives)
//!
//! Cancellation: `parkCurrent()` is a cancellation point. If the cancel
//! flag is set when we re-enter (after being unparked), it returns
//! `error.Cancelled` so the caller can unwind.
//!
//! Thread-safety (v0.1 single-threaded, design forward-compatible):
//!   `unpark` is intended to be safe to call from any thread (e.g., the
//!   reactor running on its own thread, or a wake from a sibling worker
//!   in v0.9). The current single-worker scheduler doesn't yet enforce
//!   that — `enqueue` uses an unsynchronized array list. v0.9 will swap
//!   in a lock-free injector queue and unpark becomes truly safe.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const ctx_mod = @import("../coroutine/context_arm64.zig");
const tls = @import("tls.zig");

/// Suspend the current coroutine. Returns when someone calls `unpark` on it.
///
/// MUST be called from inside a coroutine (panics otherwise). Returns
/// `error.Cancelled` if cancellation arrived before or during the park.
pub fn parkCurrent() error{Cancelled}!void {
    const coro = tls.currentCoroutine() orelse
        @panic("park.parkCurrent called outside a coroutine");

    // Pre-park cancellation check — no point parking if we're already
    // cancelled; return immediately and let the caller unwind.
    if (coro.isCancelled()) return error.Cancelled;

    coro.state = .parked;
    ctx_mod.swap(&coro.ctx, coro.scheduler_ctx);
    // Unpark resumes us here. Re-check cancellation: a cancel may have
    // arrived while we were parked, so callers need a chance to unwind
    // even if the wake reason was unrelated.
    if (coro.isCancelled()) return error.Cancelled;
}

/// Re-enqueue a parked coroutine. Idempotent in the sense that calling it
/// on an already-runnable coroutine is undefined — callers are expected to
/// hold the parking primitive's lock to ensure exactly-once unpark per park.
///
/// Takes the runtime/scheduler pointer because unparkers don't necessarily
/// run inside a coroutine (e.g., the reactor thread); they can't use TLS.
pub fn unpark(coro: *Coroutine, scheduler_ptr: anytype) !void {
    // State transition: parked -> runnable. Use atomic when v0.9 adds
    // multi-worker. v0.1 single-threaded so plain assignment is fine.
    std.debug.assert(coro.state == .parked);
    coro.state = .runnable;
    try scheduler_ptr.scheduler.enqueue(coro);
}

test "park: parkCurrent without coroutine panics" {
    // Smoke test — we can't actually catch the panic here without a child
    // process, so just verify the function exists and the TLS is empty.
    try std.testing.expect(tls.currentCoroutine() == null);
}

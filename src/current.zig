//! Thread-local "current coroutine" tracking.
//!
//! Set by the worker on dispatch (before `ctx_swap` into the coro),
//! cleared on swap-back. Used by `yield`, `park`, IO operations, sync
//! primitives — anywhere user code inside a coroutine needs to know
//! "which coro am I?".
//!
//! ## The TLS-caching gotcha (Linux x86_64 specifically)
//!
//! A coroutine may yield/park and resume on a DIFFERENT OS thread
//! (work stealing). The threadlocal `current` lives at `%fs:offset`
//! on Linux x86_64 — LLVM is free to cache the value of `%fs:offset`
//! in a general-purpose register across function calls IF it can
//! prove nothing in between modified TLS. Most things don't, so it
//! caches aggressively.
//!
//! When a coroutine yields → migrates to thread B → resumes, code
//! that reads `current.get()` after the yield can get the CACHED
//! pointer from thread A's register state instead of thread B's
//! actual TLS value. Symptom: random `current.get() == null` panics
//! in `Task.join` and similar paths.
//!
//! The fix: wrap TLS reads in a `@call(.never_inline)` thunk. The
//! call boundary opaque-ifies the TLS read to the caller — the
//! optimizer can't see through the call to cache the result.
//!
//! Darwin arm64 doesn't have this issue (TLS via `mrs tpidrro_el0`
//! + offset is cheap enough that LLVM doesn't bother caching). But
//! the wrapping costs ~1 ns on Apple Silicon and ~3 ns on x86_64 —
//! cheap insurance.
//!
//! See feedback memory `feedback_tls_caching_x86_linux` for the
//! original incident.

const std = @import("std");
const coroutine = @import("coroutine.zig");

threadlocal var current: ?*coroutine.Coroutine = null;

pub fn set(c: *coroutine.Coroutine) void {
    // No `@call(.never_inline)` here — set/clear are owner-only
    // (dispatch fence), no cross-yield reads to worry about.
    current = c;
}

pub fn clear() void {
    current = null;
}

/// Get current coroutine, or null if called outside a runtime worker.
///
/// `@call(.never_inline)` forces a real function call to `getInner`,
/// which prevents LLVM from caching the TLS read in a register across
/// caller-side yield points. Without this, Linux x86_64 builds get
/// stale `current` reads after work-stealing migration.
pub fn get() ?*coroutine.Coroutine {
    return @call(.never_inline, getInner, .{});
}

fn getInner() ?*coroutine.Coroutine {
    return current;
}

/// Get current coroutine, panic if outside one (for APIs that REQUIRE
/// being in a coroutine, like yield/park).
///
/// Same `@call(.never_inline)` trick as `get` — the TLS read must
/// happen fresh on each call, not be cached across yields.
pub fn require() *coroutine.Coroutine {
    return @call(.never_inline, requireInner, .{});
}

fn requireInner() *coroutine.Coroutine {
    return current orelse @panic("called outside a Volt coroutine — use volt.run/spawn first");
}

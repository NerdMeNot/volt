//! Thread-local "current coroutine" tracking.
//!
//! Set by the worker on dispatch (before `ctx_swap` into the coro),
//! cleared on swap-back. Used by `yield`, `park`, IO operations, sync
//! primitives — anywhere user code inside a coroutine needs to know
//! "which coro am I?".
//!
//! No fancy structure: a single `threadlocal var *?Coroutine`. The
//! TLS overhead on Darwin arm64 is a single `mrs` + ldr (≈ 2 ns).

const std = @import("std");
const coroutine = @import("coroutine.zig");

threadlocal var current: ?*coroutine.Coroutine = null;

pub fn set(c: *coroutine.Coroutine) void {
    current = c;
}

pub fn clear() void {
    current = null;
}

/// Get current coroutine, or null if called outside a runtime worker.
pub fn get() ?*coroutine.Coroutine {
    return current;
}

/// Get current coroutine, panic if outside one (for APIs that REQUIRE
/// being in a coroutine, like yield/park).
pub fn require() *coroutine.Coroutine {
    return current orelse @panic("called outside a Volt coroutine — use volt.run/spawn first");
}

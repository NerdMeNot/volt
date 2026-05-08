//! Per-thread accessors for the currently-active coroutine, runtime,
//! and worker.
//!
//! When a coroutine is executing, three slots are populated on the worker's
//! thread:
//!   - `current_coro`    — the coroutine currently on-CPU
//!   - `current_worker`  — the worker thread executing it
//!   - `current_rt`      — the runtime that owns both
//!
//! `current_worker` and `current_rt` are set once when the worker starts
//! and cleared at shutdown. `current_coro` is set/cleared on every dispatch.
//!
//! Operations like `volt.yield()`, `volt.spawn()`, `mutex.lock()` query
//! these slots to find their context without taking the runtime / worker
//! as parameters — that's what gives the public API its Go/Kotlin feel.
//!
//! Storage is `threadlocal var`; lookup cost on Apple Silicon ARM64 is
//! one instruction (~1ns).
//!
//! ## Naming note
//!
//! The module used to be called `tls` (thread-local storage). It was
//! renamed to `current` to free up the `tls` namespace for Volt's
//! Transport-Layer-Security library (`volt-tls`). This file is about
//! "what is currently running on this thread"; the storage mechanism
//! (TLS the OS feature) is an implementation detail.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;

threadlocal var current_coro: ?*Coroutine = null;
threadlocal var current_worker: ?*anyopaque = null;
threadlocal var current_rt: ?*anyopaque = null;

/// The coroutine currently executing on this thread, or null if not in one.
pub fn currentCoroutine() ?*Coroutine {
    return current_coro;
}

/// The runtime currently active on this thread, or null. Type-erased pointer
/// because Runtime is defined in a sibling module that imports this one;
/// callers cast back to *Runtime.
pub fn currentRuntimeRaw() ?*anyopaque {
    return current_rt;
}

/// The worker executing this thread, or null. Type-erased for the same
/// reason as `currentRuntimeRaw`.
pub fn currentWorkerRaw() ?*anyopaque {
    return current_worker;
}

/// Set the coroutine slot. Called by the worker immediately before swapping
/// into a coroutine. Pair with `clearCurrent` after the swap returns.
pub fn setCurrent(coro: *Coroutine, rt: *anyopaque) void {
    current_coro = coro;
    current_rt = rt;
}

/// Clear the coroutine slot but leave the runtime/worker slots intact.
pub fn clearCurrent() void {
    current_coro = null;
}

/// Set the worker slot. Called once when the worker starts.
pub fn setCurrentWorker(worker: *anyopaque) void {
    current_worker = worker;
}

pub fn clearCurrentWorker() void {
    current_worker = null;
}

pub fn setRuntime(rt: *anyopaque) void {
    current_rt = rt;
}

pub fn clearRuntime() void {
    current_rt = null;
}

test "current: starts empty" {
    try std.testing.expect(currentCoroutine() == null);
    try std.testing.expect(currentRuntimeRaw() == null);
    try std.testing.expect(currentWorkerRaw() == null);
}

test "current: setCurrent / clearCurrent round trip" {
    var dummy_rt: u32 = 42;
    var ctx_buf: @import("../coroutine/context.zig").Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &ctx_buf,
        .stack = &[_]u8{},
        .destroy_extras_fn = undefined,
        .closure_ptr = undefined,
        .args_ptr = undefined,
    };

    setCurrent(&coro, @ptrCast(&dummy_rt));
    try std.testing.expectEqual(@as(?*Coroutine, &coro), currentCoroutine());
    try std.testing.expect(currentRuntimeRaw() != null);

    clearCurrent();
    try std.testing.expect(currentCoroutine() == null);
    try std.testing.expect(currentRuntimeRaw() != null);

    clearRuntime();
    try std.testing.expect(currentRuntimeRaw() == null);
}

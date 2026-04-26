//! Thread-local state for the active coroutine and runtime.
//!
//! When a coroutine is executing, two TLS slots are populated:
//!   - `current_coroutine` — the coroutine currently on-CPU
//!   - `current_runtime`   — the runtime that owns it
//!
//! Operations like `volt.yield()`, `volt.launch(...)`, `mutex.lock()` query
//! these slots to find their context without taking the runtime as a parameter.
//! This is what gives the public API its Go/Kotlin-feel — runtime is invisible
//! at the user level.
//!
//! Cost on Apple Silicon ARM64: a TLS read is a single instruction (~1ns).

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;

/// Forward-declared opaque pointer for the runtime — actual Runtime lives
/// in src/runtime.zig and includes this module. Circular import is avoided
/// by using *anyopaque + a typed accessor in runtime.zig itself.
threadlocal var current_coro: ?*Coroutine = null;
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

/// Set both TLS slots. Called by the scheduler immediately before swapping
/// into a coroutine. Pair with `clearCurrent` after the swap returns.
pub fn setCurrent(coro: *Coroutine, rt: *anyopaque) void {
    current_coro = coro;
    current_rt = rt;
}

/// Clear the coroutine slot but leave the runtime slot intact (the scheduler
/// itself runs in the runtime's context).
pub fn clearCurrent() void {
    current_coro = null;
}

/// Set the runtime slot only. Called by `volt.run` at bootstrap, before any
/// coroutine has been spawned, so operations like `volt.allocator` resolve.
pub fn setRuntime(rt: *anyopaque) void {
    current_rt = rt;
}

/// Clear the runtime slot. Called by `volt.run` at teardown.
pub fn clearRuntime() void {
    current_rt = null;
}

test "tls: starts empty" {
    try std.testing.expect(currentCoroutine() == null);
    try std.testing.expect(currentRuntimeRaw() == null);
}

test "tls: setCurrent / clearCurrent round trip" {
    var dummy_rt: u32 = 42;
    var ctx_buf: @import("../coroutine/context_arm64.zig").Context = .{};
    var coro: Coroutine = .{
        .scheduler_ctx = &ctx_buf,
        .state = .runnable,
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
    try std.testing.expect(currentRuntimeRaw() != null); // rt not cleared

    clearRuntime();
    try std.testing.expect(currentRuntimeRaw() == null);
}

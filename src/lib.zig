//! Volt — stackful coroutine runtime for Zig.
//!
//! Multi-threaded by design: N OS threads each run a Worker dispatch
//! loop. Coroutines are scheduled on per-worker FIFO queues with
//! work-stealing, plus a per-P mailbox for cross-thread pushes.
//! Single-worker (workers = 1) is a configuration, not a special
//! code path; the same fences and queues run in both modes.
//!
//! Headline pieces:
//!   * AAPCS64 wide-save context switch
//!   * `__ulock_wait` (Darwin) / `futex` (Linux planned) Parker
//!   * Per-worker fixed-256 work-stealing queue (overflow → mailbox)
//!   * Single-slot LIFO cache on each Worker for spawn-chain locality
//!   * Comptime-specialized channels (`Spsc`, `Mpmc`, `Oneshot`,
//!     `Watch`, `Broadcast`)
//!   * kqueue reactor with single-poller-claim and inline poll
//!   * Parking-lot wait/wake (one mechanism for every sync primitive)
//!   * Grow-on-demand stacks with PROT_NONE guard pages
//!
//! ## Quick start
//!
//! ```zig
//! var rt = try volt.Runtime.init(.{ .allocator = gpa });
//! defer rt.deinit();
//! const result = try rt.run(myFn, .{ arg1, arg2 });
//! ```
//!
//! Inside a coroutine, use `volt.spawn(fn, args)` (which infers the
//! runtime) instead of `rt.spawn(fn, args)`. The returned `*Task(T)`
//! gives you `.join()` for the result.

// ─── Public namespaces ───────────────────────────────────────────────

pub const channel = @import("channel.zig");
pub const sync = @import("sync.zig");
pub const net = @import("net.zig");

/// Threadlocal "current coroutine" lookup.
pub const current = @import("current.zig");

/// Parking-lot — coroutine wait/wake by address. For advanced users
/// implementing custom sync primitives. The built-in sync types
/// (`Mutex`, `Notify`, `Semaphore`) and channels already use this.
/// See `docs/internals/parking-lot.md`.
pub const parking = @import("park.zig");

// ─── Bootstrap & types ───────────────────────────────────────────────

pub const Runtime = @import("runtime.zig").Runtime;
pub const Config = @import("runtime.zig").Config;
pub const Task = @import("task.zig").Task;
pub const MAX_WORKERS = @import("runtime.zig").MAX_WORKERS;

// ─── Inside-coroutine helpers ────────────────────────────────────────

/// Return the `Runtime` driving the current coroutine. Panics if
/// called outside one. Replaces the
/// `@ptrCast(@alignCast(volt.current.require().runtime))` ritual.
pub fn runtime() *Runtime {
    return @ptrCast(@alignCast(current.require().runtime));
}

/// Spawn a child coroutine on the running coroutine's runtime.
/// Returns `*Task(T)` whose `.join()` parks until the child completes.
/// **Must be called from inside a coroutine** — panics otherwise.
pub fn spawn(comptime user_fn: anytype, args: anytype) !*Task(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
    return runtime().spawn(user_fn, args);
}

/// Cooperative yield. Re-queues the current coroutine to the running
/// worker's tail (FIFO — yields don't bounce via lifo_slot). Must be
/// called from inside a coroutine.
pub const yield = @import("runtime.zig").yield;

/// Sleep for at least `ns` nanoseconds. Routes through the kqueue
/// reactor's `EVFILT_TIMER` — uses zero worker-thread time, just
/// like a parked socket read. Must be called from inside a coroutine.
///
/// Kernel timer resolution is bounded below by ~1 µs on Darwin arm64;
/// shorter sleeps round up.
pub fn sleep(ns: u64) void {
    const rt = runtime();
    rt.reactor.waitTimer(ns);
}

// ─── Channel & sync convenience re-exports ───────────────────────────

pub const Spsc = channel.Spsc;
pub const Mpmc = channel.Mpmc;
pub const Oneshot = channel.Oneshot;
pub const Watch = channel.Watch;
pub const Broadcast = channel.Broadcast;

pub const Mutex = sync.Mutex;
pub const Notify = sync.Notify;
pub const Semaphore = sync.Semaphore;

// ─── Internal (not for user code) ────────────────────────────────────

/// Internal-use namespaces. Stable enough that volt's tests + benches
/// can reach them, but NOT part of the public API contract. Names
/// and shapes here may change without notice between minor versions.
pub const internal = struct {
    pub const Coroutine = @import("coroutine.zig").Coroutine;
    pub const PendingKind = @import("coroutine.zig").PendingKind;
    pub const Frame = @import("runtime.zig").Frame;
    pub const STACK_SIZE = @import("runtime.zig").STACK_SIZE;
    pub const reactor = @import("reactor_kqueue.zig");
    pub const context = @import("context_arm64.zig");
    pub const park = @import("runtime.zig").park;
    pub const unpark = @import("runtime.zig").unpark;
    pub const tryDispatchInline = @import("runtime.zig").tryDispatchInline;
};

// Ensure every sub-module's tests are reachable from the test root.
test {
    _ = @import("runtime.zig");
    _ = @import("current.zig");
    _ = @import("task.zig");
    _ = @import("coroutine.zig");
    _ = @import("channel.zig");
    _ = @import("sync.zig");
    _ = @import("net.zig");
    _ = @import("park.zig");
    _ = @import("reactor_kqueue.zig");
    _ = @import("work_steal_queue.zig");
    _ = @import("p.zig");
    _ = @import("worker.zig");
    _ = @import("parker.zig");
    _ = @import("stack.zig");
    _ = @import("signal.zig");
    _ = @import("context_arm64.zig");
}

// ─── Tests ───────────────────────────────────────────────────────────

const std = @import("std");

const SleepCtx = struct { slept_ns: i128 = 0 };

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn sleepTestRoot(ctx: *SleepCtx) !void {
    const start = nanosNow();
    sleep(10 * std.time.ns_per_ms);
    ctx.slept_ns = nanosNow() - start;
}

test "sleep parks for ~10ms via kqueue timer" {
    var rt = try Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    var ctx = SleepCtx{};
    try (try rt.run(sleepTestRoot, .{&ctx}));
    try std.testing.expect(ctx.slept_ns >= 9 * std.time.ns_per_ms);
    try std.testing.expect(ctx.slept_ns < 100 * std.time.ns_per_ms);
}

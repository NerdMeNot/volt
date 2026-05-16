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

const runtime_mod = @import("runtime.zig");
const current_mod = @import("current.zig");
const task_mod = @import("task.zig");
const coroutine_mod = @import("coroutine.zig");
const channel_mod = @import("channel.zig");
const sync_mod = @import("sync.zig");
const net_mod = @import("net.zig");
const park_mod = @import("park.zig");

// ─── Bootstrap & types ───────────────────────────────────────────────

pub const Runtime = runtime_mod.Runtime;
pub const Config = runtime_mod.Config;
pub const Task = task_mod.Task;
pub const MAX_WORKERS = runtime_mod.MAX_WORKERS;

// ─── Inside-coroutine helpers ────────────────────────────────────────

/// Return the `Runtime` driving the current coroutine. Panics if
/// called outside one. Replaces the
/// `@ptrCast(@alignCast(volt.current.require().runtime))` ritual.
pub fn runtime() *Runtime {
    return @ptrCast(@alignCast(current_mod.require().runtime));
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
pub const yield = runtime_mod.yield;

// ─── Public namespaces ───────────────────────────────────────────────

pub const channel = channel_mod;
pub const sync = sync_mod;
pub const net = net_mod;

/// Threadlocal "current coroutine" lookup. `require()` panics if
/// outside one; `get()` returns null instead.
pub const current = struct {
    pub const get = current_mod.get;
    pub const require = current_mod.require;
};

/// Parking-lot — coroutine wait/wake by address. For advanced users
/// implementing custom sync primitives. The built-in sync types
/// (`Mutex`, `Notify`, `Semaphore`) and channels already use this.
/// See `docs/internals/parking-lot.md`.
pub const parking = struct {
    pub const parkOn = park_mod.parkOn;
    pub const unparkOne = park_mod.unparkOne;
    pub const unparkAll = park_mod.unparkAll;
    pub const Validator = park_mod.Validator;
};

// ─── Channel & sync convenience re-exports ───────────────────────────

pub const Spsc = channel_mod.Spsc;
pub const Mpmc = channel_mod.Mpmc;
pub const Oneshot = channel_mod.Oneshot;
pub const Watch = channel_mod.Watch;
pub const Broadcast = channel_mod.Broadcast;

pub const Mutex = sync_mod.Mutex;
pub const Notify = sync_mod.Notify;
pub const Semaphore = sync_mod.Semaphore;

// ─── Internal (not for user code) ────────────────────────────────────

/// Internal-use namespaces. Stable enough that volt's tests + benches
/// can reach them, but NOT part of the public API contract. Names
/// and shapes here may change without notice between minor versions.
pub const internal = struct {
    pub const Coroutine = coroutine_mod.Coroutine;
    pub const PendingKind = coroutine_mod.PendingKind;
    pub const Frame = runtime_mod.Frame;
    pub const STACK_SIZE = runtime_mod.STACK_SIZE;
    pub const reactor = @import("reactor_kqueue.zig");
    pub const context = @import("context_arm64.zig");
    pub const current_set = current_mod.set;
    pub const current_clear = current_mod.clear;
    pub const park = runtime_mod.park;
    pub const unpark = runtime_mod.unpark;
    pub const tryDispatchInline = runtime_mod.tryDispatchInline;
};

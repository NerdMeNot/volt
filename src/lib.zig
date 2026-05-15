//! Volt — stackful coroutine runtime for Zig.
//!
//! Multi-threaded by design: N OS threads each run a Worker dispatch
//! loop. Coroutines are scheduled on per-worker FIFO queues with
//! work-stealing, plus a global injection queue for cross-thread
//! pushes. Single-worker (workers = 1) is a configuration, not a
//! special code path; the same fences and queues run in both modes.
//!
//! Headline pieces:
//!   * AAPCS64 (and later: x86_64, others) wide-save context switch
//!   * `__ulock_wait` (Darwin) / `futex` (Linux) Parker
//!   * Per-worker fixed-256 work-stealing queue (overflow → injection)
//!   * Single-slot LIFO cache on each Worker for spawn-chain locality
//!   * Comptime-specialized SPSC channels
//!   * kqueue reactor with single-poller-claim and inline poll
//!   * Parking-lot wait/wake (one mechanism for every sync primitive)
//!
//! Public surface:
//!
//! ```zig
//! var rt = try volt.Runtime.init(.{ .allocator = gpa });
//! defer rt.deinit();
//!
//! const result = try rt.run(myFn, .{ arg1, arg2 });
//! ```
//!
//! Or, from inside the root coroutine, `rt.spawn(fn, args)` returns
//! a `*Task(T)` whose `.join()` parks until the child completes.

pub const Runtime = @import("runtime.zig").Runtime;
pub const STACK_SIZE = @import("runtime.zig").STACK_SIZE;
pub const Frame = @import("runtime.zig").Frame;
pub const yield = @import("runtime.zig").yield;

/// Parking-lot — coroutine-level wait/wake by address. The
/// foundation for every sync primitive. See `src/park.zig` and
/// `docs/internals/parking-lot.md`.
pub const parking = @import("park.zig");

pub const Coroutine = @import("coroutine.zig").Coroutine;
pub const PendingKind = @import("coroutine.zig").PendingKind;
pub const Task = @import("task.zig").Task;

pub const channel = @import("channel.zig");
pub const Spsc = channel.Spsc;
pub const Mpmc = channel.Mpmc;

pub const net = @import("net.zig");
pub const reactor = @import("reactor_kqueue.zig");

pub const sync = @import("sync.zig");
pub const Mutex = sync.Mutex;
pub const Notify = sync.Notify;
pub const Semaphore = sync.Semaphore;

pub const context = @import("context_arm64.zig");
pub const current = @import("current.zig");

//! Volt v2 — POC-validated stackful coroutine runtime.
//!
//! Architecture (validated 2026-05-12 in `spike/SYNTHESIS.md`):
//!   * Single-worker spin scheduler (POC-G: 22 ns/task floor)
//!   * Wide-save AAPCS64 ctx switch (POC-A: 6 ns/swap)
//!   * Heap-alloc'd 16 KiB stacks, smp_allocator backing
//!   * Atomic-counter WaitGroup join (no per-coro Park)
//!   * Comptime-specialized SPSC channels (POC-F: 29 ns/op)
//!   * Tight kqueue reactor with inline poll (POC-H: 2.5 µs/RTT pipe)
//!
//! Current state: foundation + comptime-typed spawn API.
//! Public surface:
//!
//! ```zig
//! var rt = volt2.Runtime.init(allocator);
//! defer rt.deinit();
//!
//! // Typed spawn with args + return value
//! var task = try rt.spawn(myFn, .{ arg1, arg2 });
//! rt.run();
//! const result = task.join();
//! ```

pub const Runtime = @import("runtime.zig").Runtime;
pub const RunQueue = @import("runtime.zig").RunQueue;
pub const STACK_SIZE = @import("runtime.zig").STACK_SIZE;
pub const Frame = @import("runtime.zig").Frame;
pub const yield = @import("runtime.zig").yield;
pub const park = @import("runtime.zig").park;
pub const unpark = @import("runtime.zig").unpark;

pub const Coroutine = @import("coroutine.zig").Coroutine;
pub const PendingKind = @import("coroutine.zig").PendingKind;
pub const WaitGroup = @import("wait_group.zig").WaitGroup;
pub const Task = @import("task.zig").Task;

pub const channel = @import("channel.zig");
pub const Spsc = channel.Spsc;

pub const net = @import("net.zig");
pub const reactor = @import("reactor_kqueue.zig");

pub const sync = @import("sync.zig");
pub const Mutex = sync.Mutex;
pub const Notify = sync.Notify;
pub const Semaphore = sync.Semaphore;

pub const context = @import("context_arm64.zig");
pub const current = @import("current.zig");

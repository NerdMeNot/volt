//! Combined module exports for tests
//!
//! This module re-exports sync and channel primitives for use in tests.
//! Since sync and channel both use internal utilities, they must be part
//! of the same module to avoid "file exists in multiple modules" errors.

// ═══════════════════════════════════════════════════════════════════════════════
// Sync Primitives
// ═══════════════════════════════════════════════════════════════════════════════

// Notify
pub const notify = @import("sync/Notify.zig");
pub const Notify = notify.Notify;
pub const NotifyWaiter = notify.Waiter;
pub const NotifyWaitFuture = notify.WaitFuture;

// Semaphore
pub const semaphore = @import("sync/Semaphore.zig");
pub const Semaphore = semaphore.Semaphore;
pub const SemaphoreWaiter = semaphore.Waiter;
pub const SemaphorePermit = semaphore.SemaphorePermit;
pub const AcquireFuture = semaphore.AcquireFuture;

// Mutex
pub const mutex = @import("sync/Mutex.zig");
pub const Mutex = mutex.Mutex;
pub const MutexWaiter = mutex.Waiter;
pub const MutexGuard = mutex.MutexGuard;
pub const LockFuture = mutex.LockFuture;

// RwLock
pub const rwlock = @import("sync/RwLock.zig");
pub const RwLock = rwlock.RwLock;
pub const ReadWaiter = rwlock.ReadWaiter;
pub const WriteWaiter = rwlock.WriteWaiter;
pub const ReadGuard = rwlock.ReadGuard;
pub const WriteGuard = rwlock.WriteGuard;
pub const ReadLockFuture = rwlock.ReadLockFuture;
pub const WriteLockFuture = rwlock.WriteLockFuture;

// Barrier
pub const barrier = @import("sync/Barrier.zig");
pub const Barrier = barrier.Barrier;
pub const BarrierWaiter = barrier.Waiter;
pub const BarrierWaitResult = barrier.BarrierWaitResult;
pub const BarrierWaitFuture = barrier.WaitFuture;

// OnceCell
pub const once_cell = @import("sync/OnceCell.zig");
pub const OnceCell = once_cell.OnceCell;
pub const OnceCellWaiter = once_cell.InitWaiter;
pub const OnceCellState = once_cell.State;
pub const GetOrInitFuture = once_cell.GetOrInitFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// Channel Primitives
// ═══════════════════════════════════════════════════════════════════════════════

const std = @import("std");
const Allocator = std.mem.Allocator;

// Bounded MPSC Channel
pub const channel = @import("channel/Channel.zig");
pub const Channel = channel.Channel;
pub const SendWaiter = channel.SendWaiter;
pub const RecvWaiter = channel.RecvWaiter;
pub const ChannelSendFuture = channel.SendFuture;
pub const ChannelRecvFuture = channel.RecvFuture;

pub fn bounded(comptime T: type, allocator: Allocator, capacity: usize) !Channel(T) {
    return Channel(T).init(allocator, capacity);
}

// Oneshot Channel
pub const oneshot_mod = @import("channel/Oneshot.zig");
pub const Oneshot = oneshot_mod.Oneshot;
pub const OneshotState = oneshot_mod.State;

pub fn oneshot(comptime T: type) Oneshot(T) {
    return Oneshot(T).init();
}

// Broadcast Channel
pub const broadcast_mod = @import("channel/Broadcast.zig");
pub const BroadcastChannel = broadcast_mod.BroadcastChannel;
pub const BroadcastRecvWaiter = broadcast_mod.RecvWaiter;

pub fn broadcast(comptime T: type, allocator: Allocator, capacity: usize) !BroadcastChannel(T) {
    return BroadcastChannel(T).init(allocator, capacity);
}

// Watch Channel
pub const watch_mod = @import("channel/Watch.zig");
pub const Watch = watch_mod.Watch;
pub const WatchChangeWaiter = watch_mod.ChangeWaiter;

pub fn watch(comptime T: type, initial: T) Watch(T) {
    return Watch(T).init(initial);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Select Support
// ═══════════════════════════════════════════════════════════════════════════════

pub const cancel_token = @import("sync/CancelToken.zig");
pub const CancelToken = cancel_token.CancelToken;

pub const select_context = @import("sync/SelectContext.zig");
pub const SelectContext = select_context.SelectContext;
pub const BranchWaker = select_context.BranchWaker;
pub const MAX_SELECT_BRANCHES = select_context.MAX_SELECT_BRANCHES;

pub const select_mod = @import("channel/select.zig");
pub const Selector = select_mod.Selector;
pub const SelectResult = select_mod.SelectResult;
pub const selector = select_mod.selector;
pub const select2Recv = select_mod.select2Recv;
pub const select3Recv = select_mod.select3Recv;

// ═══════════════════════════════════════════════════════════════════════════════
// Blocking Pool
// ═══════════════════════════════════════════════════════════════════════════════

pub const blocking = @import("internal/blocking.zig");
pub const BlockingPool = blocking.BlockingPool;
pub const BlockingHandle = blocking.BlockingHandle;
pub const BlockingTask = blocking.Task;
pub const runBlocking = blocking.runBlocking;

// ═══════════════════════════════════════════════════════════════════════════════
// Future Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const future = @import("future.zig");
pub const Context = future.Context;
pub const PollResult = future.PollResult;
pub const Waker = future.Waker;

// ═══════════════════════════════════════════════════════════════════════════════
// Scheduler Runtime (for async integration benchmarks)
// ═══════════════════════════════════════════════════════════════════════════════

pub const scheduler = @import("internal/scheduler/Runtime.zig");
pub const Runtime = scheduler.Runtime;
pub const JoinHandle = scheduler.JoinHandle;

// ═══════════════════════════════════════════════════════════════════════════════
// Common Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Function pointer type for waking a suspended task.
pub const WakerFn = *const fn (*anyopaque) void;

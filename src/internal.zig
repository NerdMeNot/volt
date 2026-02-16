//! # Internal Implementation Details
//!
//! **WARNING: This module is not part of the public API.**
//!
//! Types and functions in this module may change without notice between versions.
//! Use the public API (`io.task`, `io.sync`, `io.channel`, etc.) instead.
//!
//! ## When to Use Internal APIs
//!
//! Only use these if you're:
//! - Building custom I/O backends
//! - Implementing new sync primitives
//! - Contributing to volt itself
//! - Need low-level scheduler access for debugging

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Scheduler Internals
// ═══════════════════════════════════════════════════════════════════════════════

/// Scheduler internals - task headers, queues, work-stealing.
pub const scheduler = struct {
    pub const Task = @import("internal/scheduler/Header.zig").Task;
    pub const Header = @import("internal/scheduler/Header.zig").Header;
    pub const State = @import("internal/scheduler/Header.zig").State;
    pub const Lifecycle = @import("internal/scheduler/Header.zig").Lifecycle;
    pub const TaskQueue = @import("internal/scheduler/Header.zig").TaskQueue;
    pub const GlobalTaskQueue = @import("internal/scheduler/Header.zig").GlobalTaskQueue;
    pub const WorkStealQueue = @import("internal/scheduler/Header.zig").WorkStealQueue;
    pub const Scheduler = @import("internal/scheduler/Scheduler.zig").Scheduler;
    pub const Worker = @import("internal/scheduler/Scheduler.zig").Worker;
    pub const FastRand = @import("internal/scheduler/Scheduler.zig").FastRand;
    pub const SchedulerRuntime = @import("internal/scheduler/Runtime.zig").Runtime;
    pub const JoinHandle = @import("internal/scheduler/Runtime.zig").JoinHandle;
    pub const Deque = @import("internal/scheduler/Deque.zig").Deque;
    pub const StealResult = @import("internal/scheduler/Deque.zig").StealResult;

    pub const timer = @import("internal/scheduler/TimerWheel.zig");
    pub const TimerWheel = timer.TimerWheel;
    pub const TimerEntry = timer.TimerEntry;

    const runtime_mod = @import("internal/scheduler/Runtime.zig");
    pub const Backend = runtime_mod.Backend;
    pub const BackendType = runtime_mod.BackendType;
    pub const BackendConfig = runtime_mod.BackendConfig;
    pub const Operation = runtime_mod.Operation;
    pub const Completion = runtime_mod.Completion;
};

// ═══════════════════════════════════════════════════════════════════════════════
// I/O Backend
// ═══════════════════════════════════════════════════════════════════════════════

/// I/O backend abstraction (io_uring, kqueue, epoll, IOCP).
pub const backend = @import("internal/backend.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Executor
// ═══════════════════════════════════════════════════════════════════════════════

/// Task executor (wraps scheduler).
pub const executor = @import("internal/executor.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Blocking Pool
// ═══════════════════════════════════════════════════════════════════════════════

/// Blocking thread pool for CPU-intensive work.
const blocking_mod = @import("internal/blocking.zig");
pub const BlockingPool = blocking_mod.BlockingPool;
pub const BlockingHandle = blocking_mod.BlockingHandle;
pub const runBlocking = blocking_mod.runBlocking;

// ═══════════════════════════════════════════════════════════════════════════════
// Future Primitives
// ═══════════════════════════════════════════════════════════════════════════════

/// Stackless future primitives.
pub const future = @import("future.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Utility Data Structures
// ═══════════════════════════════════════════════════════════════════════════════

/// Internal data structures.
pub const util = struct {
    pub const Slab = @import("internal/util/slab.zig").Slab;
    pub const LinkedList = @import("internal/util/linked_list.zig").LinkedList;
    pub const Pointers = @import("internal/util/linked_list.zig").Pointers;
    pub const WakeList = @import("internal/util/wake_list.zig").WakeList;
    pub const CacheLine = @import("internal/util/cacheline.zig");
    pub const ObjectPool = @import("internal/util/pool.zig").ObjectPool;
    pub const InvocationId = @import("internal/util/invocation_id.zig").InvocationId;
    pub const WaiterInvocation = @import("internal/util/invocation_id.zig").WaiterInvocation;
    pub const StackGuard = @import("internal/util/stack_guard.zig").StackGuard;
    pub const completion = @import("internal/util/waker.zig");
};

// ═══════════════════════════════════════════════════════════════════════════════
// Testing Utilities
// ═══════════════════════════════════════════════════════════════════════════════

/// Testing utilities for concurrent code.
pub const testing = struct {
    pub const concurrency = @import("internal/test/concurrency.zig");
    pub const atomic_log = @import("internal/test/atomic_log.zig");
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "internal module compiles" {
    _ = scheduler;
    _ = backend;
    _ = executor;
    _ = BlockingPool;
    _ = future;
    _ = util;
}

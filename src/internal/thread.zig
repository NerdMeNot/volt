//! Volt's raw thread-level synchronization primitives.
//!
//! Zig 0.16 moved std.Thread.{Mutex,Condition,Futex,sleep} into std.Io, where
//! they require an Io parameter — which doesn't work inside the scheduler that
//! *implements* Io. This module provides the raw-thread equivalents for Volt's
//! internal use: scheduler global queue, blocking pool condvar, channel waiter
//! mutexes, etc.
//!
//! For async-aware synchronization (Futures that yield to the scheduler), see
//! volt.sync (Mutex, RwLock, Semaphore, Notify, Barrier).

pub const Futex = @import("thread/Futex.zig");
pub const Mutex = @import("thread/Mutex.zig");
pub const Condition = @import("thread/Condition.zig");
pub const sleep = @import("thread/sleep.zig").sleep;

test {
    _ = Futex;
    _ = Mutex;
    _ = Condition;
    _ = @import("thread/sleep.zig");
}

//! # Synchronization Primitives for Async Tasks
//!
//! Async-aware synchronization primitives that integrate with the volt
//! task system. Unlike blocking OS primitives, these yield to the scheduler
//! when contended, allowing other tasks to make progress.
//!
//! ## Available Primitives
//!
//! | Primitive | Use Case |
//! |-----------|----------|
//! | `Mutex` | Protect shared state |
//! | `RwLock` | Many readers, one writer |
//! | `Semaphore` | Limit concurrent access |
//! | `Notify` | Task wake-up (like condvar) |
//! | `Barrier` | Wait for N tasks |
//! | `OnceCell` | Lazy one-time init |
//!
//! ## API Patterns
//!
//! Each primitive provides two tiers:
//!
//! | Pattern | Behavior |
//! |---------|----------|
//! | `tryX()` | Non-blocking, returns immediately |
//! | `x()` → Future | Returns a Future for the scheduler |
//!
//! ## Usage
//!
//! ```zig
//! var mutex = io.sync.Mutex.init();
//!
//! // Non-blocking
//! if (mutex.tryLock()) {
//!     defer mutex.unlock();
//!     // critical section
//! }
//!
//! // Async (returns Future)
//! var lock_future = mutex.lock();
//! const handle = try io.spawnFuture(lock_future);
//! ```
//!
//! ```zig
//! var sem = io.sync.Semaphore.init(100);
//!
//! // Non-blocking
//! if (sem.tryAcquire(1)) {
//!     defer sem.release(1);
//!     handleConnection();
//! }
//!
//! // Async (returns Future)
//! var future = sem.acquire(2);
//! ```
//!
//! ```zig
//! var rwlock = io.sync.RwLock.init();
//!
//! // Non-blocking
//! if (rwlock.tryReadLock()) {
//!     defer rwlock.readUnlock();
//!     const value = shared_data.get();
//! }
//!
//! // Async (returns Future)
//! var read_future = rwlock.readLock();
//! var write_future = rwlock.writeLock();
//! ```
//!
//! ## Resource Management
//!
//! All primitives are zero-allocation and require no `deinit()`.
//!
//! ## Advanced
//!
//! Low-level waiter and future types are available via sub-modules
//! (e.g. `sync.mutex.Waiter`, `sync.mutex.LockFuture`) for custom
//! scheduling integration.

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Primary API — 6 types
// ═══════════════════════════════════════════════════════════════════════════════

/// Async mutual exclusion. Protect shared state from concurrent access.
/// Use `tryLock()` for non-blocking, `lock()` for async Future.
pub const Mutex = mutex.Mutex;

/// Async read-write lock. Many readers OR one writer at a time.
/// Use for read-heavy workloads where writes are infrequent.
pub const RwLock = rwlock.RwLock;

/// Counting semaphore. Limit concurrent access to a resource.
/// Initialize with count N, at most N tasks can hold permits simultaneously.
pub const Semaphore = semaphore.Semaphore;

/// Task notification primitive. Wake waiters without passing data.
/// Use when tasks need to signal each other (e.g., "data is ready").
pub const Notify = notify.Notify;

/// Synchronization barrier. Block until N tasks have arrived.
/// One task is designated "leader" for final processing.
pub const Barrier = barrier.Barrier;

/// Lazy one-time initialization. Value computed on first access.
/// Safe for concurrent initialization — only one task does the work.
pub const OnceCell = once_cell.OnceCell;

// ═══════════════════════════════════════════════════════════════════════════════
// Sub-modules — advanced/internal types (Waiter, Guard, Future, etc.)
// ═══════════════════════════════════════════════════════════════════════════════

pub const mutex = @import("sync/Mutex.zig");
pub const rwlock = @import("sync/RwLock.zig");
pub const semaphore = @import("sync/Semaphore.zig");
pub const notify = @import("sync/Notify.zig");
pub const barrier = @import("sync/Barrier.zig");
pub const once_cell = @import("sync/OnceCell.zig");

// Select support (internal)
pub const cancel_token = @import("sync/CancelToken.zig");
pub const select_context = @import("sync/SelectContext.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    // Run all sub-module tests
    _ = @import("sync/Notify.zig");
    _ = @import("sync/Semaphore.zig");
    _ = @import("sync/Mutex.zig");
    _ = @import("channel/Oneshot.zig");
    _ = @import("channel/Channel.zig");
    _ = @import("sync/RwLock.zig");
    _ = @import("sync/Barrier.zig");
    _ = @import("channel/Broadcast.zig");
    _ = @import("channel/Watch.zig");
    _ = @import("sync/OnceCell.zig");
    _ = @import("sync/CancelToken.zig");
    _ = @import("sync/SelectContext.zig");
}

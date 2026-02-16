---
title: Sync Primitives API
description: API reference for Mutex, RwLock, and Semaphore in Volt. For Notify, Barrier, and OnceCell, see the Coordination API.
---

When multiple tasks need to share state or coordinate work, you reach for sync primitives. Volt provides six of them -- each designed for a specific pattern you'll hit in real applications:

- **Mutex** -- protect a shared resource (session cache, counters, connection state)
- **RwLock** -- let many readers proceed in parallel, block only for writes (config stores, routing tables)
- **Semaphore** -- limit concurrency to N (connection pools, rate limiters, batch size caps)

For coordination primitives (Notify, Barrier, OnceCell), see the [Coordination API](/api/coordination-api/).

All primitives are **async-aware**: when contended, they yield to the scheduler instead of blocking the OS thread. They are also **zero-allocation** -- waiter structs are embedded in futures, not heap-allocated. No `deinit()` needed.

### At a Glance

```zig
const volt = @import("volt");

fn example(io: volt.Io) void {
    // Protect shared state with a Mutex (convenience -- takes io, suspends until acquired)
    var mutex = volt.sync.Mutex.init();
    mutex.lock(io);
    defer mutex.unlock();
    // critical section -- only one task at a time

    // Or non-blocking:
    if (mutex.tryLock()) {
        defer mutex.unlock();
        // critical section
    }

    // Limit concurrent DB connections with a Semaphore
    var pool = volt.sync.Semaphore.init(10); // 10 permits
    pool.acquire(io, 1); // suspends until permit available
    defer pool.release(1);
    // use one connection slot

    // Lazy-init a singleton with OnceCell
    var db = volt.sync.OnceCell(DbPool).init();
    const p = db.getOrInit(io, createPool); // first caller inits, rest get cached (~0.4ns)
}
```

Each primitive provides four API tiers:

| Tier | Pattern | Behavior |
|------|---------|----------|
| Non-blocking | `tryX()` | Returns immediately (lock-free CAS) |
| Convenience | `x(io)` | Takes `Io` handle, suspends until complete |
| Async Future | `xFuture()` | Returns a Future for manual scheduling |
| Waiter | `xWait(waiter)` | Low-level, caller manages waiter lifecycle |

The **convenience** tier (takes `io`) is recommended for most use cases. It suspends the calling task until the operation completes, keeping the code sequential and readable. The **Future** tier is for advanced patterns like spawning a lock acquisition as a separate task or composing with other futures.

## Mutex

Mutual exclusion lock. Built on `Semaphore(1)`.

```zig
const Mutex = volt.sync.Mutex;
```

### Construction

```zig
var mutex = Mutex.init();
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `tryLock` | `fn tryLock(self: *Mutex) bool` | Non-blocking lock attempt. Returns `true` if acquired. |
| `unlock` | `fn unlock(self: *Mutex) void` | Release the lock. Wakes the next waiter (FIFO). |
| `lock` | `fn lock(self: *Mutex, io: volt.Io) void` | Convenience: acquire the lock, suspending until available. Takes the `Io` handle. |
| `lockFuture` | `fn lockFuture(self: *Mutex) LockFuture` | Returns a Future that resolves when the lock is acquired. |
| `lockWait` | `fn lockWait(self: *Mutex, waiter: *Waiter) bool` | Waiter-based lock. Returns `true` if acquired immediately. |
| `cancelLock` | `fn cancelLock(self: *Mutex, waiter: *Waiter) void` | Remove a waiter from the queue. |
| `tryLockGuard` | `fn tryLockGuard(self: *Mutex) ?MutexGuard` | Non-blocking lock with RAII guard. |
| `isLocked` | `fn isLocked(self: *const Mutex) bool` | Debug: check if locked. |
| `waiterCount` | `fn waiterCount(self: *Mutex) usize` | Debug: number of queued waiters. |

### Convenience: `lock(io)`

The simplest way to acquire a mutex in an async task. Suspends the current task until the lock is available, then returns. The caller must call `unlock()` when done.

```zig
fn updateCounter(io: volt.Io, mutex: *volt.sync.Mutex, counter: *u64) void {
    mutex.lock(io);        // suspends if contended
    defer mutex.unlock();
    counter.* += 1;
}
```

### LockFuture

```zig
var future = mutex.lockFuture();
defer future.deinit();
// Output: void. Resolves when lock is acquired.
// Caller must call mutex.unlock() after use.
```

`LockFuture` implements the Future trait (`pub const Output = void`). Use `lockFuture()` when you need to spawn the lock acquisition as a separate task or compose it with other futures.

### Waiter

```zig
const Waiter = volt.sync.mutex.Waiter;

var waiter = Waiter.init();
waiter.setWaker(@ptrCast(&ctx), wakeFn);

if (mutex.lockWait(&waiter)) {
    // Acquired immediately
} else {
    // Queued -- yield and wait for wake
}
// After wake: waiter.isAcquired() == true
defer mutex.unlock();
```

### MutexGuard (RAII)

```zig
if (mutex.tryLockGuard()) |guard| {
    var g = guard;
    defer g.deinit(); // Automatically unlocks
}
```

### Example: Thread-Safe Counter

A counter shared across multiple tasks. Each task increments the counter under the mutex, ensuring no updates are lost. The convenience `lock(io)` method suspends until the lock is available.

```zig
const std = @import("std");
const volt = @import("volt");

const SharedCounter = struct {
    mutex: volt.sync.Mutex,
    value: u64,

    fn init() SharedCounter {
        return .{
            .mutex = volt.sync.Mutex.init(),
            .value = 0,
        };
    }

    /// Increment the counter by 1, returning the previous value.
    /// Uses lock(io) to suspend until the lock is acquired.
    fn increment(self: *SharedCounter, io: volt.Io) u64 {
        self.mutex.lock(io);
        defer self.mutex.unlock();
        const prev = self.value;
        self.value += 1;
        return prev;
    }

    /// Non-blocking read for contexts where you cannot suspend.
    fn tryGet(self: *SharedCounter) ?u64 {
        if (self.mutex.tryLock()) {
            defer self.mutex.unlock();
            return self.value;
        }
        return null;
    }
};

var counter = SharedCounter.init();

fn workerTask(io: volt.Io) void {
    const prev = counter.increment(io);
    std.log.info("counter was {d}, now {d}", .{ prev, prev + 1 });
}
```

### Example: Shared Cache with MutexGuard

Protecting a HashMap with a Mutex so multiple tasks can read and write cached values. The RAII guard ensures the lock is always released, even if lookup logic returns early.

```zig
const std = @import("std");
const volt = @import("volt");

const SessionCache = struct {
    mutex: volt.sync.Mutex,
    // The map itself is not thread-safe, so all access goes through the mutex.
    sessions: std.StringHashMap(SessionData),

    const SessionData = struct {
        user_id: u64,
        expires_at: i64,
    };

    fn init(allocator: std.mem.Allocator) SessionCache {
        return .{
            .mutex = volt.sync.Mutex.init(),
            .sessions = std.StringHashMap(SessionData).init(allocator),
        };
    }

    fn deinit(self: *SessionCache) void {
        self.sessions.deinit();
    }

    /// Look up a session by token. Returns null if not found or expired.
    fn lookup(self: *SessionCache, token: []const u8, now: i64) ?SessionData {
        // tryLockGuard returns an RAII guard that auto-unlocks on scope exit.
        if (self.mutex.tryLockGuard()) |guard| {
            var g = guard;
            defer g.deinit();

            const entry = self.sessions.get(token) orelse return null;
            if (entry.expires_at < now) return null; // Expired
            return entry;
        }
        return null; // Lock contended, caller can retry
    }

    /// Store a session. Uses lock(io) to suspend until lock is available.
    fn store(self: *SessionCache, io: volt.Io, token: []const u8, data: SessionData) !void {
        self.mutex.lock(io);
        defer self.mutex.unlock();
        try self.sessions.put(token, data);
    }
};
```

:::note
Use `tryLock` / `tryLockGuard` for fast non-blocking paths. In an async task, use `mutex.lock(io)` which suspends until the lock is available. For advanced use cases (spawning the lock as a task, composing with other futures), use `mutex.lockFuture()`.
:::

---

## RwLock

Read-write lock. Multiple concurrent readers OR one exclusive writer. Built on `Semaphore(MAX_READS)`.

```zig
const RwLock = volt.sync.RwLock;
```

### Construction

```zig
var rwlock = RwLock.init();
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `tryReadLock` | `fn tryReadLock(self: *RwLock) bool` | Non-blocking read lock. |
| `readUnlock` | `fn readUnlock(self: *RwLock) void` | Release read lock. |
| `readLock` | `fn readLock(self: *RwLock, io: volt.Io) void` | Convenience: acquire read lock, suspending until available. |
| `readLockFuture` | `fn readLockFuture(self: *RwLock) ReadLockFuture` | Returns a Future for read lock acquisition. |
| `readLockWait` | `fn readLockWait(self: *RwLock, waiter: *ReadWaiter) bool` | Waiter-based read lock. |
| `cancelReadLock` | `fn cancelReadLock(self: *RwLock, waiter: *ReadWaiter) void` | Cancel pending read lock. |
| `tryWriteLock` | `fn tryWriteLock(self: *RwLock) bool` | Non-blocking write lock. |
| `writeUnlock` | `fn writeUnlock(self: *RwLock) void` | Release write lock. |
| `writeLock` | `fn writeLock(self: *RwLock, io: volt.Io) void` | Convenience: acquire write lock, suspending until available. |
| `writeLockFuture` | `fn writeLockFuture(self: *RwLock) WriteLockFuture` | Returns a Future for write lock acquisition. |
| `writeLockWait` | `fn writeLockWait(self: *RwLock, waiter: *WriteWaiter) bool` | Waiter-based write lock. |
| `cancelWriteLock` | `fn cancelWriteLock(self: *RwLock, waiter: *WriteWaiter) void` | Cancel pending write lock. |
| `tryReadLockGuard` | `fn tryReadLockGuard(self: *RwLock) ?ReadGuard` | Non-blocking read lock with guard. |
| `tryWriteLockGuard` | `fn tryWriteLockGuard(self: *RwLock) ?WriteGuard` | Non-blocking write lock with guard. |
| `isWriteLocked` | `fn isWriteLocked(self: *RwLock) bool` | Debug: check if write-locked. |
| `getReaderCount` | `fn getReaderCount(self: *RwLock) usize` | Debug: number of active readers. |
| `waitingReaders` | `fn waitingReaders(self: *RwLock) usize` | Debug: queued reader count (O(n)). |
| `waitingWriters` | `fn waitingWriters(self: *RwLock) usize` | Debug: queued writer count (O(n)). |

### Convenience: `readLock(io)` / `writeLock(io)`

The simplest way to acquire a read or write lock in an async task. Suspends the current task until the lock is available.

```zig
fn readConfig(io: volt.Io, rwlock: *volt.sync.RwLock, config: *const AppConfig) AppConfig {
    rwlock.readLock(io);        // suspends if a writer is active
    defer rwlock.readUnlock();
    return config.*;
}

fn updateConfig(io: volt.Io, rwlock: *volt.sync.RwLock, config: *AppConfig, new: AppConfig) void {
    rwlock.writeLock(io);       // suspends until all readers and writers release
    defer rwlock.writeUnlock();
    config.* = new;
}
```

### Writer Priority

Writers have natural priority: a queued writer drains semaphore permits toward zero, causing `tryReadLock()` to fail for new readers. Waiters are served FIFO, so the writer runs before any reader queued behind it.

### Guards

```zig
if (rwlock.tryReadLockGuard()) |guard| {
    var g = guard;
    defer g.deinit();
    // read shared data
}

if (rwlock.tryWriteLockGuard()) |guard| {
    var g = guard;
    defer g.deinit();
    // modify shared data
}
```

### Example: Shared Configuration Store

A configuration store where many tasks read settings frequently but an admin task writes updates infrequently. RwLock allows all readers to proceed in parallel -- they only block when a writer is active.

```zig
const std = @import("std");
const volt = @import("volt");

const AppConfig = struct {
    max_connections: u32,
    request_timeout_ms: u64,
    feature_flags: u64,
    log_level: enum { debug, info, warn, err },
};

const ConfigStore = struct {
    rwlock: volt.sync.RwLock,
    config: AppConfig,

    fn init(defaults: AppConfig) ConfigStore {
        return .{
            .rwlock = volt.sync.RwLock.init(),
            .config = defaults,
        };
    }

    /// Read the current configuration. Multiple tasks can call this
    /// concurrently without blocking each other.
    fn read(self: *ConfigStore, io: volt.Io) AppConfig {
        self.rwlock.readLock(io);
        defer self.rwlock.readUnlock();
        return self.config;
    }

    /// Non-blocking read for contexts where you cannot suspend.
    fn tryRead(self: *ConfigStore) ?AppConfig {
        if (self.rwlock.tryReadLock()) {
            defer self.rwlock.readUnlock();
            return self.config;
        }
        return null; // A writer is active, caller can retry or yield
    }

    /// Check a single feature flag without copying the whole config.
    fn hasFeatureFlag(self: *ConfigStore, io: volt.Io, flag_bit: u6) bool {
        self.rwlock.readLock(io);
        defer self.rwlock.readUnlock();
        return (self.config.feature_flags & (@as(u64, 1) << flag_bit)) != 0;
    }

    /// Update the configuration. Blocks all readers until complete.
    /// Only one writer can hold the lock at a time.
    fn update(self: *ConfigStore, io: volt.Io, new_config: AppConfig) void {
        self.rwlock.writeLock(io);
        defer self.rwlock.writeUnlock();
        self.config = new_config;
    }

    /// Partially update: change only the timeout, leave everything else.
    fn setTimeout(self: *ConfigStore, io: volt.Io, timeout_ms: u64) void {
        self.rwlock.writeLock(io);
        defer self.rwlock.writeUnlock();
        self.config.request_timeout_ms = timeout_ms;
    }
};

var store = ConfigStore.init(.{
    .max_connections = 1000,
    .request_timeout_ms = 30_000,
    .feature_flags = 0,
    .log_level = .info,
});

// Task A, B, C, D (concurrent readers -- all proceed in parallel):
fn readerTask(io: volt.Io) void {
    const cfg = store.read(io);
    std.log.info("timeout = {d}ms, max_conn = {d}", .{
        cfg.request_timeout_ms,
        cfg.max_connections,
    });
}

// Admin task (exclusive writer -- blocks readers while active):
fn adminTask(io: volt.Io) void {
    store.update(io, .{
        .max_connections = 2000,
        .request_timeout_ms = 15_000,
        .feature_flags = 0b0000_0011, // Enable features 0 and 1
        .log_level = .warn,
    });
}
```

:::tip
Use RwLock when reads vastly outnumber writes. If reads and writes are equally frequent, a plain Mutex is simpler and avoids the overhead of tracking reader counts.
:::

---

## Semaphore

Counting semaphore. Limits concurrent access to a resource.

```zig
const Semaphore = volt.sync.Semaphore;
```

### Construction

```zig
var sem = Semaphore.init(10); // 10 permits
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `tryAcquire` | `fn tryAcquire(self: *Semaphore, num: usize) bool` | Non-blocking acquire (lock-free CAS). |
| `acquire` | `fn acquire(self: *Semaphore, io: volt.Io, num: usize) void` | Convenience: acquire permits, suspending until available. |
| `acquireFuture` | `fn acquireFuture(self: *Semaphore, num: usize) AcquireFuture` | Returns a Future that resolves when permits are acquired. |
| `acquireWait` | `fn acquireWait(self: *Semaphore, waiter: *Waiter) bool` | Waiter-based acquire. |
| `release` | `fn release(self: *Semaphore, num: usize) void` | Release permits. Wakes queued waiters. |
| `cancelAcquire` | `fn cancelAcquire(self: *Semaphore, waiter: *Waiter) void` | Cancel and return partial permits. |
| `tryAcquirePermit` | `fn tryAcquirePermit(self: *Semaphore, num: usize) ?SemaphorePermit` | Non-blocking acquire with RAII permit. |
| `availablePermits` | `fn availablePermits(self: *const Semaphore) usize` | Current available permits. |
| `waiterCount` | `fn waiterCount(self: *Semaphore) usize` | Debug: queued waiter count (O(n)). |

### Convenience: `acquire(io, n)`

The simplest way to acquire permits in an async task. Suspends the current task until the requested permits are available.

```zig
fn useConnection(io: volt.Io, pool_sem: *volt.sync.Semaphore) void {
    pool_sem.acquire(io, 1);     // suspends until a permit is available
    defer pool_sem.release(1);
    // use the connection
}
```

### Algorithm

- `tryAcquire`: Lock-free CAS loop. No mutex.
- `release`: Always takes the mutex, serves waiters directly (direct handoff). Only surplus permits go to the atomic counter.
- `acquireWait`: Lock-free fast path for full acquisition. If insufficient permits, locks mutex BEFORE CAS to close the race window.

Key invariant: permits never float in the atomic when waiters are queued.

### SemaphorePermit (RAII)

```zig
if (sem.tryAcquirePermit(1)) |permit| {
    var p = permit;
    defer p.deinit(); // Releases permits
    doWork();
}
```

Methods: `release()`, `forget()` (leak the permit), `deinit()`.

### Example: Connection Pool Limiter

A database connection pool that limits the number of concurrent connections. The semaphore acts as a gatekeeper: each task must acquire a permit before using a connection, and releases it when done.

```zig
const std = @import("std");
const volt = @import("volt");

const DbConnectionPool = struct {
    /// Controls how many tasks can hold a connection simultaneously.
    /// Each permit represents one available connection slot.
    semaphore: volt.sync.Semaphore,
    max_connections: usize,

    fn init(max_connections: usize) DbConnectionPool {
        return .{
            .semaphore = volt.sync.Semaphore.init(max_connections),
            .max_connections = max_connections,
        };
    }

    /// Acquire a connection slot. Suspends until one is available.
    fn getConnection(self: *DbConnectionPool, io: volt.Io) void {
        self.semaphore.acquire(io, 1);
    }

    /// Release the connection slot back to the pool.
    fn releaseConnection(self: *DbConnectionPool) void {
        self.semaphore.release(1);
    }

    /// Attempt to acquire a connection slot without waiting.
    fn tryGetConnection(self: *DbConnectionPool) ?volt.sync.semaphore.SemaphorePermit {
        return self.semaphore.tryAcquirePermit(1);
    }

    /// How many connections are currently available.
    fn availableConnections(self: *DbConnectionPool) usize {
        return self.semaphore.availablePermits();
    }

    /// How many tasks are waiting for a connection.
    fn waitingTasks(self: *DbConnectionPool) usize {
        return self.semaphore.waiterCount();
    }
};

var pool = DbConnectionPool.init(5); // Max 5 concurrent DB connections

fn handleRequest(io: volt.Io) !void {
    // Acquire a connection slot, suspending until available.
    pool.getConnection(io);
    defer pool.releaseConnection();

    // Use the connection for the duration of this scope.
    // Even if executeQuery returns an error, releaseConnection runs.
    try executeQuery();
}

fn executeQuery() !void {
    // ... actual database work ...
}
```

:::note
`SemaphorePermit` is the recommended way to use semaphores with `tryAcquire`. For the convenience `acquire(io, n)` path, use `defer sem.release(n)` to ensure permits are returned. Both approaches guarantee permits are returned even on error.
:::

### Example: Rate Limiter

Limiting concurrent outbound HTTP requests to avoid overwhelming a downstream service.

```zig
const volt = @import("volt");

/// Allow at most 20 outbound requests in flight at once.
var request_limiter = volt.sync.Semaphore.init(20);

fn fetchFromUpstream(io: volt.Io, url: []const u8) ![]const u8 {
    // Acquire 1 permit. If 20 requests are already in flight,
    // this suspends the task until one finishes.
    request_limiter.acquire(io, 1);
    defer request_limiter.release(1);
    return doHttpGet(url);
}

fn doBulkFetch(io: volt.Io, urls: []const []const u8) !void {
    // Acquire 5 permits at once for a batch operation.
    // This reserves 5 of the 20 slots, leaving 15 for other tasks.
    request_limiter.acquire(io, 5);
    defer request_limiter.release(5);
    for (urls) |url| {
        _ = try doHttpGet(url);
    }
}

fn doHttpGet(url: []const u8) ![]const u8 {
    _ = url;
    // ... actual HTTP request ...
    return "";
}
```



For Notify, Barrier, OnceCell, thread safety details, and the full "Choosing the Right Primitive" guide, see the [Coordination API](/api/coordination-api/).

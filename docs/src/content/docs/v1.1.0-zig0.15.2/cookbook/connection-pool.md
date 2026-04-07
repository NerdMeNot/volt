---
title: Connection Pool
description: A semaphore-gated connection pool with acquire/release, timeout on checkout, and pool sizing strategies.
---

Connection pools bound the number of simultaneous connections to an external resource (database, cache, upstream service). This recipe builds a production-style pool using `Semaphore` as the gating mechanism, `Deadline` for checkout timeouts, and per-connection health checking and lifetime tracking.

:::tip[What you'll learn]
- **Semaphore for resource gating** -- limit concurrent connections without manual counting
- **Deadline for checkout timeout** -- callers fail fast instead of hanging indefinitely
- **Health checking idle connections** -- ping before reuse, discard stale connections
- **Pool lifecycle** -- creation, pre-warming, checkout, return, eviction, and teardown
:::

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

/// Metadata tracked alongside each pooled connection.
fn PoolEntry(comptime Conn: type) type {
    return struct {
        conn: Conn,
        created_at: volt.time.Instant,
    };
}

/// A pool of reusable connections gated by a semaphore.
///
/// - Semaphore permits control how many connections can be checked out at once.
/// - Idle connections are health-checked before reuse.
/// - Connections older than `max_lifetime` are discarded on checkout.
pub fn ConnectionPool(comptime Conn: type) type {
    return struct {
        const Self = @This();
        const Entry = PoolEntry(Conn);

        /// Controls how many connections can be checked out simultaneously.
        /// Initialized with `max_size` permits; each checkout takes one,
        /// each return gives one back.
        semaphore: volt.sync.Semaphore,

        /// Idle connections available for reuse, stored as a stack (LIFO)
        /// so the most-recently-used connection is returned first.
        free_list: std.ArrayListUnmanaged(Entry),

        /// Protects the free list. Only held briefly during push/pop.
        free_lock: volt.sync.Mutex,

        /// Factory function that opens a new connection to the backend.
        connect_fn: *const fn () anyerror!Conn,

        /// Optional health check. Returns true if the connection is alive.
        /// Called on idle connections before returning them from the pool.
        health_check_fn: ?*const fn (Conn) bool,

        /// Maximum number of connections the pool will ever create.
        max_size: usize,

        /// Connections older than this are discarded on checkout.
        /// Set to Duration.MAX to disable lifetime eviction.
        max_lifetime: volt.time.Duration,

        /// Backing allocator for the free list.
        allocator: std.mem.Allocator,

        pub fn init(
            allocator: std.mem.Allocator,
            config: struct {
                max_size: usize,
                max_lifetime: volt.time.Duration = volt.time.Duration.fromMins(30),
                connect_fn: *const fn () anyerror!Conn,
                health_check_fn: ?*const fn (Conn) bool = null,
            },
        ) Self {
            return .{
                .semaphore = volt.sync.Semaphore.init(config.max_size),
                .free_list = .empty,
                .free_lock = volt.sync.Mutex.init(),
                .connect_fn = config.connect_fn,
                .health_check_fn = config.health_check_fn,
                .max_size = config.max_size,
                .max_lifetime = config.max_lifetime,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            // Close every idle connection still in the pool.
            for (self.free_list.items) |entry| {
                entry.conn.close();
            }
            self.free_list.deinit(self.allocator);
        }

        /// Pre-warm the pool by opening `count` connections eagerly.
        /// Call this at startup to avoid cold-start latency on the
        /// first burst of requests.
        pub fn warmUp(self: *Self, count: usize) !void {
            const to_create = @min(count, self.max_size);
            for (0..to_create) |_| {
                const conn = try self.connect_fn();
                const entry = Entry{
                    .conn = conn,
                    .created_at = volt.time.Instant.now(),
                };
                try self.free_list.append(self.allocator, entry);
            }
        }

        /// Acquire a connection from the pool.
        ///
        /// 1. Wait for a semaphore permit (bounded by `timeout_ms`).
        /// 2. Pop an idle connection from the free list.
        /// 3. Health-check it. If stale or dead, discard and try again.
        /// 4. If no idle connection is available, create a new one.
        ///
        /// Returns `null` if the timeout expires before a connection
        /// can be obtained.
        pub fn acquire(self: *Self, timeout_ms: u64) ?Entry {
            // ---- Step 1: get a semaphore permit ----
            var deadline = volt.time.Deadline.init(
                volt.time.Duration.fromMillis(timeout_ms),
            );

            if (!self.semaphore.tryAcquire(1)) {
                while (!deadline.isExpired()) {
                    if (self.semaphore.tryAcquire(1)) break;
                    std.Thread.sleep(1 * std.time.ns_per_ms);
                } else {
                    return null; // Timed out waiting for permit.
                }
            }

            // ---- Step 2 & 3: try to reuse an idle connection ----
            while (true) {
                const maybe_entry = blk: {
                    if (!self.free_lock.tryLock()) break :blk null;
                    defer self.free_lock.unlock();
                    break :blk self.free_list.popOrNull();
                };

                const entry = maybe_entry orelse break; // Free list empty.

                // Discard connections that have exceeded their max lifetime.
                if (entry.created_at.elapsed().cmp(self.max_lifetime) == .gt) {
                    entry.conn.close();
                    continue; // Try next idle connection.
                }

                // Run health check if configured.
                if (self.health_check_fn) |check| {
                    if (!check(entry.conn)) {
                        entry.conn.close();
                        continue; // Dead connection, try next.
                    }
                }

                return entry; // Good connection.
            }

            // ---- Step 4: create a fresh connection ----
            const conn = self.connect_fn() catch {
                self.semaphore.release(1);
                return null;
            };

            return Entry{
                .conn = conn,
                .created_at = volt.time.Instant.now(),
            };
        }

        /// Return a connection to the pool for reuse.
        ///
        /// The connection goes back onto the free list so the next
        /// `acquire` call can skip the cost of opening a new one.
        /// If the free list append fails (OOM), the connection is
        /// closed instead.
        pub fn release(self: *Self, entry: Entry) void {
            if (self.free_lock.tryLock()) {
                defer self.free_lock.unlock();
                self.free_list.append(self.allocator, entry) catch {
                    entry.conn.close();
                };
            } else {
                // Could not grab the lock without blocking.
                // Drop the connection rather than risk contention.
                entry.conn.close();
            }

            // Always release the semaphore permit so another caller
            // can proceed, whether we kept the connection or not.
            self.semaphore.release(1);
        }
    };
}
```

## Usage: Multiple Concurrent Clients

This example simulates 20 concurrent workers sharing a pool of 5 database connections. Each worker checks out a connection, runs a query, and returns the connection to the pool.

```zig
const std = @import("std");
const volt = @import("volt");

// ── Mock database connection ────────────────────────────────────────────

const DbConn = struct {
    id: u64,
    alive: bool,

    pub fn query(self: *DbConn, sql: []const u8) !void {
        _ = sql;
        if (!self.alive) return error.ConnectionClosed;
        // Simulate query latency.
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    pub fn ping(self: DbConn) bool {
        return self.alive;
    }

    pub fn close(self: DbConn) void {
        _ = self;
    }
};

var next_id = std.atomic.Value(u64).init(0);

fn createDbConn() anyerror!DbConn {
    return DbConn{
        .id = next_id.fetchAdd(1, .monotonic),
        .alive = true,
    };
}

fn checkDbConn(conn: DbConn) bool {
    return conn.ping();
}

// ── Application ─────────────────────────────────────────────────────────

const Pool = ConnectionPool(DbConn);

fn worker(pool: *Pool, worker_id: usize) void {
    for (0..10) |i| {
        if (pool.acquire(5000)) |entry| {
            var e = entry;
            e.conn.query("SELECT 1") catch {
                std.debug.print("[worker {}] query failed on conn {}\n", .{
                    worker_id, e.conn.id,
                });
                pool.release(e);
                continue;
            };
            std.debug.print("[worker {}] iteration {} used conn {}\n", .{
                worker_id, i, e.conn.id,
            });
            pool.release(e);
        } else {
            std.debug.print("[worker {}] timed out on iteration {}\n", .{
                worker_id, i,
            });
        }
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var pool = Pool.init(allocator, .{
        .max_size = 5,
        .max_lifetime = volt.time.Duration.fromMins(30),
        .connect_fn = createDbConn,
        .health_check_fn = checkDbConn,
    });
    defer pool.deinit();

    // Pre-warm 3 connections so the first few requests are instant.
    try pool.warmUp(3);

    // Launch 20 concurrent workers sharing 5 connections.
    var threads: [20]std.Thread = undefined;
    for (&threads, 0..) |*t, id| {
        t.* = try std.Thread.spawn(.{}, worker, .{ &pool, id });
    }
    for (&threads) |*t| {
        t.join();
    }

    std.debug.print("All workers finished.\n", .{});
}
```

Because the pool is capped at 5 and there are 20 workers, most checkout calls will briefly wait on the semaphore. Connections are reused across workers, and health checks prevent a stale connection from causing a query failure.

## Walkthrough

### Semaphore as the gate

The semaphore is initialized with `max_size` permits. Each `acquire` takes one permit and each `release` returns one. This guarantees that at most `max_size` connections are checked out at any time -- regardless of how many tasks are trying to acquire. The semaphore is the only thing you need to enforce the limit; no manual counter bookkeeping required.

### Timeout on checkout

`Deadline` captures the instant when the caller should give up. The polling loop retries `tryAcquire(1)` until either a permit is granted or the deadline expires. Returning `null` lets the caller decide what to do (retry, return an error to the user, fall back to a different backend).

In a fully async context you would use the synchronous convenience method on the semaphore with a deadline check:

```zig
// Inside an async task that receives an `io: *volt.Io` handle:
var deadline = volt.time.Deadline.init(volt.Duration.fromSecs(5));
if (!deadline.isExpired()) {
    pool.semaphore.acquire(io, 1);
}
```

### Health checking idle connections

Connections can die while sitting idle -- the database restarts, a firewall drops the socket, a TCP keepalive expires. Before handing an idle connection to the caller, the pool calls the user-supplied `health_check_fn` (typically a lightweight `SELECT 1` or protocol-level ping). If the check fails the connection is closed, and the pool tries the next idle entry. This loop continues until a healthy connection is found or the free list is empty.

### Max-lifetime eviction

Even connections that pass a health check can accumulate server-side state (temp tables, session variables, leaked prepared statements). The `created_at` timestamp on each entry lets the pool discard connections that have been alive longer than `max_lifetime`. This is checked *before* the health check to avoid wasting a round-trip on a connection that will be evicted anyway.

### Connection reuse (LIFO)

The free list is a stack. `popOrNull` returns the most-recently-returned connection, which is the most likely to still be alive and to have a warm TCP window. This is the same strategy used by most production connection pools (HikariCP, pgbouncer).

### Allocator pattern

The free list uses `std.ArrayListUnmanaged(Entry)`, which does not store the allocator internally. Instead the allocator is passed explicitly to `append` and `deinit`. This is the idiomatic Zig 0.15.x pattern for containers whose allocator is managed by an owning struct.

## Pool Sizing Strategies

| Strategy | When to use |
|----------|-------------|
| **Fixed** (`max_size = N`) | Predictable load, known database connection limits |
| **CPU-proportional** (`max_size = num_cpus * 2`) | General-purpose default |
| **Small** (`max_size = 2-5`) | Latency-sensitive; fewer connections = less lock contention on the database |
| **Large** (`max_size = 50-100`) | High-throughput batch processing with cheap connections |

A common rule of thumb from the PostgreSQL community: `connections = (core_count * 2) + disk_spindles`. For SSDs, `core_count * 2` is usually sufficient.

## Try it yourself

Here are three extensions to deepen your understanding of the pool's internals.

### 1. Min-idle connection pre-warming

The `warmUp` function above opens connections eagerly. Extend it so the pool *maintains* a minimum idle count over time -- when a connection is checked out and the idle count drops below `min_idle`, a background task opens a replacement:

```zig
/// Call this periodically (e.g., from an Interval timer) to maintain
/// at least `min_idle` connections in the free list.
pub fn maintainMinIdle(self: *Self, min_idle: usize) void {
    const current_idle = blk: {
        if (!self.free_lock.tryLock()) return;
        defer self.free_lock.unlock();
        break :blk self.free_list.items.len;
    };

    if (current_idle >= min_idle) return;

    const deficit = min_idle - current_idle;
    for (0..deficit) |_| {
        const conn = self.connect_fn() catch continue;
        const entry = Entry{
            .conn = conn,
            .created_at = volt.time.Instant.now(),
        };
        if (self.free_lock.tryLock()) {
            defer self.free_lock.unlock();
            self.free_list.append(self.allocator, entry) catch {
                entry.conn.close();
            };
        } else {
            entry.conn.close();
        }
    }
}
```

### 2. Metrics tracking

Add counters so you can observe pool behavior in production. These atomics are safe to read from any thread without locking:

```zig
/// Add these fields to ConnectionPool:
total_checkouts: std.atomic.Value(u64),
total_timeouts: std.atomic.Value(u64),
current_checked_out: std.atomic.Value(u32),

/// Initialize them in init():
.total_checkouts = std.atomic.Value(u64).init(0),
.total_timeouts = std.atomic.Value(u64).init(0),
.current_checked_out = std.atomic.Value(u32).init(0),

/// Then instrument acquire() and release():

// In acquire(), on success (before returning the entry):
_ = self.total_checkouts.fetchAdd(1, .monotonic);
_ = self.current_checked_out.fetchAdd(1, .monotonic);

// In acquire(), on timeout (before returning null):
_ = self.total_timeouts.fetchAdd(1, .monotonic);

// In release():
_ = self.current_checked_out.fetchSub(1, .monotonic);
```

You can expose these via a `/metrics` endpoint or log them on an interval:

```zig
fn logPoolMetrics(pool: *Pool) void {
    std.debug.print(
        "pool: checkouts={} timeouts={} checked_out={}\n",
        .{
            pool.total_checkouts.load(.monotonic),
            pool.total_timeouts.load(.monotonic),
            pool.current_checked_out.load(.monotonic),
        },
    );
}
```

### 3. Max-lifetime eviction on return

The pool already evicts expired connections on checkout. Add eviction on *return* as well, so stale connections are cleaned up eagerly instead of sitting in the free list until the next checkout:

```zig
/// Modified release() that discards expired connections immediately.
pub fn release(self: *Self, entry: Entry) void {
    const expired = entry.created_at.elapsed().cmp(self.max_lifetime) == .gt;

    if (!expired) {
        if (self.free_lock.tryLock()) {
            defer self.free_lock.unlock();
            self.free_list.append(self.allocator, entry) catch {
                entry.conn.close();
            };
        } else {
            entry.conn.close();
        }
    } else {
        // Connection exceeded max lifetime. Close it instead of
        // returning it to the pool.
        entry.conn.close();
    }

    self.semaphore.release(1);
}
```

Combine all three extensions and you have a pool that maintains a healthy baseline of connections, reports on its own behavior, and aggressively discards stale resources -- the same feature set you would find in HikariCP or Deadpool.

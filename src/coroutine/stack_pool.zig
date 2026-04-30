//! Stack pool — recycle mmap'd coroutine stacks across spawn/free cycles.
//!
//! Without this, every `volt.spawn` calls `mmap + mprotect` (a few µs
//! of syscall overhead). With the pool, completed coroutines push their
//! stack region back; new spawns pop from the head. The mmap call only
//! happens when the pool is empty (or full at release).
//!
//! ## Lifecycle
//!
//! - `Done.subscribe` calls `release(stack, committed_bottom)` after the
//!   coroutine's trampoline has returned. The coroutine struct stays
//!   alive (the user's Job/Task still references it for state queries),
//!   but its stack memory is recycled.
//! - `Runtime.createCoroutine` calls `acquire()` first; falls back to
//!   `stack.allocGrowable` if the pool is empty.
//! - `Runtime.deinit` calls `pool.deinit()` which munmaps every cached
//!   region.
//!
//! ## Memory hygiene
//!
//! A coroutine that grew its stack to N pages (e.g. for deep recursion)
//! returns those pages committed. The next reuse inherits them. Bounded
//! by `cap`: the pool holds at most `cap` stacks; releases past that
//! munmap immediately. So worst-case memory = `cap * largest_grown`.
//! For a typical workload this is fine; a v1.x add-on can `mprotect`
//! the grown pages back to PROT_NONE on release for stricter isolation.

const std = @import("std");
const builtin = @import("builtin");
const stack_mod = @import("stack.zig");
const thread = @import("../internal/thread.zig");

/// Maximum stacks held in the pool. Beyond this, `release` munmaps
/// instead of caching. 256 × 8 MiB = 2 GiB virtual cap; physical cost
/// only on actually-grown pages.
const DEFAULT_CAP: usize = 256;

/// A recyclable stack region. Same shape as `stack.GrowableStack`
/// but stripped to what the pool actually needs to round-trip.
pub const PooledStack = struct {
    base: usize,
    top: usize,
    reserved_size: usize,
    committed_bottom: usize,
    floor: usize,

    pub fn fromGrowable(gs: stack_mod.GrowableStack) PooledStack {
        return .{
            .base = gs.base,
            .top = gs.top,
            .reserved_size = gs.reserved_size,
            .committed_bottom = gs.committed_bottom,
            .floor = gs.floor,
        };
    }

    pub fn toGrowable(self: PooledStack) stack_mod.GrowableStack {
        return .{
            .base = self.base,
            .top = self.top,
            .reserved_size = self.reserved_size,
            .committed_bottom = self.committed_bottom,
            .floor = self.floor,
        };
    }
};

pub const StackPool = struct {
    allocator: std.mem.Allocator,
    mutex: thread.Mutex = .{},
    free_list: std.array_list.Managed(PooledStack),
    cap: usize = DEFAULT_CAP,
    /// Counter of stacks served from the pool (vs. fresh mmap).
    /// Lock-free atomic so observability can sample without lock.
    hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Counter of fresh mmaps (pool was empty).
    misses: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(allocator: std.mem.Allocator) StackPool {
        return .{
            .allocator = allocator,
            .free_list = std.array_list.Managed(PooledStack).init(allocator),
        };
    }

    pub fn deinit(self: *StackPool) void {
        for (self.free_list.items) |ps| {
            stack_mod.freeGrowable(ps.toGrowable());
        }
        self.free_list.deinit();
    }

    /// Try to pop a stack of compatible size from the pool. Returns
    /// null on empty (caller should `allocGrowable` fresh).
    /// `reserved_size` is matched exactly — different stack sizes
    /// don't get interchanged (we don't have one in v1.0 but the
    /// signature is future-proof).
    pub fn acquire(self: *StackPool, reserved_size: usize) ?stack_mod.GrowableStack {
        self.mutex.lock();
        defer self.mutex.unlock();
        var i: usize = self.free_list.items.len;
        while (i > 0) {
            i -= 1;
            const ps = self.free_list.items[i];
            if (ps.reserved_size == reserved_size) {
                _ = self.free_list.swapRemove(i);
                _ = self.hits.fetchAdd(1, .monotonic);
                return ps.toGrowable();
            }
        }
        _ = self.misses.fetchAdd(1, .monotonic);
        return null;
    }

    /// Return a stack to the pool. If the pool is at capacity,
    /// munmaps immediately instead of caching.
    pub fn release(self: *StackPool, gs: stack_mod.GrowableStack) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.free_list.items.len >= self.cap) {
            stack_mod.freeGrowable(gs);
            return;
        }
        self.free_list.append(PooledStack.fromGrowable(gs)) catch {
            // OOM growing the free list — just munmap.
            stack_mod.freeGrowable(gs);
        };
    }

    /// Snapshot of pool stats. Cheap; no lock.
    pub fn stats(self: *const StackPool) Stats {
        return .{
            .hits = self.hits.load(.acquire),
            .misses = self.misses.load(.acquire),
            .free_count = blk: {
                // Reading `items.len` without the lock — racy but for
                // diagnostic purposes only.
                break :blk self.free_list.items.len;
            },
        };
    }

    pub const Stats = struct { hits: u64, misses: u64, free_count: usize };
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "stack_pool: acquire empty returns null" {
    var pool = StackPool.init(std.testing.allocator);
    defer pool.deinit();
    try std.testing.expect(pool.acquire(stack_mod.default_reserved) == null);
    try std.testing.expectEqual(@as(u64, 1), pool.stats().misses);
}

test "stack_pool: release then acquire round-trips" {
    var pool = StackPool.init(std.testing.allocator);
    defer pool.deinit();

    const gs = try stack_mod.allocGrowable(stack_mod.defaultInitialCommit(), stack_mod.default_reserved);
    pool.release(gs);
    try std.testing.expectEqual(@as(usize, 1), pool.stats().free_count);

    const reused = pool.acquire(stack_mod.default_reserved) orelse return error.AcquireFailed;
    try std.testing.expectEqual(gs.base, reused.base);
    try std.testing.expectEqual(@as(u64, 1), pool.stats().hits);

    // Now drain so deinit doesn't double-free.
    stack_mod.freeGrowable(reused);
}

test "stack_pool: cap honored — overflow munmaps" {
    var pool = StackPool.init(std.testing.allocator);
    pool.cap = 2;
    defer pool.deinit();

    const gs1 = try stack_mod.allocGrowable(stack_mod.defaultInitialCommit(), stack_mod.default_reserved);
    const gs2 = try stack_mod.allocGrowable(stack_mod.defaultInitialCommit(), stack_mod.default_reserved);
    const gs3 = try stack_mod.allocGrowable(stack_mod.defaultInitialCommit(), stack_mod.default_reserved);

    pool.release(gs1);
    pool.release(gs2);
    pool.release(gs3); // would munmap immediately
    try std.testing.expectEqual(@as(usize, 2), pool.stats().free_count);
}

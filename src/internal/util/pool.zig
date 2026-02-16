//! Object Pool - Lock-free Slab Allocator for Fixed-Size Objects
//!
//! ZIG ADVANTAGE: Explicit allocator control allows specialized pooling
//! that Rust's global allocator model doesn't easily support.
//!
//! This pool provides:
//! - O(1) allocation and deallocation
//! - Zero fragmentation (fixed-size slots)
//! - Cache-friendly allocation (reuse recently freed slots)
//! - Lock-free fast path via atomic Treiber stack
//!
//! Usage:
//!   var pool = try ObjectPool(MyStruct).init(allocator, 1024);
//!   defer pool.deinit();
//!
//!   const obj = pool.create() orelse return error.PoolExhausted;
//!   defer pool.destroy(obj);

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Lock-free object pool for fixed-size allocations
pub fn ObjectPool(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Freelist node - overlays the object memory when free
        const FreeNode = struct {
            next: ?*FreeNode,
        };

        comptime {
            // Ensure T is large enough to hold a freelist pointer
            if (@sizeOf(T) < @sizeOf(FreeNode)) {
                @compileError("ObjectPool type must be at least pointer-sized");
            }
        }

        /// Backing allocator for slab allocation
        allocator: Allocator,

        /// Slab of pre-allocated objects
        slab: []align(@alignOf(T)) u8,

        /// Atomic freelist head for lock-free alloc/free (Treiber stack)
        freelist: std.atomic.Value(?*FreeNode),

        /// Number of objects in the pool
        capacity: usize,

        /// Statistics
        allocations: std.atomic.Value(u64),
        deallocations: std.atomic.Value(u64),

        /// Initialize a new object pool with the given capacity
        pub fn init(allocator: Allocator, capacity: usize) !Self {
            const slab_size = capacity * @sizeOf(T);
            const alignment: std.mem.Alignment = @enumFromInt(@ctz(@as(usize, @alignOf(T))));
            const slab = try allocator.alignedAlloc(u8, alignment, slab_size);

            var self = Self{
                .allocator = allocator,
                .slab = slab,
                .freelist = std.atomic.Value(?*FreeNode).init(null),
                .capacity = capacity,
                .allocations = std.atomic.Value(u64).init(0),
                .deallocations = std.atomic.Value(u64).init(0),
            };

            // Initialize freelist with all slots (in reverse for cache locality)
            var i: usize = capacity;
            while (i > 0) {
                i -= 1;
                const ptr = self.slotPtr(i);
                const node: *FreeNode = @ptrCast(@alignCast(ptr));
                node.next = self.freelist.load(.monotonic);
                self.freelist.store(node, .monotonic);
            }

            return self;
        }

        /// Clean up the pool
        pub fn deinit(self: *Self) void {
            self.allocator.free(self.slab);
        }

        /// Get pointer to slot at index
        inline fn slotPtr(self: *Self, index: usize) *T {
            const offset = index * @sizeOf(T);
            const ptr: *T = @ptrCast(@alignCast(self.slab.ptr + offset));
            return ptr;
        }

        /// Allocate an object from the pool (lock-free)
        /// Returns null if pool is exhausted
        pub fn create(self: *Self) ?*T {
            // Pop from freelist (lock-free Treiber stack pop)
            var head = self.freelist.load(.acquire);

            while (head) |node| {
                const next = node.next;
                const result = self.freelist.cmpxchgWeak(
                    head,
                    next,
                    .acq_rel,
                    .acquire,
                );

                if (result) |new_head| {
                    head = new_head;
                    continue;
                }

                // Successfully popped
                _ = self.allocations.fetchAdd(1, .monotonic);
                return @ptrCast(@alignCast(node));
            }

            // Pool exhausted
            return null;
        }

        /// Return an object to the pool (lock-free)
        pub fn destroy(self: *Self, obj: *T) void {
            const node: *FreeNode = @ptrCast(@alignCast(obj));

            // Push to freelist (lock-free Treiber stack push)
            var head = self.freelist.load(.acquire);
            while (true) {
                node.next = head;
                const result = self.freelist.cmpxchgWeak(
                    head,
                    node,
                    .release,
                    .acquire,
                );

                if (result) |new_head| {
                    head = new_head;
                    continue;
                }

                break;
            }

            _ = self.deallocations.fetchAdd(1, .monotonic);
        }

        /// Get pool statistics
        pub fn getStats(self: *const Self) Stats {
            return .{
                .capacity = self.capacity,
                .allocations = self.allocations.load(.monotonic),
                .deallocations = self.deallocations.load(.monotonic),
            };
        }

        /// Check if pool is exhausted
        pub fn isEmpty(self: *const Self) bool {
            return self.freelist.load(.acquire) == null;
        }

        /// Get approximate number of free slots
        pub fn freeCount(self: *const Self) u64 {
            const stats = self.getStats();
            return self.capacity - stats.inUse();
        }

        pub const Stats = struct {
            capacity: usize,
            allocations: u64,
            deallocations: u64,

            pub fn inUse(self: Stats) u64 {
                return self.allocations - self.deallocations;
            }
        };
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "ObjectPool - basic alloc and free" {
    const TestStruct = struct {
        value: u64,
        data: [56]u8, // Pad to 64 bytes (cache line)
    };

    var pool = try ObjectPool(TestStruct).init(std.testing.allocator, 16);
    defer pool.deinit();

    // Allocate
    const obj1 = pool.create().?;
    const obj2 = pool.create().?;

    obj1.value = 42;
    obj2.value = 123;

    try std.testing.expectEqual(@as(u64, 42), obj1.value);
    try std.testing.expectEqual(@as(u64, 123), obj2.value);

    // Free
    pool.destroy(obj1);
    pool.destroy(obj2);

    const stats = pool.getStats();
    try std.testing.expectEqual(@as(u64, 2), stats.allocations);
    try std.testing.expectEqual(@as(u64, 2), stats.deallocations);
    try std.testing.expectEqual(@as(u64, 0), stats.inUse());
}

test "ObjectPool - exhaustion" {
    const Small = struct { x: usize };

    var pool = try ObjectPool(Small).init(std.testing.allocator, 2);
    defer pool.deinit();

    const a = pool.create().?;
    const b = pool.create().?;
    const c = pool.create(); // Should fail

    try std.testing.expect(c == null);
    try std.testing.expect(pool.isEmpty());

    pool.destroy(a);
    pool.destroy(b);
    try std.testing.expect(!pool.isEmpty());
}

test "ObjectPool - reuse" {
    const Item = struct { id: u32, padding: [60]u8 };

    var pool = try ObjectPool(Item).init(std.testing.allocator, 4);
    defer pool.deinit();

    // Allocate all
    const items: [4]*Item = .{
        pool.create().?,
        pool.create().?,
        pool.create().?,
        pool.create().?,
    };

    // Free some
    pool.destroy(items[1]);
    pool.destroy(items[3]);

    // Reallocate - should reuse freed slots (LIFO order)
    const new1 = pool.create().?;
    const new2 = pool.create().?;

    // Should be same addresses (LIFO reuse)
    try std.testing.expect(new1 == items[3] or new1 == items[1]);
    try std.testing.expect(new2 == items[3] or new2 == items[1]);

    pool.destroy(items[0]);
    pool.destroy(items[2]);
    pool.destroy(new1);
    pool.destroy(new2);
}

test "ObjectPool - statistics" {
    const Data = struct { value: usize };

    var pool = try ObjectPool(Data).init(std.testing.allocator, 10);
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 10), pool.capacity);

    var objs: [5]*Data = undefined;
    for (&objs) |*p| {
        p.* = pool.create().?;
    }

    var stats = pool.getStats();
    try std.testing.expectEqual(@as(u64, 5), stats.allocations);
    try std.testing.expectEqual(@as(u64, 0), stats.deallocations);
    try std.testing.expectEqual(@as(u64, 5), stats.inUse());

    for (objs) |obj| {
        pool.destroy(obj);
    }

    stats = pool.getStats();
    try std.testing.expectEqual(@as(u64, 5), stats.allocations);
    try std.testing.expectEqual(@as(u64, 5), stats.deallocations);
    try std.testing.expectEqual(@as(u64, 0), stats.inUse());
}

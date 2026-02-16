//! Slab Allocator
//!
//! A pre-allocated, fixed-capacity data structure that provides:
//! - O(1) insertion (returns stable index/key)
//! - O(1) removal by key
//! - O(1) access by key
//! - Stable indices (keys remain valid until explicitly removed)
//!
//! Used for tracking pending I/O operations where we need to:
//! 1. Store operation state
//! 2. Look it up quickly when completion arrives
//! 3. Remove it after completion
//!

const std = @import("std");

/// A pre-allocated slab for storing values of type T.
///
/// Provides O(1) insert, remove, and access operations.
/// Keys (indices) remain stable until the entry is removed.
pub fn Slab(comptime T: type) type {
    return struct {
        entries: []Entry,
        /// Index of the first free slot (head of free list)
        next_free: usize,
        /// Number of occupied slots
        len: usize,
        allocator: std.mem.Allocator,

        const Self = @This();

        const Entry = union(enum) {
            /// Occupied slot with value
            occupied: T,
            /// Free slot pointing to next free slot (or sentinel)
            vacant: usize,
        };

        /// Sentinel value indicating end of free list
        const FREE_LIST_END: usize = std.math.maxInt(usize);

        /// Initialize a slab with the given capacity.
        pub fn init(allocator: std.mem.Allocator, cap: usize) !Self {
            const entries = try allocator.alloc(Entry, cap);

            // Initialize free list: each slot points to the next
            for (entries, 0..) |*entry, i| {
                entry.* = .{ .vacant = if (i + 1 < cap) i + 1 else FREE_LIST_END };
            }

            return .{
                .entries = entries,
                .next_free = if (cap > 0) 0 else FREE_LIST_END,
                .len = 0,
                .allocator = allocator,
            };
        }

        /// Free all memory.
        pub fn deinit(self: *Self) void {
            self.allocator.free(self.entries);
            self.* = undefined;
        }

        /// Insert a value, returning its key.
        /// Returns error.SlabFull if no capacity remaining.
        pub fn insert(self: *Self, value: T) !usize {
            if (self.next_free == FREE_LIST_END) {
                return error.SlabFull;
            }

            const key = self.next_free;
            const entry = &self.entries[key];

            // Save next free before overwriting
            self.next_free = entry.vacant;

            // Store the value
            entry.* = .{ .occupied = value };
            self.len += 1;

            return key;
        }

        /// Remove a value by key, returning it.
        /// Returns null if the key is invalid or already removed.
        pub fn remove(self: *Self, key: usize) ?T {
            if (key >= self.entries.len) return null;

            const entry = &self.entries[key];
            switch (entry.*) {
                .occupied => |value| {
                    // Add to free list
                    entry.* = .{ .vacant = self.next_free };
                    self.next_free = key;
                    self.len -= 1;
                    return value;
                },
                .vacant => return null,
            }
        }

        /// Get a pointer to the value at key.
        /// Returns null if the key is invalid or vacant.
        pub fn get(self: *Self, key: usize) ?*T {
            if (key >= self.entries.len) return null;

            return switch (self.entries[key]) {
                .occupied => |*value| value,
                .vacant => null,
            };
        }

        /// Get a const pointer to the value at key.
        pub fn getConst(self: *const Self, key: usize) ?*const T {
            if (key >= self.entries.len) return null;

            return switch (self.entries[key]) {
                .occupied => |*value| value,
                .vacant => null,
            };
        }

        /// Check if a key is occupied.
        pub fn contains(self: *const Self, key: usize) bool {
            if (key >= self.entries.len) return false;
            return self.entries[key] == .occupied;
        }

        /// Get the number of occupied slots.
        pub fn count(self: *const Self) usize {
            return self.len;
        }

        /// Get the total capacity.
        pub fn capacity(self: *const Self) usize {
            return self.entries.len;
        }

        /// Check if the slab is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.len == 0;
        }

        /// Check if the slab is full.
        pub fn isFull(self: *const Self) bool {
            return self.next_free == FREE_LIST_END;
        }

        /// Iterator over occupied entries.
        pub fn iterator(self: *Self) Iterator {
            return .{ .slab = self, .index = 0 };
        }

        pub const Iterator = struct {
            slab: *Self,
            index: usize,

            pub fn next(it: *Iterator) ?struct { key: usize, value: *T } {
                while (it.index < it.slab.entries.len) {
                    const key = it.index;
                    it.index += 1;
                    switch (it.slab.entries[key]) {
                        .occupied => |*value| return .{ .key = key, .value = value },
                        .vacant => continue,
                    }
                }
                return null;
            }
        };
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Slab - basic insert and get" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    const key = try slab.insert(42);
    try std.testing.expectEqual(@as(usize, 0), key);
    try std.testing.expectEqual(@as(u32, 42), slab.get(key).?.*);
    try std.testing.expectEqual(@as(usize, 1), slab.count());
}

test "Slab - insert multiple" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    const k0 = try slab.insert(10);
    const k1 = try slab.insert(20);
    const k2 = try slab.insert(30);

    try std.testing.expectEqual(@as(u32, 10), slab.get(k0).?.*);
    try std.testing.expectEqual(@as(u32, 20), slab.get(k1).?.*);
    try std.testing.expectEqual(@as(u32, 30), slab.get(k2).?.*);
    try std.testing.expectEqual(@as(usize, 3), slab.count());
}

test "Slab - remove and reuse" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    const k0 = try slab.insert(10);
    const k1 = try slab.insert(20);

    // Remove first entry
    const removed = slab.remove(k0);
    try std.testing.expectEqual(@as(?u32, 10), removed);
    try std.testing.expect(slab.get(k0) == null);
    try std.testing.expectEqual(@as(usize, 1), slab.count());

    // k1 should still be valid
    try std.testing.expectEqual(@as(u32, 20), slab.get(k1).?.*);

    // New insert should reuse k0
    const k2 = try slab.insert(30);
    try std.testing.expectEqual(k0, k2);
    try std.testing.expectEqual(@as(u32, 30), slab.get(k2).?.*);
}

test "Slab - full returns error" {
    var slab = try Slab(u32).init(std.testing.allocator, 2);
    defer slab.deinit();

    _ = try slab.insert(10);
    _ = try slab.insert(20);

    try std.testing.expect(slab.isFull());
    try std.testing.expectError(error.SlabFull, slab.insert(30));
}

test "Slab - iterator" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    _ = try slab.insert(10);
    _ = try slab.insert(20);
    _ = try slab.insert(30);
    _ = slab.remove(1); // Remove middle

    var sum: u32 = 0;
    var it = slab.iterator();
    while (it.next()) |entry| {
        sum += entry.value.*;
    }
    try std.testing.expectEqual(@as(u32, 40), sum); // 10 + 30
}

test "Slab - contains" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    const k0 = try slab.insert(10);
    try std.testing.expect(slab.contains(k0));
    try std.testing.expect(!slab.contains(1));
    try std.testing.expect(!slab.contains(100)); // Out of bounds

    _ = slab.remove(k0);
    try std.testing.expect(!slab.contains(k0));
}

test "Slab - remove returns null for invalid key" {
    var slab = try Slab(u32).init(std.testing.allocator, 4);
    defer slab.deinit();

    try std.testing.expect(slab.remove(0) == null);
    try std.testing.expect(slab.remove(100) == null);
}

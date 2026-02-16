//! Intrusive Doubly-Linked List
//!
//! An intrusive linked list where nodes embed their own link pointers.
//! This enables zero-allocation list operations - nodes are allocated
//! elsewhere and simply linked/unlinked from lists.
//!
//! Key benefits over std.TailQueue or external node lists:
//! - No allocation per list operation
//! - Node can be in multiple lists (with multiple Pointers fields)
//! - O(1) removal when you have the node pointer
//!
//! Safety requirements:
//! - Nodes must be pinned in memory while in a list
//! - Caller must ensure nodes are removed before being freed
//! - List does NOT free nodes on deinit
//!

const std = @import("std");
const builtin = @import("builtin");

/// Pointers for linking a node into a list.
/// Embed this in your node struct.
pub fn Pointers(comptime T: type) type {
    return struct {
        prev: ?*T = null,
        next: ?*T = null,

        const Self = @This();

        pub fn init() Self {
            return .{};
        }

        pub fn reset(self: *Self) void {
            self.prev = null;
            self.next = null;
        }
    };
}

/// An intrusive doubly-linked list.
///
/// T is the node type, which must have a field of type `Pointers(T)`.
/// The `pointers_field` parameter specifies which field contains the pointers.
pub fn LinkedList(comptime T: type, comptime pointers_field: []const u8) type {
    return struct {
        head: ?*T = null,
        tail: ?*T = null,
        len: usize = 0,

        const Self = @This();
        const PointersType = Pointers(T);

        /// Get the pointers for a node.
        inline fn getPointers(node: *T) *PointersType {
            return &@field(node, pointers_field);
        }

        /// Get const pointers for a node.
        inline fn getPointersConst(node: *const T) *const PointersType {
            return &@field(node, pointers_field);
        }

        /// Push a node to the back of the list.
        pub fn pushBack(self: *Self, node: *T) void {
            const ptrs = getPointers(node);
            std.debug.assert(ptrs.prev == null and ptrs.next == null);

            ptrs.prev = self.tail;
            ptrs.next = null;

            if (self.tail) |tail| {
                getPointers(tail).next = node;
            } else {
                self.head = node;
            }
            self.tail = node;
            self.len += 1;
        }

        /// Push a node to the front of the list.
        pub fn pushFront(self: *Self, node: *T) void {
            const ptrs = getPointers(node);
            std.debug.assert(ptrs.prev == null and ptrs.next == null);

            ptrs.prev = null;
            ptrs.next = self.head;

            if (self.head) |head| {
                getPointers(head).prev = node;
            } else {
                self.tail = node;
            }
            self.head = node;
            self.len += 1;
        }

        /// Pop a node from the front of the list.
        pub fn popFront(self: *Self) ?*T {
            const head = self.head orelse return null;
            self.remove(head);
            return head;
        }

        /// Pop a node from the back of the list.
        pub fn popBack(self: *Self) ?*T {
            const tail = self.tail orelse return null;
            self.remove(tail);
            return tail;
        }

        /// Remove a specific node from the list.
        /// The node must be in this list.
        pub fn remove(self: *Self, node: *T) void {
            const ptrs = getPointers(node);

            // Debug mode: detect double-remove
            if (builtin.mode == .Debug) {
                // If both pointers are null, node must be the sole head/tail,
                // otherwise it's already been removed (double-remove bug)
                if (ptrs.prev == null and ptrs.next == null) {
                    if (self.head != node or self.tail != node) {
                        @panic("double-remove: node is not in list (both pointers null but not head/tail)");
                    }
                }
                if (self.len == 0) {
                    @panic("double-remove: list is empty but remove was called");
                }
            }

            if (ptrs.prev) |prev| {
                getPointers(prev).next = ptrs.next;
            } else {
                self.head = ptrs.next;
            }

            if (ptrs.next) |next| {
                getPointers(next).prev = ptrs.prev;
            } else {
                self.tail = ptrs.prev;
            }

            ptrs.reset();
            self.len -= 1;
        }

        /// Check if a node appears to be linked (has prev or next set).
        /// Note: This doesn't verify the node is in THIS list.
        pub fn isLinked(node: *const T) bool {
            const ptrs = getPointersConst(node);
            return ptrs.prev != null or ptrs.next != null;
        }

        /// Get the first node without removing it.
        pub fn front(self: *const Self) ?*T {
            return self.head;
        }

        /// Get the last node without removing it.
        pub fn back(self: *const Self) ?*T {
            return self.tail;
        }

        /// Check if the list is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }

        /// Get the number of nodes in the list.
        pub fn count(self: *const Self) usize {
            return self.len;
        }

        /// Forward iterator.
        pub fn iterator(self: *const Self) Iterator {
            return .{ .current = self.head };
        }

        pub const Iterator = struct {
            current: ?*T,

            pub fn next(it: *Iterator) ?*T {
                const node = it.current orelse return null;
                it.current = getPointers(node).next;
                return node;
            }
        };

        /// Drain iterator that removes nodes as it iterates.
        pub fn drain(self: *Self) DrainIterator {
            return .{ .list = self };
        }

        pub const DrainIterator = struct {
            list: *Self,

            pub fn next(it: *DrainIterator) ?*T {
                return it.list.popFront();
            }
        };
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const TestNode = struct {
    value: u32,
    pointers: Pointers(TestNode) = .{},
};

const TestList = LinkedList(TestNode, "pointers");

test "LinkedList - push and pop back" {
    var list: TestList = .{};

    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    try std.testing.expectEqual(@as(usize, 3), list.count());

    try std.testing.expectEqual(@as(u32, 3), list.popBack().?.value);
    try std.testing.expectEqual(@as(u32, 2), list.popBack().?.value);
    try std.testing.expectEqual(@as(u32, 1), list.popBack().?.value);
    try std.testing.expect(list.popBack() == null);
    try std.testing.expect(list.isEmpty());
}

test "LinkedList - push and pop front" {
    var list: TestList = .{};

    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushFront(&n1);
    list.pushFront(&n2);
    list.pushFront(&n3);

    try std.testing.expectEqual(@as(u32, 3), list.popFront().?.value);
    try std.testing.expectEqual(@as(u32, 2), list.popFront().?.value);
    try std.testing.expectEqual(@as(u32, 1), list.popFront().?.value);
}

test "LinkedList - remove middle" {
    var list: TestList = .{};

    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    list.remove(&n2);

    try std.testing.expectEqual(@as(usize, 2), list.count());
    try std.testing.expectEqual(@as(u32, 1), list.popFront().?.value);
    try std.testing.expectEqual(@as(u32, 3), list.popFront().?.value);
}

test "LinkedList - iterator" {
    var list: TestList = .{};

    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    var sum: u32 = 0;
    var it = list.iterator();
    while (it.next()) |node| {
        sum += node.value;
    }
    try std.testing.expectEqual(@as(u32, 6), sum);
}

test "LinkedList - drain" {
    var list: TestList = .{};

    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };

    list.pushBack(&n1);
    list.pushBack(&n2);

    var drain_it = list.drain();
    try std.testing.expectEqual(@as(u32, 1), drain_it.next().?.value);
    try std.testing.expectEqual(@as(u32, 2), drain_it.next().?.value);
    try std.testing.expect(drain_it.next() == null);
    try std.testing.expect(list.isEmpty());
}

test "LinkedList - isLinked" {
    var list: TestList = .{};
    var n1 = TestNode{ .value = 1 };

    try std.testing.expect(!TestList.isLinked(&n1));

    list.pushBack(&n1);
    // Note: single node has null prev/next, but it's "linked" in the list sense
    // Actually for a single node both are null, so isLinked returns false
    // This is a limitation - isLinked only checks pointers, not list membership

    var n2 = TestNode{ .value = 2 };
    list.pushBack(&n2);
    try std.testing.expect(TestList.isLinked(&n1)); // n1 now has next
    try std.testing.expect(TestList.isLinked(&n2)); // n2 has prev
}

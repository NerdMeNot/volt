//! Stack Guard - Magic Number Stack Overflow Detection
//!
//! Provides a mechanism to detect stack overflow by placing a magic number
//! (sentinel value) at the bottom of a stack and checking if it's intact.
//! This is useful for fibers, coroutines, or any custom stack allocations.
//!
//! Inspired by zigcoro's approach to stack safety.
//!
//! ## Usage
//!
//! ```zig
//! // When allocating a custom stack
//! var stack = try allocator.alloc(u8, 64 * 1024);
//! StackGuard.init(stack);  // Places sentinel at bottom
//!
//! // ... use the stack ...
//!
//! // Check for overflow (e.g., before/after context switch)
//! if (!StackGuard.check(stack)) {
//!     @panic("Stack overflow detected!");
//! }
//! ```
//!
//! ## Design
//!
//! - Uses a magic number sentinel at the stack bottom
//! - Zero runtime cost when not checking
//! - Detects overflow after the fact (not prevention)
//! - Works with any byte slice representing a stack

const std = @import("std");
const builtin = @import("builtin");

/// Stack guard for detecting overflow via magic number sentinel.
pub const StackGuard = struct {
    /// Magic number used as sentinel value.
    /// Chosen to be unlikely to appear naturally and easy to spot in hex dumps.
    /// 0xDEADBEEF is classic, but we use a larger pattern for 64-bit systems.
    pub const MAGIC: usize = if (@sizeOf(usize) == 8)
        0xDEAD_BEEF_CAFE_BABE
    else
        0xDEAD_BEEF;

    /// Alternative magic for debug builds (helps identify which guard failed).
    /// "BAD CAFE" pattern - recognizable in debugger.
    pub const MAGIC_DEBUG: usize = if (@sizeOf(usize) == 8)
        0xBAD_CAFE_BAD_CAFE
    else
        0xBAD_CAFE;

    /// Size of the guard region in bytes.
    pub const GUARD_SIZE: usize = @sizeOf(usize);

    /// Initialize a stack guard at the bottom of a stack slice.
    /// The sentinel is placed at the lowest address (stack grows down).
    ///
    /// For stacks that grow upward, use `initTop` instead.
    pub fn init(stack: []u8) void {
        if (stack.len < GUARD_SIZE) return;

        const sentinel_ptr: *align(1) usize = @ptrCast(stack.ptr);
        sentinel_ptr.* = if (builtin.mode == .Debug) MAGIC_DEBUG else MAGIC;
    }

    /// Initialize a stack guard at the top of a stack slice.
    /// Use this for stacks that grow upward (less common).
    pub fn initTop(stack: []u8) void {
        if (stack.len < GUARD_SIZE) return;

        const end_ptr = stack.ptr + stack.len - GUARD_SIZE;
        const sentinel_ptr: *align(1) usize = @ptrCast(end_ptr);
        sentinel_ptr.* = if (builtin.mode == .Debug) MAGIC_DEBUG else MAGIC;
    }

    /// Check if the stack guard is intact (no overflow detected).
    /// Returns true if the sentinel is still valid.
    pub fn check(stack: []const u8) bool {
        if (stack.len < GUARD_SIZE) return true; // No guard to check

        const sentinel_ptr: *align(1) const usize = @ptrCast(stack.ptr);
        const expected = if (builtin.mode == .Debug) MAGIC_DEBUG else MAGIC;
        return sentinel_ptr.* == expected;
    }

    /// Check if the top guard is intact.
    pub fn checkTop(stack: []const u8) bool {
        if (stack.len < GUARD_SIZE) return true;

        const end_ptr = stack.ptr + stack.len - GUARD_SIZE;
        const sentinel_ptr: *align(1) const usize = @ptrCast(end_ptr);
        const expected = if (builtin.mode == .Debug) MAGIC_DEBUG else MAGIC;
        return sentinel_ptr.* == expected;
    }

    /// Check and panic if overflow detected.
    /// Provides a clear error message for debugging.
    pub fn checkOrPanic(stack: []const u8) void {
        if (!check(stack)) {
            @panic("Stack overflow detected: guard sentinel corrupted");
        }
    }

    /// Check top and panic if overflow detected.
    pub fn checkTopOrPanic(stack: []const u8) void {
        if (!checkTop(stack)) {
            @panic("Stack overflow detected: top guard sentinel corrupted");
        }
    }

    /// Get the usable stack size after accounting for the guard.
    pub fn usableSize(total_size: usize) usize {
        if (total_size <= GUARD_SIZE) return 0;
        return total_size - GUARD_SIZE;
    }

    /// Get the usable stack slice after the guard region.
    /// Returns the portion of the stack that can be safely used.
    pub fn usableSlice(stack: []u8) []u8 {
        if (stack.len <= GUARD_SIZE) return stack[0..0];
        return stack[GUARD_SIZE..];
    }

    /// Initialize both top and bottom guards for maximum safety.
    /// Useful when stack growth direction is uncertain.
    pub fn initBoth(stack: []u8) void {
        init(stack);
        initTop(stack);
    }

    /// Check both guards.
    pub fn checkBoth(stack: []const u8) bool {
        return check(stack) and checkTop(stack);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "StackGuard - init and check" {
    var stack: [1024]u8 = undefined;
    @memset(&stack, 0);

    StackGuard.init(&stack);
    try std.testing.expect(StackGuard.check(&stack));
}

test "StackGuard - detect corruption" {
    var stack: [1024]u8 = undefined;
    @memset(&stack, 0);

    StackGuard.init(&stack);
    try std.testing.expect(StackGuard.check(&stack));

    // Corrupt the sentinel
    stack[0] = 0xFF;

    try std.testing.expect(!StackGuard.check(&stack));
}

test "StackGuard - top guard" {
    var stack: [1024]u8 = undefined;
    @memset(&stack, 0);

    StackGuard.initTop(&stack);
    try std.testing.expect(StackGuard.checkTop(&stack));

    // Corrupt the top sentinel
    stack[stack.len - 1] = 0xFF;

    try std.testing.expect(!StackGuard.checkTop(&stack));
}

test "StackGuard - both guards" {
    var stack: [1024]u8 = undefined;
    @memset(&stack, 0);

    StackGuard.initBoth(&stack);
    try std.testing.expect(StackGuard.checkBoth(&stack));

    // Corrupt bottom
    stack[0] = 0xFF;
    try std.testing.expect(!StackGuard.checkBoth(&stack));

    // Restore bottom, corrupt top
    StackGuard.init(&stack);
    stack[stack.len - 1] = 0xFF;
    try std.testing.expect(!StackGuard.checkBoth(&stack));
}

test "StackGuard - usable size" {
    try std.testing.expectEqual(
        @as(usize, 1024 - StackGuard.GUARD_SIZE),
        StackGuard.usableSize(1024),
    );
    try std.testing.expectEqual(@as(usize, 0), StackGuard.usableSize(0));
    try std.testing.expectEqual(@as(usize, 0), StackGuard.usableSize(StackGuard.GUARD_SIZE));
}

test "StackGuard - usable slice" {
    var stack: [1024]u8 = undefined;
    StackGuard.init(&stack);

    const usable = StackGuard.usableSlice(&stack);
    try std.testing.expectEqual(@as(usize, 1024 - StackGuard.GUARD_SIZE), usable.len);
    try std.testing.expect(usable.ptr == @as([*]u8, &stack) + StackGuard.GUARD_SIZE);
}

test "StackGuard - small stack" {
    var tiny: [4]u8 = undefined;
    @memset(&tiny, 0);

    // Should handle gracefully
    StackGuard.init(&tiny);
    try std.testing.expect(StackGuard.check(&tiny));
}

test "StackGuard - empty stack" {
    var empty: [0]u8 = undefined;

    // Should not crash on empty
    StackGuard.init(&empty);
    try std.testing.expect(StackGuard.check(&empty));
}

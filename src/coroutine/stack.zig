//! Stack allocation for coroutines.
//!
//! v0.1: simple aligned heap allocation, no guard pages, no slab pooling.
//! Default size 64KB — see `default_size` for rationale. Drops to 4KB at
//! v0.9 once guard pages can catch overflow.
//!
//! Future:
//!   v0.9 — guard pages for overflow detection (mmap with PROT_NONE bottom)
//!   v0.9 — slab-pooled stacks for sub-µs spawn cost
//!   v1.0 — growing stacks (mremap on overflow)

const std = @import("std");

/// AAPCS64 requires 16-byte SP alignment.
pub const stack_alignment: std.mem.Alignment = .@"16";

/// Default stack size. Tuned in v0.1 to be safe-by-default; will drop to
/// 4KB once guard pages land.
///
/// 64KB is generous because Debug-mode allocator stack traces can use ~10KB
/// per allocation for the unwind machinery. Until we have guard pages and
/// release-mode-only test runs, we err on the safe side.
pub const default_size: usize = 64 * 1024;

/// Allocate a coroutine stack. Returns an aligned slice.
pub fn alloc(allocator: std.mem.Allocator, size: usize) ![]align(16) u8 {
    return try allocator.alignedAlloc(u8, stack_alignment, size);
}

/// Free a coroutine stack.
pub fn free(allocator: std.mem.Allocator, stack: []align(16) u8) void {
    allocator.free(stack);
}

/// Top of the stack (highest address). sp grows downward from here.
pub fn topOf(stack: []align(16) u8) [*]u8 {
    return stack.ptr + stack.len;
}

test "stack: alloc returns 16-aligned slice of requested size" {
    const s = try alloc(std.testing.allocator, default_size);
    defer free(std.testing.allocator, s);
    try std.testing.expectEqual(default_size, s.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(s.ptr) & 0xF);
}

test "stack: topOf returns correct end address" {
    const s = try alloc(std.testing.allocator, default_size);
    defer free(std.testing.allocator, s);
    const top = topOf(s);
    try std.testing.expectEqual(@intFromPtr(s.ptr) + s.len, @intFromPtr(top));
}

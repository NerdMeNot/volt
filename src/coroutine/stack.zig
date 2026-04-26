//! Stack allocation for coroutines.
//!
//! v0.4: page-aligned mmap with a `PROT_NONE` guard page at the low end.
//! Stack overflow now hits the guard and triggers SIGSEGV with a clear
//! backtrace, instead of silently corrupting whatever lives next to the
//! stack on the heap.
//!
//! Layout (sp grows downward from the top):
//!
//!   [stack.ptr]                                    [stack.ptr + stack.len]
//!   |── PROT_NONE guard ──|──── usable stack ────|
//!         page_size              `default_size`
//!
//! Future:
//!   v0.9 — slab-pooled stacks for sub-µs spawn cost (no mmap on each spawn)
//!   v1.0 — growable stacks (mremap on guard hit)

const std = @import("std");
const builtin = @import("builtin");

/// AAPCS64 requires 16-byte SP alignment. mmap returns page-aligned memory
/// which is always >= 16-byte aligned, so the topOf() pointer is always
/// safe for SP.
pub const stack_alignment: std.mem.Alignment = .@"16";

/// Usable stack size per coroutine (excludes the guard page).
///
/// 64KB is generous because Debug-mode allocator stack-trace capture uses
/// ~10KB per allocation. With guard pages catching overflow it's safe to
/// drop this — but a tighter default belongs in v0.9 along with slab pools
/// and tuned-per-platform stack benchmarks.
pub const default_size: usize = 64 * 1024;

/// Page size — used to size the guard page. Page-aligned at compile time
/// where possible (Linux/macOS: known); fall back to a sensible value.
pub const page_size: usize = std.heap.page_size_min;

/// Returns a slice covering [guard | usable_stack]. The guard page sits at
/// the LOW end (`stack.ptr` .. `stack.ptr + page_size`) and is mapped
/// PROT_NONE — any access faults. SP starts at `topOf(stack)` and grows
/// down toward the guard.
///
/// The `allocator` argument is unused: stacks are page-aligned and large,
/// so we go directly to mmap rather than through the userspace allocator.
/// The signature is preserved so callers don't have to be rewritten.
pub fn alloc(allocator: std.mem.Allocator, size: usize) ![]align(16) u8 {
    _ = allocator;
    const usable = std.mem.alignForward(usize, size, page_size);
    const total = page_size + usable;

    const map = try std.posix.mmap(
        null,
        total,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    );
    errdefer std.posix.munmap(map);

    // Mark the bottom page PROT_NONE — overflow into it triggers SIGSEGV
    // with a clean stack frame, instead of silently corrupting heap data.
    const guard_ptr: *align(page_size) anyopaque = @ptrCast(map.ptr);
    if (std.c.mprotect(guard_ptr, page_size, .{}) != 0) {
        return error.GuardPageProtectionFailed;
    }

    return @alignCast(map);
}

/// Free a coroutine stack (munmap the whole region including guard page).
pub fn free(allocator: std.mem.Allocator, stack: []align(16) u8) void {
    _ = allocator;
    const aligned: []align(page_size) const u8 = @alignCast(stack);
    std.posix.munmap(aligned);
}

/// Top of the stack (highest address). SP grows downward from here. The
/// usable region runs from `stack.ptr + page_size` (just above the guard)
/// up to this pointer.
pub fn topOf(stack: []align(16) u8) [*]u8 {
    return stack.ptr + stack.len;
}

test "stack: alloc returns a slice of (guard + requested) and is page-aligned" {
    const s = try alloc(std.testing.allocator, default_size);
    defer free(std.testing.allocator, s);
    try std.testing.expectEqual(page_size + default_size, s.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(s.ptr) & (page_size - 1));
}

test "stack: topOf is just past the end" {
    const s = try alloc(std.testing.allocator, default_size);
    defer free(std.testing.allocator, s);
    try std.testing.expectEqual(@intFromPtr(s.ptr) + s.len, @intFromPtr(topOf(s)));
}

test "stack: writing into the guard page faults (cannot test directly — proves the layout instead)" {
    const s = try alloc(std.testing.allocator, default_size);
    defer free(std.testing.allocator, s);
    // The guard page is the first `page_size` bytes. Verify the layout
    // arithmetic by checking that the usable region starts at +page_size
    // and the slice's tail is page-aligned (so SP starts aligned).
    try std.testing.expect(@intFromPtr(s.ptr) % page_size == 0);
    try std.testing.expect(s.len > page_size);
    try std.testing.expect(s.len % page_size == 0);
    // Writing into &s[page_size] (the first byte of the usable region)
    // must succeed — we can't writing into &s[0] without crashing the test.
    s[page_size] = 0xAA;
    try std.testing.expectEqual(@as(u8, 0xAA), s[page_size]);
}

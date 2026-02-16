//! Cache Line Alignment Utilities
//!
//! Provides cache-line-aligned wrappers to prevent false sharing in concurrent
//! data structures. Cache line sizes vary by architecture.
//!

const std = @import("std");
const builtin = @import("builtin");

/// Cache line size for the target architecture.
///
/// Sources:
/// - Intel Sandy Bridge+: 128 bytes (spatial prefetcher pulls pairs of 64-byte lines)
/// - ARM big.LITTLE: 128 bytes (big cores)
/// - PowerPC64: 128 bytes
/// - x86, RISC-V, WASM, SPARC64: 64 bytes
/// - ARM, MIPS, SPARC, Hexagon: 32 bytes
/// - m68k: 16 bytes
/// - s390x: 256 bytes
pub const CACHE_LINE_SIZE: usize = switch (builtin.cpu.arch) {
    // 128-byte cache lines
    .x86_64, .aarch64, .powerpc64, .powerpc64le => 128,

    // 32-byte cache lines
    .arm, .armeb, .mips, .mipsel, .mips64, .mips64el, .sparc, .hexagon => 32,

    // 16-byte cache lines
    .m68k => 16,

    // 256-byte cache lines
    .s390x => 256,

    // 64-byte cache lines (default)
    else => 64,
};

/// Wraps a type with cache line alignment to prevent false sharing.
///
/// Use this for data that will be accessed by multiple threads where
/// you want to ensure each piece of data sits on its own cache line(s).
///
/// Example:
/// ```zig
/// const Counter = CacheAligned(std.atomic.Value(usize));
/// var counters: [NUM_THREADS]Counter = undefined;
/// ```
pub fn CacheAligned(comptime T: type) type {
    return struct {
        data: T align(CACHE_LINE_SIZE),
        // Padding to ensure the struct occupies at least one full cache line
        _padding: [CACHE_LINE_SIZE - @sizeOf(T) % CACHE_LINE_SIZE]u8 = undefined,

        const Self = @This();

        pub fn init(value: T) Self {
            return .{ .data = value };
        }

        pub fn get(self: *Self) *T {
            return &self.data;
        }

        pub fn getConst(self: *const Self) *const T {
            return &self.data;
        }
    };
}

/// Pads a struct to cache line size without requiring the inner type to be specified.
/// Useful when you need padding but not strict alignment.
pub fn CachePadded(comptime T: type) type {
    const padding_needed = CACHE_LINE_SIZE - (@sizeOf(T) % CACHE_LINE_SIZE);
    const actual_padding = if (padding_needed == CACHE_LINE_SIZE) 0 else padding_needed;

    return struct {
        data: T,
        _padding: [actual_padding]u8 = undefined,

        const Self = @This();

        pub fn init(value: T) Self {
            return .{ .data = value };
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "CacheAligned - alignment" {
    const Aligned = CacheAligned(u64);
    try std.testing.expect(@alignOf(Aligned) >= CACHE_LINE_SIZE);
}

test "CacheAligned - size is at least cache line" {
    const Aligned = CacheAligned(u8);
    try std.testing.expect(@sizeOf(Aligned) >= CACHE_LINE_SIZE);
}

test "CachePadded - padding calculation" {
    const Padded = CachePadded(u8);
    // Should be padded to cache line boundary
    try std.testing.expect(@sizeOf(Padded) % CACHE_LINE_SIZE == 0 or @sizeOf(Padded) == CACHE_LINE_SIZE);
}

test "CACHE_LINE_SIZE - reasonable values" {
    // Sanity check: cache line should be power of 2 and reasonable size
    try std.testing.expect(CACHE_LINE_SIZE >= 16);
    try std.testing.expect(CACHE_LINE_SIZE <= 256);
    try std.testing.expect(@popCount(CACHE_LINE_SIZE) == 1); // Power of 2
}

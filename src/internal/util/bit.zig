//! Bit Packing Utilities
//!
//! Provides utilities for packing multiple values into a single integer,
//! commonly used for atomic state management where we need to update
//! multiple related values atomically.
//!
//!
//! Example usage:
//! ```zig
//! // Pack: | shutdown (1 bit) | tick (15 bits) | readiness (16 bits) |
//! const READINESS = Pack.leastSignificant(16);
//! const TICK = READINESS.then(15);
//! const SHUTDOWN = TICK.then(1);
//!
//! var state: u32 = 0;
//! state = READINESS.pack(0xFF, state);  // Set readiness
//! state = TICK.pack(42, state);          // Set tick
//! const tick = TICK.unpack(state);       // Read tick back
//! ```

const std = @import("std");

/// A packed bit field within a larger integer.
pub const Pack = struct {
    /// Bitmask for this field (already shifted to position)
    mask: usize,
    /// Number of bits to shift
    shift: u6,

    const Self = @This();

    /// Create a pack for the `w` least-significant bits.
    pub fn leastSignificant(comptime w: u6) Self {
        return .{
            .mask = maskFor(w),
            .shift = 0,
        };
    }

    /// Create a pack for the `w` bits immediately more significant than this pack.
    pub fn then(self: Self, comptime w: u6) Self {
        const new_shift: u6 = @intCast(@as(u7, @bitSizeOf(usize)) - @clz(self.mask));
        return .{
            .mask = maskFor(w) << new_shift,
            .shift = new_shift,
        };
    }

    /// Get the width in bits of this pack.
    pub fn width(self: Self) u6 {
        return @intCast(@as(u7, @bitSizeOf(usize)) - @clz(self.mask >> self.shift));
    }

    /// Get the maximum representable value for this pack.
    pub fn maxValue(self: Self) usize {
        return (@as(usize, 1) << self.width()) - 1;
    }

    /// Pack a value into a base integer, replacing the bits for this field.
    pub fn pack(self: Self, value: usize, base: usize) usize {
        std.debug.assert(value <= self.maxValue());
        return (base & ~self.mask) | (value << self.shift);
    }

    /// Unpack the value from a packed integer.
    pub fn unpack(self: Self, src: usize) usize {
        return (src & self.mask) >> self.shift;
    }

    /// Check if any bits in this field are set.
    pub fn isSet(self: Self, src: usize) bool {
        return (src & self.mask) != 0;
    }
};

/// Returns a usize with the right-most `n` bits set.
pub fn maskFor(comptime n: u6) usize {
    if (n == 0) return 0;
    if (n >= @bitSizeOf(usize)) return ~@as(usize, 0);
    const shift: usize = @as(usize, 1) << (n - 1);
    return shift | (shift - 1);
}

/// Unpacks a value using a mask and shift (standalone function).
pub fn unpack(src: usize, mask: usize, shift: u6) usize {
    return (src & mask) >> shift;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Pack - least significant bits" {
    const p = Pack.leastSignificant(4);
    try std.testing.expectEqual(@as(usize, 0b1111), p.mask);
    try std.testing.expectEqual(@as(u6, 0), p.shift);
    try std.testing.expectEqual(@as(u6, 4), p.width());
    try std.testing.expectEqual(@as(usize, 15), p.maxValue());
}

test "Pack - then creates next field" {
    const low = Pack.leastSignificant(4);
    const high = low.then(4);

    try std.testing.expectEqual(@as(usize, 0b11110000), high.mask);
    try std.testing.expectEqual(@as(u6, 4), high.shift);
    try std.testing.expectEqual(@as(u6, 4), high.width());
}

test "Pack - pack and unpack" {
    const low = Pack.leastSignificant(4);
    const high = low.then(4);

    var state: usize = 0;
    state = low.pack(0xA, state);
    state = high.pack(0x5, state);

    try std.testing.expectEqual(@as(usize, 0x5A), state);
    try std.testing.expectEqual(@as(usize, 0xA), low.unpack(state));
    try std.testing.expectEqual(@as(usize, 0x5), high.unpack(state));
}

test "Pack - pack replaces existing value" {
    const field = Pack.leastSignificant(8);

    var state: usize = 0xFF;
    state = field.pack(0x12, state);

    try std.testing.expectEqual(@as(usize, 0x12), state);
    try std.testing.expectEqual(@as(usize, 0x12), field.unpack(state));
}

test "Pack - three fields (I/O readiness pattern)" {
    // | shutdown (1 bit) | tick (15 bits) | readiness (16 bits) |
    const READINESS = Pack.leastSignificant(16);
    const TICK = READINESS.then(15);
    const SHUTDOWN = TICK.then(1);

    var state: usize = 0;
    state = READINESS.pack(0xFF, state);
    state = TICK.pack(42, state);
    state = SHUTDOWN.pack(1, state);

    try std.testing.expectEqual(@as(usize, 0xFF), READINESS.unpack(state));
    try std.testing.expectEqual(@as(usize, 42), TICK.unpack(state));
    try std.testing.expectEqual(@as(usize, 1), SHUTDOWN.unpack(state));
    try std.testing.expect(SHUTDOWN.isSet(state));
}

test "maskFor - various widths" {
    try std.testing.expectEqual(@as(usize, 0b1), maskFor(1));
    try std.testing.expectEqual(@as(usize, 0b11), maskFor(2));
    try std.testing.expectEqual(@as(usize, 0b1111), maskFor(4));
    try std.testing.expectEqual(@as(usize, 0xFF), maskFor(8));
    try std.testing.expectEqual(@as(usize, 0xFFFF), maskFor(16));
}

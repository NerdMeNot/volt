//! Invocation ID - Debug-Mode Use-After-Free Detection
//!
//! Provides unique identifiers for operations that help detect use-after-free
//! and ABA problems in concurrent code. Each time a waiter or resource is
//! "reset" for reuse, a new ID is generated, allowing detection of stale
//! references.
//!
//! Inspired by zigcoro's invocation tracking for coroutines.
//!
//! ## Usage
//!
//! ```zig
//! const Waiter = struct {
//!     // ... other fields ...
//!     invocation: InvocationId = .{},
//!
//!     pub fn reset(self: *Waiter) void {
//!         // ... reset other fields ...
//!         self.invocation.bump();  // New ID on reset
//!     }
//!
//!     pub fn complete(self: *Waiter, expected_id: InvocationId.Id) void {
//!         // Check that we're completing the right invocation
//!         self.invocation.verify(expected_id);
//!         // ... complete logic ...
//!     }
//! };
//! ```
//!
//! ## Design
//!
//! - In Debug mode: maintains unique IDs, performs verification
//! - In Release mode: compiles to nothing (zero overhead)
//! - Thread-safe via atomic counter
//! - Helps catch subtle concurrency bugs during development

const std = @import("std");
const builtin = @import("builtin");

/// Invocation ID for tracking reuse of pooled objects.
/// Only active in Debug builds; compiles to zero-size in Release.
pub const InvocationId = struct {
    /// The ID type - u64 to avoid wraparound in practice
    pub const Id = if (builtin.mode == .Debug) u64 else void;

    /// Global counter for generating unique IDs
    var global_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(1);

    /// Current invocation ID (only in Debug)
    /// Starts at 0 (invalid), gets assigned on first bump() or explicit init
    id: Id = if (builtin.mode == .Debug) 0 else {},

    const Self = @This();

    /// Generate a new unique ID.
    pub fn generate() Id {
        if (builtin.mode == .Debug) {
            return global_counter.fetchAdd(1, .seq_cst);
        } else {
            return {};
        }
    }

    /// Get the current ID.
    pub fn get(self: *const Self) Id {
        if (builtin.mode == .Debug) {
            return self.id;
        } else {
            return {};
        }
    }

    /// Bump to a new ID (call on reset/reuse).
    pub fn bump(self: *Self) void {
        if (builtin.mode == .Debug) {
            self.id = generate();
        }
    }

    /// Initialize with a fresh ID (runtime call, not for struct defaults).
    /// For struct field defaults, use the default value (0) and call bump() on first use.
    pub fn init() Self {
        var result = Self{};
        result.bump(); // Generate first ID at runtime
        return result;
    }

    /// Verify that the expected ID matches the current ID.
    /// Panics in Debug mode if they don't match (use-after-free detected).
    pub fn verify(self: *const Self, expected: Id) void {
        if (builtin.mode == .Debug) {
            if (self.id != expected) {
                std.debug.panic(
                    "InvocationId mismatch: expected {}, got {} (possible use-after-free)",
                    .{ expected, self.id },
                );
            }
        }
    }

    /// Check if the expected ID matches (non-panicking version).
    pub fn matches(self: *const Self, expected: Id) bool {
        if (builtin.mode == .Debug) {
            return self.id == expected;
        } else {
            return true;
        }
    }

    /// Create an invalid/sentinel ID (useful for "not initialized" state).
    pub fn invalid() Id {
        if (builtin.mode == .Debug) {
            return 0;
        } else {
            return {};
        }
    }

    /// Check if this ID is valid (non-zero in Debug).
    pub fn isValid(self: *const Self) bool {
        if (builtin.mode == .Debug) {
            return self.id != 0;
        } else {
            return true;
        }
    }
};

/// Lightweight wrapper that can be embedded in waiters.
/// Provides the same functionality with clearer semantics.
pub const WaiterInvocation = struct {
    inner: InvocationId = .{},

    const Self = @This();

    /// Create a new waiter invocation tracker (runtime call, not for struct defaults).
    /// For struct field defaults, use the default value (.{}) and call reset() on first use.
    pub fn init() Self {
        var result = Self{};
        result.reset(); // Generate first ID at runtime
        return result;
    }

    /// Reset the waiter for reuse (generates new ID).
    pub fn reset(self: *Self) void {
        self.inner.bump();
    }

    /// Get a token representing the current invocation.
    /// Store this when starting an operation.
    pub fn token(self: *const Self) InvocationId.Id {
        return self.inner.get();
    }

    /// Verify the token still matches (operation still valid).
    /// Use when completing an operation that was started earlier.
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.inner.verify(tok);
    }

    /// Check if token matches without panicking.
    pub fn tokenMatches(self: *const Self, tok: InvocationId.Id) bool {
        return self.inner.matches(tok);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "InvocationId - generate unique IDs" {
    if (builtin.mode != .Debug) return;

    const id1 = InvocationId.generate();
    const id2 = InvocationId.generate();
    const id3 = InvocationId.generate();

    try std.testing.expect(id1 != id2);
    try std.testing.expect(id2 != id3);
    try std.testing.expect(id1 != id3);
}

test "InvocationId - bump changes ID" {
    if (builtin.mode != .Debug) return;

    var inv = InvocationId.init();
    const original = inv.get();

    inv.bump();
    const after_bump = inv.get();

    try std.testing.expect(original != after_bump);
}

test "InvocationId - verify matching" {
    if (builtin.mode != .Debug) return;

    var inv = InvocationId.init();
    const expected = inv.get();

    // Should not panic
    inv.verify(expected);
    try std.testing.expect(inv.matches(expected));
}

test "InvocationId - detect mismatch" {
    if (builtin.mode != .Debug) return;

    var inv = InvocationId.init();
    const original = inv.get();

    inv.bump();

    // Should detect mismatch
    try std.testing.expect(!inv.matches(original));
}

test "InvocationId - invalid state" {
    if (builtin.mode != .Debug) return;

    var inv = InvocationId{};
    try std.testing.expect(!inv.isValid());

    inv.bump();
    try std.testing.expect(inv.isValid());
}

test "WaiterInvocation - full workflow" {
    if (builtin.mode != .Debug) return;

    var waiter = WaiterInvocation.init();

    // Start an operation
    const tok = waiter.token();

    // Verify it's still valid
    try std.testing.expect(waiter.tokenMatches(tok));

    // Reset (reuse waiter)
    waiter.reset();

    // Old token should no longer match
    try std.testing.expect(!waiter.tokenMatches(tok));

    // New token should match
    const new_tok = waiter.token();
    try std.testing.expect(waiter.tokenMatches(new_tok));
}

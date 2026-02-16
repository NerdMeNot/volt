//! CancelToken - Atomic Cancellation Coordination
//!
//! Used by select() to coordinate which branch wins. The first branch
//! to become ready atomically claims the token, and all other branches
//! see themselves as cancelled.
//!
//! ## Usage
//!
//! ```zig
//! var token = CancelToken.init();
//!
//! // Branch 0 becomes ready first
//! if (token.tryClaimWinner(0)) {
//!     // We won! Process result
//! }
//!
//! // Branch 1 becomes ready later
//! if (token.tryClaimWinner(1)) {
//!     // Won't get here - branch 0 already won
//! }
//!
//! // Check winner
//! const winner = token.getWinner(); // 0
//! ```

const std = @import("std");

/// Shared cancellation state for select branches
pub const CancelToken = struct {
    /// State: 0 = unclaimed, non-zero = (winner_branch_id + 1)
    /// We add 1 so branch 0 can be represented (0 means unclaimed)
    state: std.atomic.Value(usize),

    const Self = @This();

    /// Create a new unclaimed cancel token
    pub fn init() Self {
        return .{ .state = std.atomic.Value(usize).init(0) };
    }

    /// Try to claim as the winner. Returns true if this branch won.
    ///
    /// Only one branch can win - the first to call this successfully.
    /// All subsequent calls return false.
    pub fn tryClaimWinner(self: *Self, branch_id: usize) bool {
        // Encode branch_id as (branch_id + 1) so 0 means unclaimed
        const encoded = branch_id + 1;
        return self.state.cmpxchgStrong(0, encoded, .acq_rel, .acquire) == null;
    }

    /// Check if this token has been claimed (some branch won)
    pub fn isClaimed(self: *const Self) bool {
        return self.state.load(.acquire) != 0;
    }

    /// Check if cancelled from perspective of a specific branch.
    /// A branch is cancelled if the token is claimed by a DIFFERENT branch.
    pub fn isCancelledFor(self: *const Self, branch_id: usize) bool {
        const val = self.state.load(.acquire);
        if (val == 0) return false; // Not yet claimed
        return (val - 1) != branch_id; // Someone else won
    }

    /// Get the winning branch ID, or null if not yet determined
    pub fn getWinner(self: *const Self) ?usize {
        const val = self.state.load(.acquire);
        if (val == 0) return null;
        return val - 1;
    }

    /// Reset to unclaimed state (for reuse)
    pub fn reset(self: *Self) void {
        self.state.store(0, .release);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "CancelToken - first claim wins" {
    var token = CancelToken.init();

    try std.testing.expect(!token.isClaimed());
    try std.testing.expectEqual(@as(?usize, null), token.getWinner());

    // First claim wins
    try std.testing.expect(token.tryClaimWinner(2));
    try std.testing.expect(token.isClaimed());
    try std.testing.expectEqual(@as(?usize, 2), token.getWinner());

    // Second claim fails
    try std.testing.expect(!token.tryClaimWinner(0));
    try std.testing.expect(!token.tryClaimWinner(1));
    try std.testing.expectEqual(@as(?usize, 2), token.getWinner());
}

test "CancelToken - branch 0 can win" {
    var token = CancelToken.init();

    try std.testing.expect(token.tryClaimWinner(0));
    try std.testing.expectEqual(@as(?usize, 0), token.getWinner());
}

test "CancelToken - isCancelledFor" {
    var token = CancelToken.init();

    // Not cancelled before anyone wins
    try std.testing.expect(!token.isCancelledFor(0));
    try std.testing.expect(!token.isCancelledFor(1));

    // Branch 1 wins
    _ = token.tryClaimWinner(1);

    // Branch 0 is cancelled, branch 1 is not
    try std.testing.expect(token.isCancelledFor(0));
    try std.testing.expect(!token.isCancelledFor(1));
    try std.testing.expect(token.isCancelledFor(2));
}

test "CancelToken - reset" {
    var token = CancelToken.init();

    _ = token.tryClaimWinner(5);
    try std.testing.expect(token.isClaimed());

    token.reset();
    try std.testing.expect(!token.isClaimed());
    try std.testing.expectEqual(@as(?usize, null), token.getWinner());

    // Can claim again after reset
    try std.testing.expect(token.tryClaimWinner(3));
    try std.testing.expectEqual(@as(?usize, 3), token.getWinner());
}

test "CancelToken - concurrent claims" {
    var token = CancelToken.init();
    var winners = std.atomic.Value(usize).init(0);

    const num_threads = 8;
    var threads: [num_threads]std.Thread = undefined;

    for (0..num_threads) |i| {
        threads[i] = std.Thread.spawn(.{}, struct {
            fn run(t: *CancelToken, w: *std.atomic.Value(usize), branch: usize) void {
                if (t.tryClaimWinner(branch)) {
                    _ = w.fetchAdd(1, .acq_rel);
                }
            }
        }.run, .{ &token, &winners, i }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // Exactly one thread should have won
    try std.testing.expectEqual(@as(usize, 1), winners.load(.acquire));
    try std.testing.expect(token.getWinner() != null);
}

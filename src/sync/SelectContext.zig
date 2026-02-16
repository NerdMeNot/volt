//! SelectContext - Shared State for Select Operations
//!
//! Coordinates multiple channel waiters for a select() operation.
//! When any branch becomes ready, it claims the CancelToken and wakes
//! the waiting task.
//!
//! ## Architecture
//!
//! ```
//!                    ┌─────────────────────┐
//!                    │   SelectContext     │
//!                    │  - task_waker       │
//!                    │  - cancel_token     │
//!                    │  - completed_branch │
//!                    └──────────┬──────────┘
//!                               │
//!          ┌────────────────────┼────────────────────┐
//!          ▼                    ▼                    ▼
//!   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//!   │BranchWaker 0│     │BranchWaker 1│     │BranchWaker 2│
//!   │  (recv ch1) │     │  (recv ch2) │     │  (timeout)  │
//!   └─────────────┘     └─────────────┘     └─────────────┘
//! ```
//!
//! ## Usage
//!
//! ```zig
//! var ctx = SelectContext.init(3);
//!
//! // Register with channels
//! var waker0 = ctx.branchWaker(0);
//! channel1.registerWaiter(waker0.wakerFn(), waker0.wakerCtx());
//!
//! // When channel1 has data, it calls the waker
//! // -> branchReady(0) is called
//! // -> Claims CancelToken, sets completed_branch = 0
//! // -> Wakes the task
//! ```

const std = @import("std");
const CancelToken = @import("CancelToken.zig").CancelToken;

/// Maximum branches in a select (to avoid dynamic allocation)
pub const MAX_SELECT_BRANCHES: usize = 16;

/// Function pointer type for waking (matches channel WakerFn)
pub const WakerFn = *const fn (*anyopaque) void;

/// Shared context for a select operation
pub const SelectContext = struct {
    /// Cancellation coordination - first branch to complete wins
    cancel_token: CancelToken,

    /// Which branch completed (set atomically by winner)
    completed_branch: std.atomic.Value(usize),

    /// Number of branches in this select
    num_branches: usize,

    /// External waker to call when any branch completes
    /// This wakes the task that's waiting on the select
    external_waker: ?WakerFn,
    external_waker_ctx: ?*anyopaque,

    /// Flag indicating select is complete
    complete: std.atomic.Value(bool),

    const Self = @This();

    /// Sentinel value for "no branch completed yet"
    const NO_WINNER: usize = std.math.maxInt(usize);

    /// Create a new select context
    pub fn init(num_branches: usize) Self {
        std.debug.assert(num_branches > 0);
        std.debug.assert(num_branches <= MAX_SELECT_BRANCHES);

        return .{
            .cancel_token = CancelToken.init(),
            .completed_branch = std.atomic.Value(usize).init(NO_WINNER),
            .num_branches = num_branches,
            .external_waker = null,
            .external_waker_ctx = null,
            .complete = std.atomic.Value(bool).init(false),
        };
    }

    /// Set the external waker (the task waiting on select)
    pub fn setWaker(self: *Self, ctx: *anyopaque, waker_fn: WakerFn) void {
        self.external_waker_ctx = ctx;
        self.external_waker = waker_fn;
    }

    /// Called by a branch when it becomes ready.
    /// Returns true if this branch won (was first to complete).
    pub fn branchReady(self: *Self, branch_id: usize) bool {
        std.debug.assert(branch_id < self.num_branches);

        // Try to claim as winner
        if (self.cancel_token.tryClaimWinner(branch_id)) {
            // We won! Record which branch and mark complete
            self.completed_branch.store(branch_id, .release);
            self.complete.store(true, .release);

            // Wake the waiting task
            self.wakeExternal();

            return true;
        }

        // Lost the race - another branch already won
        return false;
    }

    /// Wake the external task
    fn wakeExternal(self: *Self) void {
        if (self.external_waker) |wf| {
            if (self.external_waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    /// Check if the select is complete
    pub fn isComplete(self: *const Self) bool {
        return self.complete.load(.acquire);
    }

    /// Get the winning branch, or null if not yet complete
    pub fn getWinner(self: *const Self) ?usize {
        const val = self.completed_branch.load(.acquire);
        if (val == NO_WINNER) return null;
        return val;
    }

    /// Check if a specific branch is cancelled (lost the race)
    pub fn isBranchCancelled(self: *const Self, branch_id: usize) bool {
        return self.cancel_token.isCancelledFor(branch_id);
    }

    /// Create a branch waker for a specific branch index
    pub fn branchWaker(self: *Self, branch_id: usize) BranchWaker {
        std.debug.assert(branch_id < self.num_branches);
        return BranchWaker.init(self, branch_id);
    }

    /// Reset for reuse
    pub fn reset(self: *Self) void {
        self.cancel_token.reset();
        self.completed_branch.store(NO_WINNER, .release);
        self.complete.store(false, .release);
        self.external_waker = null;
        self.external_waker_ctx = null;
    }
};

/// Waker for a single select branch
///
/// This is what gets registered with channels. When the channel has data,
/// it calls this waker, which in turn notifies the SelectContext.
pub const BranchWaker = struct {
    ctx: *SelectContext,
    branch_id: usize,

    const Self = @This();

    pub fn init(ctx: *SelectContext, branch_id: usize) Self {
        return .{
            .ctx = ctx,
            .branch_id = branch_id,
        };
    }

    /// Get the waker function pointer (for channel registration)
    pub fn wakerFn(self: *const Self) WakerFn {
        _ = self;
        return wakeCallback;
    }

    /// Get the waker context pointer (for channel registration)
    pub fn wakerCtx(self: *Self) *anyopaque {
        return @ptrCast(self);
    }

    /// The callback invoked when the branch's channel is ready
    fn wakeCallback(ctx_ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx_ptr));
        _ = self.ctx.branchReady(self.branch_id);
    }

    /// Check if this branch was cancelled (another branch won)
    pub fn isCancelled(self: *const Self) bool {
        return self.ctx.isBranchCancelled(self.branch_id);
    }

    /// Check if this branch won
    pub fn isWinner(self: *const Self) bool {
        if (self.ctx.getWinner()) |winner| {
            return winner == self.branch_id;
        }
        return false;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SelectContext - init and basic state" {
    var ctx = SelectContext.init(3);

    try std.testing.expect(!ctx.isComplete());
    try std.testing.expectEqual(@as(?usize, null), ctx.getWinner());
    try std.testing.expectEqual(@as(usize, 3), ctx.num_branches);
}

test "SelectContext - first branch wins" {
    var ctx = SelectContext.init(3);
    var woken = false;

    const TestWaker = struct {
        fn wake(ptr: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ptr));
            w.* = true;
        }
    };

    ctx.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Branch 1 completes first
    try std.testing.expect(ctx.branchReady(1));
    try std.testing.expect(ctx.isComplete());
    try std.testing.expectEqual(@as(?usize, 1), ctx.getWinner());
    try std.testing.expect(woken);

    // Branch 0 tries to complete - should fail
    try std.testing.expect(!ctx.branchReady(0));
    try std.testing.expectEqual(@as(?usize, 1), ctx.getWinner());
}

test "SelectContext - branch wakers" {
    var ctx = SelectContext.init(2);
    var woken = false;

    const TestWaker = struct {
        fn wake(ptr: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ptr));
            w.* = true;
        }
    };

    ctx.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Create branch wakers
    var waker0 = ctx.branchWaker(0);
    var waker1 = ctx.branchWaker(1);

    try std.testing.expect(!waker0.isCancelled());
    try std.testing.expect(!waker1.isCancelled());
    try std.testing.expect(!waker0.isWinner());
    try std.testing.expect(!waker1.isWinner());

    // Simulate channel calling branch 0's waker
    const waker_fn = waker0.wakerFn();
    waker_fn(waker0.wakerCtx());

    try std.testing.expect(ctx.isComplete());
    try std.testing.expect(woken);
    try std.testing.expect(waker0.isWinner());
    try std.testing.expect(!waker1.isWinner());
    try std.testing.expect(!waker0.isCancelled());
    try std.testing.expect(waker1.isCancelled());
}

test "SelectContext - reset" {
    var ctx = SelectContext.init(2);

    _ = ctx.branchReady(0);
    try std.testing.expect(ctx.isComplete());

    ctx.reset();
    try std.testing.expect(!ctx.isComplete());
    try std.testing.expectEqual(@as(?usize, null), ctx.getWinner());

    // Can use again
    try std.testing.expect(ctx.branchReady(1));
    try std.testing.expectEqual(@as(?usize, 1), ctx.getWinner());
}

test "SelectContext - concurrent branch completion" {
    var ctx = SelectContext.init(8);
    var winners = std.atomic.Value(usize).init(0);

    const num_threads = 8;
    var threads: [num_threads]std.Thread = undefined;

    for (0..num_threads) |i| {
        threads[i] = std.Thread.spawn(.{}, struct {
            fn run(c: *SelectContext, w: *std.atomic.Value(usize), branch: usize) void {
                if (c.branchReady(branch)) {
                    _ = w.fetchAdd(1, .acq_rel);
                }
            }
        }.run, .{ &ctx, &winners, i }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // Exactly one thread should have won
    try std.testing.expectEqual(@as(usize, 1), winners.load(.acquire));
    try std.testing.expect(ctx.isComplete());
    try std.testing.expect(ctx.getWinner() != null);
}

//! Completion - Standardized Async Completion Pattern
//!
//! Provides a unified pattern for async operation completion callbacks.
//! Combines waker functionality with optional debug-mode invocation tracking.
//!
//! This is the recommended way to implement completion callbacks in volt
//! sync primitives and I/O operations.
//!
//! ## Usage
//!
//! ```zig
//! const MyWaiter = struct {
//!     completion: Completion = .{},
//!     // ... other fields ...
//!
//!     pub fn setWaker(self: *MyWaiter, ctx: *anyopaque, wake_fn: WakerFn) void {
//!         self.completion.setWaker(ctx, wake_fn);
//!     }
//!
//!     pub fn reset(self: *MyWaiter) void {
//!         self.completion.reset();
//!     }
//! };
//!
//! // When completing an operation:
//! waiter.completion.markComplete();
//! const waker_info = waiter.completion.getWakerInfo();
//! // ... release lock ...
//! if (waker_info) |w| w.wake();
//! ```
//!
//! ## Design
//!
//! - Zero-cost abstraction for completion callbacks
//! - Integrates invocation ID for debug-mode use-after-free detection
//! - Follows the "copy waker info before signaling" pattern to prevent UAF
//! - Thread-safe through atomic complete flag

const std = @import("std");
const builtin = @import("builtin");

const InvocationId = @import("invocation_id.zig").InvocationId;
const WaiterInvocation = @import("invocation_id.zig").WaiterInvocation;

/// Function pointer type for waking.
pub const WakerFn = *const fn (*anyopaque) void;

/// Waker information that can be safely copied and invoked outside locks.
pub const WakerInfo = struct {
    context: *anyopaque,
    wake_fn: WakerFn,

    /// Invoke the waker.
    pub fn wake(self: WakerInfo) void {
        self.wake_fn(self.context);
    }
};

/// Standard completion callback handler.
/// Embed this in waiter structs for consistent async completion handling.
pub const Completion = struct {
    /// Waker function pointer.
    waker: ?WakerFn = null,
    /// Waker context.
    waker_ctx: ?*anyopaque = null,
    /// Whether the operation completed (atomic for cross-thread visibility).
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Optional closed flag for channels.
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Debug-mode invocation tracking.
    invocation: WaiterInvocation = WaiterInvocation.init(),

    const Self = @This();

    /// Initialize a new completion handler.
    pub fn init() Self {
        return .{};
    }

    /// Set the waker callback.
    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    /// Check if a waker is set.
    pub fn hasWaker(self: *const Self) bool {
        return self.waker != null and self.waker_ctx != null;
    }

    /// Get waker info for safe invocation outside locks.
    /// Returns null if no waker is set.
    ///
    /// CRITICAL: Copy this info BEFORE setting complete flag to avoid UAF.
    pub fn getWakerInfo(self: *const Self) ?WakerInfo {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                return .{ .context = ctx, .wake_fn = wf };
            }
        }
        return null;
    }

    /// Mark as complete (operation succeeded).
    pub fn markComplete(self: *Self) void {
        self.complete.store(true, .seq_cst);
    }

    /// Mark as closed (channel/resource closed).
    pub fn markClosed(self: *Self) void {
        self.closed.store(true, .seq_cst);
    }

    /// Check if complete or closed.
    pub fn isComplete(self: *const Self) bool {
        return self.complete.load(.seq_cst) or self.closed.load(.seq_cst);
    }

    /// Check if specifically completed (not just closed).
    pub fn isSuccessful(self: *const Self) bool {
        return self.complete.load(.seq_cst);
    }

    /// Check if specifically closed.
    pub fn isClosed(self: *const Self) bool {
        return self.closed.load(.seq_cst);
    }

    /// Reset for reuse.
    /// Generates a new invocation ID (debug mode).
    pub fn reset(self: *Self) void {
        self.complete.store(false, .seq_cst);
        self.closed.store(false, .seq_cst);
        self.waker = null;
        self.waker_ctx = null;
        self.invocation.reset();
    }

    /// Get the current invocation token (for debug tracking).
    pub fn token(self: *const Self) InvocationId.Id {
        return self.invocation.token();
    }

    /// Verify the invocation token matches (debug mode).
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.invocation.verifyToken(tok);
    }

    /// Complete and wake in one operation.
    /// Returns the waker info for invocation outside the lock.
    ///
    /// Typical usage:
    /// ```zig
    /// mutex.lock();
    /// const waker_info = completion.completeAndGetWaker();
    /// mutex.unlock();
    /// if (waker_info) |w| w.wake();
    /// ```
    pub fn completeAndGetWaker(self: *Self) ?WakerInfo {
        // Copy waker info BEFORE setting complete flag (UAF prevention)
        const waker_info = self.getWakerInfo();
        self.markComplete();
        return waker_info;
    }

    /// Close and wake in one operation.
    pub fn closeAndGetWaker(self: *Self) ?WakerInfo {
        const waker_info = self.getWakerInfo();
        self.markClosed();
        return waker_info;
    }
};

/// Completion with linked list pointers for intrusive lists.
/// Use when waiters need to be stored in a linked list.
pub fn CompletionWithPointers(comptime Pointers: type) type {
    return struct {
        /// Base completion functionality.
        base: Completion = .{},
        /// Intrusive list pointers.
        pointers: Pointers = .{},

        const Self = @This();

        pub fn init() Self {
            return .{};
        }

        // Delegate to base completion
        pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
            self.base.setWaker(ctx, wake_fn);
        }

        pub fn hasWaker(self: *const Self) bool {
            return self.base.hasWaker();
        }

        pub fn getWakerInfo(self: *const Self) ?WakerInfo {
            return self.base.getWakerInfo();
        }

        pub fn markComplete(self: *Self) void {
            self.base.markComplete();
        }

        pub fn markClosed(self: *Self) void {
            self.base.markClosed();
        }

        pub fn isComplete(self: *const Self) bool {
            return self.base.isComplete();
        }

        pub fn isSuccessful(self: *const Self) bool {
            return self.base.isSuccessful();
        }

        pub fn isClosed(self: *const Self) bool {
            return self.base.isClosed();
        }

        pub fn reset(self: *Self) void {
            self.base.reset();
            self.pointers.reset();
        }

        pub fn token(self: *const Self) InvocationId.Id {
            return self.base.token();
        }

        pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
            self.base.verifyToken(tok);
        }

        pub fn completeAndGetWaker(self: *Self) ?WakerInfo {
            return self.base.completeAndGetWaker();
        }

        pub fn closeAndGetWaker(self: *Self) ?WakerInfo {
            return self.base.closeAndGetWaker();
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Completion - basic workflow" {
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var completion = Completion.init();
    try std.testing.expect(!completion.isComplete());

    completion.setWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(completion.hasWaker());

    // Complete and get waker
    const waker_info = completion.completeAndGetWaker();
    try std.testing.expect(completion.isComplete());
    try std.testing.expect(!woken); // Not yet woken

    // Wake outside "lock"
    if (waker_info) |w| w.wake();
    try std.testing.expect(woken);
}

test "Completion - closed state" {
    var completion = Completion.init();

    try std.testing.expect(!completion.isClosed());
    try std.testing.expect(!completion.isComplete());

    completion.markClosed();

    try std.testing.expect(completion.isClosed());
    try std.testing.expect(completion.isComplete()); // isComplete returns true for closed
    try std.testing.expect(!completion.isSuccessful()); // But not successful
}

test "Completion - reset" {
    var completion = Completion.init();
    completion.markComplete();
    try std.testing.expect(completion.isComplete());

    completion.reset();
    try std.testing.expect(!completion.isComplete());
    try std.testing.expect(!completion.hasWaker());
}

test "Completion - no waker" {
    var completion = Completion.init();
    try std.testing.expect(!completion.hasWaker());
    try std.testing.expect(completion.getWakerInfo() == null);

    const waker_info = completion.completeAndGetWaker();
    try std.testing.expect(waker_info == null);
    try std.testing.expect(completion.isComplete());
}

test "Completion - invocation tracking" {
    if (builtin.mode != .Debug) return;

    var completion = Completion.init();
    const tok1 = completion.token();

    completion.reset();
    const tok2 = completion.token();

    // Tokens should be different after reset
    try std.testing.expect(tok1 != tok2);
}

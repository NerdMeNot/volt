//! # Group — Structured Concurrency
//!
//! Spawn multiple fire-and-forget tasks and wait for all of them.
//!
//! ```zig
//! var group = volt.Group.init(io);
//! group.spawn(fetchUser, .{id});
//! group.spawn(fetchPosts, .{id});
//! group.wait();  // blocks until all complete
//! ```
//!
//! ## Lifecycle
//!
//! 1. Create with `Group.init(io)`
//! 2. Spawn tasks with `group.spawn(func, args)`
//! 3. Wait for all with `group.wait()` or cancel with `group.cancel()`
//!
//! ## Limits
//!
//! Tracks up to `max_tasks` (32) concurrent tasks. If you need more,
//! use `io.@"async"()` directly with your own tracking.

const std = @import("std");
const thr = @import("../internal/thread.zig");
const Io = @import("../Io.zig");
const fn_future_mod = @import("../future/FnFuture.zig");
const FnFuture = fn_future_mod.FnFuture;
const FnReturnType = fn_future_mod.FnReturnType;
const sched_runtime = @import("../internal/scheduler/Runtime.zig");
const Header = @import("../internal/scheduler/Header.zig").Header;

/// Maximum number of tasks a Group can track.
pub const max_tasks = 32;

/// Structured concurrency — spawn tasks, wait for all, cancel on scope exit.
pub const Group = struct {
    const Self = @This();

    io: Io,
    headers: [max_tasks]*Header = undefined,
    count: usize = 0,

    /// Create a new empty group.
    pub fn init(io: Io) Self {
        return .{ .io = io };
    }

    /// Spawn a fire-and-forget task into the group.
    ///
    /// The task runs concurrently. Call `wait()` to block until all complete.
    /// Returns false if the group is full or spawn fails.
    pub fn spawn(self: *Self, comptime func: anytype, args: anytype) bool {
        if (self.count >= max_tasks) return false;
        const F = FnFuture(func, @TypeOf(args));
        var handle = self.io.runtime.spawn(F, F.init(args)) catch return false;
        self.headers[self.count] = handle.header;
        self.count += 1;
        // Detach — we track completion via header directly
        handle.detach();
        return true;
    }

    /// Block until all group tasks complete.
    pub fn wait(self: *Self) void {
        for (self.headers[0..self.count]) |header| {
            // Spin-wait for each task to complete
            var spin_count: u32 = 0;
            while (!header.isComplete()) {
                if (spin_count < 6) {
                    std.atomic.spinLoopHint();
                } else if (spin_count < 12) {
                    std.Thread.yield() catch {};
                } else {
                    thr.sleep(1_000);
                }
                spin_count +|= 1;
            }
        }
        self.count = 0;
    }

    /// Cancel all tasks, then wait for them to complete.
    pub fn cancel(self: *Self) void {
        for (self.headers[0..self.count]) |header| {
            _ = header.cancel();
        }
        self.wait();
    }

    /// Number of tasks currently tracked.
    pub fn taskCount(self: *const Self) usize {
        return self.count;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Group - max_tasks constant" {
    try std.testing.expectEqual(@as(usize, 32), max_tasks);
}

test "Group - struct layout" {
    comptime {
        _ = @offsetOf(Group, "io");
        _ = @offsetOf(Group, "headers");
        _ = @offsetOf(Group, "count");
    }
}

test "Group - method signatures" {
    comptime {
        _ = Group.init;
        _ = Group.spawn;
        _ = Group.wait;
        _ = Group.cancel;
        _ = Group.taskCount;
    }
}

test "Group - init default state" {
    // Group.init needs an Io, but we can verify the type instantiation
    // and default field values at comptime
    comptime {
        const G = Group;
        // count field default is 0
        const field_info = @typeInfo(G).@"struct";
        for (field_info.fields) |f| {
            if (std.mem.eql(u8, f.name, "count")) {
                // count has a default value
                std.debug.assert(f.default_value_ptr != null);
            }
        }
    }
}

test "Group - headers array size matches max_tasks" {
    comptime {
        const field_info = @typeInfo(Group).@"struct";
        for (field_info.fields) |f| {
            if (std.mem.eql(u8, f.name, "headers")) {
                // headers is an array of max_tasks pointers
                const arr_info = @typeInfo(f.type);
                std.debug.assert(arr_info == .array);
                std.debug.assert(arr_info.array.len == max_tasks);
            }
        }
    }
}

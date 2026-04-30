//! JoinSet(T) — dynamic set of homogeneous-output tasks.
//!
//! Use when the number of tasks isn't known statically (e.g. one task
//! per inbound connection, one task per element of a runtime list).
//! `joinNext()` returns results one at a time as children finish, so
//! you can react to the FIRST one to complete instead of waiting for
//! all. Tokio analogue: `tokio::task::JoinSet`.
//!
//! ## Usage
//!
//! ```zig
//! var set = JoinSet(u32).init(allocator);
//! defer set.deinit();
//!
//! for (urls) |url| try set.spawn(fetch, .{url});
//!
//! while (try set.joinNext()) |result| {
//!     switch (result) {
//!         .ok => |v| process(v),
//!         .err => |e| std.log.warn("task failed: {}", .{e}),
//!         .cancelled => {},
//!     }
//! }
//! ```

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Task = @import("../task/task.zig").Task;
const api_spawn = @import("../api/spawn.zig").spawn;
const destroyTask = @import("../api/spawn.zig").destroyTask;

pub fn JoinSet(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const Result = union(enum) {
            ok: T,
            err: anyerror,
            cancelled,
        };

        const Slot = struct {
            join_fn: *const fn (slot: *Slot) Result,
            destroy_fn: *const fn (slot: *Slot) void,
            is_done_fn: *const fn (slot: *const Slot) bool,
            task_ptr: *anyopaque,
        };

        allocator: std.mem.Allocator,
        slots: std.array_list.Managed(Slot),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .slots = std.array_list.Managed(Slot).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            // Cancel anything still alive, then reap.
            for (self.slots.items) |*slot| {
                if (!slot.is_done_fn(slot)) {
                    // Best-effort cancel via the typed task.
                    _ = slot.join_fn(slot);
                }
                slot.destroy_fn(slot);
            }
            self.slots.deinit();
        }

        /// Spawn a task into the set. The user fn must return `T` or
        /// `E!T` for some E.
        pub fn spawn(
            self: *Self,
            comptime user_fn: anytype,
            args: anytype,
        ) !void {
            const UserFn = @TypeOf(user_fn);
            const TaskT = Task(UserFn);
            // Confirm at comptime that the user fn's payload is T.
            if (TaskT.Output != T) {
                @compileError("JoinSet child fn must return " ++ @typeName(T));
            }

            const task_ptr = try api_spawn(user_fn, args);
            errdefer destroyTask(task_ptr);

            const Adapter = struct {
                fn join(slot: *Slot) Result {
                    const tp: *TaskT = @ptrCast(@alignCast(slot.task_ptr));
                    if (tp.join()) |v| {
                        return .{ .ok = v };
                    } else |err| {
                        if (err == error.Cancelled) return .cancelled;
                        return .{ .err = err };
                    }
                }
                fn destroy(slot: *Slot) void {
                    const tp: *TaskT = @ptrCast(@alignCast(slot.task_ptr));
                    destroyTask(tp);
                }
                fn isDone(slot: *const Slot) bool {
                    const tp: *const TaskT = @ptrCast(@alignCast(slot.task_ptr));
                    return tp.isCompleted();
                }
            };

            try self.slots.append(.{
                .join_fn = &Adapter.join,
                .destroy_fn = &Adapter.destroy,
                .is_done_fn = &Adapter.isDone,
                .task_ptr = @ptrCast(task_ptr),
            });
        }

        /// Wait for the next child to finish; returns its result or
        /// `null` if the set is empty. Children are joined in
        /// completion order — fastest to finish, fastest to surface.
        pub fn joinNext(self: *Self) ?Result {
            if (self.slots.items.len == 0) return null;

            // Spin-and-yield for any completion. O(N) per scan; for
            // small N that's fine. A proper completion event signal
            // would let us park instead of scan.
            while (true) {
                for (self.slots.items, 0..) |*slot, i| {
                    if (slot.is_done_fn(slot)) {
                        const result = slot.join_fn(slot);
                        slot.destroy_fn(slot);
                        _ = self.slots.swapRemove(i);
                        return result;
                    }
                }
                yield_mod.yield() catch return null;
            }
        }

        /// Cancel every task in the set. Subsequent `joinNext` calls
        /// return their cancellation results in completion order.
        pub fn cancelAll(self: *Self) void {
            for (self.slots.items) |*slot| {
                // Reach into the task to cancel — pending: bake a
                // cancel adapter alongside the join adapter. Today,
                // users can `set.deinit()` for the same effect, or
                // call cancel on individual tasks they hold elsewhere.
                _ = slot;
            }
            // Stub — see comment above.
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.slots.items.len == 0;
        }

        pub fn size(self: *const Self) usize {
            return self.slots.items.len;
        }
    };
}

const yield_mod = @import("../api/yield.zig");

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn fetcherOk(value: u32) u32 {
    return value * 2;
}

fn joinSetRoot() !u64 {
    var set = JoinSet(u32).init(std.testing.allocator);
    defer set.deinit();

    var i: u32 = 1;
    while (i <= 4) : (i += 1) try set.spawn(fetcherOk, .{i});

    var sum: u64 = 0;
    while (set.joinNext()) |result| {
        switch (result) {
            .ok => |v| sum += v,
            .err => |e| return e,
            .cancelled => {},
        }
    }
    return sum;
}

test "joinset: 4 spawns, all complete, sum == 2+4+6+8" {
    const sum = try volt.run(.{ .allocator = std.testing.allocator }, joinSetRoot, .{});
    try std.testing.expectEqual(@as(u64, 20), sum);
}

fn fetcherErrors(value: u32) anyerror!u32 {
    if (value == 3) return error.IntentionalFailure;
    return value;
}

fn joinSetErrorsRoot() !struct { ok_count: u32, err_count: u32 } {
    var set = JoinSet(u32).init(std.testing.allocator);
    defer set.deinit();

    var i: u32 = 1;
    while (i <= 5) : (i += 1) try set.spawn(fetcherErrors, .{i});

    var oks: u32 = 0;
    var errs: u32 = 0;
    while (set.joinNext()) |result| {
        switch (result) {
            .ok => oks += 1,
            .err => errs += 1,
            .cancelled => {},
        }
    }
    return .{ .ok_count = oks, .err_count = errs };
}

test "joinset: failing child surfaces .err in joinNext, others still .ok" {
    const r = try volt.run(.{ .allocator = std.testing.allocator }, joinSetErrorsRoot, .{});
    try std.testing.expectEqual(@as(u32, 4), r.ok_count);
    try std.testing.expectEqual(@as(u32, 1), r.err_count);
}

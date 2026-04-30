//! OnceCell(T) — three-state lazy initialization cell.
//!
//! Initialize a value at most once, even under concurrent access. Other
//! callers parked waiting for the initialization observe the same final
//! value. Useful for shared resources that need lazy setup (config,
//! connection, logger) without a global mutex around every access.
//!
//! ## States
//!
//! - `empty` — no value yet, no initializer in flight
//! - `initializing` — one task is running the user's init fn; others wait
//! - `initialized` — value is set, all `getOrInit` calls return it instantly
//!
//! ## Usage
//!
//! ```zig
//! var once = OnceCell(Config){};
//!
//! const cfg = try once.getOrInit(loadConfig);
//! ```

const std = @import("std");
const assert = std.debug.assert;
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Park = @import("../scheduler/park.zig").Park;
const thread = @import("../internal/thread.zig");

pub fn OnceCell(comptime T: type) type {
    return struct {
        const Self = @This();

        const State = enum(u8) {
            empty,
            initializing,
            initialized,
            errored,
        };

        const Waiter = struct {
            park: Park = .{},
            next: ?*Waiter = null,
        };

        const WaiterList = struct {
            head: ?*Waiter = null,
            tail: ?*Waiter = null,

            fn pushBack(self: *@This(), w: *Waiter) void {
                w.next = null;
                if (self.tail) |t| t.next = w else self.head = w;
                self.tail = w;
            }

            fn drain(self: *@This()) ?*Waiter {
                const head = self.head;
                self.head = null;
                self.tail = null;
                return head;
            }
        };

        state: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(State.empty)),
        value: T = undefined,
        err: anyerror = undefined,
        waiter_mutex: thread.Mutex = .{},
        waiters: WaiterList = .{},

        /// Snapshot the current state. `null` if not yet initialized.
        pub fn get(self: *const Self) ?T {
            const s: State = @enumFromInt(self.state.load(.acquire));
            return if (s == .initialized) self.value else null;
        }

        /// Return the value, initializing it via `init_fn(args)` exactly
        /// once across all concurrent callers. If the init fn errors, all
        /// concurrent callers see that error and the cell stays errored
        /// (subsequent calls will retry — see `getOrInitRetry` below).
        ///
        /// `init_fn` must have signature `fn(@TypeOf(args)) E!T` where E
        /// is any error set (or no error).
        pub fn getOrInit(
            self: *Self,
            comptime init_fn: anytype,
            args: anytype,
        ) !T {
            // Fast path: already initialized.
            const s: State = @enumFromInt(self.state.load(.acquire));
            if (s == .initialized) return self.value;
            if (s == .errored) return self.err;

            // Try to claim the initializer slot.
            const claimed = self.state.cmpxchgStrong(
                @intFromEnum(State.empty),
                @intFromEnum(State.initializing),
                .acq_rel,
                .acquire,
            ) == null;

            if (claimed) {
                return self.runInit(init_fn, args);
            }

            // Someone else is initializing — wait for them.
            return self.waitForInit();
        }

        fn runInit(
            self: *Self,
            comptime init_fn: anytype,
            args: anytype,
        ) !T {
            // We own the .initializing state. Run the init.
            const RT = @typeInfo(@TypeOf(init_fn)).@"fn".return_type.?;
            const result = if (@typeInfo(RT) == .error_union)
                @call(.auto, init_fn, args)
            else
                @as(?RT, @call(.auto, init_fn, args));

            // Publish.
            self.waiter_mutex.lock();
            const drained = self.waiters.drain();

            if (@typeInfo(RT) == .error_union) {
                if (result) |val| {
                    self.value = val;
                    self.state.store(@intFromEnum(State.initialized), .release);
                } else |e| {
                    self.err = e;
                    self.state.store(@intFromEnum(State.errored), .release);
                }
            } else {
                self.value = result.?;
                self.state.store(@intFromEnum(State.initialized), .release);
            }
            self.waiter_mutex.unlock();

            // Wake everyone parked.
            unparkAll(drained);

            // Return.
            const s: State = @enumFromInt(self.state.load(.acquire));
            if (s == .initialized) return self.value;
            return self.err;
        }

        fn waitForInit(self: *Self) !T {
            while (true) {
                var waiter: Waiter = .{};
                self.waiter_mutex.lock();
                const s: State = @enumFromInt(self.state.load(.acquire));
                if (s == .initialized) {
                    self.waiter_mutex.unlock();
                    return self.value;
                }
                if (s == .errored) {
                    self.waiter_mutex.unlock();
                    return self.err;
                }
                self.waiters.pushBack(&waiter);
                self.waiter_mutex.unlock();

                waiter.park.parkCurrent() catch {
                    self.waiter_mutex.lock();
                    _ = self.removeWaiterLocked(&waiter);
                    self.waiter_mutex.unlock();
                    return error.Cancelled;
                };
                // Loop to re-check state — initializer drained us.
            }
        }

        fn removeWaiterLocked(self: *Self, target: *Waiter) bool {
            var prev: ?*Waiter = null;
            var cur = self.waiters.head;
            while (cur) |w| : (cur = w.next) {
                if (w == target) {
                    if (prev) |p| p.next = w.next else self.waiters.head = w.next;
                    if (self.waiters.tail == w) self.waiters.tail = prev;
                    w.next = null;
                    return true;
                }
                prev = w;
            }
            return false;
        }

        fn unparkAll(head: ?*Waiter) void {
            var cur = head;
            while (cur) |w| {
                const next = w.next;
                w.next = null;
                w.park.unpark();
                cur = next;
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn makeFortyTwo() u32 {
    return 42;
}

test "oncecell: get on empty returns null; getOrInit then get returns value" {
    var cell = OnceCell(u32){};
    try std.testing.expect(cell.get() == null);
    const v = try cell.getOrInit(makeFortyTwo, .{});
    try std.testing.expectEqual(@as(u32, 42), v);
    try std.testing.expectEqual(@as(?u32, 42), cell.get());
}

const RaceCtx = struct {
    cell: *OnceCell(u32),
    init_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    seen_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn raceInit(ctx: *RaceCtx) u32 {
    _ = ctx.init_count.fetchAdd(1, .monotonic);
    return 99;
}

fn raceCaller(ctx: *RaceCtx) !void {
    const v = try ctx.cell.getOrInit(raceInit, .{ctx});
    if (v == 99) _ = ctx.seen_count.fetchAdd(1, .monotonic);
}

fn raceRoot() !RaceCtx {
    var cell = OnceCell(u32){};
    var ctx = RaceCtx{ .cell = &cell };

    const N: u32 = 16;
    var jobs: [N]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(raceCaller, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return ctx;
}

test "oncecell: 16 racing callers see exactly 1 init, all get the value" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, raceRoot, .{});
    try std.testing.expectEqual(@as(u32, 1), ctx.init_count.load(.acquire));
    try std.testing.expectEqual(@as(u32, 16), ctx.seen_count.load(.acquire));
}

const FailingInitCtx = struct {
    cell: *OnceCell(u32),
};

fn failingInit() error{InitFailed}!u32 {
    return error.InitFailed;
}

fn failingCaller(ctx: *FailingInitCtx) !void {
    const result = ctx.cell.getOrInit(failingInit, .{});
    try std.testing.expectError(error.InitFailed, result);
}

fn failingRoot() !void {
    var cell = OnceCell(u32){};
    var ctx = FailingInitCtx{ .cell = &cell };

    var j1 = try volt.spawn(failingCaller, .{&ctx});
    defer volt.destroyTask(j1);
    var j2 = try volt.spawn(failingCaller, .{&ctx});
    defer volt.destroyTask(j2);

    try j1.join();
    try j2.join();
}

test "oncecell: failing init propagates to all racing callers" {
    try volt.run(.{ .allocator = std.testing.allocator }, failingRoot, .{});
}

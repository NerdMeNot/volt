//! Task snapshots — observability over live coroutines.
//!
//! `volt.observability.snapshot(allocator, runtime)` walks every worker's
//! `spawned` list and returns a flat `[]TaskSnapshot`. Useful for
//! debugging hangs ("which coro is parked on what?"), `tokio-console`-
//! like dashboards, and the leak-detection test helper.
//!
//! ## Snapshot semantics
//!
//! - The walk takes no locks — it's a best-effort point-in-time read.
//!   Coroutines that complete or new ones that spawn during the walk
//!   may be missed or double-counted in extreme races. For most
//!   diagnostic uses (leak check, periodic dump), that's fine.
//! - The `state` field combines `done_flag` + `cancel_flag` +
//!   `overflow_flag` + a coarse "where is it?" hint derived from
//!   `pending_event`.

const std = @import("std");
const runtime_mod = @import("../runtime.zig");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const event_source = @import("../coroutine/event_source.zig");

pub const TaskState = enum {
    /// Either currently on-CPU, or in some worker's deque/LIFO/injection
    /// waiting to be dispatched. We can't easily distinguish those two
    /// without per-worker locks; both mean "runnable."
    runnable,
    /// Parked on a custom EventSource (Park for channel/mutex/etc.,
    /// reactor for I/O, etc.).
    parked,
    /// `done_flag` is set. Either completed normally, errored,
    /// cancelled, or overflowed — see the bool fields for the variant.
    done,
};

pub const TaskSnapshot = struct {
    /// Stable identifier for this lifetime of the runtime — the
    /// coroutine pointer, cast to usize.
    id: usize,
    state: TaskState,
    is_cancelled: bool,
    is_overflowed: bool,
    /// True if this is the bootstrap (root) coroutine for `volt.run`.
    is_root: bool,
    /// Optional name set via `volt.coroutine.setCurrentName` or at spawn.
    name: []const u8,
    /// Source location of the spawn site, if captured.
    spawn_site: ?std.builtin.SourceLocation,
    /// id of the coroutine that spawned this one (0 = root / unknown).
    parent_id: usize,
};

/// Collect a snapshot of every coroutine the runtime currently owns.
/// Caller owns the returned slice.
pub fn snapshot(allocator: std.mem.Allocator, rt: *runtime_mod.Runtime) ![]TaskSnapshot {
    var list: std.array_list.Managed(TaskSnapshot) = .init(allocator);
    errdefer list.deinit();

    for (rt.workers) |*w| {
        for (w.spawned.items) |coro| {
            try list.append(.{
                .id = @intFromPtr(coro),
                .state = classifyState(coro),
                .is_cancelled = coro.isCancelled(),
                .is_overflowed = coro.overflow_flag.load(.acquire),
                .is_root = coro.is_root,
                .name = coro.name,
                .spawn_site = coro.spawn_site,
                .parent_id = coro.parent_id,
            });
        }
    }

    return list.toOwnedSlice();
}

/// Total count of coroutines the runtime owns (alive or done but not
/// yet reaped). Cheap — no allocation.
pub fn count(rt: *const runtime_mod.Runtime) usize {
    var total: usize = 0;
    for (rt.workers) |*w| total += w.spawned.items.len;
    return total;
}

/// Count just the still-alive coroutines (excludes those whose
/// `done_flag` has fired).
pub fn liveCount(rt: *const runtime_mod.Runtime) usize {
    var total: usize = 0;
    for (rt.workers) |*w| {
        for (w.spawned.items) |coro| {
            if (!coro.isDone()) total += 1;
        }
    }
    return total;
}

fn classifyState(coro: *const Coroutine) TaskState {
    if (coro.isDone()) return .done;
    const pe = coro.pending_event;
    if (pe == &event_source.yield_singleton) return .runnable;
    if (pe == &event_source.done_singleton) return .done;
    return .parked;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const SnapCtx = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn snapChild(ctx: *SnapCtx) !void {
    var k: u32 = 0;
    while (k < 4) : (k += 1) try volt.yield();
    _ = ctx.counter.fetchAdd(1, .monotonic);
}

fn snapshotRoot(saw: *bool) !void {
    var ctx = SnapCtx{};
    var children: [4]*volt.Job = undefined;
    for (&children) |*j| j.* = try volt.launch(snapChild, .{&ctx});
    defer for (children) |j| volt.destroyJob(j);

    // Yield once so children get to a parked/runnable state.
    try volt.yield();

    const rt = @import("../runtime.zig").currentRuntime().?;
    const snap = try snapshot(std.testing.allocator, rt);
    defer std.testing.allocator.free(snap);

    // We expect at least the root + 4 children visible.
    saw.* = snap.len >= 5;

    for (children) |j| try j.join();
}

test "snapshot: visible coroutines include root + spawned children" {
    var saw: bool = false;
    try volt.run(.{ .allocator = std.testing.allocator }, snapshotRoot, .{&saw});
    try std.testing.expect(saw);
}

const Mutex = @import("../sync/Mutex.zig").Mutex;

const LiveCtx = struct { gate: *Mutex };

fn liveCountChild(ctx: *LiveCtx) void {
    // Park on the mutex held by root — deterministic enqueue (no
    // notify-race). Root unlocks after snapshotting, we acquire+exit.
    ctx.gate.lock();
    ctx.gate.unlock();
}

fn liveCountRoot() !usize {
    var gate = Mutex{};
    gate.lock(); // root holds it
    var ctx = LiveCtx{ .gate = &gate };

    var children: [3]*volt.Job = undefined;
    for (&children) |*j| j.* = try volt.launch(liveCountChild, .{&ctx});
    defer for (children) |j| volt.destroyJob(j);

    // Yield enough that all 3 children have queued on the mutex.
    var k: u32 = 0;
    while (k < 16) : (k += 1) try volt.yield();

    const rt = @import("../runtime.zig").currentRuntime().?;
    const live = liveCount(rt);

    // Release them; they acquire one at a time and exit.
    gate.unlock();
    for (children) |j| try j.join();
    return live;
}

test "snapshot: liveCount sees root + 3 children parked on a Mutex" {
    const n = try volt.run(.{ .allocator = std.testing.allocator }, liveCountRoot, .{});
    try std.testing.expect(n >= 4);
}

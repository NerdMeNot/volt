//! Test synchronization helpers.
//!
//! `volt.yield()` is a reschedule hint, NOT a synchronization
//! barrier — the work-stealing scheduler may not actually run a
//! launched coroutine before yield returns. Tests that say "yield N
//! times so the children have a chance to park" pass on quiet
//! systems and flake under load.
//!
//! These helpers give tests deterministic ways to wait for children
//! to reach an expected state:
//!
//!   - `WaitGroup`: counter-based. Children call `done()` *before*
//!     they park; the parent's `wait()` returns once all children
//!     have signaled. Tiny race window between `done()` and the
//!     actual park — typically irrelevant because the cancel path
//!     handles both states.
//!
//!   - `Latch`: single-shot. One side calls `signal()`; others
//!     `wait()` and proceed. Useful when a child needs to confirm
//!     it's reached a specific point.
//!
//!   - `waitForReactorPending(rt, n)`: spins on
//!     `rt.reactor.pendingCount() >= n`. Used when children park
//!     inside an opaque syscall (e.g., `stream.read()` on a
//!     never-readable socket); the reactor's pending counter is
//!     the precise signal that N coroutines are parked on I/O.
//!
//! Surfacing the audit (`scripts/audit_yield_sync.sh`) flagged 15
//! call sites in 8 test files using yield-loops as synchronization;
//! all of them migrate to one of these three primitives.

const std = @import("std");
const volt = @import("../lib.zig");

/// Counter-based group barrier. Use when each child can signal "I'm
/// about to park" before its parking syscall, and the parent wants
/// to wait until all N children have signaled.
pub const WaitGroup = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    target: u32,

    pub fn init(n: u32) WaitGroup {
        return .{ .target = n };
    }

    /// Called by each participant when they reach the synchronization
    /// point. Lock-free, safe across worker threads.
    pub fn done(self: *WaitGroup) void {
        _ = self.counter.fetchAdd(1, .release);
    }

    /// Block (yielding cooperatively) until `done()` has been called
    /// at least `target` times. Single waiter; do not call from
    /// multiple coroutines on the same WaitGroup. Returns
    /// `error.Cancelled` if the calling coroutine is cancelled while
    /// waiting; `error.Timeout` if `timeout_ns` elapses without all
    /// participants signaling.
    pub fn wait(self: *WaitGroup, timeout_ns: u64) error{ Cancelled, Timeout }!void {
        const start = volt.time.nanoTimestamp();
        while (self.counter.load(.acquire) < self.target) {
            if (@as(u64, @intCast(volt.time.nanoTimestamp() - start)) > timeout_ns) {
                return error.Timeout;
            }
            try volt.yield();
        }
    }
};

/// Single-shot signal. Useful when one side wants to confirm "I've
/// reached point X" and the other side wants to proceed only after
/// that confirmation. Both sides call `init()`-shaped values.
pub const Latch = struct {
    fired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn signal(self: *Latch) void {
        self.fired.store(true, .release);
    }

    pub fn wait(self: *Latch, timeout_ns: u64) error{ Cancelled, Timeout }!void {
        const start = volt.time.nanoTimestamp();
        while (!self.fired.load(.acquire)) {
            if (@as(u64, @intCast(volt.time.nanoTimestamp() - start)) > timeout_ns) {
                return error.Timeout;
            }
            try volt.yield();
        }
    }
};

/// Wait until the runtime's reactor has at least `n` registered
/// waits/timers — i.e., at least `n` coroutines are parked on I/O or
/// timer events. Use when children park inside an opaque syscall and
/// the test can't insert a `wg.done()` call before the park itself.
///
/// Returns `error.Cancelled` if the calling coroutine is cancelled,
/// `error.Timeout` on timeout.
pub fn waitForReactorPending(n: usize, timeout_ns: u64) error{ Cancelled, Timeout }!void {
    const rt = volt.currentRuntime() orelse @panic("waitForReactorPending called outside a runtime");
    const start = volt.time.nanoTimestamp();
    while (rt.reactor.pendingCount() < n) {
        if (@as(u64, @intCast(volt.time.nanoTimestamp() - start)) > timeout_ns) {
            return error.Timeout;
        }
        try volt.yield();
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const WgChildCtx = struct { wg: *WaitGroup };

fn wgChild(ctx: *WgChildCtx) void {
    ctx.wg.done();
}

fn wgRoot() !u32 {
    var wg = WaitGroup.init(8);
    var ctx = WgChildCtx{ .wg = &wg };
    var jobs: [8]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(wgChild, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    try wg.wait(1 * std.time.ns_per_s);
    for (jobs) |j| try j.join();
    return wg.counter.load(.acquire);
}

test "WaitGroup: 8 children all signal, parent unblocks" {
    const c = try volt.run(.{ .allocator = std.testing.allocator }, wgRoot, .{});
    try std.testing.expectEqual(@as(u32, 8), c);
}

fn latchSignaler(latch: *Latch) void {
    latch.signal();
}

fn latchRoot() !bool {
    var latch = Latch{};
    const j = try volt.launch(latchSignaler, .{&latch});
    defer volt.destroyJob(j);
    try latch.wait(1 * std.time.ns_per_s);
    try j.join();
    return latch.fired.load(.acquire);
}

test "Latch: single-shot signal unblocks waiter" {
    const ok = try volt.run(.{ .allocator = std.testing.allocator }, latchRoot, .{});
    try std.testing.expect(ok);
}

fn timeoutWaiter(latch: *Latch) void {
    // Never signal — wait should time out.
    _ = latch;
}

fn timeoutRoot() !bool {
    var latch = Latch{};
    const j = try volt.launch(timeoutWaiter, .{&latch});
    defer volt.destroyJob(j);
    try j.join();
    // Wait is bounded; expect Timeout.
    if (latch.wait(5 * std.time.ns_per_ms)) |_| return false else |err| {
        return err == error.Timeout;
    }
}

test "Latch: timeout fires when no signal arrives" {
    const got_timeout = try volt.run(.{ .allocator = std.testing.allocator }, timeoutRoot, .{});
    try std.testing.expect(got_timeout);
}

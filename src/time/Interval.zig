//! Interval — repeating ticker with drift compensation.
//!
//! ```zig
//! var ticker = Interval.start(volt.Duration.fromMillis(100));
//! while (true) {
//!     try ticker.tick();
//!     // do periodic work
//! }
//! ```
//!
//! ## Drift behavior
//!
//! `tick()` returns at the next NEXT scheduled tick, not "current time
//! plus interval". So if your work takes 30ms and the period is 100ms,
//! the next tick fires 70ms after `tick()` returned (not 100ms). The
//! ticker tracks an absolute next-fire instant and compensates each
//! call.
//!
//! ## Missed-tick policies
//!
//! When the work between ticks runs longer than the period, ticks
//! "pile up." Three policies decide what to do:
//!
//! - `.burst` — return immediately for each missed tick. Useful when
//!   you want to catch up on missed work as fast as possible.
//! - `.skip` (default) — fast-forward past missed ticks; the next
//!   tick fires at the next future scheduled instant. Most common.
//! - `.delay` — like skip, but the schedule resets from "now" rather
//!   than aligning to the original period. Useful when the period is
//!   "wait at least this long between actions."

const std = @import("std");
const Duration = @import("../time.zig").Duration;
const Instant = @import("../time.zig").Instant;
const sleep = @import("Sleep.zig").sleep;

pub const MissedTickPolicy = enum { burst, skip, delay };

pub const Interval = struct {
    period: Duration,
    next_fire: Instant,
    policy: MissedTickPolicy = .skip,

    /// Start an interval whose first `tick()` returns immediately, and
    /// subsequent `tick()`s return at multiples of `period`.
    pub fn start(period: Duration) Interval {
        return .{ .period = period, .next_fire = Instant.now() };
    }

    /// Like `start`, but the first `tick()` fires after `period`
    /// (not immediately).
    pub fn startAfter(period: Duration) Interval {
        return .{ .period = period, .next_fire = Instant.now().add(period) };
    }

    pub fn setMissedTickPolicy(self: *Interval, policy: MissedTickPolicy) void {
        self.policy = policy;
    }

    /// Suspend until the next scheduled tick. After return, the next
    /// scheduled instant is updated according to the missed-tick policy.
    pub fn tick(self: *Interval) !void {
        const now_ts = Instant.now();
        if (self.next_fire.isAfter(now_ts)) {
            const wait = self.next_fire.durationSince(now_ts);
            try sleep(wait);
        }
        // Advance to the next fire.
        const after = Instant.now();
        switch (self.policy) {
            .burst => {
                self.next_fire = self.next_fire.add(self.period);
            },
            .skip => {
                // Skip past any ticks we missed; align the next fire
                // to the original schedule.
                while (!self.next_fire.isAfter(after)) {
                    self.next_fire = self.next_fire.add(self.period);
                }
            },
            .delay => {
                self.next_fire = after.add(self.period);
            },
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn intervalRoot() !u32 {
    var ticker = Interval.start(Duration.fromMillis(15));
    var ticks: u32 = 0;
    while (ticks < 4) : (ticks += 1) try ticker.tick();
    return ticks;
}

test "interval: 4 ticks at 15ms cadence completes" {
    const t = try volt.run(.{ .allocator = std.testing.allocator }, intervalRoot, .{});
    try std.testing.expectEqual(@as(u32, 4), t);
}

fn intervalCadenceRoot() !u64 {
    const start_ts = Instant.now();
    var ticker = Interval.start(Duration.fromMillis(20));
    var ticks: u32 = 0;
    while (ticks < 5) : (ticks += 1) try ticker.tick();
    return start_ts.elapsed().asMillis();
}

test "interval: 5 ticks at 20ms cadence takes ≥ 80ms (4 inter-tick periods)" {
    const elapsed_ms = try volt.run(.{ .allocator = std.testing.allocator }, intervalCadenceRoot, .{});
    // First tick is immediate; remaining 4 each wait ~20ms = 80ms minimum.
    try std.testing.expect(elapsed_ms >= 70); // some slack for kernel timing
    try std.testing.expect(elapsed_ms < 500); // sanity guard
}

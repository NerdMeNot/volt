//! Recurring timer — fires every `period`. Configurable
//! "what to do on missed ticks" via `MissedTickBehavior`.
//!
//! Usage:
//!
//! ```zig
//! var ticker = volt.time.Ticker.init(volt.Duration.fromMillis(100));
//! while (!shutdown) {
//!     const tick = try ticker.next();
//!     _ = tick;
//!     try doWork();
//! }
//! ```
//!
//! The Ticker is a pure user-space struct that composes existing
//! `reactor.waitTimer`; no new reactor primitives needed. Internal
//! state tracks the next deadline as an absolute monotonic instant
//! to detect overruns deterministically.

const std = @import("std");
const lib = @import("../lib.zig");
const ReactorWaitError = @import("../reactor.zig").ReactorWaitError;
const cancel_mod = @import("../cancel.zig");

/// What to do when `next()` is called after the deadline has
/// already passed (the caller's work took longer than `period`):
///
///   * `.delay` (default) — reschedule from *now*. Spacing between
///     ticks stays at `period`; tick count drops as overrun bleeds
///     forward. The "drain prevention" behaviour — best when a tick
///     loop must not become a runaway storm if it falls behind.
///
///   * `.skip` — schedule for the next clean period boundary, drop
///     missed ticks. Tick count drops; absolute phase preserved.
///     Best when each tick must align to wall-clock multiples and
///     missing some is OK.
///
///   * `.burst` — return immediately for every missed tick, one per
///     call. Tick count preserved; absolute phase preserved. Best
///     when downstream MUST process every tick (rate-limited
///     producer that fell behind).
pub const MissedTickBehavior = enum { delay, skip, burst };

pub const Ticker = struct {
    period_ns: u64,
    /// Absolute monotonic ns of the next scheduled tick.
    next_tick_ns: u64,
    missed_behavior: MissedTickBehavior = .delay,

    /// Construct a Ticker firing every `period`. First tick is one
    /// `period` from now. For "first tick fires immediately" use
    /// `Ticker.initImmediate`.
    pub fn init(period: lib.Duration) Ticker {
        return .{
            .period_ns = period.ns,
            .next_tick_ns = lib.Instant.now().nanos +| period.ns,
        };
    }

    /// Construct a Ticker whose first `next()` call returns
    /// immediately. Subsequent ticks are spaced `period` apart.
    pub fn initImmediate(period: lib.Duration) Ticker {
        return .{
            .period_ns = period.ns,
            .next_tick_ns = lib.Instant.now().nanos,
        };
    }

    /// Park until the next tick fires; return the `Instant` of the
    /// tick. If a tick has already been missed, behaviour follows
    /// `missed_behavior` (see `MissedTickBehavior` doc above).
    pub fn next(self: *Ticker) ReactorWaitError!lib.Instant {
        const now_ns = lib.Instant.now().nanos;
        const tick_instant = lib.Instant{ .nanos = if (now_ns >= self.next_tick_ns) now_ns else self.next_tick_ns };
        if (now_ns < self.next_tick_ns) {
            const delay = self.next_tick_ns - now_ns;
            const rt = lib.runtime();
            try rt.reactor.waitTimer(delay);
        }
        self.advanceAfterTick();
        return tick_instant;
    }

    /// Cancel-aware variant. Returns `error.Cancelled` if `c` fires
    /// before the next tick.
    pub fn nextCancel(self: *Ticker, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!lib.Instant {
        const now_ns = lib.Instant.now().nanos;
        const tick_instant = lib.Instant{ .nanos = if (now_ns >= self.next_tick_ns) now_ns else self.next_tick_ns };
        if (now_ns < self.next_tick_ns) {
            const delay = self.next_tick_ns - now_ns;
            const rt = lib.runtime();
            try rt.reactor.waitTimerCancel(delay, c);
        }
        self.advanceAfterTick();
        return tick_instant;
    }

    /// Reschedule the next tick for one period from now. Doesn't
    /// fire a tick; affects only the future schedule.
    pub fn reset(self: *Ticker) void {
        self.next_tick_ns = lib.Instant.now().nanos +| self.period_ns;
    }

    /// Reschedule so the next `next()` fires immediately.
    pub fn resetImmediate(self: *Ticker) void {
        self.next_tick_ns = lib.Instant.now().nanos;
    }

    pub fn setMissedTickBehavior(self: *Ticker, b: MissedTickBehavior) void {
        self.missed_behavior = b;
    }

    inline fn advanceAfterTick(self: *Ticker) void {
        const now_ns = lib.Instant.now().nanos;
        switch (self.missed_behavior) {
            // Burst: every overdue tick gets returned, one per call.
            // Just advance by one period regardless of wall-time slip.
            .burst => self.next_tick_ns +|= self.period_ns,

            // Skip: jump phase forward to the first future boundary,
            // dropping all overrun ticks. Preserves "tick on multiples
            // of period from t0" semantics.
            .skip => {
                if (now_ns >= self.next_tick_ns) {
                    const slip = now_ns - self.next_tick_ns;
                    const periods_to_skip = slip / self.period_ns + 1;
                    self.next_tick_ns +|= periods_to_skip * self.period_ns;
                } else {
                    self.next_tick_ns +|= self.period_ns;
                }
            },

            // Delay: reschedule from *now*. Spacing between ticks
            // stays at one period; absolute phase drifts forward.
            .delay => self.next_tick_ns = now_ns +| self.period_ns,
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;

fn tickerNextRoot() !void {
    var ticker = Ticker.init(lib.Duration.fromMillis(10));
    const start = lib.Instant.now();
    var i: u32 = 0;
    while (i < 5) : (i += 1) _ = try ticker.next();
    const elapsed = start.elapsed();
    // 5 ticks of 10ms = 50ms minimum. Allow generous upper bound
    // for CI scheduler jitter.
    try testing.expect(elapsed.ns >= 49 * std.time.ns_per_ms);
    try testing.expect(elapsed.ns < 500 * std.time.ns_per_ms);
}

test "Ticker.next fires every period" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc });
    defer rt.deinit();
    try (try rt.run(tickerNextRoot, .{}));
}

fn tickerImmediateRoot() !void {
    var ticker = Ticker.initImmediate(lib.Duration.fromMillis(100));
    const start = lib.Instant.now();
    _ = try ticker.next(); // should fire immediately
    const elapsed = start.elapsed();
    // Should fire within scheduler-jitter window (sub-ms ideally,
    // tens of ms worst case in CI).
    try testing.expect(elapsed.ns < 50 * std.time.ns_per_ms);
}

test "Ticker.initImmediate fires first tick immediately" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc });
    defer rt.deinit();
    try (try rt.run(tickerImmediateRoot, .{}));
}

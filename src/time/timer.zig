//! Single-shot timer handle. Reusable across iterations via `reset`.
//!
//! `volt.sleep(d)` is the one-liner shortcut; `Timer` is the
//! ergonomic upgrade when you want to:
//!   * re-arm without re-constructing
//!   * pass a single handle to multiple call sites
//!   * keep deadline state on the stack across loop iterations
//!
//! Single-shot only — for recurring timers see `Ticker`.

const std = @import("std");
const lib = @import("../lib.zig");
const ReactorWaitError = @import("../reactor.zig").ReactorWaitError;
const cancel_mod = @import("../cancel.zig");

pub const Timer = struct {
    delay_ns: u64,

    /// New Timer with `d` until fire. Doesn't start the clock until
    /// `.wait()` is called.
    pub fn init(d: lib.Duration) Timer {
        return .{ .delay_ns = d.ns };
    }

    /// Park the calling coroutine until the timer fires.
    pub fn wait(self: Timer) ReactorWaitError!void {
        const rt = lib.runtime();
        return rt.reactor.waitTimer(self.delay_ns);
    }

    /// Cancel-aware variant. Returns `error.Cancelled` if `c` fires
    /// before the deadline; the kernel-side timer is deregistered
    /// eagerly so the resource isn't held past the cancel.
    pub fn waitCancel(self: Timer, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        const rt = lib.runtime();
        return rt.reactor.waitTimerCancel(self.delay_ns, c);
    }

    /// Re-arm with a new delay. The next `.wait()` uses the new
    /// value; doesn't affect a wait already in progress.
    pub fn reset(self: *Timer, d: lib.Duration) void {
        self.delay_ns = d.ns;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;

fn timerWaitRoot() !void {
    const start = lib.Instant.now();
    var t = Timer.init(lib.Duration.fromMillis(10));
    try t.wait();
    const elapsed = start.elapsed();
    try testing.expect(elapsed.ns >= 9 * std.time.ns_per_ms);
    try testing.expect(elapsed.ns < 100 * std.time.ns_per_ms);
}

test "Timer.wait parks for the specified duration" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc });
    defer rt.deinit();
    try (try rt.run(timerWaitRoot, .{}));
}

fn timerResetRoot() !void {
    var t = Timer.init(lib.Duration.fromMillis(5));
    const start = lib.Instant.now();
    try t.wait();
    t.reset(lib.Duration.fromMillis(5));
    try t.wait();
    const elapsed = start.elapsed();
    // Two 5ms waits ≈ 10ms total (plus scheduling slop).
    try testing.expect(elapsed.ns >= 9 * std.time.ns_per_ms);
}

test "Timer.reset re-arms for the next wait" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc });
    defer rt.deinit();
    try (try rt.run(timerResetRoot, .{}));
}

//! Duration / Instant — typed wrappers over `u64` nanoseconds.
//!
//! Library authors building on Volt express timeouts and deadlines
//! in these types; the reactor layer below them still works in raw
//! `u64 ns`. The split keeps the public API type-safe (no
//! `sleep(10 * 1000)` ambiguity over seconds vs milliseconds) while
//! the hot path stays an unboxed integer.
//!
//! `Instant.now()` uses the OS's monotonic clock — `clock_gettime
//! (CLOCK_MONOTONIC)` on POSIX, `QueryPerformanceCounter` on
//! Windows. Both guarantee non-decreasing readings unaffected by
//! wall-clock adjustments; that's the right notion for timeouts.

const std = @import("std");
const builtin = @import("builtin");

/// A length of time in nanoseconds. Plain wrapper over `u64` so
/// callers can write `Duration.fromMillis(50)` instead of
/// `50_000_000` and have the type system catch unit mistakes.
pub const Duration = struct {
    ns: u64,

    pub fn fromNanos(n: u64) Duration {
        return .{ .ns = n };
    }

    pub fn fromMicros(n: u64) Duration {
        return .{ .ns = n * std.time.ns_per_us };
    }

    pub fn fromMillis(n: u64) Duration {
        return .{ .ns = n * std.time.ns_per_ms };
    }

    pub fn fromSecs(n: u64) Duration {
        return .{ .ns = n * std.time.ns_per_s };
    }

    pub fn toNanos(self: Duration) u64 {
        return self.ns;
    }

    pub fn toMicros(self: Duration) u64 {
        return self.ns / std.time.ns_per_us;
    }

    pub fn toMillis(self: Duration) u64 {
        return self.ns / std.time.ns_per_ms;
    }

    pub fn toSecs(self: Duration) u64 {
        return self.ns / std.time.ns_per_s;
    }

    /// Saturating addition — never wraps. A `Duration` constructed
    /// at `u64.max` represents "essentially forever" and stays
    /// there under further additions.
    pub fn add(self: Duration, other: Duration) Duration {
        const sum, const ov = @addWithOverflow(self.ns, other.ns);
        return .{ .ns = if (ov == 1) std.math.maxInt(u64) else sum };
    }
};

/// A point on the monotonic clock. Compare two `Instant`s to get
/// a `Duration`. Constructed only via `now()` so the underlying
/// clock source can't be confused with wall-clock or process-
/// CPU-time readings.
pub const Instant = struct {
    nanos: u64,

    /// Sample the monotonic clock. Cheap (~30 ns on modern x86 /
    /// arm64).
    pub fn now() Instant {
        if (comptime builtin.os.tag == .windows) return nowWindows();
        return nowPosix();
    }

    /// Duration since `earlier`. Caller is responsible for passing
    /// the earlier reading first; if `earlier` is in the future
    /// (impossible on a monotonic clock from one process, but
    /// fence-post in tests), returns `Duration{ .ns = 0 }`.
    pub fn since(self: Instant, earlier: Instant) Duration {
        if (self.nanos <= earlier.nanos) return .{ .ns = 0 };
        return .{ .ns = self.nanos - earlier.nanos };
    }

    /// Duration from this `Instant` to `now()`. Equivalent to
    /// `Instant.now().since(self)`.
    pub fn elapsed(self: Instant) Duration {
        return Instant.now().since(self);
    }

    /// Add a `Duration` to produce a future `Instant`. Saturates
    /// at `u64.max` rather than wrapping.
    pub fn add(self: Instant, d: Duration) Instant {
        const sum, const ov = @addWithOverflow(self.nanos, d.ns);
        return .{ .nanos = if (ov == 1) std.math.maxInt(u64) else sum };
    }
};

// ─── Per-platform monotonic clock ────────────────────────────────

fn nowPosix() Instant {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return .{
        .nanos = @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec)),
    };
}

// ─── Windows ─────────────────────────────────────────────────────
//
// QueryPerformanceCounter ticks at a frequency reported by
// QueryPerformanceFrequency. We convert to nanoseconds with the
// usual `(counter * 1e9) / freq` form, taking care to avoid u64
// overflow on the multiply (split into seconds + sub-second parts).

const LARGE_INTEGER = i64;
extern "kernel32" fn QueryPerformanceCounter(lpPerformanceCount: *LARGE_INTEGER) callconv(.winapi) std.os.windows.BOOL;
extern "kernel32" fn QueryPerformanceFrequency(lpFrequency: *LARGE_INTEGER) callconv(.winapi) std.os.windows.BOOL;

var qpc_freq_cache: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

fn qpcFreq() u64 {
    const cached = qpc_freq_cache.load(.acquire);
    if (cached != 0) return cached;
    var f: LARGE_INTEGER = 0;
    _ = QueryPerformanceFrequency(&f);
    const freq: u64 = @intCast(f);
    qpc_freq_cache.store(freq, .release);
    return freq;
}

fn nowWindows() Instant {
    var c: LARGE_INTEGER = 0;
    _ = QueryPerformanceCounter(&c);
    const counter: u64 = @intCast(c);
    const freq = qpcFreq();
    // Split to dodge u64 overflow: counter / freq → whole seconds;
    // (counter % freq) * 1e9 / freq → sub-second nanos.
    const secs = counter / freq;
    const rem = counter % freq;
    const sub_ns = (rem * std.time.ns_per_s) / freq;
    return .{ .nanos = secs * std.time.ns_per_s + sub_ns };
}

// ─── Tests ───────────────────────────────────────────────────────

test "Duration: unit conversions round-trip" {
    try std.testing.expectEqual(@as(u64, 1_000), Duration.fromMicros(1).toNanos());
    try std.testing.expectEqual(@as(u64, 1_000_000), Duration.fromMillis(1).toNanos());
    try std.testing.expectEqual(@as(u64, 1_000_000_000), Duration.fromSecs(1).toNanos());
    try std.testing.expectEqual(@as(u64, 1), Duration.fromMillis(1).toMillis());
    try std.testing.expectEqual(@as(u64, 1), Duration.fromSecs(1).toSecs());
}

test "Duration: add saturates at u64.max" {
    const huge = Duration.fromNanos(std.math.maxInt(u64) - 5);
    const more = Duration.fromNanos(100);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), huge.add(more).ns);
}

test "Instant: now is monotonic across two readings" {
    const a = Instant.now();
    // Spin a touch so the clock has a chance to advance even at
    // the lowest-resolution backends.
    var i: u32 = 0;
    while (i < 1_000) : (i += 1) std.atomic.spinLoopHint();
    const b = Instant.now();
    try std.testing.expect(b.nanos >= a.nanos);
}

test "Instant: since / elapsed reflect Duration arithmetic" {
    const a = Instant.now();
    const b = a.add(Duration.fromMillis(50));
    try std.testing.expectEqual(@as(u64, 50 * std.time.ns_per_ms), b.since(a).ns);
    try std.testing.expectEqual(@as(u64, 0), a.since(b).ns); // saturating
}

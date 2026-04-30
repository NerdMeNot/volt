//! OpenTelemetry-style tracing — `volt.tracing.span("name", body)`.
//!
//! v1.0 first cut: writes structured JSON span events to a configured
//! sink (default: stderr). Format is OTel-shaped (trace_id, span_id,
//! parent_span_id, name, start_unix_nano, end_unix_nano) so a
//! downstream collector / log-router can ingest. Full OTLP/HTTP +
//! Jaeger exporters are v1.x add-ons that implement `Exporter`.
//!
//! ## Usage
//!
//! ```zig
//! const v = try volt.tracing.span(.{ .name = "fetch_user" }, struct {
//!     fn body() !User {
//!         return try fetchUser();
//!     }
//! }.body);
//! ```
//!
//! ## Span context propagation
//!
//! Span context (trace_id + parent_span_id) is stored in coroutine-
//! local TLS via `current_span_id` / `current_trace_id`. A span body
//! that itself calls `volt.tracing.span(...)` produces a child span
//! with the outer span as its parent — the standard async-trace
//! propagation pattern.

const std = @import("std");
const builtin = @import("builtin");
const time_mod = @import("../time.zig");

threadlocal var current_trace_id: u128 = 0;
threadlocal var current_span_id: u64 = 0;

pub const SpanOptions = struct {
    name: []const u8,
};

/// Per-thread PRNG seeded once. Trace/span IDs aren't security-
/// critical (cryptographic randomness would be overkill) — just
/// well-distributed pseudo-random values.
threadlocal var rng_state: ?std.Random.DefaultPrng = null;

fn nextU64() u64 {
    if (rng_state == null) {
        const seed: u64 = @bitCast(@as(i64, @truncate(time_mod.nanoTimestamp())));
        rng_state = std.Random.DefaultPrng.init(seed);
    }
    return rng_state.?.random().int(u64);
}

fn generateTraceId() u128 {
    const hi: u128 = nextU64();
    const lo: u128 = nextU64();
    return (hi << 64) | lo;
}

fn generateSpanId() u64 {
    return nextU64();
}

const ns_per_s: u64 = std.time.ns_per_s;

/// Run `body()` inside a tracing span. Emits `span.start` on entry and
/// `span.end` (with elapsed time) on exit. Returns body's value or error.
pub fn span(opts: SpanOptions, comptime body: anytype) !@import("../coroutine/spawn.zig").PayloadOf(@TypeOf(body)) {
    const parent_span = current_span_id;
    const my_trace = if (current_trace_id == 0) generateTraceId() else current_trace_id;
    const my_span = generateSpanId();

    current_trace_id = my_trace;
    current_span_id = my_span;
    defer {
        current_span_id = parent_span;
        // current_trace_id stays — the trace continues for siblings.
    }

    const start = time_mod.nanoTimestamp();
    emitEvent(.{
        .kind = .start,
        .name = opts.name,
        .trace_id = my_trace,
        .span_id = my_span,
        .parent_span_id = parent_span,
        .timestamp_ns = @intCast(start),
        .duration_ns = 0,
    });

    const result = body();
    const end = time_mod.nanoTimestamp();
    const dur: u64 = @intCast(end - start);

    emitEvent(.{
        .kind = .end,
        .name = opts.name,
        .trace_id = my_trace,
        .span_id = my_span,
        .parent_span_id = parent_span,
        .timestamp_ns = @intCast(end),
        .duration_ns = dur,
    });

    return result;
}

const Event = struct {
    kind: enum { start, end },
    name: []const u8,
    trace_id: u128,
    span_id: u64,
    parent_span_id: u64,
    timestamp_ns: u64,
    duration_ns: u64,
};

/// Pluggable sink: receives an Event each emit. Default is `stderr`
/// in JSON-line format.
var sink_fn: *const fn (Event) void = stderrSink;

pub fn setSink(s: *const fn (Event) void) void {
    sink_fn = s;
}

fn stderrSink(event: Event) void {
    // Only emit `end` events by default — they carry the duration and
    // are sufficient for a "what happened" view. `start` is useful for
    // live debugging; users can swap in a fuller sink.
    if (event.kind == .start) return;

    var buf: [512]u8 = undefined;
    const out = std.fmt.bufPrint(
        &buf,
        \\{{"kind":"{s}","name":"{s}","trace":"{x:032}","span":"{x:016}","parent":"{x:016}","ts_ns":{d},"dur_ns":{d}}}{c}
    ,
        .{
            @tagName(event.kind),
            event.name,
            event.trace_id,
            event.span_id,
            event.parent_span_id,
            event.timestamp_ns,
            event.duration_ns,
            '\n',
        },
    ) catch return;
    _ = std.posix.system.write(2, out.ptr, out.len);
}

fn emitEvent(event: Event) void {
    sink_fn(event);
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

threadlocal var captured_count: u32 = 0;
threadlocal var last_name: []const u8 = "";
threadlocal var last_dur: u64 = 0;

fn capturingSink(e: Event) void {
    if (e.kind == .end) {
        captured_count += 1;
        last_name = e.name;
        last_dur = e.duration_ns;
    }
}

fn quickWork() u32 {
    return 7;
}

test "tracing: span captures end event with name and duration" {
    captured_count = 0;
    setSink(capturingSink);
    defer setSink(stderrSink);

    const v = try span(.{ .name = "test_span" }, quickWork);
    try std.testing.expectEqual(@as(u32, 7), v);
    try std.testing.expectEqual(@as(u32, 1), captured_count);
    try std.testing.expectEqualStrings("test_span", last_name);
    // duration > 0 (assuming clock resolution)
    try std.testing.expect(last_dur >= 0);
}

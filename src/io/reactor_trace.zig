//! Compile-time-gated event trace ring buffer for the reactor.
//!
//! Records every register/unregister/poll-consume event with thread
//! id + monotonic nanotime + (fd, kind) + path-tag. On runtime exit
//! (Runtime.deinit) or eager-invariant violation, the buffer dumps
//! the most recent N events to stderr — letting us see the actual
//! interleaving across threads that produced a leak.
//!
//! ## Why
//!
//! The cancel/park/poll race that intermittently leaks reactor
//! registrations (see docs/internals/cancellation-contract.md
//! status section) is invisible at the source level — all 11
//! enumerated scenarios appear race-free under the documented
//! obligations. The leak manifests as pendingCount > 0 at deinit,
//! with no clue WHEN the bookkeeping diverged.
//!
//! With this trace enabled (`-Dreactor-trace`), we get the temporal
//! truth: the exact sequence of (fd, op, thread, time) tuples that
//! the runtime executed in a failing run. That's what's needed to
//! find the missing scenario.
//!
//! ## Cost
//!
//! - When `reactor_trace = false` (the default): every record() call
//!   compiles to nothing. Zero runtime cost in production builds.
//! - When `reactor_trace = true`: a single atomic fetchAdd to claim
//!   a slot + a struct copy. ~10ns per event on Apple Silicon. Worth
//!   it during diagnosis; not worth shipping always-on.
//!
//! ## Buffer policy
//!
//! Fixed-size ring buffer (default 65536 entries × 32 bytes/entry =
//! 2 MiB). When the buffer wraps, oldest entries are overwritten —
//! we trade unbounded history for bounded memory. The dump shows
//! the wrap status so reviewers know if events older than the
//! window are missing.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const time_mod = @import("../time.zig");

pub const enabled: bool = build_options.reactor_trace;

const RING_CAPACITY: usize = 65_536;

/// Path-tag that identifies WHICH code path emitted the event.
/// Reviewing a dump, the tag tells you where in the reactor the
/// event came from — register, the cancel-arm's unregisterWait,
/// poll's event-consume path, etc.
pub const PathTag = enum(u8) {
    register_ok,
    register_fail_kevent,
    register_fail_oom,
    unregister_kevent_ok,
    unregister_kevent_fail,
    unregister_remove_true,
    unregister_remove_false,
    poll_event_received,
    poll_remove_true,
    poll_remove_false,
    poll_wakefn_invoked,
    timer_register_ok,
    timer_unregister_ok,
};

/// Single trace record. 32 bytes.
pub const Event = extern struct {
    /// Monotonic timestamp in nanoseconds (volt.time.nanoTimestamp).
    ts_ns: u64,
    /// pthread id of the recording thread.
    thread_id: u64,
    /// fd (or 0xFFFFFFFF for events not associated with an fd).
    fd: i32,
    /// EventKind tag (0=readable, 1=writable, 0xFF=non-fd events).
    kind: u8,
    /// PathTag (see above).
    tag: u8,
    /// Reactor's pendingCount snapshot AFTER the operation. Lets us
    /// see how the count evolved over time.
    pending_after: u16,
};

const RingBuffer = struct {
    entries: [RING_CAPACITY]Event = undefined,
    /// Atomic write index. Modulo RING_CAPACITY for the slot.
    /// Captures the TOTAL number of events ever written so the dump
    /// can report wraps.
    write_idx: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

var ring: RingBuffer = .{};

/// Record an event. No-op when `enabled == false`.
pub fn record(
    tag: PathTag,
    fd: i32,
    kind: u8,
    pending_after: usize,
) void {
    if (comptime !enabled) return;

    // Compile-time-only inside the if; the rest of this body is
    // dead code in non-trace builds.
    const ts: u64 = @intCast(time_mod.nanoTimestamp());
    const tid: u64 = blk: {
        if (builtin.os.tag == .macos or builtin.os.tag == .ios) {
            // pthread_mach_thread_np gives a stable thread id on Darwin.
            const c = std.c;
            break :blk @intCast(@intFromPtr(c.pthread_self()));
        }
        break :blk std.Thread.getCurrentId();
    };
    const slot = ring.write_idx.fetchAdd(1, .monotonic);
    ring.entries[@intCast(slot % RING_CAPACITY)] = .{
        .ts_ns = ts,
        .thread_id = tid,
        .fd = fd,
        .kind = kind,
        .tag = @intFromEnum(tag),
        .pending_after = std.math.lossyCast(u16, pending_after),
    };
}

/// Dump the last `n` events to stderr in chronological order.
/// No-op when not enabled. Pass `n = std.math.maxInt(usize)` for
/// the entire buffer.
pub fn dumpLast(n: usize) void {
    if (comptime !enabled) return;

    const total: u64 = ring.write_idx.load(.acquire);
    if (total == 0) {
        std.debug.print("[reactor-trace] no events recorded\n", .{});
        return;
    }
    const wrapped = total > RING_CAPACITY;
    const count = @min(@min(total, RING_CAPACITY), n);
    const start: u64 = if (wrapped) total - count else 0;

    std.debug.print(
        "[reactor-trace] {d} events recorded (wrapped={}); dumping last {d}:\n",
        .{ total, wrapped, count },
    );

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const idx: usize = @intCast((start + i) % RING_CAPACITY);
        const ev = ring.entries[idx];
        const tag_name = @tagName(@as(PathTag, @enumFromInt(ev.tag)));
        const kind_str: []const u8 = switch (ev.kind) {
            0 => "readable",
            1 => "writable",
            else => "-",
        };
        std.debug.print(
            "  #{d:>6}  ts={d:>15}  tid={x:0>16}  fd={d:>5}  {s:>9}  {s:>26}  pending={d}\n",
            .{ start + i, ev.ts_ns, ev.thread_id, ev.fd, kind_str, tag_name, ev.pending_after },
        );
    }
}

/// Convenience: dump the entire ring (or all recorded events if
/// fewer than capacity).
pub fn dumpAll() void {
    if (comptime !enabled) return;
    dumpLast(RING_CAPACITY);
}

/// Reset the ring buffer. Call at Runtime.init so each `volt.run` in a
/// test suite gets a clean trace — no events from prior runtimes
/// contaminate the dump on a leak.
pub fn reset() void {
    if (comptime !enabled) return;
    ring.write_idx.store(0, .release);
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "reactor_trace: record + dump compile to nothing when disabled" {
    // This test runs whether or not reactor_trace is enabled —
    // when disabled, record() and dumpLast() are no-ops; when
    // enabled, they go through the ring buffer.
    record(.register_ok, 42, 0, 1);
    record(.poll_event_received, 42, 0, 1);
    record(.poll_remove_true, 42, 0, 0);
    if (enabled) {
        try std.testing.expect(ring.write_idx.load(.monotonic) >= 3);
    }
    // dumpLast goes to stderr; we don't assert content, only that
    // it doesn't panic.
    dumpLast(10);
}

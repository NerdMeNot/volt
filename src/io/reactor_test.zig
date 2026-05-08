//! Backend-agnostic reactor smoke tests.
//!
//! Same scenarios run against whichever backend the active build
//! selected (kqueue on Darwin/BSD, epoll on Linux without
//! `-Dreactor=iouring`, io_uring with it, IOCP on Windows once
//! Phase 2 lands).
//!
//! These complement the per-backend integration tests already living
//! in each `reactor_<backend>.zig`. The point of this file is to
//! exercise the *public protocol* — what `wait.zig` / `Sleep.zig` /
//! Shutdown.zig consume — through whichever impl is currently active.

const std = @import("std");
const builtin = @import("builtin");
const reactor_mod = @import("reactor.zig");
const reactor_types = @import("reactor_types.zig");
const syscall = @import("../internal/syscall.zig");
const time_mod = @import("../time.zig");

const Reactor = reactor_mod.Reactor;
const EventKind = reactor_types.EventKind;
const ReactorError = reactor_types.ReactorError;

// ─────────────────────────────────────────────────────────────────────
// Test plumbing
// ─────────────────────────────────────────────────────────────────────

/// Wake context for the synchronous `wakeFn` callback signature shared
/// by every backend. We just capture which target was woken so the
/// test can verify identity round-trips through the kernel queue.
const WakeRecorder = struct {
    woken: std.ArrayList(*anyopaque) = .empty,

    fn deinit(self: *WakeRecorder, allocator: std.mem.Allocator) void {
        self.woken.deinit(allocator);
    }

    fn wakeStatic(opaque_self: *anyopaque, target: *anyopaque) anyerror!void {
        const self: *WakeRecorder = @ptrCast(@alignCast(opaque_self));
        // Ignore allocator failures here — the test will catch a missed
        // wake via length check below.
        self.woken.append(std.testing.allocator, target) catch {};
    }
};

/// Some backends (notably IOCP) need timer registration to be
/// implementable for these tests to be meaningful. Returns true when
/// the backend supports timer ops; false skips timer-specific tests.
fn timersSupported() bool {
    // The IOCP backend currently returns ReactorError.NotImplemented
    // from registerTimer until the Phase 2 Windows port. Every other
    // backend we have today supports timers.
    return builtin.os.tag != .windows;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "reactor conformance: init + deinit leaves pendingCount at 0" {
    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: register + fire + wake delivers original target" {
    // Pipe-readable: write a byte to the write end, register the read
    // end, poll, expect a wake on the registered target. This is the
    // exact protocol `volt.io.lowlevel.waitReadable` rides on.
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);

    // Sentinel that's pointer-stable for the whole test. The reactor
    // round-trips the address through the kernel queue's user_data /
    // udata / completion-key field; we verify identity here.
    var sentinel: usize = 0xfeedface;
    try r.registerWait(fds[0], .readable, @ptrCast(&sentinel));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    _ = try syscall.write(fds[1], "x");

    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    const woken_count = try r.poll(0, @ptrCast(&rec), &WakeRecorder.wakeStatic);

    try std.testing.expectEqual(@as(usize, 1), woken_count);
    try std.testing.expectEqual(@as(usize, 1), rec.woken.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&sentinel)), rec.woken.items[0]);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: unregisterWait without fire returns pendingCount to 0" {
    // Simulates the cancel-during-I/O path: `volt.io.wait*` registers,
    // then the coroutine is cancelled before the fd becomes ready, so
    // wait.zig calls unregisterWait. The kernel registration MUST be
    // torn down — without that, idle workers block in poll waiting
    // for an event nothing will deliver.
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);

    var sentinel: usize = 0xc0ffee;
    try r.registerWait(fds[0], .readable, @ptrCast(&sentinel));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    r.unregisterWait(fds[0], .readable);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());

    // The fd never becomes readable, but a non-blocking poll should
    // return immediately with no events. Use a 1ms timeout to confirm
    // the kernel has nothing left for us.
    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    const woken_count = try r.poll(1 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);
    try std.testing.expectEqual(@as(usize, 0), woken_count);
}

test "reactor conformance: registerTimer fires after duration" {
    if (!timersSupported()) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    var sentinel: usize = 0xdeadbeef;
    const id = try r.registerTimer(2 * std.time.ns_per_ms, @ptrCast(&sentinel));
    _ = id;
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    // Block in poll for up to 100ms — kernel should fire the 2ms
    // timer well within that.
    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    const woken_count = try r.poll(100 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);

    try std.testing.expectEqual(@as(usize, 1), woken_count);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&sentinel)), rec.woken.items[0]);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: unregisterTimer before fire returns pendingCount to 0" {
    // Same shape as the unregisterWait test, but for timers. This is
    // the path Sleep.zig hits on cancellation. Without correct
    // unregister, an hour-long sleep that gets cancelled wedges idle
    // workers in poll for an hour.
    if (!timersSupported()) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    var sentinel: usize = 0xabad1dea;
    // Long-enough timer that we won't race the kernel firing it.
    const id = try r.registerTimer(60 * std.time.ns_per_s, @ptrCast(&sentinel));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    r.unregisterTimer(id);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: multiple fds register + fire independently" {
    // Two pipes registered concurrently, only the first writer fires;
    // verifies the reactor delivers exactly one wake and identifies
    // the right target. Ensures backends correctly key wakes per
    // (fd, kind) and don't conflate registrations.
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const a = try syscall.pipe();
    defer syscall.close(a[0]);
    defer syscall.close(a[1]);
    const b = try syscall.pipe();
    defer syscall.close(b[0]);
    defer syscall.close(b[1]);

    var ta: usize = 0xa;
    var tb: usize = 0xb;
    try r.registerWait(a[0], .readable, @ptrCast(&ta));
    try r.registerWait(b[0], .readable, @ptrCast(&tb));
    try std.testing.expectEqual(@as(usize, 2), r.pendingCount());

    // Fire only `a`.
    _ = try syscall.write(a[1], "x");

    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    const n = try r.poll(50 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);

    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&ta)), rec.woken.items[0]);
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    // Drain `a` so we leave a clean slate for the deferred unregister.
    var sink: [1]u8 = undefined;
    _ = syscall.read(a[0], &sink) catch {};
    r.unregisterWait(b[0], .readable);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: register-cancel-register cycle on same fd" {
    // Common pattern: a coroutine waits on an fd, gets cancelled,
    // unwinds, and another coroutine (or the same one re-launching)
    // waits on the same fd again. The backend must accept the
    // re-registration without leaking the prior one.
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);

    var t1: usize = 0x1;
    try r.registerWait(fds[0], .readable, @ptrCast(&t1));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    r.unregisterWait(fds[0], .readable);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());

    // Re-register with a different target. Must succeed.
    var t2: usize = 0x2;
    try r.registerWait(fds[0], .readable, @ptrCast(&t2));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    // Fire it; the wake must arrive on `t2`, not `t1`.
    _ = try syscall.write(fds[1], "y");
    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    const n = try r.poll(50 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&t2)), rec.woken.items[0]);
}

test "reactor conformance: timer + wait interleaved fire independently" {
    // Mixed registration: a wait + a timer share the reactor.
    // Verifies the backend dispatches each completion to the right
    // target (no cross-contamination between timer and fd queues).
    if (!timersSupported()) return error.SkipZigTest;
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);

    var fd_target: usize = 0xffd;
    var timer_target: usize = 0x71;

    // 50ms timer — long enough that the fd wake fires first.
    _ = try r.registerTimer(50 * std.time.ns_per_ms, @ptrCast(&timer_target));
    try r.registerWait(fds[0], .readable, @ptrCast(&fd_target));
    try std.testing.expectEqual(@as(usize, 2), r.pendingCount());

    _ = try syscall.write(fds[1], "z");

    // Drive the reactor until both fire. We may need multiple poll
    // calls because some backends batch; at most 2 polls (one for
    // the wait, one for the timer).
    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);
    var attempts: u32 = 0;
    while (rec.woken.items.len < 2 and attempts < 5) : (attempts += 1) {
        _ = try r.poll(200 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);
    }

    try std.testing.expectEqual(@as(usize, 2), rec.woken.items.len);
    // Order is backend-dependent; just verify both targets fired.
    var saw_fd = false;
    var saw_timer = false;
    for (rec.woken.items) |target| {
        if (target == @as(*anyopaque, @ptrCast(&fd_target))) saw_fd = true;
        if (target == @as(*anyopaque, @ptrCast(&timer_target))) saw_timer = true;
    }
    try std.testing.expect(saw_fd);
    try std.testing.expect(saw_timer);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: pendingCount property — random register/unregister mix" {
    // Property test: after a random sequence of register / cancel /
    // fire operations, the reactor's pending counter must reflect
    // exactly the in-flight registrations. Stresses the bookkeeping
    // path that bug-class #4 (cancel-leak) lived in.
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    // Build a pool of pipes; each iteration picks a random pipe to
    // operate on. 16 pipes × 1000 iterations is fast (<1s) and
    // exercises slot-reuse paths in iouring/iocp backends.
    const N: usize = 16;
    var pipes: [N][2]std.posix.fd_t = undefined;
    for (&pipes) |*p| {
        p.* = try syscall.pipe();
    }
    defer for (pipes) |p| {
        syscall.close(p[0]);
        syscall.close(p[1]);
    };

    // Per-pipe registration state — null = not registered, non-null = target.
    var registered: [N]?*anyopaque = .{null} ** N;
    // Stable per-pipe sentinel addresses so identity round-trips through wake.
    var sentinels: [N]usize = blk: {
        var arr: [N]usize = undefined;
        for (&arr, 0..) |*s, i| s.* = 0x1000 + i;
        break :blk arr;
    };

    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    const rng = prng.random();

    const ITERATIONS: usize = 1000;
    var i: usize = 0;
    while (i < ITERATIONS) : (i += 1) {
        const pipe_idx = rng.uintLessThan(usize, N);
        const action = rng.uintLessThan(u8, 3);

        switch (action) {
            // Register if not already registered.
            0 => {
                if (registered[pipe_idx] == null) {
                    const target: *anyopaque = @ptrCast(&sentinels[pipe_idx]);
                    try r.registerWait(pipes[pipe_idx][0], .readable, target);
                    registered[pipe_idx] = target;
                }
            },
            // Unregister if registered.
            1 => {
                if (registered[pipe_idx] != null) {
                    r.unregisterWait(pipes[pipe_idx][0], .readable);
                    registered[pipe_idx] = null;
                }
            },
            // Fire (write) if registered. Drain via poll afterward.
            2 => {
                if (registered[pipe_idx] != null) {
                    _ = syscall.write(pipes[pipe_idx][1], "p") catch {};
                    var rec: WakeRecorder = .{};
                    defer rec.deinit(std.testing.allocator);
                    _ = try r.poll(20 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);
                    // Drain the byte so the next register on this pipe
                    // doesn't fire immediately on the still-readable fd.
                    var sink: [1]u8 = undefined;
                    _ = syscall.read(pipes[pipe_idx][0], &sink) catch {};
                    if (rec.woken.items.len > 0) registered[pipe_idx] = null;
                }
            },
            else => unreachable,
        }
    }

    // Final invariant: count of `registered != null` slots == pendingCount.
    var expected: usize = 0;
    for (registered) |reg| {
        if (reg != null) expected += 1;
    }
    try std.testing.expectEqual(expected, r.pendingCount());

    // Clean up: unregister whatever's left.
    for (registered, 0..) |reg, idx| {
        if (reg != null) r.unregisterWait(pipes[idx][0], .readable);
    }
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor conformance: tickle returns the polling thread immediately" {
    // Cross-thread wake. A polling thread blocks in poll; a tickle()
    // from another thread (or the same thread before poll) makes the
    // next poll call return promptly.
    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    // Tickle BEFORE poll so the wake is already pending. Real cross-
    // thread wakes work the same way — the wake races with poll and
    // either drains immediately or fires a wake event.
    r.tickle();

    var rec: WakeRecorder = .{};
    defer rec.deinit(std.testing.allocator);

    // 100ms timeout — the tickle should make this return effectively
    // instantly, well under 100ms even on a loaded CI box. We don't
    // assert on woken_count because tickle is a wake-only signal:
    // some backends (kqueue EVFILT_USER) deliver a recognizable
    // tickle event that poll filters; others (epoll eventfd, IOCP
    // sentinel key) drain it without surfacing a wake. Both are
    // valid — what matters is that poll returns.
    const t0 = time_mod.nanoTimestamp();
    _ = try r.poll(100 * std.time.ns_per_ms, @ptrCast(&rec), &WakeRecorder.wakeStatic);
    const elapsed = time_mod.nanoTimestamp() - t0;

    // Generous: anything under 50ms means tickle did its job.
    try std.testing.expect(elapsed < 50 * std.time.ns_per_ms);
}

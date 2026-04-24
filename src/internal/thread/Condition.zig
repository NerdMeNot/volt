//! Raw thread-level condition variable built on Volt's internal Futex.
//!
//! Simple epoch-based design: each signal/broadcast bumps a u32 epoch and
//! wakes waiters. Waiters sample the epoch before releasing the mutex; if
//! the epoch advances between sample and park, futex.wait returns
//! immediately (value mismatch). This closes the classic lost-wakeup race
//! without tracking waiter counts.
//!
//! Spurious wakes are possible — callers must use wait() inside a
//! while-predicate loop, matching std.Thread.Condition's contract.

const std = @import("std");
const Futex = @import("Futex.zig");
const Mutex = @import("Mutex.zig");

pub const TimedWaitError = error{Timeout};

const Condition = @This();

epoch: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

/// Atomically release `mutex`, wait for signal/broadcast, re-acquire `mutex`.
/// Spurious wakes are allowed — wrap in a predicate loop.
pub fn wait(cond: *Condition, mutex: *Mutex) void {
    const epoch = cond.epoch.load(.acquire);
    mutex.unlock();
    Futex.wait(&cond.epoch, epoch);
    mutex.lock();
}

/// Like wait, but returns error.Timeout if timeout_ns elapses.
/// The mutex is always re-acquired before returning (even on timeout).
pub fn timedWait(cond: *Condition, mutex: *Mutex, timeout_ns: u64) TimedWaitError!void {
    const epoch = cond.epoch.load(.acquire);
    mutex.unlock();
    const res = Futex.timedWait(&cond.epoch, epoch, timeout_ns);
    mutex.lock();
    return res;
}

/// Wake one waiter.
pub fn signal(cond: *Condition) void {
    _ = cond.epoch.fetchAdd(1, .release);
    Futex.wake(&cond.epoch, 1);
}

/// Wake all waiters.
pub fn broadcast(cond: *Condition) void {
    _ = cond.epoch.fetchAdd(1, .release);
    Futex.wake(&cond.epoch, std.math.maxInt(u32));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Condition - signal wakes waiter" {
    const builtin = @import("builtin");
    if (builtin.single_threaded) return error.SkipZigTest;

    const Shared = struct {
        mu: Mutex = .{},
        cv: Condition = .{},
        ready: bool = false,
    };
    var s = Shared{};

    const Worker = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            // Give the main thread time to enter cv.wait().
            @import("sleep.zig").sleep(20 * std.time.ns_per_ms);
            ctx.s.mu.lock();
            ctx.s.ready = true;
            ctx.s.mu.unlock();
            ctx.s.cv.signal();
        }
    };

    const t = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .s = &s }});
    defer t.join();

    s.mu.lock();
    while (!s.ready) s.cv.wait(&s.mu);
    s.mu.unlock();
}

test "Condition - broadcast wakes all waiters" {
    const builtin = @import("builtin");
    if (builtin.single_threaded) return error.SkipZigTest;

    const Shared = struct {
        mu: Mutex = .{},
        cv: Condition = .{},
        go: bool = false,
        woken: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
    };
    var s = Shared{};

    const Waiter = struct {
        s: *Shared,
        fn run(ctx: @This()) void {
            ctx.s.mu.lock();
            while (!ctx.s.go) ctx.s.cv.wait(&ctx.s.mu);
            ctx.s.mu.unlock();
            _ = ctx.s.woken.fetchAdd(1, .release);
        }
    };

    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Waiter.run, .{Waiter{ .s = &s }});
    }

    @import("sleep.zig").sleep(50 * std.time.ns_per_ms); // let waiters park

    s.mu.lock();
    s.go = true;
    s.mu.unlock();
    s.cv.broadcast();

    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(u8, 4), s.woken.load(.acquire));
}

test "Condition - timedWait returns Timeout" {
    const builtin = @import("builtin");
    if (builtin.single_threaded) return error.SkipZigTest;

    var mu: Mutex = .{};
    var cv: Condition = .{};

    mu.lock();
    try std.testing.expectError(error.Timeout, cv.timedWait(&mu, 5 * std.time.ns_per_ms));
    mu.unlock();
}

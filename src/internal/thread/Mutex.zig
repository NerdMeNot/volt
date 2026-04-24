//! Raw thread-level mutex built on Volt's internal Futex.
//!
//! 3-state CAS + futex design (unlocked / locked / contended) — same as
//! std.Io.Mutex's state machine, minus the Io parameter. This is the primitive
//! Volt uses where std.Thread.Mutex used to sit on Zig 0.15.
//!
//! The fast path is uncontended: single CAS to acquire, single store to release.
//! Only when contention is observed do we hit the futex syscall.

const std = @import("std");
const Futex = @import("Futex.zig");

const State = enum(u32) {
    unlocked,
    locked,
    contended,
};

const Mutex = @This();

state: std.atomic.Value(State) = std.atomic.Value(State).init(.unlocked),

pub fn tryLock(m: *Mutex) bool {
    return m.state.cmpxchgStrong(.unlocked, .locked, .acquire, .monotonic) == null;
}

pub fn lock(m: *Mutex) void {
    // Fast path — a single CAS wins the common uncontended case.
    if (m.state.cmpxchgStrong(.unlocked, .locked, .acquire, .monotonic) == null) {
        @branchHint(.likely);
        return;
    }
    lockSlow(m);
}

fn lockSlow(m: *Mutex) void {
    @branchHint(.cold);
    // Walk the state machine: if currently .contended, wait immediately.
    // Otherwise, swap to .contended; if the previous state was .unlocked, we got
    // the lock and return. If not, wait. Repeat until we acquire.
    //
    // The swap-to-contended is what ensures the unlocker knows to call wake():
    // unlock() checks for .contended and issues a wake then (and only then).
    while (true) {
        const prev = m.state.swap(.contended, .acquire);
        if (prev == .unlocked) return;
        Futex.wait(@ptrCast(&m.state.raw), @intFromEnum(State.contended));
    }
}

pub fn unlock(m: *Mutex) void {
    // If state was .contended, wake exactly one waiter. Otherwise just release.
    const prev = m.state.swap(.unlocked, .release);
    std.debug.assert(prev != .unlocked);
    if (prev == .contended) {
        Futex.wake(@ptrCast(&m.state.raw), 1);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Mutex - tryLock uncontended" {
    var m: Mutex = .{};
    try std.testing.expect(m.tryLock());
    try std.testing.expect(!m.tryLock());
    m.unlock();
    try std.testing.expect(m.tryLock());
    m.unlock();
}

test "Mutex - lock/unlock uncontended" {
    var m: Mutex = .{};
    m.lock();
    m.unlock();
    m.lock();
    m.unlock();
}

test "Mutex - contended lock across threads" {
    const builtin = @import("builtin");
    if (builtin.single_threaded) return error.SkipZigTest;

    const Counter = struct {
        mu: Mutex = .{},
        value: u64 = 0,
    };
    const Ctx = struct {
        c: *Counter,
        iters: u64,
        fn run(ctx: @This()) void {
            var i: u64 = 0;
            while (i < ctx.iters) : (i += 1) {
                ctx.c.mu.lock();
                ctx.c.value += 1;
                ctx.c.mu.unlock();
            }
        }
    };

    var counter = Counter{};
    const iters_per_thread: u64 = 10_000;
    const num_threads: u8 = 8;

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .c = &counter, .iters = iters_per_thread }});
    }
    for (threads) |t| t.join();

    try std.testing.expectEqual(
        @as(u64, iters_per_thread) * num_threads,
        counter.value,
    );
}

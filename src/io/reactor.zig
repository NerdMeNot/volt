//! Reactor: bridges OS readiness notifications to coroutine wake-ups.
//!
//! Darwin/BSD: kqueue. Linux: epoll (added later). io_uring: v0.9.
//!
//! v0.2 design:
//!   - One reactor per Runtime.
//!   - Coroutines call `registerWait(fd, kind)` after a non-blocking syscall
//!     returned `WouldBlock`, then `parkCurrent()` to suspend.
//!   - Reactor.poll() is invoked by the scheduler when the ready queue is
//!     empty AND there are pending waits — it blocks on kevent, then unparks
//!     the coroutines tied to whichever events fired.
//!   - One-shot semantics (EV_ONESHOT): each registration fires once and is
//!     auto-removed by the kernel. Avoids the level-vs-edge gotchas and the
//!     bookkeeping of tracking which fd-filters are still armed.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const system = posix.system;

const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const syscall = @import("../internal/syscall.zig");
const Mutex = @import("../internal/thread.zig").Mutex;

comptime {
    if (builtin.os.tag != .macos and builtin.os.tag != .ios and
        builtin.os.tag != .freebsd and builtin.os.tag != .netbsd and
        builtin.os.tag != .dragonfly)
    {
        @compileError("reactor.zig is currently kqueue-only — Linux/epoll lands later in v0.2");
    }
}

pub const EventKind = enum(u8) {
    readable,
    writable,
};

fn filterFor(kind: EventKind) i16 {
    return switch (kind) {
        .readable => system.EVFILT.READ,
        .writable => system.EVFILT.WRITE,
    };
}

fn kindFor(filter: i16) EventKind {
    return switch (filter) {
        system.EVFILT.READ => .readable,
        system.EVFILT.WRITE => .writable,
        else => unreachable,
    };
}

pub const Reactor = struct {
    kq: i32,
    /// Currently parked coroutines, keyed by (fd, kind). Used to detect
    /// whether any wait is outstanding and as a debug aid; on event delivery
    /// the kevent's `udata` field carries the *Coroutine pointer directly,
    /// so this map is not on the wake path.
    ///
    /// Protected by `mutex` since `registerWait` and `poll` can run on
    /// different worker threads in v0.3+.
    waiters: std.AutoHashMap(WaitKey, void),
    /// Atomic count cache so `pendingCount()` doesn't have to take the mutex
    /// on the worker idle path.
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    mutex: Mutex = .{},
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        const kq = try syscall.kqueue();
        errdefer syscall.close(kq);
        return .{
            .kq = kq,
            .waiters = std.AutoHashMap(WaitKey, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        syscall.close(self.kq);
        self.waiters.deinit();
    }

    /// Lock-free read of pending count. May be slightly stale wrt to a
    /// concurrent register/poll on another thread, but that's fine for the
    /// "should I park?" decision — the parker itself re-checks under lock.
    pub fn pendingCount(self: *const Reactor) usize {
        return self.pending.load(.acquire);
    }

    /// Arm a one-shot wait for (fd, kind) and associate it with `coro`.
    /// The caller must subsequently park `coro` — the reactor does not
    /// touch coroutine state itself, only delivers wake events.
    pub fn registerWait(self: *Reactor, fd: posix.fd_t, kind: EventKind, coro: *Coroutine) !void {
        const key = WaitKey{ .fd = @intCast(fd), .kind_tag = @intFromEnum(kind) };

        const ev = posix.Kevent{
            .ident = @intCast(fd),
            .filter = filterFor(kind),
            .flags = system.EV.ADD | system.EV.ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(coro),
        };

        // We arm the kevent BEFORE inserting into the waiters map: if the
        // map insert fails (OOM), we must clean up the kevent registration.
        const changes = [_]posix.Kevent{ev};
        var dummy: [0]posix.Kevent = undefined;
        _ = try syscall.kevent(self.kq, &changes, &dummy, null);

        self.mutex.lock();
        defer self.mutex.unlock();
        std.debug.assert(!self.waiters.contains(key));
        self.waiters.put(key, {}) catch |err| {
            // Best-effort: tear down the kevent we just armed.
            const remove_ev = posix.Kevent{
                .ident = @intCast(fd),
                .filter = filterFor(kind),
                .flags = system.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            };
            const remove_changes = [_]posix.Kevent{remove_ev};
            _ = syscall.kevent(self.kq, &remove_changes, &dummy, null) catch {};
            return err;
        };
        _ = self.pending.fetchAdd(1, .release);
    }

    /// Block on kevent for up to `timeout_ns` (or forever if null), unpark
    /// each coro whose event fires, and return how many were woken.
    ///
    /// Concurrency: only one worker should be inside `poll` at a time —
    /// callers coordinate via `Runtime.tryClaimReactorPoll`. The wake
    /// callback may be invoked many times before `poll` returns; callbacks
    /// must be quick (the reactor lock is released before each callback).
    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *Coroutine) anyerror!void,
    ) !usize {
        var events: [64]posix.Kevent = undefined;
        const ts: ?posix.timespec = if (timeout_ns) |ns| .{
            .sec = @intCast(@divTrunc(ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(ns, std.time.ns_per_s)),
        } else null;

        const changes: []const posix.Kevent = &.{};
        const n = try syscall.kevent(
            self.kq,
            changes,
            &events,
            if (ts) |*t| t else null,
        );

        for (events[0..n]) |ev| {
            const coro: *Coroutine = @ptrFromInt(ev.udata);
            const key = WaitKey{
                .fd = @intCast(ev.ident),
                .kind_tag = @intFromEnum(kindFor(ev.filter)),
            };
            self.mutex.lock();
            const removed = self.waiters.remove(key);
            self.mutex.unlock();
            if (removed) _ = self.pending.fetchSub(1, .release);
            try wakeFn(wake_ctx, coro);
        }
        return n;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "reactor: init/deinit" {
    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

test "reactor: pipe readable wake" {
    var r = try Reactor.init(std.testing.allocator);
    defer r.deinit();

    const fds = try syscall.pipe();
    defer syscall.close(fds[0]);
    defer syscall.close(fds[1]);

    // Sentinel coro pointer — we won't touch *Coroutine fields, just verify
    // identity round-trips through udata.
    var fake_coro_storage: usize = 0xdeadbeef;
    const fake_coro: *Coroutine = @ptrFromInt(@intFromPtr(&fake_coro_storage));

    try r.registerWait(fds[0], .readable, fake_coro);
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    // Write to the pipe so the read end becomes readable.
    _ = try syscall.write(fds[1], "x");

    const Ctx = struct {
        woke: ?*Coroutine = null,
        fn wake(opaque_self: *anyopaque, coro: *Coroutine) anyerror!void {
            const s: *@This() = @ptrCast(@alignCast(opaque_self));
            s.woke = coro;
        }
    };
    var ctx: Ctx = .{};

    const n = try r.poll(0, &ctx, &Ctx.wake);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(fake_coro, ctx.woke.?);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

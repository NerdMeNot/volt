//! Darwin/BSD reactor backend — kqueue + EVFILT_TIMER + EV_USER.
//!
//! See `reactor.zig` for the platform dispatcher and the public
//! protocol shared by all backends.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const system = posix.system;

const syscall = @import("../internal/syscall.zig");
const Mutex = @import("../internal/thread.zig").Mutex;

comptime {
    const ok = switch (builtin.os.tag) {
        .macos, .ios, .freebsd, .netbsd, .dragonfly, .openbsd => true,
        else => false,
    };
    if (!ok) @compileError("reactor_kqueue.zig is for Darwin/BSD only");
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

/// Sentinel `udata` value used by the wake-tickle path. Any pointer that
/// won't collide with a real *Coroutine works — we use 1, since coroutines
/// are page-aligned on the heap and never have address 1.
const TICKLE_UDATA: usize = 1;

pub const Reactor = struct {
    kq: i32,
    /// Currently parked coroutines, keyed by (fd, kind). Used to detect
    /// whether any wait is outstanding and as a debug aid; on event delivery
    /// the kevent's `udata` field carries the *Coroutine pointer directly,
    /// so this map is not on the wake path.
    ///
    /// Protected by `mutex` since `registerWait` and `poll` can run on
    /// different worker threads.
    waiters: std.AutoHashMap(WaitKey, void),
    /// Atomic count cache so `pendingCount()` doesn't have to take the mutex
    /// on the worker idle path.
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    mutex: Mutex = .{},
    allocator: std.mem.Allocator,
    /// Atomic counter for unique timer IDs. EVFILT_TIMER's `ident` is
    /// just a tag (not an fd); we need uniqueness across registrations.
    /// Start at `1 << 32` so timer idents never collide with fd values.
    timer_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(1 << 32),

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        const kq = try syscall.kqueue();
        errdefer syscall.close(kq);

        // Register a persistent EV_USER event for instant wake-up. Other
        // threads call `tickle()` to NOTE_TRIGGER it, which immediately
        // returns the polling worker from kevent. This eliminates the
        // 100ms reactor poll timeout for new-work / shutdown latency.
        const tickle_ev = posix.Kevent{
            .ident = 0,
            .filter = system.EVFILT.USER,
            .flags = system.EV.ADD | system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = TICKLE_UDATA,
        };
        const changes = [_]posix.Kevent{tickle_ev};
        var dummy: [0]posix.Kevent = undefined;
        _ = try syscall.kevent(kq, &changes, &dummy, null);

        return .{
            .kq = kq,
            .waiters = std.AutoHashMap(WaitKey, void).init(allocator),
            .allocator = allocator,
        };
    }

    /// NOTE_TRIGGER the EV_USER tickle event so any thread blocked in
    /// `poll()` returns immediately. Safe to call from any thread.
    pub fn tickle(self: *Reactor) void {
        const ev = posix.Kevent{
            .ident = 0,
            .filter = system.EVFILT.USER,
            .flags = 0,
            .fflags = system.NOTE.TRIGGER,
            .data = 0,
            .udata = TICKLE_UDATA,
        };
        const changes = [_]posix.Kevent{ev};
        var dummy: [0]posix.Kevent = undefined;
        _ = syscall.kevent(self.kq, &changes, &dummy, null) catch {};
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

    /// Arm a one-shot timer that fires after `duration_ns` nanoseconds.
    /// When it fires, the reactor invokes `wakeFn(wake_ctx, target)` —
    /// same dispatch shape as fd-based waits.
    ///
    /// Implementation: kqueue's EVFILT_TIMER with EV_ONESHOT + NOTE_NSECONDS.
    /// `ident` is a per-reactor unique counter (NOT an fd).
    pub fn registerTimer(self: *Reactor, duration_ns: u64, target: *anyopaque) !void {
        const id = self.timer_id.fetchAdd(1, .monotonic);
        const ev = posix.Kevent{
            .ident = @intCast(id),
            .filter = system.EVFILT.TIMER,
            .flags = system.EV.ADD | system.EV.ONESHOT,
            .fflags = system.NOTE.NSECONDS,
            .data = @intCast(duration_ns),
            .udata = @intFromPtr(target),
        };
        const changes = [_]posix.Kevent{ev};
        var dummy: [0]posix.Kevent = undefined;
        _ = try syscall.kevent(self.kq, &changes, &dummy, null);
        _ = self.pending.fetchAdd(1, .release);
    }

    /// Arm a one-shot wait for (fd, kind). The `target` pointer is opaque
    /// to the reactor — it's stored in the kevent's `udata` field and
    /// passed back to the wake callback when the event fires. Typically
    /// a `*Park` (see `io/wait.zig`).
    pub fn registerWait(self: *Reactor, fd: posix.fd_t, kind: EventKind, target: *anyopaque) !void {
        const key = WaitKey{ .fd = @intCast(fd), .kind_tag = @intFromEnum(kind) };

        const ev = posix.Kevent{
            .ident = @intCast(fd),
            .filter = filterFor(kind),
            .flags = system.EV.ADD | system.EV.ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(target),
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

    /// Block on kevent for up to `timeout_ns` (or forever if null), call
    /// `wakeFn(wake_ctx, target)` for each fired event, and return how
    /// many events were delivered (excluding the EV_USER tickle).
    ///
    /// Concurrency: only one worker should be inside `poll` at a time —
    /// callers coordinate via `Runtime.tryClaimReactorPoll`.
    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
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

        var woken: usize = 0;
        for (events[0..n]) |ev| {
            if (ev.udata == TICKLE_UDATA) continue;
            const target: *anyopaque = @ptrFromInt(ev.udata);
            if (ev.filter == system.EVFILT.TIMER) {
                // Timer events aren't in the waiters map (no fd key).
                // EV_ONESHOT ensures kernel auto-removes the kevent.
                _ = self.pending.fetchSub(1, .release);
                try wakeFn(wake_ctx, target);
                woken += 1;
                continue;
            }
            const key = WaitKey{
                .fd = @intCast(ev.ident),
                .kind_tag = @intFromEnum(kindFor(ev.filter)),
            };
            self.mutex.lock();
            const removed = self.waiters.remove(key);
            self.mutex.unlock();
            if (removed) _ = self.pending.fetchSub(1, .release);
            try wakeFn(wake_ctx, target);
            woken += 1;
        }
        return woken;
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

    // Sentinel target — verify identity round-trips through kevent's udata.
    var sentinel: usize = 0xdeadbeef;
    try r.registerWait(fds[0], .readable, @ptrCast(&sentinel));
    try std.testing.expectEqual(@as(usize, 1), r.pendingCount());

    _ = try syscall.write(fds[1], "x");

    const Ctx = struct {
        woke: ?*anyopaque = null,
        fn wake(opaque_self: *anyopaque, target: *anyopaque) anyerror!void {
            const s: *@This() = @ptrCast(@alignCast(opaque_self));
            s.woke = target;
        }
    };
    var ctx: Ctx = .{};

    const n = try r.poll(0, &ctx, &Ctx.wake);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(&sentinel)), ctx.woke.?);
    try std.testing.expectEqual(@as(usize, 0), r.pendingCount());
}

//! Linux epoll reactor.
//!
//! Mirror of `reactor_kqueue.zig`'s design, swapping the syscall
//! family from kqueue → epoll and `EVFILT_TIMER` → `timerfd_create`.
//! `EPOLLONESHOT` matches kqueue's `EV_ONESHOT` semantics — each
//! `waitReadable`/`waitWritable` registration auto-disables after
//! one wake — so the dispatch interface is identical across both
//! platforms.
//!
//! ## Design choices (locked in via the L2a plan)
//!
//! - **`EPOLLONESHOT` + level-triggered**, not `EPOLLET`. Avoids
//!   the drain-until-EAGAIN loop that edge-triggered demands; each
//!   waiter registers fresh per call, same shape as kqueue's
//!   `EV_ONESHOT`. mio uses `EPOLLET`; we don't, because Volt's
//!   I/O helpers (`readAsync` / `writeAsync`) issue one syscall per
//!   loop iteration and we want a clean re-register on each EAGAIN.
//! - **`epoll_data.ptr` is the coroutine pointer** — exact analog
//!   of kqueue's `udata = *Coroutine`. The poll loop casts back and
//!   unparks via `runtime.unpark`.
//! - **Timers via `timerfd`**: each `volt.sleep(ns)` creates a
//!   one-shot `timerfd_create(CLOCK_MONOTONIC, ...)`, registers it
//!   in the same epoll fd with `EPOLLIN | EPOLLONESHOT`, parks the
//!   coroutine, and closes the timerfd on return. The create+close
//!   pair costs ~1 µs on Linux; for sleeps of any meaningful
//!   duration that's a rounding error. A per-P timerfd pool is the
//!   natural optimisation if profiling shows the syscall overhead
//!   matters at high sleep rates — deferred until measured.
//! - **`pending` atomic** identical to kqueue's. Same acquire/release
//!   ordering, same dispatcher gate.
//!
//! ## What's deliberately not here
//!
//! - No `signalfd` integration. Volt's user-facing signal handling
//!   lives outside the runtime.
//! - No `eventfd` for cross-thread wakeups. The parking lot owns
//!   inter-worker coordination; the reactor is purely an I/O / timer
//!   waker.
//! - No `EPOLLEXCLUSIVE` thundering-herd protection. The single-
//!   poller-claim in `runtime.zig` already serialises poll calls.

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");

const EPOLL_EVENTS_BATCH: usize = 32;

// ─── epoll syscalls ──────────────────────────────────────────────

// Flags / constants. Values are the canonical Linux ones; verified
// against `/usr/include/sys/epoll.h` and `bits/epoll.h`.
const EPOLL_CLOEXEC: c_int = 0o2000000;
const EPOLL_CTL_ADD: c_int = 1;
const EPOLL_CTL_DEL: c_int = 2;
const EPOLL_CTL_MOD: c_int = 3;
const EPOLLIN: u32 = 0x001;
const EPOLLOUT: u32 = 0x004;
const EPOLLONESHOT: u32 = 1 << 30;

// epoll_event is `__attribute__ ((packed))` on x86_64 for
// 32-bit-compat reasons. `align(1)` matches the C layout.
const epoll_event = extern struct {
    events: u32,
    data: u64, // we stash the coroutine pointer here
};

extern "c" fn epoll_create1(flags: c_int) c_int;
extern "c" fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: ?*epoll_event) c_int;
extern "c" fn epoll_wait(epfd: c_int, events: [*]epoll_event, maxevents: c_int, timeout: c_int) c_int;

// ─── timerfd syscalls ────────────────────────────────────────────

const TFD_NONBLOCK: c_int = 0o4000; // O_NONBLOCK
const TFD_CLOEXEC: c_int = 0o2000000;
const CLOCK_MONOTONIC: c_int = 1;

const timespec = extern struct {
    sec: i64,
    nsec: i64,
};
const itimerspec = extern struct {
    it_interval: timespec,
    it_value: timespec,
};

extern "c" fn timerfd_create(clockid: c_int, flags: c_int) c_int;
extern "c" fn timerfd_settime(fd: c_int, flags: c_int, new_value: *const itimerspec, old_value: ?*itimerspec) c_int;

// ─── generic libc ────────────────────────────────────────────────

extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
extern "c" fn close(fd: c_int) c_int;
extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;
extern "c" fn __errno_location() *c_int;

inline fn errnoVal() c_int {
    return __errno_location().*;
}

const EAGAIN: c_int = 11;
const EEXIST: c_int = 17;

inline fn isAgain(e: c_int) bool {
    return e == EAGAIN;
}

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    epfd: c_int = -1,

    /// In-flight epoll registrations (one per coroutine parked on
    /// an fd or timer). Read by every dispatcher to decide whether
    /// to claim the poller role; same role as kqueue's `pending`.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init() !Reactor {
        const ep = epoll_create1(EPOLL_CLOEXEC);
        if (ep < 0) return error.EpollCreateFailed;
        return .{ .epfd = ep };
    }

    pub fn deinit(self: *Reactor) void {
        if (self.epfd >= 0) _ = close(self.epfd);
        self.epfd = -1;
    }

    pub fn waitReadable(self: *Reactor, fd: i32) void {
        self.waitFd(@intCast(fd), EPOLLIN);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) void {
        self.waitFd(@intCast(fd), EPOLLOUT);
    }

    fn waitFd(self: *Reactor, fd: c_int, filter: u32) void {
        const me = current.require();
        var ev = epoll_event{
            .events = filter | EPOLLONESHOT,
            .data = @intFromPtr(me),
        };
        // `EPOLLONESHOT` auto-removes the registration after one
        // wake. On the first wait for an fd: ADD succeeds. On
        // subsequent waits (same fd, re-armed): MOD is needed.
        // Try ADD first; on EEXIST fall back to MOD.
        var rc = epoll_ctl(self.epfd, EPOLL_CTL_ADD, fd, &ev);
        if (rc < 0 and errnoVal() == EEXIST) {
            rc = epoll_ctl(self.epfd, EPOLL_CTL_MOD, fd, &ev);
        }
        if (rc < 0) @panic("epoll_ctl(ADD/MOD) failed");
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
        // Resume: pending decremented by poll() before unpark.
    }

    pub fn waitTimer(self: *Reactor, ns: u64) void {
        const me = current.require();

        // Create a one-shot timerfd. Closed before this function
        // returns — no fd leak. A per-P pool of pre-created
        // timerfds is the natural optimisation if `bench-sleep`-
        // style workloads show the syscall overhead; deferred.
        const tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd < 0) @panic("timerfd_create failed");

        const spec = itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            },
        };
        if (timerfd_settime(tfd, 0, &spec, null) < 0) @panic("timerfd_settime failed");

        var ev = epoll_event{
            .events = EPOLLIN | EPOLLONESHOT,
            .data = @intFromPtr(me),
        };
        if (epoll_ctl(self.epfd, EPOLL_CTL_ADD, tfd, &ev) < 0) @panic("epoll_ctl(timerfd) failed");

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        // Drain the 8-byte expiration count so the timerfd isn't
        // sitting in "readable" state. EPOLLONESHOT already
        // disabled the registration, but reading defensively
        // matches semantics across kernels.
        var buf: [8]u8 = undefined;
        _ = read(tfd, &buf, 8);
        _ = close(tfd);
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;
        var events: [EPOLL_EVENTS_BATCH]epoll_event = undefined;
        const timeout_ms: c_int = if (blocking) -1 else 0;
        const n = epoll_wait(self.epfd, &events, EPOLL_EVENTS_BATCH, timeout_ms);
        if (n <= 0) return 0;
        const count: usize = @intCast(n);
        _ = self.pending.fetchSub(@intCast(count), .acq_rel);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const coro: *coroutine.Coroutine = @ptrFromInt(events[i].data);
            runtime.unpark(coro);
        }
        return count;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Non-blocking IO helpers (parallel to reactor_kqueue.zig's)
// ─────────────────────────────────────────────────────────────────────

const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const O_NONBLOCK: c_int = 0o4000;

pub fn setNonblock(fd: i32) !void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) !usize {
    while (true) {
        const r = read(@intCast(fd), buf.ptr, buf.len);
        if (r >= 0) return @intCast(r);
        const e = errnoVal();
        if (!isAgain(e)) return error.ReadFailed;
        rx.waitReadable(fd);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) !usize {
    while (true) {
        const w = write(@intCast(fd), buf.ptr, buf.len);
        if (w >= 0) return @intCast(w);
        const e = errnoVal();
        if (!isAgain(e)) return error.WriteFailed;
        rx.waitWritable(fd);
    }
}

/// Read EXACTLY `buf.len` bytes (loop until EOF or full). Returns the
/// total bytes read (which may be < buf.len if EOF hit).
pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

/// Write EXACTLY `buf.len` bytes (loop on partial writes).
pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, buf[total..]);
        total += w;
    }
}

// Compile-time check: this file is only meaningful on Linux.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_epoll.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

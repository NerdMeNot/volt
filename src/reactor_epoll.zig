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

// `epoll_event` layout differs by arch: the kernel header uses
// `__attribute__((packed))` on x86_64 (12 bytes) and natural
// alignment everywhere else (16 bytes, with 4 padding bytes
// between `events` and `data`). The Zig struct must match the
// kernel's per-arch layout exactly — otherwise epoll_wait writes
// N×kernelSize bytes into our buffer but we read N×zigSize-byte
// strides, and every event past index 0 reads garbage.
//
// Comptime assertion below pins the layout against the kernel
// per arch so any drift fails the build, not silently at runtime.
const epoll_event = if (builtin.target.cpu.arch == .x86_64)
    extern struct {
        events: u32 align(1),
        data: u64 align(1),
    }
else
    extern struct {
        events: u32,
        data: u64,
    };
comptime {
    const expected: usize = if (builtin.target.cpu.arch == .x86_64) 12 else 16;
    if (@sizeOf(epoll_event) != expected) {
        @compileError("epoll_event size mismatch vs Linux kernel layout");
    }
}

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

const posix_helpers = @import("reactor_posix.zig");
const read = posix_helpers.read;
const close = posix_helpers.close;
const errnoVal = posix_helpers.errnoVal;

// Re-exported as the shim's public surface. setNonblock /
// readAsync / writeAsync / readFull / writeAll are identical
// across kqueue / epoll / io_uring.
pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

// EEXIST is specific to epoll_ctl's ADD-vs-MOD branching, so it
// stays here rather than in the shared POSIX module.
const EEXIST: c_int = 17;

const ReactorWaitError = posix_helpers.ReactorWaitError;

/// Map an `epoll_ctl` / `timerfd_*` errno into the categorical
/// `ReactorWaitError` set. Resource-exhaustion cases (EMFILE / ENFILE
/// / ENOMEM) are surfaced explicitly so libraries can decide
/// back-off; EBADF means the fd was closed under us.
fn registerError(e: c_int) ReactorWaitError {
    return switch (e) {
        posix_helpers.Errno.EBADF, posix_helpers.Errno.ENOTSOCK => error.BadDescriptor,
        posix_helpers.Errno.EMFILE, posix_helpers.Errno.ENFILE => error.OutOfDescriptors,
        posix_helpers.Errno.ENOMEM, posix_helpers.Errno.ENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
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

    pub fn waitReadable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(@intCast(fd), EPOLLIN);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(@intCast(fd), EPOLLOUT);
    }

    fn waitFd(self: *Reactor, fd: c_int, filter: u32) ReactorWaitError!void {
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
        if (rc < 0) return registerError(errnoVal());
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
        // Resume: pending decremented by poll() before unpark.
    }

    pub fn waitTimer(self: *Reactor, ns: u64) ReactorWaitError!void {
        const me = current.require();

        // Create a one-shot timerfd. Closed before this function
        // returns — no fd leak. A per-P pool of pre-created
        // timerfds is the natural optimisation if `bench-sleep`-
        // style workloads show the syscall overhead; deferred.
        const tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd < 0) return registerError(errnoVal());
        errdefer _ = close(tfd);

        const spec = itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            },
        };
        if (timerfd_settime(tfd, 0, &spec, null) < 0) return registerError(errnoVal());

        var ev = epoll_event{
            .events = EPOLLIN | EPOLLONESHOT,
            .data = @intFromPtr(me),
        };
        if (epoll_ctl(self.epfd, EPOLL_CTL_ADD, tfd, &ev) < 0) return registerError(errnoVal());

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

// Compile-time check: this file is only meaningful on Linux.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_epoll.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

//! Linux epoll reactor — **persistent registration model**.
//!
//! Each socket's `PollDesc` is registered ONCE at socket creation
//! (`registerFd`) via `EPOLL_CTL_ADD` with the mask
//! `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET`. The `*PollDesc`
//! pointer rides in `epoll_event.data` (tagged so the poll loop can
//! distinguish PD events from per-call timer events). The kernel
//! registration lives for the socket's lifetime; it's torn down by
//! `epoll_ctl(DEL)` in `unregisterFd`.
//!
//! `waitFd(fd, pd, mode)`:
//!   1. `pd.incref()` / `defer pd.decref()` (PD lifecycle ref).
//!   2. `pd.wait(mode)` — pure user-space state machine.
//!   3. On `.ready`, return; caller retries the syscall.
//!   4. On `.closing`, return `error.BadDescriptor`.
//!
//! `poll(blocking)`:
//!   1. `epoll_wait` drains ready events into a batch.
//!   2. For each event, dispatch by `data`:
//!      - `INTERRUPT_DATA` (sentinel) — drain eventfd, skip.
//!      - tagged PD ptr — parse events bitmap, call `pd.deliverReady`
//!        for each direction that fired.
//!      - untagged ptr — timer CQE; unpark the coroutine directly.
//!
//! ## Why edge-triggered (`EPOLLET`)
//!
//! Matches Go's `netpoll_epoll.go` (it uses
//! `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET`). The wake-and-retry
//! pattern in our I/O helpers (`readAsync` / `writeAsync` loop on
//! EAGAIN) cooperates with edge-triggered semantics correctly: the
//! user's read syscall sees the level state directly, returning data
//! while it's available and EAGAIN only when the kernel buffer is
//! empty. We only enter `pd.wait` on EAGAIN, by which point the
//! kernel state has actually transitioned to "not readable" — so
//! the next ET event (on the next arrival) will fire.
//!
//! ## Why this design closes the legacy races
//!
//! In the previous per-wait registration model (`Step 2b` shim), a
//! close racing with a register opened a "kernel-accepts-ADD-on-just-
//! closed-fd" window and an orphaned coroutine. The persistent
//! registration eliminates this by construction: the registration
//! happens at socket open, before any peer can see the fd. Cancel
//! is purely user-space (`pd.waitCancel` runs the state machine),
//! so it doesn't touch the kernel either.
//!
//! Timer paths (`waitTimer` / `waitTimerCancel`) keep per-call
//! `timerfd_create` + `epoll_ctl(ADD)` since timers are inherently
//! single-shot. `cancelCoro` + the `WaitOp` stash are timer-only in
//! this model.

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const cancel_mod = @import("cancel.zig");
const poll_desc = @import("poll_desc.zig");

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
const EPOLLERR: u32 = 0x008;
const EPOLLHUP: u32 = 0x010;
const EPOLLRDHUP: u32 = 0x2000;
const EPOLLONESHOT: u32 = 1 << 30;
const EPOLLET: u32 = 1 << 31;

// `epoll_event` layout differs by arch: the kernel header uses
// `__attribute__((packed))` on x86_64 (12 bytes) and natural
// alignment everywhere else (16 bytes, with 4 padding bytes
// between `events` and `data`). The Zig struct must match the
// kernel's per-arch layout exactly — otherwise epoll_wait writes
// N×kernelSize bytes into our buffer but we read N×zigSize-byte
// strides, and every event past index 0 reads garbage.
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

// ─── eventfd (interrupt mechanism) ───────────────────────────────

const EFD_NONBLOCK: c_int = 0o4000; // O_NONBLOCK
const EFD_CLOEXEC: c_int = 0o2000000;
extern "c" fn eventfd(initval: c_uint, flags: c_int) c_int;

// ─── timerfd syscalls ────────────────────────────────────────────

const TFD_NONBLOCK: c_int = 0o4000;
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
const reactor_fs = @import("reactor_fs.zig");
const read = posix_helpers.read;
const close = posix_helpers.close;
const errnoVal = posix_helpers.errnoVal;

pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

const ReactorWaitError = posix_helpers.ReactorWaitError;

/// Map `epoll_ctl` / `timerfd_*` errno into the categorical
/// `ReactorWaitError` set.
fn registerError(e: c_int) ReactorWaitError {
    return switch (e) {
        posix_helpers.Errno.EBADF, posix_helpers.Errno.ENOTSOCK => error.BadDescriptor,
        posix_helpers.Errno.EMFILE, posix_helpers.Errno.ENFILE => error.OutOfDescriptors,
        posix_helpers.Errno.ENOMEM, posix_helpers.Errno.ENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

// ─── data encoding ───────────────────────────────────────────────
//
// `epoll_event.data` is a u64 carrying any one of three things:
//
//   * `INTERRUPT_DATA` (0) — the cross-thread interrupt eventfd.
//   * `pd | POLL_DESC_TAG` — a persistent PD registration on a
//     network/socket fd.
//   * raw coroutine pointer — a per-call timerfd registration.
//
// `POLL_DESC_TAG` picks bit 2 (= 0x4). Coroutine and PollDesc heap
// allocations are both ≥ 8-byte aligned (Volt's allocator default
// gives 8; `PollDesc` is `align(std.atomic.cache_line)` so ≥ 64),
// so bit 2 is naturally 0 in their addresses. Setting bit 2 on PD
// events makes them trivially distinguishable from timer events
// without consulting any side state.

const INTERRUPT_DATA: u64 = 0;
const POLL_DESC_TAG: u64 = 0x4;

inline fn dataForPd(pd: *poll_desc.PollDesc) u64 {
    const raw: u64 = @intFromPtr(pd);
    std.debug.assert(raw & POLL_DESC_TAG == 0);
    return raw | POLL_DESC_TAG;
}

inline fn pdFromData(data: u64) *poll_desc.PollDesc {
    return @ptrFromInt(data & ~POLL_DESC_TAG);
}

inline fn isPdData(data: u64) bool {
    return data != INTERRUPT_DATA and (data & POLL_DESC_TAG) != 0;
}

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    epfd: c_int = -1,
    /// eventfd registered with the epoll instance for cross-thread
    /// wakeups (`interrupt`). Lives for the reactor's lifetime; the
    /// epoll registration is level-triggered (no `EPOLLET`) so a
    /// single ADD covers every subsequent interrupt.
    interrupt_fd: c_int = -1,

    /// In-flight kernel registrations:
    /// - One per `registerFd` (persistent socket registration). Stays
    ///   incremented for the socket's lifetime.
    /// - One per `waitTimer*` (single-shot timerfd). Decremented when
    ///   the timer fires or is cancelled.
    ///
    /// Read by the dispatcher to decide whether to claim the poller
    /// role. The interrupt eventfd is excluded — reactor-internal.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// True while the poller is iterating the `epoll_wait` events
    /// buffer. Closers that have just issued `epoll_ctl(DEL)` on an
    /// fd MUST wait for this to clear before freeing the PD:
    /// `epoll_ctl(DEL)` removes the kernel registration synchronously,
    /// but events already transferred into a poller's user-space
    /// buffer are NOT flushed — the dispatch loop would then read
    /// `data` as a freed `*PollDesc`. See `unregisterFd` for the
    /// spin discipline. Mirrors `reactor_kqueue.zig:95`.
    in_poll: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        _ = allocator; // no per-reactor allocation needed
        const ep = epoll_create1(EPOLL_CLOEXEC);
        if (ep < 0) return error.EpollCreateFailed;
        errdefer _ = posix_helpers.close(ep);

        const efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (efd < 0) return error.EpollCreateFailed;
        errdefer _ = posix_helpers.close(efd);

        // Register the interrupt eventfd. Level-triggered (no
        // `EPOLLET`) so a single ADD covers every subsequent
        // interrupt; the drain in `poll()` resets the readable state.
        var ev = epoll_event{
            .events = EPOLLIN,
            .data = INTERRUPT_DATA,
        };
        if (epoll_ctl(ep, EPOLL_CTL_ADD, efd, &ev) < 0) return error.EpollCreateFailed;

        return .{ .epfd = ep, .interrupt_fd = efd };
    }

    pub fn deinit(self: *Reactor) void {
        std.debug.assert(self.pending.load(.acquire) == 0);
        std.debug.assert(!self.in_poll.load(.acquire));
        if (self.interrupt_fd >= 0) _ = posix_helpers.close(self.interrupt_fd);
        if (self.epfd >= 0) _ = posix_helpers.close(self.epfd);
        self.interrupt_fd = -1;
        self.epfd = -1;
    }

    // ─── PollDesc-aware interface (Step 2f real implementation) ─────

    /// Register `pd` as the per-fd state machine for `fd`. Submits
    /// `EPOLL_CTL_ADD` with `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET`
    /// and `data = pd | POLL_DESC_TAG`. Edge-triggered: `poll()` sees
    /// an event each time the fd transitions to readable / writable
    /// / hung up, and `pd.deliverReady` pops a parked waiter or
    /// caches `PD_READY` for the next wait.
    ///
    /// Matches Go's `runtime/netpoll_epoll.go::netpollopen` exactly
    /// in mask + registration shape (plus our `POLL_DESC_TAG` instead
    /// of Go's `taggedPointer(pd, fdseq)`).
    pub fn registerFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) ReactorWaitError!void {
        var ev = epoll_event{
            .events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET,
            .data = dataForPd(pd),
        };
        if (epoll_ctl(self.epfd, EPOLL_CTL_ADD, @intCast(fd), &ev) < 0) {
            return registerError(errnoVal());
        }
        _ = self.pending.fetchAdd(1, .acq_rel);
    }

    /// Reverse of `registerFd`. Issues `EPOLL_CTL_DEL`, decrements
    /// `pending`, then synchronises with any in-flight poll dispatch
    /// so the caller may safely free `pd`.
    ///
    /// `EPOLL_CTL_DEL` removes the kernel registration AND discards
    /// any events triggered-but-not-yet-delivered. What it does NOT
    /// do is reach into a poller's existing user-space `events[]`
    /// buffer that the kernel already populated. The `in_poll`
    /// spin handles that — same barrier as kqueue Step 2d.2.
    pub fn unregisterFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) void {
        _ = pd;
        // ENOENT is silently fine — kernel already removed the
        // registration (fd closed by some other path) or it was
        // never added.
        _ = epoll_ctl(self.epfd, EPOLL_CTL_DEL, @intCast(fd), null);
        _ = self.pending.fetchSub(1, .acq_rel);
        // Wake any worker blocked in `epoll_wait` so it completes
        // its current dispatch promptly, then spin until `in_poll`
        // clears.
        self.interrupt();
        while (self.in_poll.load(.acquire)) std.atomic.spinLoopHint();
    }

    /// Park the current coroutine until `fd` is ready in `mode`.
    /// Runs the per-fd state machine on `pd`; the kernel side stays
    /// armed across the call (persistent ET registration).
    pub fn waitFd(
        self: *Reactor,
        fd: i32,
        pd: *poll_desc.PollDesc,
        mode: poll_desc.Mode,
    ) ReactorWaitError!void {
        _ = self;
        _ = fd;
        pd.incref();
        defer pd.decref();
        return switch (pd.wait(mode)) {
            .ready => {},
            .closing => error.BadDescriptor,
        };
    }

    /// Cancel-aware variant of `waitFd`. Returns `error.Cancelled`
    /// if `c` fires before readiness; otherwise behaves like
    /// `waitFd`.
    pub fn waitFdCancel(
        self: *Reactor,
        fd: i32,
        pd: *poll_desc.PollDesc,
        mode: poll_desc.Mode,
        c: *cancel_mod.Cancel,
    ) (ReactorWaitError || cancel_mod.Error)!void {
        _ = self;
        _ = fd;
        const result = try pd.waitCancel(mode, c);
        return switch (result) {
            .ready => {},
            .closing => error.BadDescriptor,
        };
    }

    // ─── Timer (kept per-wait — timers are inherently single-shot) ──

    pub fn waitTimer(self: *Reactor, ns: u64) ReactorWaitError!void {
        // ns=0 is a no-op. `timerfd_settime` with an all-zero
        // `it_value` disarms the timer — without this guard the
        // coroutine parks forever.
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();

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

        // Drain the 8-byte expiration count and close.
        var buf: [8]u8 = undefined;
        _ = read(tfd, &buf, 8);
        _ = close(tfd);
    }

    /// Cancel-aware variant of `waitTimer`.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        const me = current.require();
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd < 0) return registerError(errnoVal());

        const spec = itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            },
        };
        if (timerfd_settime(tfd, 0, &spec, null) < 0) {
            _ = posix_helpers.close(tfd);
            return registerError(errnoVal());
        }

        var ev = epoll_event{
            .events = EPOLLIN | EPOLLONESHOT,
            .data = @intFromPtr(me),
        };
        if (epoll_ctl(self.epfd, EPOLL_CTL_ADD, tfd, &ev) < 0) {
            _ = posix_helpers.close(tfd);
            return registerError(errnoVal());
        }

        var op = WaitOp{ .fd = tfd };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        var w = cancel_mod.Waiter{};
        const already_fired = c.registerReactor(&w, me);
        defer c.deregister(&w);
        if (already_fired) {
            _ = epoll_ctl(self.epfd, EPOLL_CTL_DEL, tfd, null);
            _ = posix_helpers.close(tfd);
            return error.Cancelled;
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        var buf: [8]u8 = undefined;
        _ = read(tfd, &buf, 8);
        _ = posix_helpers.close(tfd);

        if (c.isFired()) return error.Cancelled;
    }

    /// Per-call stash for cancel-aware waits — now only used by the
    /// timer path. The PD wait path's cancellation is purely
    /// user-space (`pd.waitCancel`).
    pub const WaitOp = struct {
        fd: c_int,
    };

    /// Cancel this coro's in-flight timer registration. `EPOLL_CTL_DEL`
    /// returns 0 if it actually removed it; ENOENT means poll() got
    /// the event first and the unpark is in flight.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        const op_ptr = c.reactor_wait_op orelse return;
        const op: *WaitOp = @ptrCast(@alignCast(op_ptr));
        const rc = epoll_ctl(self.epfd, EPOLL_CTL_DEL, op.fd, null);
        if (rc < 0) return; // ENOENT — poll() owns the unpark.
        _ = posix_helpers.close(op.fd);
        _ = self.pending.fetchSub(1, .acq_rel);
        runtime.unpark(c);
    }

    /// Close an fd that may have a PollDesc still registered with
    /// epoll. In the persistent-registration model PD lifecycle
    /// (evict + closeAndWait + unregisterFd + free) is owned by the
    /// socket type via `pd_handle.release`, called BEFORE this fn.
    /// All we do here is the libc close.
    pub fn closeFd(self: *Reactor, fd: i32) void {
        _ = self;
        _ = posix_helpers.close(fd);
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;
        var events: [EPOLL_EVENTS_BATCH]epoll_event = undefined;
        const timeout_ms: c_int = if (blocking) -1 else 0;

        // Set `in_poll` BEFORE `epoll_wait`. A concurrent
        // `unregisterFd` about to free a PD whose event the kernel
        // will transfer into our `events[]` MUST observe
        // `in_poll == true` for the entire window the buffer is
        // live. See `reactor_kqueue.zig:396` for the full rationale.
        self.in_poll.store(true, .release);
        defer self.in_poll.store(false, .release);

        const n = epoll_wait(self.epfd, &events, EPOLL_EVENTS_BATCH, timeout_ms);
        if (n <= 0) return 0;
        const count: usize = @intCast(n);
        var timer_count: u32 = 0;
        var dispatched: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const data = events[i].data;
            const ev_mask = events[i].events;

            if (data == INTERRUPT_DATA) {
                // Drain the eventfd counter to clear its readable
                // state. Level-triggered registration would re-fire
                // on the next epoll_wait otherwise. Doesn't count
                // toward `pending`.
                var drain_buf: [8]u8 = undefined;
                _ = read(self.interrupt_fd, &drain_buf, 8);
                continue;
            }

            if (isPdData(data)) {
                // Persistent PD registration — parse the events
                // bitmap and dispatch to the appropriate direction(s).
                // POLLERR / POLLHUP wake both directions so a hung-up
                // fd doesn't strand parked readers or writers.
                const pd = pdFromData(data);
                const wake_read = (ev_mask & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0;
                const wake_write = (ev_mask & (EPOLLOUT | EPOLLHUP | EPOLLERR)) != 0;
                if (wake_read) {
                    if (pd.deliverReady(.read)) |c| runtime.unpark(c);
                }
                if (wake_write) {
                    if (pd.deliverReady(.write)) |c| runtime.unpark(c);
                }
                dispatched += 1;
                continue;
            }

            // Timer event — `data` is a raw coroutine pointer.
            // Per-call timerfd registration; decrement `pending`.
            const coro: *coroutine.Coroutine = @ptrFromInt(data);
            runtime.unpark(coro);
            timer_count += 1;
            dispatched += 1;
        }
        // Only timer events decrement `pending` — persistent PD
        // registrations stay armed (`EPOLLET` is edge-triggered,
        // not single-shot).
        if (timer_count > 0) _ = self.pending.fetchSub(timer_count, .acq_rel);
        return dispatched;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to call
    /// from any thread. The eventfd write is non-blocking; if a
    /// previous interrupt's counter hasn't been drained yet, the
    /// kernel coalesces.
    pub fn interrupt(self: *Reactor) void {
        const one: u64 = 1;
        var bytes: [8]u8 = undefined;
        @memcpy(&bytes, std.mem.asBytes(&one));
        _ = posix_helpers.write(self.interrupt_fd, &bytes, 8);
    }

    // ─── Fs op surface (Phase 1: spawnBlocking proxies) ──────────────
    //
    // The epoll backend is the fallback when io_uring isn't available
    // (older kernels, sysctl-disabled, init failed). Phase 2 wires the
    // io_uring backend's fs methods to per-worker rings; the epoll
    // variant stays on this proxy because there is no async file I/O
    // available outside io_uring on Linux.

    pub fn fsRead(self: *Reactor, fd: i32, buf: []u8, offset: u64) reactor_fs.FsResult {
        _ = self;
        return reactor_fs.fsRead(fd, buf, offset);
    }

    pub fn fsWrite(self: *Reactor, fd: i32, buf: []const u8, offset: u64) reactor_fs.FsResult {
        _ = self;
        return reactor_fs.fsWrite(fd, buf, offset);
    }

    pub fn fsFsync(self: *Reactor, fd: i32) reactor_fs.FsResult {
        _ = self;
        return reactor_fs.fsFsync(fd);
    }

    pub fn fsFdatasync(self: *Reactor, fd: i32) reactor_fs.FsResult {
        _ = self;
        return reactor_fs.fsFdatasync(fd);
    }

    pub fn fsOpen(self: *Reactor, path_ptr: [*:0]const u8, flags: c_int, mode: c_uint) reactor_fs.FdResult {
        _ = self;
        return reactor_fs.fsOpen(path_ptr, flags, mode);
    }

    pub fn fsClose(self: *Reactor, fd: i32) reactor_fs.FsResult {
        _ = self;
        return reactor_fs.fsClose(fd);
    }
};

// Compile-time check: Linux-only.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_epoll.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

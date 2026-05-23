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
const cancel_mod = @import("cancel.zig");

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

// ─── eventfd (interrupt mechanism) ───────────────────────────────

const EFD_NONBLOCK: c_int = 0o4000; // O_NONBLOCK
const EFD_CLOEXEC: c_int = 0o2000000;
extern "c" fn eventfd(initval: c_uint, flags: c_int) c_int;

// Sentinel `data` value for the interrupt registration. Coroutine
// pointers are heap-allocated and never null, so 0 is safe.
const INTERRUPT_DATA: u64 = 0;

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
    /// eventfd registered with the epoll instance for cross-thread
    /// wakeups (`interrupt`). Lives for the reactor's lifetime; the
    /// epoll registration is level-triggered (no EPOLLONESHOT) so a
    /// single ADD covers every subsequent interrupt.
    interrupt_fd: c_int = -1,

    /// In-flight epoll registrations (one per coroutine parked on
    /// an fd or timer). Read by every dispatcher to decide whether
    /// to claim the poller role; same role as kqueue's `pending`.
    /// The eventfd interrupt registration is excluded — it's a
    /// reactor-internal wake channel, not a coroutine park.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init() !Reactor {
        const ep = epoll_create1(EPOLL_CLOEXEC);
        if (ep < 0) return error.EpollCreateFailed;
        errdefer _ = posix_helpers.close(ep);

        const efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (efd < 0) return error.EpollCreateFailed;
        errdefer _ = posix_helpers.close(efd);

        // Register the interrupt eventfd. Level-triggered (no
        // ONESHOT) so re-arm isn't needed — the drain in `poll`
        // resets the readable state.
        var ev = epoll_event{
            .events = EPOLLIN,
            .data = INTERRUPT_DATA,
        };
        if (epoll_ctl(ep, EPOLL_CTL_ADD, efd, &ev) < 0) return error.EpollCreateFailed;

        return .{ .epfd = ep, .interrupt_fd = efd };
    }

    pub fn deinit(self: *Reactor) void {
        // See `reactor_kqueue.zig` for the in-flight-at-deinit
        // contract. Caller is responsible for ensuring no coros
        // are parked in the kernel before tearing down.
        std.debug.assert(self.pending.load(.acquire) == 0);
        if (self.interrupt_fd >= 0) _ = posix_helpers.close(self.interrupt_fd);
        if (self.epfd >= 0) _ = posix_helpers.close(self.epfd);
        self.interrupt_fd = -1;
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
        // ns=0 is a no-op. timerfd_settime(2) treats an all-zero
        // it_value as "disarm the timer" — without this guard the
        // coroutine parks forever waiting for a fire that never
        // comes. Caught by the sleep(0) reactor-conformance test.
        if (ns == 0) return;

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

    /// Per-call stash for cancel-aware waits. `fd` is the
    /// epoll-registered descriptor (a socket fd or a timerfd).
    /// `is_timer` differentiates the two on the cancel path —
    /// timer cancels must also close the per-call timerfd, since
    /// the wait function won't run its own `close` if it never
    /// resumes naturally.
    pub const WaitOp = struct {
        fd: c_int,
        is_timer: bool = false,
    };

    /// Cancel this coro's in-flight epoll registration.
    /// Race-free single-unpark: `EPOLL_CTL_DEL` returns 0 if it
    /// actually removed the registration; ENOENT means the
    /// registration was already consumed by poll() and the normal
    /// unpark path is in flight.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        const op_ptr = c.reactor_wait_op orelse return;
        const op: *WaitOp = @ptrCast(@alignCast(op_ptr));
        const rc = epoll_ctl(self.epfd, EPOLL_CTL_DEL, op.fd, null);
        if (rc < 0) return; // ENOENT — poll() owns the unpark.
        // For timer waits, the per-call timerfd is also our
        // responsibility — the wait fn won't run its natural
        // close-after-read path because it never resumes.
        if (op.is_timer) _ = posix_helpers.close(op.fd);
        _ = self.pending.fetchSub(1, .acq_rel);
        runtime.unpark(c);
    }

    pub fn waitReadableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(@intCast(fd), EPOLLIN, c);
    }

    pub fn waitWritableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(@intCast(fd), EPOLLOUT, c);
    }

    fn waitFdCancel(self: *Reactor, fd: c_int, filter: u32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        const me = current.require();

        var op = WaitOp{ .fd = fd };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        // ─── Kernel registration FIRST (closes register-then-fire race) ──
        //
        // Old order — registerReactor then waitFd — opened a window
        // where fire() could call cancelCoro before this coroutine
        // had registered with the kernel. EPOLL_CTL_DEL then returned
        // ENOENT (nothing to delete); cancelCoro's "ENOENT means
        // poll() owns the unpark" heuristic silently returned, and
        // the coroutine then registered and parked forever.
        var ev = epoll_event{
            .events = filter | EPOLLONESHOT,
            .data = @intFromPtr(me),
        };
        var rc = epoll_ctl(self.epfd, EPOLL_CTL_ADD, fd, &ev);
        if (rc < 0 and errnoVal() == EEXIST) {
            rc = epoll_ctl(self.epfd, EPOLL_CTL_MOD, fd, &ev);
        }
        if (rc < 0) return registerError(errnoVal());
        _ = self.pending.fetchAdd(1, .acq_rel);

        // ─── Cancel-list registration ───────────────────────────────────
        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            // Fire happened before our register. cancelCoro was not
            // called for us (we weren't in the list) — own cleanup.
            if (epoll_ctl(self.epfd, EPOLL_CTL_DEL, fd, null) >= 0) {
                _ = self.pending.fetchSub(1, .acq_rel);
            }
            return error.Cancelled;
        }
        defer c.deregister(&w);

        // ─── Park ───────────────────────────────────────────────────────
        //
        // park_state RUNNING → NOTIFIED handles fire() landing in
        // the window between registerReactor returning and the swap
        // below: dispatch sees NOTIFIED and re-queues us immediately
        // (runtime.zig:160 documents the machine).
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Cancel-aware variant of `waitTimer`. The timerfd is set up
    /// inline (we can't share state with `waitTimer` because the
    /// op stash would be a stack-frame address held across one of
    /// our calls). On cancel, `cancelCoro` is what closes the
    /// timerfd via the `is_timer` branch.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        const me = current.require();
        try c.checkpoint();

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

        var op = WaitOp{ .fd = tfd, .is_timer = true };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        var w = cancel_mod.Waiter{};
        const already_fired = c.registerReactor(&w, me);
        defer c.deregister(&w);
        if (already_fired) {
            // Cancel fired between checkpoint and register — tear
            // down without parking.
            _ = epoll_ctl(self.epfd, EPOLL_CTL_DEL, tfd, null);
            _ = posix_helpers.close(tfd);
            return error.Cancelled;
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        // We resumed naturally (timer fired, not cancel). Drain
        // the 8-byte expiration count and close the timerfd.
        var buf: [8]u8 = undefined;
        _ = read(tfd, &buf, 8);
        _ = posix_helpers.close(tfd);

        if (c.isFired()) return error.Cancelled;
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;
        var events: [EPOLL_EVENTS_BATCH]epoll_event = undefined;
        const timeout_ms: c_int = if (blocking) -1 else 0;
        const n = epoll_wait(self.epfd, &events, EPOLL_EVENTS_BATCH, timeout_ms);
        if (n <= 0) return 0;
        const count: usize = @intCast(n);
        var real_count: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            // Interrupt event: drain the eventfd counter to clear
            // its readable state (level-triggered registration would
            // re-fire on the next epoll_wait otherwise). Doesn't
            // count toward `pending`.
            if (events[i].data == INTERRUPT_DATA) {
                var drain_buf: [8]u8 = undefined;
                _ = read(self.interrupt_fd, &drain_buf, 8);
                continue;
            }
            real_count += 1;
            const coro: *coroutine.Coroutine = @ptrFromInt(events[i].data);
            runtime.unpark(coro);
        }
        if (real_count > 0) _ = self.pending.fetchSub(@intCast(real_count), .acq_rel);
        return real_count;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to call
    /// from any thread. The eventfd write is non-blocking; if a
    /// previous interrupt's counter hasn't been drained yet, the
    /// kernel coalesces (writes ADD to the u64). The first epoll
    /// return will drain whatever has accumulated.
    pub fn interrupt(self: *Reactor) void {
        const one: u64 = 1;
        var bytes: [8]u8 = undefined;
        @memcpy(&bytes, std.mem.asBytes(&one));
        _ = posix_helpers.write(self.interrupt_fd, &bytes, 8);
    }
};

// Compile-time check: this file is only meaningful on Linux.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_epoll.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

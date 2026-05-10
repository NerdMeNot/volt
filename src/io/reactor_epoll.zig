//! Linux reactor backend — epoll + eventfd + timerfd.
//!
//! Mirrors the kqueue backend's interface (Reactor, EventKind,
//! init/deinit/registerWait/registerTimer/poll/tickle/...). Same
//! protocol: one-shot semantics for fd waits, dedicated tickle path
//! for cross-thread wakes, dedicated timer path for `volt.sleep`.
//!
//! ## One-shot vs level-triggered
//!
//! We use `EPOLLONESHOT`. After a registered (fd, EPOLLIN/OUT) fires
//! once, epoll automatically masks it; we don't need to track active
//! filters. Re-registration is via `EPOLL_CTL_MOD` on the next wait.
//!
//! ## Tickle (cross-thread wake)
//!
//! `eventfd` with `EFD_NONBLOCK | EFD_SEMAPHORE`. `tickle()` writes a
//! 1; the polling worker's epoll_wait returns; we drain the eventfd
//! by reading it; control loops back to find the new work.
//!
//! ## Timers
//!
//! Each `registerTimer` allocates a fresh `timerfd_create + settime`.
//! When it fires, epoll wakes us, we read the expiration count to
//! drain it, and close the fd. Per-timer fd is heavyweight compared
//! to kqueue's EVFILT_TIMER but it works on every Linux ≥ 2.6.25.
//! Timer-wheel optimization is v1.1+.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const linux = std.os.linux;

const syscall = @import("../internal/syscall.zig");
const Mutex = @import("../internal/thread.zig").Mutex;
const reactor_types = @import("reactor_types.zig");

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_epoll.zig is for Linux only");
    }
}

/// Re-exported so `reactor.zig`'s conformance check can read it from
/// `impl.EventKind`. The canonical declaration lives in
/// `reactor_types.zig`.
pub const EventKind = reactor_types.EventKind;

const ReactorError = reactor_types.ReactorError;

fn epollEventsFor(kind: EventKind) u32 {
    return switch (kind) {
        .readable => linux.EPOLL.IN,
        .writable => linux.EPOLL.OUT,
    };
}

/// Sentinel udata pointer values used internally to distinguish
/// non-coroutine epoll events. Real *Park pointers won't collide
/// because the runtime allocator returns aligned addresses ≥ 16.
const TICKLE_TAG: usize = 1;
const TIMER_TAG_BIT: usize = 1 << 63; // OR'd into the udata for timer events.

/// Heap-allocated entry for an in-flight timer. The udata holds
/// `@intFromPtr(entry) | TIMER_TAG_BIT`; on fire we cast back, close
/// the timerfd, free the entry, and deliver the wake to `target`.
const TimerEntry = struct {
    target: *anyopaque,
    timerfd: i32,
};

pub const Reactor = struct {
    epfd: i32,
    /// eventfd for `tickle`. Read-drained by `poll()` whenever it fires.
    tickle_fd: i32,

    /// Mirrors the kqueue backend: pending count + per-(fd,kind) waiter
    /// map (debug aid). The protocol works without the map; we keep it
    /// for `pendingCount()` cheapness and assertion symmetry.
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    mutex: Mutex = .{},
    waiters: std.AutoHashMap(WaitKey, void),
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) ReactorError!Reactor {
        const epfd = linuxCreateEpoll() catch return error.InitFailed;
        errdefer syscall.close(epfd);

        const tfd = linuxCreateEventfd() catch return error.InitFailed;
        errdefer syscall.close(tfd);

        // Register the tickle eventfd with the epoll set so any
        // tickle() write returns the polling worker.
        var ev = linux.epoll_event{
            .events = linux.EPOLL.IN,
            .data = .{ .ptr = TICKLE_TAG },
        };
        if (linux.epoll_ctl(epfd, linux.EPOLL.CTL_ADD, tfd, &ev) != 0) {
            return error.InitFailed;
        }

        return .{
            .epfd = epfd,
            .tickle_fd = tfd,
            .waiters = std.AutoHashMap(WaitKey, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        syscall.close(self.tickle_fd);
        syscall.close(self.epfd);
        self.waiters.deinit();
    }

    pub fn pendingCount(self: *const Reactor) usize {
        return self.pending.load(.acquire);
    }

    /// Decrement `pending` with an underflow guard. See kqueue's
    /// pendingDec for D4 rationale.
    inline fn pendingDec(self: *Reactor) void {
        std.debug.assert(self.pending.load(.acquire) > 0);
        _ = self.pending.fetchSub(1, .release);
    }

    /// Cross-thread wake: write 1 to the eventfd; the polling worker's
    /// epoll_wait returns immediately.
    pub fn tickle(self: *Reactor) void {
        const one: u64 = 1;
        const buf: []const u8 = std.mem.asBytes(&one);
        _ = syscall.write(self.tickle_fd, buf) catch {};
    }

    /// Register `target` to be `wakeFn`-invoked when `fd` is ready for
    /// `kind`. One-shot — the kernel auto-disables after one delivery.
    ///
    /// CRITICAL ORDERING (R2, mirroring `reactor_kqueue.zig`):
    /// waiters.put + pending++ happen BEFORE epoll_ctl ADD. The kernel
    /// can queue an event the moment the fd is added; if the put hasn't
    /// happened, the (theoretical) waiters-map check would miss it.
    /// epoll's current poll() doesn't query waiters at all (it trusts
    /// `data.ptr`), so the race manifestation here is a pending-counter
    /// undercount rather than a hang — but the ordering rule is
    /// uniform across backends.
    pub fn registerWait(
        self: *Reactor,
        fd: posix.fd_t,
        kind: EventKind,
        target: *anyopaque,
    ) ReactorError!void {
        const key = WaitKey{ .fd = @intCast(fd), .kind_tag = @intFromEnum(kind) };

        var ev = linux.epoll_event{
            .events = epollEventsFor(kind) | linux.EPOLL.ONESHOT,
            .data = .{ .ptr = @intFromPtr(target) },
        };

        self.mutex.lock();
        // NB: unlike kqueue, epoll's ONESHOT auto-disables but doesn't
        // remove the fd from the epoll set; re-registration uses
        // CTL_MOD on the existing entry. So a `waiters.put` here CAN
        // legitimately hit an existing key (the kernel-side state
        // matches via EEXIST → MOD branch below). We don't assert
        // !contains.
        self.waiters.put(key, {}) catch {
            self.mutex.unlock();
            return error.OutOfMemory;
        };
        _ = self.pending.fetchAdd(1, .release);

        // ADD if first registration; MOD on re-register (epoll
        // remembers the fd even after ONESHOT disabled it).
        //
        // `linux.epoll_ctl` is a raw syscall — return is `-errno` packed
        // into a `usize`. `posix.errno` (= `std.c.errno` under libc) is
        // the wrong extractor here: it's libc-shaped (`if (rc == -1)`)
        // and silently returns `.SUCCESS` for any other value, so we'd
        // never see EEXIST and would always take the error path.
        // `linux.errno` is the raw-syscall variant.
        const rc1 = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_ADD, fd, &ev);
        if (rc1 != 0) {
            const errno = linux.errno(rc1);
            if (errno == .EXIST) {
                const rc2 = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_MOD, fd, &ev);
                if (rc2 != 0) {
                    _ = self.waiters.remove(key);
                    self.pendingDec();
                    self.mutex.unlock();
                    return error.RegistrationFailed;
                }
            } else {
                _ = self.waiters.remove(key);
                self.pendingDec();
                self.mutex.unlock();
                return error.RegistrationFailed;
            }
        }
        self.mutex.unlock();
    }

    /// Arm a one-shot timer that fires after `duration_ns` nanoseconds.
    /// Allocates a fresh timerfd + heap-allocated TimerEntry; on
    /// expiry, `poll` closes the fd, frees the entry, and delivers
    /// the wake to `target`.
    ///
    /// Returns an opaque id (the TimerEntry pointer cast to u64).
    /// Callers that need to cancel before fire must pass it to
    /// `unregisterTimer` — without that, a cancelled sleep leaks
    /// the timerfd, the heap entry, and the pending counter,
    /// wedging idle workers in `poll` (same kqueue-side bug just
    /// fixed in `reactor_kqueue`).
    pub fn registerTimer(
        self: *Reactor,
        duration_ns: u64,
        target: *anyopaque,
    ) ReactorError!u64 {
        const entry = self.allocator.create(TimerEntry) catch return error.OutOfMemory;
        errdefer self.allocator.destroy(entry);

        const tfd = linux.timerfd_create(linux.TIMERFD_CLOCK.MONOTONIC, .{ .NONBLOCK = true });
        const tfd_i: i32 = @intCast(tfd);
        if (tfd_i < 0) return error.RegistrationFailed;
        errdefer syscall.close(tfd_i);

        const spec = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(@divTrunc(duration_ns, std.time.ns_per_s)),
                .nsec = @intCast(@mod(duration_ns, std.time.ns_per_s)),
            },
        };
        if (linux.timerfd_settime(tfd_i, .{}, &spec, null) != 0) {
            return error.RegistrationFailed;
        }

        entry.* = .{ .target = target, .timerfd = tfd_i };

        // udata = `entry_ptr | TIMER_TAG_BIT`. Real heap ptrs fit in
        // 47 bits on mainstream Linux; bit 63 is free for our tag.
        var ev = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ONESHOT,
            .data = .{ .ptr = @intFromPtr(entry) | TIMER_TAG_BIT },
        };
        if (linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_ADD, tfd_i, &ev) != 0) {
            return error.RegistrationFailed;
        }

        _ = self.pending.fetchAdd(1, .release);
        return @intFromPtr(entry);
    }

    /// Cancel a pending timer. If the timer hasn't fired, EPOLL_CTL_DEL
    /// removes the registration, we close the timerfd + free the
    /// entry, and decrement pending. Best-effort on failure paths;
    /// double-cancel is safe-ish (the entry pointer is unique per
    /// timer, but freeing a freed entry is UB — callers must call
    /// at most once).
    pub fn unregisterTimer(self: *Reactor, id: u64) void {
        const entry: *TimerEntry = @ptrFromInt(id);
        const tfd = entry.timerfd;
        // EPOLL_CTL_DEL with null event ptr is fine post-2.6.9.
        _ = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_DEL, tfd, null);
        syscall.close(tfd);
        self.allocator.destroy(entry);
        self.pendingDec();
    }

    /// Cancel a pending fd wait. Same shape as `unregisterTimer`.
    pub fn unregisterWait(self: *Reactor, fd: posix.fd_t, kind: EventKind) void {
        _ = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_DEL, fd, null);
        self.mutex.lock();
        const key = WaitKey{ .fd = @intCast(fd), .kind_tag = @intFromEnum(kind) };
        _ = self.waiters.remove(key);
        self.mutex.unlock();
        self.pendingDec();
    }

    /// Block on epoll_wait for up to `timeout_ns`. For each ready
    /// event, invoke `wakeFn(wake_ctx, target)`. Returns the number of
    /// events delivered (excluding internal tickle).
    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
    ) anyerror!usize {
        var events: [64]linux.epoll_event = undefined;
        const timeout_ms: i32 = if (timeout_ns) |ns|
            @intCast(@min(@divTrunc(ns, std.time.ns_per_ms), std.math.maxInt(i32)))
        else
            -1;

        const n: usize = linux.epoll_wait(self.epfd, &events, events.len, timeout_ms);

        var woken: usize = 0;
        for (events[0..n]) |ev| {
            const raw = ev.data.ptr;

            // Tickle: drain the eventfd so we don't immediately re-wake.
            if (raw == TICKLE_TAG) {
                var sink: [8]u8 = undefined;
                _ = syscall.read(self.tickle_fd, &sink) catch {};
                continue;
            }

            // Timer event: cast back to TimerEntry, close timerfd,
            // free entry, deliver wake to the original target.
            if ((raw & TIMER_TAG_BIT) != 0) {
                const entry: *TimerEntry = @ptrFromInt(raw & ~TIMER_TAG_BIT);
                const target_ptr = entry.target;
                // Close the timerfd; the kernel auto-removed the
                // EPOLLONESHOT registration when it fired. Then free
                // the heap entry. Order matters less here since we're
                // single-poller-per-reactor (only one thread runs poll).
                syscall.close(entry.timerfd);
                self.allocator.destroy(entry);
                self.pendingDec();
                try wakeFn(wake_ctx, target_ptr);
                woken += 1;
                continue;
            }

            // Ordinary fd readiness.
            const target: *anyopaque = @ptrFromInt(raw);
            // We can't reverse-derive (fd, kind) from `events` without
            // storing it — epoll_event carries `events` (the bitmask)
            // and our tagged `data.ptr`. The waiters-map cleanup is
            // best-effort: we decrement pending and call wakeFn.
            // Memory cleanup of the map happens on the next
            // registerWait collision or at deinit.
            self.pendingDec();
            try wakeFn(wake_ctx, target);
            woken += 1;
        }
        return woken;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Linux syscall wrappers (kept here so the kqueue backend stays clean)
// ─────────────────────────────────────────────────────────────────────

// Internal-only: errors here flow up through `init`, which translates
// them all to ReactorError.InitFailed. Names are kept distinct for
// debugging visibility (a stack trace through the failing call stays
// informative) but they don't escape the file.
fn linuxCreateEpoll() error{EpollCreateFailed}!i32 {
    const flags = linux.EPOLL.CLOEXEC;
    const rc: i32 = @intCast(linux.epoll_create1(flags));
    if (rc < 0) return error.EpollCreateFailed;
    return rc;
}

fn linuxCreateEventfd() error{EventfdCreateFailed}!i32 {
    const flags: u32 = linux.EFD.NONBLOCK | linux.EFD.CLOEXEC;
    const rc: i32 = @intCast(linux.eventfd(0, flags));
    if (rc < 0) return error.EventfdCreateFailed;
    return rc;
}

//! kqueue reactor (Darwin / BSD).
//!
//! Inline-poll design: when the dispatch loop has no runnable work
//! and `reactor.pending > 0`, the worker claims the single poller
//! role and calls `reactor.poll(blocking=true)`. kevent returns the
//! ready set; each event's `udata` is the parked coroutine's pointer,
//! which the reactor unparks back onto the run queue.
//!
//! `waitReadable(fd)` / `waitWritable(fd)`, called from inside a
//! coroutine waiting on `fd`:
//!   1. Submit `EV_ADD | EV_ONESHOT` with the coro's pointer as udata.
//!   2. Set `current.pending = .park`.
//!   3. Swap to main_ctx.
//!   4. Worker observes `.park`; coroutine sits idle in kqueue's hands.
//!   5. fd ready → kevent → `poll` → `runtime.unpark(coro)`.
//!
//! ONESHOT means each wait re-registers — keeps the registration set
//! tight (no leftover stale entries) and matches the wait-once-then-
//! resume usage pattern.

const std = @import("std");
const posix = std.posix;
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const posix_helpers = @import("reactor_posix.zig");
const ReactorWaitError = posix_helpers.ReactorWaitError;

const KEV_BATCH: usize = 32;

// Darwin sys/event.h constants for EVFILT_TIMER.
const EVFILT_TIMER: i16 = -7;
const NOTE_NSECONDS: u32 = 0x00000004;

pub const Reactor = struct {
    kq: i32 = -1,
    /// In-flight kqueue registrations (one per coroutine parked on
    /// an fd). Read by every dispatcher (`tryFindAndDispatch` and
    /// `parkWorker`) to decide whether to claim the poller role;
    /// incremented by `waitFd` from any worker; decremented by
    /// `poll` from the worker that claimed the poller. Atomic
    /// because multiple threads write/read it concurrently.
    ///
    /// Ordering rationale: `.acq_rel` on the RMW pairs the increment
    /// (which happens before the kevent registration is observable
    /// to the kernel) with the dispatcher's `.acquire` load that
    /// decides to poll. The actual happens-before for fd readiness
    /// is carried by the kernel's kqueue, not by this counter — the
    /// counter only gates "should we bother calling kevent at all".
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init() !Reactor {
        const kq = std.c.kqueue();
        if (kq < 0) return error.KqueueInitFailed;
        return .{ .kq = kq };
    }

    pub fn deinit(self: *Reactor) void {
        if (self.kq >= 0) _ = std.c.close(self.kq);
        self.kq = -1;
    }

    /// Park the current coroutine until `fd` is readable.
    /// Caller is expected to retry the read after this returns.
    pub fn waitReadable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(fd, posix.system.EVFILT.READ);
    }

    /// Park the current coroutine until `fd` is writable.
    pub fn waitWritable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(fd, posix.system.EVFILT.WRITE);
    }

    /// Park the current coroutine for at least `ns` nanoseconds.
    /// Single-shot kqueue timer keyed on the coroutine's pointer
    /// (each coro can have at most one outstanding sleep at a time).
    /// Kernel timer resolution is bounded below by ~1 µs on Darwin.
    pub fn waitTimer(self: *Reactor, ns: u64) ReactorWaitError!void {
        const me = current.require();
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = @intFromPtr(me);
        kev.filter = EVFILT_TIMER;
        kev.flags = posix.system.EV.ADD | posix.system.EV.ONESHOT;
        kev.fflags = NOTE_NSECONDS;
        kev.data = @intCast(ns);
        kev.udata = @intFromPtr(me);
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        const n = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
        if (n < 0) return registerError(posix_helpers.errnoVal());
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    fn waitFd(self: *Reactor, fd: i32, filter: i16) ReactorWaitError!void {
        const me = current.require();
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = @intCast(fd);
        kev.filter = filter;
        kev.flags = posix.system.EV.ADD | posix.system.EV.ONESHOT;
        kev.udata = @intFromPtr(me);
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        const n = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
        if (n < 0) return registerError(posix_helpers.errnoVal());
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
        // Resume: pending decremented by poll() before unpark.
    }

    /// Map a `kevent` registration errno into the categorical
    /// `ReactorWaitError` set. EBADF means the fd was closed under
    /// us; resource-exhaustion variants are the common
    /// retry-and-back-off targets for downstream libraries.
    fn registerError(e: c_int) ReactorWaitError {
        return switch (e) {
            posix_helpers.Errno.EBADF, posix_helpers.Errno.ENOTSOCK => error.BadDescriptor,
            posix_helpers.Errno.EMFILE, posix_helpers.Errno.ENFILE => error.OutOfDescriptors,
            posix_helpers.Errno.ENOMEM, posix_helpers.Errno.ENOBUFS => error.SystemResources,
            else => error.Unexpected,
        };
    }

    /// Number of in-flight registrations. Used by the dispatcher to
    /// decide whether to claim the poller role.
    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    /// Drain ready events. Returns the number of coroutines unparked.
    /// If `blocking` is true and `pending > 0`, blocks until ≥ 1
    /// event fires. If `blocking` is false, returns immediately
    /// (timeout = 0).
    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;
        var events: [KEV_BATCH]posix.Kevent = undefined;
        const timeout_ptr: ?*const posix.timespec =
            if (blocking) null else &posix.timespec{ .sec = 0, .nsec = 0 };
        const n = posix.system.kevent(self.kq, &.{}, 0, &events, KEV_BATCH, timeout_ptr);
        if (n <= 0) return 0;
        const count: usize = @intCast(n);
        _ = self.pending.fetchSub(@intCast(count), .acq_rel);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const coro: *coroutine.Coroutine = @ptrFromInt(events[i].udata);
            runtime.unpark(coro);
        }
        return count;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Non-blocking IO helpers — shared with epoll / io_uring backends.
// ─────────────────────────────────────────────────────────────────────

pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

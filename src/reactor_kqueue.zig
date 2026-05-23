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
const cancel_mod = @import("cancel.zig");
const posix_helpers = @import("reactor_posix.zig");
const ReactorWaitError = posix_helpers.ReactorWaitError;

const KEV_BATCH: usize = 32;

// Darwin sys/event.h constants for EVFILT_TIMER.
const EVFILT_TIMER: i16 = -7;
const NOTE_NSECONDS: u32 = 0x00000004;

// EVFILT_USER — user-triggered events; no fd needed. Volt uses it
// for the cross-thread reactor wakeup (`interrupt`). NOTE_TRIGGER
// fires the registered user event from any thread, waking a poll
// blocked in kevent without the self-pipe pattern.
const EVFILT_USER: i16 = -10;
const NOTE_TRIGGER: u32 = 0x01000000;

// Sentinel ident for the EVFILT_USER interrupt registration. Picked
// because no real fd is 0 in our usage (stdin would be, but we never
// register stdin), and the kevent layout doesn't need ident
// uniqueness across filters — `(ident, filter)` is the key.
const INTERRUPT_IDENT: usize = 0;

// Sentinel udata on the interrupt event. The poll loop distinguishes
// interrupt completions from coroutine wakeups by checking
// `udata == INTERRUPT_UDATA`. Zero is safe — coroutine pointers are
// heap-allocated and never null.
const INTERRUPT_UDATA: usize = 0;

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

        // Register the EVFILT_USER interrupt event. EV_CLEAR makes it
        // edge-triggered (the event auto-resets after each fire), so
        // a single registration serves every subsequent interrupt.
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = INTERRUPT_IDENT;
        kev.filter = EVFILT_USER;
        kev.flags = posix.system.EV.ADD | posix.system.EV.CLEAR;
        kev.udata = INTERRUPT_UDATA;
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        if (posix.system.kevent(kq, &changes, 1, &dummy, 0, null) < 0) {
            _ = std.c.close(kq);
            return error.KqueueInitFailed;
        }
        return .{ .kq = kq };
    }

    pub fn deinit(self: *Reactor) void {
        // In-flight registrations at deinit means a coroutine is
        // still parked in the kernel — fundamentally a shutdown-
        // ordering bug in the caller. Asserting here catches it in
        // debug builds; in release we close the kq anyway and the
        // kernel cleans up the registrations on file-handle close.
        std.debug.assert(self.pending.load(.acquire) == 0);
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
        // ns=0 is a no-op. EVFILT_TIMER with data=0 happens to fire
        // immediately on Darwin but the contract isn't documented and
        // other kqueue platforms (FreeBSD/NetBSD) differ — keep the
        // cross-reactor behaviour explicit at every impl site.
        if (ns == 0) return;
        // EVFILT_TIMER's `data` field is `intptr_t` (i64 on 64-bit).
        // ns > i64_max would wrap silently; surface explicitly.
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

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

    /// Per-call stash for cancel-aware waits: carries the (ident,
    /// filter) key needed to issue `EV_DELETE` for this coro's
    /// in-flight registration. Lives on the waiter's stack and is
    /// pointed at by `Coroutine.reactor_wait_op` for the lifetime
    /// of the park.
    pub const WaitOp = struct {
        ident: usize,
        filter: i16,
    };

    /// Cancel this coro's in-flight kqueue registration.
    ///
    /// Race-free single-unpark: `EV_DELETE` returns 0 if it actually
    /// removed the registration (meaning the kernel hadn't yet
    /// pulled an event for it) — only that path decrements
    /// `pending` and unparks. If the call returns ENOENT, the
    /// registration was already consumed by a concurrent poll();
    /// poll's normal unpark path is in flight, and we no-op.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        const op_ptr = c.reactor_wait_op orelse return;
        const op: *WaitOp = @ptrCast(@alignCast(op_ptr));
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = op.ident;
        kev.filter = op.filter;
        kev.flags = posix.system.EV.DELETE;
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        const rc = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
        if (rc < 0) return; // ENOENT — poll() owns the unpark.
        _ = self.pending.fetchSub(1, .acq_rel);
        runtime.unpark(c);
    }

    /// Cancel-aware variant of `waitReadable`. Returns
    /// `error.Cancelled` if the cancel fires before or during the
    /// park; otherwise behaves like `waitReadable` and returns
    /// `ReactorWaitError` on register failure.
    pub fn waitReadableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(fd, posix.system.EVFILT.READ, c);
    }

    /// Cancel-aware variant of `waitWritable`.
    pub fn waitWritableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(fd, posix.system.EVFILT.WRITE, c);
    }

    fn waitFdCancel(self: *Reactor, fd: i32, filter: i16, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        const me = current.require();

        var op = WaitOp{ .ident = @intCast(fd), .filter = filter };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        // ─── Kernel registration FIRST (closes register-then-fire race) ──
        //
        // Old order — registerReactor then waitFd — opened a window
        // where fire() could call cancelCoro before this coroutine
        // had registered with the kernel. The kevent DEL would
        // return ENOENT (nothing to delete), cancelCoro's "ENOENT
        // means poll() owns the unpark" heuristic would silently
        // return, and the coroutine would then register and park
        // forever. New order: register kernel side first so any
        // later cancelCoro finds a real kevent.
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

        // ─── Cancel-list registration ───────────────────────────────────
        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            // Fired before our register. cancelCoro was not called
            // for us (we weren't in the list) — own the kevent
            // cleanup.
            deregisterKevent(self, kev.ident, filter);
            return error.Cancelled;
        }
        defer c.deregister(&w);

        // ─── Park ───────────────────────────────────────────────────────
        //
        // Wake source is one of:
        //   * poll() pulled the event → normal unpark path
        //   * cancelCoro from a later fire() → kevent DEL + unpark
        // If fire() happens between registerReactor returning and
        // the swap below, runtime.unpark transitions park_state
        // RUNNING → NOTIFIED. Dispatch's `.park` branch then
        // re-queues immediately instead of leaving us stranded.
        // That's the race the park_state machine exists to close
        // (see runtime.zig:160).
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Issue EV_DELETE for a (ident, filter) registration this
    /// coroutine owns. Used by the cancel-aware wait functions'
    /// "fired before register" cleanup path. ENOENT means poll()
    /// or cancelCoro already removed it; only the successful
    /// path decrements `pending`.
    fn deregisterKevent(self: *Reactor, ident: usize, filter: i16) void {
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = ident;
        kev.filter = filter;
        kev.flags = posix.system.EV.DELETE;
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        const rc = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
        if (rc >= 0) _ = self.pending.fetchSub(1, .acq_rel);
    }

    /// Cancel-aware variant of `waitTimer`. The kqueue timer
    /// registration is keyed on `(coro_ptr, EVFILT_TIMER)`, so the
    /// same `WaitOp` mechanism + `EV_DELETE` deregister carries
    /// through unchanged from the fd cancel path.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        // ns=0 mirrors waitTimer — the early-return contract is the
        // same on every reactor (see waitTimer's comment). A zero-
        // duration cancellable sleep should still respect a fire
        // that happened before the call.
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        var op = WaitOp{ .ident = @intFromPtr(me), .filter = EVFILT_TIMER };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        // Kernel registration FIRST — see waitFdCancel for the
        // register-then-fire race this ordering closes.
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

        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            deregisterKevent(self, kev.ident, EVFILT_TIMER);
            return error.Cancelled;
        }
        defer c.deregister(&w);

        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
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
        var real_count: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            // Interrupt events carry the sentinel udata and don't
            // count toward `pending` — skip the unpark and the
            // decrement. The kevent return itself was the wake.
            if (events[i].udata == INTERRUPT_UDATA) continue;
            real_count += 1;
            const coro: *coroutine.Coroutine = @ptrFromInt(events[i].udata);
            runtime.unpark(coro);
        }
        if (real_count > 0) _ = self.pending.fetchSub(@intCast(real_count), .acq_rel);
        return real_count;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to call
    /// from any thread. If no worker is in kevent, the trigger is
    /// retained by EVFILT_USER (level-cleared on the next fire) so a
    /// poll entered shortly after still observes the wake.
    pub fn interrupt(self: *Reactor) void {
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = INTERRUPT_IDENT;
        kev.filter = EVFILT_USER;
        kev.fflags = NOTE_TRIGGER;
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        _ = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
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

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "Reactor: interrupt wakes blocking poll" {
    var rx = try Reactor.init();
    // LIFO defer: pending reset runs before deinit's `pending == 0`
    // assertion. Without the bump, poll() takes its early-return
    // path and never enters kevent.
    defer rx.deinit();
    rx.pending.store(1, .release);
    defer rx.pending.store(0, .release);

    var done = std.atomic.Value(bool).init(false);

    const Worker = struct {
        fn run(reactor: *Reactor, flag: *std.atomic.Value(bool)) void {
            _ = reactor.poll(true);
            flag.store(true, .release);
        }
    };
    const t = try std.Thread.spawn(.{}, Worker.run, .{ &rx, &done });

    // Spin-interrupt until the worker observes the wake. EVFILT_USER
    // with EV_CLEAR coalesces multiple triggers into one event, so
    // repeated interrupts are safe — whichever lands while the
    // worker is in (or about to enter) kevent will release it. The
    // attempt cap converts a broken interrupt into a clean test
    // failure rather than a hang.
    var attempts: u32 = 0;
    while (!done.load(.acquire)) : (attempts += 1) {
        if (attempts > 1_000_000) return error.InterruptDidNotWake;
        rx.interrupt();
        std.atomic.spinLoopHint();
    }
    t.join();
}

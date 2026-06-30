//! kqueue reactor (Darwin / BSD) — persistent registration model.
//!
//! Each socket's `PollDesc` is registered with kqueue ONCE at socket
//! creation (via `registerFd`) for both `EVFILT_READ` and
//! `EVFILT_WRITE`, with the `*PollDesc` pointer carried in `udata`
//! and `EV_CLEAR` for edge-triggered semantics. The registration
//! lives for the socket's lifetime and is auto-removed by the kernel
//! when the fd is closed — no per-wait kevent submissions.
//!
//! `waitFd(fd, pd, mode)`:
//!   1. Run `pd.wait(mode)` — pure user-space state machine.
//!   2. On `.ready`, return; caller retries the syscall.
//!   3. On `.closing`, return `error.BadDescriptor`.
//!
//! `poll(blocking)`:
//!   1. `kevent` drains ready events into a batch buffer.
//!   2. For each event: extract `*PollDesc` from `udata`, call
//!      `pd.deliverReady(mode)` — the state-machine CAS either pops
//!      a parked coro (we `runtime.unpark`) or caches PD_READY for
//!      the next wait.
//!
//! Why this design eliminates the per-wait registration races:
//!   * **close-vs-register**: no per-wait register, so a peer closing
//!     the fd while we're "between insertWaiter and kevent ADD" is
//!     no longer a thing. The registration was made at socket open,
//!     before any peer could see the fd.
//!   * **cancel-vs-register**: same. Cancel-aware wait is purely
//!     user-space — `pd.waitCancel` registers a `cancel.Waiter` that
//!     wakes via `pd.deliverCancel` (state-machine CAS), no kernel
//!     deregister.
//!   * **kevent silent-error swallowing on closed fd**: there's no
//!     kevent ADD during wait, so the "kernel accepts ADD on a just-
//!     closed fd, reports success, never fires" footgun is gone too.
//!
//! Timers (`waitTimer` / `waitTimerCancel`) keep per-wait
//! registration since they're inherently per-call. `cancelCoro` and
//! the `WaitOp` it consumes are timer-only in this model.

const std = @import("std");
const posix = std.posix;
const coroutine = @import("../coroutine.zig");
const runtime = @import("../runtime.zig");
const current = @import("../current.zig");
const context = @import("../context.zig");
const cancel_mod = @import("../cancel.zig");
const poll_desc = @import("../poll_desc.zig");
const posix_helpers = @import("posix.zig");
const reactor_fs = @import("fs.zig");
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
// `udata == INTERRUPT_UDATA`. Zero is safe — coroutine pointers and
// PollDesc pointers are heap-allocated and never null.
const INTERRUPT_UDATA: usize = 0;

pub const Reactor = struct {
    kq: i32 = -1,
    /// Sum of currently-registered kernel events (per-fd registrations
    /// count as 2 — one each for READ and WRITE — plus one per
    /// in-flight timer). Read by the dispatcher to decide whether to
    /// claim the poller role; incremented at `registerFd` /
    /// `waitTimer*`, decremented at `unregisterFd` / `poll` (timer
    /// fires only).
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// True while the poller is iterating the events[] buffer returned
    /// by a `kevent()` call. Closers that have just issued EV_DELETE
    /// on an fd MUST wait for this to clear before freeing the PD:
    /// EV_DELETE removes the kernel registration synchronously, but
    /// events already pulled into a poller's user-space `events[]`
    /// (via a prior `kevent()`) are NOT flushed — that buffer is owned
    /// by the poller, the kernel can't reach into it. The dispatch
    /// loop then dereferences `udata` as `*PollDesc`, and if the
    /// closer has already `destroy`d the PD, that's a use-after-free.
    /// See `unregisterFd`'s wait spin.
    in_poll: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        _ = allocator; // no per-reactor allocation needed in the persistent-registration model
        const kq = std.c.kqueue();
        if (kq < 0) return error.KqueueInitFailed;

        // Register the EVFILT_USER interrupt event. EV_CLEAR makes it
        // edge-triggered (the event auto-resets after each fire), so
        // a single registration serves every subsequent interrupt.
        //
        // EV_RECEIPT — request per-change acknowledgement; nevents=0
        // would silently drop ADD errors. See `submitOneChange` for
        // the full rationale on this pattern.
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = INTERRUPT_IDENT;
        kev.filter = EVFILT_USER;
        kev.flags = posix.system.EV.ADD | posix.system.EV.CLEAR | posix.system.EV.RECEIPT;
        kev.udata = INTERRUPT_UDATA;
        var changes = [_]posix.Kevent{kev};
        var receipt: [1]posix.Kevent = undefined;
        const rc = posix.system.kevent(kq, &changes, 1, &receipt, 1, null);
        if (rc != 1 or receipt[0].data != 0) {
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
        std.debug.assert(!self.in_poll.load(.acquire));
        if (self.kq >= 0) _ = std.c.close(self.kq);
        self.kq = -1;
    }

    // ─── PollDesc-aware interface (Step 2d real implementation) ─────

    /// Register `pd` as the per-fd state machine for `fd`. Submits
    /// `EV_ADD | EV_CLEAR | EV_RECEIPT` for both `EVFILT_READ` and
    /// `EVFILT_WRITE` with `udata = ptr(pd)`. EV_CLEAR is edge-
    /// triggered: poll() sees an event each time the fd transitions
    /// to ready, and `pd.deliverReady` either pops a parked waiter
    /// or caches PD_READY for the next wait.
    ///
    /// Failure (e.g. EBADF on a stale fd) returns the categorised
    /// error and the registration is unwound — `pending` is not
    /// touched and no waiters can possibly be parked yet.
    pub fn registerFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) ReactorWaitError!void {
        const udata = @intFromPtr(pd);
        const add_flags = posix.system.EV.ADD | posix.system.EV.CLEAR | posix.system.EV.RECEIPT;
        var changes = [_]posix.Kevent{
            .{ .ident = @intCast(fd), .filter = posix.system.EVFILT.READ, .flags = add_flags, .fflags = 0, .data = 0, .udata = udata },
            .{ .ident = @intCast(fd), .filter = posix.system.EVFILT.WRITE, .flags = add_flags, .fflags = 0, .data = 0, .udata = udata },
        };
        var receipt: [2]posix.Kevent = undefined;
        const n = posix.system.kevent(self.kq, &changes, 2, &receipt, 2, null);
        if (n < 0) return registerError(posix_helpers.errnoVal());
        if (n != 2) return error.Unexpected;
        // Both receipts must report success. If either failed, undo
        // the other — kqueue applies changelist entries in order and
        // a partial success leaves the first registration live.
        const r0 = if (receipt[0].data == 0) true else false;
        const r1 = if (receipt[1].data == 0) true else false;
        if (!r0 or !r1) {
            // Best-effort cleanup of the half that succeeded.
            if (r0) issueDelete(self.kq, @intCast(fd), posix.system.EVFILT.READ);
            if (r1) issueDelete(self.kq, @intCast(fd), posix.system.EVFILT.WRITE);
            return registerError(@intCast(if (!r0) receipt[0].data else receipt[1].data));
        }
        _ = self.pending.fetchAdd(2, .acq_rel);
    }

    /// Reverse of `registerFd`. Issues `EV_DELETE` for both filters,
    /// decrements `pending` by 2, then synchronizes with any in-flight
    /// poll dispatch so the caller may safely free `pd`.
    ///
    /// The synchronization is what makes this `…Safe`. EV_DELETE
    /// removes the kernel registration AND (per macOS man kevent)
    /// discards any events triggered-but-not-yet-delivered to user
    /// space. What it does NOT do is reach into a poller's existing
    /// `events[]` buffer that was already populated by a prior
    /// `kevent()`. If a poller is mid-dispatch with an entry whose
    /// `udata` is our PD, and we destroy the PD before the dispatch
    /// loop reads it, that's a use-after-free (signal BUS / scribbled
    /// reads on macOS DebugAllocator).
    ///
    /// Fix: `interrupt()` to wake any poller blocked in `kevent()`
    /// (so it can complete its current dispatch and release `in_poll`),
    /// then spin until `in_poll == false`. The spin is short — the
    /// dispatch loop is non-blocking — and only contends with the
    /// (at-most-one) active poller.
    pub fn unregisterFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) void {
        _ = pd;
        issueDelete(self.kq, @intCast(fd), posix.system.EVFILT.READ);
        issueDelete(self.kq, @intCast(fd), posix.system.EVFILT.WRITE);
        _ = self.pending.fetchSub(2, .acq_rel);
        // Synchronize with any in-flight poll dispatch. EV_DELETE
        // removes the kernel registration AND discards events that
        // were triggered-but-not-yet-delivered (per macOS man kevent).
        // What it does NOT do is flush events that the kernel already
        // transferred into a poller's user-space `events[]` buffer
        // via a prior `kevent()`. If a poller is mid-dispatch with
        // such an entry whose udata is our PD, and the caller frees
        // `pd` once we return, dispatch reads freed memory — signal
        // BUS on macOS DebugAllocator, or "double free" / "second
        // free" depending on which CAS lands in the freed slot.
        //
        // Fix: interrupt any poller blocked in `kevent()` so it
        // completes its current dispatch (which may include a stale
        // entry for our fd, but the PD is still alive — we haven't
        // returned to the caller yet), then spin until `in_poll`
        // clears. The dispatch loop is non-blocking; the spin is
        // typically a handful of cache-line bounces.
        self.interrupt();
        while (self.in_poll.load(.acquire)) std.atomic.spinLoopHint();
    }

    /// Park the current coroutine until `fd` is ready in `mode`.
    /// Runs the per-fd state machine on `pd`; the kernel side stays
    /// armed across the call (persistent registration).
    ///
    /// incref/decref around `pd.wait` so a concurrent socket-close
    /// (which calls `pd.closeAndWait` and frees the PD) can't free
    /// the PD while we're in the post-wake epilogue still touching
    /// its slot. `waitCancel` does its own incref; `wait` doesn't,
    /// so this is the wrapper's job.
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
    /// if `c` fires before readiness; otherwise behaves like `waitFd`.
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
        kev.flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.RECEIPT;
        kev.fflags = NOTE_NSECONDS;
        kev.data = @intCast(ns);
        kev.udata = @intFromPtr(me);
        var changes = [_]posix.Kevent{kev};
        var receipt: [1]posix.Kevent = undefined;
        const n = posix.system.kevent(self.kq, &changes, 1, &receipt, 1, null);
        if (n < 0) return registerError(posix_helpers.errnoVal());
        if (n != 1 or receipt[0].data != 0) {
            return registerError(@intCast(receipt[0].data));
        }
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    /// Cancel-aware variant of `waitTimer`.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        var op = WaitOp{ .ident = @intFromPtr(me), .filter = EVFILT_TIMER };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        // Kernel registration FIRST — same register-then-fire race
        // closure as the old submitPollCancel. fire() between
        // registerReactor and our context.swap would otherwise call
        // cancelCoro on a coro with no kevent to EV_DELETE.
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = @intFromPtr(me);
        kev.filter = EVFILT_TIMER;
        kev.flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.RECEIPT;
        kev.fflags = NOTE_NSECONDS;
        kev.data = @intCast(ns);
        kev.udata = @intFromPtr(me);
        var changes = [_]posix.Kevent{kev};
        var receipt: [1]posix.Kevent = undefined;
        const n = posix.system.kevent(self.kq, &changes, 1, &receipt, 1, null);
        if (n < 0) return registerError(posix_helpers.errnoVal());
        if (n != 1 or receipt[0].data != 0) {
            return registerError(@intCast(receipt[0].data));
        }
        _ = self.pending.fetchAdd(1, .acq_rel);

        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            deregisterTimer(self, kev.ident);
            return error.Cancelled;
        }
        defer c.deregister(&w);

        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Per-call stash for cancel-aware waits — now only used by the
    /// timer path. Lives on the waiter's stack and is pointed at by
    /// `Coroutine.reactor_wait_op` for the lifetime of the park.
    pub const WaitOp = struct {
        ident: usize,
        filter: i16,
    };

    /// Cancel this coro's in-flight kqueue timer registration.
    ///
    /// Race-free single-unpark: `EV_DELETE` returns 0 if it actually
    /// removed the registration (kernel hadn't pulled the event) —
    /// only that path decrements `pending` and unparks. ENOENT means
    /// poll() already pulled the event and its unpark is in flight.
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

    /// Close an fd that may have a PollDesc still registered with
    /// kqueue. In the persistent-registration model the kernel auto-
    /// removes EVFILT_READ/WRITE registrations on `close(fd)`, so all
    /// we do here is the libc close. PD lifecycle (evict +
    /// closeAndWait + unregisterFd + free) is owned by the socket
    /// type via `pd_handle.release`, called BEFORE this fn.
    pub fn closeFd(self: *Reactor, fd: i32) void {
        _ = self;
        _ = posix_helpers.close(fd);
    }

    /// Number of in-flight registrations. Used by the dispatcher to
    /// decide whether to claim the poller role.
    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    /// Drain ready events. For each kernel event, decode the filter
    /// to determine whether it's an fd readiness event (dispatch via
    /// `pd.deliverReady`) or a timer (unpark coro directly). Returns
    /// the number of events processed.
    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;
        var events: [KEV_BATCH]posix.Kevent = undefined;
        const timeout_ptr: ?*const posix.timespec =
            if (blocking) null else &posix.timespec{ .sec = 0, .nsec = 0 };
        // Set `in_poll` BEFORE `kevent`. A concurrent `unregisterFd`
        // about to free a PD whose event the kernel will transfer
        // into our `events[]` MUST observe `in_poll == true` for the
        // entire window the buffer is live. If set after `kevent`,
        // there's a race window between `kevent` returning a
        // populated buffer and our `.store(true)` — a closer in that
        // window sees false, frees, then we dispatch the stale udata.
        //
        // Cost: closers spin across a blocking `kevent()` too. That's
        // bounded because `unregisterFd` issues `interrupt()` first,
        // so the blocking `kevent()` returns promptly with the
        // EVFILT_USER event (plus any events the kernel had queued
        // before our EV_DELETE flushed its pre-delivery list).
        self.in_poll.store(true, .release);
        defer self.in_poll.store(false, .release);
        const n = posix.system.kevent(self.kq, &.{}, 0, &events, KEV_BATCH, timeout_ptr);
        if (n <= 0) return 0;
        const count: usize = @intCast(n);
        var dispatched: usize = 0;
        var timer_count: u32 = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            // Interrupt events carry the sentinel udata.
            if (events[i].udata == INTERRUPT_UDATA) continue;
            switch (events[i].filter) {
                posix.system.EVFILT.READ => {
                    const pd: *poll_desc.PollDesc = @ptrFromInt(events[i].udata);
                    if (pd.deliverReady(.read)) |coro| runtime.unpark(coro);
                    dispatched += 1;
                },
                posix.system.EVFILT.WRITE => {
                    const pd: *poll_desc.PollDesc = @ptrFromInt(events[i].udata);
                    if (pd.deliverReady(.write)) |coro| runtime.unpark(coro);
                    dispatched += 1;
                },
                EVFILT_TIMER => {
                    const coro: *coroutine.Coroutine = @ptrFromInt(events[i].udata);
                    runtime.unpark(coro);
                    dispatched += 1;
                    timer_count += 1;
                },
                else => {},
            }
        }
        // Only timer events decrement `pending` here — fd
        // registrations stay armed across fires (EV_CLEAR is edge-
        // triggered, not single-shot).
        if (timer_count > 0) _ = self.pending.fetchSub(timer_count, .acq_rel);
        return dispatched;
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

    // ─── Fs op surface (Phase 1: spawnBlocking proxies) ──────────────
    //
    // kqueue has no async file I/O equivalent to io_uring; Darwin/BSD
    // file ops always bridge through the blocking pool. The methods
    // exist on the Reactor surface so fs/file.zig has a single call
    // shape on every platform — Phase 2's Linux io_uring backend swaps
    // these out for real async ops without touching fs callers.

    // Phase 3D: signatures now take an optional cancel even
    // though kqueue (Darwin) has no async file I/O path. spawn-
    // Blocking pool can't be interrupted mid-syscall, so cancel
    // is checked once before submit and otherwise ignored —
    // honest "boundary-only" semantics, same as the
    // pre-Phase-3 readCancel behaviour on Darwin.
    pub fn fsRead(
        self: *Reactor,
        fd: i32,
        buf: []u8,
        offset: u64,
        cancel: ?*@import("../cancel.zig").Cancel,
    ) reactor_fs.FsResult {
        _ = self;
        if (cancel) |c| if (c.isFired()) return runtime.cancelledFsResult();
        return reactor_fs.fsRead(fd, buf, offset);
    }

    pub fn fsWrite(
        self: *Reactor,
        fd: i32,
        buf: []const u8,
        offset: u64,
        cancel: ?*@import("../cancel.zig").Cancel,
    ) reactor_fs.FsResult {
        _ = self;
        if (cancel) |c| if (c.isFired()) return runtime.cancelledFsResult();
        return reactor_fs.fsWrite(fd, buf, offset);
    }

    pub fn fsFsync(
        self: *Reactor,
        fd: i32,
        cancel: ?*@import("../cancel.zig").Cancel,
    ) reactor_fs.FsResult {
        _ = self;
        if (cancel) |c| if (c.isFired()) return runtime.cancelledFsResult();
        return reactor_fs.fsFsync(fd);
    }

    pub fn fsFdatasync(
        self: *Reactor,
        fd: i32,
        cancel: ?*@import("../cancel.zig").Cancel,
    ) reactor_fs.FsResult {
        _ = self;
        if (cancel) |c| if (c.isFired()) return runtime.cancelledFsResult();
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

/// Map a `kevent` registration errno into the categorical
/// `ReactorWaitError` set.
fn registerError(e: c_int) ReactorWaitError {
    return switch (e) {
        posix_helpers.Errno.EBADF, posix_helpers.Errno.ENOTSOCK => error.BadDescriptor,
        posix_helpers.Errno.EMFILE, posix_helpers.Errno.ENFILE => error.OutOfDescriptors,
        posix_helpers.Errno.ENOMEM, posix_helpers.Errno.ENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

/// Issue EV_DELETE for one (ident, filter) registration without
/// affecting `pending`. Errors silently ignored — the only expected
/// non-success is ENOENT (already removed), which is harmless.
fn issueDelete(kq: i32, ident: usize, filter: i16) void {
    var kev = std.mem.zeroes(posix.Kevent);
    kev.ident = ident;
    kev.filter = filter;
    kev.flags = posix.system.EV.DELETE;
    var changes = [_]posix.Kevent{kev};
    var dummy: [1]posix.Kevent = undefined;
    _ = posix.system.kevent(kq, &changes, 1, &dummy, 0, null);
}

/// Cancel-aware-timer rollback helper. Removes the timer kevent and
/// decrements `pending` if the EV_DELETE actually removed it (rc >=
/// 0). ENOENT (rc < 0) means poll() already pulled the event and is
/// in flight on the unpark — leave it alone.
fn deregisterTimer(self: *Reactor, ident: usize) void {
    var kev = std.mem.zeroes(posix.Kevent);
    kev.ident = ident;
    kev.filter = EVFILT_TIMER;
    kev.flags = posix.system.EV.DELETE;
    var changes = [_]posix.Kevent{kev};
    var dummy: [1]posix.Kevent = undefined;
    const rc = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
    if (rc >= 0) _ = self.pending.fetchSub(1, .acq_rel);
}

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
    var rx = try Reactor.init(std.testing.allocator);
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

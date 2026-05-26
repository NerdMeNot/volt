//! Linux io_uring reactor — **persistent registration via POLL_ADD_MULTI**.
//!
//! Step 2f migration: structural parity with kqueue's Step 2d.2.
//! Each socket's `PollDesc` is registered with io_uring ONCE at socket
//! creation (`registerFd`) via `IORING_OP_POLL_ADD` with
//! `IORING_POLL_ADD_MULTI` set — a single SQE that produces a CQE
//! every time the fd transitions to readable / writable / errored,
//! until cancelled. The `*PollDesc` pointer rides in `user_data` (tagged
//! to distinguish from timer / interrupt / cancel-ack CQEs); the
//! kernel-side registration lives for the socket's lifetime and is
//! retired by `ASYNC_CANCEL` in `unregisterFd`.
//!
//! `waitFd(fd, pd, mode)`:
//!   1. `pd.incref()` / `defer pd.decref()` (the PD lifecycle ref).
//!   2. `pd.wait(mode)` — pure user-space state machine.
//!   3. On `.ready`, return; caller retries the syscall.
//!   4. On `.closing`, return `error.BadDescriptor`.
//!
//! `poll(blocking)`:
//!   1. Flush the SQ under `lock`; block in `io_uring_enter` outside it.
//!   2. For each CQE, dispatch by `user_data`:
//!      - `CANCEL_SQE_USER_DATA` (sentinel) — skip (cancel ack).
//!      - `INTERRUPT_USER_DATA` (sentinel) — drain eventfd + re-arm.
//!      - `pd | POLL_DESC_TAG` — multi-shot PD CQE. Parse `revents` in
//!        `cqe.res`, call `pd.deliverReady(.read)` / `(.write)` for
//!        each event that fired. `pd.deliverReady` either pops a parked
//!        coro (we `runtime.unpark`) or caches PD_READY for the next
//!        wait. The `IORING_CQE_F_MORE` flag distinguishes intermediate
//!        CQEs (`F_MORE` set — multi-shot still live) from the
//!        terminal CQE (no `F_MORE` — registration retired).
//!      - other (coroutine pointer) — timer CQE; unpark directly.
//!
//! Why this design eliminates the legacy per-wait races:
//!   * **close-vs-register**: no per-wait SQE submit, so the close-vs-
//!     register race the legacy `submitPoll` carried (insert-waiter
//!     then submit poll_add, with a `closeFd` racing in between to
//!     submit a cancel SQE before the target poll_add lands) is gone.
//!     The registration was made at socket open, before any peer
//!     could see the fd.
//!   * **cancel-vs-register**: same — cancel-aware wait is purely
//!     user-space (`pd.waitCancel` runs the state machine), no kernel
//!     SQE submit during wait.
//!
//! Timers (`waitTimer` / `waitTimerCancel`) keep per-wait SQE
//! submission since they're inherently single-shot. `cancelCoro` and
//! the `WaitOp` it consumes are timer-only in this model.
//!
//! ## Threading model
//!
//! `IoUring`'s SQE production is **not** thread-safe by default —
//! producers race on the SQ tail counter. Volt has multiple workers
//! concurrently submitting SQEs (registerFd from any worker, cancel
//! from unregisterFd / waitTimerCancel), so production needs a
//! lock. CQE consumption stays on the single claimed poller worker
//! (`runtime.tryClaimPoller`), so no additional sync there.
//!
//! Critical invariant: the SQ lock MUST NOT be held across the
//! blocking `io_uring_enter(GETEVENTS)` call in `poll()`. A peer
//! coroutine's `unregisterFd` or `waitTimerCancel` needs the lock to
//! push the cancel SQE that generates the CQE we're waiting for —
//! if we hold it through the kernel wait, the firer can't submit and
//! we never wake. `poll()` is two-phase: flush the SQ under the
//! lock, then wait outside it.
//!
//! A future optimisation could use `IORING_SETUP_SINGLE_ISSUER` to
//! shard production per-worker via a fan-in ring; for now the
//! spinlock is fine — production is rare in steady-state TCP
//! workloads (one SQE per socket register + one per close, not per
//! byte).

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const cancel_mod = @import("cancel.zig");
const poll_desc = @import("poll_desc.zig");

const linux = std.os.linux;
const CQE_BATCH: usize = 32;
const DEFAULT_RING_ENTRIES: u16 = 256;

// Linux poll(2) mask bits — io_uring's POLL_ADD uses the same encoding.
const POLLIN: u32 = 0x0001;
const POLLOUT: u32 = 0x0004;
const POLLERR: u32 = 0x0008;
const POLLHUP: u32 = 0x0010;
const POLLRDHUP: u32 = 0x2000;

// eventfd constants — matches reactor_epoll.zig.
const EFD_NONBLOCK: c_int = 0o4000;
const EFD_CLOEXEC: c_int = 0o2000000;
extern "c" fn eventfd(initval: c_uint, flags: c_int) c_int;

// Shared POSIX helpers — same setNonblock / readAsync / writeAsync
// / readFull / writeAll as kqueue + epoll. The reactor-facing
// helpers take the reactor as `anytype`, so they bind to this
// backend at the call site without a shared base class.
const posix_helpers = @import("reactor_posix.zig");
const ReactorWaitError = posix_helpers.ReactorWaitError;
pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

// ─── user_data encoding ──────────────────────────────────────────
//
// We multiplex four kinds of completions through the IOCP:
//
//   * Cancel-SQE acks      — user_data == CANCEL_SQE_USER_DATA (0x1).
//   * Interrupt eventfd    — user_data == INTERRUPT_USER_DATA (0x2).
//   * Multi-shot PD CQEs   — user_data == @intFromPtr(pd) | POLL_DESC_TAG.
//   * Timer CQEs           — user_data == @intFromPtr(coro).
//
// The tag picks bit 2 (= 0x4). Coroutine and PollDesc heap
// allocations are both ≥ 8-byte aligned (Volt's allocator default
// gives 8; `PollDesc` is `align(std.atomic.cache_line)` so ≥ 64),
// so bit 2 is naturally 0 in their addresses. Setting bit 2 on PD
// CQEs makes them trivially distinguishable from timer CQEs (which
// also carry a heap pointer) without consulting any side state.

const CANCEL_SQE_USER_DATA: u64 = 0x1;
const INTERRUPT_USER_DATA: u64 = 0x2;
const POLL_DESC_TAG: u64 = 0x4;

inline fn userDataForPd(pd: *poll_desc.PollDesc) u64 {
    const raw: u64 = @intFromPtr(pd);
    // Sanity: alignment must leave bit 2 free.
    std.debug.assert(raw & POLL_DESC_TAG == 0);
    return raw | POLL_DESC_TAG;
}

inline fn pdFromUserData(user_data: u64) *poll_desc.PollDesc {
    return @ptrFromInt(user_data & ~POLL_DESC_TAG);
}

inline fn isPdUserData(user_data: u64) bool {
    return (user_data & POLL_DESC_TAG) != 0 and
        user_data != CANCEL_SQE_USER_DATA and
        user_data != INTERRUPT_USER_DATA;
}

// ─── Per-PD backend state ────────────────────────────────────────
//
// Held in `pd.backend_data`. Tracks the multi-shot's terminal-CQE
// arrival so `unregisterFd` knows when it's safe to free the PD.
// The kernel guarantees: after `ASYNC_CANCEL` targeting our
// `user_data`, exactly one CQE without `IORING_CQE_F_MORE` arrives;
// after that CQE, no more CQEs reference our `user_data`.

const IoUringPdBackend = struct {
    /// Set true by the poll loop when it dispatches the terminal
    /// (no-F_MORE) CQE for this PD's multi-shot. `unregisterFd`
    /// spins on this before freeing — combined with the `in_poll`
    /// barrier, it guarantees no concurrent dispatch is still
    /// reading our PD's memory.
    drained: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    ring: linux.IoUring,
    /// eventfd used for cross-thread wake (`interrupt`). A single
    /// `POLL_ADD_MULTI` SQE on this fd, carrying the
    /// `INTERRUPT_USER_DATA` sentinel, is submitted at init and
    /// stays armed for the runtime's lifetime.
    interrupt_fd: c_int = -1,

    /// Sum of in-flight kernel registrations:
    /// - One per `registerFd` (the persistent multi-shot poll). Stays
    ///   incremented for the socket's lifetime; decremented when the
    ///   terminal CQE arrives after `unregisterFd`'s cancel.
    /// - One per `waitTimer*` (single-shot timer SQE). Decremented
    ///   when the timer fires or is cancelled.
    ///
    /// The interrupt eventfd's POLL_ADD does NOT count — it's a
    /// reactor-internal wake channel, not a coroutine park.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// TTAS spinlock around SQE production. The ring's SQ tail
    /// counter is shared mutable state; multiple workers calling
    /// `registerFd`/`unregisterFd`/`waitTimer*` need to be serialised
    /// on the producer side. Submission and CQE consumption are
    /// single-threaded by the poller-claim in `runtime.zig`.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// True while the poller is iterating the CQEs returned by a
    /// kernel `enter()` call. `unregisterFd` MUST wait for this to
    /// clear before freeing the PD backend: the kernel may have
    /// already transferred a CQE referencing our PD into a poller's
    /// user-space buffer, and that buffer is owned by the poller —
    /// the kernel can't reach into it to invalidate. The dispatch
    /// loop then dereferences `user_data` as `*PollDesc`, and if the
    /// closer has already freed the PD, that's a use-after-free.
    /// See `unregisterFd`'s drain spin.
    in_poll: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        // `flags = 0`: vanilla setup. `IORING_SETUP_SINGLE_ISSUER`
        // + `IORING_SETUP_DEFER_TASKRUN` would reduce kernel
        // coordination but require kernel ≥ 6.0 and a more careful
        // submission model (we'd have to route all production to
        // a single thread). Deferred until perf data justifies.
        var ring = linux.IoUring.init(DEFAULT_RING_ENTRIES, 0) catch return error.IoUringInitFailed;
        errdefer ring.deinit();

        const efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (efd < 0) return error.IoUringInitFailed;
        errdefer _ = posix_helpers.close(efd);

        // Arm the interrupt eventfd as a multi-shot POLL_ADD so a
        // single registration covers every subsequent interrupt
        // without per-wake re-arm. Single-threaded here (init runs
        // once, before any worker spawns) so no acquire/release.
        const sqe = ring.poll_add(INTERRUPT_USER_DATA, efd, POLLIN) catch return error.IoUringInitFailed;
        sqe.len = linux.IORING_POLL_ADD_MULTI;
        _ = ring.submit() catch return error.IoUringInitFailed;

        return .{ .ring = ring, .interrupt_fd = efd, .allocator = allocator };
    }

    pub fn deinit(self: *Reactor) void {
        // In-flight registrations at deinit means either a coroutine
        // is still parked in the kernel, or a multi-shot poll is
        // still armed. Asserting here catches the shutdown-ordering
        // bug at the source instead of a mysterious crash later.
        std.debug.assert(self.pending.load(.acquire) == 0);
        std.debug.assert(!self.in_poll.load(.acquire));
        if (self.interrupt_fd >= 0) _ = posix_helpers.close(self.interrupt_fd);
        self.interrupt_fd = -1;
        self.ring.deinit();
    }

    // ─── PollDesc-aware interface (Step 2f real implementation) ─────

    /// Register `pd` as the per-fd state machine for `fd`. Submits a
    /// multi-shot `POLL_ADD` covering both directions plus error
    /// conditions; `pd.deliverReady` is called for each event the
    /// kernel reports.
    ///
    /// Allocates an `IoUringPdBackend` and stores it in
    /// `pd.backend_data` — needed by `unregisterFd` to detect
    /// terminal-CQE arrival.
    pub fn registerFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) ReactorWaitError!void {
        const backend = self.allocator.create(IoUringPdBackend) catch
            return error.SystemResources;
        backend.* = .{};
        pd.backend_data = backend;
        errdefer {
            pd.backend_data = null;
            self.allocator.destroy(backend);
        }

        const mask: u32 = POLLIN | POLLOUT | POLLERR | POLLHUP | POLLRDHUP;
        const ud = userDataForPd(pd);

        self.acquire();
        const submitted = (self.ring.poll_add(ud, fd, mask) catch blk: {
            _ = self.ring.submit() catch {};
            break :blk self.ring.poll_add(ud, fd, mask) catch null;
        });
        if (submitted) |sqe| {
            sqe.len = linux.IORING_POLL_ADD_MULTI;
        }
        self.release();
        if (submitted == null) return error.SystemResources;

        _ = self.pending.fetchAdd(1, .acq_rel);
    }

    /// Reverse of `registerFd`. Submits `ASYNC_CANCEL` targeting our
    /// `user_data`, then drains until the kernel's terminal CQE
    /// arrives. The `in_poll` barrier guarantees no concurrent
    /// dispatch is still reading our PD's memory when we free it.
    pub fn unregisterFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) void {
        _ = fd;
        const backend_raw = pd.backend_data orelse return;
        const backend: *IoUringPdBackend = @ptrCast(@alignCast(backend_raw));

        // Submit ASYNC_CANCEL targeting our user_data. The kernel
        // posts:
        //   1. A cancel-ack CQE (CANCEL_SQE_USER_DATA — skipped).
        //   2. The cancelled multi-shot's terminal CQE (no F_MORE —
        //      sets backend.drained = true via the poll loop).
        const ud = userDataForPd(pd);
        self.acquire();
        // Submit any queued SQEs first so the target poll_add (if it
        // hasn't been flushed yet from a recent registerFd) is
        // visible to the kernel before the cancel SQE.
        _ = self.ring.submit() catch {};
        _ = self.ring.cancel(CANCEL_SQE_USER_DATA, ud, 0) catch {};
        _ = self.ring.submit() catch {};
        self.release();

        // Wake any worker blocked in poll() so it processes the
        // pending CQEs promptly.
        self.interrupt();

        // Drain until the terminal CQE has been dispatched:
        //   1. `poll(false)` pulls visible CQEs into this thread and
        //      dispatches them (the terminal CQE sets drained=true).
        //   2. Spin until `in_poll == false` — catches concurrent
        //      workers mid-dispatch with our PD in their buffer.
        //   3. If `drained == true`, we're safe to free.
        //   4. Otherwise loop — the kernel hasn't posted the terminal
        //      CQE yet (typically a handful of microseconds after the
        //      cancel SQE was consumed).
        var spins: u32 = 0;
        while (!backend.drained.load(.acquire)) {
            _ = self.poll(false);
            while (self.in_poll.load(.acquire)) std.atomic.spinLoopHint();
            if (backend.drained.load(.acquire)) break;
            spins += 1;
            // Safety bound: in practice the terminal CQE arrives in
            // <10 iterations. If we hit this we have a kernel bug
            // or a structural error — bail rather than hang.
            if (spins > 100_000) break;
            std.atomic.spinLoopHint();
        }

        pd.backend_data = null;
        self.allocator.destroy(backend);
    }

    /// Park the current coroutine until `fd` is ready in `mode`.
    /// Runs the per-fd state machine on `pd`; the kernel side stays
    /// armed across the call (persistent multi-shot registration).
    ///
    /// incref/decref around `pd.wait` so a concurrent socket-close
    /// (which calls `pd.closeAndWait` and frees the PD) can't free
    /// the PD while we're in the post-wake epilogue still touching
    /// its slot.
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

    /// Cancel-aware variant of `waitFd`. `pd.waitCancel` handles its
    /// own refcount and wake-via-state-machine; no kernel SQE for
    /// cancel is needed (the multi-shot stays alive, the parked coro
    /// just gets woken via `deliverCancel`).
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
        // ns=0 is a no-op. IORING_OP_TIMEOUT with an all-zero
        // timespec has historically had ambiguous behaviour across
        // io_uring versions — fail closed by short-circuiting.
        if (ns == 0) return;
        // __kernel_timespec.tv_sec is i64; ns > i64_max wraps after divide.
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        self.acquire();
        const submitted = (self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch blk: {
            _ = self.ring.submit() catch {};
            break :blk self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch null;
        }) != null;
        self.release();
        if (!submitted) return error.SystemResources;

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    /// Cancel-aware variant of `waitTimer`. The timeout SQE's
    /// `user_data` is `@intFromPtr(coro)` (untagged, so the poll
    /// loop's coro/PD discriminator works), so the existing
    /// `ASYNC_CANCEL`-by-user_data path matches it without any new
    /// bookkeeping.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        // Stash the user_data so cancelCoro can target it.
        var op = WaitOp{ .ident = @intFromPtr(me) };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        // Timeout SQE FIRST — same race rationale as the legacy
        // submitPollCancel: the target SQE must be visible to the
        // kernel before a cancel SQE referencing it goes in.
        self.acquire();
        const submitted = (self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch blk: {
            _ = self.ring.submit() catch {};
            break :blk self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch null;
        }) != null;
        self.release();
        if (!submitted) return error.SystemResources;
        _ = self.pending.fetchAdd(1, .acq_rel);

        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            self.acquire();
            _ = self.ring.submit() catch {};
            _ = self.ring.cancel(CANCEL_SQE_USER_DATA, @intFromPtr(me), 0) catch {};
            _ = self.ring.submit() catch {};
            self.release();
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
    };

    /// Cancel this coro's in-flight io_uring op. In the Step 2f
    /// model this only fires for timer parks (PD waits go through
    /// `pd.waitCancel`'s state machine without touching the reactor).
    ///
    /// Submit-then-cancel-then-submit ordering: the target SQE must
    /// be visible to the kernel before our cancel SQE goes in, or
    /// the cancel hits ENOENT and the original op runs to completion.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        const op_ptr = c.reactor_wait_op orelse return;
        const op: *WaitOp = @ptrCast(@alignCast(op_ptr));
        self.acquire();
        defer self.release();
        _ = self.ring.submit() catch {};
        _ = self.ring.cancel(CANCEL_SQE_USER_DATA, op.ident, 0) catch {
            _ = self.ring.submit() catch {};
            _ = self.ring.cancel(CANCEL_SQE_USER_DATA, op.ident, 0) catch {};
        };
        _ = self.ring.submit() catch {};
    }

    /// Close an fd that may have a PollDesc still registered. PD
    /// lifecycle (evict + closeAndWait + unregisterFd + free) is
    /// owned by the socket type via `pd_handle.release`, called
    /// BEFORE this fn. All we do here is the libc close — same
    /// as kqueue's persistent-registration model.
    pub fn closeFd(self: *Reactor, fd: i32) void {
        _ = self;
        _ = posix_helpers.close(fd);
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;

        // Two-phase: flush the SQ under the lock, then block in the
        // kernel WITHOUT the lock. Holding the SQ spinlock across a
        // blocking `io_uring_enter` deadlocks the firing path: a peer
        // coroutine's `unregisterFd` (or `waitTimerCancel`) needs the
        // same lock to push the cancel SQE that generates the CQE
        // we're waiting for.
        const wait_nr: u32 = if (blocking) 1 else 0;

        // Phase 1: push queued SQEs to the kernel under the lock.
        // `submit()` is non-blocking (wait_nr = 0 internally) — it
        // only flushes the SQ tail and tells the kernel via a short
        // `io_uring_enter` how many SQEs to consume.
        self.acquire();
        _ = self.ring.submit() catch {
            self.release();
            return 0;
        };
        self.release();

        // Phase 2: block waiting for ≥ wait_nr completions outside
        // the lock. `enter(0, wait_nr, GETEVENTS)` submits nothing
        // new — it just asks the kernel to wait for completions on
        // the already-submitted SQEs. Concurrent producers can now
        // acquire the lock, flush their own SQEs, and the kernel
        // can complete them while we wait on the same CQ.
        if (blocking) {
            _ = self.ring.enter(0, wait_nr, linux.IORING_ENTER_GETEVENTS) catch return 0;
        }

        // Set `in_poll` BEFORE reading CQEs. A concurrent
        // `unregisterFd` about to free a PD whose CQE the kernel
        // will transfer into our buffer MUST observe `in_poll == true`
        // for the entire window the buffer is live. See
        // `reactor_kqueue.zig:396` for the full rationale (the same
        // barrier discipline applies here).
        self.in_poll.store(true, .release);
        defer self.in_poll.store(false, .release);

        // CQE consumption is single-threaded by the
        // `reactor_poller_taken` claim in runtime.zig — no lock needed.
        var cqes: [CQE_BATCH]linux.io_uring_cqe = undefined;
        const n = self.ring.copy_cqes(&cqes, 0) catch return 0;
        if (n == 0) return 0;
        const count: usize = @intCast(n);
        var pending_dec: u32 = 0;
        var dispatched: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const c = &cqes[i];
            const ud = c.user_data;

            // Cancel SQE ack — skip (the cancelled op's own CQE
            // arrives separately).
            if (ud == CANCEL_SQE_USER_DATA) continue;

            // Interrupt eventfd fired. Multi-shot, so no re-arm
            // needed; just drain the counter to clear its readable
            // state. Doesn't count toward `pending`.
            if (ud == INTERRUPT_USER_DATA) {
                // Multi-shot POLL_ADD on the eventfd: drain the
                // counter to clear its readable state. No re-arm
                // needed — the kernel keeps reporting until the
                // multi-shot is cancelled (we never cancel it).
                var drain_buf: [8]u8 = undefined;
                _ = posix_helpers.read(self.interrupt_fd, &drain_buf, 8);
                continue;
            }

            dispatched += 1;

            if (isPdUserData(ud)) {
                // Multi-shot PD CQE. `c.res` is either a revents
                // bitmap (success) or a negative errno (the
                // registration was cancelled or the kernel detected
                // an error).
                const pd = pdFromUserData(ud);
                const more = (c.flags & linux.IORING_CQE_F_MORE) != 0;

                if (c.res >= 0) {
                    const revents: u32 = @intCast(c.res);
                    // POLLERR / POLLHUP are delivered alongside any
                    // direction-specific bit; for safety also dispatch
                    // to both modes on those signals so a parked
                    // reader on a hung-up fd wakes.
                    const wake_read = (revents & (POLLIN | POLLERR | POLLHUP | POLLRDHUP)) != 0;
                    const wake_write = (revents & (POLLOUT | POLLERR | POLLHUP)) != 0;
                    if (wake_read) {
                        if (pd.deliverReady(.read)) |coro| runtime.unpark(coro);
                    }
                    if (wake_write) {
                        if (pd.deliverReady(.write)) |coro| runtime.unpark(coro);
                    }
                } else {
                    // Negative res — typically -ECANCELED on the
                    // terminal CQE after `unregisterFd`'s cancel.
                    // No userland-visible wake here; the PD is being
                    // torn down and any waiters were already evicted
                    // via `pd.closeAndWait` before `unregisterFd` ran.
                }

                if (!more) {
                    // Terminal CQE — kernel guarantees no further
                    // completions for this user_data. Mark the
                    // backend drained so `unregisterFd` can free it,
                    // and decrement the per-registration `pending`
                    // entry that `registerFd` added.
                    if (pd.backend_data) |raw| {
                        const backend: *IoUringPdBackend = @ptrCast(@alignCast(raw));
                        backend.drained.store(true, .release);
                    }
                    pending_dec += 1;
                }
                continue;
            }

            // Timer CQE — user_data is a raw coroutine pointer.
            // Unpark and account for the per-timer `pending` entry.
            const coro: *coroutine.Coroutine = @ptrFromInt(ud);
            runtime.unpark(coro);
            pending_dec += 1;
        }
        if (pending_dec > 0) _ = self.pending.fetchSub(pending_dec, .acq_rel);
        return dispatched;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to call
    /// from any thread. The eventfd `write` is non-blocking; multiple
    /// interrupts before the first drain coalesce in the kernel's
    /// 8-byte counter.
    pub fn interrupt(self: *Reactor) void {
        const one: u64 = 1;
        var bytes: [8]u8 = undefined;
        @memcpy(&bytes, std.mem.asBytes(&one));
        _ = posix_helpers.write(self.interrupt_fd, &bytes, 8);
    }

    inline fn acquire(self: *Reactor) void {
        while (true) {
            while (self.lock.load(.monotonic) != 0) std.atomic.spinLoopHint();
            if (self.lock.cmpxchgWeak(0, 1, .acquire, .monotonic) == null) return;
        }
    }

    inline fn release(self: *Reactor) void {
        self.lock.store(0, .release);
    }
};

// Compile-time check: Linux-only.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_io_uring.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

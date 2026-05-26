//! Linux io_uring reactor — **persistent registration via POLL_ADD_MULTI**.
//!
//! Step 2f migration: structural parity with kqueue's Step 2d.2 and
//! epoll's Step 2f. Each socket's `PollDesc` is registered with
//! io_uring ONCE at socket creation (`registerFd`) via
//! `IORING_OP_POLL_ADD` with `IORING_POLL_ADD_MULTI` set — a single
//! SQE that produces a CQE every time the fd transitions to
//! readable / writable / errored, until cancelled. The `*PollDesc`
//! pointer rides in `user_data` (tagged to distinguish from timer /
//! interrupt / cancel-ack CQEs); the kernel registration lives for
//! the socket's lifetime and is retired by `ASYNC_CANCEL` in
//! `unregisterFd`.
//!
//! `waitFd(fd, pd, mode)`:
//!   1. `pd.incref()` / `defer pd.decref()` (PD lifecycle ref).
//!   2. `pd.wait(mode)` — pure user-space state machine.
//!   3. On `.ready`, return; caller retries the syscall.
//!   4. On `.closing`, return `error.BadDescriptor`.
//!
//! `poll(blocking)`:
//!   1. Flush the SQ under the producer `lock`.
//!   2. Block in `io_uring_enter(GETEVENTS)` outside the lock.
//!   3. Set `in_poll = true`; for each CQE, dispatch by `user_data`:
//!      - `CANCEL_SQE_USER_DATA` (sentinel) — cancel-ack, skip.
//!      - `INTERRUPT_USER_DATA` (sentinel) — drain eventfd, skip.
//!      - `pd | POLL_DESC_TAG` — multi-shot PD CQE. Parse revents
//!        in `cqe.res`, call `pd.deliverReady(.read)` / `(.write)`
//!        for each direction that fired. `IORING_CQE_F_MORE`
//!        distinguishes intermediate CQEs (multi-shot live) from
//!        the terminal CQE (registration retired — also sets
//!        `backend.drained` so `unregisterFd` can safely free).
//!      - other (raw coroutine pointer) — timer CQE; unpark.
//!   4. Clear `in_poll`.
//!
//! ## Critical invariant — `copy_cqes` is single-threaded
//!
//! CQE consumption goes through one path only: `poll()`, called by
//! the worker that has won `runtime.tryClaimPoller`. `unregisterFd`
//! MUST NOT call `poll()` directly — an earlier (reverted) attempt
//! did exactly that and produced torn reads on the CQ ring
//! (`cqe.user_data` came back as zero, dispatch routed it to the
//! timer branch, `@ptrFromInt(0)` panicked). The fix: `unregisterFd`
//! issues the cancel SQE + `interrupt()`s the blocking poller, then
//! yields (in coroutine context) or spin-hints (otherwise) while
//! waiting for the claimed poller to dispatch the terminal CQE and
//! flip `backend.drained`.
//!
//! ## Why this design eliminates the legacy per-wait races
//!
//! In the previous per-wait registration model (`Step 2b` shim),
//! `submitPoll` did `insertWaiter(fd, me)` BEFORE submitting the
//! `poll_add` SQE to the kernel. A concurrent `closeFd` found us in
//! the side table, submitted a cancel SQE — but our `poll_add`
//! hadn't reached the kernel yet. The cancel hit `-ENOENT`; our
//! `poll_add` then registered, and we parked forever (audit §4A).
//! The persistent-registration model eliminates this by
//! construction: the registration happens at socket open, before
//! any peer can see the fd; cancel is purely user-space
//! (`pd.waitCancel` runs the state machine), so no SQE
//! submission during wait.
//!
//! Timers (`waitTimer` / `waitTimerCancel`) keep per-wait SQE
//! submission since they're inherently single-shot. `cancelCoro`
//! and the `WaitOp` it consumes are timer-only in this model.
//!
//! ## Threading model
//!
//! `IoUring`'s SQE production is **not** thread-safe by default —
//! producers race on the SQ tail counter. Volt has multiple workers
//! concurrently submitting SQEs (registerFd from any worker,
//! cancel from unregisterFd / waitTimerCancel), so production
//! needs a lock. CQE consumption is single-threaded by the
//! poller-claim in `runtime.zig`.
//!
//! Critical invariant: the SQ lock MUST NOT be held across the
//! blocking `io_uring_enter(GETEVENTS)` call in `poll()`. A peer
//! coroutine's `unregisterFd` or `waitTimerCancel` needs the lock
//! to push the cancel SQE that generates the CQE we're waiting
//! for — if we hold it through the kernel wait, the firer can't
//! submit and we never wake. `poll()` is two-phase: flush the SQ
//! under the lock, then wait outside it.

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

const posix_helpers = @import("reactor_posix.zig");
const ReactorWaitError = posix_helpers.ReactorWaitError;
pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

// ─── user_data encoding ──────────────────────────────────────────
//
// Four CQE kinds multiplex through the IOCP:
//
//   * Cancel-SQE acks    — user_data == CANCEL_SQE_USER_DATA (0x1).
//   * Interrupt eventfd  — user_data == INTERRUPT_USER_DATA (0x2).
//   * Multi-shot PD CQEs — user_data == @intFromPtr(pd) | POLL_DESC_TAG.
//   * Timer CQEs         — user_data == @intFromPtr(coro).
//
// `POLL_DESC_TAG` picks bit 2 (= 0x4). Coroutine and PollDesc heap
// allocations are both ≥ 8-byte aligned (Volt's allocator default
// gives 8; `PollDesc` is `align(std.atomic.cache_line)` so ≥ 64),
// so bit 2 is naturally 0 in their addresses. Setting bit 2 on PD
// CQEs makes them trivially distinguishable from timer CQEs.

const CANCEL_SQE_USER_DATA: u64 = 0x1;
const INTERRUPT_USER_DATA: u64 = 0x2;
const POLL_DESC_TAG: u64 = 0x4;

inline fn userDataForPd(pd: *poll_desc.PollDesc) u64 {
    const raw: u64 = @intFromPtr(pd);
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
// arrival so `unregisterFd` knows when it's safe to free. The kernel
// guarantees: after `ASYNC_CANCEL` targeting our `user_data`,
// exactly one CQE without `IORING_CQE_F_MORE` arrives; after that
// CQE, no more CQEs reference our `user_data`. The poll loop sets
// `drained = true` on the terminal CQE.

const IoUringPdBackend = struct {
    drained: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    ring: linux.IoUring,
    /// eventfd used for cross-thread wake (`interrupt`). A single
    /// `POLL_ADD_MULTI` SQE on this fd, carrying `INTERRUPT_USER_DATA`,
    /// is submitted at init and stays armed for the runtime's
    /// lifetime.
    interrupt_fd: c_int = -1,

    /// In-flight kernel registrations:
    /// - One per `registerFd` (persistent multi-shot poll). Stays
    ///   incremented until the terminal CQE arrives after
    ///   `unregisterFd`'s cancel.
    /// - One per `waitTimer*` (single-shot timer SQE). Decremented
    ///   when the timer fires or is cancelled.
    ///
    /// The interrupt eventfd's POLL_ADD does NOT count — it's a
    /// reactor-internal wake channel.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// TTAS spinlock around SQE production.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// True while the poller is iterating CQEs returned by an
    /// `enter()` call. `unregisterFd` MUST wait for this to clear
    /// before freeing the PD backend — the kernel may have already
    /// transferred a CQE referencing our PD into a poller's
    /// user-space buffer. Same barrier discipline as kqueue
    /// (`reactor_kqueue.zig:95`).
    in_poll: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        var ring = linux.IoUring.init(DEFAULT_RING_ENTRIES, 0) catch return error.IoUringInitFailed;
        errdefer ring.deinit();

        const efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (efd < 0) return error.IoUringInitFailed;
        errdefer _ = posix_helpers.close(efd);

        // Multi-shot POLL_ADD on the interrupt eventfd: a single
        // registration serves every subsequent interrupt without
        // per-wake re-arm. Single-threaded here (init runs once,
        // before any worker spawns) so no acquire/release.
        const sqe = ring.poll_add(INTERRUPT_USER_DATA, efd, POLLIN) catch return error.IoUringInitFailed;
        sqe.len = linux.IORING_POLL_ADD_MULTI;
        _ = ring.submit() catch return error.IoUringInitFailed;

        return .{ .ring = ring, .interrupt_fd = efd, .allocator = allocator };
    }

    pub fn deinit(self: *Reactor) void {
        std.debug.assert(self.pending.load(.acquire) == 0);
        std.debug.assert(!self.in_poll.load(.acquire));
        if (self.interrupt_fd >= 0) _ = posix_helpers.close(self.interrupt_fd);
        self.interrupt_fd = -1;
        self.ring.deinit();
    }

    // ─── PollDesc-aware interface ───────────────────────────────────

    /// Register `pd` as the per-fd state machine for `fd`. Allocates
    /// the backend state and queues a multi-shot POLL_ADD SQE
    /// covering both directions + error conditions.
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
        const sqe = (self.ring.poll_add(ud, fd, mask) catch blk: {
            // SQ-full: drain and retry once.
            _ = self.ring.submit() catch {};
            break :blk self.ring.poll_add(ud, fd, mask) catch null;
        });
        if (sqe) |s| {
            s.len = linux.IORING_POLL_ADD_MULTI;
        }
        self.release();
        if (sqe == null) return error.SystemResources;

        _ = self.pending.fetchAdd(1, .acq_rel);
    }

    /// Reverse of `registerFd`. Submits `ASYNC_CANCEL` targeting
    /// our `user_data`, then waits for the kernel's terminal CQE to
    /// be dispatched by the runtime's claimed poller.
    ///
    /// ## Why we do NOT call `poll()` ourselves
    ///
    /// `copy_cqes` is single-threaded by `tryClaimPoller`. An earlier
    /// version of this function called `self.poll(false)` in the
    /// drain loop — observed in CI run 26434710271 to panic with
    /// "cast causes pointer to be null" on the close-while-parked
    /// test, because our `copy_cqes` raced the claimed poller's and
    /// produced torn reads with `cqe.user_data == 0`. The fix is to
    /// rely on the claimed poller for all CQE consumption.
    ///
    /// Mechanism:
    /// - Submit the cancel SQE under the producer lock.
    /// - `interrupt()` to wake any worker blocked in `poll(true)`.
    /// - In coroutine context: `yield()` between checks so the
    ///   current worker can dispatch other coroutines and eventually
    ///   reach its find-work + tryClaimPoller path.
    /// - Outside coroutine context (driver-thread teardown after
    ///   `rt.run`): workers M[1..N-1] are still in their dispatch
    ///   loops; spin-hint between checks.
    /// - Either way, the claimed poller eventually processes the
    ///   terminal (no-F_MORE) CQE, which sets `backend.drained`.
    /// - The `in_poll` spin between checks catches any concurrent
    ///   dispatcher that copied a CQE referencing our PD into its
    ///   `cqes[]` buffer before we look at `drained`.
    pub fn unregisterFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) void {
        _ = fd;
        const backend_raw = pd.backend_data orelse return;
        const backend: *IoUringPdBackend = @ptrCast(@alignCast(backend_raw));

        // Submit ASYNC_CANCEL targeting our multi-shot's user_data.
        // Drain-and-retry on SQ-full mirrors the registerFd pattern.
        const ud = userDataForPd(pd);
        self.acquire();
        // Flush any queued SQEs first — guarantees the multi-shot
        // poll_add (if it hasn't been flushed yet from a recent
        // registerFd) is visible to the kernel before our cancel SQE.
        _ = self.ring.submit() catch {};
        const cancel_sqe = self.ring.cancel(CANCEL_SQE_USER_DATA, ud, 0) catch blk: {
            _ = self.ring.submit() catch {};
            break :blk self.ring.cancel(CANCEL_SQE_USER_DATA, ud, 0) catch null;
        };
        _ = cancel_sqe;
        _ = self.ring.submit() catch {};
        self.release();

        self.interrupt();

        // Wait for the claimed poller to dispatch the terminal CQE.
        // The `drained` flag is set by `poll()` when it sees a CQE
        // for our PD without `IORING_CQE_F_MORE`.
        const in_coro = current.get() != null;
        while (!backend.drained.load(.acquire)) {
            // Catch any concurrent dispatcher that's mid-loop with
            // our PD in its `cqes[]` buffer.
            while (self.in_poll.load(.acquire)) std.atomic.spinLoopHint();
            if (backend.drained.load(.acquire)) break;

            if (in_coro) {
                // Yielding lets the current worker dispatch other
                // coroutines and eventually reach find-work +
                // tryClaimPoller, where it will enter poll() and
                // process our terminal CQE.
                runtime.yield();
            } else {
                // Driver-thread teardown context — workers M[1..]
                // are still running their loops; just spin while
                // they reach poll().
                std.atomic.spinLoopHint();
            }

            // Re-fire the interrupt. The first interrupt may have
            // been consumed by a poller that returned before our
            // cancel SQE reached the kernel; subsequent interrupts
            // ensure the poller cycles again.
            self.interrupt();
        }

        pd.backend_data = null;
        self.allocator.destroy(backend);
    }

    /// Park the current coroutine until `fd` is ready in `mode`.
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

    /// Cancel-aware variant of `waitFd`.
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
        if (ns == 0) return;
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

    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        var op = WaitOp{ .ident = @intFromPtr(me) };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

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

    /// Per-call stash for cancel-aware waits — timer path only.
    pub const WaitOp = struct {
        ident: usize,
    };

    /// Cancel this coro's in-flight timer SQE. PD waits go through
    /// `pd.waitCancel`'s state machine and don't touch this path.
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
    /// owned by `pd_handle.release`, called BEFORE this fn.
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
        // kernel WITHOUT the lock. Holding the SQ lock across
        // `io_uring_enter` deadlocks the firing path.
        const wait_nr: u32 = if (blocking) 1 else 0;

        self.acquire();
        _ = self.ring.submit() catch {
            self.release();
            return 0;
        };
        self.release();

        if (blocking) {
            _ = self.ring.enter(0, wait_nr, linux.IORING_ENTER_GETEVENTS) catch return 0;
        }

        // Set in_poll BEFORE consuming CQEs. A concurrent
        // unregisterFd about to free a PD whose CQE the kernel
        // transferred into our buffer MUST observe in_poll == true
        // for the entire window the buffer is live.
        self.in_poll.store(true, .release);
        defer self.in_poll.store(false, .release);

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

            if (ud == CANCEL_SQE_USER_DATA) continue;

            if (ud == INTERRUPT_USER_DATA) {
                // Multi-shot eventfd POLL_ADD: drain the counter to
                // clear its readable state. No re-arm needed.
                var drain_buf: [8]u8 = undefined;
                _ = posix_helpers.read(self.interrupt_fd, &drain_buf, 8);
                continue;
            }

            dispatched += 1;

            if (isPdUserData(ud)) {
                const pd = pdFromUserData(ud);
                const more = (c.flags & linux.IORING_CQE_F_MORE) != 0;

                if (c.res >= 0) {
                    const revents: u32 = @intCast(c.res);
                    // POLLERR / POLLHUP wake both directions so a
                    // hung-up fd doesn't strand parked readers or
                    // writers.
                    const wake_read = (revents & (POLLIN | POLLERR | POLLHUP | POLLRDHUP)) != 0;
                    const wake_write = (revents & (POLLOUT | POLLERR | POLLHUP)) != 0;
                    if (wake_read) {
                        if (pd.deliverReady(.read)) |coro| runtime.unpark(coro);
                    }
                    if (wake_write) {
                        if (pd.deliverReady(.write)) |coro| runtime.unpark(coro);
                    }
                }
                // Negative c.res is typically -ECANCELED on the
                // terminal CQE after unregisterFd's cancel; no
                // userland-visible wake here (the PD is being torn
                // down and any waiters were already evicted by
                // pd.closeAndWait before unregisterFd ran).

                if (!more) {
                    // Terminal CQE — kernel guarantees no further
                    // completions for this user_data. Set drained
                    // so unregisterFd can free the backend, and
                    // decrement the per-registration `pending`
                    // entry that registerFd added.
                    if (pd.backend_data) |raw| {
                        const backend: *IoUringPdBackend = @ptrCast(@alignCast(raw));
                        backend.drained.store(true, .release);
                    }
                    pending_dec += 1;
                }
                continue;
            }

            // Timer CQE — `ud` is a raw coroutine pointer.
            const coro: *coroutine.Coroutine = @ptrFromInt(ud);
            runtime.unpark(coro);
            pending_dec += 1;
        }
        if (pending_dec > 0) _ = self.pending.fetchSub(pending_dec, .acq_rel);
        return dispatched;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to
    /// call from any thread. The eventfd write is non-blocking;
    /// multiple interrupts before the first drain coalesce in the
    /// kernel's 8-byte counter.
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

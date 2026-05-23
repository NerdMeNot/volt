//! Linux io_uring reactor — **poll mode**.
//!
//! Uses `IORING_OP_POLL_ADD` for fd readiness and `IORING_OP_TIMEOUT`
//! for sleeps. Same readiness contract as kqueue / epoll — the
//! caller does its own `read` / `write` after a wake — so `net.zig`
//! works unchanged. The buffer-ownership ops (`IORING_OP_RECV` /
//! `_SEND`) that give io_uring its full perf advantage are
//! deliberately not used; they'd force a parallel completion-based
//! path in `net.zig`, which is out of scope for this landing.
//!
//! ## Why io_uring at all if we're not using completion mode?
//!
//! Even in poll mode, io_uring beats epoll on the syscall path:
//!
//! - **Batched submission.** Each `waitFd` / `waitTimer` enqueues a
//!   submission queue entry (SQE) without invoking a syscall. Only
//!   when the dispatcher calls `poll(blocking=true)` do we
//!   `submit_and_wait` — that single syscall submits the entire
//!   pending batch and waits for at least one completion.
//! - **Userspace ring polling.** The SQ/CQ rings are mmap'd shared
//!   memory; producers and consumers communicate via the rings
//!   without crossing the syscall boundary at all in the steady
//!   state.
//!
//! Result: high-concurrency I/O workloads do fewer
//! `io_uring_enter` syscalls per RTT than they would `epoll_ctl` +
//! `epoll_wait`. Expected ~10–20% TCP throughput improvement on
//! loopback. Real workloads vary.
//!
//! ## Threading model
//!
//! `IoUring`'s SQE production is **not** thread-safe by default —
//! producers race on the SQ tail counter. Volt has multiple workers
//! concurrently parking coroutines on I/O, so SQE production needs
//! a lock. Submission and CQE consumption stay on the single
//! claimed poller worker (`runtime.tryClaimPoller`), so no
//! additional sync there.
//!
//! A future optimisation could use `IORING_SETUP_SINGLE_ISSUER` to
//! shard production per-worker via a fan-in ring; for now the
//! spinlock is fine — production is rare in steady-state TCP
//! workloads (one SQE per EAGAIN, not per byte).

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const cancel_mod = @import("cancel.zig");

const linux = std.os.linux;
const CQE_BATCH: usize = 32;
const DEFAULT_RING_ENTRIES: u16 = 256;

// Linux poll(2) mask bits — io_uring's POLL_ADD uses the same encoding.
const POLLIN: u32 = 0x0001;
const POLLOUT: u32 = 0x0004;

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

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    ring: linux.IoUring,
    /// eventfd used for cross-thread wake (`interrupt`). A
    /// `POLL_ADD` SQE on this fd, carrying the `INTERRUPT_USER_DATA`
    /// sentinel, is submitted at init and re-armed after each wake.
    interrupt_fd: c_int = -1,

    /// In-flight registrations (one per coroutine parked on an SQE).
    /// Producers (waitFd / waitTimer) increment under `lock`; the
    /// poller decrements as it drains CQEs. Same role as kqueue's
    /// `pending`.
    ///
    /// The interrupt eventfd's POLL_ADD does NOT count here — it's
    /// a reactor-internal wake channel, not a coroutine park.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// TTAS spinlock around SQE production. The ring's SQ tail
    /// counter is shared mutable state; multiple workers calling
    /// `waitFd` need to be serialised on the producer side.
    /// Submission and CQE consumption are single-threaded by the
    /// poller-claim in `runtime.zig`.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// Reverse lookup `fd → parked coro`. closeFd uses it to find
    /// the parked waiter and submit a cancel SQE targeting its
    /// user_data (== coro pointer). io_uring's cancellation model
    /// is async — the cancel SQE eventually produces a CQE that the
    /// poll loop dispatches — so unlike kqueue/epoll, no `op.closed`
    /// flag is needed. The coro resumes via the normal CQE path
    /// (with -ECANCELED) and the next libc recv after our close
    /// surfaces EBADF, which `errnoToIoError` already maps to
    /// BadDescriptor.
    fd_waiters: std.AutoHashMapUnmanaged(i32, *coroutine.Coroutine) = .{},
    fd_waiters_lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
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

        // Arm the interrupt POLL_ADD. Single-threaded here (init is
        // called once, before any worker spawns), so no acquire/
        // release needed.
        _ = ring.poll_add(INTERRUPT_USER_DATA, efd, POLLIN) catch return error.IoUringInitFailed;
        _ = ring.submit() catch return error.IoUringInitFailed;

        return .{ .ring = ring, .interrupt_fd = efd, .allocator = allocator };
    }

    inline fn lockWaiters(self: *Reactor) void {
        while (self.fd_waiters_lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    inline fn unlockWaiters(self: *Reactor) void {
        self.fd_waiters_lock.store(0, .release);
    }
    fn insertWaiter(self: *Reactor, fd: i32, c: *coroutine.Coroutine) std.mem.Allocator.Error!void {
        self.lockWaiters();
        defer self.unlockWaiters();
        try self.fd_waiters.put(self.allocator, fd, c);
    }
    fn removeWaiter(self: *Reactor, fd: i32) ?*coroutine.Coroutine {
        self.lockWaiters();
        defer self.unlockWaiters();
        if (self.fd_waiters.fetchRemove(fd)) |kv| return kv.value;
        return null;
    }

    /// Close a fd whose io_uring POLL_ADD may have a parked
    /// coroutine waiting on it. Mirrors reactor_kqueue.zig closeFd
    /// in spirit but uses io_uring's async-cancel model: submit a
    /// cancel SQE targeting the parked coro's user_data, then close
    /// the fd. The cancel CQE eventually delivers and the normal
    /// poll-loop dispatch path wakes the coro with the (cancelled)
    /// completion; the next libc recv/send after the close gets
    /// EBADF, surfacing as BadDescriptor via errnoToIoError. No
    /// `op.closed` flag is needed because the EBADF path is
    /// deterministic — io_uring's POLL_ADD operates on the fd by
    /// number, and the kernel returns -EBADF as soon as the fd is
    /// gone.
    pub fn closeFd(self: *Reactor, fd: i32) void {
        if (self.removeWaiter(fd)) |c| {
            self.acquire();
            // Submit-then-cancel-then-submit, same rationale as in
            // cancelCoro — the target poll_add must reach the kernel
            // before the cancel SQE.
            _ = self.ring.submit() catch {};
            _ = self.ring.cancel(CANCEL_SQE_USER_DATA, @intFromPtr(c), 0) catch {};
            _ = self.ring.submit() catch {};
            self.release();
        }
        _ = posix_helpers.close(fd);
    }

    pub fn deinit(self: *Reactor) void {
        // In-flight ops at deinit would have their completion
        // posted to a no-longer-mapped CQ ring after teardown —
        // kernel-side undefined behaviour. Asserting catches the
        // shutdown-ordering bug at the source instead of as a
        // mysterious crash later.
        std.debug.assert(self.pending.load(.acquire) == 0);
        std.debug.assert(self.fd_waiters.count() == 0);
        self.fd_waiters.deinit(self.allocator);
        if (self.interrupt_fd >= 0) _ = posix_helpers.close(self.interrupt_fd);
        self.interrupt_fd = -1;
        self.ring.deinit();
    }

    pub fn waitReadable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(@intCast(fd), POLLIN);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.waitFd(@intCast(fd), POLLOUT);
    }

    fn waitFd(self: *Reactor, fd: i32, mask: u32) ReactorWaitError!void {
        const me = current.require();

        // ─── Side-table insert FIRST, poll_add SQE second ──────────────
        //
        // See reactor_kqueue.zig waitFd: a racing closeFd between
        // SQE submission and side-table insert orphans the parked
        // coroutine. Side-table first means closeFd finds us before
        // it submits the cancel SQE; our subsequent poll_add either
        // succeeds (closeFd's cancel will land on it) or fails with
        // EBADF after the fd is closed (registerError-style surface).
        self.insertWaiter(fd, me) catch return error.SystemResources;
        errdefer _ = self.removeWaiter(fd);

        self.acquire();
        const submitted = (self.ring.poll_add(@intFromPtr(me), fd, mask) catch blk: {
            // SQ-full is recoverable: a submit hands queued SQEs to
            // the kernel and frees up user-space slots. Drain and
            // retry once before giving up.
            _ = self.ring.submit() catch {};
            break :blk self.ring.poll_add(@intFromPtr(me), fd, mask) catch null;
        }) != null;
        self.release();
        if (!submitted) return error.SystemResources;

        // SQE is queued but not yet submitted. The dispatcher's
        // poll() call will submit_and_wait — that batched syscall
        // is the whole perf win over epoll.
        _ = self.pending.fetchAdd(1, .acq_rel);

        defer _ = self.removeWaiter(fd);

        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

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

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    /// Sentinel `user_data` for cancel SQEs themselves — the poll
    /// path skips CQEs carrying this value (they're acknowledgement
    /// completions for `ASYNC_CANCEL` ops, not parked coroutines).
    /// Zero would alias the null pointer at @ptrFromInt time, so we
    /// pick a value that can't collide with any real coroutine
    /// address (heap allocations are at least 8-byte aligned;
    /// 0x1 is reserved as our sentinel).
    const CANCEL_SQE_USER_DATA: u64 = 0x1;

    /// Sentinel `user_data` for the interrupt-eventfd POLL_ADD SQE.
    /// Distinct from `CANCEL_SQE_USER_DATA` so the poll loop can
    /// distinguish "interrupt fired — drain + re-arm" from "cancel
    /// SQE acknowledged — skip".
    const INTERRUPT_USER_DATA: u64 = 0x2;

    /// Cancel this coro's in-flight io_uring op.
    ///
    /// Unlike kqueue/epoll, io_uring's `ASYNC_CANCEL` produces a
    /// CQE for the cancelled op (with a Cancelled status). The
    /// poll path picks it up and unparks via the normal route —
    /// we just submit the cancel and let the kernel arbitrate.
    /// No double-unpark / double-decrement risk here because there
    /// is exactly one CQE per submitted op.
    ///
    /// `user_data` for the original op == `@intFromPtr(coro)`
    /// (set in `waitFd`/`waitTimer`), so no per-coro stash needed.
    /// The CANCEL SQE itself carries the sentinel `user_data` so
    /// the poll loop discards its own completion.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        self.acquire();
        defer self.release();
        // Drain pending SQEs FIRST so the target user_data is visible
        // to the kernel before our cancel SQE goes in. Without this
        // submit, a cancel batched with a not-yet-submitted poll_add
        // it targets can race: the kernel may process the cancel
        // before the poll_add is registered, the cancel hits -ENOENT,
        // and the coro then parks on the poll_add forever. Per
        // io_uring_prep_cancel(3): submit the target in one batch,
        // then submit the cancel in a separate batch. This is the
        // root cause of the Linux CI recvCancel stress hang that
        // local Mac kqueue + un-loaded podman never exposed; it
        // surfaces under contention where the firer coroutine reaches
        // cancelCoro before any worker has called submit_and_wait.
        _ = self.ring.submit() catch {};
        _ = self.ring.cancel(CANCEL_SQE_USER_DATA, @intFromPtr(c), 0) catch {
            // SQ full even after the proactive submit — extremely
            // rare. Drain once more and retry; if that fails too,
            // silently return — the original op will eventually
            // complete via the normal CQE path (fd close, hangup, or
            // peer activity), and the cancel waiter's `c.isFired()`
            // check will surface error.Cancelled when the coro wakes.
            _ = self.ring.submit() catch {};
            _ = self.ring.cancel(CANCEL_SQE_USER_DATA, @intFromPtr(c), 0) catch {};
        };
        _ = self.ring.submit() catch {};
    }

    pub fn waitReadableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(@intCast(fd), POLLIN, c);
    }

    pub fn waitWritableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.waitFdCancel(@intCast(fd), POLLOUT, c);
    }

    fn waitFdCancel(self: *Reactor, fd: i32, mask: u32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        const me = current.require();

        // ─── Side-table insert FIRST (closes close-vs-register race) ──────
        //
        // See reactor_kqueue.zig waitFd for the rationale.
        self.insertWaiter(fd, me) catch return error.SystemResources;
        defer _ = self.removeWaiter(fd);

        // ─── SQE submission SECOND (closes register-then-fire race) ──────
        //
        // Old order — registerReactor then waitFd — opened a window
        // where fire() could call cancelCoro before this coroutine's
        // poll_add SQE was queued. The cancel SQE would land in the
        // ring before the target op SQE; the kernel processes them
        // in submission order so the cancel hits -ENOENT, the
        // poll_add then registers, and the coroutine parks forever.
        // Queueing the poll_add SQE BEFORE registerReactor guarantees
        // the kernel sees it before any later cancel SQE from
        // cancelCoro (the lock around .acquire()/.release()
        // serialises queue order across threads).
        self.acquire();
        const submitted = (self.ring.poll_add(@intFromPtr(me), fd, mask) catch blk: {
            _ = self.ring.submit() catch {};
            break :blk self.ring.poll_add(@intFromPtr(me), fd, mask) catch null;
        }) != null;
        self.release();
        if (!submitted) return error.SystemResources;
        _ = self.pending.fetchAdd(1, .acq_rel);

        // ─── Cancel-list registration THIRD ─────────────────────────────
        var w = cancel_mod.Waiter{};
        if (c.registerReactor(&w, me)) {
            // Fired before our register. Issue a CANCEL SQE
            // ourselves to retire the orphan poll_add; its CQE will
            // be delivered to poll() and runtime.unpark'd, which
            // (since we never park) just transitions park_state
            // RUNNING → NOTIFIED — harmless once we return.
            // Submit the poll_add FIRST so the kernel has it before
            // the cancel SQE goes in (see cancelCoro for the
            // io_uring batching race rationale).
            self.acquire();
            _ = self.ring.submit() catch {};
            _ = self.ring.cancel(CANCEL_SQE_USER_DATA, @intFromPtr(me), 0) catch {};
            _ = self.ring.submit() catch {};
            self.release();
            return error.Cancelled;
        }
        defer c.deregister(&w);

        // ─── Park ───────────────────────────────────────────────────────
        //
        // park_state RUNNING → NOTIFIED handles fire() landing in
        // the window between registerReactor returning and the swap
        // below. See runtime.zig:160 for the machine's contract.
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Cancel-aware variant of `waitTimer`. The timeout SQE's
    /// `user_data` is `@intFromPtr(coro)` (same as fd waits), so
    /// the existing `ASYNC_CANCEL`-by-user_data path in
    /// `cancelCoro` matches it without any new bookkeeping.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        try c.checkpoint();
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        // Timeout SQE FIRST — same race rationale as waitFdCancel.
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
            // Submit-then-cancel-then-submit, same rationale as in
            // cancelCoro — the target SQE must reach the kernel
            // before the cancel SQE.
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

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;

        // Submit any queued SQEs (the batched accumulation from
        // every waitFd / waitTimer since the last poll) and wait
        // for ≥ 1 completion if blocking.
        const wait_nr: u32 = if (blocking) 1 else 0;

        self.acquire();
        _ = self.ring.submit_and_wait(wait_nr) catch {
            self.release();
            return 0;
        };
        self.release();

        var cqes: [CQE_BATCH]linux.io_uring_cqe = undefined;
        const n = self.ring.copy_cqes(&cqes, 0) catch return 0;
        if (n == 0) return 0;
        const count: usize = @intCast(n);
        var real_count: usize = 0;
        var saw_interrupt = false;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            // CANCEL SQE acknowledgements arrive with the sentinel
            // user_data — skip them (the cancelled op's own CQE
            // arrives separately and carries the real coro ptr).
            if (cqes[i].user_data == CANCEL_SQE_USER_DATA) continue;
            // Interrupt POLL_ADD fired: drain the eventfd counter
            // (clears its readable state) and flag for re-arm.
            // Doesn't count toward `pending`.
            if (cqes[i].user_data == INTERRUPT_USER_DATA) {
                var drain_buf: [8]u8 = undefined;
                _ = posix_helpers.read(self.interrupt_fd, &drain_buf, 8);
                saw_interrupt = true;
                continue;
            }
            real_count += 1;
            // `user_data` for normal ops is the coroutine pointer
            // we set in waitFd/waitTimer. Cast back and unpark.
            const coro: *coroutine.Coroutine = @ptrFromInt(cqes[i].user_data);
            runtime.unpark(coro);
        }
        // Pending was incremented per real-op submit, not per
        // cancel SQE — only decrement by the real-op count.
        if (real_count > 0) _ = self.pending.fetchSub(@intCast(real_count), .acq_rel);

        // Re-arm the interrupt POLL_ADD if it fired. POLL_ADD is
        // single-shot in io_uring (unlike epoll's level-triggered
        // mode), so each wake needs a fresh SQE.
        if (saw_interrupt) {
            self.acquire();
            _ = self.ring.poll_add(INTERRUPT_USER_DATA, self.interrupt_fd, POLLIN) catch {};
            _ = self.ring.submit() catch {};
            self.release();
        }
        return real_count;
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

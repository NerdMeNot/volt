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

const linux = std.os.linux;
const CQE_BATCH: usize = 32;
const DEFAULT_RING_ENTRIES: u16 = 256;

// Linux poll(2) mask bits — io_uring's POLL_ADD uses the same encoding.
const POLLIN: u32 = 0x0001;
const POLLOUT: u32 = 0x0004;

// Shared POSIX helpers — same setNonblock / readAsync / writeAsync
// / readFull / writeAll as kqueue + epoll. The reactor-facing
// helpers take the reactor as `anytype`, so they bind to this
// backend at the call site without a shared base class.
const posix_helpers = @import("reactor_posix.zig");
pub const setNonblock = posix_helpers.setNonblock;
pub const readAsync = posix_helpers.readAsync;
pub const writeAsync = posix_helpers.writeAsync;
pub const readFull = posix_helpers.readFull;
pub const writeAll = posix_helpers.writeAll;

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    ring: linux.IoUring,

    /// In-flight registrations (one per coroutine parked on an SQE).
    /// Producers (waitFd / waitTimer) increment under `lock`; the
    /// poller decrements as it drains CQEs. Same role as kqueue's
    /// `pending`.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// TTAS spinlock around SQE production. The ring's SQ tail
    /// counter is shared mutable state; multiple workers calling
    /// `waitFd` need to be serialised on the producer side.
    /// Submission and CQE consumption are single-threaded by the
    /// poller-claim in `runtime.zig`.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init() !Reactor {
        // `flags = 0`: vanilla setup. `IORING_SETUP_SINGLE_ISSUER`
        // + `IORING_SETUP_DEFER_TASKRUN` would reduce kernel
        // coordination but require kernel ≥ 6.0 and a more careful
        // submission model (we'd have to route all production to
        // a single thread). Deferred until perf data justifies.
        const ring = linux.IoUring.init(DEFAULT_RING_ENTRIES, 0) catch return error.IoUringInitFailed;
        return .{ .ring = ring };
    }

    pub fn deinit(self: *Reactor) void {
        self.ring.deinit();
    }

    pub fn waitReadable(self: *Reactor, fd: i32) void {
        self.waitFd(@intCast(fd), POLLIN);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) void {
        self.waitFd(@intCast(fd), POLLOUT);
    }

    fn waitFd(self: *Reactor, fd: i32, mask: u32) void {
        const me = current.require();

        self.acquire();
        self.ring.poll_add(@intFromPtr(me), fd, mask) catch {
            // SQ-full is recoverable: a submit hands queued SQEs to
            // the kernel and frees up user-space slots. Drain and
            // retry once before giving up.
            _ = self.ring.submit() catch {};
            _ = self.ring.poll_add(@intFromPtr(me), fd, mask) catch {
                self.release();
                std.debug.panic("io_uring: SQ still full after submit (ring_size={d}; consider raising DEFAULT_RING_ENTRIES)", .{DEFAULT_RING_ENTRIES});
            };
        };
        self.release();

        // SQE is queued but not yet submitted. The dispatcher's
        // poll() call will submit_and_wait — that batched syscall
        // is the whole perf win over epoll.
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    pub fn waitTimer(self: *Reactor, ns: u64) void {
        const me = current.require();
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        self.acquire();
        self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch {
            _ = self.ring.submit() catch {};
            _ = self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch {
                self.release();
                std.debug.panic("io_uring: SQ still full after submit on timeout (ring_size={d})", .{DEFAULT_RING_ENTRIES});
            };
        };
        self.release();

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
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
        _ = self.pending.fetchSub(@intCast(count), .acq_rel);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            // `user_data` is the coroutine pointer we set in
            // waitFd/waitTimer. Cast back and unpark.
            const coro: *coroutine.Coroutine = @ptrFromInt(cqes[i].user_data);
            runtime.unpark(coro);
        }
        return count;
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

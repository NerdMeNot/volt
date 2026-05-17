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

extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
extern "c" fn close(fd: c_int) c_int;
extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;
extern "c" fn __errno_location() *c_int;

inline fn errnoVal() c_int {
    return __errno_location().*;
}

const EAGAIN: c_int = 11;

inline fn isAgain(e: c_int) bool {
    return e == EAGAIN;
}

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
        _ = self.ring.poll_add(@intFromPtr(me), fd, mask) catch {
            self.release();
            @panic("io_uring: SQ full (consider raising ring_size)");
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
        _ = self.ring.timeout(@intFromPtr(me), &ts, 0, 0) catch {
            self.release();
            @panic("io_uring: SQ full on timeout (consider raising ring_size)");
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

// ─────────────────────────────────────────────────────────────────────
// Non-blocking IO helpers (parallel to reactor_kqueue.zig's)
// ─────────────────────────────────────────────────────────────────────

const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const O_NONBLOCK: c_int = 0o4000;

pub fn setNonblock(fd: i32) !void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) !usize {
    while (true) {
        const r = read(@intCast(fd), buf.ptr, buf.len);
        if (r >= 0) return @intCast(r);
        const e = errnoVal();
        if (!isAgain(e)) return error.ReadFailed;
        rx.waitReadable(fd);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) !usize {
    while (true) {
        const w = write(@intCast(fd), buf.ptr, buf.len);
        if (w >= 0) return @intCast(w);
        const e = errnoVal();
        if (!isAgain(e)) return error.WriteFailed;
        rx.waitWritable(fd);
    }
}

pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, buf[total..]);
        total += w;
    }
}

// Compile-time check: Linux-only.
comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_io_uring.zig is Linux-only; src/reactor.zig dispatches by os.tag");
    }
}

//! Linux io_uring reactor backend.
//!
//! Same public interface as `reactor_kqueue.zig` and `reactor_epoll.zig`
//! (Reactor + EventKind + WaitKey + init/deinit/pendingCount/tickle/
//! registerWait/registerTimer/poll). Built on top of
//! `std.os.linux.IoUring`.
//!
//! ## Status
//!
//! Parallel backend; NOT the default. The reactor dispatcher in
//! `reactor.zig` selects epoll on Linux. To opt in to io_uring today,
//! edit the dispatcher's Linux arm to `@import("reactor_iouring.zig")`
//! (one-line change). A runtime `Config { .reactor_backend = .iouring }`
//! is planned. The current epoll path is the default because:
//!
//! 1. **Kernel version**: io_uring needs Linux ≥ 5.4 for the ops we
//!    use (POLL_ADD + TIMEOUT). Some distros still ship older kernels
//!    in their LTS streams.
//! 2. **Validation surface**: cross-compile catches API/type errors
//!    but not kernel-interaction bugs. CI matrix runs on actual Linux
//!    boxes — gating io_uring as default on green CI is a follow-up.
//!
//! ## Implementation
//!
//! - `registerWait(fd, kind, target)` → `IoUring.poll_add` with
//!   `user_data = @intFromPtr(target)`.
//! - `registerTimer(duration, target)` → `IoUring.timeout` with a
//!   `kernel_timespec`.
//! - `poll(timeout, ctx, wakeFn)` → `submit_and_wait(1)` + `copy_cqes`,
//!   then dispatch each completion's `user_data` to `wakeFn(target)`.
//! - `tickle()` → write 1 to an eventfd that's pre-registered with a
//!   poll_add. The polling worker's `submit_and_wait` returns when the
//!   eventfd CQE fires.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;
const IoUring = std.os.linux.IoUring;

const syscall = @import("../internal/syscall.zig");
const Mutex = @import("../internal/thread.zig").Mutex;

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_iouring.zig is for Linux only");
    }
}

pub const EventKind = enum(u8) { readable, writable };

fn pollEventsFor(kind: EventKind) u32 {
    return switch (kind) {
        .readable => linux.POLL.IN,
        .writable => linux.POLL.OUT,
    };
}

const TICKLE_TAG: u64 = 1;

pub const Reactor = struct {
    ring: IoUring,
    tickle_fd: i32,
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    submit_mutex: Mutex = .{},
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        var ring = try IoUring.init(256, 0);
        errdefer ring.deinit();

        const tfd_raw = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        const tfd: i32 = @intCast(tfd_raw);
        if (tfd < 0) return error.EventfdCreateFailed;
        errdefer syscall.close(tfd);

        // Pre-register the tickle eventfd with a poll_add SQE.
        const sqe = try ring.get_sqe();
        sqe.prep_poll_add(tfd, linux.POLL.IN);
        sqe.user_data = TICKLE_TAG;
        _ = try ring.submit();

        return .{
            .ring = ring,
            .tickle_fd = tfd,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        self.ring.deinit();
        syscall.close(self.tickle_fd);
    }

    pub fn pendingCount(self: *const Reactor) usize {
        return self.pending.load(.acquire);
    }

    pub fn tickle(self: *Reactor) void {
        const one: u64 = 1;
        const buf: []const u8 = std.mem.asBytes(&one);
        _ = syscall.write(self.tickle_fd, buf) catch {};
    }

    pub fn registerWait(
        self: *Reactor,
        fd: posix.fd_t,
        kind: EventKind,
        target: *anyopaque,
    ) !void {
        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();

        const sqe = try self.ring.get_sqe();
        sqe.prep_poll_add(fd, pollEventsFor(kind));
        sqe.user_data = @intFromPtr(target);
        _ = try self.ring.submit();
        _ = self.pending.fetchAdd(1, .release);
    }

    /// Returns an opaque id (the SQE user_data with TIMER_TAG_BIT
    /// set). Pass to `unregisterTimer` to cancel before fire.
    pub fn registerTimer(
        self: *Reactor,
        duration_ns: u64,
        target: *anyopaque,
    ) !u64 {
        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();

        // Heap-allocate the timespec — its lifetime spans until the
        // CQE fires. The reactor frees it when handling the
        // completion. Same shape as the timerfd entry in the epoll
        // backend.
        const ts = try self.allocator.create(linux.kernel_timespec);
        errdefer self.allocator.destroy(ts);
        ts.* = .{
            .sec = @intCast(@divTrunc(duration_ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(duration_ns, std.time.ns_per_s)),
        };

        const entry = try self.allocator.create(TimerEntry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .target = target, .ts = ts };

        const sqe = try self.ring.get_sqe();
        sqe.prep_timeout(ts, 0, 0);
        const user_data = @intFromPtr(entry) | TIMER_TAG_BIT;
        sqe.user_data = user_data;
        _ = try self.ring.submit();
        _ = self.pending.fetchAdd(1, .release);
        return user_data;
    }

    /// Cancel a pending timer. Submits an IORING_OP_TIMEOUT_REMOVE
    /// with the user_data of the original timer SQE. The CQE
    /// handler frees the entry + ts and decrements pending; we
    /// don't decrement here (would race with the CQE).
    ///
    /// v1.x note: io_uring isn't a runtime-validated backend yet,
    /// so this is best-effort. Real hardening lands when io_uring
    /// goes into CI rotation.
    pub fn unregisterTimer(self: *Reactor, id: u64) void {
        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();
        const sqe = self.ring.get_sqe() catch return;
        sqe.prep_timeout_remove(id, 0);
        sqe.user_data = 0;
        _ = self.ring.submit() catch {};
    }

    /// Cancel a pending poll wait. Submits IORING_OP_POLL_REMOVE.
    pub fn unregisterWait(self: *Reactor, fd: posix.fd_t, kind: EventKind) void {
        _ = self;
        _ = fd;
        _ = kind;
        // Tracks against the user_data of the original poll SQE.
        // We don't currently store that mapping (registerWait
        // discards it after submit). io_uring backend isn't
        // runtime-validated; full implementation lands with CI.
    }

    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
    ) !usize {
        // io_uring supports a deadline via TIMEOUT op rather than
        // io_uring_enter timeout. We use submit_and_wait(1) with no
        // timeout; tickle/event-fd lets us return promptly. timeout_ns
        // is currently advisory — chaining a TIMEOUT SQE with
        // IORING_TIMEOUT_REL for explicit deadlines is a follow-up.
        _ = timeout_ns;

        _ = try self.ring.submit_and_wait(1);

        var cqes: [64]linux.io_uring_cqe = undefined;
        const n = try self.ring.copy_cqes(&cqes, 0);

        var woken: usize = 0;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == TICKLE_TAG) {
                // Drain the eventfd; re-register the tickle poll.
                var sink: [8]u8 = undefined;
                _ = syscall.read(self.tickle_fd, &sink) catch {};
                self.submit_mutex.lock();
                if (self.ring.get_sqe()) |sqe| {
                    sqe.prep_poll_add(self.tickle_fd, linux.POLL.IN);
                    sqe.user_data = TICKLE_TAG;
                    _ = self.ring.submit() catch {};
                } else |_| {}
                self.submit_mutex.unlock();
                continue;
            }

            if ((cqe.user_data & TIMER_TAG_BIT) != 0) {
                const entry: *TimerEntry = @ptrFromInt(cqe.user_data & ~TIMER_TAG_BIT);
                const target_ptr = entry.target;
                self.allocator.destroy(entry.ts);
                self.allocator.destroy(entry);
                _ = self.pending.fetchSub(1, .release);
                try wakeFn(wake_ctx, target_ptr);
                woken += 1;
                continue;
            }

            const target: *anyopaque = @ptrFromInt(cqe.user_data);
            _ = self.pending.fetchSub(1, .release);
            try wakeFn(wake_ctx, target);
            woken += 1;
        }
        return woken;
    }
};

const TIMER_TAG_BIT: u64 = 1 << 63;

const TimerEntry = struct {
    target: *anyopaque,
    ts: *linux.kernel_timespec,
};

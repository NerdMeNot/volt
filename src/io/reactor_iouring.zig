//! Linux io_uring reactor backend.
//!
//! Same public surface as `reactor_kqueue.zig` and `reactor_epoll.zig`,
//! enforced by the comptime conformance check in `reactor.zig`. Built
//! on top of `std.os.linux.IoUring`.
//!
//! ## Tracked registrations
//!
//! io_uring's cancel model is fundamentally different from kqueue's
//! `EV_DELETE` and epoll's `EPOLL_CTL_DEL`: cancelling a poll/timer
//! op submits an `IORING_OP_ASYNC_CANCEL` (or `_TIMEOUT_REMOVE`) SQE
//! that asks the kernel to abort the original op. The original CQE
//! still arrives — it just has a `-ECANCELED` result. The cancel SQE
//! also produces a CQE.
//!
//! That means the naive scheme — pack `*Park` into `user_data` —
//! breaks: a cancelled Park is freed (it lives on a coroutine stack
//! that has unwound), but the kernel will still deliver a CQE for it.
//! Dispatching that CQE to a freed Park's `unpark()` is UAF.
//!
//! Solution (ported from tokio-uring's `Op` registry pattern): keep
//! a slab-backed `Registrations` table; `user_data` packs a `(slot,
//! generation, kind)` triple. On cancel, bump the slot's generation
//! so when the kernel CQE arrives we can recognize it as stale and
//! drop it without touching the now-freed target.
//!
//! ## Layout (`UserData`)
//!
//! ```
//! ┌──────────────────────┬──────────────┬────────┐
//! │   slot index (32)    │   gen (16)   │ kind   │
//! └──────────────────────┴──────────────┴────────┘
//!  bits 32..63             bits 16..31    bits 0..15
//! ```
//!
//! - `slot`: index into the `Registrations` slab (32-bit lets us scale to
//!   millions of concurrent ops; capacity is set at init).
//! - `generation`: bumped on each cancel/free; CQE delivery checks it
//!   matches before firing the wake. Stale CQEs (cancel completions,
//!   races between submit and cancel) are silently dropped.
//! - `kind`: tickle / wait / timer / cancel / unused-pad. Lets `poll`
//!   dispatch correctly without a separate per-CQE table lookup.
//!
//! ## Status
//!
//! Linux opt-in via `-Dreactor=iouring`. Default is epoll because:
//!   1. Kernel ≥ 5.4 needed for the ops we use.
//!   2. CI rotation pending — io_uring matrix entries are
//!      `allow_failure: true` until this backend has been green for
//!      ≥3 consecutive runs.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;
const IoUring = std.os.linux.IoUring;

const syscall = @import("../internal/syscall.zig");
const Mutex = @import("../internal/thread.zig").Mutex;
const reactor_types = @import("reactor_types.zig");
const Slab = @import("../internal/util/slab.zig").Slab;

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("reactor_iouring.zig is for Linux only");
    }
}

/// Re-exported so `reactor.zig`'s conformance check can read it from
/// `impl.EventKind`. The canonical declaration lives in
/// `reactor_types.zig`.
pub const EventKind = reactor_types.EventKind;

const ReactorError = reactor_types.ReactorError;

fn pollEventsFor(kind: EventKind) u32 {
    return switch (kind) {
        .readable => linux.POLL.IN,
        .writable => linux.POLL.OUT,
    };
}

// ─────────────────────────────────────────────────────────────────────
// User-data layout
// ─────────────────────────────────────────────────────────────────────

const UserDataKind = enum(u4) {
    /// Tickle eventfd CQE — pre-registered at init, drained in poll.
    tickle = 0,
    /// Poll-readiness wait. Slot holds the target Park pointer.
    wait = 1,
    /// One-shot timer. Slot holds target Park pointer + heap-owned
    /// kernel_timespec whose lifetime spans until the CQE arrives.
    timer = 2,
    /// Cancel SQE we issued — its CQE is informational; we drop it.
    cancel = 3,
};

const UserData = packed struct(u64) {
    kind: UserDataKind,
    _pad: u12 = 0,
    generation: u16,
    slot: u32,

    fn pack(self: UserData) u64 {
        return @bitCast(self);
    }

    fn unpack(value: u64) UserData {
        return @bitCast(value);
    }
};

const TICKLE_USER_DATA: u64 = (UserData{
    .kind = .tickle,
    .generation = 0,
    .slot = 0,
}).pack();

const CANCEL_USER_DATA: u64 = (UserData{
    .kind = .cancel,
    .generation = 0,
    .slot = 0,
}).pack();

// ─────────────────────────────────────────────────────────────────────
// Registration table
// ─────────────────────────────────────────────────────────────────────

const Registration = struct {
    /// Wake target — typically a `*Park` on the registering coroutine's
    /// stack. Stale CQEs are detected by generation mismatch and never
    /// dereference this pointer.
    target: *anyopaque,
    /// Bumped on each free / cancel. When a CQE arrives with a slot
    /// whose generation no longer matches, we drop the CQE silently.
    generation: u16,
    /// Whether this slot is currently in flight (true between
    /// register and CQE-delivery / cancel-completion).
    in_flight: bool,
    /// For `kind == .timer`: heap-owned timespec the kernel reads.
    /// Lifetime spans the slot's in_flight period — freed on CQE.
    timer_ts: ?*linux.kernel_timespec,
    /// What kind of op this slot represents (used for cleanup
    /// dispatch when the slot is reaped).
    kind: UserDataKind,
    /// For `kind == .wait`: the (fd, event_kind) the SQE was armed
    /// for. Lets `unregisterWait(fd, kind)` find this slot when the
    /// caller cancels — io_uring's cancel API needs the original
    /// SQE's user_data, which we reconstruct from `slot + generation`.
    fd: posix.fd_t,
    event_kind: EventKind,
};

/// Slab capacity. Sized for 65k concurrent registrations — well
/// beyond any realistic spawn-count for a single process. Each slot
/// is ~32 bytes, so the table is ≤ 2 MB at full capacity.
///
/// If a registration exceeds capacity, `registerWait`/`registerTimer`
/// fail with `ReactorError.RegistrationFailed`. Future work: dynamic
/// resize. For v1.1 the fixed cap is the right trade-off — it matches
/// the kernel's SQ ring capacity scale without per-op allocator
/// pressure.
const SLAB_CAPACITY: usize = 65_536;

// ─────────────────────────────────────────────────────────────────────
// Reactor
// ─────────────────────────────────────────────────────────────────────

pub const Reactor = struct {
    ring: IoUring,
    tickle_fd: i32,
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    submit_mutex: Mutex = .{},
    /// Protects `regs`. Held briefly during register/cancel/CQE-handling.
    regs_mutex: Mutex = .{},
    regs: Slab(Registration),
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) ReactorError!Reactor {
        var ring = IoUring.init(256, 0) catch return error.InitFailed;
        errdefer ring.deinit();

        const tfd_raw = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        const tfd: i32 = @intCast(tfd_raw);
        if (tfd < 0) return error.InitFailed;
        errdefer syscall.close(tfd);

        var regs = Slab(Registration).init(allocator, SLAB_CAPACITY) catch return error.OutOfMemory;
        errdefer regs.deinit();

        // Pre-register the tickle eventfd with a poll_add SQE. Its
        // user_data identifies it as a tickle event so `poll` can
        // re-arm it after each fire without going through the slab.
        const sqe = ring.get_sqe() catch return error.InitFailed;
        sqe.prep_poll_add(tfd, linux.POLL.IN);
        sqe.user_data = TICKLE_USER_DATA;
        _ = ring.submit() catch return error.InitFailed;

        return .{
            .ring = ring,
            .tickle_fd = tfd,
            .regs = regs,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        // Free any timer timespecs still in flight. Don't cancel the
        // SQEs — the ring is being torn down so its kernel state is
        // about to be released anyway.
        var it = self.regs.iterator();
        while (it.next()) |entry| {
            if (entry.value.timer_ts) |ts| {
                self.allocator.destroy(ts);
            }
        }
        self.regs.deinit();
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

    /// Allocate a slot, fill it with the given target, and return its
    /// `UserData`. Caller is responsible for freeing the slot via
    /// `freeSlot` when the CQE arrives or `cancelSlot` on cancel.
    fn allocSlot(
        self: *Reactor,
        target: *anyopaque,
        kind: UserDataKind,
        timer_ts: ?*linux.kernel_timespec,
        fd: posix.fd_t,
        event_kind: EventKind,
    ) ReactorError!UserData {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        const slot = self.regs.insert(.{
            .target = target,
            .generation = 0,
            .in_flight = true,
            .timer_ts = timer_ts,
            .kind = kind,
            .fd = fd,
            .event_kind = event_kind,
        }) catch return error.RegistrationFailed;

        return .{
            .kind = kind,
            .generation = 0,
            .slot = @intCast(slot),
        };
    }

    /// Bump the slot's generation and remove it from the slab. Called
    /// after a real CQE has been delivered (the slot's lifetime ended
    /// on its own). Also frees any owned timer_ts.
    fn freeSlot(self: *Reactor, slot: u32) void {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        if (self.regs.remove(slot)) |entry| {
            if (entry.timer_ts) |ts| {
                self.allocator.destroy(ts);
            }
        }
    }

    /// Mark the slot as cancelled (bump generation, set in_flight =
    /// false) but keep it allocated until the kernel delivers its
    /// final CQE for the original op. The CQE handler will detect
    /// the generation mismatch, treat the CQE as stale, and call
    /// `freeSlot` then.
    fn markCancelled(self: *Reactor, slot: u32) void {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        if (self.regs.get(slot)) |entry| {
            entry.generation +%= 1; // wrap-around safe — we only check equality
            entry.in_flight = false;
        }
    }

    pub fn registerWait(
        self: *Reactor,
        fd: posix.fd_t,
        kind: EventKind,
        target: *anyopaque,
    ) ReactorError!void {
        const ud = try self.allocSlot(target, .wait, null, fd, kind);
        errdefer self.freeSlot(ud.slot);

        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();

        const sqe = self.ring.get_sqe() catch return error.RegistrationFailed;
        sqe.prep_poll_add(fd, pollEventsFor(kind));
        sqe.user_data = ud.pack();
        _ = self.ring.submit() catch return error.RegistrationFailed;
        _ = self.pending.fetchAdd(1, .release);
    }

    pub fn registerTimer(
        self: *Reactor,
        duration_ns: u64,
        target: *anyopaque,
    ) ReactorError!u64 {
        // Heap-allocate timespec — kernel reads it asynchronously,
        // lifetime must span until CQE. Freed by freeSlot.
        const ts = self.allocator.create(linux.kernel_timespec) catch return error.OutOfMemory;
        errdefer self.allocator.destroy(ts);
        ts.* = .{
            .sec = @intCast(@divTrunc(duration_ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(duration_ns, std.time.ns_per_s)),
        };

        const ud = self.allocSlot(target, .timer, ts, -1, .readable) catch |err| {
            self.allocator.destroy(ts);
            return err;
        };
        errdefer self.freeSlot(ud.slot);

        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();

        const sqe = self.ring.get_sqe() catch return error.RegistrationFailed;
        sqe.prep_timeout(ts, 0, 0);
        sqe.user_data = ud.pack();
        _ = self.ring.submit() catch return error.RegistrationFailed;
        _ = self.pending.fetchAdd(1, .release);
        return ud.pack();
    }

    /// Cancel a pending timer. The original SQE's `user_data` was
    /// returned from `registerTimer` (an opaque `u64` packed
    /// UserData); we submit `IORING_OP_TIMEOUT_REMOVE` keyed on it.
    /// The kernel delivers two CQEs: the original timer's (with
    /// `-ECANCELED`) and the remove SQE's. Both are dropped by the
    /// CQE handler — the timer slot is freed on the first one's
    /// arrival via the in_flight flag.
    pub fn unregisterTimer(self: *Reactor, id: u64) void {
        const ud = UserData.unpack(id);
        if (ud.kind != .timer) return;

        self.markCancelled(ud.slot);

        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();
        const sqe = self.ring.get_sqe() catch return;
        sqe.prep_timeout_remove(id, 0);
        sqe.user_data = CANCEL_USER_DATA;
        _ = self.ring.submit() catch {};
    }

    /// Cancel a pending fd wait. Walks the registration table to
    /// find the in-flight slot whose (fd, kind) matches, marks it
    /// cancelled (bumps generation, clears in_flight), and submits
    /// `IORING_OP_ASYNC_CANCEL` keyed on the original user_data so
    /// the kernel aborts the original poll op. The original CQE
    /// will still arrive — but the CQE handler sees the generation
    /// mismatch and drops it without touching the now-freed target.
    ///
    /// Volt's wait protocol guarantees at most one Park per
    /// (fd, kind) per coroutine (`wait.zig` asserts on dup register),
    /// so the table walk finds at most one match. O(N) over the slab
    /// capacity (~65k); only fired on cancellation, which is rare.
    pub fn unregisterWait(self: *Reactor, fd: posix.fd_t, kind: EventKind) void {
        const found = self.findAndMarkWaitSlot(fd, kind) orelse return;

        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();
        const sqe = self.ring.get_sqe() catch return;
        // ASYNC_CANCEL takes the user_data of the SQE to cancel.
        // We reconstruct it as (slot, original_generation, .wait).
        const original_ud = (UserData{
            .kind = .wait,
            .generation = found.original_generation,
            .slot = found.slot,
        }).pack();
        sqe.prep_cancel(original_ud, 0);
        sqe.user_data = CANCEL_USER_DATA;
        _ = self.ring.submit() catch {};
    }

    /// Walk the registration slab looking for the in-flight wait
    /// slot matching `(fd, kind)`. On match: bump generation, clear
    /// in_flight, return `(slot, original_generation)` so the caller
    /// can reconstruct the original user_data for the cancel SQE.
    fn findAndMarkWaitSlot(
        self: *Reactor,
        fd: posix.fd_t,
        kind: EventKind,
    ) ?struct { slot: u32, original_generation: u16 } {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        var it = self.regs.iterator();
        while (it.next()) |entry| {
            const reg = entry.value;
            if (reg.kind != .wait or !reg.in_flight) continue;
            if (reg.fd != fd or reg.event_kind != kind) continue;

            const orig_gen = reg.generation;
            entry.value.generation +%= 1;
            entry.value.in_flight = false;
            return .{
                .slot = @intCast(entry.key),
                .original_generation = orig_gen,
            };
        }
        return null;
    }

    /// Block on the io_uring CQ for up to one event, then drain all
    /// available CQEs and dispatch them. Stale CQEs (slot generation
    /// no longer matches user_data's) drop silently.
    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
    ) anyerror!usize {
        // io_uring's deadline mechanism is a TIMEOUT op chained into
        // the wait, not an io_uring_enter timeout. We use
        // submit_and_wait(1) without a deadline; the tickle eventfd
        // gives us prompt wake-ups for new work / shutdown. A future
        // revision can chain a TIMEOUT SQE with IORING_TIMEOUT_REL
        // for explicit deadlines (matches what tokio-uring does).
        _ = timeout_ns;

        _ = self.ring.submit_and_wait(1) catch return error.PollFailed;

        var cqes: [64]linux.io_uring_cqe = undefined;
        const n = self.ring.copy_cqes(&cqes, 0) catch return error.PollFailed;

        var woken: usize = 0;
        for (cqes[0..n]) |cqe| {
            const ud = UserData.unpack(cqe.user_data);

            switch (ud.kind) {
                .tickle => {
                    // Drain the eventfd; re-register the tickle poll.
                    var sink: [8]u8 = undefined;
                    _ = syscall.read(self.tickle_fd, &sink) catch {};
                    self.submit_mutex.lock();
                    if (self.ring.get_sqe()) |sqe| {
                        sqe.prep_poll_add(self.tickle_fd, linux.POLL.IN);
                        sqe.user_data = TICKLE_USER_DATA;
                        _ = self.ring.submit() catch {};
                    } else |_| {}
                    self.submit_mutex.unlock();
                    continue;
                },
                .cancel => {
                    // Cancel SQE's own CQE — informational, not a wake.
                    continue;
                },
                .wait, .timer => {
                    const target = self.consumeSlot(ud) orelse {
                        // Stale CQE (cancelled before fire, or generation
                        // mismatch). Decrement pending: the slot was
                        // accounted for at register time and the kernel
                        // is done with it.
                        _ = self.pending.fetchSub(1, .release);
                        continue;
                    };
                    _ = self.pending.fetchSub(1, .release);
                    try wakeFn(wake_ctx, target);
                    woken += 1;
                },
            }
        }
        return woken;
    }

    /// Look up a slot for CQE delivery. Returns the target pointer if
    /// (slot, generation) match the live registration; nil if the
    /// slot is empty or has been cancelled (generation mismatch). On
    /// success, frees the slot.
    fn consumeSlot(self: *Reactor, ud: UserData) ?*anyopaque {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        const entry = self.regs.get(ud.slot) orelse return null;
        if (!entry.in_flight or entry.generation != ud.generation) {
            // Cancelled — the cancel path already marked this; drop the
            // CQE and clean up.
            if (entry.timer_ts) |ts| self.allocator.destroy(ts);
            _ = self.regs.remove(ud.slot);
            return null;
        }
        const target = entry.target;
        if (entry.timer_ts) |ts| self.allocator.destroy(ts);
        _ = self.regs.remove(ud.slot);
        return target;
    }
};

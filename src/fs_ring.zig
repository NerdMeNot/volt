//! Per-worker io_uring instance for async file I/O.
//!
//! Phase 2A — standalone low-level wrapper. No Coroutine integration
//! yet; that lands in Phase 2C alongside the per-P fs ring lifecycle
//! (Phase 2B). This file owns the kernel-facing surface: setup flag
//! selection, SQE prep helpers, lazy-batch submit, drain-to-slice
//! CQE handling. Tests at the bottom exercise the kernel
//! interactions directly (no coroutine context required).
//!
//! Linux-only by construction. The whole module is compile-gated;
//! Darwin / Windows builds never see it.
//!
//! Design references (see docs/internals/async-fs-io.md §11 for full
//! citations):
//!
//!   * Setup flags: `SINGLE_ISSUER | DEFER_TASKRUN | SUBMIT_ALL` on
//!     6.1+, fall back to weaker combos on older kernels. No SQPOLL
//!     (mutually exclusive with DEFER_TASKRUN's same-task invariant;
//!     none of TigerBeetle / Glommio / Seastar enable it).
//!   * `MADV_DONTFORK` on the mmap'd ring memory — fork+exec in a
//!     child shouldn't inherit locked mappings. (Seastar
//!     `reactor_backend.cc:1228`.)
//!   * Lazy batched submit: `prepX` only writes the userspace ring;
//!     `flush` calls `io_uring_enter` once. (TigerBeetle
//!     `src/io/linux.zig`.)
//!   * Drain CQEs to caller's buffer, dispatch separately. Required
//!     to avoid the recursion footgun where a resumed coroutine
//!     re-suspends on another fs op while still inside the drain
//!     loop (TigerBeetle's load-bearing comment).

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;
const reactor_fs = @import("reactor_fs.zig");

/// Re-export so callers within the fs_ring module / Phase 2C
/// `drainFsRingInto` helper share one result vocabulary with
/// the Phase 1 spawnBlocking proxy. Shape: `value` is the
/// io_uring CQE's `res` field cast to isize (positive = bytes
/// or fd, negative = -errno); `err` is unpacked errno on
/// failure, 0 on success — matches what `reactor_fs.fsRead`
/// etc. return.
pub const FsResult = reactor_fs.FsResult;

// Linux-only by *use*: every public method ultimately calls
// `std.os.linux.IoUring.*` which only does anything useful on
// Linux. The module is intentionally importable from other
// platforms — Runtime needs the type to declare an `?[]FsRing`
// field that's always null off Linux. Per-test skips guard
// runtime-failing tests; `FsRing.init` on non-Linux returns
// `error.IoUringUnavailable` cleanly.

/// Default SQ/CQ depth. Matches the conservative Seastar default
/// (`s_queue_len = 200`; rounded to a power of two). Caller may
/// override via `init`.
pub const DEFAULT_RING_ENTRIES: u16 = 256;

/// Setup-flag tiers, tried in order from most aggressive to bare.
/// Stops at the first combo that succeeds.
///
/// **Why NOT `DEFER_TASKRUN`:** under DEFER_TASKRUN the kernel
/// only runs completion work (including making CQEs visible AND
/// writing to the registered eventfd) on the submitter's next
/// `io_uring_enter(GETEVENTS)`. Our pattern is "submit one SQE,
/// park the coroutine, eventually drain CQEs from a different
/// scheduling context"; with DEFER_TASKRUN the eventfd never
/// fires until we manually call enter(GETEVENTS), which would
/// add a syscall per drain and erase the lazy-batch win.
/// DEFER_TASKRUN benefits workloads that BATCH many SQEs between
/// drains; ours is the opposite shape. Skip it.
///
/// **Why NOT `SINGLE_ISSUER`:** the cancel-and-drain path
/// (Phase 3) submits `IORING_OP_ASYNC_CANCEL` after the
/// coroutine resumes from `park()`. By that time the coroutine
/// may have migrated to a different worker (work stealing), and
/// SINGLE_ISSUER causes the kernel to reject `io_uring_enter`
/// from any task other than the original submitter — `EEXIST`,
/// surfaced as `error.InvalidThread`. Cross-thread submission of
/// the SQ ring itself is safe (sequential with park acting as
/// release/acquire); only the kernel-level SINGLE_ISSUER flag
/// rejects it. Initial Phase 2A draft enabled SINGLE_ISSUER, but
/// the Phase 3 cancel path exposed the conflict. Dropped here.
///
/// SUBMIT_ALL (5.18+) keeps submitting on per-SQE error rather
/// than stopping at the first failure — strict win.
const SETUP_TIERS = [_]u32{
    linux.IORING_SETUP_SUBMIT_ALL,
    0,
};

pub const InitError = error{
    /// `io_uring_setup` failed even with flags=0. Most likely
    /// `kernel.io_uring_disabled=1`, the container's seccomp
    /// profile blocks the syscall, or the kernel predates io_uring
    /// entirely (< 5.1). The probe script (`scripts/probe-linux.sh`)
    /// distinguishes these cases.
    IoUringUnavailable,
    SystemResources,
};

/// Per-op state record. Allocated on the calling coroutine's
/// stack at the `fsRead`/etc call site — the coroutine's stack
/// VA is stable for its lifetime (CLAUDE.md invariant 5), and
/// the coroutine waits for its own CQE before returning, so the
/// frame outlives the kernel's in-flight window. SQE.user_data
/// is the address of an `FsOp`; the CQE drainer dereferences it,
/// writes `result`, and unparks `coro`.
///
/// Phase 3 cancellation: the `state` field drives the four-state
/// machine documented in `docs/internals/phase-3-design.md §1`.
/// CASes from `STATE_PENDING` are mutually exclusive between
/// drainer (→ Completed) and cancel-deliver (→ Cancelling); the
/// Cancelling → CancelledAndDrained transition is single-writer
/// (drainer only). The coroutine must wait for the ORIGINAL op
/// CQE before returning (NOT just the cancel ack) so the kernel
/// has provably stopped touching `result` and any caller buffer.
pub const FsOp = struct {
    coro: *@import("coroutine.zig").Coroutine,
    result: FsResult,
    /// Phase 3: state machine for cancel/completion arbitration.
    /// Default `STATE_PENDING` so existing Phase 2 callers (which
    /// don't use cancel) keep the same shape — the drainer's CAS
    /// `Pending → Completed` succeeds on the very first try and
    /// the path matches Phase 2 exactly.
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(STATE_PENDING),
};

/// FsOp.state values. See `docs/internals/phase-3-design.md §1`
/// for the state-machine diagram.
pub const STATE_PENDING: u8 = 0;
pub const STATE_COMPLETED: u8 = 1;
pub const STATE_CANCELLING: u8 = 2;
pub const STATE_CANCELLED_AND_DRAINED: u8 = 3;

/// Phase 3: the cancel SQE's user_data is the original FsOp
/// pointer with bit 0 set. The drainer fast-paths on this bit
/// — decrement `fs_in_flight`, ignore the rest. Avoids a second
/// stack-allocated FsOp for the cancel ack (which carries no
/// useful info for the coroutine; the coroutine waits on the
/// ORIGINAL CQE for buffer safety).
///
/// FsOp pointers are heap- or stack-aligned to at least 8 bytes,
/// so bit 0 is naturally free.
pub const CANCEL_ACK_TAG: u64 = 0x1;

pub inline fn isCancelAckUserData(user_data: u64) bool {
    return (user_data & CANCEL_ACK_TAG) != 0;
}

pub inline fn cancelUserDataFor(op_user_data: u64) u64 {
    return op_user_data | CANCEL_ACK_TAG;
}

pub const FsRing = struct {
    ring: linux.IoUring,
    /// Which `IORING_SETUP_*` mask the kernel accepted. Useful for
    /// later runtime decisions (e.g. whether to use features that
    /// depend on `DEFER_TASKRUN`'s same-task invariant).
    flags_used: u32,
    /// Set by every `prepX`; checked by `flush` to skip
    /// `io_uring_enter` when nothing has been queued. Lazy-batch
    /// dirty bit, Seastar `reactor_backend.cc:1380-1389` pattern.
    has_pending: bool = false,
    /// eventfd registered with the ring via
    /// `IORING_REGISTER_EVENTFD` (Phase 2C.1). The kernel writes
    /// to this fd whenever a CQE lands — used by the shared
    /// reactor poller as a wake signal. Created lazily by
    /// `registerEventFd`; -1 until then. Closed by `deinit`.
    eventfd: i32 = -1,

    /// Initialise with `DEFAULT_RING_ENTRIES`. Returns the first
    /// setup tier the kernel accepts.
    pub fn init() InitError!FsRing {
        return initWithEntries(DEFAULT_RING_ENTRIES);
    }

    pub fn initWithEntries(entries: u16) InitError!FsRing {
        for (SETUP_TIERS) |flags| {
            if (linux.IoUring.init(entries, flags)) |r_const| {
                var ring = r_const;
                applyDontFork(&ring);
                return .{ .ring = ring, .flags_used = flags };
            } else |_| {
                // Try the next tier.
            }
        }
        // Every tier failed. If even flags=0 fails, the environment
        // doesn't permit io_uring at all (kernel too old / sysctl /
        // seccomp). The probe script is the diagnostic tool.
        return error.IoUringUnavailable;
    }

    pub fn deinit(self: *FsRing) void {
        // Close the eventfd BEFORE the ring teardown. The kernel
        // unregisters the eventfd association on ring close, but
        // we own the fd's lifecycle.
        if (self.eventfd >= 0) {
            _ = c_close(self.eventfd);
            self.eventfd = -1;
        }
        self.ring.deinit();
    }

    // ─── eventfd integration (Phase 2C.1) ─────────────────────────

    /// Create an eventfd and register it with the ring. The kernel
    /// writes to the eventfd on every CQE landing — the shared
    /// reactor's epoll watches this fd and uses it as a wake
    /// signal. EFD_NONBLOCK so the poller's drain `read` never
    /// blocks; EFD_CLOEXEC so fork+exec children don't inherit.
    ///
    /// Idempotent: a second call is a no-op (returns success
    /// without touching the existing registration). Errors only
    /// from the eventfd syscall itself or the kernel-level
    /// register call.
    pub fn registerEventFd(self: *FsRing) !void {
        if (self.eventfd >= 0) return;
        const rc = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        if (linux.errno(rc) != .SUCCESS) return error.EventFdCreateFailed;
        const fd: i32 = @intCast(rc);
        errdefer _ = c_close(fd);
        try self.ring.register_eventfd(fd);
        self.eventfd = fd;
    }

    /// Drain the kernel counter on the registered eventfd. MUST be
    /// called by the reactor poller every time the eventfd fires —
    /// a short read or no read leaves the counter non-zero, and
    /// level-triggered epoll will re-fire indefinitely (see
    /// `docs/internals/phase-2c-design.md §2`). Returns silently
    /// on EAGAIN (the kernel may have already had the counter
    /// drained by a previous wake on the same epoll batch).
    pub fn drainEventFd(self: *FsRing) void {
        if (self.eventfd < 0) return;
        var buf: [8]u8 = undefined;
        _ = c_read(self.eventfd, &buf, 8);
        // Errors ignored: EAGAIN means already drained, EBADF
        // means concurrent deinit (we can't do anything useful
        // either way).
    }

    // ─── SQE prep helpers — lazy, no syscall ──────────────────────
    //
    // Each `prepX` claims an SQE slot in the userspace ring, fills
    // it in, and sets `user_data` (the kernel echoes this back in
    // the matching CQE so the caller can correlate). `flush` makes
    // the SQEs visible to the kernel.

    pub fn prepRead(
        self: *FsRing,
        fd: linux.fd_t,
        buf: []u8,
        offset: u64,
        user_data: u64,
    ) !void {
        const sqe = try self.ring.get_sqe();
        sqe.prep_read(fd, buf, offset);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    pub fn prepWrite(
        self: *FsRing,
        fd: linux.fd_t,
        buf: []const u8,
        offset: u64,
        user_data: u64,
    ) !void {
        const sqe = try self.ring.get_sqe();
        sqe.prep_write(fd, buf, offset);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    /// Datasync via `IORING_FSYNC_DATASYNC` — io_uring doesn't have
    /// a separate FDATASYNC opcode, the data-only behaviour is a
    /// flag on FSYNC.
    pub fn prepFsync(
        self: *FsRing,
        fd: linux.fd_t,
        user_data: u64,
        datasync: bool,
    ) !void {
        const sqe = try self.ring.get_sqe();
        const flags: u32 = if (datasync) linux.IORING_FSYNC_DATASYNC else 0;
        sqe.prep_fsync(fd, flags);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    pub fn prepOpenAt(
        self: *FsRing,
        dirfd: linux.fd_t,
        path: [*:0]const u8,
        flags: linux.O,
        mode: linux.mode_t,
        user_data: u64,
    ) !void {
        const sqe = try self.ring.get_sqe();
        sqe.prep_openat(dirfd, path, flags, mode);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    pub fn prepClose(
        self: *FsRing,
        fd: linux.fd_t,
        user_data: u64,
    ) !void {
        const sqe = try self.ring.get_sqe();
        sqe.prep_close(fd);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    /// Phase 3: submit IORING_OP_ASYNC_CANCEL targeting an
    /// in-flight SQE identified by its original `user_data`.
    /// The cancel ack arrives as a CQE whose own `user_data` is
    /// `user_data` (the cancel SQE's identifier — caller's
    /// choice, conventionally `cancelUserDataFor(orig)` so the
    /// drainer can fast-path-recognise it via the low bit).
    ///
    /// Kernel semantics:
    ///   * cancel ack `res == 0`: cancellation initiated; the
    ///     original op CQE will fire with `-ECANCELED`.
    ///   * cancel ack `res == -ENOENT`: original op already
    ///     completed; the original CQE arrives normally with
    ///     its result.
    ///   * cancel ack `res == -EALREADY`: cancellation is
    ///     in progress (someone else already cancelled).
    ///
    /// Either way, the caller MUST wait for the ORIGINAL op CQE
    /// before considering its buffer free — only that CQE
    /// signals "kernel has stopped touching the buffer."
    pub fn prepCancel(
        self: *FsRing,
        target_user_data: u64,
        user_data: u64,
    ) !void {
        const sqe = try self.ring.get_sqe();
        sqe.prep_cancel(target_user_data, 0);
        sqe.user_data = user_data;
        self.has_pending = true;
    }

    // ─── Submission ────────────────────────────────────────────────

    /// Flush all pending SQEs to the kernel. One `io_uring_enter`
    /// per call regardless of how many SQEs are queued. Returns the
    /// count the kernel acknowledged. Idempotent when no SQEs
    /// pending (returns 0, no syscall).
    pub fn flush(self: *FsRing) !u32 {
        if (!self.has_pending) return 0;
        const n = try self.ring.submit();
        self.has_pending = false;
        return n;
    }

    // ─── Completion ────────────────────────────────────────────────

    /// Drain up to `out.len` ready CQEs into `out`. Non-blocking —
    /// returns 0 if no CQEs are ready. The caller MUST iterate the
    /// returned slice and dispatch each (resume the parked
    /// coroutine, run any callback) AFTER the drain finishes, not
    /// inside the iteration. See TigerBeetle's `flush_completions`
    /// comment for why: dispatch can recursively submit new SQEs;
    /// running it inside the drain risks unbounded stack growth and
    /// confused traces.
    pub fn peekBatch(self: *FsRing, out: []linux.io_uring_cqe) !u32 {
        return self.ring.copy_cqes(out, 0);
    }

    /// Like `peekBatch` but blocks until at least one CQE is
    /// available. Used in tests; production code on a worker uses
    /// the eventfd-into-reactor integration (Phase 2C) instead so
    /// the worker doesn't block exclusively on this ring.
    pub fn waitOne(self: *FsRing, out: []linux.io_uring_cqe) !u32 {
        return self.ring.copy_cqes(out, 1);
    }
};

// ─── Helpers ─────────────────────────────────────────────────────

/// Mark the ring's mmap'd memory as `MADV_DONTFORK` so a forked
/// child doesn't inherit the locked mapping. Errors are swallowed —
/// `madvise` failing here doesn't break correctness, only the
/// fork+exec contract on pathological systems. Seastar
/// `reactor_backend.cc:1228` does the same.
fn applyDontFork(ring: *linux.IoUring) void {
    posix.madvise(
        @alignCast(ring.sq.mmap.ptr),
        ring.sq.mmap.len,
        linux.MADV.DONTFORK,
    ) catch {};
    // Pre-5.4 kernels mmap'd SQEs separately; modern kernels
    // (5.4+, which is our floor) put them in the same region —
    // but the IoUring impl still tracks both slices. Apply
    // DONTFORK to both unconditionally; on modern kernels the
    // second call is a cheap no-op on already-marked pages.
    if (ring.sq.mmap_sqes.ptr != ring.sq.mmap.ptr) {
        posix.madvise(
            @alignCast(ring.sq.mmap_sqes.ptr),
            ring.sq.mmap_sqes.len,
            linux.MADV.DONTFORK,
        ) catch {};
    }
}

// ─── Tests ────────────────────────────────────────────────────────
//
// Tests exercise the kernel interactions directly — they don't need
// a Runtime / Coroutine because Phase 2A is the standalone wrapper.
// Run via `scripts/test-linux.sh` (which must pass
// `--security-opt seccomp=unconfined --security-opt label=disable`
// on Fedora-based hosts; see the probe-linux.sh comments for the
// full rationale).

const testing = std.testing;

// Stack-allocated NUL-terminated temp path. mkstemp(2) takes a
// trailing `XXXXXX` template and rewrites it in place; we have to
// keep the buffer alive for the file's lifetime.
const TmpPath = struct {
    buf: [32:0]u8,

    fn create() !TmpPath {
        var t: TmpPath = .{ .buf = undefined };
        const tmpl = "/tmp/volt-fsring-XXXXXX";
        @memcpy(t.buf[0..tmpl.len], tmpl);
        t.buf[tmpl.len] = 0;
        const fd = c_mkstemp(&t.buf);
        if (fd < 0) return error.MkstempFailed;
        _ = c_close(fd);
        return t;
    }

    fn ptr(self: *const TmpPath) [*:0]const u8 {
        return &self.buf;
    }

    fn cleanup(self: *const TmpPath) void {
        _ = c_unlink(self.ptr());
    }
};

extern "c" fn mkstemp(template: [*:0]u8) c_int;
const c_mkstemp = &mkstemp;
extern "c" fn close(fd: c_int) c_int;
const c_close = &close;
extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
const c_read = &read;
extern "c" fn unlink(path: [*:0]const u8) c_int;
const c_unlink = &unlink;

test "FsRing: init accepts at least one setup tier" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        // `IoUringUnavailable` means the test environment can't
        // run io_uring at all (sysctl-disabled / seccomp-blocked
        // / kernel too old). Skip rather than fail — the probe
        // script is the authoritative diagnostic.
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();
    // Any tier accepted: target stack OR a fallback. The fact
    // that *something* worked is the assertion.
    try testing.expect(ring.flags_used != 0xffff_ffff);
}

test "FsRing: openat + write + fsync + close round-trip" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    var tmp = try TmpPath.create();
    defer tmp.cleanup();

    // Stage 1: open the file via io_uring. user_data = 1.
    const open_flags: linux.O = .{ .ACCMODE = .WRONLY, .TRUNC = true };
    try ring.prepOpenAt(linux.AT.FDCWD, tmp.ptr(), open_flags, 0o644, 1);
    try testing.expectEqual(@as(u32, 1), try ring.flush());

    var cqes: [4]linux.io_uring_cqe = undefined;
    var n = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expectEqual(@as(u64, 1), cqes[0].user_data);
    try testing.expect(cqes[0].res >= 0); // res = fd on success
    const fd: linux.fd_t = cqes[0].res;

    // Stage 2: write "hello world" at offset 0. user_data = 2.
    const payload = "hello world";
    try ring.prepWrite(fd, payload, 0, 2);
    // Stage 3: fsync. user_data = 3. Batched in the same submit.
    try ring.prepFsync(fd, 3, false);
    try testing.expectEqual(@as(u32, 2), try ring.flush());

    // Two CQEs; order isn't guaranteed by io_uring but for a
    // single-fd serial workload they almost always come in order.
    // Match by user_data instead of relying on order.
    n = try ring.waitOne(&cqes);
    if (n < 2) {
        const more = try ring.waitOne(cqes[n..]);
        n += more;
    }
    try testing.expectEqual(@as(u32, 2), n);

    var saw_write = false;
    var saw_fsync = false;
    for (cqes[0..n]) |cqe| {
        switch (cqe.user_data) {
            2 => {
                saw_write = true;
                try testing.expectEqual(@as(i32, payload.len), cqe.res);
            },
            3 => {
                saw_fsync = true;
                try testing.expectEqual(@as(i32, 0), cqe.res);
            },
            else => return error.UnexpectedUserData,
        }
    }
    try testing.expect(saw_write and saw_fsync);

    // Stage 4: close. user_data = 4.
    try ring.prepClose(fd, 4);
    try testing.expectEqual(@as(u32, 1), try ring.flush());

    n = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expectEqual(@as(u64, 4), cqes[0].user_data);
    try testing.expectEqual(@as(i32, 0), cqes[0].res);
}

test "FsRing: read what we wrote" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    var tmp = try TmpPath.create();
    defer tmp.cleanup();

    // Pre-populate the file via blocking libc (we're testing the
    // read path, not the write path).
    {
        const open_flags: linux.O = .{ .ACCMODE = .WRONLY, .TRUNC = true };
        try ring.prepOpenAt(linux.AT.FDCWD, tmp.ptr(), open_flags, 0o644, 11);
        _ = try ring.flush();
        var cqes: [1]linux.io_uring_cqe = undefined;
        _ = try ring.waitOne(&cqes);
        const fd = cqes[0].res;
        try ring.prepWrite(fd, "abcdefghij", 0, 12);
        _ = try ring.flush();
        _ = try ring.waitOne(&cqes);
        try ring.prepClose(fd, 13);
        _ = try ring.flush();
        _ = try ring.waitOne(&cqes);
    }

    // Now read it back through io_uring.
    const open_flags: linux.O = .{ .ACCMODE = .RDONLY };
    try ring.prepOpenAt(linux.AT.FDCWD, tmp.ptr(), open_flags, 0, 21);
    _ = try ring.flush();
    var cqes: [4]linux.io_uring_cqe = undefined;
    var n = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expect(cqes[0].res >= 0);
    const fd: linux.fd_t = cqes[0].res;

    var buf: [16]u8 = undefined;
    try ring.prepRead(fd, &buf, 0, 22);
    _ = try ring.flush();
    n = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expectEqual(@as(u64, 22), cqes[0].user_data);
    try testing.expectEqual(@as(i32, 10), cqes[0].res);
    try testing.expectEqualStrings("abcdefghij", buf[0..10]);

    try ring.prepClose(fd, 23);
    _ = try ring.flush();
    _ = try ring.waitOne(&cqes);
}

test "FsRing: peekBatch returns 0 when no CQEs ready" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    var cqes: [8]linux.io_uring_cqe = undefined;
    try testing.expectEqual(@as(u32, 0), try ring.peekBatch(&cqes));
}

test "FsRing: lazy batch — multiple preps, one flush" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    var tmp = try TmpPath.create();
    defer tmp.cleanup();

    const open_flags: linux.O = .{ .ACCMODE = .WRONLY, .TRUNC = true };
    try ring.prepOpenAt(linux.AT.FDCWD, tmp.ptr(), open_flags, 0o644, 100);
    _ = try ring.flush();
    var cqes: [16]linux.io_uring_cqe = undefined;
    _ = try ring.waitOne(&cqes);
    const fd = cqes[0].res;

    // Queue eight writes without flushing between.
    const chunks = "ABCDEFGH";
    var i: u64 = 0;
    while (i < 8) : (i += 1) {
        try ring.prepWrite(fd, chunks[i .. i + 1], i, 200 + i);
    }
    try testing.expect(ring.has_pending);
    const submitted = try ring.flush();
    try testing.expectEqual(@as(u32, 8), submitted);
    try testing.expect(!ring.has_pending);
    // A second flush with nothing pending is a no-op.
    try testing.expectEqual(@as(u32, 0), try ring.flush());

    var total: u32 = 0;
    while (total < 8) {
        total += try ring.waitOne(cqes[total..]);
    }
    try testing.expectEqual(@as(u32, 8), total);
    for (cqes[0..total]) |cqe| {
        try testing.expect(cqe.user_data >= 200 and cqe.user_data < 208);
        try testing.expectEqual(@as(i32, 1), cqe.res);
    }

    try ring.prepClose(fd, 300);
    _ = try ring.flush();
    _ = try ring.waitOne(&cqes);
}

// Use poll(2) directly to test whether the eventfd is readable
// without consuming bytes from it. Avoids depending on the
// reactor's epoll plumbing (this test is standalone).
extern "c" fn poll(fds: [*]PollFd, nfds: u32, timeout_ms: c_int) c_int;
const PollFd = extern struct {
    fd: c_int,
    events: i16,
    revents: i16,
};
const POLLIN: i16 = 0x001;

fn eventFdReadable(fd: i32) bool {
    var pfd = [_]PollFd{.{ .fd = @intCast(fd), .events = POLLIN, .revents = 0 }};
    const rc = poll(&pfd, 1, 0);
    if (rc <= 0) return false;
    return (pfd[0].revents & POLLIN) != 0;
}

test "FsRing: eventfd fires on CQE landing, drain clears it" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    try ring.registerEventFd();
    try testing.expect(ring.eventfd >= 0);
    // Nothing in flight → eventfd is NOT readable.
    try testing.expect(!eventFdReadable(ring.eventfd));

    // Submit an fsync on stderr (always-open fd; the syscall is
    // either a no-op or returns EINVAL — either is a CQE we can
    // wait for). user_data = 1.
    try ring.prepFsync(2, 1, false);
    _ = try ring.flush();

    // Block until the CQE lands so we know the kernel has had a
    // chance to also write to the eventfd. (The eventfd write
    // happens at CQE post time, before the userspace `wait_cqe`
    // returns.)
    var cqes: [1]linux.io_uring_cqe = undefined;
    _ = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u64, 1), cqes[0].user_data);

    // Now the eventfd MUST be readable — the kernel wrote 1 to
    // its counter when the CQE landed.
    try testing.expect(eventFdReadable(ring.eventfd));

    // Drain it (Phase 2C.1's contract): the 8-byte read clears
    // the counter, and a subsequent poll reports non-readable.
    ring.drainEventFd();
    try testing.expect(!eventFdReadable(ring.eventfd));
}

test "FsRing: registerEventFd is idempotent" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    try ring.registerEventFd();
    const fd_first = ring.eventfd;
    try ring.registerEventFd(); // no-op second call
    try testing.expectEqual(fd_first, ring.eventfd);
}

// ─── Phase 3B tests: FsOp state + prepCancel + cancel-ack tag ───

test "FsOp: default state is Pending" {
    // Constructable without a Linux runtime; the FsOp struct is
    // pure data. Caller fills `coro` and `result` at use site;
    // `state` defaults to STATE_PENDING so existing Phase 2
    // call sites (which don't touch state) still work unchanged.
    var dummy_coro: @import("coroutine.zig").Coroutine = undefined;
    var op: FsOp = .{
        .coro = &dummy_coro,
        .result = .{ .value = 0, .err = 0 },
    };
    try testing.expectEqual(STATE_PENDING, op.state.load(.acquire));
}

test "cancel-ack tag encoding round-trips" {
    // FsOp pointers are at least 8-byte aligned, so bit 0 is
    // free for the cancel-ack tag.
    const fake_op: u64 = 0xCAFEBABE_DEAD_0000; // 8-byte aligned
    try testing.expect(!isCancelAckUserData(fake_op));
    const tagged = cancelUserDataFor(fake_op);
    try testing.expect(isCancelAckUserData(tagged));
    // Both encode "the same op" — only the tag bit differs.
    try testing.expectEqual(fake_op | CANCEL_ACK_TAG, tagged);
}

test "FsRing: prepCancel submits, drain sees cancel-ack" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var ring = FsRing.init() catch |e| {
        if (e == error.IoUringUnavailable) return error.SkipZigTest;
        return e;
    };
    defer ring.deinit();

    // Submit a long-ish op (an fsync on stderr — quick but
    // gives the kernel something to point ASYNC_CANCEL at).
    // user_data = 0x100 (low bit clear → "original op").
    try ring.prepFsync(2, 0x100, false);
    _ = try ring.flush();

    // Drain the original CQE first so the kernel has finished
    // with op 0x100 — otherwise ASYNC_CANCEL will return ENOENT
    // for "already completed." We're not testing cancellation
    // semantics here, just that prepCancel actually submits AND
    // that the cancel ack arrives with its own user_data.
    var cqes: [2]linux.io_uring_cqe = undefined;
    _ = try ring.waitOne(&cqes);

    // Now submit a cancel targeting a non-existent op (use a
    // fake target_user_data that the kernel will respond ENOENT
    // to). user_data on the cancel = 0x201 (low bit set → cancel
    // ack tag).
    try ring.prepCancel(0xDEADBEEF, 0x201);
    _ = try ring.flush();

    _ = try ring.waitOne(&cqes);
    try testing.expectEqual(@as(u64, 0x201), cqes[0].user_data);
    try testing.expect(isCancelAckUserData(cqes[0].user_data));
    // ENOENT (errno 2) because the target user_data doesn't
    // exist — proves the cancel SQE actually reached the kernel
    // (vs. being silently dropped) and that the user_data
    // round-tripped.
    try testing.expectEqual(@as(i32, -2), cqes[0].res);
}

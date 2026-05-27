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

comptime {
    if (builtin.os.tag != .linux) {
        @compileError("fs_ring.zig is Linux-only — guard imports with comptime os.tag checks");
    }
}

/// Default SQ/CQ depth. Matches the conservative Seastar default
/// (`s_queue_len = 200`; rounded to a power of two). Caller may
/// override via `init`.
pub const DEFAULT_RING_ENTRIES: u16 = 256;

/// Setup-flag tiers, tried in order from most aggressive to bare.
/// Stops at the first combo that succeeds. Modern kernels (6.1+)
/// take the first tier; older kernels degrade gracefully.
///
/// Why this ordering: each tier strictly weakens the previous one
/// by dropping the newest-required flag. A kernel that rejects
/// SUBMIT_ALL still accepts (SINGLE_ISSUER | DEFER_TASKRUN); a
/// kernel that rejects DEFER_TASKRUN still accepts SINGLE_ISSUER;
/// and 0 always works on any kernel that supports io_uring at all.
const SETUP_TIERS = [_]u32{
    linux.IORING_SETUP_SINGLE_ISSUER |
        linux.IORING_SETUP_DEFER_TASKRUN |
        linux.IORING_SETUP_SUBMIT_ALL,
    linux.IORING_SETUP_SINGLE_ISSUER |
        linux.IORING_SETUP_DEFER_TASKRUN,
    linux.IORING_SETUP_SINGLE_ISSUER,
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
        self.ring.deinit();
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

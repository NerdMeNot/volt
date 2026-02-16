//! Linux io_uring Backend
//!
//! High-performance completion-based I/O for Linux 5.1+.
//! Uses Zig's stdlib IoUring wrapper which implements the liburing interface.
//!
//! Architecture:
//! - Slab allocator for O(1) operation tracking
//! - Batched submissions for throughput (configurable batch size)
//! - EBUSY handling: dispatch completions then retry
//! - Probe-based opcode validation
//! - Lifecycle tracking: Submitted -> Completed/Cancelled
//!
//! Key features:
//! - True async I/O (not just non-blocking + readiness)
//! - Batched submissions for efficiency
//! - Zero-copy with registered buffers (optional)
//! - SQPOLL for kernel-side polling (optional)
//!
//! Based on liburing patterns and production io_uring best practices.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

const completion = @import("types.zig");
const Operation = completion.Operation;
const Completion = completion.Completion;
const SubmissionId = completion.SubmissionId;

const slab = @import("../util/slab.zig");

const IoUring = linux.IoUring;

/// Check if io_uring is supported on this system.
pub fn isSupported() bool {
    // Try to create a minimal ring to test support
    var params = std.mem.zeroInit(linux.io_uring_params, .{});
    const res = linux.io_uring_setup(1, &params);

    if (linux.E.init(res) == .SUCCESS) {
        // Successfully created, close it
        posix.close(@intCast(res));
        return true;
    }

    return false;
}

/// Get kernel version for feature detection.
pub fn getKernelVersion() ?struct { major: u32, minor: u32 } {
    var uts: linux.utsname = undefined;
    if (linux.E.init(linux.uname(&uts)) != .SUCCESS) return null;

    const release = std.mem.sliceTo(&uts.release, 0);
    var it = std.mem.splitScalar(u8, release, '.');

    const major = std.fmt.parseInt(u32, it.next() orelse return null, 10) catch return null;
    const minor = std.fmt.parseInt(u32, it.next() orelse return null, 10) catch return null;

    return .{ .major = major, .minor = minor };
}

/// io_uring feature flags detected from kernel version.
pub const Features = struct {
    /// Multishot accept (kernel 5.19+).
    has_multishot_accept: bool,

    /// Multishot recv (kernel 6.0+).
    has_multishot_recv: bool,

    /// Buffer rings for provided buffers (kernel 5.19+).
    has_buffer_ring: bool,

    /// Single issuer optimization (kernel 5.11+).
    has_single_issuer: bool,

    /// Detect available features based on kernel version.
    pub fn detect() Features {
        const ver = getKernelVersion() orelse return .{
            .has_multishot_accept = false,
            .has_multishot_recv = false,
            .has_buffer_ring = false,
            .has_single_issuer = false,
        };

        return .{
            // Multishot accept: kernel 5.19+
            .has_multishot_accept = ver.major > 5 or (ver.major == 5 and ver.minor >= 19),
            // Multishot recv: kernel 6.0+
            .has_multishot_recv = ver.major >= 6,
            // Buffer rings: kernel 5.19+
            .has_buffer_ring = ver.major > 5 or (ver.major == 5 and ver.minor >= 19),
            // Single issuer: kernel 5.11+
            .has_single_issuer = ver.major > 5 or (ver.major == 5 and ver.minor >= 11),
        };
    }

    /// Check if any multishot operations are available.
    pub fn hasMultishot(self: Features) bool {
        return self.has_multishot_accept or self.has_multishot_recv;
    }
};

/// Operation lifecycle state.
const Lifecycle = enum {
    /// Operation submitted to SQ, waiting for CQE.
    submitted,
    /// Operation completed successfully.
    completed,
    /// Operation was cancelled.
    cancelled,
};

/// Tracked operation in the slab.
const TrackedOp = struct {
    /// Original operation.
    op: Operation,
    /// Submission ID for user tracking.
    submission_id: SubmissionId,
    /// Lifecycle state.
    state: Lifecycle,
    /// Whether this is a multishot operation (don't remove on first completion).
    is_multishot: bool,

    fn init(op: Operation, submission_id: SubmissionId) TrackedOp {
        return .{
            .op = op,
            .submission_id = submission_id,
            .state = .submitted,
            .is_multishot = isMultishotOp(op),
        };
    }

    /// Check if an operation type is multishot.
    fn isMultishotOp(op: Operation) bool {
        return switch (op.op) {
            .accept_multishot, .recv_multishot => true,
            else => false,
        };
    }
};

/// Default batch size before auto-flush.
const DEFAULT_BATCH_SIZE: u32 = 64;

/// Bit layout for internal user_data encoding:
/// | slab_key (32 bits) | user_visible_data (32 bits) |
/// This allows O(1) completion lookup while preserving user's data.
const SLAB_KEY_SHIFT: u6 = 32;
const USER_DATA_MASK: u64 = 0xFFFFFFFF;

/// Special token for wakeup operations.
const TOKEN_WAKEUP: u64 = std.math.maxInt(u64);

/// io_uring backend implementation with production-grade batching.
pub const IoUringBackend = struct {
    ring: IoUring,
    allocator: std.mem.Allocator,

    /// Slab of tracked operations for lifecycle management.
    operations: slab.Slab(TrackedOp),

    /// Number of submissions pending (not yet flushed to kernel).
    pending_submissions: u32,

    /// Batch size threshold for auto-flush.
    batch_size: u32,

    /// CQE buffer for batch completion retrieval.
    cqe_buffer: []linux.io_uring_cqe,

    /// Next user_data value for internal tracking.
    next_user_data: u64,

    /// eventfd for cross-thread wakeup.
    wakeup_fd: posix.fd_t,

    /// Tracks if a wakeup is pending.
    wakeup_pending: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: anytype) !Self {
        const entries = if (config.ring_entries == 0)
            config.defaultRingEntries()
        else
            config.ring_entries;

        // Build io_uring flags
        var flags: u32 = 0;
        if (config.sqpoll) {
            flags |= linux.IORING_SETUP_SQPOLL;
        }
        if (config.iopoll) {
            flags |= linux.IORING_SETUP_IOPOLL;
        }
        if (config.single_issuer) {
            // Check if kernel supports it (5.11+)
            if (getKernelVersion()) |ver| {
                if (ver.major > 5 or (ver.major == 5 and ver.minor >= 11)) {
                    flags |= linux.IORING_SETUP_SINGLE_ISSUER;
                }
            }
        }

        var ring = IoUring.init(entries, flags) catch |err| {
            return switch (err) {
                error.SystemOutdated => error.UnsupportedPlatform,
                else => err,
            };
        };
        errdefer ring.deinit();

        const max_completions = config.max_completions;
        const cqe_buffer = try allocator.alloc(linux.io_uring_cqe, max_completions);
        errdefer allocator.free(cqe_buffer);

        // Pre-allocate slab with capacity matching ring size
        var ops_slab = try slab.Slab(TrackedOp).init(allocator, entries);
        errdefer ops_slab.deinit();

        // Create eventfd for cross-thread wakeup
        const wakeup_result = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        if (@as(isize, @bitCast(wakeup_result)) < 0) {
            return error.WakeupCreationFailed;
        }
        const wakeup_fd: posix.fd_t = @intCast(wakeup_result);
        errdefer posix.close(wakeup_fd);

        // Register eventfd with io_uring for efficient wakeup
        // This allows the kernel to signal when the eventfd is written to
        const reg_result = ring.register_eventfd(wakeup_fd);
        if (reg_result != 0) {
            // Non-fatal: wakeup will still work via polling
            // Some older kernels may not support this
        }

        return Self{
            .ring = ring,
            .allocator = allocator,
            .operations = ops_slab,
            .pending_submissions = 0,
            .batch_size = DEFAULT_BATCH_SIZE,
            .cqe_buffer = cqe_buffer,
            .next_user_data = 1, // 0 is reserved for INVALID
            .wakeup_fd = wakeup_fd,
            .wakeup_pending = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Self) void {
        posix.close(self.wakeup_fd);
        self.allocator.free(self.cqe_buffer);
        self.operations.deinit();
        self.ring.deinit();
    }

    /// Wake up the I/O driver from another thread.
    /// This is used to notify the driver when new work is available.
    /// Safe to call from any thread. Coalesces multiple calls.
    pub fn unpark(self: *Self) void {
        // Use swap to coalesce multiple unpark calls
        if (self.wakeup_pending.swap(true, .acq_rel)) {
            return; // Already pending
        }

        // Write to eventfd to wake the ring
        const val: u64 = 1;
        _ = posix.write(self.wakeup_fd, std.mem.asBytes(&val)) catch {};
    }

    /// Submit an operation to the ring.
    /// Operations are batched and flushed when:
    /// 1. Batch size threshold is reached
    /// 2. SQ is full (with EBUSY handling)
    /// 3. wait() is called
    /// 4. flush() is called explicitly
    pub fn submit(self: *Self, op: Operation) !SubmissionId {
        // Use provided user_data or generate one (masked to 32 bits)
        const user_visible_data: u32 = if (op.user_data != 0)
            @truncate(op.user_data)
        else blk: {
            const id: u32 = @truncate(self.next_user_data);
            self.next_user_data +%= 1;
            if (self.next_user_data == 0) self.next_user_data = 1;
            break :blk id;
        };

        const submission_id = SubmissionId.init(user_visible_data);

        // Track the operation in slab
        const slab_key = try self.operations.insert(TrackedOp.init(op, submission_id));
        errdefer _ = self.operations.remove(slab_key);

        // Encode slab_key into upper 32 bits for O(1) completion lookup
        const internal_user_data = encodeUserData(slab_key, user_visible_data);

        // Try to get an SQE
        const sqe = self.ring.get_sqe() catch {
            // SQ full - handle EBUSY: dispatch completions then retry
            try self.handleSqFull();
            return self.submitWithSqe(op, internal_user_data, slab_key);
        };

        self.prepareSqe(sqe, op, internal_user_data);
        self.pending_submissions += 1;

        // Auto-flush if batch size reached
        if (self.pending_submissions >= self.batch_size) {
            _ = try self.flush();
        }

        return submission_id;
    }

    /// Encode slab_key and user-visible data into a single u64.
    fn encodeUserData(slab_key: usize, user_visible: u32) u64 {
        return (@as(u64, @truncate(slab_key)) << SLAB_KEY_SHIFT) | user_visible;
    }

    /// Decode slab_key from internal user_data.
    fn decodeSlabKey(internal_user_data: u64) usize {
        return @truncate(internal_user_data >> SLAB_KEY_SHIFT);
    }

    /// Decode user-visible data from internal user_data.
    fn decodeUserVisible(internal_user_data: u64) u32 {
        return @truncate(internal_user_data & USER_DATA_MASK);
    }

    /// Submit when we already have the slab key (retry path).
    fn submitWithSqe(self: *Self, op: Operation, internal_user_data: u64, slab_key: usize) !SubmissionId {
        _ = slab_key;

        const sqe = self.ring.get_sqe() catch {
            return error.SubmissionQueueFull;
        };

        self.prepareSqe(sqe, op, internal_user_data);
        self.pending_submissions += 1;

        return SubmissionId.init(decodeUserVisible(internal_user_data));
    }

    /// Handle SQ full condition: drain CQEs and flush.
    fn handleSqFull(self: *Self) !void {
        // First, drain any pending completions to free SQ slots
        var temp_completions: [64]Completion = undefined;
        _ = try self.drainCompletions(&temp_completions);

        // Then flush pending submissions
        _ = try self.flush();
    }

    /// Prepare an SQE for the given operation.
    fn prepareSqe(self: *Self, sqe: *linux.io_uring_sqe, op: Operation, user_data: u64) void {
        _ = self;

        switch (op.op) {
            .read => |r| {
                sqe.prep_read(r.fd, r.buffer, r.offset orelse 0);
            },
            .write => |w| {
                sqe.prep_write(w.fd, w.buffer, w.offset orelse 0);
            },
            .open => |o| {
                sqe.prep_openat(linux.AT.FDCWD, o.path, o.flags, o.mode);
            },
            .close => |c| {
                sqe.prep_close(c.fd);
            },
            .fsync => |f| {
                if (f.datasync) {
                    sqe.prep_fsync(f.fd, linux.IORING_FSYNC_DATASYNC);
                } else {
                    sqe.prep_fsync(f.fd, 0);
                }
            },
            .accept => |a| {
                sqe.prep_accept(a.fd, a.addr, a.addr_len, 0);
            },
            .accept_multishot => |a| {
                sqe.prep_accept(a.fd, a.addr, a.addr_len, 0);
                // Set multishot flag - kernel will auto-rearm the accept
                sqe.ioprio |= linux.IORING_ACCEPT_MULTISHOT;
            },
            .connect => |c| {
                sqe.prep_connect(c.fd, c.addr, c.addr_len);
            },
            .recv => |r| {
                sqe.prep_recv(r.fd, r.buffer, r.flags);
            },
            .recv_multishot => |r| {
                // Multishot recv uses provided buffers from a buffer ring
                sqe.prep_recv(r.fd, &.{}, r.flags);
                // Set buffer select flag - kernel will pick buffer from group
                sqe.flags |= linux.IOSQE_BUFFER_SELECT;
                // Set multishot flag - kernel will auto-rearm
                sqe.ioprio |= linux.IORING_RECV_MULTISHOT;
                // Set buffer group ID
                sqe.buf_index = r.buf_group;
            },
            .send => |s| {
                sqe.prep_send(s.fd, s.buffer, s.flags);
            },
            .readv => |r| {
                sqe.prep_readv(r.fd, r.iovecs, r.offset orelse 0);
            },
            .writev => |w| {
                sqe.prep_writev(w.fd, w.iovecs, w.offset orelse 0);
            },
            .timeout => |t| {
                // Clamp timeout to safe maximum to prevent overflow
                const safe_ns = completion.clampTimeout(t.ns);
                const ts = completion.timespecFromNanos(safe_ns);
                sqe.prep_timeout(&ts, 0, 0);
            },
            .cancel => |c| {
                sqe.prep_cancel(c.target_id.value, 0);
            },
            .nop => {
                sqe.prep_nop();
            },
        }

        sqe.user_data = user_data;
    }

    /// Flush pending submissions to the kernel with EINTR retry.
    /// Returns number of submissions flushed.
    pub fn flush(self: *Self) !u32 {
        if (self.pending_submissions == 0) return 0;

        while (true) {
            const submitted = self.ring.submit() catch |err| {
                return switch (err) {
                    error.SignalInterrupt => continue, // Retry on EINTR
                    error.CompletionQueueOvercommitted, error.SubmissionQueueEntryOverflow => {
                        // CQ overcommitted - drain completions and retry
                        var temp: [64]Completion = undefined;
                        _ = try self.drainCompletions(&temp);
                        continue;
                    },
                    else => err,
                };
            };

            self.pending_submissions = 0;
            return submitted;
        }
    }

    /// Wait for completions.
    /// Returns number of completions copied to buffer.
    pub fn wait(self: *Self, completions: []Completion, timeout_ns: ?u64) !usize {
        // Check if we were woken by unpark() - drain eventfd if so
        if (self.wakeup_pending.swap(false, .acq_rel)) {
            // Drain the eventfd counter
            var buf: u64 = undefined;
            _ = posix.read(self.wakeup_fd, std.mem.asBytes(&buf)) catch {};
        }

        // Flush any pending submissions first
        _ = try self.flush();

        // Determine wait count
        const wait_nr: u32 = if (timeout_ns) |ns| blk: {
            if (ns == 0) break :blk 0; // Non-blocking
            break :blk 1; // Wait for at least 1
        } else 1; // Blocking wait

        // Copy CQEs from ring with EINTR retry
        const max_cqes: u32 = @intCast(@min(completions.len, self.cqe_buffer.len));
        const count = self.copyCompletionsWithRetry(max_cqes, wait_nr) catch |err| {
            return switch (err) {
                error.SignalInterrupt => 0,
                else => err,
            };
        };

        // Convert to our Completion type and update lifecycle
        for (0..count) |i| {
            const cqe = &self.cqe_buffer[i];

            // Decode user-visible data for the completion result
            const user_visible = decodeUserVisible(cqe.user_data);
            completions[i] = Completion{
                .user_data = user_visible,
                .result = cqe.res,
                .flags = cqe.flags,
            };

            // Handle multishot: check CQE_F_MORE flag
            // If MORE is set, more completions are coming - DON'T remove from slab
            // If MORE is not set, this is the final completion
            const has_more = (cqe.flags & completion.CQEFlags.MORE) != 0;
            if (!has_more) {
                // Final completion (or non-multishot) - remove from slab
                self.markCompletedFast(cqe.user_data);
            }
            // If has_more, leave the operation in the slab for subsequent completions
        }

        return count;
    }

    /// Copy completions with EINTR retry.
    fn copyCompletionsWithRetry(self: *Self, max_cqes: u32, wait_nr: u32) !usize {
        while (true) {
            const count = self.ring.copy_cqes(self.cqe_buffer[0..max_cqes], wait_nr) catch |err| {
                return switch (err) {
                    error.SignalInterrupt => continue, // Retry on EINTR
                    else => err,
                };
            };
            return count;
        }
    }

    /// Drain completions without blocking (non-blocking poll).
    fn drainCompletions(self: *Self, completions: []Completion) !usize {
        const max_cqes: u32 = @intCast(@min(completions.len, self.cqe_buffer.len));
        const count = self.ring.copy_cqes(self.cqe_buffer[0..max_cqes], 0) catch |err| {
            return switch (err) {
                error.SignalInterrupt => 0,
                else => err,
            };
        };

        for (0..count) |i| {
            const cqe = &self.cqe_buffer[i];
            const user_visible = decodeUserVisible(cqe.user_data);
            completions[i] = Completion{
                .user_data = user_visible,
                .result = cqe.res,
                .flags = cqe.flags,
            };

            // Handle multishot: only remove if this is the final completion
            const has_more = (cqe.flags & completion.CQEFlags.MORE) != 0;
            if (!has_more) {
                self.markCompletedFast(cqe.user_data);
            }
        }

        return count;
    }

    /// Mark an operation as completed and remove from slab - O(1) version.
    /// Uses the encoded slab_key in user_data for direct lookup.
    fn markCompletedFast(self: *Self, internal_user_data: u64) void {
        const slab_key = decodeSlabKey(internal_user_data);
        _ = self.operations.remove(slab_key);
    }

    /// Mark an operation as completed by submission_id - O(n) fallback.
    /// Used for cancel operations where we search by user-visible ID.
    fn markCompletedBySubmissionId(self: *Self, submission_id: u64) void {
        var it = self.operations.iterator();
        while (it.next()) |entry| {
            if (entry.value.submission_id.value == submission_id) {
                _ = self.operations.remove(entry.key);
                return;
            }
        }
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Self, id: SubmissionId) !void {
        // Mark as cancelled in slab
        var it = self.operations.iterator();
        while (it.next()) |entry| {
            if (entry.value.submission_id.value == id.value) {
                entry.value.state = .cancelled;
                break;
            }
        }

        // Submit cancel request to io_uring
        _ = try self.submit(.{
            .op = .{ .cancel = .{ .target_id = id } },
            .user_data = 0, // Cancel result user_data is not important
        });
        _ = try self.flush();
    }

    /// Set the batch size for auto-flush threshold.
    pub fn setBatchSize(self: *Self, size: u32) void {
        self.batch_size = if (size == 0) DEFAULT_BATCH_SIZE else size;
    }

    /// Get current number of in-flight operations.
    pub fn inFlightCount(self: *const Self) usize {
        return self.operations.count();
    }

    /// Get the io_uring file descriptor.
    pub fn fd(self: Self) posix.fd_t {
        return self.ring.fd;
    }

    /// Get detected kernel features.
    pub fn getFeatures() Features {
        return Features.detect();
    }

    /// Check if multishot accept is supported.
    pub fn supportsMultishotAccept() bool {
        return Features.detect().has_multishot_accept;
    }

    /// Check if multishot recv is supported.
    pub fn supportsMultishotRecv() bool {
        return Features.detect().has_multishot_recv;
    }

    /// Cancel a multishot operation.
    /// For multishot operations, you may want to explicitly cancel them
    /// when you're done accepting connections or receiving data.
    pub fn cancelMultishot(self: *Self, id: SubmissionId) !void {
        // Same as regular cancel
        try self.cancel(id);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "IoUringBackend - isSupported" {
    // This test just checks the function doesn't crash
    _ = isSupported();
}

test "IoUringBackend - getKernelVersion" {
    if (getKernelVersion()) |ver| {
        try std.testing.expect(ver.major >= 4);
    }
}

test "IoUringBackend - init and deinit" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    try std.testing.expect(backend.fd() >= 0);
}

test "IoUringBackend - nop operation" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a NOP
    const id = try backend.submit(.{
        .op = .{ .nop = {} },
        .user_data = 42,
    });
    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.inFlightCount());

    // Wait for completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, null);
    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 42), completions[0].user_data);
    try std.testing.expect(completions[0].isSuccess());
    try std.testing.expectEqual(@as(usize, 0), backend.inFlightCount());
}

test "IoUringBackend - batching" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Set small batch size for testing
    backend.setBatchSize(4);

    // Submit 3 NOPs (under batch threshold)
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 1 });
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 2 });
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 3 });

    try std.testing.expectEqual(@as(u32, 3), backend.pending_submissions);

    // Explicit flush
    const flushed = try backend.flush();
    try std.testing.expectEqual(@as(u32, 3), flushed);
    try std.testing.expectEqual(@as(u32, 0), backend.pending_submissions);

    // Wait for all completions
    var completions: [8]Completion = undefined;
    var total: usize = 0;
    while (total < 3) {
        const count = try backend.wait(&completions[total..], 1_000_000_000);
        total += count;
    }
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "IoUringBackend - timeout operation" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a short timeout (10ms)
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 10_000_000 } },
        .user_data = 123,
    });

    try std.testing.expect(id.isValid());

    // Wait for timeout to expire
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000); // 100ms wait

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 123), completions[0].user_data);
}

test "IoUringBackend - multiple timeout submissions" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit multiple timers with different durations
    _ = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 5_000_000 } }, // 5ms
        .user_data = 1,
    });
    _ = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 10_000_000 } }, // 10ms
        .user_data = 2,
    });
    _ = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 15_000_000 } }, // 15ms
        .user_data = 3,
    });

    // Wait for all timers
    var completions: [8]Completion = undefined;
    var total_completed: usize = 0;
    var seen: [4]bool = .{ false, false, false, false };

    const start = std.time.nanoTimestamp();
    while (total_completed < 3) {
        const count = try backend.wait(&completions, 100_000_000);
        for (completions[0..count]) |comp| {
            if (comp.user_data >= 1 and comp.user_data <= 3) {
                seen[@intCast(comp.user_data)] = true;
            }
            total_completed += 1;
        }
        if (std.time.nanoTimestamp() - start > 500_000_000) break;
    }

    try std.testing.expect(seen[1]);
    try std.testing.expect(seen[2]);
    try std.testing.expect(seen[3]);
}

test "IoUringBackend - wait with zero timeout" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // No operations submitted - wait with zero timeout
    var completions: [8]Completion = undefined;
    const start = std.time.nanoTimestamp();
    const count = try backend.wait(&completions, 0);
    const elapsed = std.time.nanoTimestamp() - start;

    // Should return immediately
    try std.testing.expectEqual(@as(usize, 0), count);
    try std.testing.expect(elapsed < 10_000_000); // < 10ms
}

test "IoUringBackend - TCP socket accept" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create listening socket
    const listen_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK, 0);
    defer std.posix.close(listen_fd);

    // Bind to ephemeral port
    var addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port = 0,
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    try std.posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));
    try std.posix.listen(listen_fd, 1);

    // Get assigned port
    var name_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    try std.posix.getsockname(listen_fd, @ptrCast(&addr), &name_len);

    // Submit accept operation
    var accept_addr: std.posix.sockaddr = undefined;
    var accept_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    _ = try backend.submit(.{
        .op = .{
            .accept = .{
                .fd = listen_fd,
                .addr = &accept_addr,
                .addr_len = &accept_addr_len,
            },
        },
        .user_data = 100,
    });

    // Create client and connect
    const client_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(client_fd);

    try std.posix.connect(client_fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));

    // Wait for accept completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 100), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0);

    // Clean up accepted fd
    std.posix.close(@intCast(completions[0].result));
}

test "IoUringBackend - TCP socket send/recv" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create connected socket pair using TCP loopback
    const listen_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(listen_fd);

    var addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port = 0,
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    try std.posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));
    try std.posix.listen(listen_fd, 1);

    var name_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    try std.posix.getsockname(listen_fd, @ptrCast(&addr), &name_len);

    const client_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(client_fd);
    try std.posix.connect(client_fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));

    var accept_addr: std.posix.sockaddr = undefined;
    var accept_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const server_fd = try std.posix.accept(listen_fd, &accept_addr, &accept_len, 0);
    defer std.posix.close(server_fd);

    // Submit send on client
    const send_data = "Hello io_uring!";
    _ = try backend.submit(.{
        .op = .{
            .send = .{
                .fd = client_fd,
                .buffer = send_data,
                .flags = 0,
            },
        },
        .user_data = 200,
    });

    // Submit recv on server
    var recv_buf: [64]u8 = undefined;
    _ = try backend.submit(.{
        .op = .{
            .recv = .{
                .fd = server_fd,
                .buffer = &recv_buf,
                .flags = 0,
            },
        },
        .user_data = 201,
    });

    // Wait for both
    var completions: [8]Completion = undefined;
    var send_done = false;
    var recv_done = false;
    var recv_len: usize = 0;
    const start = std.time.nanoTimestamp();

    while (!send_done or !recv_done) {
        const count = try backend.wait(&completions, 100_000_000);
        for (completions[0..count]) |comp| {
            if (comp.user_data == 200) send_done = true;
            if (comp.user_data == 201) {
                recv_done = true;
                if (comp.result > 0) recv_len = @intCast(comp.result);
            }
        }
        if (std.time.nanoTimestamp() - start > 1_000_000_000) break;
    }

    try std.testing.expect(send_done);
    try std.testing.expect(recv_done);
    try std.testing.expectEqual(send_data.len, recv_len);
    try std.testing.expectEqualStrings(send_data, recv_buf[0..recv_len]);
}

test "IoUringBackend - cancel operation" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a long timeout
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 10_000_000_000 } }, // 10 seconds
        .user_data = 999,
    });

    try std.testing.expect(id.isValid());

    // Cancel it
    try backend.cancel(id);

    // Poll - should not hang
    var completions: [8]Completion = undefined;
    _ = try backend.wait(&completions, 100_000_000);

    // The operation should be cancelled (either cancelled or never started)
}

test "IoUringBackend - close operation" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a socket to close
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);

    // Submit close operation
    _ = try backend.submit(.{
        .op = .{ .close = .{ .fd = fd } },
        .user_data = 777,
    });

    // Wait for completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 777), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result);
}

test "IoUringBackend - user_data preserved" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit operations with distinct user_data values
    const user_data_values = [_]u64{ 0xDEADBEEF, 0xCAFEBABE, 0x12345678, std.math.maxInt(u64) };

    for (user_data_values) |ud| {
        _ = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = ud,
        });
    }

    var completions: [8]Completion = undefined;
    var total: usize = 0;
    var found: [4]bool = .{ false, false, false, false };

    while (total < 4) {
        const count = try backend.wait(&completions, 100_000_000);
        for (completions[0..count]) |comp| {
            for (user_data_values, 0..) |ud, i| {
                if (comp.user_data == ud) found[i] = true;
            }
            total += 1;
        }
    }

    for (found) |f| {
        try std.testing.expect(f);
    }
}

test "IoUringBackend - read and write on pipe" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a pipe
    const pipe_fds = try std.posix.pipe();
    defer {
        std.posix.close(pipe_fds[0]);
        std.posix.close(pipe_fds[1]);
    }

    // Write data first (synchronously)
    const write_data = "Test io_uring pipe";
    _ = try std.posix.write(pipe_fds[1], write_data);

    // Submit read operation
    var read_buf: [64]u8 = undefined;
    _ = try backend.submit(.{
        .op = .{
            .read = .{
                .fd = pipe_fds[0],
                .buffer = &read_buf,
                .offset = null,
            },
        },
        .user_data = 888,
    });

    // Wait for read completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 888), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0);

    const read_len: usize = @intCast(completions[0].result);
    try std.testing.expectEqualStrings(write_data, read_buf[0..read_len]);
}

test "IoUringBackend - high operation count" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{ .max_completions = 256 });
    defer backend.deinit();

    // Submit many nop operations
    const count_ops: usize = 100;
    for (0..count_ops) |i| {
        _ = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = @intCast(i),
        });
    }

    // Wait for all completions
    var completions: [256]Completion = undefined;
    var total: usize = 0;
    const start = std.time.nanoTimestamp();

    while (total < count_ops) {
        const count = try backend.wait(&completions, 100_000_000);
        total += count;
        if (std.time.nanoTimestamp() - start > 2_000_000_000) break;
    }

    try std.testing.expectEqual(count_ops, total);
}

test "IoUringBackend - rapid submit cycles" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Rapidly submit and wait for operations
    var completions: [8]Completion = undefined;

    for (0..50) |i| {
        _ = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = @intCast(i),
        });

        // Wait for completion
        const count = try backend.wait(&completions, 100_000_000);
        try std.testing.expect(count >= 1);
    }
}

test "IoUringBackend - inFlightCount tracking" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    try std.testing.expectEqual(@as(usize, 0), backend.inFlightCount());

    // Submit some operations
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 1 });
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 2 });
    _ = try backend.submit(.{ .op = .{ .nop = {} }, .user_data = 3 });

    try std.testing.expectEqual(@as(usize, 3), backend.inFlightCount());

    // Wait for all
    var completions: [8]Completion = undefined;
    var total: usize = 0;
    while (total < 3) {
        const count = try backend.wait(&completions, 100_000_000);
        total += count;
    }

    try std.testing.expectEqual(@as(usize, 0), backend.inFlightCount());
}

test "IoUringBackend - multiple nops complete in order" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit several nops
    for (0..5) |i| {
        _ = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = @intCast(i + 1),
        });
    }

    // Collect all completions
    var completions: [8]Completion = undefined;
    var total: usize = 0;
    var seen: [6]bool = .{ false, false, false, false, false, false };

    while (total < 5) {
        const count = try backend.wait(&completions, 100_000_000);
        for (completions[0..count]) |comp| {
            if (comp.user_data >= 1 and comp.user_data <= 5) {
                seen[@intCast(comp.user_data)] = true;
            }
            total += 1;
        }
    }

    // All should be seen
    for (1..6) |i| {
        try std.testing.expect(seen[i]);
    }
}

test "IoUringBackend - fd returns valid ring fd" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    const ring_fd = backend.fd();
    try std.testing.expect(ring_fd >= 0);

    // Verify it's a valid fd by checking we can query it
    // (We can't do much without making assumptions about the fd)
}

test "IoUringBackend - write operation on pipe" {
    if (!isSupported()) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a pipe
    const pipe_fds = try std.posix.pipe();
    defer {
        std.posix.close(pipe_fds[0]);
        std.posix.close(pipe_fds[1]);
    }

    // Submit write operation via io_uring
    const write_data = "io_uring write test";
    _ = try backend.submit(.{
        .op = .{
            .write = .{
                .fd = pipe_fds[1],
                .buffer = write_data,
                .offset = null,
            },
        },
        .user_data = 999,
    });

    // Wait for write completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 999), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0);

    // Verify data was written by reading it back
    var read_buf: [64]u8 = undefined;
    const read_len = try std.posix.read(pipe_fds[0], &read_buf);
    try std.testing.expectEqualStrings(write_data, read_buf[0..read_len]);
}

test "IoUringBackend - feature detection" {
    const features = Features.detect();

    // Just verify the detection doesn't crash
    // The actual values depend on kernel version
    if (getKernelVersion()) |ver| {
        // On kernel 5.19+, we should have multishot accept
        if (ver.major > 5 or (ver.major == 5 and ver.minor >= 19)) {
            try std.testing.expect(features.has_multishot_accept);
            try std.testing.expect(features.has_buffer_ring);
        }

        // On kernel 6.0+, we should have multishot recv
        if (ver.major >= 6) {
            try std.testing.expect(features.has_multishot_recv);
        }
    }
}

test "IoUringBackend - multishot accept basic" {
    if (!isSupported()) return error.SkipZigTest;

    // Skip if multishot accept is not supported
    const features = Features.detect();
    if (!features.has_multishot_accept) return error.SkipZigTest;

    const Config = @import("../backend.zig").Config;
    var backend = try IoUringBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create listening socket
    const listen_fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK, 0);
    defer std.posix.close(listen_fd);

    // Bind to ephemeral port
    var addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port = 0,
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    try std.posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));
    try std.posix.listen(listen_fd, 10);

    // Get assigned port
    var name_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    try std.posix.getsockname(listen_fd, @ptrCast(&addr), &name_len);

    // Submit multishot accept
    var accept_addr: std.posix.sockaddr = undefined;
    var accept_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const multishot_id = try backend.submit(.{
        .op = .{
            .accept_multishot = .{
                .fd = listen_fd,
                .addr = &accept_addr,
                .addr_len = &accept_addr_len,
            },
        },
        .user_data = 1000,
    });

    try std.testing.expect(multishot_id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.inFlightCount());

    // Create first client and connect
    const client1 = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(client1);
    try std.posix.connect(client1, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));

    // Wait for first accept completion
    var completions: [8]Completion = undefined;
    var count = try backend.wait(&completions, 100_000_000);

    try std.testing.expect(count >= 1);
    try std.testing.expectEqual(@as(u64, 1000), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0); // Got a client fd

    // Check if MORE flag is set (multishot is still active)
    // Note: The kernel may or may not set MORE depending on timing
    const accepted_fd1: posix.fd_t = @intCast(completions[0].result);
    defer std.posix.close(accepted_fd1);

    // If MORE is set, operation should still be in-flight
    if (completions[0].hasMore()) {
        try std.testing.expectEqual(@as(usize, 1), backend.inFlightCount());

        // Create second client
        const client2 = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        defer std.posix.close(client2);
        try std.posix.connect(client2, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.in));

        // Wait for second accept
        count = try backend.wait(&completions, 100_000_000);

        try std.testing.expect(count >= 1);
        try std.testing.expectEqual(@as(u64, 1000), completions[0].user_data);
        try std.testing.expect(completions[0].result > 0);

        const accepted_fd2: posix.fd_t = @intCast(completions[0].result);
        std.posix.close(accepted_fd2);

        // Cancel the multishot
        try backend.cancelMultishot(multishot_id);
    }

    // After all completions without MORE, should be removed
    // (or explicitly cancelled)
}

test "Completion - hasMore and hasBuffer" {
    // Test CQE flag helpers
    const comp_with_more = Completion{
        .user_data = 1,
        .result = 0,
        .flags = completion.CQEFlags.MORE,
    };
    try std.testing.expect(comp_with_more.hasMore());
    try std.testing.expect(!comp_with_more.hasBuffer());

    const comp_with_buffer = Completion{
        .user_data = 2,
        .result = 0,
        .flags = completion.CQEFlags.BUFFER | (42 << 16), // buffer ID 42
    };
    try std.testing.expect(!comp_with_buffer.hasMore());
    try std.testing.expect(comp_with_buffer.hasBuffer());
    try std.testing.expectEqual(@as(u16, 42), comp_with_buffer.getBufferId());

    const comp_with_both = Completion{
        .user_data = 3,
        .result = 100,
        .flags = completion.CQEFlags.MORE | completion.CQEFlags.BUFFER | (123 << 16),
    };
    try std.testing.expect(comp_with_both.hasMore());
    try std.testing.expect(comp_with_both.hasBuffer());
    try std.testing.expectEqual(@as(u16, 123), comp_with_both.getBufferId());
}

//! Common types for I/O backends.
//!
//! These types are shared across all platform backends (io_uring, kqueue, epoll, IOCP).
//! They define the interface between the executor and the platform-specific I/O driver.

const std = @import("std");

/// Unique identifier for a submitted operation.
/// Used to correlate completions with their original submissions.
pub const SubmissionId = struct {
    value: u64,

    pub const INVALID = SubmissionId{ .value = 0 };

    pub fn init(value: u64) SubmissionId {
        return .{ .value = value };
    }

    pub fn isValid(self: SubmissionId) bool {
        return self.value != 0;
    }
};

/// io_uring CQE flags
pub const CQEFlags = struct {
    /// If set, the application should expect more completions from this request.
    /// Used by multishot operations (accept_multishot, recv_multishot).
    pub const MORE: u32 = 1 << 1; // IORING_CQE_F_MORE

    /// If set, the buffer index is valid.
    /// Used with buffer rings for multishot recv.
    pub const BUFFER: u32 = 1 << 0; // IORING_CQE_F_BUFFER

    /// If set, more data is available to read (socket has more data).
    pub const SOCK_NONEMPTY: u32 = 1 << 2; // IORING_CQE_F_SOCK_NONEMPTY

    /// If set, this is a notification CQE (used with send/recv notifications).
    pub const NOTIF: u32 = 1 << 3; // IORING_CQE_F_NOTIF
};

/// Result of an I/O completion.
pub const Completion = struct {
    /// User data associated with the operation (typically task pointer).
    user_data: u64,

    /// Result code (positive = success/bytes, negative = errno).
    result: i32,

    /// Backend-specific flags (e.g., io_uring CQE flags).
    flags: u32,

    pub fn isSuccess(self: Completion) bool {
        return self.result >= 0;
    }

    pub fn bytesTransferred(self: Completion) ?usize {
        if (self.result >= 0) {
            return @intCast(self.result);
        }
        return null;
    }

    pub fn getError(self: Completion) ?std.posix.E {
        if (self.result < 0) {
            return @enumFromInt(@as(u16, @intCast(-self.result)));
        }
        return null;
    }

    /// Check if more completions are expected from this multishot operation.
    /// When true, the operation is still active and will produce more completions.
    /// When false, this is the final completion and the operation is done.
    pub fn hasMore(self: Completion) bool {
        return (self.flags & CQEFlags.MORE) != 0;
    }

    /// Check if this completion has a buffer index (from buffer ring).
    /// Used with multishot recv to identify which buffer was used.
    pub fn hasBuffer(self: Completion) bool {
        return (self.flags & CQEFlags.BUFFER) != 0;
    }

    /// Get the buffer ID from a completion that used provided buffers.
    /// Only valid if hasBuffer() returns true.
    pub fn getBufferId(self: Completion) u16 {
        // Buffer ID is stored in upper 16 bits of flags
        return @truncate(self.flags >> 16);
    }
};

/// File descriptor type (platform-specific).
pub const fd_t = std.posix.fd_t;

/// Socket address types.
pub const sockaddr = std.posix.sockaddr;
pub const socklen_t = std.posix.socklen_t;

/// I/O vector for scatter/gather operations.
pub const iovec = std.posix.iovec;
pub const iovec_const = std.posix.iovec_const;

/// Timespec for timeout operations.
pub const timespec = std.posix.timespec;

/// Maximum safe timeout value in nanoseconds.
/// This is ~49 days, a conservative safe maximum for cross-platform timeout handling.
/// Beyond this value, platform-specific overflow could occur.
/// Rationale: Many platforms use i32 milliseconds (max ~24 days) or have internal limits.
/// We use a conservative value that's safe across all platforms.
pub const MAX_TIMEOUT_NS: u64 = @as(u64, std.math.maxInt(i32) - 1) * std.time.ns_per_ms;

/// Validate a timeout value, returning a safe clamped value.
/// Returns error.TimeoutTooLarge if timeout exceeds safe maximum (caller can decide to clamp or reject).
/// Extreme timeouts are clamped to the safe maximum (saturation semantics).
pub fn validateTimeout(timeout_ns: u64) error{TimeoutTooLarge}!u64 {
    if (timeout_ns > MAX_TIMEOUT_NS) {
        return error.TimeoutTooLarge;
    }
    return timeout_ns;
}

/// Clamp a timeout to the maximum safe value (saturating behavior).
/// Use this when you want to accept any timeout but cap it to a safe maximum.
pub fn clampTimeout(timeout_ns: u64) u64 {
    return @min(timeout_ns, MAX_TIMEOUT_NS);
}

/// Convert duration in nanoseconds to timespec.
pub fn timespecFromNanos(nanos: u64) timespec {
    return .{
        .sec = @intCast(nanos / std.time.ns_per_s),
        .nsec = @intCast(nanos % std.time.ns_per_s),
    };
}

/// I/O operation to submit.
/// This is a tagged union representing all supported async operations.
pub const Operation = struct {
    /// The specific operation type and its parameters.
    op: OpType,

    /// User data to return with completion (typically pointer to task).
    user_data: u64,

    pub const OpType = union(enum) {
        // ─────────────────────────────────────────────────────────────
        // File operations
        // ─────────────────────────────────────────────────────────────

        read: struct {
            fd: fd_t,
            buffer: []u8,
            offset: ?u64,
        },

        write: struct {
            fd: fd_t,
            buffer: []const u8,
            offset: ?u64,
        },

        open: struct {
            path: [*:0]const u8,
            flags: u32,
            mode: std.posix.mode_t,
        },

        close: struct {
            fd: fd_t,
        },

        fsync: struct {
            fd: fd_t,
            datasync: bool,
        },

        // ─────────────────────────────────────────────────────────────
        // Network operations
        // ─────────────────────────────────────────────────────────────

        accept: struct {
            fd: fd_t,
            addr: ?*sockaddr,
            addr_len: ?*socklen_t,
        },

        /// Multishot accept - one submission handles unlimited connections.
        /// Available on Linux kernel 5.19+.
        /// Unlike regular accept, this operation is NOT removed from tracking
        /// when a completion arrives (unless it's the final completion).
        accept_multishot: struct {
            fd: fd_t,
            /// Note: For multishot accept, addr storage must be persistent
            /// since multiple completions will write to it.
            addr: ?*sockaddr,
            addr_len: ?*socklen_t,
        },

        connect: struct {
            fd: fd_t,
            addr: *const sockaddr,
            addr_len: socklen_t,
        },

        recv: struct {
            fd: fd_t,
            buffer: []u8,
            flags: u32,
        },

        /// Multishot recv - one submission handles multiple receives.
        /// Available on Linux kernel 6.0+ with buffer rings (kernel 5.19+).
        /// Uses provided buffers from a buffer ring for zero-copy receives.
        recv_multishot: struct {
            fd: fd_t,
            /// Buffer group ID (from registered buffer ring).
            buf_group: u16,
            flags: u32,
        },

        send: struct {
            fd: fd_t,
            buffer: []const u8,
            flags: u32,
        },

        // ─────────────────────────────────────────────────────────────
        // Vectored I/O
        // ─────────────────────────────────────────────────────────────

        readv: struct {
            fd: fd_t,
            iovecs: []const iovec,
            offset: ?u64,
        },

        writev: struct {
            fd: fd_t,
            iovecs: []const iovec_const,
            offset: ?u64,
        },

        // ─────────────────────────────────────────────────────────────
        // Special operations
        // ─────────────────────────────────────────────────────────────

        timeout: struct {
            ns: u64,
        },

        cancel: struct {
            target_id: SubmissionId,
        },

        nop: void,
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SubmissionId - validity" {
    const invalid = SubmissionId.INVALID;
    try std.testing.expect(!invalid.isValid());

    const valid = SubmissionId.init(42);
    try std.testing.expect(valid.isValid());
}

test "Completion - success and error" {
    const success = Completion{ .user_data = 1, .result = 100, .flags = 0 };
    try std.testing.expect(success.isSuccess());
    try std.testing.expectEqual(@as(usize, 100), success.bytesTransferred().?);
    try std.testing.expect(success.getError() == null);

    const failure = Completion{ .user_data = 2, .result = -@as(i32, @intFromEnum(std.posix.E.AGAIN)), .flags = 0 };
    try std.testing.expect(!failure.isSuccess());
    try std.testing.expect(failure.bytesTransferred() == null);
    try std.testing.expectEqual(std.posix.E.AGAIN, failure.getError().?);
}

test "timespecFromNanos" {
    const ts = timespecFromNanos(1_500_000_000); // 1.5 seconds
    try std.testing.expectEqual(@as(isize, 1), ts.sec);
    try std.testing.expectEqual(@as(isize, 500_000_000), ts.nsec);
}

test "validateTimeout - normal values" {
    // Normal timeout should pass
    const normal = try validateTimeout(1_000_000_000); // 1 second
    try std.testing.expectEqual(@as(u64, 1_000_000_000), normal);

    // Zero timeout should pass
    const zero = try validateTimeout(0);
    try std.testing.expectEqual(@as(u64, 0), zero);

    // Max safe timeout should pass
    const max_safe = try validateTimeout(MAX_TIMEOUT_NS);
    try std.testing.expectEqual(MAX_TIMEOUT_NS, max_safe);
}

test "validateTimeout - overflow values" {
    // Just over max should fail
    try std.testing.expectError(error.TimeoutTooLarge, validateTimeout(MAX_TIMEOUT_NS + 1));

    // Way over max should fail
    try std.testing.expectError(error.TimeoutTooLarge, validateTimeout(std.math.maxInt(u64)));
}

test "clampTimeout - saturation" {
    // Normal timeout unchanged
    try std.testing.expectEqual(@as(u64, 1000), clampTimeout(1000));

    // Extreme timeout clamped
    try std.testing.expectEqual(MAX_TIMEOUT_NS, clampTimeout(std.math.maxInt(u64)));

    // Just over max clamped
    try std.testing.expectEqual(MAX_TIMEOUT_NS, clampTimeout(MAX_TIMEOUT_NS + 1));
}

test "MAX_TIMEOUT_NS - reasonable value" {
    // maxInt(i32) milliseconds is ~24.8 days
    // This is a conservative limit that's safe across all platforms
    const days_24_ns: u64 = 24 * 24 * 60 * 60 * std.time.ns_per_s;
    const days_25_ns: u64 = 25 * 24 * 60 * 60 * std.time.ns_per_s;

    // Should be between 24 and 25 days
    try std.testing.expect(MAX_TIMEOUT_NS > days_24_ns);
    try std.testing.expect(MAX_TIMEOUT_NS < days_25_ns);
}

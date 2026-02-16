//! Platform I/O Backend Interface
//!
//! This module provides a unified interface across platform-specific I/O backends:
//! - Linux: io_uring (primary), epoll (fallback)
//! - macOS/BSD: kqueue
//! - Windows: IOCP
//!
//! The backend auto-detects the best available option at initialization.

const std = @import("std");
const builtin = @import("builtin");

// Re-export common types
pub const completion = @import("backend/types.zig");
pub const Operation = completion.Operation;
pub const Completion = completion.Completion;
pub const SubmissionId = completion.SubmissionId;
pub const timespecFromNanos = completion.timespecFromNanos;

// Platform-specific backends
pub const io_uring = if (builtin.os.tag == .linux) @import("backend/io_uring.zig") else struct {};
pub const kqueue = if (builtin.os.tag == .macos or builtin.os.tag == .freebsd or builtin.os.tag == .openbsd or builtin.os.tag == .netbsd) @import("backend/kqueue.zig") else struct {};
pub const epoll = if (builtin.os.tag == .linux) @import("backend/epoll.zig") else struct {};
pub const iocp = @import("backend/iocp.zig");
pub const poll = @import("backend/poll.zig");

/// Backend selection strategy.
pub const BackendType = enum {
    /// Automatically select best backend for platform.
    auto,
    /// Linux io_uring (kernel 5.1+).
    io_uring,
    /// Linux epoll (fallback).
    epoll,
    /// macOS/BSD kqueue.
    kqueue,
    /// Windows IOCP.
    iocp,
    /// POSIX poll (universal fallback).
    poll,
};

/// Configuration limits to prevent resource exhaustion.
pub const ConfigLimits = struct {
    /// Minimum completions (must have at least some buffer)
    pub const MIN_COMPLETIONS: u32 = 8;
    /// Maximum completions (1M is extremely generous)
    pub const MAX_COMPLETIONS: u32 = 1024 * 1024;
    /// Maximum ring entries
    pub const MAX_RING_ENTRIES: u16 = 32768;
};

/// Backend configuration.
pub const Config = struct {
    /// Which backend to use.
    backend_type: BackendType = .auto,

    /// io_uring: submission queue size (power of 2, 0 = auto).
    ring_entries: u16 = 0,

    /// io_uring: enable SQPOLL (kernel-side submission polling).
    sqpoll: bool = false,

    /// io_uring: enable IOPOLL (busy-wait for completions).
    iopoll: bool = false,

    /// io_uring: single issuer optimization.
    single_issuer: bool = true,

    /// Maximum completions to process per poll.
    max_completions: u32 = 256,

    /// Default optimal ring size based on system resources.
    pub fn defaultRingEntries() u16 {
        // Conservative default that works on most systems
        return 256;
    }

    /// Validate configuration and clamp values to safe ranges.
    /// Returns a validated config (does not modify in place).
    pub fn validated(self: Config) Config {
        var result = self;

        // Clamp max_completions to safe range
        result.max_completions = @max(
            ConfigLimits.MIN_COMPLETIONS,
            @min(self.max_completions, ConfigLimits.MAX_COMPLETIONS),
        );

        // Clamp ring_entries (0 means auto, which is fine)
        if (self.ring_entries > ConfigLimits.MAX_RING_ENTRIES) {
            result.ring_entries = ConfigLimits.MAX_RING_ENTRIES;
        }

        return result;
    }
};

/// Unified backend handle.
/// Tagged union of platform-specific backends.
pub const Backend = union(BackendType) {
    auto: void, // Never actually used - resolved at init
    io_uring: if (builtin.os.tag == .linux) io_uring.IoUringBackend else void,
    epoll: if (builtin.os.tag == .linux) epoll.EpollBackend else void,
    kqueue: if (builtin.os.tag == .macos or builtin.os.tag == .freebsd) kqueue.KqueueBackend else void,
    iocp: if (builtin.os.tag == .windows) iocp.IocpBackend else void,
    poll: poll.PollBackend, // Universal POSIX fallback

    /// Initialize the backend with given configuration.
    /// Configuration values are validated and clamped to safe ranges.
    pub fn init(allocator: std.mem.Allocator, config: Config) !Backend {
        // Validate configuration to prevent resource exhaustion
        const safe_config = config.validated();

        const backend_type = if (safe_config.backend_type == .auto) detectBestBackend() else safe_config.backend_type;

        return switch (backend_type) {
            .auto => unreachable, // Already resolved
            .io_uring => {
                if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
                return Backend{ .io_uring = try io_uring.IoUringBackend.init(allocator, safe_config) };
            },
            .epoll => {
                if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
                return Backend{ .epoll = try epoll.EpollBackend.init(allocator, safe_config) };
            },
            .kqueue => {
                if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) return error.UnsupportedPlatform;
                return Backend{ .kqueue = try kqueue.KqueueBackend.init(allocator, safe_config) };
            },
            .iocp => {
                if (builtin.os.tag != .windows) return error.UnsupportedPlatform;
                return Backend{ .iocp = try iocp.IocpBackend.init(allocator, safe_config) };
            },
            .poll => {
                return Backend{ .poll = try poll.PollBackend.init(allocator, safe_config) };
            },
        };
    }

    /// Clean up backend resources.
    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            inline .io_uring, .epoll, .kqueue, .iocp, .poll => |*b| {
                if (@TypeOf(b.*) != void) b.deinit();
            },
            .auto => {},
        }
    }

    /// Submit an I/O operation.
    /// Returns a submission ID for tracking/cancellation.
    pub fn submit(self: *Backend, op: Operation) !SubmissionId {
        switch (self.*) {
            inline .io_uring, .epoll, .kqueue, .iocp, .poll => |*b| {
                if (@TypeOf(b.*) != void) return b.submit(op);
            },
            .auto => {},
        }
        return error.NotImplemented;
    }

    /// Wait for completions (blocking).
    /// Returns number of completions written to the buffer.
    pub fn wait(self: *Backend, completions: []Completion, timeout_ns: ?u64) !usize {
        switch (self.*) {
            inline .io_uring, .epoll, .kqueue, .iocp, .poll => |*b| {
                if (@TypeOf(b.*) != void) return b.wait(completions, timeout_ns);
            },
            .auto => {},
        }
        return error.NotImplemented;
    }

    /// Poll for completions (non-blocking).
    /// Returns number of completions written to the buffer.
    pub fn pollCompletions(self: *Backend, completions: []Completion) !usize {
        return self.wait(completions, 0);
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Backend, id: SubmissionId) !void {
        switch (self.*) {
            inline .io_uring, .epoll, .kqueue, .iocp, .poll => |*b| {
                if (@TypeOf(b.*) != void) return b.cancel(id);
            },
            .auto => {},
        }
        return error.NotImplemented;
    }

    /// Flush pending submissions to the kernel.
    /// Some backends (io_uring) batch submissions for efficiency.
    pub fn flush(self: *Backend) !u32 {
        switch (self.*) {
            inline .io_uring => |*b| {
                if (@TypeOf(b.*) != void) return b.flush();
            },
            inline .epoll, .kqueue, .iocp, .poll => |*b| {
                if (@TypeOf(b.*) != void) return 0; // No batching
            },
            .auto => {},
        }
        return error.NotImplemented;
    }

    /// Get the underlying file descriptor for integration with other event loops.
    /// Returns null if not applicable (e.g., Windows IOCP uses HANDLEs, poll has no fd).
    pub fn fd(self: Backend) ?std.posix.fd_t {
        switch (self) {
            inline .io_uring, .epoll, .kqueue => |b| {
                if (@TypeOf(b) != void) return b.fd();
            },
            inline .iocp, .poll => |b| {
                // IOCP uses HANDLEs, poll has no central fd
                if (@TypeOf(b) != void) return b.fd();
            },
            .auto => {},
        }
        return null;
    }

    /// Get the active backend type.
    pub fn backendType(self: Backend) BackendType {
        return self;
    }
};

/// Detect the best available backend for the current platform.
pub fn detectBestBackend() BackendType {
    switch (builtin.os.tag) {
        .linux => {
            // Try io_uring first, fall back to epoll
            if (io_uring.isSupported()) {
                return .io_uring;
            }
            return .epoll;
        },
        .macos, .freebsd, .openbsd, .netbsd => return .kqueue,
        .windows => return .iocp,
        else => return .poll,
    }
}

/// Check if io_uring is supported on this system.
pub fn isIoUringSupported() bool {
    if (builtin.os.tag != .linux) return false;
    return io_uring.isSupported();
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Backend - detect best" {
    const best = detectBestBackend();
    switch (builtin.os.tag) {
        .linux => try std.testing.expect(best == .io_uring or best == .epoll),
        .macos => try std.testing.expectEqual(BackendType.kqueue, best),
        else => {},
    }
}

test "Backend - init and deinit" {
    var backend = Backend.init(std.testing.allocator, .{}) catch |err| {
        // Skip test if backend not supported
        if (err == error.UnsupportedPlatform or err == error.NotImplemented) return;
        return err;
    };
    defer backend.deinit();

    try std.testing.expect(backend.fd() != null or backend.backendType() == .poll);
}

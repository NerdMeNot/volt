//! Windows I/O Completion Ports (IOCP) Backend
//!
//! High-performance completion-based I/O for Windows.
//! IOCP is Windows' equivalent to Linux's io_uring - both are completion-based
//! rather than readiness-based (like epoll/kqueue).
//!
//! Architecture:
//! - Slab allocator for O(1) operation tracking
//! - OVERLAPPED structures for async I/O state
//! - GetQueuedCompletionStatusEx for batched completions
//! - Lifecycle tracking: Submitted -> Completed/Cancelled
//!
//! Key concepts:
//! - IOCP is a kernel object that queues I/O completions
//! - File handles must be associated with the IOCP before async I/O
//! - Each async operation needs an OVERLAPPED structure
//! - Completions can be dequeued in batches for efficiency
//!
//! Based on Windows I/O Completion Ports documentation and production IOCP patterns.

const std = @import("std");
const builtin = @import("builtin");

const completion = @import("types.zig");
const Operation = completion.Operation;
const Completion = completion.Completion;
const SubmissionId = completion.SubmissionId;

const slab = @import("../util/slab.zig");

// Only compile on Windows
const windows = if (builtin.os.tag == .windows) std.os.windows else struct {
    // Stub types for non-Windows compilation
    pub const HANDLE = *anyopaque;
    pub const INVALID_HANDLE_VALUE = @as(HANDLE, @ptrFromInt(std.math.maxInt(usize)));
    pub const OVERLAPPED = extern struct {
        Internal: usize = 0,
        InternalHigh: usize = 0,
        DUMMYUNIONNAME: extern union {
            DUMMYSTRUCTNAME: extern struct {
                Offset: u32,
                OffsetHigh: u32,
            },
            Pointer: ?*anyopaque,
        } = .{ .DUMMYSTRUCTNAME = .{ .Offset = 0, .OffsetHigh = 0 } },
        hEvent: ?HANDLE = null,
    };
    pub const OVERLAPPED_ENTRY = extern struct {
        lpCompletionKey: usize,
        lpOverlapped: ?*OVERLAPPED,
        Internal: usize,
        dwNumberOfBytesTransferred: u32,
    };
    pub const DWORD = u32;
    pub const ULONG = u32;
    pub const ULONG_PTR = usize;
    pub const BOOL = i32;
    pub const TRUE: BOOL = 1;
    pub const FALSE: BOOL = 0;
    pub const INFINITE: DWORD = 0xFFFFFFFF;
    pub const kernel32 = struct {
        pub fn CreateIoCompletionPort(_: HANDLE, _: ?HANDLE, _: ULONG_PTR, _: DWORD) callconv(.winapi) ?HANDLE {
            return null;
        }
        pub fn GetQueuedCompletionStatusEx(_: HANDLE, _: [*]OVERLAPPED_ENTRY, _: ULONG, _: *ULONG, _: DWORD, _: BOOL) callconv(.winapi) BOOL {
            return FALSE;
        }
        pub fn PostQueuedCompletionStatus(_: HANDLE, _: DWORD, _: ULONG_PTR, _: ?*OVERLAPPED) callconv(.winapi) BOOL {
            return FALSE;
        }
        pub fn CloseHandle(_: HANDLE) callconv(.winapi) BOOL {
            return FALSE;
        }
        pub fn CancelIoEx(_: HANDLE, _: ?*OVERLAPPED) callconv(.winapi) BOOL {
            return FALSE;
        }
        pub fn GetLastError() callconv(.winapi) DWORD {
            return 0;
        }
    };
    pub const Win32Error = enum(u32) {
        SUCCESS = 0,
        IO_PENDING = 997,
        OPERATION_ABORTED = 995,
        INVALID_HANDLE = 6,
        NOT_ENOUGH_MEMORY = 8,
        _,
    };
};

/// Default batch size for completion dequeuing.
const DEFAULT_BATCH_SIZE: u32 = 64;

/// Default maximum concurrent operations.
const DEFAULT_MAX_OPERATIONS: usize = 4096;

/// Absolute maximum to prevent runaway memory usage.
const ABSOLUTE_MAX_OPERATIONS: usize = 1024 * 1024;

/// Maximum number of timers to prevent unbounded memory growth.
const MAX_TIMERS: usize = 65536;

/// Operation lifecycle state.
const Lifecycle = enum {
    /// Operation submitted, waiting for completion.
    submitted,
    /// Operation completed.
    completed,
    /// Operation was cancelled.
    cancelled,
};

/// Tracked operation in the slab.
/// Contains the OVERLAPPED structure and operation metadata.
const TrackedOp = struct {
    /// OVERLAPPED structure for Windows async I/O.
    /// Must be first field for pointer casting.
    overlapped: windows.OVERLAPPED,

    /// Original operation parameters.
    op: Operation,

    /// Submission ID for user tracking.
    submission_id: SubmissionId,

    /// Lifecycle state.
    state: Lifecycle,

    /// File handle associated with this operation.
    handle: windows.HANDLE,

    /// Buffer for read operations (to maintain lifetime).
    buffer_ptr: ?[*]u8,
    buffer_len: usize,

    pub fn init(op: Operation, submission_id: SubmissionId, handle: windows.HANDLE) TrackedOp {
        var overlapped = std.mem.zeroes(windows.OVERLAPPED);

        // Set offset for file operations
        switch (op.op) {
            .read => |r| {
                if (r.offset) |off| {
                    overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.Offset = @truncate(off);
                    overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.OffsetHigh = @truncate(off >> 32);
                }
            },
            .write => |w| {
                if (w.offset) |off| {
                    overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.Offset = @truncate(off);
                    overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.OffsetHigh = @truncate(off >> 32);
                }
            },
            else => {},
        }

        return .{
            .overlapped = overlapped,
            .op = op,
            .submission_id = submission_id,
            .state = .submitted,
            .handle = handle,
            .buffer_ptr = null,
            .buffer_len = 0,
        };
    }
};

/// Timer entry for timeout tracking.
const Timer = struct {
    id: u64,
    /// Deadline as nanoseconds since start_instant (monotonic).
    deadline_ns: u64,
    user_data: u64,
};

/// Windows IOCP Backend.
pub const IocpBackend = struct {
    /// The I/O completion port handle.
    iocp: windows.HANDLE,

    /// Memory allocator.
    allocator: std.mem.Allocator,

    /// Slab of tracked operations.
    operations: slab.Slab(TrackedOp),

    /// Buffer for dequeuing completions.
    completion_buffer: []windows.OVERLAPPED_ENTRY,

    /// Batch size for completions.
    batch_size: u32,

    /// Maximum allowed concurrent operations.
    max_operations: usize,

    /// Set of handles already associated with IOCP.
    /// IOCP requires each handle to be associated exactly once.
    associated_handles: std.AutoHashMap(usize, void),

    /// Pending timer expirations.
    timers: std.ArrayList(Timer),

    /// Next timer ID.
    next_timer_id: u64,

    /// Start time for monotonic timer calculations.
    start_instant: std.time.Instant,

    const Self = @This();

    /// Initialize the IOCP backend.
    pub fn init(allocator: std.mem.Allocator, config: anytype) !Self {
        if (builtin.os.tag != .windows) {
            return error.UnsupportedPlatform;
        }

        // Create the I/O completion port
        // NumberOfConcurrentThreads = 0 means use number of processors
        const iocp = windows.kernel32.CreateIoCompletionPort(
            windows.INVALID_HANDLE_VALUE,
            null,
            0,
            0,
        ) orelse {
            return error.SystemResources;
        };
        errdefer _ = windows.kernel32.CloseHandle(iocp);

        const batch_size = if (@hasField(@TypeOf(config), "max_completions"))
            config.max_completions
        else
            DEFAULT_BATCH_SIZE;

        // Get max operations from config, clamped to absolute max
        const max_ops = if (@hasField(@TypeOf(config), "max_operations"))
            @min(config.max_operations, ABSOLUTE_MAX_OPERATIONS)
        else
            DEFAULT_MAX_OPERATIONS;

        // Allocate completion buffer
        const completion_buffer = try allocator.alloc(windows.OVERLAPPED_ENTRY, batch_size);
        errdefer allocator.free(completion_buffer);

        // Initialize operations slab with bounded capacity
        var operations = try slab.Slab(TrackedOp).init(allocator, max_ops);
        errdefer operations.deinit();

        return Self{
            .iocp = iocp,
            .allocator = allocator,
            .operations = operations,
            .completion_buffer = completion_buffer,
            .batch_size = batch_size,
            .max_operations = max_ops,
            .associated_handles = std.AutoHashMap(usize, void).init(allocator),
            .timers = std.ArrayList(Timer).empty,
            .next_timer_id = 1,
            .start_instant = std.time.Instant.now() catch std.time.Instant{
                .timestamp = .{ .sec = 0, .nsec = 0 },
            },
        };
    }

    /// Get current monotonic time as nanoseconds since start_instant.
    fn monotonicNow(self: *Self) u64 {
        const now = std.time.Instant.now() catch return 0;
        return now.since(self.start_instant);
    }

    /// Clean up resources.
    pub fn deinit(self: *Self) void {
        // Cancel all pending operations
        var it = self.operations.iterator();
        while (it.next()) |entry| {
            if (entry.value.state == .submitted) {
                _ = windows.kernel32.CancelIoEx(entry.value.handle, &entry.value.overlapped);
            }
        }

        self.operations.deinit();
        self.timers.deinit(self.allocator);
        self.allocator.free(self.completion_buffer);
        self.associated_handles.deinit();
        _ = windows.kernel32.CloseHandle(self.iocp);
    }

    /// Associate a handle with the IOCP.
    /// Must be called before async I/O on that handle.
    pub fn associateHandle(self: *Self, handle: windows.HANDLE, completion_key: usize) !void {
        const handle_val = @intFromPtr(handle);

        // Check if already associated
        if (self.associated_handles.contains(handle_val)) {
            return; // Already associated
        }

        // Associate with IOCP
        const result = windows.kernel32.CreateIoCompletionPort(
            handle,
            self.iocp,
            completion_key,
            0,
        );

        if (result == null) {
            const err = windows.kernel32.GetLastError();
            return mapWindowsError(err);
        }

        try self.associated_handles.put(handle_val, {});
    }

    /// Submit an I/O operation.
    pub fn submit(self: *Self, op: Operation) !SubmissionId {
        const user_data = op.user_data;
        const submission_id = SubmissionId.init(user_data);

        // Handle timeout operations separately - they don't need IOCP or OVERLAPPED
        if (op.op == .timeout) {
            const t = op.op.timeout;
            if (!self.registerTimeout(t.ns, user_data)) {
                return error.SystemResources;
            }
            return submission_id;
        }

        // Get handle from operation
        const handle = getHandleFromOp(op) orelse {
            return error.InvalidArgument;
        };

        // Ensure handle is associated with IOCP
        try self.associateHandle(handle, user_data);

        // Track the operation
        const slab_key = try self.operations.insert(TrackedOp.init(op, submission_id, handle));
        errdefer _ = self.operations.remove(slab_key);

        // Get pointer to tracked op for OVERLAPPED
        const tracked = self.operations.get(slab_key) orelse return error.InvalidArgument;

        // Issue the async I/O
        const success = switch (op.op) {
            .read => |r| self.issueRead(handle, r.buffer, &tracked.overlapped),
            .write => |w| self.issueWrite(handle, w.buffer, &tracked.overlapped),
            .nop => blk: {
                // Use PostQueuedCompletionStatus for NOP
                break :blk windows.kernel32.PostQueuedCompletionStatus(
                    self.iocp,
                    0,
                    user_data,
                    &tracked.overlapped,
                ) == windows.TRUE;
            },
            .timeout => unreachable, // Handled above
            else => {
                _ = self.operations.remove(slab_key);
                return error.OperationNotSupported;
            },
        };

        if (!success) {
            const err = windows.kernel32.GetLastError();
            // IO_PENDING is expected for async operations
            if (@as(windows.Win32Error, @enumFromInt(err)) != .IO_PENDING) {
                _ = self.operations.remove(slab_key);
                return mapWindowsError(err);
            }
        }

        return submission_id;
    }

    /// Issue an async read.
    fn issueRead(self: *Self, handle: windows.HANDLE, buffer: []u8, overlapped: *windows.OVERLAPPED) bool {
        _ = self;
        if (builtin.os.tag != .windows) return false;

        // Use ReadFile for async read
        var bytes_read: windows.DWORD = 0;
        const result = std.os.windows.kernel32.ReadFile(
            handle,
            buffer.ptr,
            @intCast(buffer.len),
            &bytes_read,
            overlapped,
        );
        return result == windows.TRUE;
    }

    /// Issue an async write.
    fn issueWrite(self: *Self, handle: windows.HANDLE, buffer: []const u8, overlapped: *windows.OVERLAPPED) bool {
        _ = self;
        if (builtin.os.tag != .windows) return false;

        var bytes_written: windows.DWORD = 0;
        const result = std.os.windows.kernel32.WriteFile(
            handle,
            buffer.ptr,
            @intCast(buffer.len),
            &bytes_written,
            overlapped,
        );
        return result == windows.TRUE;
    }

    /// Register a timeout in the timer list.
    /// Returns true if timer was registered, false if limit reached.
    fn registerTimeout(self: *Self, ns: u64, user_data: u64) bool {
        // Enforce timer limit to prevent unbounded memory growth
        if (self.timers.items.len >= MAX_TIMERS) {
            return false;
        }

        const now = self.monotonicNow();
        // Use saturating add to prevent overflow
        const deadline = now +| ns;

        self.timers.append(self.allocator, Timer{
            .id = self.next_timer_id,
            .deadline_ns = deadline,
            .user_data = user_data,
        }) catch return false;

        self.next_timer_id += 1;
        return true;
    }

    /// Wait for completions.
    pub fn wait(self: *Self, completions: []Completion, timeout_ns: ?u64) !usize {
        // First check for expired timers
        var count = self.completeTimers(completions);
        if (count > 0) return count;

        // Calculate effective timeout considering pending timers
        const timeout_ms = self.calculateTimeout(timeout_ns);

        const max_entries: windows.ULONG = @intCast(@min(completions.len, self.completion_buffer.len));

        var entries_removed: windows.ULONG = 0;

        // GetQueuedCompletionStatusEx for batched dequeue
        const result = windows.kernel32.GetQueuedCompletionStatusEx(
            self.iocp,
            self.completion_buffer.ptr,
            max_entries,
            &entries_removed,
            timeout_ms,
            windows.FALSE, // Not alertable
        );

        if (result == windows.FALSE) {
            const err = windows.kernel32.GetLastError();

            // WAIT_TIMEOUT is not an error - check timers again
            if (err == 258) { // WAIT_TIMEOUT
                return self.completeTimers(completions);
            }

            return mapWindowsError(err);
        }

        // Process completions
        for (self.completion_buffer[0..entries_removed]) |entry| {
            if (count >= completions.len) break;

            // Find the tracked operation from the OVERLAPPED pointer
            const bytes_transferred = entry.dwNumberOfBytesTransferred;
            const user_data = entry.lpCompletionKey;

            // Check if operation was successful
            // Internal contains the NTSTATUS
            const status = entry.Internal;
            const is_success = (status == 0); // STATUS_SUCCESS

            completions[count] = Completion{
                .user_data = user_data,
                .result = if (is_success) @intCast(bytes_transferred) else mapNtStatusToErrno(status),
                .flags = 0,
            };
            count += 1;

            // Clean up tracked operation if we can find it
            self.cleanupCompletedOp(entry.lpOverlapped);
        }

        return count;
    }

    /// Complete expired timers.
    fn completeTimers(self: *Self, completions: []Completion) usize {
        const now = self.monotonicNow();
        var count: usize = 0;
        var i: usize = 0;

        while (i < self.timers.items.len) {
            if (count >= completions.len) break;

            if (self.timers.items[i].deadline_ns <= now) {
                completions[count] = Completion{
                    .user_data = self.timers.items[i].user_data,
                    .result = 0, // Timeout completed successfully
                    .flags = 0,
                };
                count += 1;
                _ = self.timers.swapRemove(i);
            } else {
                i += 1;
            }
        }

        return count;
    }

    /// Calculate IOCP wait timeout considering pending timers.
    fn calculateTimeout(self: *Self, user_timeout_ns: ?u64) windows.DWORD {
        var timeout_ms: windows.DWORD = if (user_timeout_ns) |ns|
            @intCast(@min(ns / std.time.ns_per_ms, std.math.maxInt(windows.DWORD) - 1))
        else
            windows.INFINITE;

        // Adjust for nearest timer deadline
        if (self.timers.items.len > 0) {
            const now = self.monotonicNow();
            var min_deadline: u64 = std.math.maxInt(u64);

            for (self.timers.items) |timer| {
                if (timer.deadline_ns < min_deadline) {
                    min_deadline = timer.deadline_ns;
                }
            }

            // Calculate remaining time (saturating subtraction)
            const remaining_ns = if (min_deadline > now) min_deadline - now else 0;
            const timer_ms: windows.DWORD = @intCast(@min(
                remaining_ns / std.time.ns_per_ms,
                std.math.maxInt(windows.DWORD) - 1,
            ));

            if (timeout_ms == windows.INFINITE or timer_ms < timeout_ms) {
                timeout_ms = timer_ms;
            }
        }

        return timeout_ms;
    }

    /// Clean up a completed operation.
    fn cleanupCompletedOp(self: *Self, overlapped: ?*windows.OVERLAPPED) void {
        if (overlapped == null) return;

        // Find and remove from slab
        // The TrackedOp contains the OVERLAPPED as first field
        var it = self.operations.iterator();
        while (it.next()) |entry| {
            if (&entry.value.overlapped == overlapped) {
                entry.value.state = .completed;
                _ = self.operations.remove(entry.key);
                return;
            }
        }
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Self, id: SubmissionId) !void {
        // Check timers first
        for (self.timers.items, 0..) |timer, i| {
            if (timer.user_data == id.value) {
                _ = self.timers.swapRemove(i);
                return;
            }
        }

        // Find the operation in slab
        var it = self.operations.iterator();
        while (it.next()) |entry| {
            if (entry.value.submission_id.value == id.value and entry.value.state == .submitted) {
                // Cancel the I/O
                const result = windows.kernel32.CancelIoEx(
                    entry.value.handle,
                    &entry.value.overlapped,
                );

                if (result == windows.FALSE) {
                    const err = windows.kernel32.GetLastError();
                    // ERROR_NOT_FOUND means already completed
                    if (err != 1168) { // ERROR_NOT_FOUND
                        return mapWindowsError(err);
                    }
                }

                entry.value.state = .cancelled;
                return;
            }
        }
    }

    /// Flush is a no-op for IOCP (operations are immediately submitted).
    pub fn flush(self: *Self) !u32 {
        _ = self;
        return 0;
    }

    /// Get the IOCP handle for external integration.
    pub fn fd(self: Self) ?std.posix.fd_t {
        // On Windows, HANDLE is not directly compatible with fd_t
        // Return null to indicate this is Windows-specific
        _ = self;
        return null;
    }

    /// Get the IOCP handle.
    pub fn getIocpHandle(self: Self) windows.HANDLE {
        return self.iocp;
    }
};

/// Extract handle from an operation.
fn getHandleFromOp(op: Operation) ?windows.HANDLE {
    return switch (op.op) {
        .read => |r| @ptrFromInt(@as(usize, @intCast(r.fd))),
        .write => |w| @ptrFromInt(@as(usize, @intCast(w.fd))),
        .close => |c| @ptrFromInt(@as(usize, @intCast(c.fd))),
        .accept => |a| @ptrFromInt(@as(usize, @intCast(a.fd))),
        .connect => |c| @ptrFromInt(@as(usize, @intCast(c.fd))),
        .recv => |r| @ptrFromInt(@as(usize, @intCast(r.fd))),
        .send => |s| @ptrFromInt(@as(usize, @intCast(s.fd))),
        .nop, .timeout, .cancel => windows.INVALID_HANDLE_VALUE,
        else => null,
    };
}

/// Map Windows error code to Zig error.
fn mapWindowsError(err: windows.DWORD) error{
    SystemResources,
    InvalidArgument,
    Unexpected,
    OperationNotSupported,
    Cancelled,
} {
    const win_err: windows.Win32Error = @enumFromInt(err);
    return switch (win_err) {
        .INVALID_HANDLE => error.InvalidArgument,
        .NOT_ENOUGH_MEMORY => error.SystemResources,
        .OPERATION_ABORTED => error.Cancelled,
        else => error.Unexpected,
    };
}

/// Map NTSTATUS to errno-style result.
fn mapNtStatusToErrno(status: usize) i32 {
    // Simplified mapping - in production, this would be comprehensive
    if (status == 0) return 0; // STATUS_SUCCESS

    // STATUS_CANCELLED
    if (status == 0xC0000120) return -@as(i32, @intFromEnum(std.posix.E.CANCELED));

    // STATUS_END_OF_FILE
    if (status == 0xC0000011) return 0; // EOF returns 0 bytes

    // Generic error
    return -@as(i32, @intFromEnum(std.posix.E.IO));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "IOCP - unsupported on non-Windows" {
    if (builtin.os.tag == .windows) return;

    const result = IocpBackend.init(std.testing.allocator, .{});
    try std.testing.expectError(error.UnsupportedPlatform, result);
}

test "IOCP - TrackedOp initialization" {
    const op = Operation{
        .op = .{ .nop = {} },
        .user_data = 42,
    };
    const tracked = TrackedOp.init(op, SubmissionId.init(42), windows.INVALID_HANDLE_VALUE);

    try std.testing.expectEqual(Lifecycle.submitted, tracked.state);
    try std.testing.expectEqual(@as(u64, 42), tracked.submission_id.value);
}

test "IOCP - TrackedOp with offset" {
    const offset: u64 = 0x123456789ABCDEF0;
    const op = Operation{
        .op = .{ .read = .{
            .fd = 0,
            .buffer = &[_]u8{},
            .offset = offset,
        } },
        .user_data = 1,
    };
    const tracked = TrackedOp.init(op, SubmissionId.init(1), windows.INVALID_HANDLE_VALUE);

    const low: u32 = @truncate(offset);
    const high: u32 = @truncate(offset >> 32);

    try std.testing.expectEqual(low, tracked.overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.Offset);
    try std.testing.expectEqual(high, tracked.overlapped.DUMMYUNIONNAME.DUMMYSTRUCTNAME.OffsetHigh);
}

test "IOCP - error mapping" {
    // Invalid handle maps to InvalidArgument
    const err1 = mapWindowsError(6); // ERROR_INVALID_HANDLE
    try std.testing.expectEqual(error.InvalidArgument, err1);

    // Operation aborted maps to Cancelled
    const err2 = mapWindowsError(995); // ERROR_OPERATION_ABORTED
    try std.testing.expectEqual(error.Cancelled, err2);

    // Unknown error maps to Unexpected
    const err3 = mapWindowsError(9999);
    try std.testing.expectEqual(error.Unexpected, err3);
}

test "IOCP - NTSTATUS mapping" {
    // Success
    try std.testing.expectEqual(@as(i32, 0), mapNtStatusToErrno(0));

    // Cancelled
    const cancelled_errno = mapNtStatusToErrno(0xC0000120);
    try std.testing.expect(cancelled_errno < 0);
}

test "IOCP - Timer struct" {
    const timer = Timer{
        .id = 1,
        .deadline_ns = 1_000_000_000,
        .user_data = 42,
    };

    try std.testing.expectEqual(@as(u64, 1), timer.id);
    try std.testing.expectEqual(@as(u64, 1_000_000_000), timer.deadline_ns);
    try std.testing.expectEqual(@as(u64, 42), timer.user_data);
}

test "IOCP - Timer deadline saturating add" {
    // Test the saturating add behavior for timer deadlines
    // This simulates what registerTimeout does

    const now: u64 = std.math.maxInt(u64) - 1000;
    const timeout_ns: u64 = 2000;

    // With regular add, this would overflow and wrap to ~999
    // With saturating add (+|), it should clamp to maxInt
    const deadline = now +| timeout_ns;

    try std.testing.expectEqual(std.math.maxInt(u64), deadline);
}

test "IOCP - calculateTimeout with no timers" {
    // This tests the calculateTimeout logic without needing IOCP
    // We just verify the basic timeout conversion

    // 1 second in ns -> should be ~1000ms
    const timeout_ns: u64 = 1_000_000_000;
    const timeout_ms: u32 = @intCast(@min(timeout_ns / std.time.ns_per_ms, std.math.maxInt(u32) - 1));

    try std.testing.expectEqual(@as(u32, 1000), timeout_ms);
}

test "IOCP - calculateTimeout max value handling" {
    // Test that very large timeouts don't overflow

    const huge_timeout_ns: u64 = std.math.maxInt(u64);
    const timeout_ms: u32 = @intCast(@min(huge_timeout_ns / std.time.ns_per_ms, std.math.maxInt(u32) - 1));

    // Should be clamped to maxInt(u32) - 1
    try std.testing.expectEqual(std.math.maxInt(u32) - 1, timeout_ms);
}

test "IOCP - CreateIoCompletionPort and init" {
    // Test that IOCP backend initializes correctly on Windows
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // IOCP handle should be valid
    try std.testing.expect(backend.iocp != windows.INVALID_HANDLE_VALUE);
}

test "IOCP - init with custom config" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{
        .max_completions = 128,
        .max_operations = 1024,
    });
    defer backend.deinit();

    try std.testing.expectEqual(@as(u32, 128), backend.batch_size);
    try std.testing.expectEqual(@as(usize, 1024), backend.max_operations);
}

test "IOCP - NOP operation via PostQueuedCompletionStatus" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Submit a NOP operation - uses PostQueuedCompletionStatus
    const op = Operation{
        .op = .{ .nop = {} },
        .user_data = 12345,
    };

    const sub_id = try backend.submit(op);
    try std.testing.expectEqual(@as(u64, 12345), sub_id.value);

    // Wait for completion
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 1_000_000_000); // 1 second timeout

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 12345), completions[0].user_data);
}

test "IOCP - timeout registration and completion" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Register a short timeout
    const op = Operation{
        .op = .{ .timeout = .{ .ns = 10_000_000 } }, // 10ms
        .user_data = 999,
    };

    const sub_id = try backend.submit(op);
    try std.testing.expectEqual(@as(u64, 999), sub_id.value);

    // Wait for timeout to fire
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000); // 100ms timeout

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 999), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result); // Timeout success
}

test "IOCP - cancel timeout" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Register a long timeout
    const op = Operation{
        .op = .{ .timeout = .{ .ns = 10_000_000_000 } }, // 10 seconds
        .user_data = 777,
    };

    const sub_id = try backend.submit(op);

    // Cancel it immediately
    try backend.cancel(sub_id);

    // Verify timer list is empty
    try std.testing.expectEqual(@as(usize, 0), backend.timers.items.len);
}

test "IOCP - Overlapped lifecycle tracking" {
    // Test TrackedOp state transitions
    const op = Operation{
        .op = .{ .nop = {} },
        .user_data = 100,
    };

    var tracked = TrackedOp.init(op, SubmissionId.init(100), windows.INVALID_HANDLE_VALUE);

    // Initial state is submitted
    try std.testing.expectEqual(Lifecycle.submitted, tracked.state);

    // Transition to completed
    tracked.state = .completed;
    try std.testing.expectEqual(Lifecycle.completed, tracked.state);

    // Or cancelled
    tracked.state = .cancelled;
    try std.testing.expectEqual(Lifecycle.cancelled, tracked.state);
}

test "IOCP - getHandleFromOp" {
    // Test handle extraction from various operations
    const read_op = Operation{
        .op = .{ .read = .{ .fd = 123, .buffer = &[_]u8{}, .offset = null } },
        .user_data = 0,
    };
    const read_handle = getHandleFromOp(read_op);
    try std.testing.expect(read_handle != null);

    const write_op = Operation{
        .op = .{ .write = .{ .fd = 456, .buffer = &[_]u8{}, .offset = null } },
        .user_data = 0,
    };
    const write_handle = getHandleFromOp(write_op);
    try std.testing.expect(write_handle != null);

    // NOP returns invalid handle
    const nop_op = Operation{
        .op = .{ .nop = {} },
        .user_data = 0,
    };
    const nop_handle = getHandleFromOp(nop_op);
    try std.testing.expectEqual(windows.INVALID_HANDLE_VALUE, nop_handle.?);
}

test "IOCP - multiple timeouts ordering" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Register multiple timeouts
    _ = try backend.submit(Operation{
        .op = .{ .timeout = .{ .ns = 30_000_000 } }, // 30ms
        .user_data = 3,
    });
    _ = try backend.submit(Operation{
        .op = .{ .timeout = .{ .ns = 10_000_000 } }, // 10ms
        .user_data = 1,
    });
    _ = try backend.submit(Operation{
        .op = .{ .timeout = .{ .ns = 20_000_000 } }, // 20ms
        .user_data = 2,
    });

    try std.testing.expectEqual(@as(usize, 3), backend.timers.items.len);

    // Wait for all to complete
    var completions: [8]Completion = undefined;
    var total_completed: usize = 0;

    for (0..10) |_| {
        const count = try backend.wait(&completions, 50_000_000); // 50ms
        total_completed += count;
        if (total_completed >= 3) break;
    }

    try std.testing.expectEqual(@as(usize, 3), total_completed);
}

test "IOCP - flush is no-op" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Flush should return 0 (operations are submitted immediately)
    const flushed = try backend.flush();
    try std.testing.expectEqual(@as(u32, 0), flushed);
}

test "IOCP - fd returns null on Windows" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var backend = try IocpBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Windows HANDLE is not compatible with POSIX fd
    try std.testing.expect(backend.fd() == null);
}

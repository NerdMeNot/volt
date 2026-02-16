//! POSIX poll(2) Backend
//!
//! Universal fallback I/O backend using POSIX poll().
//! Works on any POSIX-compliant system but with lower performance
//! than platform-specific backends (epoll, kqueue, io_uring, IOCP).
//!
//! Architecture:
//! - Slab allocator for O(1) operation tracking
//! - Readiness-based (like epoll/kqueue)
//! - Converts readiness events to completions
//! - EINTR retry loops for signal safety
//!
//! Limitations vs specialized backends:
//! - O(n) fd scanning on each poll() call
//! - No edge-triggered mode
//! - No native async file I/O (only sockets/pipes)
//! - No batched submissions
//!
//! Use cases:
//! - Exotic POSIX platforms (AIX, HP-UX, QNX, etc.)
//! - Development/debugging (simpler than io_uring)
//! - Systems where other backends aren't available
//!
//! Reference: POSIX.1-2017, Stevens UNIX Network Programming

const std = @import("std");
const posix = std.posix;

const completion = @import("types.zig");
const Operation = completion.Operation;
const Completion = completion.Completion;
const SubmissionId = completion.SubmissionId;

const slab = @import("../util/slab.zig");

/// Maximum number of file descriptors to poll.
/// Can be increased but affects memory usage.
const DEFAULT_MAX_FDS: usize = 1024;

/// Maximum number of timers to prevent unbounded memory growth.
const MAX_TIMERS: usize = 65536;

/// When timer capacity exceeds this multiple of count, shrink.
const TIMER_SHRINK_THRESHOLD: usize = 4;

/// Interest flags for poll operations.
const Interest = struct {
    readable: bool = false,
    writable: bool = false,

    fn toPollEvents(self: Interest) i16 {
        var events: i16 = 0;
        if (self.readable) events |= posix.POLL.IN;
        if (self.writable) events |= posix.POLL.OUT;
        return events;
    }
};

/// Registration state for a file descriptor.
const Registration = struct {
    /// The file descriptor.
    fd: posix.fd_t,

    /// Original operation.
    op: Operation,

    /// Submission ID for tracking.
    submission_id: SubmissionId,

    /// What we're interested in.
    interest: Interest,

    /// Whether this is a one-shot registration.
    one_shot: bool,

    pub fn init(fd: posix.fd_t, op: Operation, submission_id: SubmissionId, interest: Interest) Registration {
        return .{
            .fd = fd,
            .op = op,
            .submission_id = submission_id,
            .interest = interest,
            .one_shot = true,
        };
    }
};

/// Special marker for wakeup pipe in pollfd array.
const WAKEUP_FD_INDEX: usize = 0;

/// POSIX poll backend.
pub const PollBackend = struct {
    /// Memory allocator.
    allocator: std.mem.Allocator,

    /// Slab of active registrations.
    registrations: slab.Slab(Registration),

    /// Poll file descriptor array (rebuilt on each poll).
    /// Index 0 is reserved for the wakeup pipe read end.
    pollfds: []posix.pollfd,

    /// Mapping from pollfd index to slab key.
    pollfd_to_key: []usize,

    /// Number of active pollfds (not counting wakeup pipe).
    active_count: usize,

    /// Pending timer expirations (simple list for now).
    timers: std.ArrayList(Timer),

    /// Next timer ID.
    next_timer_id: u64,

    /// Start time for monotonic timer calculations.
    /// All timer deadlines are relative to this instant.
    start_instant: std.time.Instant,

    /// Self-pipe for cross-thread wakeup.
    /// [0] = read end (polled), [1] = write end (signaled)
    wakeup_pipe: [2]posix.fd_t,

    /// Tracks if a wakeup is pending.
    wakeup_pending: std.atomic.Value(bool),

    const Self = @This();

    const Timer = struct {
        id: u64,
        /// Deadline as nanoseconds since start_instant (monotonic).
        deadline_ns: u64,
        user_data: u64,
    };

    /// Initialize the poll backend.
    pub fn init(allocator: std.mem.Allocator, config: anytype) !Self {
        const max_fds = if (@hasField(@TypeOf(config), "max_completions"))
            @as(usize, config.max_completions)
        else
            DEFAULT_MAX_FDS;

        // +1 for wakeup pipe
        const pollfds = try allocator.alloc(posix.pollfd, max_fds + 1);
        errdefer allocator.free(pollfds);

        const pollfd_to_key = try allocator.alloc(usize, max_fds + 1);
        errdefer allocator.free(pollfd_to_key);

        var registrations = try slab.Slab(Registration).init(allocator, max_fds);
        errdefer registrations.deinit();

        // Create self-pipe for cross-thread wakeup
        const wakeup_pipe = try posix.pipe();
        errdefer {
            posix.close(wakeup_pipe[0]);
            posix.close(wakeup_pipe[1]);
        }

        // Set both ends to non-blocking
        _ = try posix.fcntl(wakeup_pipe[0], posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));
        _ = try posix.fcntl(wakeup_pipe[1], posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

        return Self{
            .allocator = allocator,
            .registrations = registrations,
            .pollfds = pollfds,
            .pollfd_to_key = pollfd_to_key,
            .active_count = 0,
            .timers = std.ArrayList(Timer).empty,
            .next_timer_id = 1,
            .start_instant = std.time.Instant.now() catch std.time.Instant{
                // Fallback if monotonic clock unavailable (shouldn't happen on modern systems)
                .timestamp = .{ .sec = 0, .nsec = 0 },
            },
            .wakeup_pipe = wakeup_pipe,
            .wakeup_pending = std.atomic.Value(bool).init(false),
        };
    }

    /// Get current monotonic time as nanoseconds since start_instant.
    fn monotonicNow(self: *Self) u64 {
        const now = std.time.Instant.now() catch return 0;
        return now.since(self.start_instant);
    }

    /// Clean up resources.
    pub fn deinit(self: *Self) void {
        posix.close(self.wakeup_pipe[0]);
        posix.close(self.wakeup_pipe[1]);
        self.timers.deinit(self.allocator);
        self.registrations.deinit();
        self.allocator.free(self.pollfd_to_key);
        self.allocator.free(self.pollfds);
    }

    /// Wake up the I/O driver from another thread.
    /// This is used to notify the driver when new work is available.
    /// Safe to call from any thread. Coalesces multiple calls.
    pub fn unpark(self: *Self) void {
        // Use swap to coalesce multiple unpark calls
        if (self.wakeup_pending.swap(true, .acq_rel)) {
            return; // Already pending
        }

        // Write a byte to wake the poll
        const byte = [_]u8{1};
        _ = posix.write(self.wakeup_pipe[1], &byte) catch {};
    }

    /// Submit an I/O operation.
    pub fn submit(self: *Self, op: Operation) !SubmissionId {
        const user_data = op.user_data;
        const submission_id = SubmissionId.init(user_data);

        switch (op.op) {
            .read => |r| {
                const reg = Registration.init(r.fd, op, submission_id, .{ .readable = true });
                _ = try self.registrations.insert(reg);
            },
            .write => |w| {
                const reg = Registration.init(w.fd, op, submission_id, .{ .writable = true });
                _ = try self.registrations.insert(reg);
            },
            .accept => |a| {
                const reg = Registration.init(a.fd, op, submission_id, .{ .readable = true });
                _ = try self.registrations.insert(reg);
            },
            .connect => |c| {
                const reg = Registration.init(c.fd, op, submission_id, .{ .writable = true });
                _ = try self.registrations.insert(reg);
            },
            .recv => |r| {
                const reg = Registration.init(r.fd, op, submission_id, .{ .readable = true });
                _ = try self.registrations.insert(reg);
            },
            .send => |s| {
                const reg = Registration.init(s.fd, op, submission_id, .{ .writable = true });
                _ = try self.registrations.insert(reg);
            },
            .timeout => |t| {
                // Enforce timer limit to prevent unbounded memory growth
                if (self.timers.items.len >= MAX_TIMERS) {
                    return error.SystemResources;
                }
                const now = self.monotonicNow();
                // Use saturating add to prevent overflow - if we overflow, deadline is maxInt
                // which effectively means "very far in the future" rather than wrapping to past
                const deadline = now +| t.ns;
                try self.timers.append(self.allocator, Timer{
                    .id = self.next_timer_id,
                    .deadline_ns = deadline,
                    .user_data = user_data,
                });
                self.next_timer_id += 1;
            },
            .nop => {
                // NOP completes immediately - we'll handle in wait()
                const reg = Registration.init(-1, op, submission_id, .{});
                _ = try self.registrations.insert(reg);
            },
            .close => |c| {
                // Close is synchronous
                posix.close(c.fd);
                // Will complete immediately in wait()
                const reg = Registration.init(-1, op, submission_id, .{});
                _ = try self.registrations.insert(reg);
            },
            else => return error.OperationNotSupported,
        }

        return submission_id;
    }

    /// Build the pollfd array from registrations.
    fn buildPollfds(self: *Self) void {
        // Index 0 is always the wakeup pipe read end
        self.pollfds[WAKEUP_FD_INDEX] = .{
            .fd = self.wakeup_pipe[0],
            .events = posix.POLL.IN,
            .revents = 0,
        };
        self.pollfd_to_key[WAKEUP_FD_INDEX] = std.math.maxInt(usize); // Sentinel

        // Start user fds at index 1
        self.active_count = 0;

        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            if (entry.value.fd < 0) continue; // Skip non-fd operations

            // +1 to account for wakeup pipe at index 0
            if (self.active_count + 1 >= self.pollfds.len) break;

            self.pollfds[self.active_count + 1] = .{
                .fd = entry.value.fd,
                .events = entry.value.interest.toPollEvents(),
                .revents = 0,
            };
            self.pollfd_to_key[self.active_count + 1] = entry.key;
            self.active_count += 1;
        }
    }

    /// Wait for completions.
    pub fn wait(self: *Self, completions: []Completion, timeout_ns: ?u64) !usize {
        var count: usize = 0;

        // First, complete any immediate operations (nop, close)
        count += self.completeImmediate(completions);
        if (count > 0) return count;

        // Check for expired timers
        count += self.completeTimers(completions[count..]);
        if (count > 0) return count;

        // Build pollfd array
        self.buildPollfds();

        // Even with no user fds, we still poll the wakeup pipe
        // So only skip if there are also no timers AND it's non-blocking
        if (self.active_count == 0 and self.timers.items.len == 0 and timeout_ns == @as(?u64, 0)) {
            return 0;
        }

        // Calculate timeout
        const timeout_ms = self.calculateTimeout(timeout_ns);

        // Call poll with EINTR retry
        const ready_count = self.pollWithRetry(timeout_ms);

        if (ready_count == 0) {
            // Timeout - check timers again
            return self.completeTimers(completions);
        }

        // Check wakeup pipe first (index 0)
        if (self.pollfds[WAKEUP_FD_INDEX].revents != 0) {
            // Drain the pipe to reset it
            var buf: [64]u8 = undefined;
            while (true) {
                _ = posix.read(self.wakeup_pipe[0], &buf) catch break;
            }
            // Clear pending flag
            self.wakeup_pending.store(false, .release);
        }

        // Process ready file descriptors (start at index 1, skipping wakeup pipe)
        // Poll array layout: [wakeup_pipe][fd_0][fd_1]...[fd_n]
        // So we iterate indices 1..active_count+1
        for (1..self.active_count + 1) |i| {
            const pfd = self.pollfds[i];
            if (pfd.revents == 0) continue;
            if (count >= completions.len) break;

            const key = self.pollfd_to_key[i];
            const reg = self.registrations.get(key) orelse continue;

            // Perform the actual I/O
            const result = self.performIo(reg);

            completions[count] = Completion{
                .user_data = reg.op.user_data,
                .result = result,
                .flags = 0,
            };
            count += 1;

            // Remove one-shot registrations
            if (reg.one_shot) {
                _ = self.registrations.remove(key);
            }
        }

        return count;
    }

    /// Complete immediate operations (nop, close).
    /// Uses a fixed-size buffer to avoid unbounded allocation.
    fn completeImmediate(self: *Self, completions: []Completion) usize {
        const MAX_IMMEDIATE_BATCH = 64;
        var to_remove: [MAX_IMMEDIATE_BATCH]usize = undefined;
        var remove_count: usize = 0;
        var count: usize = 0;

        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            // Stop if completions buffer OR remove buffer is full
            if (count >= completions.len or remove_count >= MAX_IMMEDIATE_BATCH) break;

            const is_immediate = switch (entry.value.op.op) {
                .nop, .close => true,
                else => false,
            };

            if (is_immediate) {
                completions[count] = Completion{
                    .user_data = entry.value.op.user_data,
                    .result = 0,
                    .flags = 0,
                };
                to_remove[remove_count] = entry.key;
                remove_count += 1;
                count += 1;
            }
        }

        for (to_remove[0..remove_count]) |key| {
            _ = self.registrations.remove(key);
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

        // Compact timer list if capacity is much larger than count
        // This prevents memory from staying high after burst timer usage
        if (self.timers.capacity > 64 and
            self.timers.capacity > self.timers.items.len * TIMER_SHRINK_THRESHOLD)
        {
            // Shrink to 2x current count (or minimum 64)
            const new_cap = @max(64, self.timers.items.len * 2);
            self.timers.shrinkAndFree(self.allocator, new_cap);
        }

        return count;
    }

    /// Calculate poll timeout considering timers.
    fn calculateTimeout(self: *Self, user_timeout_ns: ?u64) i32 {
        var timeout_ms: i32 = if (user_timeout_ns) |ns|
            @intCast(@min(ns / std.time.ns_per_ms, std.math.maxInt(i32) - 1))
        else
            -1; // Infinite

        // Adjust for nearest timer (using monotonic time)
        if (self.timers.items.len > 0) {
            const now = self.monotonicNow();
            var min_deadline: u64 = std.math.maxInt(u64);

            for (self.timers.items) |timer| {
                if (timer.deadline_ns < min_deadline) {
                    min_deadline = timer.deadline_ns;
                }
            }

            // Calculate remaining time (saturating subtraction to avoid underflow)
            const remaining_ns = if (min_deadline > now) min_deadline - now else 0;
            const timer_ms: i32 = @intCast(@min(
                remaining_ns / std.time.ns_per_ms,
                std.math.maxInt(i32) - 1,
            ));

            if (timeout_ms < 0 or timer_ms < timeout_ms) {
                timeout_ms = timer_ms;
            }
        }

        return timeout_ms;
    }

    /// Call poll() with EINTR retry.
    fn pollWithRetry(self: *Self, timeout_ms: i32) usize {
        while (true) {
            // +1 for wakeup pipe at index 0
            const result = posix.poll(self.pollfds[0 .. self.active_count + 1], timeout_ms) catch |err| {
                switch (err) {
                    error.NetworkSubsystemFailed, error.SystemResources => return 0,
                    error.Unexpected => return 0,
                }
            };
            return result;
        }
    }

    /// Perform the actual I/O operation.
    fn performIo(self: *Self, reg: *const Registration) i32 {
        _ = self;

        return switch (reg.op.op) {
            .read => |r| blk: {
                const n = posix.read(r.fd, r.buffer) catch |err| {
                    break :blk -@as(i32, @intFromEnum(errToErrno(err)));
                };
                break :blk @intCast(n);
            },
            .write => |w| blk: {
                const n = posix.write(w.fd, w.buffer) catch |err| {
                    break :blk -@as(i32, @intFromEnum(errToErrno(err)));
                };
                break :blk @intCast(n);
            },
            .recv => |r| blk: {
                const n = posix.recv(r.fd, r.buffer, r.flags) catch |err| {
                    break :blk -@as(i32, @intFromEnum(errToErrno(err)));
                };
                break :blk @intCast(n);
            },
            .send => |s| blk: {
                const n = posix.send(s.fd, s.buffer, s.flags) catch |err| {
                    break :blk -@as(i32, @intFromEnum(errToErrno(err)));
                };
                break :blk @intCast(n);
            },
            .accept => |a| blk: {
                const result = posix.accept(a.fd, a.addr, a.addr_len, 0) catch |err| {
                    break :blk -@as(i32, @intFromEnum(errToErrno(err)));
                };
                break :blk @intCast(result);
            },
            .connect => |c| blk: {
                posix.connect(c.fd, c.addr, c.addr_len) catch |err| {
                    // Check if already connected or in progress
                    const errno = errToErrno(err);
                    if (errno == .ISCONN) break :blk 0;
                    break :blk -@as(i32, @intFromEnum(errno));
                };
                break :blk 0;
            },
            else => 0,
        };
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Self, id: SubmissionId) !void {
        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            if (entry.value.submission_id.value == id.value) {
                _ = self.registrations.remove(entry.key);
                return;
            }
        }

        // Check timers
        for (self.timers.items, 0..) |timer, i| {
            if (timer.user_data == id.value) {
                _ = self.timers.swapRemove(i);
                return;
            }
        }
    }

    /// Flush is a no-op for poll backend.
    pub fn flush(self: *Self) !u32 {
        _ = self;
        return 0;
    }

    /// Get a file descriptor (not applicable for poll).
    pub fn fd(self: Self) ?posix.fd_t {
        _ = self;
        return null;
    }
};

/// Convert Zig error to POSIX errno.
fn errToErrno(err: anyerror) posix.E {
    return switch (err) {
        error.WouldBlock => .AGAIN,
        error.ConnectionRefused => .CONNREFUSED,
        error.ConnectionResetByPeer => .CONNRESET,
        error.BrokenPipe => .PIPE,
        error.NotOpenForReading => .BADF,
        error.NotOpenForWriting => .BADF,
        error.InputOutput => .IO,
        error.AccessDenied => .ACCES,
        error.InvalidArgument => .INVAL,
        else => .IO,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Poll - init and deinit" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    try std.testing.expectEqual(@as(usize, 0), backend.active_count);
}

test "Poll - submit nop" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    const id = try backend.submit(.{
        .op = .{ .nop = {} },
        .user_data = 42,
    });

    try std.testing.expect(id.isValid());
}

test "Poll - immediate completion for nop" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    _ = try backend.submit(.{
        .op = .{ .nop = {} },
        .user_data = 123,
    });

    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 123), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result);
}

test "Poll - timer expiration" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Submit a very short timer (1ms)
    _ = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 1_000_000 } },
        .user_data = 456,
    });

    // Wait long enough for timer to expire
    std.Thread.sleep(5_000_000); // 5ms

    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 456), completions[0].user_data);
}

test "Poll - cancel operation" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 1_000_000_000 } }, // 1 second
        .user_data = 789,
    });

    try backend.cancel(id);

    try std.testing.expectEqual(@as(usize, 0), backend.timers.items.len);
}

test "Poll - Interest to poll events" {
    const readable = Interest{ .readable = true };
    try std.testing.expect((readable.toPollEvents() & posix.POLL.IN) != 0);

    const writable = Interest{ .writable = true };
    try std.testing.expect((writable.toPollEvents() & posix.POLL.OUT) != 0);

    const both = Interest{ .readable = true, .writable = true };
    const events = both.toPollEvents();
    try std.testing.expect((events & posix.POLL.IN) != 0);
    try std.testing.expect((events & posix.POLL.OUT) != 0);
}

test "Poll - timer with max timeout does not overflow" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Submit a timer with maximum possible timeout
    // This tests the saturating add fix - should not wrap around
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = std.math.maxInt(u64) } },
        .user_data = 0xDEADBEEF,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.timers.items.len);

    // Verify deadline is at maxInt (saturated), not wrapped to small value
    const timer = backend.timers.items[0];
    try std.testing.expectEqual(std.math.maxInt(u64), timer.deadline_ns);

    // Clean up
    try backend.cancel(id);
}

test "Poll - zero timeout completes immediately" {
    var backend = try PollBackend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Submit a zero timeout
    _ = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 0 } },
        .user_data = 111,
    });

    // Should complete immediately
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 111), completions[0].user_data);
}

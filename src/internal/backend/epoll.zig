//! Linux epoll Backend
//!
//! Readiness-based I/O multiplexing for Linux (fallback when io_uring unavailable).
//! Uses tick-based readiness tracking to prevent ABA race conditions.
//!
//! Architecture:
//! - Slab allocator for O(1) registration lookup (token = slab index)
//! - ScheduledIo per registration for tick-based readiness state
//! - EINTR retry loop for signal safety
//! - timerfd for timeout operations
//! - Edge-triggered mode (EPOLLET) with EPOLLONESHOT
//!
//! Modeled after production epoll event loop patterns with battle-tested edge case handling.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;

const completion = @import("types.zig");
const Operation = completion.Operation;
const Completion = completion.Completion;
const SubmissionId = completion.SubmissionId;

const slab = @import("../util/slab.zig");
const scheduled_io = @import("scheduled_io.zig");
const ScheduledIo = scheduled_io.ScheduledIo;
const Ready = scheduled_io.Ready;

/// Registration entry in the slab.
const Registration = struct {
    /// The submitted operation.
    op: Operation,
    /// Tick-based readiness state.
    io: ScheduledIo,
    /// Submission ID for user tracking.
    submission_id: SubmissionId,
    /// File descriptor being monitored (for cleanup).
    monitored_fd: ?posix.fd_t,
    /// Timer fd if this is a timeout operation.
    timer_fd: ?posix.fd_t,

    fn init(op: Operation, submission_id: SubmissionId, monitored_fd: ?posix.fd_t) Registration {
        return .{
            .op = op,
            .io = ScheduledIo.init(),
            .submission_id = submission_id,
            .monitored_fd = monitored_fd,
            .timer_fd = null,
        };
    }
};

/// Special token for wakeup event (max value to avoid collision with slab indices).
const TOKEN_WAKEUP: usize = std.math.maxInt(usize);

/// epoll backend implementation with production-grade data structures.
pub const EpollBackend = struct {
    epoll_fd: posix.fd_t,
    allocator: std.mem.Allocator,

    /// Slab of registrations - token is slab index for O(1) lookup.
    registrations: slab.Slab(Registration),

    /// Events buffer.
    event_buffer: []linux.epoll_event,

    /// eventfd for cross-thread wakeup (written to from any thread, polled by driver).
    wakeup_fd: posix.fd_t,

    /// Tracks if a wakeup is pending (for coalescing multiple unpark calls).
    wakeup_pending: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: anytype) !Self {
        const epoll_fd = try posix.epoll_create1(.{ .CLOEXEC = true });
        errdefer posix.close(epoll_fd);

        const max_events = config.max_completions;
        const event_buffer = try allocator.alloc(linux.epoll_event, max_events);
        errdefer allocator.free(event_buffer);

        // Pre-allocate slab with reasonable capacity
        var reg_slab = try slab.Slab(Registration).init(allocator, max_events);
        errdefer reg_slab.deinit();

        // Create eventfd for cross-thread wakeup mechanism
        const wakeup_result = linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        if (@as(isize, @bitCast(wakeup_result)) < 0) {
            return error.WakeupCreationFailed;
        }
        const wakeup_fd: posix.fd_t = @intCast(wakeup_result);
        errdefer posix.close(wakeup_fd);

        // Register wakeup fd with epoll (edge-triggered for efficiency)
        var wakeup_event = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ET,
            .data = .{ .ptr = TOKEN_WAKEUP },
        };
        const ctl_result = linux.epoll_ctl(epoll_fd, .ADD, wakeup_fd, &wakeup_event);
        if (@as(isize, @bitCast(ctl_result)) < 0) {
            return error.WakeupRegistrationFailed;
        }

        return Self{
            .epoll_fd = epoll_fd,
            .allocator = allocator,
            .registrations = reg_slab,
            .event_buffer = event_buffer,
            .wakeup_fd = wakeup_fd,
            .wakeup_pending = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Self) void {
        // Shutdown all registrations and clean up timer fds
        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            entry.value.io.shutdown(); // Wakes all waiters
            entry.value.io.clearWakers(); // Break any remaining reference cycles
            if (entry.value.timer_fd) |tfd| {
                posix.close(tfd);
            }
        }

        posix.close(self.wakeup_fd);
        posix.close(self.epoll_fd);
        self.registrations.deinit();
        self.allocator.free(self.event_buffer);
    }

    /// Wake up the I/O driver from another thread.
    /// This is used to notify the driver when new work is available.
    /// Safe to call from any thread. Coalesces multiple calls.
    pub fn unpark(self: *Self) void {
        // Use swap to coalesce multiple unpark calls - only one wakeup needed
        if (self.wakeup_pending.swap(true, .acq_rel)) {
            // Already pending, no need to write again
            return;
        }

        // Write to eventfd to wake the epoll_wait
        const val: u64 = 1;
        _ = posix.write(self.wakeup_fd, std.mem.asBytes(&val)) catch {
            // Best-effort: if write fails (shouldn't happen with eventfd),
            // the driver will eventually wake anyway on timeout
        };
    }

    /// Submit an operation.
    pub fn submit(self: *Self, op: Operation) !SubmissionId {
        const user_data = op.user_data;
        const submission_id = SubmissionId.init(user_data);

        // Determine the fd to monitor (if any)
        const monitored_fd = getMonitoredFd(op);

        // Allocate registration in slab
        const token = try self.registrations.insert(Registration.init(op, submission_id, monitored_fd));
        errdefer _ = self.registrations.remove(token);

        // Register with epoll if needed
        try self.registerWithEpoll(op, token);

        return submission_id;
    }

    /// Get the fd being monitored for an operation.
    fn getMonitoredFd(op: Operation) ?posix.fd_t {
        return switch (op.op) {
            .read => |r| r.fd,
            .write => |w| w.fd,
            .accept => |a| a.fd,
            .connect => |c| c.fd,
            .recv => |r| r.fd,
            .send => |s| s.fd,
            else => null,
        };
    }

    /// Register an operation with epoll.
    fn registerWithEpoll(self: *Self, op: Operation, token: usize) !void {
        switch (op.op) {
            .read => |r| try self.addFdToEpoll(r.fd, linux.EPOLL.IN, token),
            .write => |w| try self.addFdToEpoll(w.fd, linux.EPOLL.OUT, token),
            .accept => |a| try self.addFdToEpoll(a.fd, linux.EPOLL.IN, token),
            .connect => |c| try self.addFdToEpoll(c.fd, linux.EPOLL.OUT, token),
            .recv => |r| try self.addFdToEpoll(r.fd, linux.EPOLL.IN, token),
            .send => |s| try self.addFdToEpoll(s.fd, linux.EPOLL.OUT, token),
            .timeout => |t| try self.setupTimer(t.ns, token),
            // These don't need epoll registration
            .open, .close, .fsync, .nop, .cancel, .readv, .writev => {},
            // Linux-only multishot operations - epoll fallback doesn't support these
            // Use io_uring backend for multishot support
            .accept_multishot, .recv_multishot => {},
        }
    }

    /// Add a file descriptor to epoll.
    fn addFdToEpoll(self: *Self, target_fd: posix.fd_t, events: u32, token: usize) !void {
        var event = linux.epoll_event{
            .events = events | linux.EPOLL.ET | linux.EPOLL.ONESHOT | linux.EPOLL.RDHUP,
            .data = .{ .ptr = token },
        };

        // Try to add, if already exists, modify
        posix.epoll_ctl(self.epoll_fd, .ADD, target_fd, &event) catch |err| {
            if (err == error.FileDescriptorAlreadyPresentInSet) {
                try posix.epoll_ctl(self.epoll_fd, .MOD, target_fd, &event);
            } else {
                return err;
            }
        };
    }

    /// Setup a timerfd for timeout operations.
    /// IMPORTANT: This function follows a strict resource acquisition order:
    /// 1. Create timerfd
    /// 2. Configure timer
    /// 3. Add to epoll
    /// 4. Store in registration (only after all kernel ops succeed)
    /// This ensures no partial state on failure and no double-close.
    fn setupTimer(self: *Self, timeout_ns: u64, token: usize) !void {
        // Create timerfd
        const timer_fd_raw = linux.timerfd_create(.MONOTONIC, .{
            .CLOEXEC = true,
            .NONBLOCK = true,
        });

        if (@as(isize, @bitCast(timer_fd_raw)) < 0) {
            return error.SystemResources;
        }

        const timer_fd: posix.fd_t = @intCast(timer_fd_raw);

        // From here, we own timer_fd and must close it on any error path.
        // We use a simple flag to track whether we've handed off ownership.
        var timer_fd_owned = true;
        defer if (timer_fd_owned) posix.close(timer_fd);

        // Clamp timeout to safe maximum to prevent overflow
        const safe_ns = completion.clampTimeout(timeout_ns);

        // Set the timer
        const ts = linux.itimerspec{
            .it_interval = .{ .sec = 0, .nsec = 0 },
            .it_value = .{
                .sec = @intCast(safe_ns / std.time.ns_per_s),
                .nsec = @intCast(safe_ns % std.time.ns_per_s),
            },
        };

        const result = linux.timerfd_settime(timer_fd, .{}, &ts, null);
        if (@as(isize, @bitCast(result)) < 0) {
            return error.SystemResources;
        }

        // Add to epoll BEFORE storing in registration
        // This way, if epoll_ctl fails, we don't have a dangling reference
        var event = linux.epoll_event{
            .events = linux.EPOLL.IN | linux.EPOLL.ONESHOT,
            .data = .{ .ptr = token },
        };

        try posix.epoll_ctl(self.epoll_fd, .ADD, timer_fd, &event);

        // All kernel operations succeeded - now store in registration
        // and transfer ownership (defer won't close)
        if (self.registrations.get(token)) |reg| {
            reg.timer_fd = timer_fd;
            timer_fd_owned = false; // Registration now owns it
        } else {
            // Registration disappeared (shouldn't happen, but be safe)
            // Remove from epoll since we're about to close
            // Best-effort cleanup: DEL errors are non-critical during error recovery
            posix.epoll_ctl(self.epoll_fd, .DEL, timer_fd, null) catch {};
            return error.InvalidArgument;
        }
    }

    /// Wait for completions with EINTR retry.
    pub fn wait(self: *Self, completions: []Completion, timeout_ns: ?u64) !usize {
        // First, handle operations that don't need epoll
        const immediate_count = try self.processImmediateOps(completions);
        if (immediate_count > 0) {
            return immediate_count;
        }

        // Convert timeout
        const timeout_ms: i32 = if (timeout_ns) |ns| blk: {
            if (ns == 0) break :blk 0;
            break :blk @intCast(@min(ns / std.time.ns_per_ms, std.math.maxInt(i32)));
        } else -1; // Infinite

        // Wait on epoll with EINTR retry
        const nevents = try self.epollWaitWithRetry(timeout_ms);

        // Process events with tick-based readiness
        var count: usize = 0;
        for (self.event_buffer[0..nevents]) |event| {
            if (count >= completions.len) break;

            const token = event.data.ptr;

            // Handle wakeup event (cross-thread notification)
            if (token == TOKEN_WAKEUP) {
                // Drain the eventfd to reset it (required for edge-triggered)
                var buf: u64 = undefined;
                _ = posix.read(self.wakeup_fd, std.mem.asBytes(&buf)) catch {};
                // Clear pending flag so future unpark() calls will write again
                self.wakeup_pending.store(false, .release);
                continue;
            }

            // Get registration from slab (O(1) lookup)
            const reg = self.registrations.get(token) orelse continue;

            // Determine readiness from event
            const ready = readyFromEvent(event);

            // Update tick-based readiness state
            const ready_event = reg.io.setReadiness(ready);

            // Check for errors in the event
            const has_error = (event.events & linux.EPOLL.ERR) != 0;

            // Perform the actual operation now that fd is ready
            const result = if (has_error)
                -@as(i32, @intCast(@intFromEnum(linux.E.IO)))
            else
                self.performOperation(reg.op);

            completions[count] = Completion{
                .user_data = reg.submission_id.value,
                .result = result,
                .flags = if (ready_event.is_shutdown) 1 else 0,
            };

            // Clean up timer fd if present
            if (reg.timer_fd) |tfd| {
                // Best-effort removal: timer already fired, DEL failure is non-critical
                posix.epoll_ctl(self.epoll_fd, .DEL, tfd, null) catch {};
                posix.close(tfd);
            }

            // Remove registration from slab
            _ = self.registrations.remove(token);
            count += 1;
        }

        return count;
    }

    /// epoll_wait with EINTR retry loop.
    fn epollWaitWithRetry(self: *Self, timeout_ms: i32) !usize {
        while (true) {
            const result = linux.epoll_wait(
                self.epoll_fd,
                self.event_buffer.ptr,
                @intCast(self.event_buffer.len),
                timeout_ms,
            );

            if (@as(isize, @bitCast(result)) >= 0) {
                return result;
            }

            const err = linux.E.init(result);
            switch (err) {
                .INTR => continue, // Retry on EINTR
                .BADF => return error.BadFileDescriptor,
                .FAULT => return error.BadAddress,
                .INVAL => return error.InvalidArgument,
                else => return error.Unexpected,
            }
        }
    }

    /// Convert epoll event to Ready flags.
    fn readyFromEvent(event: linux.epoll_event) Ready {
        var ready = Ready.EMPTY;

        if ((event.events & linux.EPOLL.IN) != 0) {
            ready.readable = true;
        }
        if ((event.events & linux.EPOLL.OUT) != 0) {
            ready.writable = true;
        }
        if ((event.events & linux.EPOLL.HUP) != 0) {
            ready.read_closed = true;
            ready.write_closed = true;
        }
        if ((event.events & linux.EPOLL.RDHUP) != 0) {
            ready.read_closed = true;
        }
        if ((event.events & linux.EPOLL.ERR) != 0) {
            ready.err = true;
        }
        if ((event.events & linux.EPOLL.PRI) != 0) {
            ready.priority = true;
        }

        return ready;
    }

    /// Process operations that don't need epoll (sync ops).
    /// Uses a fixed-size buffer to avoid unbounded allocation.
    fn processImmediateOps(self: *Self, completions: []Completion) !usize {
        // Fixed-size buffer for keys to remove (avoids unbounded allocation)
        const MAX_IMMEDIATE_BATCH = 64;
        var to_remove: [MAX_IMMEDIATE_BATCH]usize = undefined;
        var remove_count: usize = 0;
        var count: usize = 0;

        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            // Stop if completions buffer is full OR our remove buffer is full
            if (count >= completions.len or remove_count >= MAX_IMMEDIATE_BATCH) break;

            const is_immediate = switch (entry.value.op.op) {
                .open, .close, .fsync, .nop => true,
                else => false,
            };

            if (is_immediate) {
                const result = self.performOperation(entry.value.op);
                completions[count] = Completion{
                    .user_data = entry.value.submission_id.value,
                    .result = result,
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

    /// Perform the actual I/O operation after epoll indicates readiness.
    fn performOperation(self: *Self, op: Operation) i32 {
        _ = self;
        return switch (op.op) {
            .read => |r| blk: {
                const n = posix.read(r.fd, r.buffer) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk @intCast(n);
            },
            .write => |w| blk: {
                const n = posix.write(w.fd, w.buffer) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk @intCast(n);
            },
            .accept => |a| blk: {
                const result = posix.accept(a.fd, a.addr, a.addr_len, 0) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk @intCast(result);
            },
            .connect => |_| blk: {
                // connect() was already initiated, just return success
                break :blk 0;
            },
            .recv => |r| blk: {
                const result = posix.recv(r.fd, r.buffer, r.flags) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk @intCast(result);
            },
            .send => |s| blk: {
                const result = posix.send(s.fd, s.buffer, s.flags) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk @intCast(result);
            },
            .open => |o| blk: {
                const open_fd = linux.openat(linux.AT.FDCWD, o.path, @bitCast(o.flags), o.mode);
                const result = linux.E.init(open_fd);
                if (result != .SUCCESS) {
                    break :blk -@as(i32, @intCast(@intFromEnum(result)));
                }
                break :blk @intCast(open_fd);
            },
            .close => |c| blk: {
                posix.close(c.fd);
                break :blk 0;
            },
            .fsync => |f| blk: {
                posix.fsync(f.fd) catch |e| {
                    break :blk -errnoFromError(e);
                };
                break :blk 0;
            },
            .timeout => 0, // Timer expired successfully
            .nop => 0,
            else => -@as(i32, @intCast(@intFromEnum(linux.E.NOSYS))),
        };
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Self, id: SubmissionId) !void {
        // Find the registration by submission ID
        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            if (entry.value.submission_id.value == id.value) {
                // Shutdown the ScheduledIo to wake any waiters
                entry.value.io.shutdown();

                // Remove from epoll - best-effort, fd is being cancelled anyway
                if (entry.value.monitored_fd) |mon_fd| {
                    posix.epoll_ctl(self.epoll_fd, .DEL, mon_fd, null) catch {};
                }

                // Clean up timer fd if present (and mark as null to prevent double-close)
                if (entry.value.timer_fd) |tfd| {
                    // Best-effort removal: operation is cancelled, DEL failure is non-critical
                    posix.epoll_ctl(self.epoll_fd, .DEL, tfd, null) catch {};
                    posix.close(tfd);
                    entry.value.timer_fd = null; // Prevent double-close in deinit
                }

                _ = self.registrations.remove(entry.key);
                return;
            }
        }
    }

    /// Get epoll file descriptor.
    pub fn fd(self: Self) posix.fd_t {
        return self.epoll_fd;
    }
};

/// Convert Zig error to errno.
fn errnoFromError(err: anyerror) i32 {
    const e: linux.E = switch (err) {
        error.WouldBlock => .AGAIN,
        error.ConnectionRefused => .CONNREFUSED,
        error.ConnectionResetByPeer => .CONNRESET,
        error.BrokenPipe => .PIPE,
        error.NotOpenForReading, error.NotOpenForWriting => .BADF,
        error.AccessDenied => .ACCES,
        error.FileNotFound => .NOENT,
        error.InputOutput => .IO,
        error.InvalidArgument => .INVAL,
        error.NetworkUnreachable => .NETUNREACH,
        error.NotConnected => .NOTCONN,
        error.TimedOut => .TIMEDOUT,
        else => .IO,
    };
    return @intCast(@intFromEnum(e));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "EpollBackend - init and deinit" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    try std.testing.expect(backend.fd() >= 0);
}

test "EpollBackend - slab-based registration" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a nop operation
    const id = try backend.submit(.{
        .op = .{ .nop = {} },
        .user_data = 42,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.registrations.count());

    // Wait should process immediate ops
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 42), completions[0].user_data);
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - timer expiration" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a short timer (10ms)
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 10_000_000 } },
        .user_data = 789,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.registrations.count());

    // Verify timer_fd was set
    var it = backend.registrations.iterator();
    if (it.next()) |entry| {
        try std.testing.expect(entry.value.timer_fd != null);
    }

    // Wait for timer to expire
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000); // 100ms timeout

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 789), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result);
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - timer cancellation cleans up timerfd" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a long timer (1 second)
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 1_000_000_000 } },
        .user_data = 999,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.registrations.count());

    // Capture timer_fd before cancel
    var timer_fd_before: ?posix.fd_t = null;
    var iter = backend.registrations.iterator();
    if (iter.next()) |entry| {
        timer_fd_before = entry.value.timer_fd;
        try std.testing.expect(timer_fd_before != null);
    }

    // Cancel the timer
    try backend.cancel(id);

    // Registration should be removed and no timer_fd leak
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - multiple timeout submissions" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
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

    try std.testing.expectEqual(@as(usize, 3), backend.registrations.count());

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

test "EpollBackend - wait with zero timeout returns immediately" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // No operations submitted
    var completions: [8]Completion = undefined;
    const start = std.time.nanoTimestamp();
    const count = try backend.wait(&completions, 0);
    const elapsed = std.time.nanoTimestamp() - start;

    // Should return immediately with no completions
    try std.testing.expectEqual(@as(usize, 0), count);
    // Should be fast (< 10ms)
    try std.testing.expect(elapsed < 10_000_000);
}

test "EpollBackend - TCP socket accept readiness" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create listening socket
    const listen_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
    defer posix.close(listen_fd);

    // Bind to ephemeral port
    var addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port = 0, // Ephemeral
        .addr = std.mem.nativeToBig(u32, 0x7f000001), // 127.0.0.1
    };
    try posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    try posix.listen(listen_fd, 1);

    // Get assigned port
    var name_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    try posix.getsockname(listen_fd, @ptrCast(&addr), &name_len);

    // Submit accept operation
    var accept_addr: posix.sockaddr = undefined;
    var accept_addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
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
    const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(client_fd);

    try posix.connect(client_fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));

    // Wait for accept to complete
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 100), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0); // Valid fd

    // Clean up accepted fd
    posix.close(@intCast(completions[0].result));
}

test "EpollBackend - TCP socket send/recv readiness" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create connected socket pair using TCP loopback
    const listen_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(listen_fd);

    var addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port = 0,
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    try posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    try posix.listen(listen_fd, 1);

    var name_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    try posix.getsockname(listen_fd, @ptrCast(&addr), &name_len);

    const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(client_fd);
    try posix.connect(client_fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));

    var accept_addr: posix.sockaddr = undefined;
    var accept_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const server_fd = try posix.accept(listen_fd, &accept_addr, &accept_len, 0);
    defer posix.close(server_fd);

    // Set non-blocking
    _ = try posix.fcntl(client_fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));
    _ = try posix.fcntl(server_fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    // Submit send on client
    const send_data = "Hello epoll!";
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

test "EpollBackend - cancel non-existent operation is safe" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Try to cancel a non-existent operation
    const fake_id = SubmissionId.init(99999);
    try backend.cancel(fake_id);

    // Should not crash, registrations still empty
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - rapid submit and cancel" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Rapidly submit and cancel operations
    var ids: [50]SubmissionId = undefined;

    for (0..50) |i| {
        ids[i] = try backend.submit(.{
            .op = .{ .timeout = .{ .ns = 1_000_000_000 } }, // 1 second
            .user_data = @intCast(i),
        });
    }

    try std.testing.expectEqual(@as(usize, 50), backend.registrations.count());

    // Cancel all
    for (ids) |id| {
        try backend.cancel(id);
    }

    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - nop operation completes immediately" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit multiple nops
    for (0..5) |i| {
        _ = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = @intCast(i),
        });
    }

    try std.testing.expectEqual(@as(usize, 5), backend.registrations.count());

    // All should complete immediately
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 5), count);
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "EpollBackend - close operation" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a socket to close
    const fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);

    // Submit close operation
    _ = try backend.submit(.{
        .op = .{ .close = .{ .fd = fd } },
        .user_data = 777,
    });

    // Should complete immediately
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 777), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result); // Success
}

test "EpollBackend - user_data preserved correctly" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
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
    const count = try backend.wait(&completions, 0);

    try std.testing.expectEqual(@as(usize, 4), count);

    // Verify all user_data values are present
    var found: [4]bool = .{ false, false, false, false };
    for (completions[0..count]) |comp| {
        for (user_data_values, 0..) |ud, i| {
            if (comp.user_data == ud) found[i] = true;
        }
    }

    for (found) |f| {
        try std.testing.expect(f);
    }
}

test "EpollBackend - read and write on pipe" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a pipe for read/write testing
    const pipe_fds = try posix.pipe();
    defer {
        posix.close(pipe_fds[0]);
        posix.close(pipe_fds[1]);
    }

    // Set read end to non-blocking
    _ = try posix.fcntl(pipe_fds[0], posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    // Write some data to the pipe (using posix.write directly)
    const write_data = "Test epoll pipe data";
    _ = try posix.write(pipe_fds[1], write_data);

    // Submit read operation via backend
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

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 888), completions[0].user_data);
    try std.testing.expect(completions[0].result > 0);

    const read_len: usize = @intCast(completions[0].result);
    try std.testing.expectEqualStrings(write_data, read_buf[0..read_len]);
}

test "EpollBackend - EPOLLET edge-triggered behavior" {
    if (builtin.os.tag != .linux) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try EpollBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a pipe
    const pipe_fds = try posix.pipe();
    defer {
        posix.close(pipe_fds[0]);
        posix.close(pipe_fds[1]);
    }

    // Set non-blocking
    _ = try posix.fcntl(pipe_fds[0], posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    // Write data before registering
    _ = try posix.write(pipe_fds[1], "data1");

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
        .user_data = 111,
    });

    // Should get the data
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expect(completions[0].result > 0);
}

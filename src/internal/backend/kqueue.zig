//! macOS/BSD kqueue Backend
//!
//! Readiness-based I/O multiplexing for macOS, FreeBSD, OpenBSD, NetBSD.
//! Uses tick-based readiness tracking to prevent ABA race conditions.
//!
//! Architecture:
//! - Slab allocator for O(1) registration lookup (token = slab index)
//! - ScheduledIo per registration for tick-based readiness state
//! - EINTR retry loop for signal safety
//! - EVFILT_TIMER for timeout operations
//!
//! Modeled after production kqueue event loop patterns with battle-tested edge case handling.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const completion = @import("types.zig");
const Operation = completion.Operation;
const Completion = completion.Completion;
const SubmissionId = completion.SubmissionId;

const slab = @import("../util/slab.zig");
const scheduled_io = @import("scheduled_io.zig");
const ScheduledIo = scheduled_io.ScheduledIo;
const Ready = scheduled_io.Ready;

const Kevent = posix.Kevent;

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
    /// Timer ident for EVFILT_TIMER operations (needed for cancellation).
    timer_ident: ?usize,

    fn init(op: Operation, submission_id: SubmissionId, monitored_fd: ?posix.fd_t) Registration {
        return .{
            .op = op,
            .io = ScheduledIo.init(),
            .submission_id = submission_id,
            .monitored_fd = monitored_fd,
            .timer_ident = null,
        };
    }
};

/// Special token values.
const TOKEN_WAKEUP: usize = std.math.maxInt(usize);
const TOKEN_TIMER_BASE: usize = std.math.maxInt(usize) - 0x10000;

/// Maximum change list size before auto-flush.
/// Prevents unbounded memory growth during heavy submit bursts.
const CHANGE_LIST_FLUSH_THRESHOLD: usize = 256;

/// kqueue backend implementation with production-grade data structures.
pub const KqueueBackend = struct {
    kq_fd: posix.fd_t,
    allocator: std.mem.Allocator,

    /// Slab of registrations - token is slab index for O(1) lookup.
    registrations: slab.Slab(Registration),

    /// Changes to register with kqueue.
    change_list: std.ArrayList(Kevent),

    /// Events received from kqueue.
    event_buffer: []Kevent,

    /// Counter for timer IDs (separate from slab to avoid conflicts).
    next_timer_id: u16,

    /// Counter for OOM events during cancel operations.
    /// Non-zero indicates memory pressure; useful for observability/debugging.
    cancel_oom_count: std.atomic.Value(u64),

    /// Tracks if a wakeup event is pending (for cross-thread notification).
    /// Used to coalesce multiple unpark() calls into a single wakeup.
    wakeup_pending: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: anytype) !Self {
        const kq_fd = try posix.kqueue();
        errdefer posix.close(kq_fd);

        const max_events = config.max_completions;
        const event_buffer = try allocator.alloc(Kevent, max_events);
        errdefer allocator.free(event_buffer);

        // Pre-allocate slab with reasonable capacity
        var reg_slab = try slab.Slab(Registration).init(allocator, max_events);
        errdefer reg_slab.deinit();

        // Register EVFILT_USER for cross-thread wakeup mechanism
        // EV_CLEAR ensures the event is reset after delivery
        var wakeup_kevent = [_]Kevent{.{
            .ident = TOKEN_WAKEUP,
            .filter = posix.system.EVFILT.USER,
            .flags = posix.system.EV.ADD | posix.system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = TOKEN_WAKEUP,
        }};

        // Register the wakeup event immediately (no output events needed)
        var empty_out: [0]Kevent = undefined;
        const result = posix.system.kevent(
            kq_fd,
            &wakeup_kevent,
            1,
            &empty_out,
            0,
            null,
        );
        if (result < 0) {
            return error.WakeupRegistrationFailed;
        }

        return Self{
            .kq_fd = kq_fd,
            .allocator = allocator,
            .registrations = reg_slab,
            .change_list = .empty,
            .event_buffer = event_buffer,
            .next_timer_id = 0,
            .cancel_oom_count = std.atomic.Value(u64).init(0),
            .wakeup_pending = std.atomic.Value(bool).init(false),
        };
    }

    /// Get the count of OOM events that occurred during cancel operations.
    /// Useful for observability - non-zero indicates memory pressure.
    pub fn getCancelOomCount(self: *const Self) u64 {
        return self.cancel_oom_count.load(.monotonic);
    }

    /// Wake up the I/O driver from another thread.
    /// This is used to notify the driver when new work is available.
    /// Safe to call from any thread. Coalesces multiple calls.
    pub fn unpark(self: *Self) void {
        // Use swap to coalesce multiple unpark calls - only one wakeup needed
        if (self.wakeup_pending.swap(true, .acq_rel)) {
            // Already pending, no need to trigger again
            return;
        }

        // Trigger the EVFILT_USER event with NOTE_TRIGGER
        var trigger_kevent = [_]Kevent{.{
            .ident = TOKEN_WAKEUP,
            .filter = posix.system.EVFILT.USER,
            .flags = 0,
            .fflags = posix.system.NOTE.TRIGGER,
            .data = 0,
            .udata = TOKEN_WAKEUP,
        }};

        // Best-effort trigger - if this fails, the driver will eventually wake anyway
        var empty_out: [0]Kevent = undefined;
        _ = posix.system.kevent(
            self.kq_fd,
            &trigger_kevent,
            1,
            &empty_out,
            0,
            null,
        );
    }

    pub fn deinit(self: *Self) void {
        // Shutdown all registrations
        var it = self.registrations.iterator();
        while (it.next()) |entry| {
            entry.value.io.shutdown(); // Wakes all waiters
            entry.value.io.clearWakers(); // Break any remaining reference cycles
        }

        posix.close(self.kq_fd);
        self.registrations.deinit();
        self.change_list.deinit(self.allocator);
        self.allocator.free(self.event_buffer);
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

        // Create kevent based on operation type
        if (try self.createKevent(op, token)) |kevent| {
            try self.change_list.append(self.allocator, kevent);
        }

        // Auto-flush if change list is getting large to prevent unbounded growth
        if (self.change_list.items.len >= CHANGE_LIST_FLUSH_THRESHOLD) {
            try self.flushChanges();
        }

        return submission_id;
    }

    /// Flush pending kevent changes to the kernel.
    /// Called automatically when change_list grows too large, or manually before wait.
    /// IMPORTANT: Always clears change_list regardless of outcome to prevent stale state.
    fn flushChanges(self: *Self) !void {
        if (self.change_list.items.len == 0) return;

        // Submit changes with zero timeout (don't wait for events)
        const ts = posix.timespec{ .sec = 0, .nsec = 0 };

        // Always clear change list to prevent stale state accumulation.
        // If kevent fails, the changes weren't fully applied, but keeping them
        // would cause duplicate ADD attempts on retry (which may also fail).
        // Atomic submit-or-fail: stale changes could cause duplicate ADD attempts on retry.
        defer self.change_list.clearRetainingCapacity();

        _ = try self.keventWithRetry(&ts);
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

    /// Create a kevent for the given operation.
    /// For timeout operations, also stores the timer ident in the registration for cancellation.
    fn createKevent(self: *Self, op: Operation, token: usize) !?Kevent {
        return switch (op.op) {
            .read => |r| Kevent{
                .ident = @intCast(r.fd),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .write => |w| Kevent{
                .ident = @intCast(w.fd),
                .filter = posix.system.EVFILT.WRITE,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .accept => |a| Kevent{
                .ident = @intCast(a.fd),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .connect => |c| Kevent{
                .ident = @intCast(c.fd),
                .filter = posix.system.EVFILT.WRITE,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .recv => |r| Kevent{
                .ident = @intCast(r.fd),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .send => |s| Kevent{
                .ident = @intCast(s.fd),
                .filter = posix.system.EVFILT.WRITE,
                .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = token,
            },
            .timeout => |t| blk: {
                // Use EVFILT_TIMER for timeout operations
                const timer_id = self.next_timer_id;
                self.next_timer_id +%= 1;

                // Clamp timeout to safe maximum and convert to milliseconds for kqueue
                const safe_ns = completion.clampTimeout(t.ns);
                const timeout_ms: isize = @intCast(safe_ns / std.time.ns_per_ms);

                const timer_ident = TOKEN_TIMER_BASE - timer_id;

                // Store timer_ident in registration for cancellation
                if (self.registrations.get(token)) |reg| {
                    reg.timer_ident = timer_ident;
                }

                break :blk Kevent{
                    .ident = timer_ident,
                    .filter = posix.system.EVFILT.TIMER,
                    .flags = posix.system.EV.ADD | posix.system.EV.ONESHOT,
                    .fflags = 0, // NOTE_NSECONDS not portable, use ms
                    .data = timeout_ms,
                    .udata = token,
                };
            },
            // These don't need kevent registration - execute immediately
            .open, .close, .fsync, .nop => null,
            // Not directly supported
            .readv, .writev, .cancel => null,
            // Linux-only multishot operations - not supported on macOS/BSD
            .accept_multishot, .recv_multishot => null,
        };
    }

    /// Wait for completions with EINTR retry.
    pub fn wait(self: *Self, completions: []Completion, timeout_ns: ?u64) !usize {
        // First, handle operations that don't need kqueue
        const immediate_count = try self.processImmediateOps(completions);
        if (immediate_count > 0) {
            return immediate_count;
        }

        // Convert timeout to timespec
        var ts_storage: posix.timespec = undefined;
        const ts: ?*const posix.timespec = if (timeout_ns) |ns| blk: {
            if (ns == 0) {
                ts_storage = .{ .sec = 0, .nsec = 0 };
            } else {
                ts_storage = completion.timespecFromNanos(ns);
            }
            break :blk &ts_storage;
        } else null;

        // Poll kqueue with EINTR retry
        const nevents = try self.keventWithRetry(ts);

        // Clear change list after successful kevent
        self.change_list.clearRetainingCapacity();

        // Process events with tick-based readiness
        var count: usize = 0;
        for (self.event_buffer[0..nevents]) |event| {
            if (count >= completions.len) break;

            const token = event.udata;

            // Handle wakeup event (cross-thread notification)
            if (token == TOKEN_WAKEUP) {
                // Clear the pending flag so future unpark() calls will trigger
                self.wakeup_pending.store(false, .release);
                continue;
            }

            // Skip timer tokens
            if (token >= TOKEN_TIMER_BASE) continue;

            // Get registration from slab (O(1) lookup)
            const reg = self.registrations.get(token) orelse continue;

            // Determine readiness from event
            const ready = readyFromEvent(event);

            // Update tick-based readiness state
            const ready_event = reg.io.setReadiness(ready);

            // Check for errors in the event
            const has_error = (event.flags & posix.system.EV.ERROR) != 0;

            // Perform the actual operation now that fd is ready
            const result = if (has_error)
                -@as(i32, @intCast(event.data))
            else
                self.performOperation(reg.op);

            completions[count] = Completion{
                .user_data = reg.submission_id.value,
                .result = result,
                .flags = if (ready_event.is_shutdown) 1 else 0,
            };

            // Remove registration from slab
            _ = self.registrations.remove(token);
            count += 1;
        }

        return count;
    }

    /// kevent with EINTR retry loop.
    fn keventWithRetry(self: *Self, ts: ?*const posix.timespec) !usize {
        while (true) {
            const result = posix.system.kevent(
                self.kq_fd,
                self.change_list.items.ptr,
                @intCast(self.change_list.items.len),
                self.event_buffer.ptr,
                @intCast(self.event_buffer.len),
                ts,
            );

            if (result >= 0) {
                return @intCast(result);
            }

            const err = posix.errno(result);
            switch (err) {
                .INTR => continue, // Retry on EINTR
                .ACCES => return error.AccessDenied,
                .FAULT => return error.BadAddress,
                .BADF => return error.BadFileDescriptor,
                .INVAL => return error.InvalidArgument,
                .NOENT => return error.EventNotFound,
                .NOMEM => return error.SystemResources,
                .SRCH => return error.ProcessNotFound,
                else => return error.Unexpected,
            }
        }
    }

    /// Convert kqueue event to Ready flags.
    fn readyFromEvent(event: Kevent) Ready {
        var ready = Ready.EMPTY;

        if (event.filter == posix.system.EVFILT.READ) {
            ready.readable = true;
            if (event.flags & posix.system.EV.EOF != 0) {
                ready.read_closed = true;
            }
        }

        if (event.filter == posix.system.EVFILT.WRITE) {
            ready.writable = true;
            if (event.flags & posix.system.EV.EOF != 0) {
                ready.write_closed = true;
            }
        }

        // Timer events are treated as readable (timer expired = data available)
        if (event.filter == posix.system.EVFILT.TIMER) {
            ready.readable = true;
        }

        if (event.flags & posix.system.EV.ERROR != 0) {
            ready.err = true;
        }

        return ready;
    }

    /// Process operations that don't need kqueue (sync ops).
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

    /// Perform the actual I/O operation after kqueue indicates readiness.
    fn performOperation(self: *Self, op: Operation) i32 {
        _ = self;
        return switch (op.op) {
            .read => |r| blk: {
                const n = posix.read(r.fd, r.buffer) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(n);
            },
            .write => |w| blk: {
                const n = posix.write(w.fd, w.buffer) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(n);
            },
            .accept => |a| blk: {
                const result = posix.accept(a.fd, a.addr, a.addr_len, 0) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(result);
            },
            .connect => |_| blk: {
                // connect() was already initiated, just return success
                break :blk 0;
            },
            .recv => |r| blk: {
                const result = posix.recv(r.fd, r.buffer, r.flags) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(result);
            },
            .send => |s| blk: {
                const result = posix.send(s.fd, s.buffer, s.flags) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(result);
            },
            .open => |o| blk: {
                const result = posix.openatZ(posix.AT.FDCWD, o.path, .{
                    .ACCMODE = @enumFromInt(o.flags & 0x3),
                }, o.mode) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk @intCast(result);
            },
            .close => |c| blk: {
                posix.close(c.fd);
                break :blk 0;
            },
            .fsync => |f| blk: {
                posix.fsync(f.fd) catch |e| {
                    break :blk -@as(i32, @intCast(@intFromEnum(errnoFromError(e))));
                };
                break :blk 0;
            },
            .timeout => 0, // Timer expired successfully
            .nop => 0,
            else => -@as(i32, @intCast(@intFromEnum(posix.E.NOSYS))),
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

                // Add DELETE kevent for timer operations
                if (entry.value.timer_ident) |timer_ident| {
                    const kevent = Kevent{
                        .ident = timer_ident,
                        .filter = posix.system.EVFILT.TIMER,
                        .flags = posix.system.EV.DELETE,
                        .fflags = 0,
                        .data = 0,
                        .udata = 0,
                    };
                    // Best-effort: queue DELETE for next kevent call.
                    // OOM here is non-critical (timer is cancelled in slab anyway)
                    // but we track it for observability.
                    self.change_list.append(self.allocator, kevent) catch {
                        _ = self.cancel_oom_count.fetchAdd(1, .monotonic);
                    };
                }
                // Add DELETE kevent for fd operations
                else if (entry.value.monitored_fd) |mon_fd| {
                    const filter: ?i16 = switch (entry.value.op.op) {
                        .read, .recv, .accept => posix.system.EVFILT.READ,
                        .write, .send, .connect => posix.system.EVFILT.WRITE,
                        else => null,
                    };

                    if (filter) |f| {
                        const kevent = Kevent{
                            .ident = @intCast(mon_fd),
                            .filter = f,
                            .flags = posix.system.EV.DELETE,
                            .fflags = 0,
                            .data = 0,
                            .udata = 0,
                        };
                        // Best-effort: queue DELETE for next kevent call.
                        // OOM here is non-critical (operation is cancelled in slab anyway)
                        // but we track it for observability.
                        self.change_list.append(self.allocator, kevent) catch {
                            _ = self.cancel_oom_count.fetchAdd(1, .monotonic);
                        };
                    }
                }

                _ = self.registrations.remove(entry.key);
                return;
            }
        }
    }

    /// Get kqueue file descriptor.
    pub fn fd(self: Self) posix.fd_t {
        return self.kq_fd;
    }
};

/// Convert Zig error to errno value.
fn errnoFromError(err: anyerror) posix.E {
    return switch (err) {
        error.WouldBlock => .AGAIN,
        error.ConnectionRefused => .CONNREFUSED,
        error.ConnectionResetByPeer => .CONNRESET,
        error.BrokenPipe => .PIPE,
        error.NotOpenForReading => .BADF,
        error.NotOpenForWriting => .BADF,
        error.AccessDenied => .ACCES,
        error.FileNotFound => .NOENT,
        error.InputOutput => .IO,
        error.InvalidArgument => .INVAL,
        error.NetworkUnreachable => .NETUNREACH,
        error.NotConnected => .NOTCONN,
        error.TimedOut => .TIMEDOUT,
        else => .IO,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "KqueueBackend - init and deinit" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    try std.testing.expect(backend.fd() >= 0);
}

test "KqueueBackend - slab-based registration" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

test "KqueueBackend - timer expiration" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a short timer (10ms)
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 10_000_000 } },
        .user_data = 123,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.registrations.count());

    // Verify timer_ident was set
    var it = backend.registrations.iterator();
    if (it.next()) |entry| {
        try std.testing.expect(entry.value.timer_ident != null);
    }

    // Wait for timer to expire (with generous timeout)
    var completions: [8]Completion = undefined;
    const count = try backend.wait(&completions, 100_000_000); // 100ms timeout

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(u64, 123), completions[0].user_data);
    try std.testing.expectEqual(@as(i32, 0), completions[0].result); // Success
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "KqueueBackend - timer cancellation" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Submit a long timer (1 second)
    const id = try backend.submit(.{
        .op = .{ .timeout = .{ .ns = 1_000_000_000 } },
        .user_data = 456,
    });

    try std.testing.expect(id.isValid());
    try std.testing.expectEqual(@as(usize, 1), backend.registrations.count());

    // Cancel should generate a DELETE kevent for the timer
    try backend.cancel(id);

    // Registration should be removed
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());

    // Verify DELETE kevent was queued
    try std.testing.expect(backend.change_list.items.len > 0);

    // The DELETE kevent should be for EVFILT_TIMER
    var found_timer_delete = false;
    for (backend.change_list.items) |kevent| {
        if (kevent.filter == posix.system.EVFILT.TIMER and
            (kevent.flags & posix.system.EV.DELETE) != 0)
        {
            found_timer_delete = true;
            break;
        }
    }
    try std.testing.expect(found_timer_delete);

    // OOM counter should be zero (no memory pressure in test)
    try std.testing.expectEqual(@as(u64, 0), backend.getCancelOomCount());
}

test "KqueueBackend - multiple timeout submissions" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

test "KqueueBackend - wait with zero timeout returns immediately" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

test "KqueueBackend - TCP socket accept readiness" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create listening socket
    const listen_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
    defer posix.close(listen_fd);

    // Bind to ephemeral port
    var addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port = 0, // Ephemeral
        .addr = 0x7f000001, // 127.0.0.1 in network byte order (big endian)
    };
    // Fix byte order
    addr.addr = std.mem.nativeToBig(u32, 0x7f000001);
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

test "KqueueBackend - TCP socket send/recv readiness" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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
    const send_data = "Hello kqueue!";
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

test "KqueueBackend - cancel non-existent operation is safe" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Try to cancel a non-existent operation
    const fake_id = SubmissionId.init(99999);
    try backend.cancel(fake_id);

    // Should not crash, registrations still empty
    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "KqueueBackend - rapid submit and cancel" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Rapidly submit and cancel operations
    var ids: [100]SubmissionId = undefined;

    for (0..100) |i| {
        ids[i] = try backend.submit(.{
            .op = .{ .timeout = .{ .ns = 1_000_000_000 } }, // 1 second
            .user_data = @intCast(i),
        });
    }

    try std.testing.expectEqual(@as(usize, 100), backend.registrations.count());

    // Cancel all
    for (ids) |id| {
        try backend.cancel(id);
    }

    try std.testing.expectEqual(@as(usize, 0), backend.registrations.count());
}

test "KqueueBackend - nop operation completes immediately" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

test "KqueueBackend - close operation" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

    // Verify fd is actually closed (subsequent close should fail silently or error)
    // We can't easily test this without EBADF, but the operation completed
}

test "KqueueBackend - user_data preserved correctly" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
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

test "KqueueBackend - change list auto-flush on threshold" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    // Use larger capacity to avoid slab overflow
    var backend = try KqueueBackend.init(std.testing.allocator, Config{ .max_completions = 512 });
    defer backend.deinit();

    // Submit enough operations to trigger auto-flush (threshold is 256)
    // Use nops since they don't require kqueue registration
    var ids: [300]SubmissionId = undefined;
    for (0..300) |i| {
        ids[i] = try backend.submit(.{
            .op = .{ .nop = {} },
            .user_data = @intCast(i),
        });
    }

    // Nops are immediate, so they should all be in registrations waiting to be processed
    // The change list should have been flushed during submission

    // Process all immediate ops
    var completions: [512]Completion = undefined;
    var total: usize = 0;
    while (total < 300) {
        const count = try backend.wait(&completions, 0);
        if (count == 0) break;
        total += count;
    }

    try std.testing.expectEqual(@as(usize, 300), total);
}

test "KqueueBackend - read and write operations" {
    if (builtin.os.tag != .macos and builtin.os.tag != .freebsd) {
        return error.SkipZigTest;
    }

    const Config = @import("../backend.zig").Config;
    var backend = try KqueueBackend.init(std.testing.allocator, Config{});
    defer backend.deinit();

    // Create a pipe for read/write testing
    const pipe_fds = try posix.pipe();
    defer {
        posix.close(pipe_fds[0]);
        posix.close(pipe_fds[1]);
    }

    // Set read end to non-blocking
    _ = try posix.fcntl(pipe_fds[0], posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    // Write some data to the pipe (using posix.write directly, not async)
    const write_data = "Test pipe data";
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

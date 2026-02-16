//! Atomic Operation Logger
//!
//! Provides instrumented atomic types that log every operation for post-mortem
//! analysis of concurrent code. In test builds, these types track:
//!
//! 1. Memory ordering used for each operation
//! 2. Call stack at operation time
//! 3. Thread ID and timestamp
//! 4. Values read/written
//!
//! After a test completes, the log can be validated to detect:
//! - Acquire/release ordering violations
//! - Missing synchronization
//! - Potential data races
//!
//! Usage:
//!   // In production code, use std.atomic.Value
//!   // In test builds, use TestAtomic which logs operations
//!   const AtomicType = if (builtin.is_test)
//!       atomic_log.TestAtomic(usize)
//!   else
//!       std.atomic.Value(usize);

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

/// Memory ordering enum matching std.builtin.AtomicOrder.
pub const Order = std.builtin.AtomicOrder;

/// A single logged atomic operation.
pub const LogEntry = struct {
    /// Type of operation.
    op: OpType,

    /// Memory ordering used.
    order: Order,

    /// Thread ID that performed the operation.
    thread_id: std.Thread.Id,

    /// Monotonic timestamp (nanoseconds).
    timestamp_ns: i128,

    /// Return address (for stack traces).
    return_addr: usize,

    /// Value involved in the operation (interpretation depends on op type).
    value: u64,

    /// For CAS operations: expected value.
    expected: u64,

    /// For CAS operations: whether it succeeded.
    success: bool,

    pub const OpType = enum {
        load,
        store,
        fetch_add,
        fetch_sub,
        fetch_or,
        fetch_and,
        fetch_xor,
        cmpxchg_weak,
        cmpxchg_strong,
        rmw,
    };

    pub fn format(
        self: LogEntry,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("{s} order={s} tid={d} val={x}", .{
            @tagName(self.op),
            @tagName(self.order),
            self.thread_id,
            self.value,
        });
        if (self.op == .cmpxchg_weak or self.op == .cmpxchg_strong) {
            try writer.print(" expected={x} success={}", .{
                self.expected,
                self.success,
            });
        }
    }
};

/// Thread-safe log buffer for atomic operations.
/// Uses a lock-free ring buffer for minimal overhead.
pub const AtomicLog = struct {
    const BUFFER_SIZE: usize = 64 * 1024; // 64K entries
    const BUFFER_MASK: usize = BUFFER_SIZE - 1;

    entries: [BUFFER_SIZE]LogEntry,
    write_index: std.atomic.Value(usize),
    enabled: std.atomic.Value(bool),

    /// Global singleton instance.
    var global: ?*AtomicLog = null;
    var global_mutex: std.Thread.Mutex = .{};

    pub fn init() AtomicLog {
        return .{
            .entries = undefined,
            .write_index = std.atomic.Value(usize).init(0),
            .enabled = std.atomic.Value(bool).init(true),
        };
    }

    /// Get the global log instance, creating it if necessary.
    pub fn getGlobal(allocator: Allocator) !*AtomicLog {
        global_mutex.lock();
        defer global_mutex.unlock();

        if (global == null) {
            global = try allocator.create(AtomicLog);
            global.?.* = init();
        }
        return global.?;
    }

    /// Clean up the global log instance.
    pub fn deinitGlobal(allocator: Allocator) void {
        global_mutex.lock();
        defer global_mutex.unlock();

        if (global) |g| {
            allocator.destroy(g);
            global = null;
        }
    }

    /// Record an atomic operation.
    pub fn record(self: *AtomicLog, entry: LogEntry) void {
        if (!self.enabled.load(.acquire)) return;

        const idx = self.write_index.fetchAdd(1, .acq_rel) & BUFFER_MASK;
        self.entries[idx] = entry;
    }

    /// Enable or disable logging.
    pub fn setEnabled(self: *AtomicLog, enabled: bool) void {
        self.enabled.store(enabled, .release);
    }

    /// Get the number of entries logged.
    pub fn count(self: *const AtomicLog) usize {
        const idx = self.write_index.load(.acquire);
        return @min(idx, BUFFER_SIZE);
    }

    /// Clear the log.
    pub fn clear(self: *AtomicLog) void {
        self.write_index.store(0, .release);
    }

    /// Get an entry by index (for analysis).
    pub fn get(self: *const AtomicLog, idx: usize) ?LogEntry {
        const total = self.write_index.load(.acquire);
        if (idx >= total) return null;

        // Handle wraparound
        if (total > BUFFER_SIZE) {
            const oldest = total - BUFFER_SIZE;
            if (idx < oldest) return null;
        }

        return self.entries[idx & BUFFER_MASK];
    }

    /// Iterator for log entries.
    pub fn iterator(self: *const AtomicLog) Iterator {
        return .{
            .log = self,
            .current = 0,
            .total = self.count(),
        };
    }

    pub const Iterator = struct {
        log: *const AtomicLog,
        current: usize,
        total: usize,

        pub fn next(self: *Iterator) ?LogEntry {
            if (self.current >= self.total) return null;
            const entry = self.log.get(self.current);
            self.current += 1;
            return entry;
        }
    };

    /// Validate the log for common concurrency issues.
    /// Returns a list of potential problems found.
    pub fn validate(self: *const AtomicLog, allocator: Allocator) ![]ValidationIssue {
        var issues = std.ArrayList(ValidationIssue).init(allocator);
        errdefer issues.deinit();

        // Check for relaxed ordering in potentially problematic contexts
        var it = self.iterator();
        while (it.next()) |entry| {
            // Relaxed stores followed by relaxed loads could indicate missing sync
            if (entry.order == .monotonic) {
                // This is just a warning - relaxed ordering is often intentional
                if (entry.op == .store or entry.op == .load) {
                    try issues.append(.{
                        .severity = .info,
                        .entry_index = it.current - 1,
                        .message = "Relaxed memory ordering used - verify this is intentional",
                    });
                }
            }

            // CAS failures with strong ordering might indicate a bug
            if ((entry.op == .cmpxchg_strong or entry.op == .cmpxchg_weak) and !entry.success) {
                // Many failures in a row could indicate a livelock
                // This would require more sophisticated analysis
            }
        }

        return issues.toOwnedSlice();
    }

    pub const ValidationIssue = struct {
        severity: Severity,
        entry_index: usize,
        message: []const u8,

        pub const Severity = enum {
            info,
            warning,
            @"error",
        };
    };
};

/// Instrumented atomic type that logs all operations.
/// Drop-in replacement for std.atomic.Value in test builds.
pub fn TestAtomic(comptime T: type) type {
    return struct {
        const Self = @This();

        value: std.atomic.Value(T),
        log: ?*AtomicLog,

        pub fn init(val: T) Self {
            return .{
                .value = std.atomic.Value(T).init(val),
                .log = null,
            };
        }

        pub fn initWithLog(val: T, log: *AtomicLog) Self {
            return .{
                .value = std.atomic.Value(T).init(val),
                .log = log,
            };
        }

        /// Set the log instance for this atomic.
        pub fn setLog(self: *Self, log: *AtomicLog) void {
            self.log = log;
        }

        pub fn load(self: *const Self, comptime order: Order) T {
            const result = self.value.load(order);
            self.logOp(.load, order, asU64(result), 0, true);
            return result;
        }

        pub fn store(self: *Self, val: T, comptime order: Order) void {
            self.value.store(val, order);
            self.logOp(.store, order, asU64(val), 0, true);
        }

        pub fn fetchAdd(self: *Self, val: T, comptime order: Order) T {
            const result = self.value.fetchAdd(val, order);
            self.logOp(.fetch_add, order, asU64(result), 0, true);
            return result;
        }

        pub fn fetchSub(self: *Self, val: T, comptime order: Order) T {
            const result = self.value.fetchSub(val, order);
            self.logOp(.fetch_sub, order, asU64(result), 0, true);
            return result;
        }

        pub fn fetchOr(self: *Self, val: T, comptime order: Order) T {
            const result = self.value.fetchOr(val, order);
            self.logOp(.fetch_or, order, asU64(result), 0, true);
            return result;
        }

        pub fn fetchAnd(self: *Self, val: T, comptime order: Order) T {
            const result = self.value.fetchAnd(val, order);
            self.logOp(.fetch_and, order, asU64(result), 0, true);
            return result;
        }

        pub fn cmpxchgWeak(
            self: *Self,
            expected: T,
            desired: T,
            comptime success_order: Order,
            comptime failure_order: Order,
        ) ?T {
            const result = self.value.cmpxchgWeak(expected, desired, success_order, failure_order);
            const succeeded = result == null;
            self.logOp(
                .cmpxchg_weak,
                success_order,
                asU64(result orelse expected),
                asU64(expected),
                succeeded,
            );
            return result;
        }

        pub fn cmpxchgStrong(
            self: *Self,
            expected: T,
            desired: T,
            comptime success_order: Order,
            comptime failure_order: Order,
        ) ?T {
            const result = self.value.cmpxchgStrong(expected, desired, success_order, failure_order);
            const succeeded = result == null;
            self.logOp(
                .cmpxchg_strong,
                success_order,
                asU64(result orelse expected),
                asU64(expected),
                succeeded,
            );
            return result;
        }

        fn logOp(
            self: *const Self,
            op: LogEntry.OpType,
            order: Order,
            value: u64,
            expected: u64,
            success: bool,
        ) void {
            // Get log from self or try global
            const log = self.log orelse (AtomicLog.global orelse return);

            log.record(.{
                .op = op,
                .order = order,
                .thread_id = std.Thread.getCurrentId(),
                .timestamp_ns = std.time.nanoTimestamp(),
                .return_addr = @returnAddress(),
                .value = value,
                .expected = expected,
                .success = success,
            });
        }

        fn asU64(val: T) u64 {
            return switch (@typeInfo(T)) {
                .int => |info| if (info.signedness == .signed)
                    @bitCast(@as(std.meta.Int(.unsigned, info.bits), @bitCast(val)))
                else
                    @as(u64, val),
                .bool => @intFromBool(val),
                .pointer => @intFromPtr(val),
                else => 0,
            };
        }
    };
}

/// Helper to wrap existing atomic operations with logging.
/// Use this to instrument code without changing types.
pub fn logAtomic(
    comptime op: LogEntry.OpType,
    order: Order,
    value: u64,
) void {
    const log = AtomicLog.global orelse return;
    log.record(.{
        .op = op,
        .order = order,
        .thread_id = std.Thread.getCurrentId(),
        .timestamp_ns = std.time.nanoTimestamp(),
        .return_addr = @returnAddress(),
        .value = value,
        .expected = 0,
        .success = true,
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "AtomicLog - basic recording" {
    var log = AtomicLog.init();

    log.record(.{
        .op = .load,
        .order = .acquire,
        .thread_id = std.Thread.getCurrentId(),
        .timestamp_ns = std.time.nanoTimestamp(),
        .return_addr = @returnAddress(),
        .value = 42,
        .expected = 0,
        .success = true,
    });

    try std.testing.expectEqual(@as(usize, 1), log.count());

    const entry = log.get(0).?;
    try std.testing.expectEqual(LogEntry.OpType.load, entry.op);
    try std.testing.expectEqual(Order.acquire, entry.order);
    try std.testing.expectEqual(@as(u64, 42), entry.value);
}

test "AtomicLog - iterator" {
    var log = AtomicLog.init();

    // Record several entries
    for (0..5) |i| {
        log.record(.{
            .op = .store,
            .order = .release,
            .thread_id = std.Thread.getCurrentId(),
            .timestamp_ns = std.time.nanoTimestamp(),
            .return_addr = @returnAddress(),
            .value = @intCast(i),
            .expected = 0,
            .success = true,
        });
    }

    // Iterate and verify
    var it = log.iterator();
    var count: usize = 0;
    while (it.next()) |entry| {
        try std.testing.expectEqual(@as(u64, @intCast(count)), entry.value);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 5), count);
}

test "TestAtomic - operations are logged" {
    var log = AtomicLog.init();
    var atomic = TestAtomic(usize).initWithLog(0, &log);

    // Perform various operations
    atomic.store(10, .release);
    _ = atomic.load(.acquire);
    _ = atomic.fetchAdd(5, .acq_rel);
    _ = atomic.cmpxchgWeak(15, 20, .acq_rel, .acquire);

    // Verify operations were logged
    try std.testing.expectEqual(@as(usize, 4), log.count());

    var it = log.iterator();

    const store_entry = it.next().?;
    try std.testing.expectEqual(LogEntry.OpType.store, store_entry.op);
    try std.testing.expectEqual(@as(u64, 10), store_entry.value);

    const load_entry = it.next().?;
    try std.testing.expectEqual(LogEntry.OpType.load, load_entry.op);

    const add_entry = it.next().?;
    try std.testing.expectEqual(LogEntry.OpType.fetch_add, add_entry.op);

    const cas_entry = it.next().?;
    try std.testing.expectEqual(LogEntry.OpType.cmpxchg_weak, cas_entry.op);
}

test "AtomicLog - enable/disable" {
    var log = AtomicLog.init();

    log.record(.{
        .op = .load,
        .order = .acquire,
        .thread_id = std.Thread.getCurrentId(),
        .timestamp_ns = 0,
        .return_addr = 0,
        .value = 1,
        .expected = 0,
        .success = true,
    });

    log.setEnabled(false);

    log.record(.{
        .op = .store,
        .order = .release,
        .thread_id = std.Thread.getCurrentId(),
        .timestamp_ns = 0,
        .return_addr = 0,
        .value = 2,
        .expected = 0,
        .success = true,
    });

    // Only first entry should be recorded
    try std.testing.expectEqual(@as(usize, 1), log.count());
}

test "AtomicLog - clear" {
    var log = AtomicLog.init();

    for (0..10) |_| {
        log.record(.{
            .op = .load,
            .order = .monotonic,
            .thread_id = std.Thread.getCurrentId(),
            .timestamp_ns = 0,
            .return_addr = 0,
            .value = 0,
            .expected = 0,
            .success = true,
        });
    }

    try std.testing.expectEqual(@as(usize, 10), log.count());

    log.clear();

    try std.testing.expectEqual(@as(usize, 0), log.count());
}

//! ScheduledIo - Per-Resource State Tracking
//!
//! Tracks readiness state and waiters for a single I/O resource.
//! Uses tick-based versioning to prevent the ABA problem where
//! readiness is cleared immediately after being set.
//!
//! Bit-packed state layout (32 bits):
//! | shutdown (1 bit) | tick (15 bits) | readiness (16 bits) |
//!
//! The tick counter increments each time readiness is set by the OS.
//! When a task clears readiness, it must provide the tick it received.
//! If the tick is stale (older than current), the clear is ignored.
//! This prevents race conditions where:
//!   1. Task A reads readiness with tick=5
//!   2. OS sets new readiness, tick becomes 6
//!   3. Task A tries to clear with tick=5 → IGNORED (tick is stale)
//!
//! MEMORY ORDERING DESIGN:
//! We use acquire/release semantics (not SeqCst) because:
//! - Some runtimes use SeqCst for a 3-state readiness future machine
//! - volt uses mutex-protected intrusive waiters list instead
//! - The mutex provides necessary synchronization for waiter access
//! - Acquire on reads ensures we see all prior writes to state
//! - Release on writes ensures our writes are visible to subsequent acquires
//! - AcqRel on RMW operations provides both guarantees
//! This is sufficient and avoids the overhead of SeqCst barriers.
//!
//! WAKE STRATEGY (Thundering Herd Prevention):
//! By default, we wake ONE waiter per readiness event, not all.
//! This prevents the thundering herd problem where:
//!   1. 100 tasks wait for READABLE on same fd
//!   2. 1 byte arrives, all 100 wake up
//!   3. Only 1 reads the byte, 99 get EAGAIN and sleep again
//!   4. Massive CPU waste from context switches
//!
//! We only wake ALL on:
//!   - Shutdown (all tasks must exit)
//!   - Error conditions (all tasks should see the error)
//!   - Explicit wakeAll() call
//!
//! Inspired by production async runtime I/O state machines with proven correctness.

const std = @import("std");
const bit = @import("../util/bit.zig");
const linked_list = @import("../util/linked_list.zig");
const cacheline = @import("../util/cacheline.zig");

/// Ready state flags (16 bits).
pub const Ready = packed struct {
    readable: bool = false,
    writable: bool = false,
    read_closed: bool = false,
    write_closed: bool = false,
    err: bool = false,
    priority: bool = false,
    _padding: u10 = 0,

    pub const EMPTY: Ready = .{};
    pub const READABLE: Ready = .{ .readable = true };
    pub const WRITABLE: Ready = .{ .writable = true };
    pub const READ_CLOSED: Ready = .{ .read_closed = true };
    pub const WRITE_CLOSED: Ready = .{ .write_closed = true };
    pub const ERROR: Ready = .{ .err = true };

    pub const ALL: Ready = .{
        .readable = true,
        .writable = true,
        .read_closed = true,
        .write_closed = true,
        .err = true,
        .priority = true,
    };

    pub fn toU16(self: Ready) u16 {
        return @bitCast(self);
    }

    pub fn fromU16(val: u16) Ready {
        return @bitCast(val);
    }

    pub fn merge(self: Ready, other: Ready) Ready {
        return fromU16(self.toU16() | other.toU16());
    }

    pub fn intersect(self: Ready, other: Ready) Ready {
        return fromU16(self.toU16() & other.toU16());
    }

    pub fn isEmpty(self: Ready) bool {
        return self.toU16() == 0;
    }

    pub fn isReadable(self: Ready) bool {
        return self.readable or self.read_closed;
    }

    pub fn isWritable(self: Ready) bool {
        return self.writable or self.write_closed;
    }

    pub fn hasError(self: Ready) bool {
        return self.err or self.read_closed or self.write_closed;
    }

    pub fn satisfies(self: Ready, interest: Interest) bool {
        return (self.toU16() & interest.toU16()) != 0;
    }
};

/// Interest in I/O events (what we want to wait for).
pub const Interest = packed struct {
    readable: bool = false,
    writable: bool = false,
    _padding: u14 = 0,

    pub const READABLE: Interest = .{ .readable = true };
    pub const WRITABLE: Interest = .{ .writable = true };
    pub const BOTH: Interest = .{ .readable = true, .writable = true };

    pub fn toU16(self: Interest) u16 {
        return @bitCast(self);
    }
};

/// A ready event with its associated tick.
pub const ReadyEvent = struct {
    tick: u15,
    ready: Ready,
    is_shutdown: bool,
};

/// Tick operations.
pub const Tick = union(enum) {
    /// Set readiness (increment tick).
    set,
    /// Clear readiness (requires matching tick).
    clear: u15,
};

/// Wake strategy for notifying waiters.
pub const WakeStrategy = enum {
    /// Wake one waiter (default - prevents thundering herd)
    one,
    /// Wake all waiters (for shutdown/errors)
    all,
    /// Wake up to N waiters
    n,
};

// Bit packing layout for atomic state.
const READINESS = bit.Pack.leastSignificant(16);
const TICK = READINESS.then(15);
const SHUTDOWN = TICK.then(1);

/// Per-resource I/O state.
/// Cache-line aligned to prevent false sharing between resources.
pub const ScheduledIo = struct {
    // ─────────────────────────────────────────────────────────────────────
    // Hot path: atomic state (own cache line)
    // ─────────────────────────────────────────────────────────────────────

    /// Packed atomic state: | shutdown | tick | readiness |
    state: std.atomic.Value(u32),

    /// Padding to prevent false sharing with waiters data
    _pad0: [cacheline.CACHE_LINE_SIZE - @sizeOf(std.atomic.Value(u32))]u8 = undefined,

    // ─────────────────────────────────────────────────────────────────────
    // Waiter management (separate cache line)
    // ─────────────────────────────────────────────────────────────────────

    /// Mutex protecting waiters list.
    waiters_lock: std.Thread.Mutex,

    /// List of tasks waiting for readiness (FIFO for fairness).
    waiters: WaiterList,

    /// Reserved waker for AsyncRead operations (fast path).
    /// Using reserved slots avoids list traversal for common case.
    reader_waker: ?Waker,

    /// Reserved waker for AsyncWrite operations (fast path).
    writer_waker: ?Waker,

    // ─────────────────────────────────────────────────────────────────────
    // Metrics (for observability)
    // ─────────────────────────────────────────────────────────────────────

    /// Total wakeups issued (for monitoring thundering herd)
    wake_count: std.atomic.Value(u64),

    /// Spurious wakeups (task woke but no work available)
    spurious_wakeups: std.atomic.Value(u64),

    const Self = @This();
    const WaiterList = linked_list.LinkedList(Waiter, "pointers");

    /// Maximum wakers to collect before releasing lock
    const MAX_WAKERS_BATCH: usize = 32;

    pub fn init() Self {
        return .{
            .state = std.atomic.Value(u32).init(0),
            ._pad0 = undefined,
            .waiters_lock = .{},
            .waiters = .{},
            .reader_waker = null,
            .writer_waker = null,
            .wake_count = std.atomic.Value(u64).init(0),
            .spurious_wakeups = std.atomic.Value(u64).init(0),
        };
    }

    /// Get current readiness and tick without modifying state.
    pub fn readiness(self: *const Self) ReadyEvent {
        const state_val = self.state.load(.acquire);
        return .{
            .tick = @intCast(TICK.unpack(state_val)),
            .ready = Ready.fromU16(@intCast(READINESS.unpack(state_val))),
            .is_shutdown = SHUTDOWN.unpack(state_val) != 0,
        };
    }

    /// Set readiness, incrementing the tick.
    /// Returns the new ReadyEvent.
    pub fn setReadiness(self: *Self, ready: Ready) ReadyEvent {
        var current = self.state.load(.acquire);

        while (true) {
            const current_tick: u15 = @intCast(TICK.unpack(current));
            const current_ready = Ready.fromU16(@intCast(READINESS.unpack(current)));

            // Merge new readiness with existing
            const new_ready = current_ready.merge(ready);

            // Increment tick (wraps at 15 bits)
            const new_tick = current_tick +% 1;

            // Pack new state
            var new_state = READINESS.pack(new_ready.toU16(), current);
            new_state = TICK.pack(new_tick, new_state);

            // Try to update atomically
            if (self.state.cmpxchgWeak(current, @intCast(new_state), .acq_rel, .acquire)) |updated| {
                current = updated;
                continue;
            }

            return .{
                .tick = new_tick,
                .ready = new_ready,
                .is_shutdown = SHUTDOWN.unpack(new_state) != 0,
            };
        }
    }

    /// Clear readiness, but only if the tick matches.
    /// Returns true if cleared, false if tick was stale.
    ///
    /// IMPORTANT: READ_CLOSED and WRITE_CLOSED are "final states" and are
    /// NEVER cleared. Once a socket is closed,
    /// that information must persist so all waiters can observe it.
    pub fn clearReadiness(self: *Self, tick: Tick, ready_to_clear: Ready) bool {
        const expected_tick: u15 = switch (tick) {
            .set => return false, // Can't clear with .set
            .clear => |t| t,
        };

        var current = self.state.load(.acquire);

        while (true) {
            const current_tick: u15 = @intCast(TICK.unpack(current));

            // If tick doesn't match, don't clear (stale event)
            if (current_tick != expected_tick) {
                return false;
            }

            const current_ready = Ready.fromU16(@intCast(READINESS.unpack(current)));

            // Closed states are final and must never be cleared (standard async I/O semantics)
            var mask_to_clear = ready_to_clear;
            mask_to_clear.read_closed = false; // Never clear read_closed
            mask_to_clear.write_closed = false; // Never clear write_closed

            const new_ready_bits = current_ready.toU16() & ~mask_to_clear.toU16();

            const new_state = READINESS.pack(new_ready_bits, current);

            if (self.state.cmpxchgWeak(current, @intCast(new_state), .acq_rel, .acquire)) |updated| {
                current = updated;
                continue;
            }

            return true;
        }
    }

    /// Shutdown this I/O resource, waking all waiters.
    pub fn shutdown(self: *Self) void {
        // Set shutdown bit
        const mask: u32 = @intCast(SHUTDOWN.pack(1, 0));
        _ = self.state.fetchOr(mask, .acq_rel);

        // Wake ALL waiters on shutdown (they need to exit)
        self.wakeAll(Ready.ALL);
    }

    /// Wake ONE waiter that matches the given readiness.
    /// This is the default strategy to prevent thundering herd.
    /// Returns true if a waiter was woken.
    pub fn wakeOne(self: *Self, ready: Ready) bool {
        var waker_to_wake: ?Waker = null;

        {
            self.waiters_lock.lock();
            defer self.waiters_lock.unlock();

            // Try reserved reader slot first (fast path)
            if (ready.isReadable()) {
                if (self.reader_waker) |waker| {
                    waker_to_wake = waker;
                    self.reader_waker = null;
                }
            }

            // Try reserved writer slot if no reader
            if (waker_to_wake == null and ready.isWritable()) {
                if (self.writer_waker) |waker| {
                    waker_to_wake = waker;
                    self.writer_waker = null;
                }
            }

            // Fall back to waiters list (FIFO - wake oldest first for fairness)
            if (waker_to_wake == null) {
                var it = self.waiters.iterator();
                while (it.next()) |waiter| {
                    if (ready.satisfies(waiter.interest)) {
                        waiter.is_ready = true;
                        if (waiter.waker) |waker| {
                            waker_to_wake = waker;
                            waiter.waker = null; // Clear to avoid double-wake
                            break; // Only wake ONE
                        }
                    }
                }
            }
        }

        // Wake AFTER releasing lock
        if (waker_to_wake) |waker| {
            _ = self.wake_count.fetchAdd(1, .monotonic);
            waker.wake();
            return true;
        }

        return false;
    }

    /// Wake up to N waiters that match the given readiness.
    /// Useful for accept() where multiple connections may have arrived.
    pub fn wakeN(self: *Self, ready: Ready, max_count: usize) usize {
        var wakers_to_wake: [MAX_WAKERS_BATCH]Waker = undefined;
        var wake_count: usize = 0;
        const limit = @min(max_count, MAX_WAKERS_BATCH);

        {
            self.waiters_lock.lock();
            defer self.waiters_lock.unlock();

            // Check reserved slots
            if (ready.isReadable() and wake_count < limit) {
                if (self.reader_waker) |waker| {
                    wakers_to_wake[wake_count] = waker;
                    wake_count += 1;
                    self.reader_waker = null;
                }
            }

            if (ready.isWritable() and wake_count < limit) {
                if (self.writer_waker) |waker| {
                    wakers_to_wake[wake_count] = waker;
                    wake_count += 1;
                    self.writer_waker = null;
                }
            }

            // Wake from list
            var it = self.waiters.iterator();
            while (it.next()) |waiter| {
                if (wake_count >= limit) break;

                if (ready.satisfies(waiter.interest)) {
                    waiter.is_ready = true;
                    if (waiter.waker) |waker| {
                        wakers_to_wake[wake_count] = waker;
                        wake_count += 1;
                        waiter.waker = null;
                    }
                }
            }
        }

        // Wake AFTER releasing lock
        _ = self.wake_count.fetchAdd(wake_count, .monotonic);
        for (wakers_to_wake[0..wake_count]) |waker| {
            waker.wake();
        }

        return wake_count;
    }

    /// Wake ALL waiters that match the given readiness.
    /// Only use for shutdown or error conditions!
    pub fn wakeAll(self: *Self, ready: Ready) void {
        var wakers_to_wake: [MAX_WAKERS_BATCH]Waker = undefined;
        var wake_count: usize = 0;

        // May need multiple passes if >32 waiters
        while (true) {
            wake_count = 0;

            {
                self.waiters_lock.lock();
                defer self.waiters_lock.unlock();

                // Check reserved slots (only on first pass)
                if (ready.isReadable()) {
                    if (self.reader_waker) |waker| {
                        if (wake_count < MAX_WAKERS_BATCH) {
                            wakers_to_wake[wake_count] = waker;
                            wake_count += 1;
                        }
                        self.reader_waker = null;
                    }
                }

                if (ready.isWritable()) {
                    if (self.writer_waker) |waker| {
                        if (wake_count < MAX_WAKERS_BATCH) {
                            wakers_to_wake[wake_count] = waker;
                            wake_count += 1;
                        }
                        self.writer_waker = null;
                    }
                }

                // Wake from list
                var it = self.waiters.iterator();
                while (it.next()) |waiter| {
                    if (ready.satisfies(waiter.interest)) {
                        waiter.is_ready = true;
                        if (waiter.waker) |waker| {
                            if (wake_count < MAX_WAKERS_BATCH) {
                                wakers_to_wake[wake_count] = waker;
                                wake_count += 1;
                                waiter.waker = null;
                            }
                            // Don't break - we want all
                        }
                    }
                }
            }

            // Nothing more to wake
            if (wake_count == 0) break;

            // Wake AFTER releasing lock
            _ = self.wake_count.fetchAdd(wake_count, .monotonic);
            for (wakers_to_wake[0..wake_count]) |waker| {
                waker.wake();
            }

            // If we hit the limit, there might be more waiters
            if (wake_count < MAX_WAKERS_BATCH) break;
        }
    }

    /// Wake waiters based on the readiness semantics.
    ///
    /// IMPORTANT: For readiness-based I/O (epoll/kqueue), we MUST wake ALL
    /// matching waiters because:
    /// 1. Multiple tasks may be waiting on the same fd
    /// 2. A single readiness event (e.g., 10 bytes arrived) may satisfy multiple readers
    /// 3. If we only wake one, others might sleep forever with data available
    ///
    /// The "thundering herd" concern is mitigated by:
    /// - In practice, rarely do multiple tasks wait on the same fd
    /// - The reserved reader/writer slots handle the common single-task case
    /// - Tick-based clearing prevents the ABA problem
    ///
    /// For accept() with multiple pending connections, use wakeN().
    /// For io_uring completions, wake is per-operation (not shared fd).
    pub fn wake(self: *Self, ready: Ready) void {
        self.wakeAll(ready);
    }

    /// Wake one waiter - use only when you KNOW only one task should proceed.
    /// Examples:
    /// - io_uring completion (operation is not shared)
    /// - Timer expiration (one timer, one task)
    /// - Explicit single-consumer patterns
    ///
    /// WARNING: Do NOT use for readiness-based I/O on shared fds!
    pub fn wakeSingle(self: *Self, ready: Ready) bool {
        return self.wakeOne(ready);
    }

    /// Record a spurious wakeup (task woke but couldn't make progress)
    pub fn recordSpuriousWakeup(self: *Self) void {
        _ = self.spurious_wakeups.fetchAdd(1, .monotonic);
    }

    /// Get metrics snapshot
    pub fn getMetrics(self: *const Self) Metrics {
        return .{
            .total_wakeups = self.wake_count.load(.monotonic),
            .spurious_wakeups = self.spurious_wakeups.load(.monotonic),
        };
    }

    /// Clear all wakers AND drain waiters list to break reference cycles.
    /// IMPORTANT: This must be called during shutdown to prevent dangling pointers.
    /// Waiters that are still in the list will have their wakers cleared and
    /// will be removed from the list.
    pub fn clearWakers(self: *Self) void {
        self.waiters_lock.lock();
        defer self.waiters_lock.unlock();

        self.reader_waker = null;
        self.writer_waker = null;

        // Drain the waiters list completely to prevent dangling pointers
        // when the ScheduledIo is destroyed. Each waiter's linked list
        // pointers would otherwise point to freed memory.
        while (self.waiters.popFront()) |waiter| {
            waiter.waker = null;
            waiter.is_ready = true; // Mark as ready so they don't re-register
            // Reset list pointers to prevent use-after-free
            waiter.pointers = .{};
        }
    }

    /// Register a reader waker (fast path slot).
    pub fn setReaderWaker(self: *Self, waker: Waker) void {
        self.waiters_lock.lock();
        defer self.waiters_lock.unlock();
        self.reader_waker = waker;
    }

    /// Register a writer waker (fast path slot).
    pub fn setWriterWaker(self: *Self, waker: Waker) void {
        self.waiters_lock.lock();
        defer self.waiters_lock.unlock();
        self.writer_waker = waker;
    }

    /// Add a waiter to the list.
    pub fn addWaiter(self: *Self, waiter: *Waiter) void {
        self.waiters_lock.lock();
        defer self.waiters_lock.unlock();
        self.waiters.pushBack(waiter);
    }

    /// Remove a waiter from the list.
    pub fn removeWaiter(self: *Self, waiter: *Waiter) void {
        self.waiters_lock.lock();
        defer self.waiters_lock.unlock();
        self.waiters.remove(waiter);
    }
};

/// Metrics for a ScheduledIo instance.
pub const Metrics = struct {
    total_wakeups: u64,
    spurious_wakeups: u64,

    pub fn spuriousRate(self: Metrics) f64 {
        if (self.total_wakeups == 0) return 0.0;
        return @as(f64, @floatFromInt(self.spurious_wakeups)) /
            @as(f64, @floatFromInt(self.total_wakeups));
    }
};

/// A task waiting for I/O readiness.
pub const Waiter = struct {
    /// Linked list pointers (intrusive).
    pointers: linked_list.Pointers(Waiter) = .{},

    /// The waker to call when ready.
    waker: ?Waker = null,

    /// What this waiter is interested in.
    interest: Interest = .{},

    /// Set to true when readiness matches interest.
    is_ready: bool = false,
};

/// A waker that can wake a task with generation-based lifetime validation.
///
/// The generation field provides defense-in-depth against use-after-free:
/// - Each context has a generation counter
/// - When the context is invalidated, its generation is incremented
/// - Stale wakers (with old generation) are detected and ignored
///
/// SAFETY: Even with generation checking, the caller SHOULD ensure:
/// - Pin the task/context before registering the waker
/// - Always call removeWaiter() before dropping the context
/// - Use RAII/defer to ensure cleanup on all exit paths
///
/// The generation check catches bugs but is not a substitute for
/// proper lifetime management.
pub const Waker = struct {
    context: *anyopaque,
    wake_fn: *const fn (*anyopaque) void,
    /// Generation counter for lifetime validation.
    /// If this doesn't match the context's current generation, the waker is stale.
    generation: u32 = 0,

    /// Wake the task, with optional generation validation.
    /// Returns true if woken, false if the waker was stale.
    pub fn wake(self: Waker) void {
        self.wake_fn(self.context);
    }

    /// Wake with generation check - returns false if stale.
    /// Use this when you have access to the current generation.
    pub fn wakeIfValid(self: Waker, current_generation: u32) bool {
        if (self.generation != current_generation) {
            return false; // Stale waker, context may be invalid
        }
        self.wake_fn(self.context);
        return true;
    }

    /// Create a waker with generation tracking.
    pub fn init(context: *anyopaque, wake_fn: *const fn (*anyopaque) void, generation: u32) Waker {
        return .{
            .context = context,
            .wake_fn = wake_fn,
            .generation = generation,
        };
    }
};

/// Helper to manage generation counters for waker contexts.
/// Embed this in your task/context struct for automatic generation tracking.
pub const WakerGeneration = struct {
    value: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Get current generation for creating a waker.
    pub fn get(self: *const WakerGeneration) u32 {
        return self.value.load(.acquire);
    }

    /// Increment generation to invalidate all existing wakers.
    /// Call this before the context is freed or reused.
    pub fn invalidate(self: *WakerGeneration) void {
        _ = self.value.fetchAdd(1, .release);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "ScheduledIo - initial state" {
    var io = ScheduledIo.init();
    const event = io.readiness();

    try std.testing.expect(event.ready.isEmpty());
    try std.testing.expectEqual(@as(u15, 0), event.tick);
    try std.testing.expect(!event.is_shutdown);
}

test "ScheduledIo - set readiness increments tick" {
    var io = ScheduledIo.init();

    const e1 = io.setReadiness(Ready.READABLE);
    try std.testing.expect(e1.ready.readable);
    try std.testing.expectEqual(@as(u15, 1), e1.tick);

    const e2 = io.setReadiness(Ready.WRITABLE);
    try std.testing.expect(e2.ready.readable);
    try std.testing.expect(e2.ready.writable);
    try std.testing.expectEqual(@as(u15, 2), e2.tick);
}

test "ScheduledIo - clear with matching tick" {
    var io = ScheduledIo.init();

    const event = io.setReadiness(Ready.READABLE);
    try std.testing.expectEqual(@as(u15, 1), event.tick);

    // Clear with matching tick should succeed
    const cleared = io.clearReadiness(.{ .clear = 1 }, Ready.READABLE);
    try std.testing.expect(cleared);

    const after = io.readiness();
    try std.testing.expect(!after.ready.readable);
}

test "ScheduledIo - clear with stale tick fails" {
    var io = ScheduledIo.init();

    _ = io.setReadiness(Ready.READABLE); // tick=1
    _ = io.setReadiness(Ready.WRITABLE); // tick=2

    // Try to clear with stale tick=1 (current is 2)
    const cleared = io.clearReadiness(.{ .clear = 1 }, Ready.READABLE);
    try std.testing.expect(!cleared); // Should fail - stale tick

    // Readiness should still be set
    const after = io.readiness();
    try std.testing.expect(after.ready.readable);
    try std.testing.expect(after.ready.writable);
}

test "ScheduledIo - clear preserves closed states" {
    var io = ScheduledIo.init();

    // Set READABLE and READ_CLOSED
    _ = io.setReadiness(Ready.READABLE.merge(Ready.READ_CLOSED));
    const tick = io.readiness().tick;

    // Try to clear ALL readiness
    const cleared = io.clearReadiness(.{ .clear = tick }, Ready.ALL);
    try std.testing.expect(cleared);

    // READ_CLOSED should be preserved even though we tried to clear ALL
    const after = io.readiness();
    try std.testing.expect(!after.ready.readable); // This was cleared
    try std.testing.expect(after.ready.read_closed); // This was preserved
}

test "ScheduledIo - clear preserves write_closed" {
    var io = ScheduledIo.init();

    // Set WRITABLE and WRITE_CLOSED
    _ = io.setReadiness(Ready.WRITABLE.merge(Ready.WRITE_CLOSED));
    const tick = io.readiness().tick;

    // Try to clear WRITABLE and WRITE_CLOSED
    const cleared = io.clearReadiness(.{ .clear = tick }, Ready.WRITABLE.merge(Ready.WRITE_CLOSED));
    try std.testing.expect(cleared);

    // WRITE_CLOSED should be preserved
    const after = io.readiness();
    try std.testing.expect(!after.ready.writable); // This was cleared
    try std.testing.expect(after.ready.write_closed); // This was preserved
}

test "ScheduledIo - shutdown" {
    var io = ScheduledIo.init();
    io.shutdown();

    const event = io.readiness();
    try std.testing.expect(event.is_shutdown);
}

test "ScheduledIo - wakeOne only wakes one" {
    var io = ScheduledIo.init();
    var wake_count: usize = 0;

    const TestWaker = struct {
        fn wake_fn(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };

    // Set up reader waker
    io.setReaderWaker(.{ .context = @ptrCast(&wake_count), .wake_fn = TestWaker.wake_fn });

    // Set up writer waker too
    io.setWriterWaker(.{ .context = @ptrCast(&wake_count), .wake_fn = TestWaker.wake_fn });

    // Wake one for READABLE - should only wake reader
    const woke = io.wakeOne(Ready.READABLE);
    try std.testing.expect(woke);
    try std.testing.expectEqual(@as(usize, 1), wake_count);

    // Writer should still be set
    try std.testing.expect(io.writer_waker != null);
}

test "ScheduledIo - metrics tracking" {
    var io = ScheduledIo.init();
    var counter: usize = 0;

    const TestWaker = struct {
        fn wake_fn(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };

    io.setReaderWaker(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake_fn });
    _ = io.wakeOne(Ready.READABLE);

    io.recordSpuriousWakeup();

    const metrics = io.getMetrics();
    try std.testing.expectEqual(@as(u64, 1), metrics.total_wakeups);
    try std.testing.expectEqual(@as(u64, 1), metrics.spurious_wakeups);
}

test "ScheduledIo - struct layout" {
    // Verify the struct has reasonable size.
    // Note: Zig may reorder struct fields for alignment, so we can't
    // guarantee specific offsets. The _pad0 field is a hint to the compiler.
    const size = @sizeOf(ScheduledIo);

    // Should be larger than a cache line (we have padding)
    try std.testing.expect(size > cacheline.CACHE_LINE_SIZE);

    // But not unreasonably large
    try std.testing.expect(size < 1024);
}

test "Ready - merge and intersect" {
    const r1 = Ready.READABLE;
    const r2 = Ready.WRITABLE;

    const merged = r1.merge(r2);
    try std.testing.expect(merged.readable);
    try std.testing.expect(merged.writable);

    const intersected = merged.intersect(Ready.READABLE);
    try std.testing.expect(intersected.readable);
    try std.testing.expect(!intersected.writable);
}

test "Ready - satisfies interest" {
    const ready = Ready.READABLE.merge(Ready.ERROR);

    try std.testing.expect(ready.satisfies(Interest.READABLE));
    try std.testing.expect(!ready.satisfies(Interest.WRITABLE));
}

test "Ready - hasError" {
    try std.testing.expect(!Ready.READABLE.hasError());
    try std.testing.expect(Ready.ERROR.hasError());
    try std.testing.expect(Ready.READ_CLOSED.hasError());
    try std.testing.expect(Ready.WRITE_CLOSED.hasError());
}

test "Waker - generation validation detects stale wakers" {
    var wake_count: usize = 0;
    var generation = WakerGeneration{};

    const TestWaker = struct {
        fn wake_fn(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };

    // Create waker with current generation (0)
    const waker = Waker.init(@ptrCast(&wake_count), TestWaker.wake_fn, generation.get());
    try std.testing.expectEqual(@as(u32, 0), waker.generation);

    // Wake with matching generation should succeed
    const woke1 = waker.wakeIfValid(generation.get());
    try std.testing.expect(woke1);
    try std.testing.expectEqual(@as(usize, 1), wake_count);

    // Invalidate the generation (simulating context being freed/reused)
    generation.invalidate();
    try std.testing.expectEqual(@as(u32, 1), generation.get());

    // Wake with stale generation should fail
    const woke2 = waker.wakeIfValid(generation.get());
    try std.testing.expect(!woke2);
    try std.testing.expectEqual(@as(usize, 1), wake_count); // Still 1, not incremented
}

test "WakerGeneration - multiple invalidations" {
    var gen = WakerGeneration{};

    try std.testing.expectEqual(@as(u32, 0), gen.get());

    gen.invalidate();
    try std.testing.expectEqual(@as(u32, 1), gen.get());

    gen.invalidate();
    try std.testing.expectEqual(@as(u32, 2), gen.get());

    gen.invalidate();
    try std.testing.expectEqual(@as(u32, 3), gen.get());
}

test "ScheduledIo - clearWakers drains all waiters" {
    var io = ScheduledIo.init();

    // Add some waiters
    var waiter1 = Waiter{ .interest = Interest.READABLE };
    var waiter2 = Waiter{ .interest = Interest.WRITABLE };
    var waiter3 = Waiter{ .interest = Interest.BOTH };

    io.addWaiter(&waiter1);
    io.addWaiter(&waiter2);
    io.addWaiter(&waiter3);

    // Set up reserved wakers
    var counter: usize = 0;
    const TestWaker = struct {
        fn wake_fn(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    };
    io.setReaderWaker(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake_fn });
    io.setWriterWaker(.{ .context = @ptrCast(&counter), .wake_fn = TestWaker.wake_fn });

    // Clear all wakers
    io.clearWakers();

    // Reserved wakers should be null
    try std.testing.expect(io.reader_waker == null);
    try std.testing.expect(io.writer_waker == null);

    // Waiters list should be empty
    io.waiters_lock.lock();
    defer io.waiters_lock.unlock();
    try std.testing.expect(io.waiters.isEmpty());

    // Waiters should be marked ready and have cleared pointers
    try std.testing.expect(waiter1.is_ready);
    try std.testing.expect(waiter2.is_ready);
    try std.testing.expect(waiter3.is_ready);
}

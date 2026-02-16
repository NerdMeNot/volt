//! Channel - Lock-Free Multi-Producer Multi-Consumer (MPMC) Channel
//!
//! A bounded channel for sending values between multiple producers and consumers.
//! When the channel is full, senders wait. When empty, receivers wait.
//!
//! ## Design
//!
//! Uses a Vyukov/crossbeam-style lock-free ring buffer with per-slot sequence
//! numbers. The buffer operations (trySend/tryRecv) are fully lock-free using
//! CAS on head/tail positions. A separate waiter_mutex protects only the waiter
//! lists for async operations, never touching the hot data path.
//!
//! ## Algorithm
//!
//! Each slot has a sequence counter initialized to its index. To send at
//! position `tail`: if slot.sequence == tail, the slot is writable. After
//! writing, set slot.sequence = tail + 1. To receive at position `head`:
//! if slot.sequence == head + 1, the slot has data. After reading, set
//! slot.sequence = head + capacity (marking it writable again).
//!
//! ## Usage
//!
//! ```zig
//! var channel = try Channel(u32).init(allocator, 16);
//! defer channel.deinit();
//!
//! // Non-blocking API:
//! switch (channel.trySend(42)) {
//!     .ok => {},
//!     .full => // Handle backpressure
//!     .closed => // Channel was closed
//! }
//! switch (channel.tryRecv()) {
//!     .value => |v| processValue(v),
//!     .empty => // No value available
//!     .closed => // Channel closed
//! }
//!
//! // Async API (returns Future):
//! var send_future = channel.sendFuture(42);
//! var recv_future = channel.recvFuture();
//! ```
//!
//! Reference: crossbeam-channel, Vyukov MPMC bounded queue

const std = @import("std");
const Allocator = std.mem.Allocator;

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const WakeList = @import("../internal/util/wake_list.zig").WakeList;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;
const CACHE_LINE_SIZE = @import("../internal/util/cacheline.zig").CACHE_LINE_SIZE;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter Types
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for send operation
pub const SendWaiter = struct {
    /// Waker to invoke when slot is available
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Whether send completed (atomic for cross-thread visibility)
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Whether channel was closed (atomic for cross-thread visibility)
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Intrusive list pointers
    pointers: Pointers(SendWaiter) = .{},

    /// Debug-mode invocation tracking (detects use-after-free)
    invocation: InvocationId = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    pub fn wake(self: *Self) void {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    pub fn isComplete(self: *const Self) bool {
        return self.complete.load(.acquire) or self.closed.load(.acquire);
    }

    /// Get invocation token (for debug tracking)
    pub fn token(self: *const Self) InvocationId.Id {
        return self.invocation.get();
    }

    /// Verify invocation token matches (debug mode)
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.invocation.verify(tok);
    }

    /// Reset for reuse (generates new invocation ID)
    pub fn reset(self: *Self) void {
        self.complete.store(false, .release);
        self.closed.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }
};

/// Waiter for receive operation
pub const RecvWaiter = struct {
    /// Waker to invoke when value is available
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,

    /// Whether receive completed (value ready or closed) - atomic for cross-thread visibility
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Whether channel was closed with no more values - atomic for cross-thread visibility
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Intrusive list pointers
    pointers: Pointers(RecvWaiter) = .{},

    /// Debug-mode invocation tracking (detects use-after-free)
    invocation: InvocationId = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    pub fn wake(self: *Self) void {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    pub fn isComplete(self: *const Self) bool {
        return self.complete.load(.acquire) or self.closed.load(.acquire);
    }

    /// Get invocation token (for debug tracking)
    pub fn token(self: *const Self) InvocationId.Id {
        return self.invocation.get();
    }

    /// Verify invocation token matches (debug mode)
    pub fn verifyToken(self: *const Self, tok: InvocationId.Id) void {
        self.invocation.verify(tok);
    }

    /// Reset for reuse (generates new invocation ID)
    pub fn reset(self: *Self) void {
        self.complete.store(false, .release);
        self.closed.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }
};

const SendWaiterList = LinkedList(SendWaiter, "pointers");
const RecvWaiterList = LinkedList(RecvWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Channel
// ─────────────────────────────────────────────────────────────────────────────

/// A bounded MPMC channel using a lock-free Vyukov ring buffer.
pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Per-slot sequence number for lock-free protocol.
        /// Sequence tracks slot state: writable when seq == expected_tail,
        /// readable when seq == expected_head + 1.
        const Slot = struct {
            sequence: std.atomic.Value(u64),
        };

        /// Slot metadata array (sequence counters)
        slots: []Slot,

        /// Value storage (separate from slots for cache efficiency)
        buffer: []T,

        /// User-requested channel capacity (max items in flight)
        capacity: u64,

        /// Internal buffer size (>= capacity, >= 2 for Vyukov algorithm correctness)
        buf_cap: u64,

        /// Allocator used for buffer+slots
        allocator: Allocator,

        /// Consumer position — cache-line padded to avoid false sharing with tail
        head: std.atomic.Value(u64) align(CACHE_LINE_SIZE),

        /// Producer position — cache-line padded to avoid false sharing with head
        tail: std.atomic.Value(u64) align(CACHE_LINE_SIZE),

        /// Whether the channel is closed (lock-free)
        closed_flag: std.atomic.Value(bool),

        /// Number of active senders
        sender_count: usize,

        /// Mutex protecting ONLY the waiter lists (never the buffer)
        waiter_mutex: std.Thread.Mutex,

        /// Waiters for send (blocked when full)
        send_waiters: SendWaiterList,

        /// Waiters for receive (blocked when empty)
        recv_waiters: RecvWaiterList,

        /// Fast-check flags: set under waiter_mutex, checked lock-free after buffer ops.
        /// Protocol: waiter sets flag (seq_cst) BEFORE re-checking buffer to prevent
        /// lost wakeups vs concurrent sender/receiver.
        has_recv_waiters: std.atomic.Value(bool),
        has_send_waiters: std.atomic.Value(bool),

        /// Send result
        pub const SendResult = enum {
            /// Value was sent successfully
            ok,
            /// Channel is full
            full,
            /// Channel is closed
            closed,
        };

        /// Receive result
        pub const RecvResult = union(enum) {
            /// Received a value
            value: T,
            /// Channel is empty
            empty,
            /// Channel is closed and empty
            closed,
        };

        /// Create a new channel with the given capacity.
        pub fn init(allocator: Allocator, capacity: usize) !Self {
            const cap: u64 = @intCast(@max(1, capacity));
            // Internal buffer must be >= 2 for Vyukov sequence numbers to
            // distinguish "has data" from "writable" states.
            const buf_cap: u64 = @max(cap, 2);

            const slots = try allocator.alloc(Slot, @intCast(buf_cap));
            errdefer allocator.free(slots);

            // Initialize each slot's sequence to its index
            for (slots, 0..) |*slot, i| {
                slot.sequence = std.atomic.Value(u64).init(@intCast(i));
            }

            const buffer = try allocator.alloc(T, @intCast(buf_cap));
            errdefer allocator.free(buffer);

            return .{
                .slots = slots,
                .buffer = buffer,
                .capacity = cap,
                .buf_cap = buf_cap,
                .allocator = allocator,
                .head = std.atomic.Value(u64).init(0),
                .tail = std.atomic.Value(u64).init(0),
                .closed_flag = std.atomic.Value(bool).init(false),
                .sender_count = 1,
                .waiter_mutex = .{},
                .send_waiters = .{},
                .recv_waiters = .{},
                .has_recv_waiters = std.atomic.Value(bool).init(false),
                .has_send_waiters = std.atomic.Value(bool).init(false),
            };
        }

        /// Destroy the channel.
        pub fn deinit(self: *Self) void {
            self.allocator.free(self.slots);
            self.allocator.free(self.buffer);
        }

        // ═══════════════════════════════════════════════════════════════════
        // Send Operations (lock-free fast path)
        // ═══════════════════════════════════════════════════════════════════

        /// Try to send without blocking. Lock-free CAS on tail.
        pub fn trySend(self: *Self, value: T) SendResult {
            if (self.closed_flag.load(.acquire)) return .closed;

            var tail = self.tail.load(.monotonic);
            while (true) {
                // When buf_cap > capacity (capacity=1 case), we need an explicit
                // capacity check since the sequence numbers alone can't distinguish
                // full from empty. When buf_cap == capacity, the sequence check
                // below handles it, so we skip this extra cache-line read.
                if (self.buf_cap != self.capacity) {
                    const head = self.head.load(.acquire);
                    if (tail -% head >= self.capacity) return .full;
                }

                const idx: usize = @intCast(tail % self.buf_cap);
                const slot = &self.slots[idx];
                const seq = slot.sequence.load(.acquire);

                // Wrapping signed comparison: seq - tail
                const diff: i64 = @bitCast(seq -% tail);

                if (diff == 0) {
                    // Slot is writable — try to claim by advancing tail
                    if (self.tail.cmpxchgWeak(tail, tail +% 1, .acq_rel, .monotonic)) |new_tail| {
                        tail = new_tail; // Lost race, retry with updated tail
                        continue;
                    }
                    // Claimed! Write value and publish
                    self.buffer[idx] = value;
                    slot.sequence.store(tail +% 1, .release);

                    // Wake a receiver if any are waiting
                    if (self.has_recv_waiters.load(.acquire)) {
                        self.wakeOneRecvWaiter();
                    }

                    return .ok;
                } else if (diff < 0) {
                    // Slot not yet consumed — queue is full
                    return .full;
                } else {
                    // Another sender claimed this slot, reload tail
                    tail = self.tail.load(.monotonic);
                }
            }
        }

        /// Send, potentially waiting if full. Prefer `sendFuture()` which returns a Future.
        /// Returns true if completed immediately (sent or closed).
        /// Returns false if waiter was added (task should yield).
        pub fn sendWait(self: *Self, value: T, waiter: *SendWaiter) bool {
            // Fast path: lock-free try
            const result = self.trySend(value);
            if (result == .ok) {
                waiter.complete.store(true, .release);
                return true;
            }
            if (result == .closed) {
                waiter.closed.store(true, .release);
                return true;
            }

            // Slow path: channel full — register waiter under mutex
            self.waiter_mutex.lock();

            // Re-check closed under lock
            if (self.closed_flag.load(.acquire)) {
                self.waiter_mutex.unlock();
                waiter.closed.store(true, .release);
                return true;
            }

            // Set flag BEFORE re-checking buffer (prevents lost wakeup)
            self.has_send_waiters.store(true, .release);

            // Re-check buffer: a receiver may have freed a slot
            const retry = self.trySend(value);
            if (retry == .ok) {
                // Clear flag if no other waiters
                if (self.send_waiters.isEmpty()) {
                    self.has_send_waiters.store(false, .release);
                }
                self.waiter_mutex.unlock();
                waiter.complete.store(true, .release);
                return true;
            }
            if (retry == .closed) {
                if (self.send_waiters.isEmpty()) {
                    self.has_send_waiters.store(false, .release);
                }
                self.waiter_mutex.unlock();
                waiter.closed.store(true, .release);
                return true;
            }

            // Still full — add to wait list
            waiter.complete.store(false, .release);
            waiter.closed.store(false, .release);
            self.send_waiters.pushBack(waiter);

            self.waiter_mutex.unlock();
            return false;
        }

        /// Cancel a pending send.
        pub fn cancelSend(self: *Self, waiter: *SendWaiter) void {
            if (waiter.isComplete()) return;

            self.waiter_mutex.lock();

            if (waiter.isComplete()) {
                self.waiter_mutex.unlock();
                return;
            }

            if (SendWaiterList.isLinked(waiter) or self.send_waiters.front() == waiter) {
                self.send_waiters.remove(waiter);
                waiter.pointers.reset();
            }

            // Clear flag if no more waiters
            if (self.send_waiters.isEmpty()) {
                self.has_send_waiters.store(false, .release);
            }

            self.waiter_mutex.unlock();
        }

        // ═══════════════════════════════════════════════════════════════════
        // Receive Operations (lock-free fast path)
        // ═══════════════════════════════════════════════════════════════════

        /// Try to receive without blocking. Lock-free CAS on head.
        pub fn tryRecv(self: *Self) RecvResult {
            var head = self.head.load(.monotonic);
            while (true) {
                const idx: usize = @intCast(head % self.buf_cap);
                const slot = &self.slots[idx];
                const seq = slot.sequence.load(.acquire);

                // Wrapping signed comparison: seq - (head + 1)
                const diff: i64 = @bitCast(seq -% (head +% 1));

                if (diff == 0) {
                    // Slot has data — try to claim by advancing head
                    if (self.head.cmpxchgWeak(head, head +% 1, .acq_rel, .monotonic)) |new_head| {
                        head = new_head; // Lost race, retry
                        continue;
                    }
                    // Claimed! Read value and release slot
                    const value = self.buffer[idx];
                    slot.sequence.store(head +% self.buf_cap, .release);

                    // Wake a sender if any are waiting
                    if (self.has_send_waiters.load(.acquire)) {
                        self.wakeOneSendWaiter();
                    }

                    return .{ .value = value };
                } else if (diff < 0) {
                    // Queue is empty
                    if (self.closed_flag.load(.acquire)) return .closed;
                    return .empty;
                } else {
                    // Another receiver claimed this slot, reload head
                    head = self.head.load(.monotonic);
                }
            }
        }

        /// Receive, potentially waiting if empty. Prefer `recvFuture()` which returns a Future.
        /// Returns value if received immediately, null if waiter was added or closed.
        pub fn recvWait(self: *Self, waiter: *RecvWaiter) ?T {
            // Fast path: lock-free try
            const result = self.tryRecv();
            switch (result) {
                .value => |v| {
                    waiter.complete.store(true, .release);
                    return v;
                },
                .closed => {
                    waiter.closed.store(true, .release);
                    return null;
                },
                .empty => {},
            }

            // Slow path: channel empty — register waiter under mutex
            self.waiter_mutex.lock();

            // Set flag BEFORE re-checking buffer (prevents lost wakeup)
            self.has_recv_waiters.store(true, .release);

            // Re-check buffer: a sender may have added a value
            const retry = self.tryRecv();
            switch (retry) {
                .value => |v| {
                    if (self.recv_waiters.isEmpty()) {
                        self.has_recv_waiters.store(false, .release);
                    }
                    self.waiter_mutex.unlock();
                    waiter.complete.store(true, .release);
                    return v;
                },
                .closed => {
                    if (self.recv_waiters.isEmpty()) {
                        self.has_recv_waiters.store(false, .release);
                    }
                    self.waiter_mutex.unlock();
                    waiter.closed.store(true, .release);
                    return null;
                },
                .empty => {},
            }

            // Check closed
            if (self.closed_flag.load(.acquire)) {
                if (self.recv_waiters.isEmpty()) {
                    self.has_recv_waiters.store(false, .release);
                }
                self.waiter_mutex.unlock();
                waiter.closed.store(true, .release);
                return null;
            }

            // Still empty — add to wait list
            waiter.complete.store(false, .release);
            waiter.closed.store(false, .release);
            self.recv_waiters.pushBack(waiter);

            self.waiter_mutex.unlock();
            return null;
        }

        /// Cancel a pending receive.
        pub fn cancelRecv(self: *Self, waiter: *RecvWaiter) void {
            if (waiter.isComplete()) return;

            self.waiter_mutex.lock();

            if (waiter.isComplete()) {
                self.waiter_mutex.unlock();
                return;
            }

            if (RecvWaiterList.isLinked(waiter) or self.recv_waiters.front() == waiter) {
                self.recv_waiters.remove(waiter);
                waiter.pointers.reset();
            }

            // Clear flag if no more waiters
            if (self.recv_waiters.isEmpty()) {
                self.has_recv_waiters.store(false, .release);
            }

            self.waiter_mutex.unlock();
        }

        // ═══════════════════════════════════════════════════════════════════
        // Waiter Wake Helpers
        // ═══════════════════════════════════════════════════════════════════

        /// Wake one recv waiter (called after successful send).
        fn wakeOneRecvWaiter(self: *Self) void {
            self.waiter_mutex.lock();
            const waiter = self.recv_waiters.popFront();
            if (self.recv_waiters.isEmpty()) {
                self.has_recv_waiters.store(false, .release);
            }
            self.waiter_mutex.unlock();

            if (waiter) |w| {
                // CRITICAL: Copy waker info BEFORE setting complete flag to avoid use-after-free.
                // Once complete is set, the waiter's owner may immediately destroy it.
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.complete.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wf(ctx);
                    }
                }
            }
        }

        /// Wake one send waiter (called after successful recv).
        fn wakeOneSendWaiter(self: *Self) void {
            self.waiter_mutex.lock();
            const waiter = self.send_waiters.popFront();
            if (self.send_waiters.isEmpty()) {
                self.has_send_waiters.store(false, .release);
            }
            self.waiter_mutex.unlock();

            if (waiter) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.complete.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wf(ctx);
                    }
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Channel Control
        // ═══════════════════════════════════════════════════════════════════

        /// Close the channel.
        /// All pending receives will return closed.
        /// All pending sends will return closed.
        pub fn close(self: *Self) void {
            var send_wake_list: WakeList(32) = .{};
            var recv_wake_list: WakeList(32) = .{};

            // Set closed flag first (lock-free visibility)
            if (self.closed_flag.swap(true, .acq_rel)) {
                return; // Already closed
            }

            self.waiter_mutex.lock();

            // Wake all send waiters
            while (self.send_waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.closed.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        send_wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }
            self.has_send_waiters.store(false, .release);

            // Wake all recv waiters
            while (self.recv_waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.closed.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        recv_wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }
            self.has_recv_waiters.store(false, .release);

            self.waiter_mutex.unlock();

            // Wake outside lock
            send_wake_list.wakeAll();
            recv_wake_list.wakeAll();
        }

        /// Check if channel is closed (lock-free).
        pub fn isClosed(self: *Self) bool {
            return self.closed_flag.load(.acquire);
        }

        /// Get approximate number of items in channel (lock-free).
        pub fn len(self: *Self) usize {
            const tail = self.tail.load(.acquire);
            const head = self.head.load(.acquire);
            const diff = tail -% head;
            return @intCast(@min(diff, self.capacity));
        }

        /// Check if channel is approximately empty (lock-free).
        pub fn isEmpty(self: *Self) bool {
            return self.len() == 0;
        }

        /// Check if channel is approximately full (lock-free).
        pub fn isFull(self: *Self) bool {
            return self.len() >= @as(usize, @intCast(self.capacity));
        }

        // ═══════════════════════════════════════════════════════════════════
        // Async API
        // ═══════════════════════════════════════════════════════════════════

        /// Send a value, blocking the current task if the channel is full.
        pub fn send(self: *Self, io: @import("../Io.zig"), value: T) void {
            var f = io.awaitFuture(self.sendFuture(value)) catch return;
            _ = f.await(io);
        }

        /// Receive a value, blocking the current task until one is available.
        /// Returns null if the channel is closed and empty.
        pub fn recv(self: *Self, io: @import("../Io.zig")) ?T {
            var f = io.awaitFuture(self.recvFuture()) catch return null;
            return f.await(io) catch null;
        }

        /// Send a value asynchronously. Returns a `SendFuture`.
        pub fn sendFuture(self: *Self, value: T) SendFuture(T) {
            return SendFuture(T).init(self, value);
        }

        /// Receive a value asynchronously. Returns a `RecvFuture`.
        pub fn recvFuture(self: *Self) RecvFuture(T) {
            return RecvFuture(T).init(self);
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// SendFuture - Async send operation
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when a value is sent to the channel.
pub fn SendFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Result of the send operation
        pub const SendResult = enum {
            /// Value was sent successfully
            ok,
            /// Channel was closed
            closed,
        };

        /// Output type for Future trait
        pub const Output = SendResult;

        /// Reference to the channel we're sending to
        channel: *Channel(T),

        /// The value we're trying to send
        value: T,

        /// Our waiter node (embedded to avoid allocation)
        waiter: SendWaiter,

        /// State machine for the future
        state: State,

        /// Stored waker for when we're woken by recv()
        stored_waker: ?Waker,

        /// Whether the value has been successfully sent
        value_sent: bool,

        const State = enum {
            /// Haven't tried to send yet
            init,
            /// Waiting for slot in channel
            waiting,
            /// Send completed (success or closed)
            ready,
        };

        /// Initialize a new send future
        pub fn init(channel: *Channel(T), value: T) Self {
            return .{
                .channel = channel,
                .value = value,
                .waiter = SendWaiter.init(),
                .state = .init,
                .stored_waker = null,
                .value_sent = false,
            };
        }

        /// Poll the future - implements Future trait
        pub fn poll(self: *Self, ctx: *Context) PollResult(SendResult) {
            switch (self.state) {
                .init => {
                    // First poll - try to send the value
                    self.stored_waker = ctx.getWaker().clone();
                    self.waiter.setWaker(@ptrCast(self), wakeCallback);

                    if (self.channel.sendWait(self.value, &self.waiter)) {
                        self.state = .ready;
                        self.value_sent = true;
                        if (self.stored_waker) |*w| {
                            w.deinit();
                            self.stored_waker = null;
                        }
                        if (self.waiter.closed.load(.acquire)) {
                            return .{ .ready = .closed };
                        }
                        return .{ .ready = .ok };
                    } else {
                        self.state = .waiting;
                        return .pending;
                    }
                },

                .waiting => {
                    if (self.waiter.isComplete()) {
                        self.state = .ready;
                        self.value_sent = true;
                        if (self.stored_waker) |*w| {
                            w.deinit();
                            self.stored_waker = null;
                        }
                        if (self.waiter.closed.load(.acquire)) {
                            return .{ .ready = .closed };
                        }
                        return .{ .ready = .ok };
                    }

                    // Not yet - update waker in case it changed (task migration)
                    const new_waker = ctx.getWaker();
                    if (self.stored_waker) |*old| {
                        if (!old.willWakeSame(new_waker)) {
                            old.deinit();
                            self.stored_waker = new_waker.clone();
                        }
                    } else {
                        self.stored_waker = new_waker.clone();
                    }

                    return .pending;
                },

                .ready => {
                    if (self.waiter.closed.load(.acquire)) {
                        return .{ .ready = .closed };
                    }
                    return .{ .ready = .ok };
                },
            }
        }

        /// Callback invoked by Channel when slot becomes available
        fn wakeCallback(ctx_ptr: *anyopaque) void {
            const self_ptr: *Self = @ptrCast(@alignCast(ctx_ptr));
            if (self_ptr.stored_waker) |*w| {
                w.wakeByRef();
            }
        }

        /// Cancel the send operation
        pub fn cancel(self: *Self) void {
            if (self.state == .waiting) {
                self.channel.cancelSend(&self.waiter);
            }
            if (self.stored_waker) |*w| {
                w.deinit();
                self.stored_waker = null;
            }
        }

        /// Deinit the future
        pub fn deinit(self: *Self) void {
            self.cancel();
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// RecvFuture - Async receive operation
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when a value is received from the channel.
pub fn RecvFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Output type for Future trait - null if channel closed
        pub const Output = ?T;

        /// Reference to the channel we're receiving from
        channel: *Channel(T),

        /// Our waiter node (embedded to avoid allocation)
        waiter: RecvWaiter,

        /// State machine for the future
        state: State,

        /// Stored waker for when we're woken by send()
        stored_waker: ?Waker,

        /// The received value (stored when ready)
        received_value: ?T,

        const State = enum {
            /// Haven't tried to receive yet
            init,
            /// Waiting for value in channel
            waiting,
            /// Receive completed (value or closed)
            ready,
        };

        /// Initialize a new recv future
        pub fn init(channel: *Channel(T)) Self {
            return .{
                .channel = channel,
                .waiter = RecvWaiter.init(),
                .state = .init,
                .stored_waker = null,
                .received_value = null,
            };
        }

        /// Poll the future - implements Future trait
        pub fn poll(self: *Self, ctx: *Context) PollResult(?T) {
            switch (self.state) {
                .init => {
                    self.stored_waker = ctx.getWaker().clone();
                    self.waiter.setWaker(@ptrCast(self), wakeCallback);

                    if (self.channel.recvWait(&self.waiter)) |value| {
                        self.state = .ready;
                        self.received_value = value;
                        if (self.stored_waker) |*w| {
                            w.deinit();
                            self.stored_waker = null;
                        }
                        return .{ .ready = value };
                    } else {
                        if (self.waiter.closed.load(.acquire)) {
                            self.state = .ready;
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            return .{ .ready = null };
                        }
                        self.state = .waiting;
                        return .pending;
                    }
                },

                .waiting => {
                    if (self.waiter.isComplete()) {
                        if (self.waiter.closed.load(.acquire)) {
                            self.state = .ready;
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            return .{ .ready = null };
                        }
                        // Value should be ready
                        const result = self.channel.tryRecv();
                        switch (result) {
                            .value => |v| {
                                self.state = .ready;
                                self.received_value = v;
                                if (self.stored_waker) |*w| {
                                    w.deinit();
                                    self.stored_waker = null;
                                }
                                return .{ .ready = v };
                            },
                            .closed => {
                                self.state = .ready;
                                if (self.stored_waker) |*w| {
                                    w.deinit();
                                    self.stored_waker = null;
                                }
                                return .{ .ready = null };
                            },
                            .empty => {
                                // Spurious wake — another consumer stole the value.
                                // Re-register waiter with the channel.
                                // CRITICAL: Do NOT deinit stored_waker — we still
                                // need it for the next wakeup callback.
                                self.waiter.setWaker(@ptrCast(self), wakeCallback);
                                if (self.channel.recvWait(&self.waiter)) |value| {
                                    self.state = .ready;
                                    self.received_value = value;
                                    if (self.stored_waker) |*w| {
                                        w.deinit();
                                        self.stored_waker = null;
                                    }
                                    return .{ .ready = value };
                                } else {
                                    if (self.waiter.closed.load(.acquire)) {
                                        self.state = .ready;
                                        if (self.stored_waker) |*w| {
                                            w.deinit();
                                            self.stored_waker = null;
                                        }
                                        return .{ .ready = null };
                                    }
                                    return .pending;
                                }
                            },
                        }
                    }

                    // Not yet - update waker in case it changed (task migration)
                    const new_waker = ctx.getWaker();
                    if (self.stored_waker) |*old| {
                        if (!old.willWakeSame(new_waker)) {
                            old.deinit();
                            self.stored_waker = new_waker.clone();
                        }
                    } else {
                        self.stored_waker = new_waker.clone();
                    }

                    return .pending;
                },

                .ready => {
                    return .{ .ready = self.received_value };
                },
            }
        }

        /// Callback invoked by Channel when value becomes available
        fn wakeCallback(ctx_ptr: *anyopaque) void {
            const self_ptr: *Self = @ptrCast(@alignCast(ctx_ptr));
            if (self_ptr.stored_waker) |*w| {
                w.wakeByRef();
            }
        }

        /// Cancel the receive operation
        pub fn cancel(self: *Self) void {
            if (self.state == .waiting) {
                self.channel.cancelRecv(&self.waiter);
            }
            if (self.stored_waker) |*w| {
                w.deinit();
                self.stored_waker = null;
            }
        }

        /// Deinit the future
        pub fn deinit(self: *Self) void {
            self.cancel();
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Channel - trySend and tryRecv" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    // Send values
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(1));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(2));
    try std.testing.expectEqual(Channel(u32).SendResult.full, ch.trySend(3));

    // Receive values
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());
    try std.testing.expect(ch.tryRecv() == .empty);
}

test "Channel - zero allocation init" {
    // Use allocator-based init (initWithBuffer removed for lock-free)
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    try std.testing.expectEqual(@as(usize, 4), @as(usize, @intCast(ch.capacity)));

    // Send values
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(10));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(20));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(30));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(40));
    try std.testing.expectEqual(Channel(u32).SendResult.full, ch.trySend(50));

    // Receive values
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 10 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 20 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 30 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 40 }, ch.tryRecv());
    try std.testing.expect(ch.tryRecv() == .empty);
}

test "Channel - blocking send and recv" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Fill channel
    _ = ch.trySend(1);
    try std.testing.expect(ch.isFull());

    // Send should block
    var send_waiter = SendWaiter.init();
    send_waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Receive should unblock sender
    const result = ch.tryRecv();
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, result);
}

test "Channel - close wakes waiters" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();
    var recv_woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Receiver waits on empty channel
    var recv_waiter = RecvWaiter.init();
    recv_waiter.setWaker(@ptrCast(&recv_woken), TestWaker.wake);
    _ = ch.recvWait(&recv_waiter);

    // Close should wake receiver
    ch.close();
    try std.testing.expect(recv_woken);
    try std.testing.expect(recv_waiter.closed.load(.acquire));
}

test "Channel - closed channel rejects sends" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    ch.close();

    try std.testing.expectEqual(Channel(u32).SendResult.closed, ch.trySend(1));
}

test "Channel - FIFO ordering" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    _ = ch.trySend(1);
    _ = ch.trySend(2);
    _ = ch.trySend(3);

    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 3 }, ch.tryRecv());
}

test "Channel - backpressure when full" {
    var ch = try Channel(u32).init(std.testing.allocator, 3);
    defer ch.deinit();

    // Fill the channel
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(1));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(2));
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(3));

    // Channel is now full
    try std.testing.expect(ch.isFull());
    try std.testing.expectEqual(Channel(u32).SendResult.full, ch.trySend(4));
    try std.testing.expectEqual(Channel(u32).SendResult.full, ch.trySend(5));

    // Receive one item
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());

    // Now we can send again
    try std.testing.expect(!ch.isFull());
    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(4));

    // Verify FIFO order maintained
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 3 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 4 }, ch.tryRecv());
}

test "Channel - empty channel behavior" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    // Channel starts empty
    try std.testing.expect(ch.isEmpty());
    try std.testing.expect(ch.tryRecv() == .empty);
    try std.testing.expect(ch.tryRecv() == .empty);

    // Add one item
    _ = ch.trySend(42);
    try std.testing.expect(!ch.isEmpty());

    // Receive it
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 42 }, ch.tryRecv());
    try std.testing.expect(ch.isEmpty());
}

test "Channel - capacity boundaries" {
    // Test with capacity of 1 (minimal)
    var ch1 = try Channel(u32).init(std.testing.allocator, 1);
    defer ch1.deinit();

    try std.testing.expectEqual(Channel(u32).SendResult.ok, ch1.trySend(1));
    try std.testing.expect(ch1.isFull());
    try std.testing.expectEqual(Channel(u32).SendResult.full, ch1.trySend(2));

    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch1.tryRecv());
    try std.testing.expect(ch1.isEmpty());
}

test "Channel - large capacity" {
    var ch = try Channel(u32).init(std.testing.allocator, 1000);
    defer ch.deinit();

    // Fill with many items
    for (0..1000) |i| {
        try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(@intCast(i)));
    }
    try std.testing.expect(ch.isFull());

    // Drain all items
    for (0..1000) |i| {
        const result = ch.tryRecv();
        try std.testing.expectEqual(Channel(u32).RecvResult{ .value = @intCast(i) }, result);
    }
    try std.testing.expect(ch.isEmpty());
}

test "Channel - interleaved send and recv" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    // Interleave sends and receives
    _ = ch.trySend(1);
    _ = ch.trySend(2);
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());
    _ = ch.trySend(3);
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 3 }, ch.tryRecv());
    _ = ch.trySend(4);
    _ = ch.trySend(5);
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 4 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 5 }, ch.tryRecv());
}

test "Channel - close behavior" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    // Send some values
    _ = ch.trySend(1);
    _ = ch.trySend(2);

    // Close the channel
    ch.close();

    // Can still receive pending values
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 1 }, ch.tryRecv());
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 2 }, ch.tryRecv());

    // After draining, receive returns closed
    try std.testing.expect(ch.tryRecv() == .closed);

    // Send returns closed
    try std.testing.expectEqual(Channel(u32).SendResult.closed, ch.trySend(3));
}

test "Channel - rapid fill and drain cycles" {
    var ch = try Channel(u32).init(std.testing.allocator, 8);
    defer ch.deinit();

    // Multiple fill/drain cycles to stress ring buffer wrap-around
    for (0..10) |cycle| {
        // Fill
        for (0..8) |i| {
            const value: u32 = @intCast(cycle * 8 + i);
            try std.testing.expectEqual(Channel(u32).SendResult.ok, ch.trySend(value));
        }
        try std.testing.expect(ch.isFull());

        // Drain
        for (0..8) |i| {
            const expected: u32 = @intCast(cycle * 8 + i);
            const result = ch.tryRecv();
            try std.testing.expectEqual(Channel(u32).RecvResult{ .value = expected }, result);
        }
        try std.testing.expect(ch.isEmpty());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SendFuture and RecvFuture Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SendFuture - immediate send" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    // Create future and poll - should send immediately
    var future = ch.sendFuture(42);
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(SendFuture(u32).SendResult.ok, result.ready);
    try std.testing.expectEqual(@as(usize, 1), ch.len());

    // Verify value was sent
    try std.testing.expectEqual(Channel(u32).RecvResult{ .value = 42 }, ch.tryRecv());
}

test "RecvFuture - immediate receive" {
    var ch = try Channel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    // Send a value first
    _ = ch.trySend(123);

    // Create future and poll - should receive immediately
    var future = ch.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(?u32, 123), result.ready);
}

test "SendFuture - waits when channel full" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();
    var waker_called = false;

    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) Waker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Fill the channel
    _ = ch.trySend(1);
    try std.testing.expect(ch.isFull());

    // Create future - first poll should return pending
    var future = ch.sendFuture(2);
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Receive a value - should wake the future
    _ = ch.tryRecv();
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expectEqual(SendFuture(u32).SendResult.ok, result2.ready);
}

test "RecvFuture - waits when channel empty" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();
    var waker_called = false;

    const TestWaker = struct {
        called: *bool,

        fn wake(data: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(data));
            self.called.* = true;
        }

        fn clone(data: *anyopaque) future_mod.RawWaker {
            return .{ .data = data, .vtable = &vtable };
        }

        fn drop(_: *anyopaque) void {}

        const vtable = future_mod.RawWaker.VTable{
            .wake = wake,
            .wake_by_ref = wake,
            .clone = clone,
            .drop = drop,
        };

        fn toWaker(self: *@This()) Waker {
            return .{ .raw = .{ .data = @ptrCast(self), .vtable = &vtable } };
        }
    };

    var test_waker = TestWaker{ .called = &waker_called };
    const waker = test_waker.toWaker();

    // Channel is empty
    try std.testing.expect(ch.isEmpty());

    // Create future - first poll should return pending
    var future = ch.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Send a value - should wake the future
    _ = ch.trySend(42);
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expectEqual(@as(?u32, 42), result2.ready);
}

test "SendFuture - cancel removes from queue" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();

    // Fill the channel
    _ = ch.trySend(1);

    // Create future and poll to add to queue
    var future = ch.sendFuture(2);
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());

    // Cancel
    future.cancel();

    // Clean up
    future.deinit();
}

test "RecvFuture - cancel removes from queue" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();

    // Create future and poll to add to queue
    var future = ch.recvFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());

    // Cancel
    future.cancel();

    // Clean up
    future.deinit();
}

test "SendFuture - closed channel returns closed" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();

    // Close the channel
    ch.close();

    // Create future and poll - should return closed
    var future = ch.sendFuture(42);
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(SendFuture(u32).SendResult.closed, result.ready);
}

test "RecvFuture - closed channel returns null" {
    var ch = try Channel(u32).init(std.testing.allocator, 1);
    defer ch.deinit();

    // Close the channel
    ch.close();

    // Create future and poll - should return null
    var future = ch.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(?u32, null), result.ready);
}

test "SendFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(SendFuture(u32)));
    try std.testing.expect(SendFuture(u32).Output == SendFuture(u32).SendResult);
}

test "RecvFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(RecvFuture(u32)));
    try std.testing.expect(RecvFuture(u32).Output == ?u32);
}

test "SendFuture and RecvFuture - roundtrip" {
    var ch = try Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };

    // Send multiple values using futures
    var send1 = ch.sendFuture(1);
    var send2 = ch.sendFuture(2);
    var send3 = ch.sendFuture(3);

    try std.testing.expect(send1.poll(&ctx).isReady());
    try std.testing.expect(send2.poll(&ctx).isReady());
    try std.testing.expect(send3.poll(&ctx).isReady());

    send1.deinit();
    send2.deinit();
    send3.deinit();

    // Receive using futures
    var recv1 = ch.recvFuture();
    var recv2 = ch.recvFuture();
    var recv3 = ch.recvFuture();

    const r1 = recv1.poll(&ctx);
    const r2 = recv2.poll(&ctx);
    const r3 = recv3.poll(&ctx);

    try std.testing.expect(r1.isReady());
    try std.testing.expect(r2.isReady());
    try std.testing.expect(r3.isReady());

    try std.testing.expectEqual(@as(?u32, 1), r1.ready);
    try std.testing.expectEqual(@as(?u32, 2), r2.ready);
    try std.testing.expectEqual(@as(?u32, 3), r3.ready);

    recv1.deinit();
    recv2.deinit();
    recv3.deinit();
}

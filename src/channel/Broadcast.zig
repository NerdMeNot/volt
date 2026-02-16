//! Broadcast Channel - Multi-Producer Multi-Consumer
//!
//! A broadcast channel where each sent value is received by ALL receivers.
//! Uses a ring buffer with reference counting per slot.
//!
//! ## Usage
//!
//! ```zig
//! var channel = try BroadcastChannel(u32).init(allocator, 16);
//! defer channel.deinit();
//!
//! // Create receivers
//! var rx1 = channel.subscribe();
//! var rx2 = channel.subscribe();
//!
//! // Send broadcasts to all
//! _ = channel.send(42);
//!
//! // Each receiver gets the value
//! _ = rx1.tryRecv();  // 42
//! _ = rx2.tryRecv();  // 42
//! ```
//!
//! ## Design
//!
//! - Ring buffer with capacity
//! - Each receiver tracks its position
//! - Lagging receivers may miss messages (configurable behavior)
//! - Senders never block
//!

const std = @import("std");
const Allocator = std.mem.Allocator;

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const WakeList = @import("../internal/util/wake_list.zig").WakeList;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for receive operation
pub const RecvWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether receive completed (atomic for cross-thread visibility)
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Whether channel was closed (atomic for cross-thread visibility)
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
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

const RecvWaiterList = LinkedList(RecvWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// BroadcastChannel
// ─────────────────────────────────────────────────────────────────────────────

/// A broadcast channel.
pub fn BroadcastChannel(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Slot in the ring buffer
        const Slot = struct {
            value: T,
            /// Sequence number when this was written
            seq: u64,
        };

        /// Ring buffer
        buffer: []Slot,

        /// Allocator
        allocator: Allocator,

        /// Capacity
        capacity: usize,

        /// Current write position (next sequence to write)
        write_seq: u64,

        /// Number of active receivers
        receiver_count: usize,

        /// Whether the channel is closed
        closed: bool,

        /// Mutex protecting internal state
        mutex: std.Thread.Mutex,

        /// Waiting receivers
        recv_waiters: RecvWaiterList,

        /// Receiver handle
        pub const Receiver = struct {
            channel: *Self,
            /// Next sequence this receiver expects
            read_seq: u64,

            /// Receive result
            pub const RecvResult = union(enum) {
                value: T,
                empty,
                lagged: u64, // Number of missed messages
                closed,
            };

            /// Try to receive without waiting.
            pub fn tryRecv(self: *Receiver) RecvResult {
                self.channel.mutex.lock();
                defer self.channel.mutex.unlock();

                // Check if we're lagging
                const oldest_seq = if (self.channel.write_seq > self.channel.capacity)
                    self.channel.write_seq - self.channel.capacity
                else
                    0;

                if (self.read_seq < oldest_seq) {
                    // We missed some messages
                    const missed = oldest_seq - self.read_seq;
                    self.read_seq = oldest_seq;
                    return .{ .lagged = missed };
                }

                // Check if anything to read
                if (self.read_seq >= self.channel.write_seq) {
                    if (self.channel.closed) {
                        return .closed;
                    }
                    return .empty;
                }

                // Read from buffer
                const idx = self.read_seq % self.channel.capacity;
                const slot = &self.channel.buffer[idx];

                // Verify sequence matches (slot hasn't been overwritten)
                if (slot.seq != self.read_seq) {
                    // Slot was overwritten - we lagged
                    const missed = self.channel.write_seq - self.read_seq;
                    self.read_seq = self.channel.write_seq;
                    return .{ .lagged = missed };
                }

                const value = slot.value;
                self.read_seq += 1;
                return .{ .value = value };
            }

            /// Receive, potentially waiting if empty. Prefer `recv(io)` or `recvFuture()`.
            /// Returns value if received immediately, null if waiting.
            pub fn recvWait(self: *Receiver, waiter: *RecvWaiter) ?RecvResult {
                self.channel.mutex.lock();

                // Check if we're lagging
                const oldest_seq = if (self.channel.write_seq > self.channel.capacity)
                    self.channel.write_seq - self.channel.capacity
                else
                    0;

                if (self.read_seq < oldest_seq) {
                    const missed = oldest_seq - self.read_seq;
                    self.read_seq = oldest_seq;
                    self.channel.mutex.unlock();
                    waiter.complete.store(true, .release);
                    return .{ .lagged = missed };
                }

                // Check if anything to read
                if (self.read_seq >= self.channel.write_seq) {
                    if (self.channel.closed) {
                        self.channel.mutex.unlock();
                        waiter.closed.store(true, .release);
                        return .closed;
                    }

                    // Wait for sender
                    waiter.complete.store(false, .release);
                    waiter.closed.store(false, .release);
                    self.channel.recv_waiters.pushBack(waiter);
                    self.channel.mutex.unlock();
                    return null;
                }

                // Read from buffer
                const idx = self.read_seq % self.channel.capacity;
                const slot = &self.channel.buffer[idx];

                if (slot.seq != self.read_seq) {
                    const missed = self.channel.write_seq - self.read_seq;
                    self.read_seq = self.channel.write_seq;
                    self.channel.mutex.unlock();
                    waiter.complete.store(true, .release);
                    return .{ .lagged = missed };
                }

                const value = slot.value;
                self.read_seq += 1;
                self.channel.mutex.unlock();
                waiter.complete.store(true, .release);
                return .{ .value = value };
            }

            /// Cancel pending receive.
            pub fn cancelRecv(self: *Receiver, waiter: *RecvWaiter) void {
                if (waiter.isComplete()) return;

                self.channel.mutex.lock();

                if (waiter.isComplete()) {
                    self.channel.mutex.unlock();
                    return;
                }

                if (RecvWaiterList.isLinked(waiter) or self.channel.recv_waiters.front() == waiter) {
                    self.channel.recv_waiters.remove(waiter);
                    waiter.pointers.reset();
                }

                self.channel.mutex.unlock();
            }

            /// Check if this receiver is lagging.
            pub fn isLagging(self: *const Receiver) bool {
                const oldest_seq = if (self.channel.write_seq > self.channel.capacity)
                    self.channel.write_seq - self.channel.capacity
                else
                    0;
                return self.read_seq < oldest_seq;
            }

            /// Receive a value, blocking the current task until available.
            ///
            /// Requires an `Io` handle. For manual Future control, use `recvFuture()`.
            ///
            /// ## Example
            ///
            /// ```zig
            /// switch (receiver.recv(io)) {
            ///     .value => |v| { /* use v */ },
            ///     .lagged => |n| { /* missed n messages */ },
            ///     .closed => { /* channel closed */ },
            ///     .empty => { /* shouldn't happen with async wait */ },
            /// }
            /// ```
            pub fn recv(self: *Receiver, io: @import("../Io.zig")) RecvFuture.Output {
                var f = io.awaitFuture(self.recvFuture()) catch return .closed;
                return f.await(io);
            }

            /// Receive asynchronously.
            ///
            /// Returns a `RecvFuture` that resolves when a value is available.
            /// This integrates with the scheduler for true async behavior - the task
            /// will be suspended (not spin-waiting) until a value is sent.
            /// Prefer `recv(io)` unless you need manual Future control.
            ///
            /// ## Example
            ///
            /// ```zig
            /// var future = receiver.recvFuture();
            /// // Poll through your runtime...
            /// // When future.poll() returns .ready, you have the value
            /// ```
            pub fn recvFuture(self: *Receiver) RecvFuture {
                return RecvFuture.init(self);
            }
        };

        /// Future for async receive operations.
        ///
        /// This integrates with the scheduler's Future system for true async behavior.
        /// The task will be suspended (not spin-waiting or blocking) until a value
        /// is available or the channel is closed.
        ///
        /// ## Usage
        ///
        /// ```zig
        /// var future = receiver.recvFuture();
        /// // ... poll the future through your runtime ...
        /// // When ready, the result contains the value or error
        /// ```
        pub const RecvFuture = struct {
            const FutureSelf = @This();

            /// Output type for Future trait
            pub const Output = Receiver.RecvResult;

            /// Reference to the receiver we're receiving from
            receiver: *Receiver,

            /// Our waiter node (embedded to avoid allocation)
            waiter: RecvWaiter,

            /// State machine for the future
            state: FutureState,

            /// Stored waker for when we're woken by send()
            stored_waker: ?Waker,

            const FutureState = enum {
                /// Haven't tried to receive yet
                init,
                /// Waiting for value
                waiting,
                /// Value received or channel closed
                ready,
            };

            /// Initialize a new recv future
            pub fn init(receiver: *Receiver) FutureSelf {
                return .{
                    .receiver = receiver,
                    .waiter = RecvWaiter.init(),
                    .state = .init,
                    .stored_waker = null,
                };
            }

            /// Poll the future - implements Future trait
            ///
            /// Returns `.pending` if no value available yet (task will be woken when available).
            /// Returns `.{ .ready = result }` when a value is received or channel is closed.
            pub fn poll(self: *FutureSelf, ctx: *Context) PollResult(Receiver.RecvResult) {
                switch (self.state) {
                    .init => {
                        // First poll - try to receive
                        // Store the waker so we can be woken when send() provides a value
                        self.stored_waker = ctx.getWaker().clone();

                        // Set up the waiter's callback to wake us via our stored waker
                        self.waiter.setWaker(@ptrCast(self), wakeCallback);

                        // Try to receive
                        if (self.receiver.recvWait(&self.waiter)) |result| {
                            // Got a value immediately
                            self.state = .ready;
                            // Clean up stored waker since we don't need it
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            return .{ .ready = result };
                        } else {
                            // Added to wait queue - will be woken when value available
                            self.state = .waiting;
                            return .pending;
                        }
                    },

                    .waiting => {
                        // Check if we've been woken
                        if (self.waiter.isComplete()) {
                            self.state = .ready;
                            // Clean up stored waker
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            // Check if channel was closed
                            if (self.waiter.closed.load(.acquire)) {
                                return .{ .ready = .closed };
                            }
                            // Try to get value now
                            return .{ .ready = self.receiver.tryRecv() };
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
                        // Already completed - try to get another value
                        return .{ .ready = self.receiver.tryRecv() };
                    },
                }
            }

            /// Callback invoked by send() when value is available
            /// This bridges the RecvWaiter's WakerFn to the Future's Waker
            fn wakeCallback(ctx_ptr: *anyopaque) void {
                const self_ptr: *FutureSelf = @ptrCast(@alignCast(ctx_ptr));
                if (self_ptr.stored_waker) |*w| {
                    // Wake the task through the Future system's Waker
                    w.wakeByRef();
                }
            }

            /// Cancel the receive operation
            ///
            /// If the receive hasn't completed yet, removes us from the wait queue.
            pub fn cancel(self: *FutureSelf) void {
                if (self.state == .waiting) {
                    self.receiver.cancelRecv(&self.waiter);
                }
                // Clean up stored waker
                if (self.stored_waker) |*w| {
                    w.deinit();
                    self.stored_waker = null;
                }
            }

            /// Deinit the future
            ///
            /// If waiting, cancels the receive operation.
            pub fn deinit(self: *FutureSelf) void {
                self.cancel();
            }
        };

        /// Send result
        pub const SendResult = union(enum) {
            /// Number of receivers that will receive this message
            ok: usize,
            /// Channel is closed
            closed,
        };

        /// Create a new broadcast channel.
        pub fn init(allocator: Allocator, capacity: usize) !Self {
            const buffer = try allocator.alloc(Slot, capacity);
            for (buffer) |*slot| {
                slot.seq = 0;
            }

            return .{
                .buffer = buffer,
                .allocator = allocator,
                .capacity = capacity,
                .write_seq = 0,
                .receiver_count = 0,
                .closed = false,
                .mutex = .{},
                .recv_waiters = .{},
            };
        }

        /// Destroy the channel.
        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        /// Create a new receiver subscribed to this channel.
        pub fn subscribe(self: *Self) Receiver {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.receiver_count += 1;
            return .{
                .channel = self,
                .read_seq = self.write_seq, // Start from current position
            };
        }

        /// Send a value to all receivers.
        pub fn send(self: *Self, value: T) SendResult {
            var wake_list: WakeList(64) = .{};

            self.mutex.lock();

            if (self.closed) {
                self.mutex.unlock();
                return .closed;
            }

            // Write to buffer
            const idx = self.write_seq % self.capacity;
            self.buffer[idx] = .{
                .value = value,
                .seq = self.write_seq,
            };
            self.write_seq += 1;

            const receivers = self.receiver_count;

            // Wake all waiting receivers
            // CRITICAL: Copy waker info BEFORE setting complete flag to avoid use-after-free
            while (self.recv_waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.complete.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }

            self.mutex.unlock();

            wake_list.wakeAll();

            return .{ .ok = receivers };
        }

        /// Close the channel.
        pub fn close(self: *Self) void {
            var wake_list: WakeList(64) = .{};

            self.mutex.lock();

            if (self.closed) {
                self.mutex.unlock();
                return;
            }

            self.closed = true;

            // CRITICAL: Copy waker info BEFORE setting closed flag to avoid use-after-free
            while (self.recv_waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.closed.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }

            self.mutex.unlock();

            wake_list.wakeAll();
        }

        /// Check if closed.
        pub fn isClosed(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.closed;
        }

        /// Get receiver count.
        pub fn receiverCount(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.receiver_count;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "BroadcastChannel - single receiver" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var rx = ch.subscribe();

    _ = ch.send(1);
    _ = ch.send(2);

    try std.testing.expectEqual(BroadcastChannel(u32).Receiver.RecvResult{ .value = 1 }, rx.tryRecv());
    try std.testing.expectEqual(BroadcastChannel(u32).Receiver.RecvResult{ .value = 2 }, rx.tryRecv());
    try std.testing.expect(rx.tryRecv() == .empty);
}

test "BroadcastChannel - multiple receivers" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var rx1 = ch.subscribe();
    var rx2 = ch.subscribe();

    _ = ch.send(42);

    // Both receivers get the same value
    try std.testing.expectEqual(BroadcastChannel(u32).Receiver.RecvResult{ .value = 42 }, rx1.tryRecv());
    try std.testing.expectEqual(BroadcastChannel(u32).Receiver.RecvResult{ .value = 42 }, rx2.tryRecv());
}

test "BroadcastChannel - lagging receiver" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 2);
    defer ch.deinit();

    var rx = ch.subscribe();

    // Fill and overflow buffer
    _ = ch.send(1);
    _ = ch.send(2);
    _ = ch.send(3); // Overwrites slot 0
    _ = ch.send(4); // Overwrites slot 1

    // Receiver should detect lag
    const result = rx.tryRecv();
    switch (result) {
        .lagged => |missed| try std.testing.expect(missed >= 2),
        else => try std.testing.expect(false),
    }
}

test "BroadcastChannel - new subscriber starts at current" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    _ = ch.send(1);
    _ = ch.send(2);

    // New subscriber doesn't see old messages
    var rx = ch.subscribe();
    try std.testing.expect(rx.tryRecv() == .empty);

    // But sees new ones
    _ = ch.send(3);
    try std.testing.expectEqual(BroadcastChannel(u32).Receiver.RecvResult{ .value = 3 }, rx.tryRecv());
}

test "BroadcastChannel - close wakes waiters" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var rx = ch.subscribe();
    var waiter = RecvWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    _ = rx.recvWait(&waiter);

    ch.close();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.closed.load(.acquire));
}

test "BroadcastChannel - send returns receiver count" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    _ = ch.subscribe();
    _ = ch.subscribe();

    const result = ch.send(42);
    switch (result) {
        .ok => |count| try std.testing.expectEqual(@as(usize, 2), count),
        .closed => try std.testing.expect(false),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RecvFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "RecvFuture - immediate value" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var rx = ch.subscribe();

    // Send a value before receiving
    _ = ch.send(42);

    // Create future and poll - should receive immediately
    var future = rx.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    switch (result.ready) {
        .value => |v| try std.testing.expectEqual(@as(u32, 42), v),
        else => try std.testing.expect(false),
    }
}

test "RecvFuture - waits for value" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();
    var waker_called = false;

    // Create a test waker that tracks calls
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

    var rx = ch.subscribe();

    // Create future - first poll should return pending (no value yet)
    var future = rx.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Send value - should wake the future
    _ = ch.send(123);
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    switch (result2.ready) {
        .value => |v| try std.testing.expectEqual(@as(u32, 123), v),
        else => try std.testing.expect(false),
    }
}

test "RecvFuture - cancel removes from queue" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var rx = ch.subscribe();

    // Create future and poll to add to queue
    var future = rx.recvFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());

    // Cancel
    future.cancel();

    // Clean up
    future.deinit();
}

test "RecvFuture - closed channel" {
    var ch = try BroadcastChannel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    var rx = ch.subscribe();

    // Create future and poll to add to queue
    var future = rx.recvFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());

    // Close channel
    ch.close();

    // Second poll should return closed
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(result2.ready == .closed);

    future.deinit();
}

test "RecvFuture - is valid Future type" {
    try std.testing.expect(future_mod.isFuture(BroadcastChannel(u32).RecvFuture));
    try std.testing.expect(BroadcastChannel(u32).RecvFuture.Output == BroadcastChannel(u32).Receiver.RecvResult);
}

//! Oneshot - Single-Value Channel
//!
//! A channel for sending a single value from one task to another.
//! The sender can send exactly one value, and the receiver receives it.
//!
//! ## Usage
//!
//! ```zig
//! // Preferred: Use factory function (see io.channel.oneshot)
//! var os = io.channel.oneshot(u32);
//!
//! // Sender side:
//! _ = os.sender.send(42);  // Sends value, returns true if delivered
//!
//! // Receiver side (non-blocking):
//! if (os.receiver.tryRecv()) |value| {
//!     // Got value immediately
//! }
//!
//! // Receiver side (async with waiter):
//! var waiter = Oneshot(u32).RecvWaiter.init();
//! waiter.setWaker(ctx, myWaker);
//! if (!os.receiver.recvWait(&waiter)) {
//!     // Yield to scheduler, will be woken when value arrives
//! }
//! if (waiter.value) |value| {
//!     // Use value
//! } else if (waiter.closed) {
//!     // Sender closed without sending
//! }
//! ```
//!
//! ## Alternative: Manual Shared State
//!
//! For advanced use cases where you need to control the shared state allocation:
//!
//! ```zig
//! var shared = Oneshot(u32).Shared.init();
//! var pair = Oneshot(u32).fromShared(&shared);
//! ```
//!
//! ## Design
//!
//! - Zero-allocation: sender/receiver are value types
//! - State machine tracks: empty, value_sent, receiver_waiting, closed
//! - Thread-safe through atomic state transitions
//!

const std = @import("std");

const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────

/// Channel state
pub const State = enum(u8) {
    /// Channel is empty, no value sent
    empty,
    /// Value has been sent, waiting for receiver
    value_sent,
    /// Receiver is waiting for value
    receiver_waiting,
    /// Channel is closed (sender dropped without sending)
    closed,
};

// ─────────────────────────────────────────────────────────────────────────────
// Oneshot
// ─────────────────────────────────────────────────────────────────────────────

/// A oneshot channel for sending a single value.
pub fn Oneshot(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Shared state between sender and receiver
        pub const Shared = struct {
            /// Channel state
            state: std.atomic.Value(u8),

            /// The value (valid when state == .value_sent)
            value: ?T,

            /// Waiting receiver (valid when state == .receiver_waiting)
            /// We store the waiter pointer so sender can set waiter.value directly
            waiter: ?*RecvWaiter,

            /// Mutex for state transitions
            mutex: std.Thread.Mutex,

            pub fn init() Shared {
                return .{
                    .state = std.atomic.Value(u8).init(@intFromEnum(State.empty)),
                    .value = null,
                    .waiter = null,
                    .mutex = .{},
                };
            }
        };

        /// Waiter for receive operation
        pub const RecvWaiter = struct {
            /// Received value (set when notified)
            value: ?T = null,

            /// Error if channel closed (atomic for cross-thread visibility)
            closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

            /// Waker to invoke when value arrives
            waker: ?WakerFn = null,
            waker_ctx: ?*anyopaque = null,

            pub fn init() RecvWaiter {
                return .{};
            }

            pub fn setWaker(self: *RecvWaiter, ctx: *anyopaque, wake_fn: WakerFn) void {
                self.waker_ctx = ctx;
                self.waker = wake_fn;
            }

            pub fn isComplete(self: *const RecvWaiter) bool {
                return self.value != null or self.closed.load(.acquire);
            }

            pub fn reset(self: *RecvWaiter) void {
                self.value = null;
                self.closed.store(false, .release);
                self.waker = null;
                self.waker_ctx = null;
            }
        };

        /// Function pointer type for waking
        pub const WakerFn = *const fn (*anyopaque) void;

        /// Sender half of the channel
        pub const Sender = struct {
            shared: *Shared,
            sent: bool = false,

            /// Send a value through the channel.
            /// Returns true if value was delivered (or will be), false if receiver is gone.
            pub fn send(self: *Sender, value: T) bool {
                if (self.sent) return false;
                self.sent = true;

                self.shared.mutex.lock();

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                switch (state) {
                    .empty => {
                        // No receiver waiting, store value
                        self.shared.value = value;
                        self.shared.state.store(@intFromEnum(State.value_sent), .seq_cst);
                        self.shared.mutex.unlock();
                        return true;
                    },
                    .receiver_waiting => {
                        // Receiver is waiting, deliver directly to waiter
                        // Copy waiter pointer and waker info BEFORE setting value
                        // (to avoid use-after-free if waiter checks value and destroys itself)
                        const waiter = self.shared.waiter.?;
                        const waker = waiter.waker;
                        const ctx = waiter.waker_ctx;

                        // Store value in both places
                        self.shared.value = value;
                        waiter.value = value;

                        // Clear waiter pointer and update state
                        self.shared.waiter = null;
                        self.shared.state.store(@intFromEnum(State.value_sent), .seq_cst);

                        self.shared.mutex.unlock();

                        // Wake receiver outside lock using copied waker
                        if (waker) |wf| {
                            if (ctx) |c| {
                                wf(c);
                            }
                        }
                        return true;
                    },
                    .closed, .value_sent => {
                        // Already closed or sent (shouldn't happen)
                        self.shared.mutex.unlock();
                        return false;
                    },
                }
            }

            /// Check if receiver is still alive.
            pub fn isReceiverAlive(self: *const Sender) bool {
                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));
                return state != .closed;
            }

            /// Close without sending (signals error to receiver).
            pub fn close(self: *Sender) void {
                if (self.sent) return;
                self.sent = true;

                self.shared.mutex.lock();

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                if (state == .receiver_waiting) {
                    // Copy waiter pointer and waker info BEFORE setting closed flag
                    const waiter = self.shared.waiter.?;
                    const waker = waiter.waker;
                    const ctx = waiter.waker_ctx;

                    // Set closed flag on waiter (atomic for cross-thread visibility)
                    waiter.closed.store(true, .release);

                    // Clear waiter pointer and update state
                    self.shared.waiter = null;
                    self.shared.state.store(@intFromEnum(State.closed), .seq_cst);

                    self.shared.mutex.unlock();

                    // Wake receiver outside lock using copied waker
                    if (waker) |wf| {
                        if (ctx) |c| {
                            wf(c);
                        }
                    }
                } else {
                    self.shared.state.store(@intFromEnum(State.closed), .seq_cst);
                    self.shared.mutex.unlock();
                }
            }
        };

        /// Receiver half of the channel
        pub const Receiver = struct {
            shared: *Shared,
            received: bool = false,

            /// Try to receive without waiting.
            /// Returns the value if available, null otherwise.
            pub fn tryRecv(self: *Receiver) ?T {
                if (self.received) return null;

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                if (state == .value_sent) {
                    self.received = true;
                    return self.shared.value;
                }

                return null;
            }

            /// Try to receive, returning error info.
            pub const TryRecvResult = union(enum) {
                value: T,
                empty: void,
                closed: void,
            };

            pub fn tryRecvResult(self: *Receiver) TryRecvResult {
                if (self.received) return .{ .closed = {} };

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                switch (state) {
                    .value_sent => {
                        self.received = true;
                        return .{ .value = self.shared.value.? };
                    },
                    .closed => {
                        self.received = true;
                        return .{ .closed = {} };
                    },
                    else => return .{ .empty = {} },
                }
            }

            /// Receive, potentially waiting. Prefer `recv(io)` or `recvFuture()`.
            /// Returns true if value was received immediately.
            /// Returns false if receiver is now waiting (task should yield).
            ///
            /// After waking, check waiter.value or waiter.closed.
            pub fn recvWait(self: *Receiver, waiter: *RecvWaiter) bool {
                if (self.received) {
                    waiter.closed.store(true, .release);
                    return true;
                }

                self.shared.mutex.lock();

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                switch (state) {
                    .value_sent => {
                        self.received = true;
                        waiter.value = self.shared.value;
                        self.shared.mutex.unlock();
                        return true;
                    },
                    .closed => {
                        self.received = true;
                        waiter.closed.store(true, .release);
                        self.shared.mutex.unlock();
                        return true;
                    },
                    .empty => {
                        // Register waiter (store pointer so sender can set waiter.value)
                        self.shared.waiter = waiter;
                        self.shared.state.store(@intFromEnum(State.receiver_waiting), .seq_cst);
                        self.shared.mutex.unlock();
                        return false;
                    },
                    .receiver_waiting => {
                        // Already waiting (shouldn't happen)
                        self.shared.mutex.unlock();
                        return false;
                    },
                }
            }

            /// Cancel a pending receive.
            pub fn cancelRecv(self: *Receiver) void {
                self.shared.mutex.lock();

                const state: State = @enumFromInt(self.shared.state.load(.seq_cst));

                if (state == .receiver_waiting) {
                    self.shared.waiter = null;
                    self.shared.state.store(@intFromEnum(State.empty), .seq_cst);
                }

                self.shared.mutex.unlock();
            }

            /// Close the receiver (signals to sender).
            pub fn close(self: *Receiver) void {
                if (self.received) return;
                self.received = true;

                self.shared.mutex.lock();
                self.shared.state.store(@intFromEnum(State.closed), .seq_cst);
                self.shared.mutex.unlock();
            }

            /// Receive the value, blocking the current task until sent.
            ///
            /// Requires an `Io` handle. For manual Future control, use `recvFuture()`.
            ///
            /// ## Example
            ///
            /// ```zig
            /// switch (receiver.recv(io)) {
            ///     .value => |v| { /* use v */ },
            ///     .closed => { /* sender closed without sending */ },
            /// }
            /// ```
            pub fn recv(self: *Receiver, io: @import("../Io.zig")) RecvFuture.Output {
                var f = io.awaitFuture(self.recvFuture()) catch return .closed;
                return f.await(io);
            }

            /// Receive asynchronously.
            ///
            /// Returns a `RecvFuture` that resolves when a value is received
            /// or the sender is closed. Prefer `recv(io)` unless you need
            /// manual Future control.
            ///
            /// ## Example
            ///
            /// ```zig
            /// var future = receiver.recvFuture();
            /// // Poll through your runtime...
            /// // When future.poll() returns .ready, check the result
            /// switch (result) {
            ///     .value => |v| { /* use v */ },
            ///     .closed => { /* sender closed without sending */ },
            /// }
            /// ```
            pub fn recvFuture(self: *Receiver) RecvFuture {
                return RecvFuture.init(self);
            }
        };

        /// Future for async receive
        pub const RecvFuture = struct {
            const FutureSelf = @This();
            pub const Output = RecvResult;

            /// Result of receiving
            pub const RecvResult = union(enum) {
                value: T,
                closed,
            };

            receiver: *Receiver,
            waiter: RecvWaiter,
            state: FutureState,
            stored_waker: ?Waker,

            const FutureState = enum { init, waiting, ready };

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
            /// Returns `.pending` if value not yet received (task will be woken when available).
            /// Returns `.{ .ready = .{ .value = v } }` when value is received.
            /// Returns `.{ .ready = .closed }` when sender closed without sending.
            pub fn poll(self: *FutureSelf, ctx: *Context) PollResult(RecvResult) {
                switch (self.state) {
                    .init => {
                        // First poll - try to receive
                        // Store the waker so we can be woken when send() delivers the value
                        self.stored_waker = ctx.getWaker().clone();

                        // Set up the waiter's callback to wake us via our stored waker
                        self.waiter.setWaker(@ptrCast(self), wakeCallback);

                        // Try to receive
                        if (self.receiver.recvWait(&self.waiter)) {
                            // Completed immediately
                            self.state = .ready;
                            // Clean up stored waker since we don't need it
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            if (self.waiter.value) |v| {
                                return .{ .ready = .{ .value = v } };
                            } else {
                                return .{ .ready = .closed };
                            }
                        } else {
                            // Added to wait queue - will be woken when value available
                            self.state = .waiting;
                            return .pending;
                        }
                    },

                    .waiting => {
                        // Check if we've received the value or sender closed
                        if (self.waiter.isComplete()) {
                            self.state = .ready;
                            // Clean up stored waker
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            if (self.waiter.value) |v| {
                                return .{ .ready = .{ .value = v } };
                            } else {
                                return .{ .ready = .closed };
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
                        // Already have the value
                        if (self.waiter.value) |v| {
                            return .{ .ready = .{ .value = v } };
                        } else {
                            return .{ .ready = .closed };
                        }
                    },
                }
            }

            /// Callback invoked by Sender.send() when value is delivered
            /// This bridges the Oneshot's WakerFn to the Future's Waker
            fn wakeCallback(ctx_ptr: *anyopaque) void {
                const self_ptr: *FutureSelf = @ptrCast(@alignCast(ctx_ptr));
                if (self_ptr.stored_waker) |*w| {
                    // Wake the task through the Future system's Waker
                    w.wakeByRef();
                }
            }

            /// Cancel the receive operation
            ///
            /// If the value hasn't been received yet, removes us from the wait queue.
            pub fn cancel(self: *FutureSelf) void {
                if (self.state == .waiting) {
                    self.receiver.cancelRecv();
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

        /// Create a new oneshot channel.
        /// Returns the shared state and sender/receiver pair.
        pub fn init() struct { shared: Shared, sender: Sender, receiver: Receiver } {
            var result: struct { shared: Shared, sender: Sender, receiver: Receiver } = undefined;
            result.shared = Shared.init();
            result.sender = .{ .shared = &result.shared };
            result.receiver = .{ .shared = &result.shared };
            return result;
        }

        /// Create sender and receiver from existing shared state.
        /// Use this when shared state is allocated separately.
        pub fn fromShared(shared: *Shared) struct { sender: Sender, receiver: Receiver } {
            return .{
                .sender = .{ .shared = shared },
                .receiver = .{ .shared = shared },
            };
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Oneshot - send then receive" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);

    // Send value
    try std.testing.expect(pair.sender.send(42));

    // Receive should get it immediately
    const value = pair.receiver.tryRecv();
    try std.testing.expectEqual(@as(?u32, 42), value);
}

test "Oneshot - receive then send" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Try receive - should be empty
    try std.testing.expect(pair.receiver.tryRecv() == null);

    // Start waiting
    var waiter = Ch.RecvWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    try std.testing.expect(!pair.receiver.recvWait(&waiter));
    try std.testing.expect(!waiter.isComplete());

    // Send value
    try std.testing.expect(pair.sender.send(42));
    try std.testing.expect(woken);

    // Waiter should now have value (need to check shared state)
    try std.testing.expectEqual(@as(?u32, 42), shared.value);
}

test "Oneshot - sender close wakes receiver" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Start waiting
    var waiter = Ch.RecvWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    _ = pair.receiver.recvWait(&waiter);

    // Close sender
    pair.sender.close();
    try std.testing.expect(woken);

    // State should be closed
    const state: State = @enumFromInt(shared.state.load(.acquire));
    try std.testing.expectEqual(State.closed, state);
}

test "Oneshot - double send fails" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);

    try std.testing.expect(pair.sender.send(1));
    try std.testing.expect(!pair.sender.send(2)); // Should fail
}

test "Oneshot - tryRecvResult" {
    const Ch = Oneshot(u32);

    // Test empty
    {
        var shared = Ch.Shared.init();
        var pair = Ch.fromShared(&shared);
        const result = pair.receiver.tryRecvResult();
        try std.testing.expect(result == .empty);
    }

    // Test value
    {
        var shared = Ch.Shared.init();
        var pair = Ch.fromShared(&shared);
        _ = pair.sender.send(42);
        const result = pair.receiver.tryRecvResult();
        try std.testing.expectEqual(Ch.Receiver.TryRecvResult{ .value = 42 }, result);
    }

    // Test closed
    {
        var shared = Ch.Shared.init();
        var pair = Ch.fromShared(&shared);
        pair.sender.close();
        const result = pair.receiver.tryRecvResult();
        try std.testing.expect(result == .closed);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RecvFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "RecvFuture - immediate value reception" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);

    // Send value first
    _ = pair.sender.send(42);

    // Create future and poll - should receive immediately
    var future = pair.receiver.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(Ch.RecvFuture.RecvResult{ .value = 42 }, result.ready);
}

test "RecvFuture - waits for value" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);
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

    // Create future - first poll should return pending
    var future = pair.receiver.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Send value - should wake the future
    _ = pair.sender.send(42);
    try std.testing.expect(waker_called);

    // Second poll should return ready with value
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expectEqual(Ch.RecvFuture.RecvResult{ .value = 42 }, result2.ready);
}

test "RecvFuture - sender close returns closed" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);
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

    // Create future - first poll should return pending
    var future = pair.receiver.recvFuture();
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());

    // Close sender - should wake the future
    pair.sender.close();
    try std.testing.expect(waker_called);

    // Second poll should return ready with closed
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expect(result2.ready == .closed);
}

test "RecvFuture - cancel removes waiter" {
    const Ch = Oneshot(u32);
    var shared = Ch.Shared.init();
    var pair = Ch.fromShared(&shared);

    // Create future and poll to start waiting
    var future = pair.receiver.recvFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());

    // State should be receiver_waiting
    const state_before: State = @enumFromInt(shared.state.load(.acquire));
    try std.testing.expectEqual(State.receiver_waiting, state_before);

    // Cancel
    future.cancel();

    // State should be back to empty
    const state_after: State = @enumFromInt(shared.state.load(.acquire));
    try std.testing.expectEqual(State.empty, state_after);

    // Clean up
    future.deinit();
}

test "RecvFuture - is valid Future type" {
    const Ch = Oneshot(u32);
    try std.testing.expect(future_mod.isFuture(Ch.RecvFuture));
    try std.testing.expect(Ch.RecvFuture.Output == Ch.RecvFuture.RecvResult);
}

//! Watch Channel - Single Value with Change Notification
//!
//! A watch channel holds a single value. The sender can update the value,
//! and multiple receivers can watch for changes. Only the latest value is kept.
//!
//! ## Usage
//!
//! ```zig
//! var watch = Watch(Config).init(default_config);
//! defer watch.deinit();
//!
//! // Sender updates value
//! watch.send(new_config);
//!
//! // Receivers watch for changes
//! var rx = watch.subscribe();
//! if (rx.hasChanged()) {
//!     const config = rx.borrow();
//!     // Use config
//!     rx.markSeen();
//! }
//! ```
//!
//! ## Design
//!
//! - Single value storage with version number
//! - Receivers track last seen version
//! - No history - only latest value kept
//! - Good for configuration/state broadcasting
//!

const std = @import("std");

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;

// Future system imports
const future_mod = @import("../future.zig");
const Waker = future_mod.Waker;
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;
const WakeList = @import("../internal/util/wake_list.zig").WakeList;
const InvocationId = @import("../internal/util/invocation_id.zig").InvocationId;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for change notification
pub const ChangeWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether notification was received (atomic for cross-thread visibility)
    notified: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Whether channel was closed (atomic for cross-thread visibility)
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    pointers: Pointers(ChangeWaiter) = .{},
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
        return self.notified.load(.acquire) or self.closed.load(.acquire);
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
        self.notified.store(false, .release);
        self.closed.store(false, .release);
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }
};

const ChangeWaiterList = LinkedList(ChangeWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Watch
// ─────────────────────────────────────────────────────────────────────────────

/// A watch channel for a single value.
pub fn Watch(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Current value
        value: T,

        /// Version number (increments on each update)
        version: u64,

        /// Number of receivers
        receiver_count: usize,

        /// Whether the sender is closed
        closed: bool,

        /// Mutex protecting internal state
        mutex: std.Thread.Mutex,

        /// Waiters for change notification
        waiters: ChangeWaiterList,

        /// Receiver handle
        pub const Receiver = struct {
            watch: *Self,
            /// Last seen version
            seen_version: u64,

            /// Check if value has changed since last seen.
            pub fn hasChanged(self: *const Receiver) bool {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                return self.seen_version < self.watch.version;
            }

            /// Borrow the current value (read-only reference).
            ///
            /// WARNING: Not thread-safe. The returned pointer is not protected
            /// by any lock. Only safe when no concurrent `send()` calls are
            /// possible (e.g., single-writer scenarios or after the sender is
            /// dropped). For thread-safe access, use `get()` which holds the
            /// mutex, or `getAndUpdate()` which also marks the value as seen.
            pub fn borrow(self: *const Receiver) *const T {
                return &self.watch.value;
            }

            /// Get a copy of the current value.
            pub fn get(self: *const Receiver) T {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                return self.watch.value;
            }

            /// Get the current value and mark as seen.
            pub fn getAndUpdate(self: *Receiver) T {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                self.seen_version = self.watch.version;
                return self.watch.value;
            }

            /// Mark current value as seen.
            pub fn markSeen(self: *Receiver) void {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                self.seen_version = self.watch.version;
            }

            /// Wait for a change. Prefer `changed(io)` or `changedFuture()`.
            /// Returns true if change detected immediately.
            /// Returns false if waiting (task should yield).
            pub fn changedWait(self: *Receiver, waiter: *ChangeWaiter) bool {
                self.watch.mutex.lock();

                // Check if already changed
                if (self.seen_version < self.watch.version) {
                    self.watch.mutex.unlock();
                    waiter.notified.store(true, .release);
                    return true;
                }

                // Check if closed
                if (self.watch.closed) {
                    self.watch.mutex.unlock();
                    waiter.closed.store(true, .release);
                    return true;
                }

                // Wait for change
                waiter.notified.store(false, .release);
                waiter.closed.store(false, .release);
                self.watch.waiters.pushBack(waiter);

                self.watch.mutex.unlock();
                return false;
            }

            /// Cancel pending wait for `changedWait()`.
            pub fn cancelChangedWait(self: *Receiver, waiter: *ChangeWaiter) void {
                if (waiter.isComplete()) return;

                self.watch.mutex.lock();

                if (waiter.isComplete()) {
                    self.watch.mutex.unlock();
                    return;
                }

                if (ChangeWaiterList.isLinked(waiter) or self.watch.waiters.front() == waiter) {
                    self.watch.waiters.remove(waiter);
                    waiter.pointers.reset();
                }

                self.watch.mutex.unlock();
            }

            /// Check if sender is closed.
            pub fn isClosed(self: *const Receiver) bool {
                self.watch.mutex.lock();
                defer self.watch.mutex.unlock();
                return self.watch.closed;
            }

            /// Wait for the value to change, blocking the current task.
            ///
            /// Requires an `Io` handle. For manual Future control, use `changedFuture()`.
            ///
            /// ## Example
            ///
            /// ```zig
            /// switch (receiver.changed(io)) {
            ///     .changed => {
            ///         const value = receiver.getAndUpdate();
            ///         // Use value...
            ///     },
            ///     .closed => {
            ///         // Sender closed
            ///     },
            /// }
            /// ```
            pub fn changed(self: *Receiver, io: @import("../Io.zig")) ChangedFuture.Output {
                var f = io.awaitFuture(self.changedFuture()) catch return .closed;
                return f.await(io);
            }

            /// Wait for a change asynchronously.
            ///
            /// Returns a `ChangedFuture` that resolves when a change is detected or
            /// the sender is closed. This integrates with the scheduler for true async
            /// behavior - the task will be suspended until a change occurs.
            /// Prefer `changed(io)` unless you need manual Future control.
            ///
            /// ## Example
            ///
            /// ```zig
            /// var future = receiver.changedFuture();
            /// // Poll through your runtime...
            /// switch (future.poll(&ctx)) {
            ///     .ready => |result| switch (result) {
            ///         .changed => {
            ///             const value = receiver.getAndUpdate();
            ///             // Use value...
            ///         },
            ///         .closed => {
            ///             // Sender closed
            ///         },
            ///     },
            ///     .pending => {
            ///         // Will be woken when change occurs
            ///     },
            /// }
            /// ```
            pub fn changedFuture(self: *Receiver) ChangedFuture {
                return ChangedFuture.init(self);
            }
        };

        /// Future for async change notification.
        ///
        /// This integrates with the scheduler's Future system for true async behavior.
        /// The task will be suspended (not spin-waiting or blocking) until a change
        /// is available or the sender is closed.
        ///
        /// ## Usage
        ///
        /// ```zig
        /// var future = receiver.changedFuture();
        /// // ... poll the future through your runtime ...
        /// // When ready, check if it was a change or close
        /// ```
        pub const ChangedFuture = struct {
            const FutureSelf = @This();

            /// Output type for Future trait
            pub const Output = ChangedResult;

            /// Result of waiting for change
            pub const ChangedResult = union(enum) {
                /// A change was detected
                changed,
                /// The sender was closed
                closed,
            };

            /// Reference to the receiver we're watching
            receiver: *Receiver,

            /// Our waiter node (embedded to avoid allocation)
            waiter: ChangeWaiter,

            /// State machine for the future
            state: FutureState,

            /// Stored waker for when we're woken by send()/close()
            stored_waker: ?Waker,

            const FutureState = enum {
                /// Haven't tried to check for change yet
                init,
                /// Waiting for change
                waiting,
                /// Change detected or closed
                ready,
            };

            /// Initialize a new changed future
            pub fn init(receiver: *Receiver) FutureSelf {
                return .{
                    .receiver = receiver,
                    .waiter = ChangeWaiter.init(),
                    .state = .init,
                    .stored_waker = null,
                };
            }

            /// Poll the future - implements Future trait
            ///
            /// Returns `.pending` if no change yet (task will be woken when available).
            /// Returns `.{ .ready = .changed }` when a change is detected.
            /// Returns `.{ .ready = .closed }` when the sender is closed.
            pub fn poll(self: *FutureSelf, ctx: *Context) PollResult(ChangedResult) {
                switch (self.state) {
                    .init => {
                        // Cooperative budgeting: yield if this task has consumed its budget
                        if (!ctx.pollProceed()) return .pending;

                        // First poll - try to detect change
                        // Store the waker so we can be woken when send()/close() happens
                        self.stored_waker = ctx.getWaker().clone();

                        // Set up the waiter's callback to wake us via our stored waker
                        self.waiter.setWaker(@ptrCast(self), wakeCallback);

                        // Try to detect change
                        if (self.receiver.changedWait(&self.waiter)) {
                            // Completed immediately
                            self.state = .ready;
                            // Clean up stored waker since we don't need it
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            if (self.waiter.closed.load(.acquire)) {
                                return .{ .ready = .closed };
                            }
                            return .{ .ready = .changed };
                        } else {
                            // Added to wait queue - will be woken when change available
                            self.state = .waiting;
                            return .pending;
                        }
                    },

                    .waiting => {
                        // Check if we've been notified
                        if (self.waiter.isComplete()) {
                            self.state = .ready;
                            // Clean up stored waker
                            if (self.stored_waker) |*w| {
                                w.deinit();
                                self.stored_waker = null;
                            }
                            if (self.waiter.closed.load(.acquire)) {
                                return .{ .ready = .closed };
                            }
                            return .{ .ready = .changed };
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
                        // Already completed
                        if (self.waiter.closed.load(.acquire)) {
                            return .{ .ready = .closed };
                        }
                        return .{ .ready = .changed };
                    },
                }
            }

            /// Callback invoked by Watch.send()/close() when change occurs
            /// This bridges the Watch's WakerFn to the Future's Waker
            fn wakeCallback(ctx_ptr: *anyopaque) void {
                const self_ptr: *FutureSelf = @ptrCast(@alignCast(ctx_ptr));
                if (self_ptr.stored_waker) |*w| {
                    // Wake the task through the Future system's Waker
                    w.wakeByRef();
                }
            }

            /// Cancel the change wait
            ///
            /// If the change hasn't been detected yet, removes us from the wait queue.
            /// If already completed, this is a no-op.
            pub fn cancel(self: *FutureSelf) void {
                if (self.state == .waiting) {
                    self.receiver.cancelChangedWait(&self.waiter);
                }
                // Clean up stored waker
                if (self.stored_waker) |*w| {
                    w.deinit();
                    self.stored_waker = null;
                }
            }

            /// Deinit the future
            ///
            /// If waiting, cancels the change wait.
            pub fn deinit(self: *FutureSelf) void {
                self.cancel();
            }
        };

        /// Create a new watch channel with initial value.
        pub fn init(initial: T) Self {
            return .{
                .value = initial,
                .version = 1, // Start at 1 so receivers can detect initial value
                .receiver_count = 0,
                .closed = false,
                .mutex = .{},
                .waiters = .{},
            };
        }

        /// No cleanup needed (no allocations).
        pub fn deinit(self: *Self) void {
            _ = self;
        }

        /// Subscribe to changes.
        pub fn subscribe(self: *Self) Receiver {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.receiver_count += 1;
            return .{
                .watch = self,
                .seen_version = 0, // Will see initial value as "changed"
            };
        }

        /// Subscribe but mark current value as already seen.
        pub fn subscribeNoInitial(self: *Self) Receiver {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.receiver_count += 1;
            return .{
                .watch = self,
                .seen_version = self.version,
            };
        }

        /// Send a new value, waking all waiters.
        pub fn send(self: *Self, value: T) void {
            var wake_list: WakeList(64) = .{};

            self.mutex.lock();

            self.value = value;
            self.version +%= 1;

            // Wake all waiters
            // CRITICAL: Copy waker info BEFORE setting notified flag to avoid use-after-free
            while (self.waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.notified.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }

            self.mutex.unlock();

            wake_list.wakeAll();
        }

        /// Modify the value in place.
        pub fn sendModify(self: *Self, modify_fn: *const fn (*T) void) void {
            var wake_list: WakeList(64) = .{};

            self.mutex.lock();

            modify_fn(&self.value);
            self.version +%= 1;

            // CRITICAL: Copy waker info BEFORE setting notified flag to avoid use-after-free
            while (self.waiters.popFront()) |w| {
                const waker_fn = w.waker;
                const waker_ctx = w.waker_ctx;
                w.notified.store(true, .release);
                if (waker_fn) |wf| {
                    if (waker_ctx) |ctx| {
                        wake_list.push(.{ .context = ctx, .wake_fn = wf });
                    }
                }
            }

            self.mutex.unlock();

            wake_list.wakeAll();
        }

        /// Borrow current value (read-only).
        pub fn borrow(self: *const Self) *const T {
            return &self.value;
        }

        /// Close the sender, waking all waiters.
        pub fn close(self: *Self) void {
            var wake_list: WakeList(64) = .{};

            self.mutex.lock();

            if (self.closed) {
                self.mutex.unlock();
                return;
            }

            self.closed = true;

            // CRITICAL: Copy waker info BEFORE setting closed flag to avoid use-after-free
            while (self.waiters.popFront()) |w| {
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

test "Watch - initial value" {
    var watch = Watch(u32).init(42);
    defer watch.deinit();

    var rx = watch.subscribe();

    // New subscriber sees initial value as changed
    try std.testing.expect(rx.hasChanged());
    try std.testing.expectEqual(@as(u32, 42), rx.get());

    rx.markSeen();
    try std.testing.expect(!rx.hasChanged());
}

test "Watch - send updates value" {
    var watch = Watch(u32).init(1);
    defer watch.deinit();

    var rx = watch.subscribe();
    rx.markSeen();

    try std.testing.expect(!rx.hasChanged());

    watch.send(2);

    try std.testing.expect(rx.hasChanged());
    try std.testing.expectEqual(@as(u32, 2), rx.getAndUpdate());
    try std.testing.expect(!rx.hasChanged());
}

test "Watch - multiple receivers" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();

    var rx1 = watch.subscribe();
    var rx2 = watch.subscribe();
    rx1.markSeen();
    rx2.markSeen();

    watch.send(100);

    // Both see the change
    try std.testing.expect(rx1.hasChanged());
    try std.testing.expect(rx2.hasChanged());
    try std.testing.expectEqual(@as(u32, 100), rx1.get());
    try std.testing.expectEqual(@as(u32, 100), rx2.get());
}

test "Watch - wait for change" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var rx = watch.subscribe();
    rx.markSeen();

    var waiter = ChangeWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    // Should wait (no change yet)
    const immediate = rx.changedWait(&waiter);
    try std.testing.expect(!immediate);

    // Send wakes waiter
    watch.send(42);
    try std.testing.expect(woken);
    try std.testing.expect(waiter.notified.load(.acquire));
}

test "Watch - close wakes waiters" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    var rx = watch.subscribe();
    rx.markSeen();

    var waiter = ChangeWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);
    _ = rx.changedWait(&waiter);

    watch.close();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.closed.load(.acquire));
    try std.testing.expect(rx.isClosed());
}

test "Watch - subscribeNoInitial" {
    var watch = Watch(u32).init(42);
    defer watch.deinit();

    var rx = watch.subscribeNoInitial();

    // Should not see initial value as changed
    try std.testing.expect(!rx.hasChanged());

    watch.send(100);
    try std.testing.expect(rx.hasChanged());
}

test "Watch - sendModify" {
    var watch = Watch(u32).init(10);
    defer watch.deinit();

    var rx = watch.subscribe();
    rx.markSeen();

    const double = struct {
        fn f(val: *u32) void {
            val.* *= 2;
        }
    }.f;

    watch.sendModify(double);

    try std.testing.expect(rx.hasChanged());
    try std.testing.expectEqual(@as(u32, 20), rx.get());
}

// ─────────────────────────────────────────────────────────────────────────────
// ChangedFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "ChangedFuture - immediate change detection" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();

    var rx = watch.subscribe();
    // Don't mark seen - initial value should be detected as change

    // Create future and poll - should detect change immediately
    var f = rx.changedFuture();
    defer f.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isReady());
    switch (result) {
        .ready => |r| try std.testing.expectEqual(Watch(u32).ChangedFuture.ChangedResult.changed, r),
        .pending => unreachable,
    }
}

test "ChangedFuture - waits when no change" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();
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

    var rx = watch.subscribe();
    rx.markSeen(); // Mark as seen so no immediate change

    // Create future - first poll should return pending
    var f = rx.changedFuture();
    defer f.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = f.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Send a value - should wake the future
    watch.send(42);
    try std.testing.expect(waker_called);

    // Second poll should return ready with .changed
    const result2 = f.poll(&ctx);
    try std.testing.expect(result2.isReady());
    switch (result2) {
        .ready => |r| try std.testing.expectEqual(Watch(u32).ChangedFuture.ChangedResult.changed, r),
        .pending => unreachable,
    }
}

test "ChangedFuture - close wakes with closed result" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();
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

    var rx = watch.subscribe();
    rx.markSeen();

    // Create future - first poll should return pending
    var f = rx.changedFuture();
    defer f.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = f.poll(&ctx);

    try std.testing.expect(result1.isPending());

    // Close the sender - should wake with closed
    watch.close();
    try std.testing.expect(waker_called);

    // Second poll should return ready with .closed
    const result2 = f.poll(&ctx);
    try std.testing.expect(result2.isReady());
    switch (result2) {
        .ready => |r| try std.testing.expectEqual(Watch(u32).ChangedFuture.ChangedResult.closed, r),
        .pending => unreachable,
    }
}

test "ChangedFuture - cancel removes from queue" {
    var watch = Watch(u32).init(0);
    defer watch.deinit();

    var rx = watch.subscribe();
    rx.markSeen();

    // Create future and poll to add to queue
    var f = rx.changedFuture();
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = f.poll(&ctx);

    try std.testing.expect(result.isPending());

    // Cancel should remove from wait queue
    f.cancel();

    // Clean up
    f.deinit();
}

test "ChangedFuture - is valid Future type" {
    const ChangedFuture = Watch(u32).ChangedFuture;
    try std.testing.expect(future_mod.isFuture(ChangedFuture));
    try std.testing.expect(ChangedFuture.Output == ChangedFuture.ChangedResult);
}

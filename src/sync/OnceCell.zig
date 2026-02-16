//! OnceCell - Lazy One-Time Initialization
//!
//! A cell that can be initialized exactly once. Multiple tasks can race to
//! initialize, but only one succeeds. All waiters get the same value.
//!
//! ## Usage
//!
//! ```zig
//! var cell = OnceCell(ExpensiveResource).init();
//!
//! // Multiple tasks can call getOrInit concurrently
//! const resource = cell.getOrInit(initExpensiveResource);
//! // First caller initializes, others wait and get same value
//! ```
//!
//! ## Design
//!
//! - State machine: EMPTY -> INITIALIZING -> INITIALIZED
//! - First caller to transition to INITIALIZING does the init
//! - Other callers wait until INITIALIZED
//! - Value is stored inline (no allocation)
//!

const std = @import("std");

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
// State
// ─────────────────────────────────────────────────────────────────────────────

pub const State = enum(u8) {
    empty,
    initializing,
    initialized,
};

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for initialization completion
pub const InitWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    /// Whether initialization completed (atomic for cross-thread visibility)
    complete: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    pointers: Pointers(InitWaiter) = .{},
    /// Debug-mode invocation tracking (detects use-after-free)
    invocation: InvocationId = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Create a waiter with waker already configured.
    /// Reduces the 3-step ceremony (init + setWaker + getOrInitWith) to 2 steps.
    pub fn initWithWaker(ctx: *anyopaque, wake_fn: WakerFn) Self {
        return .{
            .waker_ctx = ctx,
            .waker = wake_fn,
        };
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

    /// Check if the waiter is ready (initialization completed).
    /// This is the unified completion check method across all sync primitives.
    pub fn isReady(self: *const Self) bool {
        return self.complete.load(.acquire);
    }

    /// Check if initialization is complete (alias for isReady).
    pub const isComplete = isReady;

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
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
        self.invocation.bump();
    }
};

const InitWaiterList = LinkedList(InitWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// OnceCell
// ─────────────────────────────────────────────────────────────────────────────

/// A cell for lazy one-time initialization.
pub fn OnceCell(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Current state
        state: std.atomic.Value(u8),

        /// The value (valid when state == .initialized)
        value: T,

        /// Mutex for waiters list
        mutex: std.Thread.Mutex,

        /// Waiters for initialization
        waiters: InitWaiterList,

        /// Create an empty OnceCell.
        pub fn init() Self {
            return .{
                .state = std.atomic.Value(u8).init(@intFromEnum(State.empty)),
                .value = undefined,
                .mutex = .{},
                .waiters = .{},
            };
        }

        /// Create an already-initialized OnceCell.
        pub fn initWith(value: T) Self {
            return .{
                .state = std.atomic.Value(u8).init(@intFromEnum(State.initialized)),
                .value = value,
                .mutex = .{},
                .waiters = .{},
            };
        }

        /// No cleanup needed.
        pub fn deinit(self: *Self) void {
            _ = self;
        }

        /// Check if initialized.
        /// Uses acquire ordering to synchronize-with the release in completeInit().
        pub fn isInitialized(self: *const Self) bool {
            return @as(State, @enumFromInt(self.state.load(.acquire))) == .initialized;
        }

        /// Get the value if initialized.
        pub fn get(self: *Self) ?*T {
            if (self.isInitialized()) {
                return &self.value;
            }
            return null;
        }

        /// Get the value (const version).
        pub fn getConst(self: *const Self) ?*const T {
            if (self.isInitialized()) {
                return &self.value;
            }
            return null;
        }

        /// Try to set the value. Returns false if already initialized.
        pub fn set(self: *Self, value: T) bool {
            // Try to transition EMPTY -> INITIALIZING
            // acq_rel: acquire on read to see any prior writes, release on success
            const result = self.state.cmpxchgStrong(
                @intFromEnum(State.empty),
                @intFromEnum(State.initializing),
                .acq_rel,
                .acquire,
            );

            if (result != null) {
                // Already initializing or initialized
                return false;
            }

            // We won the race - store value
            self.value = value;

            // Transition to INITIALIZED and wake waiters
            self.completeInit();

            return true;
        }

        /// Get or init with a waiter (low-level waiter API).
        /// Returns the value if already initialized or if we initialize it.
        /// Returns null if we need to wait (waiter added to queue).
        /// For the async Future API, use `getOrInit(fn)` instead.
        pub fn getOrInitWith(
            self: *Self,
            comptime init_fn: fn () T,
            waiter: *InitWaiter,
        ) ?*T {
            // Fast path: already initialized
            if (self.isInitialized()) {
                waiter.complete.store(true, .release);
                return &self.value;
            }

            // Try to become the initializer
            const result = self.state.cmpxchgStrong(
                @intFromEnum(State.empty),
                @intFromEnum(State.initializing),
                .acq_rel,
                .acquire,
            );

            if (result == null) {
                // We won - initialize
                self.value = init_fn();
                self.completeInit();
                waiter.complete.store(true, .release);
                return &self.value;
            }

            // Check if already initialized (race between cmpxchg and now)
            if (self.isInitialized()) {
                waiter.complete.store(true, .release);
                return &self.value;
            }

            // Someone else is initializing - register waiter
            self.mutex.lock();

            // Re-check under lock
            if (self.isInitialized()) {
                self.mutex.unlock();
                waiter.complete.store(true, .release);
                return &self.value;
            }

            waiter.complete.store(false, .release);
            self.waiters.pushBack(waiter);
            self.mutex.unlock();

            return null;
        }

        /// Cancel a pending wait.
        pub fn cancelWait(self: *Self, waiter: *InitWaiter) void {
            if (waiter.isComplete()) return;

            self.mutex.lock();

            if (waiter.isComplete()) {
                self.mutex.unlock();
                return;
            }

            if (InitWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
                self.waiters.remove(waiter);
                waiter.pointers.reset();
            }

            self.mutex.unlock();
        }

        /// Complete initialization and wake waiters.
        fn completeInit(self: *Self) void {
            var wake_list: WakeList(32) = .{};

            self.mutex.lock();

            // Set to initialized - release ordering so readers see the value
            self.state.store(@intFromEnum(State.initialized), .release);

            // Wake all waiters
            // CRITICAL: Copy waker info BEFORE setting complete flag to avoid use-after-free
            while (self.waiters.popFront()) |w| {
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
        }

        // ═══════════════════════════════════════════════════════════════════════
        // Async API
        // ═══════════════════════════════════════════════════════════════════════

        /// Get or initialize the value, blocking the current task if initialization is in progress.
        pub fn getOrInit(self: *Self, io: @import("../Io.zig"), comptime init_fn: fn () T) *T {
            // Fast path: already initialized
            if (self.get()) |ptr| return ptr;
            var f = io.awaitFuture(self.getOrInitFuture(init_fn)) catch {
                // If spawn fails, try sync path
                return self.get() orelse unreachable;
            };
            return f.await(io) catch {
                return self.get() orelse unreachable;
            };
        }

        /// Get or initialize the value.
        ///
        /// Returns a `GetOrInitFuture` that resolves to a pointer to the value.
        /// This integrates with the scheduler for true async behavior - the task
        /// will be suspended (not spin-waiting) until initialization is complete.
        ///
        /// ## Example
        ///
        /// ```zig
        /// var future = cell.getOrInitFuture(initExpensiveResource);
        /// // Poll through your runtime...
        /// // When future.poll() returns .ready, you have the value pointer
        /// const value = result.ready;
        /// ```
        pub fn getOrInitFuture(self: *Self, comptime init_fn: fn () T) GetOrInitFuture(T, init_fn) {
            return GetOrInitFuture(T, init_fn).init(self);
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// GetOrInitFuture - Async initialization
// ─────────────────────────────────────────────────────────────────────────────

/// A future that resolves when the OnceCell is initialized.
///
/// This integrates with the scheduler's Future system for true async behavior.
/// The task will be suspended (not spin-waiting or blocking) until initialization
/// is complete.
///
/// ## Usage
///
/// ```zig
/// var future = cell.getOrInitFuture(initExpensiveResource);
/// // ... poll the future through your runtime ...
/// // When ready, you have a pointer to the initialized value
/// const value = result.ready;
/// ```
pub fn GetOrInitFuture(comptime T: type, comptime init_fn: fn () T) type {
    return struct {
        const Self = @This();

        /// Output type for Future trait
        pub const Output = *T;

        /// Reference to the cell we're initializing
        cell: *OnceCell(T),

        /// Our waiter node (embedded to avoid allocation)
        waiter: InitWaiter,

        /// State machine for the future
        state: FutureState,

        /// Stored waker for when we're woken by initialization completion
        stored_waker: ?Waker,

        const FutureState = enum {
            /// Haven't tried to get/init yet
            init,
            /// Waiting for initialization to complete
            waiting,
            /// Value is ready
            ready,
        };

        /// Initialize a new get-or-init future
        pub fn init(cell: *OnceCell(T)) Self {
            return .{
                .cell = cell,
                .waiter = InitWaiter.init(),
                .state = .init,
                .stored_waker = null,
            };
        }

        /// Poll the future - implements Future trait
        ///
        /// Returns `.pending` if initialization not yet complete (task will be woken when ready).
        /// Returns `.{ .ready = value_ptr }` when value is available.
        pub fn poll(self: *Self, ctx: *Context) PollResult(*T) {
            switch (self.state) {
                .init => {
                    // First poll - try to get or initialize
                    // Store the waker so we can be woken when initialization completes
                    self.stored_waker = ctx.getWaker().clone();

                    // Set up the waiter's callback to wake us via our stored waker
                    self.waiter.setWaker(@ptrCast(self), wakeCallback);

                    // Try to get or initialize
                    if (self.cell.getOrInitWith(init_fn, &self.waiter)) |value_ptr| {
                        // Got it immediately (already initialized or we initialized it)
                        self.state = .ready;
                        // Clean up stored waker since we don't need it
                        if (self.stored_waker) |*w| {
                            w.deinit();
                            self.stored_waker = null;
                        }
                        return .{ .ready = value_ptr };
                    } else {
                        // Added to wait queue - will be woken when init complete
                        self.state = .waiting;
                        return .pending;
                    }
                },

                .waiting => {
                    // Check if initialization is complete
                    if (self.waiter.isComplete()) {
                        self.state = .ready;
                        // Clean up stored waker
                        if (self.stored_waker) |*w| {
                            w.deinit();
                            self.stored_waker = null;
                        }
                        return .{ .ready = self.cell.get().? };
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
                    return .{ .ready = self.cell.get().? };
                },
            }
        }

        /// Cancel the initialization wait
        ///
        /// If initialization hasn't completed yet, removes us from the wait queue.
        /// If already ready, this is a no-op.
        pub fn cancel(self: *Self) void {
            if (self.state == .waiting) {
                self.cell.cancelWait(&self.waiter);
            }
            // Clean up stored waker
            if (self.stored_waker) |*w| {
                w.deinit();
                self.stored_waker = null;
            }
        }

        /// Deinit the future
        ///
        /// If waiting, cancels the wait.
        pub fn deinit(self: *Self) void {
            self.cancel();
        }

        /// Callback invoked when initialization completes
        /// This bridges the OnceCell's WakerFn to the Future's Waker
        fn wakeCallback(ctx_ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx_ptr));
            if (self.stored_waker) |*w| {
                // Wake the task through the Future system's Waker
                w.wakeByRef();
            }
        }
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "OnceCell - init empty" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();

    try std.testing.expect(!cell.isInitialized());
    try std.testing.expect(cell.get() == null);
}

test "OnceCell - initWith" {
    var cell = OnceCell(u32).initWith(42);
    defer cell.deinit();

    try std.testing.expect(cell.isInitialized());
    try std.testing.expectEqual(@as(u32, 42), cell.get().?.*);
}

test "OnceCell - set once" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();

    try std.testing.expect(cell.set(42));
    try std.testing.expect(cell.isInitialized());
    try std.testing.expectEqual(@as(u32, 42), cell.get().?.*);

    // Second set fails
    try std.testing.expect(!cell.set(100));
    try std.testing.expectEqual(@as(u32, 42), cell.get().?.*);
}

test "OnceCell - async wait" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();
    var woken = false;

    const TestWaker = struct {
        fn wake(ctx: *anyopaque) void {
            const w: *bool = @ptrCast(@alignCast(ctx));
            w.* = true;
        }
    };

    // Simulate another task initializing
    _ = cell.state.cmpxchgStrong(
        @intFromEnum(State.empty),
        @intFromEnum(State.initializing),
        .acq_rel,
        .acquire,
    );

    // This task tries to get
    var waiter = InitWaiter.init();
    waiter.setWaker(@ptrCast(&woken), TestWaker.wake);

    const initFn = struct {
        fn f() u32 {
            return 99;
        }
    }.f;

    const result = cell.getOrInitWith(initFn, &waiter);
    try std.testing.expect(result == null); // Should wait

    // Complete initialization
    cell.value = 42;
    cell.completeInit();

    try std.testing.expect(woken);
    try std.testing.expect(waiter.complete.load(.acquire));
    try std.testing.expectEqual(@as(u32, 42), cell.get().?.*);
}

// ─────────────────────────────────────────────────────────────────────────────
// GetOrInitFuture Tests (Async API)
// ─────────────────────────────────────────────────────────────────────────────

test "GetOrInitFuture - immediate initialization" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();

    const initFn = struct {
        fn f() u32 {
            return 42;
        }
    }.f;

    // Create future and poll - should initialize and return immediately
    var future = cell.getOrInitFuture(initFn);
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(u32, 42), result.ready.*);
    try std.testing.expect(cell.isInitialized());
}

test "GetOrInitFuture - already initialized" {
    var cell = OnceCell(u32).initWith(99);
    defer cell.deinit();

    const initFn = struct {
        fn f() u32 {
            return 42; // Should not be called
        }
    }.f;

    // Create future and poll - should return existing value immediately
    var future = cell.getOrInitFuture(initFn);
    defer future.deinit();

    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isReady());
    try std.testing.expectEqual(@as(u32, 99), result.ready.*);
}

test "GetOrInitFuture - waits for initialization" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();
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

    // Simulate another task initializing (set state to initializing)
    _ = cell.state.cmpxchgStrong(
        @intFromEnum(State.empty),
        @intFromEnum(State.initializing),
        .acq_rel,
        .acquire,
    );

    const initFn = struct {
        fn f() u32 {
            return 42;
        }
    }.f;

    // Create future - first poll should return pending
    var future = cell.getOrInitFuture(initFn);
    defer future.deinit();

    var ctx = Context{ .waker = &waker };
    const result1 = future.poll(&ctx);

    try std.testing.expect(result1.isPending());
    try std.testing.expect(!waker_called);

    // Complete initialization
    cell.value = 77;
    cell.completeInit();
    try std.testing.expect(waker_called);

    // Second poll should return ready
    const result2 = future.poll(&ctx);
    try std.testing.expect(result2.isReady());
    try std.testing.expectEqual(@as(u32, 77), result2.ready.*);
}

test "GetOrInitFuture - cancel removes from queue" {
    var cell = OnceCell(u32).init();
    defer cell.deinit();

    // Simulate another task initializing
    _ = cell.state.cmpxchgStrong(
        @intFromEnum(State.empty),
        @intFromEnum(State.initializing),
        .acq_rel,
        .acquire,
    );

    const initFn = struct {
        fn f() u32 {
            return 42;
        }
    }.f;

    // Create future and poll to add to queue
    var future = cell.getOrInitFuture(initFn);
    var ctx = Context{ .waker = &future_mod.noop_waker };
    const result = future.poll(&ctx);

    try std.testing.expect(result.isPending());

    // Cancel
    future.cancel();

    // Clean up
    future.deinit();

    // Complete initialization anyway (for cleanup)
    cell.value = 42;
    cell.completeInit();
}

test "GetOrInitFuture - is valid Future type" {
    const initFn = struct {
        fn f() u32 {
            return 42;
        }
    }.f;

    const FutureType = GetOrInitFuture(u32, initFn);
    try std.testing.expect(future_mod.isFuture(FutureType));
    try std.testing.expect(FutureType.Output == *u32);
}

//! # Async Signal Handling
//!
//! Provides async-aware signal handling that integrates with the
//! volt task system.
//!
//! ## Overview
//!
//! | Type | Description |
//! |------|-------------|
//! | `AsyncSignal` | Async signal handler with waiter-based API |
//! | `Signal` | Signal type enum (SIGINT, SIGTERM, etc.) |
//! | `SignalSet` | Set of signals to handle |
//! | `SignalWaiter` | Waiter for signal notification |
//! | `SignalFuture` | Future for use with Select/Join combinators |
//!
//! | Function | Description |
//! |----------|-------------|
//! | `AsyncSignal.ctrlC()` | Handle Ctrl+C (SIGINT) |
//! | `AsyncSignal.terminate()` | Handle SIGTERM |
//! | `AsyncSignal.shutdown()` | Handle SIGINT + SIGTERM |
//! | `waitForCtrlC()` | Blocking wait for Ctrl+C |
//! | `waitForShutdown()` | Blocking wait for shutdown signal |
//!
//! ## Usage
//!
//! ```zig
//! const blitz = @import("volt");
//!
//! // Wait for Ctrl+C
//! var ctrl_c = try blitz.AsyncSignal.ctrlC();
//! defer ctrl_c.deinit();
//!
//! // In async context
//! var waiter = blitz.SignalWaiter.init();
//! if (!ctrl_c.wait(&waiter)) {
//!     // Yield to scheduler, will be woken when signal arrives
//! }
//! // Signal received!
//! ```
//!
//! ## Design
//!
//! - Wraps SignalHandler with async waiter pattern
//! - Permit-based semantics for signal-before-wait race
//! - Multiple waiters can wait on the same signal
//!

const std = @import("std");
const thr = @import("internal/thread.zig");
const builtin = @import("builtin");
const posix = std.posix;

const LinkedList = @import("internal/util/linked_list.zig").LinkedList;
const Pointers = @import("internal/util/linked_list.zig").Pointers;
const WakeList = @import("internal/util/wake_list.zig").WakeList;

// Re-export underlying signal types
pub const signal_handler = @import("internal/util/signal.zig");
pub const Signal = signal_handler.Signal;
pub const SignalSet = signal_handler.SignalSet;
pub const SignalHandler = signal_handler.SignalHandler;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// A waiter for signal notification.
pub const SignalWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    notified: bool = false,
    received_signal: ?Signal = null,
    pointers: Pointers(SignalWaiter) = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Create a waiter with waker already configured.
    /// Reduces the 3-step ceremony (init + setWaker + wait) to 2 steps.
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

    /// Check if the waiter is ready (signal received).
    /// This is the unified completion check method across all sync primitives.
    pub fn isReady(self: *const Self) bool {
        return self.notified;
    }

    /// Check if complete (alias for isReady).
    pub const isComplete = isReady;

    /// Get the signal that was received (if any).
    pub fn signal(self: *const Self) ?Signal {
        return self.received_signal;
    }

    pub fn reset(self: *Self) void {
        self.notified = false;
        self.received_signal = null;
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

const SignalWaiterList = LinkedList(SignalWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// AsyncSignal
// ─────────────────────────────────────────────────────────────────────────────

/// Async signal handler with waiter-based API.
///
/// Create with `ctrlC()`, `terminate()`, or `init()` for custom signals.
pub const AsyncSignal = struct {
    handler: SignalHandler,

    /// Pending signals that arrived before anyone waited
    pending: SignalSet,

    /// Mutex protecting state
    mutex: thr.Mutex,

    /// Waiters list
    waiters: SignalWaiterList,

    const Self = @This();

    /// Create an async signal handler for SIGINT (Ctrl+C).
    pub fn ctrlC() !Self {
        var signals = SignalSet.empty();
        signals.add(.SIGINT);
        return init(signals);
    }

    /// Create an async signal handler for SIGTERM (termination request).
    pub fn terminate() !Self {
        var signals = SignalSet.empty();
        signals.add(.SIGTERM);
        return init(signals);
    }

    /// Create an async signal handler for SIGHUP (hangup/reload).
    pub fn hangup() !Self {
        var signals = SignalSet.empty();
        signals.add(.SIGHUP);
        return init(signals);
    }

    /// Create an async signal handler for shutdown signals (SIGINT + SIGTERM).
    pub fn shutdown() !Self {
        var signals = SignalSet.empty();
        signals.add(.SIGINT);
        signals.add(.SIGTERM);
        return init(signals);
    }

    /// Create an async signal handler for the given signals.
    pub fn init(signals: SignalSet) !Self {
        return .{
            .handler = try SignalHandler.init(signals),
            .pending = SignalSet.empty(),
            .mutex = .{},
            .waiters = .{},
        };
    }

    /// Clean up resources.
    pub fn deinit(self: *Self) void {
        self.handler.deinit();
    }

    /// Get the file descriptor (Unix) or event handle (Windows) for integration with event loops.
    /// Register this fd for READABLE events (Unix) or wait on the handle (Windows).
    pub fn getFd(self: Self) if (builtin.os.tag == .windows) std.os.windows.HANDLE else posix.fd_t {
        return self.handler.getFd();
    }

    /// Poll for signals without blocking.
    /// Returns the signals that fired, or an empty set if none.
    /// Call this when the fd becomes readable.
    pub fn poll(self: *Self) !SignalSet {
        return self.handler.read();
    }

    /// Try to receive a signal without waiting.
    /// Returns the signal if one is pending, null otherwise.
    pub fn tryRecv(self: *Self) !?Signal {
        self.mutex.lock();

        // Check pending signals first
        if (self.pending.mask != 0) {
            const sig = self.popPendingSignal();
            self.mutex.unlock();
            return sig;
        }

        self.mutex.unlock();

        // Try to read from handler
        const fired = try self.handler.read();
        if (fired.mask == 0) {
            return null;
        }

        // Return one signal, queue the rest
        self.mutex.lock();
        defer self.mutex.unlock();

        self.pending.mask |= fired.mask;
        return self.popPendingSignal();
    }

    /// Wait for a signal.
    /// Returns true if a signal was received immediately.
    /// Returns false if the waiter was added to the queue (task should yield).
    pub fn wait(self: *Self, waiter: *SignalWaiter) !bool {
        self.mutex.lock();

        // Check pending signals first
        if (self.pending.mask != 0) {
            const sig = self.popPendingSignal();
            self.mutex.unlock();
            waiter.notified = true;
            waiter.received_signal = sig;
            return true;
        }

        // Try to read from handler
        const fired = self.handler.read() catch |err| {
            self.mutex.unlock();
            return err;
        };

        if (fired.mask != 0) {
            self.pending.mask |= fired.mask;
            const sig = self.popPendingSignal();
            self.mutex.unlock();
            waiter.notified = true;
            waiter.received_signal = sig;
            return true;
        }

        // No signal pending - add waiter to queue
        waiter.notified = false;
        waiter.received_signal = null;
        self.waiters.pushBack(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Cancel a pending wait.
    pub fn cancelWait(self: *Self, waiter: *SignalWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();

        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        if (SignalWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        self.mutex.unlock();
    }

    /// Called when the signal fd becomes readable.
    /// Reads pending signals and wakes waiters.
    pub fn handleReadable(self: *Self) !void {
        const fired = try self.handler.read();
        if (fired.mask == 0) return;

        var wake_list: WakeList(16) = .{};

        self.mutex.lock();

        // Add to pending
        self.pending.mask |= fired.mask;

        // Wake waiters, one per pending signal
        while (self.waiters.popFront()) |waiter| {
            if (self.pending.mask == 0) {
                // No more signals, put waiter back
                self.waiters.pushFront(waiter);
                break;
            }

            const sig = self.popPendingSignal();
            waiter.notified = true;
            waiter.received_signal = sig;
            if (waiter.waker) |wf| {
                if (waiter.waker_ctx) |ctx| {
                    wake_list.push(.{ .context = ctx, .wake_fn = wf });
                }
            }
        }

        self.mutex.unlock();

        wake_list.wakeAll();
    }

    /// Pop one signal from pending set.
    /// Caller must hold mutex and ensure pending is non-empty.
    fn popPendingSignal(self: *Self) ?Signal {
        if (self.pending.mask == 0) return null;

        // Find lowest set bit (first signal)
        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if (self.pending.contains(sig)) {
                self.pending.remove(sig);
                return sig;
            }
        }
        return null;
    }

    /// Get the number of waiters (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// SignalFuture - For use with Select/Join combinators
// ─────────────────────────────────────────────────────────────────────────────

/// Poll result type for futures.
pub fn PollResult(comptime T: type) type {
    return union(enum) {
        ready: T,
        pending: void,
        err: anyerror,
    };
}

/// A future that completes when a signal is received.
///
/// Use this with Select for graceful shutdown patterns:
///
/// ```zig
/// const async_ops = @import("volt").async_ops;
///
/// // Create signal handler and future
/// var shutdown = try AsyncSignal.shutdown();
/// defer shutdown.deinit();
///
/// // Race accept against shutdown
/// var accept_future = listener.accept();
/// var signal_future = shutdown.future();
///
/// var select = async_ops.Select2(@TypeOf(accept_future), SignalFuture).init(
///     accept_future,
///     signal_future,
/// );
///
/// switch (select.poll(waker)) {
///     .first => |conn| handleConnection(conn),
///     .second => |sig| {
///         log.info("Received {}", .{sig});
///         break; // Exit accept loop
///     },
///     .pending => {},
/// }
/// ```
pub const SignalFuture = struct {
    handler: *AsyncSignal,
    waiter: SignalWaiter,
    started: bool,

    const Self = @This();

    /// Create a future from an AsyncSignal.
    pub fn init(handler: *AsyncSignal) Self {
        return .{
            .handler = handler,
            .waiter = SignalWaiter.init(),
            .started = false,
        };
    }

    /// Poll for a signal.
    pub fn poll(self: *Self, waker: Waker) PollResult(Signal) {
        // Check if already complete
        if (self.waiter.isComplete()) {
            if (self.waiter.received_signal) |sig| {
                return .{ .ready = sig };
            }
            // Shouldn't happen, but handle gracefully
            return .pending;
        }

        // First poll - try immediate receive or register waiter
        if (!self.started) {
            self.started = true;

            // Set up waker
            if (waker.wake_fn) |wf| {
                if (waker.ctx) |ctx| {
                    self.waiter.setWaker(ctx, wf);
                }
            }

            // Try to receive
            const immediate = self.handler.wait(&self.waiter) catch |err| {
                return .{ .err = err };
            };

            if (immediate) {
                if (self.waiter.received_signal) |sig| {
                    return .{ .ready = sig };
                }
            }

            return .pending;
        }

        // Subsequent polls - check completion
        if (self.waiter.isComplete()) {
            if (self.waiter.received_signal) |sig| {
                return .{ .ready = sig };
            }
        }

        // Update waker if changed
        if (waker.wake_fn) |wf| {
            if (waker.ctx) |ctx| {
                self.waiter.setWaker(ctx, wf);
            }
        }

        return .pending;
    }

    /// Cancel the wait.
    pub fn cancel(self: *Self) void {
        self.handler.cancelWait(&self.waiter);
    }

    /// Reset for reuse.
    pub fn reset(self: *Self) void {
        self.handler.cancelWait(&self.waiter);
        self.waiter.reset();
        self.started = false;
    }
};

/// Waker type for SignalFuture.
pub const Waker = struct {
    ctx: ?*anyopaque = null,
    wake_fn: ?WakerFn = null,

    pub fn wake(self: Waker) void {
        if (self.wake_fn) |wf| {
            if (self.ctx) |ctx| {
                wf(ctx);
            }
        }
    }
};

// Add future() method to AsyncSignal
// This is done via a wrapper since we can't modify the struct directly in this context

/// Create a SignalFuture for waiting on signals.
///
/// ## Example
///
/// ```zig
/// var shutdown = try AsyncSignal.shutdown();
/// var future = signalFuture(&shutdown);
///
/// // Use with Select
/// var select = Select2(AcceptFuture, SignalFuture).init(
///     listener.accept(),
///     future,
/// );
/// ```
pub fn signalFuture(handler: *AsyncSignal) SignalFuture {
    return SignalFuture.init(handler);
}

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Wait for a Ctrl+C signal (blocking).
/// Convenience function for simple use cases.
pub fn waitForCtrlC() !void {
    var handler = try AsyncSignal.ctrlC();
    defer handler.deinit();

    while (true) {
        if (try handler.tryRecv()) |_| {
            return;
        }

        if (builtin.os.tag == .windows) {
            // Wait on the Windows event handle
            _ = std.os.windows.kernel32.WaitForSingleObject(handler.getFd(), std.os.windows.INFINITE);
        } else {
            // Use a simple polling loop (Unix)
            var pfd = [_]posix.pollfd{.{
                .fd = handler.getFd(),
                .events = posix.POLL.IN,
                .revents = 0,
            }};
            _ = posix.poll(&pfd, -1) catch continue;
        }
    }
}

/// Wait for shutdown signal (SIGTERM or SIGINT, blocking).
pub fn waitForShutdown() !Signal {
    var handler = try AsyncSignal.shutdown();
    defer handler.deinit();

    while (true) {
        if (try handler.tryRecv()) |sig| {
            return sig;
        }

        if (builtin.os.tag == .windows) {
            // Wait on the Windows event handle
            _ = std.os.windows.kernel32.WaitForSingleObject(handler.getFd(), std.os.windows.INFINITE);
        } else {
            // Use a simple polling loop (Unix)
            var pfd = [_]posix.pollfd{.{
                .fd = handler.getFd(),
                .events = posix.POLL.IN,
                .revents = 0,
            }};
            _ = posix.poll(&pfd, -1) catch continue;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "AsyncSignal - init and deinit" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var handler = AsyncSignal.ctrlC() catch |err| {
        // May fail in restricted environments
        if (err == error.SignalfdFailed or err == error.KqueueFailed) return;
        return err;
    };
    defer handler.deinit();

    try std.testing.expect(handler.getFd() >= 0);
}

test "AsyncSignal - shutdown combines signals" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var handler = AsyncSignal.shutdown() catch |err| {
        if (err == error.SignalfdFailed or err == error.KqueueFailed) return;
        return err;
    };
    defer handler.deinit();

    // Verify it's watching both signals
    try std.testing.expect(handler.handler.signals.contains(.SIGINT));
    try std.testing.expect(handler.handler.signals.contains(.SIGTERM));
}

test "AsyncSignal - tryRecv returns null when empty" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var handler = AsyncSignal.ctrlC() catch |err| {
        if (err == error.SignalfdFailed or err == error.KqueueFailed) return;
        return err;
    };
    defer handler.deinit();

    const result = try handler.tryRecv();
    try std.testing.expect(result == null);
}

test "AsyncSignal - wait adds waiter when no signal" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var handler = AsyncSignal.ctrlC() catch |err| {
        if (err == error.SignalfdFailed or err == error.KqueueFailed) return;
        return err;
    };
    defer handler.deinit();

    var waiter = SignalWaiter.init();
    const immediate = try handler.wait(&waiter);

    // Should return false (no signal pending)
    try std.testing.expect(!immediate);
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expectEqual(@as(usize, 1), handler.waiterCount());

    // Cancel the wait
    handler.cancelWait(&waiter);
    try std.testing.expectEqual(@as(usize, 0), handler.waiterCount());
}

test "SignalWaiter - basic operations" {
    var waiter = SignalWaiter.init();

    try std.testing.expect(!waiter.isComplete());
    try std.testing.expect(waiter.signal() == null);

    waiter.notified = true;
    waiter.received_signal = .SIGINT;

    try std.testing.expect(waiter.isComplete());
    try std.testing.expectEqual(Signal.SIGINT, waiter.signal().?);

    waiter.reset();
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expect(waiter.signal() == null);
}

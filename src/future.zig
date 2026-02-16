//! Future Module - Stackless Async Primitives
//!
//! This module provides the core types for stackless async programming:
//!
//! | Type | Description |
//! |------|-------------|
//! | `Poll` | Result of polling a future (.pending or .ready) |
//! | `PollResult(T)` | Poll with a value |
//! | `Waker` | Mechanism to wake a suspended task |
//! | `Context` | Polling context containing the waker |
//! | `Future` | Trait for async operations |
//!
//! ## Stackless vs Stackful
//!
//! Stackful (old):
//! - Each task has a dedicated stack (16-64KB)
//! - Suspend/resume via assembly context switch
//! - Simple to write but memory-heavy
//!
//! Stackless (this module):
//! - Each task is a state machine (~256-512 bytes)
//! - Suspend returns `.pending`, resume calls `poll()` again
//! - Memory-efficient, cache-friendly
//!
//! ## Example
//!
//! ```zig
//! const volt = @import("volt");
//!
//! // Define a future
//! const MyFuture = struct {
//!     pub const Output = i32;
//!     state: enum { init, done } = .init,
//!
//!     pub fn poll(self: *@This(), ctx: *volt.future.Context) volt.future.PollResult(i32) {
//!         switch (self.state) {
//!             .init => {
//!                 // Register waker, return pending
//!                 _ = ctx.getWaker();
//!                 self.state = .done;
//!                 return .pending;
//!             },
//!             .done => return .{ .ready = 42 },
//!         }
//!     }
//! };
//! ```

// ═══════════════════════════════════════════════════════════════════════════════
// Core Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const poll = @import("future/Poll.zig");
pub const Poll = poll.Poll;
pub const PollResult = poll.PollResult;
pub const PollError = poll.PollError;

pub const waker = @import("future/Waker.zig");
pub const Waker = waker.Waker;
pub const RawWaker = waker.RawWaker;
pub const Context = waker.Context;
pub const noop_waker = waker.noop_waker;

pub const future = @import("future/Future.zig");
pub const isFuture = future.isFuture;
pub const FutureOutput = future.FutureOutput;
pub const DynFuture = future.DynFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// Built-in Futures
// ═══════════════════════════════════════════════════════════════════════════════

/// A future that is immediately ready with a value
pub const Ready = future.Ready;
pub const ready = future.ready;

/// A future that never completes
pub const Pending = future.Pending;
pub const pending = future.pending;

/// A future that computes lazily on first poll
pub const Lazy = future.Lazy;
pub const lazy = future.lazy;

// ═══════════════════════════════════════════════════════════════════════════════
// Function Wrapper
// ═══════════════════════════════════════════════════════════════════════════════

const fn_future = @import("future/FnFuture.zig");

/// Wrap a comptime function as a single-poll Future.
pub const FnFuture = fn_future.FnFuture;

/// Extract return type from a function type.
pub const FnReturnType = fn_future.FnReturnType;

/// Extract payload type (strips error union) from a function type.
pub const FnPayload = fn_future.FnPayload;

// ═══════════════════════════════════════════════════════════════════════════════
// Composable Combinators
// ═══════════════════════════════════════════════════════════════════════════════

const compose_mod = @import("future/Compose.zig");

/// Wrap a Future with fluent combinator methods (.map, .andThen, .withTimeout).
pub const Compose = compose_mod.Compose;

/// Create a Compose wrapper from any Future value — entry point for fluent API.
pub const compose = compose_mod.compose;

/// Transform a Future's output with a comptime function.
pub const MapFuture = @import("future/MapFuture.zig").MapFuture;

/// Chain two Futures sequentially (monadic bind).
pub const AndThenFuture = @import("future/AndThenFuture.zig").AndThenFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    _ = @import("future/Poll.zig");
    _ = @import("future/Waker.zig");
    _ = @import("future/Future.zig");
    _ = @import("future/FnFuture.zig");
    _ = @import("future/MapFuture.zig");
    _ = @import("future/AndThenFuture.zig");
    _ = @import("future/Compose.zig");
}

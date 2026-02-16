//! # Channel Module — Message Passing Primitives
//!
//! Async-aware channels for communicating between tasks.
//!
//! ## Available Channels
//!
//! | Channel | Description |
//! |---------|-------------|
//! | `Channel` | Bounded MPSC (multi-producer, single-consumer) |
//! | `Oneshot` | Single-value delivery (one send, one recv) |
//! | `BroadcastChannel` | Multi-consumer pub/sub (all receivers get all messages) |
//! | `Watch` | Single value with change notification |
//!
//! ## API Patterns
//!
//! | Pattern | Behavior |
//! |---------|----------|
//! | `trySend(v)` / `tryRecv()` | Non-blocking, returns immediately |
//! | `send(v)` / `recv()` → Future | Returns a Future for the scheduler |
//!
//! ## Usage
//!
//! ```zig
//! // Bounded MPSC
//! var ch = try io.channel.bounded(Task, allocator, 100);
//! defer ch.deinit();
//!
//! switch (ch.trySend(task)) {
//!     .ok => {},
//!     .full => {}, // Handle backpressure
//!     .closed => {}, // Channel was closed
//! }
//!
//! // Oneshot
//! var os = io.channel.oneshot(Result);
//! os.sender.send(computeResult());
//! if (os.receiver.tryRecv()) |result| { ... }
//!
//! // Watch
//! var config = io.channel.watch(Config, default_config);
//! config.send(new_config);
//!
//! // Broadcast
//! var bc = try io.channel.broadcast(Event, allocator, 16);
//! defer bc.deinit();
//! _ = bc.send(event);
//! ```
//!
//! ## Resource Management
//!
//! | Type | Allocator | deinit() Required |
//! |------|-----------|-------------------|
//! | **Channel(T)** | **Yes** | **Yes** |
//! | **BroadcastChannel(T)** | **Yes** | **Yes** |
//! | Oneshot(T) | No | No |
//! | Watch(T) | No | No |
//!
//! ## Choosing the Right Channel
//!
//! | Need | Use |
//! |------|-----|
//! | Work queue with backpressure | `Channel` (bounded MPSC) |
//! | Return a single result | `Oneshot` |
//! | Config that changes at runtime | `Watch` |
//! | Event fan-out to multiple consumers | `BroadcastChannel` |

const std = @import("std");
const Allocator = std.mem.Allocator;

// ═══════════════════════════════════════════════════════════════════════════════
// Primary API
// ═══════════════════════════════════════════════════════════════════════════════

/// Bounded MPSC (multi-producer, single-consumer) channel.
pub const Channel = channel_mod.Channel;

/// Single-value channel. Send exactly one value from producer to consumer.
pub const Oneshot = oneshot_mod.Oneshot;

/// Broadcast channel. All receivers get every message (fan-out pattern).
pub const BroadcastChannel = broadcast_mod.BroadcastChannel;

/// Watch channel. Holds a single value; receivers wait for changes.
pub const Watch = watch_mod.Watch;

/// Selector for waiting on multiple channels simultaneously.
pub const Selector = select_mod.Selector;

/// Result of a select operation.
pub const SelectResult = select_mod.SelectResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Factory Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Create a bounded channel with the given capacity.
pub fn bounded(comptime T: type, allocator: Allocator, capacity: usize) !Channel(T) {
    return Channel(T).init(allocator, capacity);
}

/// Create a oneshot channel pair.
pub fn oneshot(comptime T: type) Oneshot(T) {
    return Oneshot(T).init();
}

/// Create a broadcast channel with the given capacity.
pub fn broadcast(comptime T: type, allocator: Allocator, capacity: usize) !BroadcastChannel(T) {
    return BroadcastChannel(T).init(allocator, capacity);
}

/// Create a watch channel with an initial value.
pub fn watch(comptime T: type, initial: T) Watch(T) {
    return Watch(T).init(initial);
}

/// Create a selector with default max branches.
pub const selector = select_mod.selector;

// ═══════════════════════════════════════════════════════════════════════════════
// Sub-modules — advanced/internal types (Waiter, Future, State, etc.)
// ═══════════════════════════════════════════════════════════════════════════════

pub const channel_mod = @import("channel/Channel.zig");
pub const oneshot_mod = @import("channel/Oneshot.zig");
pub const broadcast_mod = @import("channel/Broadcast.zig");
pub const watch_mod = @import("channel/Watch.zig");
pub const select_mod = @import("channel/select.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "channel module imports" {
    // Just verify the module can be imported
    _ = Channel;
    _ = Oneshot;
    _ = BroadcastChannel;
    _ = Watch;
}

test {
    // Run sub-module tests
    _ = @import("channel/Channel.zig");
    _ = @import("channel/Oneshot.zig");
    _ = @import("channel/Broadcast.zig");
    _ = @import("channel/Watch.zig");
    _ = @import("channel/select.zig");
}

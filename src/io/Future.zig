//! # Future(T) — Simplified Task Result Handle
//!
//! A `Future(T)` is a handle to a spawned async task's result. Obtain one
//! via `io.@"async"()`, then retrieve the result with `.@"await"(io)`.
//!
//! This is the user-facing API. The internal Future/Poll/Waker state machine
//! still drives the scheduler — it's just not exposed.
//!
//! ## Usage
//!
//! ```zig
//! var f = try io.@"async"(compute, .{42});
//! const result = f.@"await"(io);
//! ```
//!
//! ## Idempotent
//!
//! Calling `.@"await"(io)` a second time returns the cached result.
//!
//! ## Cancel
//!
//! ```zig
//! var f = try io.@"async"(longRunning, .{});
//! const result = f.cancel(io); // request cancellation, then wait
//! ```

const std = @import("std");
const Io = @import("../Io.zig");

// Internal scheduler types — NOT exposed to users
const sched_runtime = @import("../internal/scheduler/Runtime.zig");

/// Handle to an async task's result. Obtain via `io.@"async"()`.
///
/// Two methods:
/// - `.@"await"(io)` — block until the task completes, return result
/// - `.cancel(io)` — request cancellation, then wait for completion
pub fn Future(comptime T: type) type {
    const JH = sched_runtime.JoinHandle(T);

    return struct {
        const Self = @This();

        /// The result type. For `void` tasks this is `void`.
        /// For value tasks this includes `error{TaskCancelled}`.
        pub const Result = JH.Output;

        _handle: JH,
        _awaited: bool = false,
        _cached: CachedResult = .pending,

        const CachedResult = union(enum) {
            pending,
            done: if (Result == void) void else Result,
        };

        /// Block until the task completes and return its result.
        /// Idempotent — second call returns cached result.
        pub fn await(self: *Self, _: Io) Result {
            switch (self._cached) {
                .done => |v| return if (Result == void) {} else v,
                .pending => {},
            }
            const result = self._handle.join();
            self._cached = .{ .done = if (Result == void) {} else result };
            self._awaited = true;
            return result;
        }

        /// Request cancellation, then wait for completion.
        /// The task may still complete normally if cancellation arrives too late.
        pub fn cancel(self: *Self, io: Io) Result {
            self._handle.cancel();
            return self.await(io);
        }

        /// Check if the task has completed without blocking.
        pub fn isDone(self: *const Self) bool {
            return self._awaited or self._handle.isDone();
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Future - type instantiation for various T" {
    comptime {
        _ = Future(u32);
        _ = Future(void);
        _ = Future([]const u8);
        _ = Future(bool);
        _ = Future(i64);
    }
}

test "Future - Result type alias" {
    comptime {
        // For value types, Result should be JoinHandle's output type
        _ = Future(u32).Result;
        _ = Future(void).Result;
    }
}

test "Future - CachedResult union" {
    comptime {
        // CachedResult should have pending and done variants
        const F = Future(u32);
        _ = F.CachedResult;
        const cr: F.CachedResult = .pending;
        _ = cr;
    }
}

test "Future - struct fields exist" {
    comptime {
        const F = Future(u32);
        // Verify field existence
        _ = @offsetOf(F, "_awaited");
        _ = @offsetOf(F, "_cached");
    }
}

test "Future - method signatures" {
    comptime {
        _ = Future(u32).await;
        _ = Future(u32).cancel;
        _ = Future(u32).isDone;
    }
}

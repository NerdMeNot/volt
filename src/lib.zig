//! # Volt — A stackful coroutine runtime for Zig
//!
//! Status: rebuilding. The stackless implementation has been stripped; the
//! stackful rewrite is in progress. Public API is intentionally tiny right
//! now — it grows as the new core comes online.
//!
//! Spike validation (Darwin-ARM64, ReleaseFast):
//!   - 10ns/switch one-way   (12-15× faster than Go)
//!   - 622ns/spawn (gpa)     (~5× faster than Go)
//!
//! See `docs/design/api-design.md` for the API roadmap (forthcoming).

const std = @import("std");

// ─────────────────────────────────────────────────────────────────────────────
// Time primitives — kept from the prior tree (model-agnostic types)
// ─────────────────────────────────────────────────────────────────────────────

pub const time = @import("time.zig");
pub const Duration = time.Duration;
pub const Instant = time.Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Internal — kept platform infrastructure
//
// These layers are execution-model agnostic. They survived the stackful
// pivot because the rebuilt scheduler / I/O integration / sync primitives
// will sit on top of them.
// ─────────────────────────────────────────────────────────────────────────────

pub const internal = struct {
    /// Raw thread-level sync primitives: Mutex, Condition, Futex, sleep.
    /// 0.16 moved std.Thread.{Mutex,Condition,Futex,sleep} into std.Io;
    /// these are Volt's own raw versions for use inside the runtime itself.
    pub const thread = @import("internal/thread.zig");

    /// Raw syscall wrappers (socket, pipe, fcntl, kqueue, etc.) — replaces
    /// the medium-level `std.posix.*` functions removed in 0.16.
    pub const syscall = @import("internal/syscall.zig");

    /// Utility data structures kept because they're useful for any runtime:
    /// intrusive linked list, slab allocator, object pool, stack guard,
    /// cacheline alignment, bit manipulation.
    pub const util = struct {
        pub const linked_list = @import("internal/util/linked_list.zig");
        pub const slab = @import("internal/util/slab.zig");
        pub const pool = @import("internal/util/pool.zig");
        pub const stack_guard = @import("internal/util/stack_guard.zig");
        pub const cacheline = @import("internal/util/cacheline.zig");
        pub const bit = @import("internal/util/bit.zig");
        pub const invocation_id = @import("internal/util/invocation_id.zig");
        pub const signal = @import("internal/util/signal.zig");
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Version
// ─────────────────────────────────────────────────────────────────────────────

pub const version = struct {
    pub const major = 0;
    pub const minor = 1;
    pub const patch = 0;
    pub const string = "0.1.0-zig0.16.0";
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pulls in inline tests from kept files only.
// ─────────────────────────────────────────────────────────────────────────────

test {
    _ = time;
    _ = internal.thread;
    _ = internal.syscall;
    _ = internal.util.linked_list;
    _ = internal.util.slab;
    _ = internal.util.pool;
    _ = internal.util.stack_guard;
    _ = internal.util.cacheline;
    _ = internal.util.bit;
    _ = internal.util.invocation_id;
    _ = internal.util.signal;
}

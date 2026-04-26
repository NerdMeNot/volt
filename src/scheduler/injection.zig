//! Global injection queue — fallback path for cross-thread scheduling.
//!
//! Used when:
//!   - The scheduler is invoked from a non-worker thread (rare in v0.3+)
//!   - A wake from a sibling worker has nowhere local to go
//!
//! Bounded mutex-protected queue. Workers check this after their local
//! deque and before stealing. The bound exists so a runaway producer can't
//! OOM the runtime; once full, `push` returns `error.QueueFull` and the
//! caller decides what to do (currently: fall back to local push or wake
//! the home worker).

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Mutex = @import("../internal/thread.zig").Mutex;

/// Default cap. Picked to be large enough for practical workloads
/// (thousands of pending cross-thread spawns) without being so large that
/// runaway producers can OOM the process. Override via `init_with_cap`.
pub const default_capacity: usize = 16 * 1024;

pub const PushError = error{ QueueFull, OutOfMemory };

pub const Injection = struct {
    mutex: Mutex = .{},
    items: std.array_list.Managed(*Coroutine),
    capacity: usize,

    pub fn init(allocator: std.mem.Allocator) Injection {
        return initWithCapacity(allocator, default_capacity);
    }

    pub fn initWithCapacity(allocator: std.mem.Allocator, capacity: usize) Injection {
        std.debug.assert(capacity > 0);
        return .{
            .items = std.array_list.Managed(*Coroutine).init(allocator),
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *Injection) void {
        self.items.deinit();
    }

    /// Push a coroutine. Returns `error.QueueFull` if the queue is at
    /// capacity (caller should fall back) or `error.OutOfMemory` if the
    /// underlying allocation fails.
    pub fn push(self: *Injection, coro: *Coroutine) PushError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.items.items.len >= self.capacity) return error.QueueFull;
        self.items.append(coro) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
        };
    }

    pub fn tryPop(self: *Injection) ?*Coroutine {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    pub fn isEmpty(self: *Injection) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.items.items.len == 0;
    }

    /// Approximate length. Reads the underlying length WITHOUT taking the
    /// mutex — the value may be slightly stale wrt to a concurrent push/pop
    /// from another worker. Used by `notifyOneWorker` and similar hot paths.
    pub fn lenApprox(self: *const Injection) usize {
        return self.items.items.len;
    }
};

test "injection: push/pop FIFO" {
    var inj = Injection.init(std.testing.allocator);
    defer inj.deinit();

    var c1: Coroutine = undefined;
    var c2: Coroutine = undefined;
    try inj.push(&c1);
    try inj.push(&c2);
    try std.testing.expectEqual(@as(?*Coroutine, &c1), inj.tryPop());
    try std.testing.expectEqual(@as(?*Coroutine, &c2), inj.tryPop());
    try std.testing.expectEqual(@as(?*Coroutine, null), inj.tryPop());
}

test "injection: hits QueueFull at the cap" {
    var inj = Injection.initWithCapacity(std.testing.allocator, 2);
    defer inj.deinit();

    var coros: [3]Coroutine = undefined;
    try inj.push(&coros[0]);
    try inj.push(&coros[1]);
    try std.testing.expectError(error.QueueFull, inj.push(&coros[2]));
    // Pop one to make room.
    _ = inj.tryPop();
    try inj.push(&coros[2]);
}

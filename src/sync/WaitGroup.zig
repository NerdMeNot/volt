//! WaitGroup — wait for N concurrent operations to complete.
//!
//! Direct analog of Go's `sync.WaitGroup`. Use when you want to spawn
//! N coroutines and wait for ALL of them with a single block, instead
//! of `for (jobs) |j| j.join()` which incurs N park/unpark cycles.
//!
//! Pattern:
//! ```zig
//! var wg = WaitGroup{};
//! wg.add(8);
//! for (0..8) |_| {
//!     _ = try volt.launch(struct {
//!         fn body(g: *WaitGroup) void {
//!             defer g.done();
//!             // ... work ...
//!         }
//!     }.body, .{&wg});
//! }
//! try wg.wait();   // single park; single wake when counter hits 0
//! ```
//!
//! ## Design
//!
//! - `counter` is a u32 atomic — concurrent `add`/`done` race-safe.
//! - `waiter_park` is a single Park slot — the runtime guarantees one
//!   waiter at a time. `done` only unparks if the counter transitions
//!   1→0; other `done()` calls are pure atomic decrements with no
//!   wake overhead.
//! - `wait` loops on the counter with `parkCurrent` between checks.
//!   The Park's NOTIFIED state handles the wait-vs-done race: a `done`
//!   that fires before `wait` parks stores NOTIFIED, which the next
//!   `parkCurrent` consumes inline.
//!
//! ## Comparison to per-coro `.join()`
//!
//! `for (jobs) |j| j.join()` does N park/unpark cycles — for batches
//! of 10k+ coroutines this is the bottleneck on spawn+join workloads.
//! WaitGroup does at most ONE park-unpark cycle regardless of N, at
//! the cost of an extra atomic op per coro completion. Net win above
//! ~3-4 children.

const std = @import("std");
const Park = @import("../scheduler/park.zig").Park;

pub const WaitGroup = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    waiter_park: Park = .{},

    /// Add `n` to the counter. Typically called BEFORE spawning the
    /// children (so the waiter doesn't accidentally observe counter=0
    /// in between spawns).
    pub fn add(self: *WaitGroup, n: u32) void {
        _ = self.counter.fetchAdd(n, .acq_rel);
    }

    /// Decrement the counter. If this transition reaches zero, wake
    /// the (single) waiter. Safe to call from any coroutine / thread.
    ///
    /// The unpark fires only on the 1→0 transition, so the typical
    /// case (counter > 1 after decrement) is a single atomic op with
    /// no scheduler interaction.
    pub fn done(self: *WaitGroup) void {
        const prev = self.counter.fetchSub(1, .acq_rel);
        if (prev == 1) {
            // We took the counter from 1 to 0 — wake the waiter.
            // unparkLocal: the waiter resumes on the calling worker
            // (typical: same worker that just finished the last child).
            self.waiter_park.unparkLocal();
        }
    }

    /// Block until the counter reaches zero. Returns `error.Cancelled`
    /// if the calling coroutine is cancelled during the wait.
    ///
    /// Only one coroutine should call `wait` per WaitGroup — the
    /// Park primitive holds a single waiter slot. (Multiple waiters
    /// would race on the slot.)
    pub fn wait(self: *WaitGroup) error{Cancelled}!void {
        while (self.counter.load(.acquire) != 0) {
            try self.waiter_park.parkCurrent();
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const SimpleWgCtx = struct {
    wg: *WaitGroup,
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn simpleWgChild(ctx: *SimpleWgCtx) void {
    defer ctx.wg.done();
    _ = ctx.counter.fetchAdd(1, .release);
}

fn simpleWgRoot() !u32 {
    const N: u32 = 32;
    var wg = WaitGroup{};
    var ctx = SimpleWgCtx{ .wg = &wg };

    wg.add(N);
    var i: u32 = 0;
    while (i < N) : (i += 1) {
        const j = try volt.launch(simpleWgChild, .{&ctx});
        volt.destroyJob(j);
    }
    try wg.wait();
    return ctx.counter.load(.acquire);
}

test "WaitGroup: 32 children all complete before wait returns" {
    const counter = try volt.run(.{ .allocator = std.testing.allocator }, simpleWgRoot, .{});
    try std.testing.expectEqual(@as(u32, 32), counter);
}

fn doneBeforeWaitRoot() !void {
    // If counter hits 0 BEFORE wait() is entered, wait must still
    // return immediately (counter check first).
    var wg = WaitGroup{};
    wg.add(1);
    wg.done(); // counter now 0
    try wg.wait(); // should return immediately
}

test "WaitGroup: wait returns immediately when counter already zero" {
    try volt.run(.{ .allocator = std.testing.allocator }, doneBeforeWaitRoot, .{});
}

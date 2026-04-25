//! Baseline: a hand-written Future for a 2-suspension async function.
//!
//! The "user wanted to write" code (in pseudo-async syntax):
//!
//!   async fn doubleAndAdd(a: i32, b: i32) i32 {
//!       const doubled = await delayedDouble(a);   // suspension 1
//!       const sum = doubled + b;                  // captured `b` used here
//!       const final = await delayedDouble(sum);   // suspension 2
//!       return final;
//!   }
//!
//! What they actually have to write today is below. Count the boilerplate.

const std = @import("std");
const proto = @import("proto.zig");
const Delayed = @import("delayed.zig").Delayed;

const PollResult = proto.PollResult;
const Context = proto.Context;

/// `async fn doubleAndAdd(a, b) -> i32`, expressed as a Future state machine.
pub const DoubleAndAddFuture = struct {
    const Self = @This();
    pub const Output = i32;

    // Captured "args" — read-only after construction
    a: i32,
    b: i32,

    // State machine
    state: State,

    const State = union(enum) {
        // Phase 1: about to start the first delayed double
        start: void,
        // Phase 2: polling the first delayed double
        awaiting_first: Delayed(i32),
        // Phase 3: polling the second delayed double, with `sum` saved
        awaiting_second: Delayed(i32),
        // Terminal — should never poll again
        done: void,
    };

    pub fn init(a: i32, b: i32) Self {
        return .{ .a = a, .b = b, .state = .{ .start = {} } };
    }

    pub fn poll(self: *Self, ctx: *Context) PollResult(i32) {
        // The state machine is a switch driven by self.state.
        // Each transition: poll the current inner future; if pending, return pending;
        // if ready, advance state with the captured locals.
        while (true) {
            switch (self.state) {
                .start => {
                    self.state = .{ .awaiting_first = Delayed(i32).init(self.a * 2) };
                },
                .awaiting_first => |*inner| {
                    const r = inner.poll(ctx);
                    if (r.isPending()) return .pending;
                    const doubled = r.unwrap();
                    // Use captured `b` from outer scope
                    const sum = doubled + self.b;
                    self.state = .{ .awaiting_second = Delayed(i32).init(sum * 2) };
                },
                .awaiting_second => |*inner| {
                    const r = inner.poll(ctx);
                    if (r.isPending()) return .pending;
                    const final = r.unwrap();
                    self.state = .done;
                    return .{ .ready = final };
                },
                .done => @panic("polled after ready"),
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "DoubleAndAddFuture - basic" {
    // doubleAndAdd(3, 5):
    //   doubled = 3*2 = 6        (Delayed: pends once, then ready)
    //   sum = 6 + 5 = 11
    //   final = 11*2 = 22         (Delayed: pends once, then ready)
    var f = DoubleAndAddFuture.init(3, 5);
    var ctx = Context{ .waker = &proto.noop_waker };

    // First poll: starts, immediately polls inner1, inner1 pends → pending
    const r1 = f.poll(&ctx);
    try std.testing.expect(r1.isPending());

    // Second poll: inner1 ready → 6, transitions to inner2, inner2 pends → pending
    const r2 = f.poll(&ctx);
    try std.testing.expect(r2.isPending());

    // Third poll: inner2 ready → 22, terminal
    const r3 = f.poll(&ctx);
    try std.testing.expect(r3.isReady());
    try std.testing.expectEqual(@as(i32, 22), r3.unwrap());
}

test "DoubleAndAddFuture - shape sanity" {
    try std.testing.expect(proto.isFuture(DoubleAndAddFuture));
    try std.testing.expect(DoubleAndAddFuture.Output == i32);
}

//! Ticker heartbeat — fires every 100ms for 10 ticks.
//!
//! Demonstrates: volt.Ticker.init + .next() in a loop. Uses .delay
//! missed-tick behaviour (default): if a tick is missed (because
//! the work inside the loop took >100ms), the next tick is
//! rescheduled from now — no runaway storm.
//!
//! Run: zig build run-ticker-heartbeat

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(heartbeat, .{}));
}

fn heartbeat() !void {
    var ticker = volt.Ticker.init(volt.Duration.fromMillis(100));
    const start = volt.Instant.now();
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const tick = try ticker.next();
        const elapsed = tick.since(start);
        std.debug.print("tick {d:2}  at +{d:5} ms\n", .{ i + 1, elapsed.ns / std.time.ns_per_ms });
    }
}

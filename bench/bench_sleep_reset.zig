//! Micro-bench: cost of cancel-and-new-sleep, the pattern for any
//! "resettable timeout" use case.
//!
//! Many timer-driven control loops (heartbeat watchdogs, election
//! timeouts, RPC deadlines, retry backoffs) want a sleep that can
//! be cancelled and re-issued on each event. Volt doesn't ship a
//! dedicated `Sleep` handle with `reset(new_duration)` — the
//! pattern is `cancel + new sleep`. This bench measures whether
//! that's fast enough for hot-path use.
//!
//! Methodology: launch a child that sleeps. Cancel and join. Spawn
//! a fresh child sleeping. Repeat. Report ns/iteration.
//!
//! Measured cost on Darwin / M-class arm64 (v1.1 with reactor
//! cleanup landed): ~20 µs/reset. That's two kevent syscalls
//! (unregister + register) plus the spawn-cancel-join orchestration.
//!
//! Acceptability rule of thumb:
//!   - Reset cadence ≥ 1 ms (Raft elections, RPC deadlines, retry
//!     backoffs): cancel-and-new is fine — overhead is ≤ 2%.
//!   - Reset cadence < 100 µs (control-loop tickers): a dedicated
//!     `Sleep` handle with `reset(new_duration)` would shave the
//!     cancel-spawn-join pieces. Tracked for v1.2 if a workload
//!     needs it.

const std = @import("std");
const volt = @import("volt");

const ITERATIONS: u32 = 1000;

fn sleeper() void {
    // Hour-long sleep — never fires; we always cancel.
    _ = volt.sleep(volt.Duration.fromSecs(3600)) catch {};
}

fn benchRoot() !u64 {
    const t0 = volt.time.nanoTimestamp();

    var i: u32 = 0;
    while (i < ITERATIONS) : (i += 1) {
        const job = try volt.launch(sleeper, .{});
        // Yield so the child enters sleep and parks on the timer.
        try volt.yield();
        job.cancel();
        job.join() catch {};
        volt.destroyJob(job);
    }

    const wall: u64 = @intCast(volt.time.nanoTimestamp() - t0);
    return wall;
}

pub fn main() !void {
    const wall = try volt.run(.{ .allocator = std.heap.page_allocator }, benchRoot, .{});
    const ns_per_reset = wall / ITERATIONS;
    std.debug.print(
        \\Sleep reset (cancel + new sleep) micro-bench:
        \\  iterations: {d}
        \\  wall      : {d} ns
        \\  per reset : {d} ns
        \\
        \\  rule of thumb:
        \\    reset cadence ≥ 1 ms → cancel-and-new is fine
        \\    reset cadence < 100 µs → consider a dedicated Sleep
        \\                              handle with reset(new_duration)
        \\
    ,
        .{ ITERATIONS, wall, ns_per_reset },
    );
}

//! Cookbook: race work against a deadline with `volt.withTimeout`,
//! retry on timeout up to N attempts.
//!
//! `volt.withTimeout(duration, fn, args)` runs `fn(args)` as a child
//! task and returns either its value, `error.Timeout`, or whatever
//! `fn` returned. The losing branch is cancelled — and because parks
//! are cancellable from anywhere in v1.0, sleep / I/O / channel waits
//! all surface `error.Cancelled` promptly.

const std = @import("std");
const volt = @import("volt");

const max_attempts = 3;

/// Simulated flaky operation: succeeds with probability ~50%, otherwise
/// sleeps for a long time (longer than our timeout).
fn flakyOp(attempt: u32) !u32 {
    var rng = std.Random.DefaultPrng.init(@as(u64, attempt));
    if (rng.random().boolean()) {
        // Fast path.
        volt.sleep(volt.Duration.fromMillis(20)) catch return error.Cancelled;
        return attempt * 100;
    }
    // Slow path — exceeds timeout, will be cancelled.
    volt.sleep(volt.Duration.fromSecs(60)) catch return error.Cancelled;
    return 0;
}

fn root() !u32 {
    const deadline = volt.Duration.fromMillis(50);
    var attempt: u32 = 1;
    while (attempt <= max_attempts) : (attempt += 1) {
        std.debug.print("attempt {d}: ", .{attempt});
        const result = volt.withTimeout(deadline, flakyOp, .{attempt}) catch |err| switch (err) {
            error.Timeout => {
                std.debug.print("timed out\n", .{});
                continue;
            },
            else => return err,
        };
        std.debug.print("ok → {d}\n", .{result});
        return result;
    }
    return error.GaveUp;
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const v = volt.run(.{ .allocator = gpa.allocator() }, root, .{}) catch |err| {
        std.debug.print("all attempts exhausted: {s}\n", .{@errorName(err)});
        return;
    };
    std.debug.print("final value: {d}\n", .{v});
}

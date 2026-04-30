---
title: Timeout with Retry
description: withTimeout + a retry loop that backs off and surfaces a clean error on exhaustion.
---

```zig
const std = @import("std");
const volt = @import("volt");

const max_attempts = 3;

fn flakyOp(attempt: u32) !u32 {
    var rng = std.Random.DefaultPrng.init(attempt);
    if (rng.random().boolean()) {
        // Fast path
        try volt.sleep(volt.Duration.fromMillis(20));
        return attempt * 100;
    }
    // Slow path — exceeds timeout
    try volt.sleep(volt.Duration.fromSecs(60));
    return 0;
}

fn root() !u32 {
    const deadline = volt.Duration.fromMillis(50);
    var attempt: u32 = 1;
    while (attempt <= max_attempts) : (attempt += 1) {
        std.debug.print("attempt {d}: ", .{attempt});
        const result = volt.withTimeout(deadline, flakyOp, .{attempt});
        const v = result catch |err| switch (err) {
            error.Timeout => {
                std.debug.print("timed out\n", .{});
                continue;
            },
            else => return err,
        };
        std.debug.print("ok → {d}\n", .{v});
        return v;
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
```

## Why this works

`volt.withTimeout(duration, fn, args)`:

1. Spawns `fn(args)` as a child coroutine.
2. Spawns a watcher that sleeps for `duration`.
3. Whoever finishes first wins.
4. Cancels the loser. Cancellation propagates through any nested
   suspension — `flakyOp`'s `volt.sleep(60s)` surfaces
   `error.Cancelled` immediately.

Because the cancel propagates through arbitrary blocking calls, you
don't have to write `flakyOp` to be cancellation-aware. You just
write blocking code; `withTimeout` handles the timer race
transparently.

## With exponential backoff

Add a backoff between attempts:

```zig
fn root() !u32 {
    var attempt: u32 = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        const deadline = volt.Duration.fromMillis(100 << @intCast(attempt));
        const r = volt.withTimeout(deadline, flakyOp, .{attempt});
        const v = r catch |err| switch (err) {
            error.Timeout => {
                // Wait before retrying.
                try volt.sleep(volt.Duration.fromMillis(50 << @intCast(attempt)));
                continue;
            },
            else => return err,
        };
        return v;
    }
    return error.GaveUp;
}
```

Each attempt's deadline doubles (100ms → 200ms → 400ms), and the
gap between attempts also doubles (50ms → 100ms → 200ms).

## With jitter

Constant exponential backoff causes thundering herds when many
clients retry in lockstep. Add jitter:

```zig
fn jitteredBackoff(attempt: u32, prng: *std.Random.DefaultPrng) volt.Duration {
    const base_ms: u64 = @as(u64, 50) << @intCast(attempt);
    const jitter_ms = prng.random().intRangeAtMost(u64, 0, base_ms / 2);
    return volt.Duration.fromMillis(base_ms + jitter_ms);
}
```

## Distinguishing timeout from intrinsic failure

`flakyOp` might fail for reasons other than slowness. Treat them
differently:

```zig
const r = volt.withTimeout(deadline, flakyOp, .{attempt});
const v = r catch |err| switch (err) {
    error.Timeout => continue,           // retry
    error.NoNetwork => continue,         // retry
    error.AuthFailed => return err,       // don't retry; bubble up
    else => return err,
};
```

The function's own error union flows through `withTimeout` — only
`error.Timeout` is added by the wrapper.

## Whole-request deadline

If you want a *total* deadline across all retries, wrap the loop:

```zig
const final = volt.withTimeout(volt.Duration.fromSecs(10), retryLoop, .{});
```

If 10 seconds elapse before retryLoop succeeds, the outer cancel
fires and propagates through whatever `retryLoop` was doing —
typically a `volt.sleep` between attempts or a child `flakyOp`.

## Source

[`examples/timeout_retry.zig`](https://github.com/NerdMeNot/volt/blob/main/examples/timeout_retry.zig) in the repo.

```sh
zig build run-timeout-retry
```

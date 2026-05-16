---
title: Timeout with Retry
description: scope + watchdog as a timeout primitive. Retry loop with exponential backoff and jitter. Cancel-aware blocking ops for clean abandonment.
---

Volt doesn't ship a `withTimeout` helper in core. The pattern is
`volt.scope` + a watchdog coroutine that fires a Cancel when the
deadline elapses. Cancel-aware blocking ops (`recvCancel`,
`lockCancel`, etc.) wake with `error.Cancelled` when the deadline
fires, letting the operation unwind cleanly.

## The pattern

A reusable timeout helper:

```zig
const std = @import("std");
const volt = @import("volt");

/// Run `body(*Cancel)`. If it doesn't return within `ns_deadline`,
/// fire the Cancel and let `body` see `error.Cancelled`. Returns
/// whatever `body` returned (which is `error.Cancelled` on timeout).
fn withTimeout(ns_deadline: u64, comptime body: anytype) anyerror!void {
    try volt.scope(struct {
        fn b(c: *volt.Cancel) anyerror!void {
            // Watchdog: sleep then fire.
            const watchdog = struct {
                fn run(ns: u64, cancel: *volt.Cancel) void {
                    volt.sleep(ns);
                    cancel.fire();
                }
            }.run;
            const wd = try volt.spawn(watchdog, .{ ns_deadline, c });

            // Run user body — propagate its result.
            const result = body(c);

            // Whether body succeeded, errored, or was cancelled,
            // make sure the watchdog stops trying to fire.
            // (Fire is idempotent; safe to do regardless.)
            c.fire();
            wd.join();

            return result;
        }
    }.b);
}
```

## Retry loop with backoff

```zig
const max_attempts: u32 = 3;

fn flakyOp(attempt: u32, c: *volt.Cancel) error{Cancelled, FlakyFailed}!u32 {
    // Simulate work that might be slow:
    var rng = std.Random.DefaultPrng.init(attempt);
    if (rng.random().boolean()) {
        // Fast path
        volt.sleep(20 * std.time.ns_per_ms);
        try c.checkpoint();   // observe cancel if it fired during sleep
        return attempt * 100;
    }
    // Slow path — exceeds typical deadline
    volt.sleep(60 * std.time.ns_per_s);
    try c.checkpoint();
    return error.FlakyFailed;
}

fn root() !u32 {
    var attempt: u32 = 1;
    while (attempt <= max_attempts) : (attempt += 1) {
        const deadline_ms: u64 = 100 << @intCast(attempt - 1);   // 100ms, 200ms, 400ms
        std.debug.print("attempt {d} (deadline {d}ms): ", .{ attempt, deadline_ms });

        var maybe_result: ?u32 = null;

        const captured = struct {
            var result_slot: ?u32 = null;
        };
        captured.result_slot = null;

        const body = struct {
            fn b(c: *volt.Cancel) anyerror!void {
                captured.result_slot = flakyOp(attempt, c) catch |e| switch (e) {
                    error.Cancelled => return e,
                    else => return e,
                };
            }
        }.b;

        withTimeout(deadline_ms * std.time.ns_per_ms, body) catch |e| switch (e) {
            error.Cancelled => {
                std.debug.print("timed out\n", .{});
                // Backoff before retry:
                volt.sleep(50 << @intCast(attempt - 1) * std.time.ns_per_ms);
                continue;
            },
            else => return e,
        };
        maybe_result = captured.result_slot;
        if (maybe_result) |v| {
            std.debug.print("ok -> {d}\n", .{v});
            return v;
        }
    }
    return error.GaveUp;
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    const v = (try rt.run(root, .{})) catch |err| {
        std.debug.print("all attempts exhausted: {s}\n", .{@errorName(err)});
        return;
    };
    std.debug.print("final value: {d}\n", .{v});
}
```

The example uses a `captured.result_slot` static to thread the
result out of the scope body — the body returns `anyerror!void`
since `scope` requires that signature, but we want to keep the
value. Awkward but workable. A first-class `volt.withTimeout(dur,
fn, args) !ReturnType` would clean this up; it's roadmapped.

## Why this works

`withTimeout` runs `body(*Cancel)` inside a `volt.scope`. A
sibling watchdog coroutine sleeps for the deadline and fires the
Cancel.

- If `body` returns first, `withTimeout` returns its result (the
  Cancel is fired afterward but the watchdog has already exited
  or will exit on join).
- If the watchdog fires first, the Cancel goes hot. Any
  cancel-aware blocking op `body` is parked on (via
  `recvCancel`, `lockCancel`, etc.) wakes with `error.Cancelled`.
  `body` unwinds and returns the error; scope joins the
  watchdog; `withTimeout` returns `error.Cancelled`.

The pattern requires that `body`'s blocking ops are cancel-aware.
`volt.sleep` is not cancel-aware today — a sleep mid-body will run
to completion regardless. Use `c.checkpoint()` after sleeps to
observe the Cancel at the next syscall-boundary.

## Exponential backoff with jitter

Constant exponential backoff causes thundering herds when many
clients retry in lockstep. Add jitter:

```zig
fn jitteredBackoff(attempt: u32, rng: *std.Random.DefaultPrng) u64 {
    const base_ns: u64 = (@as(u64, 50) << @intCast(attempt)) * std.time.ns_per_ms;
    const jitter_ns = rng.random().intRangeAtMost(u64, 0, base_ns / 2);
    return base_ns + jitter_ns;
}

// In the retry loop:
const backoff_ns = jitteredBackoff(attempt, &rng);
volt.sleep(backoff_ns);
```

50ms-base doubling per attempt; jitter spread over [0, 50%] of
base.

## Distinguishing timeout from intrinsic failure

`flakyOp` might fail for reasons other than slowness. Treat them
differently:

```zig
const result = body_fn(c) catch |err| switch (err) {
    error.Cancelled => continue,         // retry
    error.NoNetwork => continue,         // retry
    error.AuthFailed => return err,      // don't retry; bubble up
    error.OutOfMemory => return err,
    else => return err,
};
```

`error.Cancelled` is what `withTimeout` injects; intrinsic
failures come back with their original error type.

## Whole-request deadline

If you want a *total* deadline across all retries, wrap the loop:

```zig
try withTimeout(10 * std.time.ns_per_s, struct {
    fn b(c: *volt.Cancel) anyerror!void {
        _ = c;
        _ = try retryLoop();
    }
}.b);
```

If 10 seconds elapse before `retryLoop` succeeds, the outer Cancel
fires; the next cancel-aware blocking op inside surfaces
`error.Cancelled`.

## When the body has nothing cancel-aware

If `body` is pure CPU work, cancel doesn't propagate by itself.
You need to insert `c.checkpoint()` calls:

```zig
fn cpuWork(c: *volt.Cancel) error{Cancelled}!u64 {
    var sum: u64 = 0;
    var i: u64 = 0;
    while (i < 100_000_000) : (i += 1) {
        if (i % 10_000 == 0) try c.checkpoint();
        sum +%= heavy(i);
    }
    return sum;
}
```

`checkpoint` is one atomic load + branch. Hot enough to call every
10k iterations.

## See also

- [Time](/usage/time/) — `sleep` semantics and the watchdog pattern.
- [Structured Concurrency](/usage/structured-concurrency/) — Cancel + scope.
- [Roadmap](/appendix/roadmap/) — a first-class `withTimeout` may land later.

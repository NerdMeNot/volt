---
title: Writing Async Tests
description: Patterns for testing coroutines — keeping tests fast, deterministic, and meaningful.
---

Testing async code in Volt is "test it like you'd test a synchronous
function" — because Volt-shape code looks synchronous. The wrapper
around every test:

```zig
test "my test" {
    const result = try volt.run(.{
        .allocator = std.testing.allocator,
    }, testRoot, .{});
    try std.testing.expectEqual(expected, result);
}

fn testRoot() !ResultType {
    // your async-shaped test code
}
```

That's it. `volt.run` bootstraps the runtime; `testRoot` runs;
`std.testing.allocator` catches leaks.

## Basic patterns

### Test a primitive's happy path

```zig
test "channel: send + recv round-trip" {
    const v = try volt.run(.{
        .allocator = std.testing.allocator,
    }, channelRoundtrip, .{});
    try std.testing.expectEqual(@as(u32, 42), v);
}

fn channelRoundtrip() !u32 {
    var ch = try volt.channel.Channel(u32).init(std.testing.allocator, 4);
    defer ch.deinit();

    const sender = try volt.launch(struct {
        fn body(c: *volt.channel.Channel(u32)) !void {
            try c.send(42);
        }
    }.body, .{&ch});
    defer volt.destroyJob(sender);

    const v = try ch.recv();
    try sender.join();
    return v;
}
```

### Multi-worker stress

For concurrency bugs that only surface across cores:

```zig
test "mutex stress: 8 coros × 200 increments == 1600" {
    const result = try volt.run(.{
        .allocator = std.testing.allocator,
    }, mutexStressRoot, .{ @as(u32, 8), @as(u32, 200) });
    try std.testing.expectEqual(@as(u64, 1600), result);
}

fn mutexStressRoot(workers: u32, iters: u32) !u64 {
    var mu: volt.sync.Mutex = .{};
    var counter: u64 = 0;

    const alloc = std.testing.allocator;
    const jobs = try alloc.alloc(*volt.Job, workers);
    defer alloc.free(jobs);

    for (jobs) |*j| {
        j.* = try volt.launch(struct {
            fn body(mu_: *volt.sync.Mutex, c: *u64, n: u32) void {
                var i: u32 = 0;
                while (i < n) : (i += 1) {
                    mu_.lock();
                    c.* += 1;
                    mu_.unlock();
                }
            }
        }.body, .{ &mu, &counter, iters });
    }
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();
    return counter;
}
```

The default worker count is `getCpuCount()` so this actually
exercises multi-worker scheduling. To force single-worker for
deterministic comparison:

```zig
const result = try volt.run(.{
    .allocator = std.testing.allocator,
    .deterministic = true,   // single worker, fixed RNG
}, mutexStressRoot, .{ ... });
```

### Asserting cancellation

```zig
test "withTimeout: cancels long-running task" {
    const result = volt.run(.{
        .allocator = std.testing.allocator,
    }, timeoutRoot, .{});
    try std.testing.expectError(error.Timeout, result);
}

fn timeoutRoot() !u32 {
    return try volt.withTimeout(volt.Duration.fromMillis(50), longTask, .{});
}

fn longTask() !u32 {
    try volt.sleep(volt.Duration.fromSecs(60));
    return 0;
}
```

`expectError` matches against the error union; `error.Timeout`
fires before `volt.sleep` completes.

## Patterns to avoid

### Don't use `std.Thread.sleep` to time-coordinate

```zig
// BAD:
const j = try volt.launch(produce, .{ &ch });
std.Thread.sleep(100 * std.time.ns_per_ms);   // hope produce ran by now
const v = try ch.recv();
```

`std.Thread.sleep` blocks a Volt worker. The test will pass on
fast machines and flake on slow ones. Use `volt.sleep` if you
need a delay, or restructure to use `Notify` / `Barrier` for
proper synchronization.

### Don't `expectEqual` non-deterministic state

```zig
// BAD:
fn shouldBeFour() !u32 {
    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    // 4 coroutines each increment counts[i]
    return counts[0] + counts[1] + counts[2] + counts[3];
}
test "broken" {
    const v = try volt.run(.{...}, shouldBeFour, .{});
    try std.testing.expectEqual(@as(u32, 4), v);   // works, but hides race
}
```

If `counts[i]++` isn't atomic and synchronization between the
producers and the read isn't explicit, the test passes by luck.
Better:

```zig
fn shouldBeFour() !u32 {
    var counts: [4]std.atomic.Value(u32) = .{
        .{ .raw = 0 }, .{ .raw = 0 }, .{ .raw = 0 }, .{ .raw = 0 }
    };
    // 4 coroutines each fetchAdd(1, .monotonic)
    // ... join them ...
    var sum: u32 = 0;
    for (&counts) |*c| sum += c.load(.acquire);
    return sum;
}
```

The atomic store + acquire-load ordering makes the read
correctness explicit.

### Don't test exact context-switch counts

The work-stealing scheduler is deliberately non-deterministic
about *which* worker runs *which* coroutine. Tests that assert
"this coroutine ran on worker 0" will flake. Test invariants
(values, ordering relations, counts) — not scheduling
implementation details.

## Leak detection

`std.testing.allocator` panics on leaks at the end of the test:

```
[gpa] (err): memory address 0x... leaked
```

If this fires:

1. Look for missing `deinit` on a Channel/Broadcast/JoinSet.
2. Look for missing `destroyJob` / `destroyTask`.
3. If neither, run with `--summary all` to see the allocation
   site in the trace.

Volt-internal allocations should always be freed before
`volt.run` returns. Per-coroutine stacks go back to the slab
pool, which the runtime drains on teardown. If you're seeing
leaks attributed to `coroutine/stack.zig` etc., file an issue —
that's a runtime bug, not a test bug.

## Layered tests

For testing a library you've built on top of Volt:

```zig
test "my library: end-to-end" {
    const result = try volt.run(.{
        .allocator = std.testing.allocator,
    }, struct {
        fn body() !u32 {
            var lib = try MyLib.init(std.testing.allocator);
            defer lib.deinit();

            return try lib.doTheThing();
        }
    }.body, .{});
    try std.testing.expectEqual(@as(u32, 42), result);
}
```

Your library wraps Volt-aware code; the test wraps your library
in `volt.run`. Standard onion.

## Inline vs separate test files

Volt's convention:

- **Per-primitive unit tests**: inline in the source file
  (`src/sync/Mutex.zig` etc.). The standard `// ─── Tests ───`
  section at the bottom.
- **Cross-file integration tests**: in `src/test/` (e.g.,
  `multi_worker_test.zig`, `channel_integration_test.zig`).

Inline tests are easier to keep in sync with the code they test.
Cross-file integration tests are for behaviors that span
multiple primitives.

## Reproducing flaky tests

If a test sometimes hangs or fails in CI:

1. Reproduce locally with `deterministic = true`. If it now
   reproduces, you have a deterministic-mode failure that's
   easy to debug.
2. If it only fails under multi-worker, run it in a tight loop:

   ```sh
   for i in $(seq 200); do zig build test --filter "your test" || break; done
   ```

   200 iterations is usually enough to catch a 1% flake.
3. Add `std.log.debug` traces on the suspected critical path,
   re-run.
4. If it's a runtime bug (panic, hang in dispatch loop, memory
   error), file an issue with the trace.

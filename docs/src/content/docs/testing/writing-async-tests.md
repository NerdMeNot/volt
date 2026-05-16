---
title: Writing async tests
description: Patterns for testing coroutine code — keeping tests fast, deterministic, and meaningful.
---

Testing async code in Volt is "test it like you'd test a
synchronous function" — because Volt-shape code looks
synchronous. The wrapper around every test:

```zig
test "my test" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,                  // single-worker for leak detection
    });
    defer rt.deinit();
    const result = try (try rt.run(testRoot, .{}));
    try std.testing.expectEqual(expected, result);
}

fn testRoot() !ResultType {
    // your coroutine code
}
```

That's it. `Runtime.init` + `rt.run` bootstraps the runtime;
`testRoot` runs as the root coroutine; `std.testing.allocator`
catches leaks at `deinit`.

## Allocator choice

Volt has two test-friendly allocators:

| Allocator | Use for | Why |
|---|---|---|
| `std.testing.allocator` | Single-worker tests (`workers = 1`) | Leak detection; the canonical default |
| `std.heap.smp_allocator` | Multi-worker tests | Thread-safe; `testing.allocator` is NOT thread-safe on all paths |

`std.testing.allocator` is `GeneralPurposeAllocator{.safety =
true}`. Its stack-trace capture for double-free detection uses a
process-global hash map that isn't safe under contention. Tests
that use it under multi-worker crash with `EXC_BAD_ACCESS` in
`array_hash_map.ensureTotalCapacityContext`.

The pattern: **single-worker for leak gates, multi-worker for
concurrency exercises.**

## Basic patterns

### Test a primitive's happy path

```zig
test "Spsc: send + recv round-trip" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,
    });
    defer rt.deinit();
    const v = try (try rt.run(spscRoundtrip, .{}));
    try std.testing.expectEqual(@as(u32, 42), v);
}

fn spscRoundtrip() !u32 {
    var ch: volt.Spsc(u32, 4) = .{};

    const sender = try volt.spawn(struct {
        fn body(c: *volt.Spsc(u32, 4)) !void {
            try c.send(42);
        }
    }.body, .{&ch});

    const v = try ch.recv();
    _ = sender.join();
    return v;
}
```

Note: `sender.join()` is required — it frees the Task struct. The
leak detector catches missing joins.

### Multi-worker stress

For concurrency bugs that only surface across cores:

```zig
test "mutex stress: 8 coros × 200 increments == 1600" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.heap.smp_allocator,    // multi-worker → smp
        // workers defaults to NumCPU
    });
    defer rt.deinit();
    const result = try (try rt.run(mutexStress, .{}));
    try std.testing.expectEqual(@as(u64, 1600), result);
}

fn mutexStress() !u64 {
    var mu = volt.Mutex.init();
    defer mu.deinit();
    var counter: u64 = 0;

    const workers: u32 = 8;
    const iters: u32 = 200;

    const tasks = try std.heap.smp_allocator.alloc(*volt.Task(void), workers);
    defer std.heap.smp_allocator.free(tasks);

    for (tasks) |*t| {
        t.* = try volt.spawn(struct {
            fn body(mu_: *volt.Mutex, c: *u64, n: u32) void {
                var i: u32 = 0;
                while (i < n) : (i += 1) {
                    mu_.lock();
                    c.* += 1;
                    mu_.unlock();
                }
            }
        }.body, .{ &mu, &counter, iters });
    }
    for (tasks) |t| t.join();
    return counter;
}
```

`getCpuCount()` workers contending on one Mutex; the assertion
catches any non-atomic increment.

### Asserting cancellation

```zig
test "cancel-aware recv wakes on Cancel.fire" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.heap.smp_allocator,
    });
    defer rt.deinit();
    try std.testing.expectError(error.Cancelled, try rt.run(cancelTest, .{}));
}

fn cancelTest() !void {
    var c = volt.Cancel.init(volt.runtime());
    defer c.deinit();
    var ch: volt.Spsc(u32, 4) = .{};

    // Spawn a coroutine that fires the Cancel after a delay.
    const firer = try volt.spawn(struct {
        fn body(cancel: *volt.Cancel) void {
            volt.sleep(20 * std.time.ns_per_ms);
            cancel.fire();
        }
    }.body, .{&c});

    // Block on cancel-aware recv; should wake with error.Cancelled.
    _ = try ch.recvCancel(&c);

    firer.join();
}
```

The cancel-aware recv parks on the channel. The firer eventually
calls `Cancel.fire`, which wakes the parked recv via the parking
lot, which observes the Cancel flag, returns `error.Cancelled`.

## Patterns to avoid

### Don't use `std.Thread.sleep` to time-coordinate

```zig
// BAD:
const t = try volt.spawn(produce, .{&ch});
std.Thread.sleep(100 * std.time.ns_per_ms);   // hope produce ran by now
const v = try ch.recv();
```

`std.Thread.sleep` blocks a Volt worker. The test will pass on
fast machines and flake on slow ones. Use `volt.sleep` if you
need a delay, or restructure to use `Notify` for proper
synchronisation.

### Don't `expectEqual` non-atomic shared state

```zig
// BAD:
fn shouldBeFour() !u32 {
    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    // 4 coroutines each increment counts[i] (non-atomic)
    // ... join them ...
    return counts[0] + counts[1] + counts[2] + counts[3];
}
```

If `counts[i]++` isn't atomic and synchronisation between the
producers and the read isn't explicit, the test passes by luck.
Better:

```zig
fn shouldBeFour() !u32 {
    var counts: [4]std.atomic.Value(u32) = .{
        .{ .raw = 0 }, .{ .raw = 0 }, .{ .raw = 0 }, .{ .raw = 0 }
    };
    // 4 coroutines each fetchAdd(1, .acq_rel)
    // ... join them (the joins establish happens-before)...
    var sum: u32 = 0;
    for (&counts) |*c| sum += c.load(.acquire);
    return sum;
}
```

The atomic store + acquire-load ordering makes the read
correctness explicit.

### Don't test exact scheduling

The work-stealing scheduler is deliberately non-deterministic
about which worker runs which coroutine. Tests that assert "this
coroutine ran on worker 0" will flake. Test invariants (values,
ordering relations, counts) — not scheduling implementation
details.

### Don't forget to join

```zig
// BAD:
test "missing join" {
    var rt = try volt.Runtime.init(.{ ... });
    defer rt.deinit();
    try (try rt.run(struct {
        fn b() !void {
            _ = try volt.spawn(work, .{});   // never joined
        }
    }.b, .{}));
}
// → testing.allocator reports a leak (the Task struct)
```

Every `volt.spawn` returns a `*Task(T)` that needs joining. If you
genuinely want fire-and-forget for a test, accept the leak by
using `smp_allocator` (no leak detection) — but a join is
almost always correct.

## Leak detection

`std.testing.allocator` panics on leaks at the end of the test:

```
[gpa] (err): memory address 0x... leaked
```

If this fires:

1. Look for missing `deinit` on `Watch`, `Broadcast`, or any
   sync primitive you `init`'d.
2. Look for missing `Task.join()`.
3. Look for missing `Cancel.deinit()` (and missing
   `Cancel.fire()` if the Cancel had registered waiters — leaks
   are flagged by the assertion in `Cancel.deinit`).
4. If none, the runtime might have a leak. File an issue.

Volt-internal allocations are released by `Runtime.deinit`. Per-
coroutine stacks return to the slab arena; the arena's mmap is
released as part of `deinit`. If you're seeing leaks attributed
to `runtime.zig` / `stack.zig` / etc., that's a runtime bug, not
a test bug.

## Layered tests (testing a library on top of Volt)

```zig
test "my library: end-to-end" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,
    });
    defer rt.deinit();
    try (try rt.run(struct {
        fn body() !void {
            var lib = try MyLib.init(std.testing.allocator);
            defer lib.deinit();

            const result = try lib.doTheThing();
            try std.testing.expectEqual(@as(u32, 42), result);
        }
    }.body, .{}));
}
```

The library wraps Volt-aware code; the test wraps the library in
`rt.run`. Standard onion.

## Inline vs separate test files

Volt's convention:

- **Per-primitive unit tests**: inline at the bottom of the
  source file (`src/sync.zig`, `src/channel.zig`, etc.). The
  standard `// ─── Tests ───` section.
- **Cross-cutting integration tests**: in `src/lib.zig`'s root
  test block, or in dedicated files if they grow large.

Inline tests are easier to keep in sync with the code they test.
Cross-file integration tests are for behaviours that span
multiple primitives or files.

## Reproducing flaky tests

If a test sometimes hangs or fails in CI:

1. **Reproduce locally with `workers = 1`.** If it now
   reproduces, you have a deterministic-mode failure that's easy
   to debug.
2. **If it only fails under multi-worker**, run it in a tight
   loop:

   ```sh
   for i in $(seq 200); do zig build test -- --filter "your test" || break; done
   ```

   200 iterations is usually enough to catch a 1% flake.
3. **Add `std.log.debug` traces** on the suspected critical
   path; re-run.
4. **If it's a runtime panic / hang / EXC_BAD_ACCESS**, that's a
   runtime bug. File an issue with the trace; include
   `rt.dumpState()` output if you can attach a debugger.

## See also

- [Running tests](/testing/running-tests/) — `zig build test`, allocator choices, failure interpretation.
- [Benchmarking](/testing/benchmarking/) — perf gate alongside the test gate.
- [Common pitfalls](/guides/common-pitfalls/) — the bugs to write tests against.

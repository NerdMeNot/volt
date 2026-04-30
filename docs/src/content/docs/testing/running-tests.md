---
title: Running Tests
description: zig build test, what the test suite covers, and how to interpret failures.
---

```sh
zig build test
```

Runs the full test suite — about 200 tests covering the
scheduler, every primitive, every channel, and the end-to-end
integration paths. On Apple Silicon it takes about 7 seconds.

```
Build Summary: 3/3 steps succeeded; 208/209 tests passed (1 skipped)
test success
+- run test 208 pass, 1 skip (209 total) 8s MaxRSS:880M
   +- compile test Debug native success 2s MaxRSS:390M
```

The 1 skipped test is a deliberate platform-conditional — typically
a `if (native_os == .windows) return error.SkipZigTest;` line for
something not yet ported.

## What's covered

The suite runs against the actual stackful runtime — every test
goes through `volt.run(.{ .allocator = std.testing.allocator },
test_root, .{})`. So you're testing the dispatch loop, the
reactor, and the primitive together, not in isolation.

Coverage by area:

| Area | Tests |
|---|---|
| Bootstrap (`volt.run`, spawn/launch) | ~15 |
| Multi-worker scheduling (work-stealing, injection) | ~8 |
| I/O (TCP loopback echo, pipe read/write across cores) | ~10 |
| Channels (Channel, Oneshot, Watch, Broadcast, select) | ~50 |
| Sync (Mutex, RwLock, Semaphore, Notify, Barrier, OnceCell) | ~40 |
| Structured concurrency (Scope, JoinSet, CancellationToken) | ~15 |
| Time (sleep, withTimeout, Interval) | ~20 |
| Stack overflow recovery | ~5 |
| Stress / fuzz harnesses | ~10 |
| Address parsing | ~4 |
| Process / signal / fs | ~10 |

Each primitive has unit tests AND a multi-worker stress test
(spawns N coroutines, hammers the primitive, asserts an
invariant). The stress tests are what surface concurrency bugs.

## Stress tests

Some tests run for measurable wall-clock time — multi-worker
mutexes, channel hangs that took weeks to find before they were
fixed. The defaults are tuned so the suite finishes in seconds;
you can crank up the iteration count locally:

```sh
zig build test -Dstress=200          # if such an option were exposed
```

(Not currently a build option — the iteration counts are constants
in the test files. Edit the constant and rebuild if you need to
hammer something harder.)

## Cross-compile

Volt's CI matrix runs `zig build-lib` for six target triples:

```sh
zig build-lib src/lib.zig -target x86_64-linux-gnu     -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-linux-gnu    -lc -fno-emit-bin
zig build-lib src/lib.zig -target x86_64-windows-gnu   -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-windows-gnu  -lc -fno-emit-bin
zig build-lib src/lib.zig -target x86_64-macos         -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-macos        -lc -fno-emit-bin
```

All six should exit `0`. If you're on Apple Silicon, you can run
the actual test build for the arm64 Linux target (compiles +
links + cannot-execute on a non-Linux host):

```sh
zig build test -Dtarget=aarch64-linux-gnu
```

Build Summary will say `1/3 steps succeeded (1 failed)` — the
failed step is the run, which can't execute Linux binaries on
Darwin. The compile step (the one that matters) is green.

## Determinism mode

For test traces you want to reproduce exactly:

```zig
test "my deterministic test" {
    const result = try volt.run(.{
        .allocator = std.testing.allocator,
        .deterministic = true,
    }, root, .{});
    // ...
}
```

`deterministic = true` forces single-worker mode and uses a fixed
RNG seed for steal selection. Same input → same scheduler trace.
Useful for bisecting flaky tests or reproducing race-window bugs
from logs.

It's not perfect — the OS thread scheduler still nudges futex
wakes, the syscall layer can re-order — but it eliminates Volt's
contributions to non-determinism.

## Filtering tests

`zig test`'s built-in filtering works:

```sh
zig build test -- --filter "channel"     # any test with "channel" in name
```

Useful when you're working on one primitive and don't want to wait
for the full suite each iteration.

## Failure interpretation

Volt's failures are usually clear:

- **Panic during dispatch** — a primitive misuse (calling Volt
  outside `volt.run`, double-park on the same Park, etc.). The
  panic message says exactly what.
- **Hang** — a missed wake or a closed channel waiting on a never-
  arriving close. Run with `--workers 1` to serialize and add
  `std.log.debug` traces; usually clear within a few iterations.
- **Memory leak detected by `std.testing.allocator`** — you
  forgot a `deinit` on a Channel, JoinSet, or the like. Or
  forgot to `destroyJob` / `destroyTask`. The allocator's report
  points at the allocation site.
- **`error.Cancelled`** unexpected — usually a `withTimeout` that
  fired earlier than you thought.

## Adding tests

Inline tests in source files are automatically picked up by `zig
build test`. The standard pattern:

```zig
test "channel: SPSC, 1000 messages, sum invariant" {
    const result = try volt.run(.{
        .allocator = std.testing.allocator,
    }, channelSpscRoot, .{});
    try std.testing.expectEqual(@as(u64, 499500), result);
}

fn channelSpscRoot() !u64 {
    var ch = try volt.channel.Channel(u64).init(std.testing.allocator, 64);
    defer ch.deinit();
    // ... use the channel ...
    return computed_sum;
}
```

Tests live inline with the code they test. Every primitive's
source has its own `// ─── Tests ───` section near the bottom.

For multi-file integration tests that don't fit cleanly inline,
there's `src/test/`. See the files there for shape:
`integration_test.zig`, `multi_worker_test.zig`,
`channel_integration_test.zig`, `scheduler_fuzz_test.zig`.

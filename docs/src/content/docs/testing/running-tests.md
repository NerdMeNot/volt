---
title: Running tests
description: zig build test, what the test suite covers, how to interpret failures, and how to write tests against the runtime.
---

```sh
zig build test
```

Runs the full Volt test suite — ~47 tests covering the scheduler,
every primitive, every channel shape, and end-to-end integration
paths. On Darwin arm64 it takes a few seconds.

The pre-commit hook does **not** run tests — it type-checks via
`zig build-lib`. CI runs the real tests on every push. The
trade-off: faster pre-commit (no `zig test` IPC hang on loaded
hosts), authoritative coverage in CI. See
[Contributing](/appendix/contributing/) for the hook setup.

## What's covered

The suite runs against the actual stackful runtime. Every test
goes through `Runtime.init` → `rt.run(...)` → `rt.deinit()` — so
you're testing the dispatch loop, the reactor, and the primitive
together, not in isolation.

Coverage by area:

| Area | Tests |
|---|---|
| Bootstrap (Runtime.init/run/deinit, spawn) | several |
| Multi-worker scheduling (work-stealing, mailbox) | several |
| Parking lot + Parker | several |
| Channels (Spsc, Mpmc, Oneshot, Watch, Broadcast) | many |
| Sync (Mutex, Notify, Semaphore) | several |
| Cancellation (Cancel + scope + cancel-aware variants) | several |
| Slab arena (alloc / free / mprotect, exhaustion) | several |
| Stack growth via SIGSEGV | a few |
| TCP loopback echo | a few |
| Memory model invariants | inline asserts |

The exact count grows as new primitives land. Run `zig build
test` to see the current total.

Each primitive has unit tests + at least one multi-worker
exercise. The 45-second `zig build stress` test is a longer-form
gate that hammers spawn-join / Mutex / Spsc concurrently — see
[Benchmarking](/testing/benchmarking/).

## Allocator choice

Tests use one of two allocators:

- **`std.heap.smp_allocator`** — thread-safe. Use for multi-worker
  tests. No leak detection.
- **`std.testing.allocator`** — leak-detecting (`GeneralPurpose
  Allocator{.safety = true}`). Use for single-worker tests.
  **Not thread-safe** on all paths (the stack-trace-capture
  hash map for double-free detection is process-global). Tests
  that use it under multi-worker crash with `EXC_BAD_ACCESS`.

Pattern:

```zig
test "single-worker behaviour with leak detection" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,
    });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

test "multi-worker stress" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.heap.smp_allocator,
        // workers defaults to NumCPU
    });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

The leak gate (`testing.allocator`) is what catches "I added a new
allocation path and forgot to free." Every new allocation in the
runtime needs a single-worker test that exercises alloc + free
via `testing.allocator`.

## Cross-compile sanity

CI compiles `zig build-lib` for Linux targets even though the
reactor doesn't ship there:

```sh
zig build-lib src/lib.zig -target x86_64-linux-gnu  -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-linux-gnu -lc -fno-emit-bin
```

Both should exit `0`. Catches type errors that would block the
eventual Linux backend port. Neither produces a working binary —
kqueue is Darwin-specific.

## Reproducible test runs

For test traces you want to reproduce exactly, set `workers = 1`:

```zig
test "deterministic single-worker" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,
    });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

Single-worker eliminates work-stealing non-determinism (the
randomized steal target choice). The OS scheduler still nudges
syscall timing, so it's not fully deterministic — but it's
predictable enough for bisecting flaky tests.

Volt does not ship a `Config.deterministic` flag — single-worker
is the closest you get.

## Filtering tests

Zig's built-in filtering works:

```sh
zig build test -- --filter "channel"     # tests matching "channel"
zig build test -- --filter "Mpmc"        # tests matching "Mpmc"
```

Useful when iterating on one primitive.

## Failure interpretation

| Symptom | Usual cause |
|---|---|
| Panic during dispatch | A primitive misuse (calling Volt outside a coroutine, joining a Task twice). The panic message says what. |
| Hang | A missed wake or a channel that's never closed. Run with `workers = 1` + `std.log.debug` traces; usually clear within a few iterations. |
| Memory leak detected by `testing.allocator` | A missing `deinit` on Watch/Broadcast, or a Task never joined. The allocator report points at the allocation site. |
| `EXC_BAD_ACCESS` in `array_hash_map.ensureTotalCapacity` | Using `testing.allocator` from multi-worker code. Switch to `smp_allocator`. |
| `error.ArenaExhausted` | Spawned more than `max_concurrent_stacks` coroutines without joining. |

## Adding tests

Inline `test "..."` blocks at the bottom of the source file that
implements the thing being tested. Zig's test discovery (`zig
build test`) picks them up automatically.

```zig
// in src/channel.zig

test "Spsc: 1000 messages, sum invariant" {
    var rt = try volt.Runtime.init(.{
        .allocator = std.testing.allocator,
        .workers = 1,
    });
    defer rt.deinit();
    const sum = try (try rt.run(spscSumRoot, .{}));
    try std.testing.expectEqual(@as(u64, 499500), sum);
}

fn spscSumRoot() !u64 {
    var ch: Spsc(u64, 16) = .{};
    // ... producer + consumer ...
    return computed_sum;
}
```

Tests near implementation = read alongside the code = harder to
let them drift. Every `src/*.zig` file has its tests at the
bottom under a `// ─── Tests ───` comment.

## Diagnostic helpers in tests

`rt.dumpState()` prints scheduler atomics. Call it from a test
right before an assert if you need to know what the scheduler is
doing:

```zig
test "investigate hang" {
    // ... setup ...
    if (suspected_hang) rt.dumpState();
    // ...
}
```

## See also

- [Writing async tests](/testing/writing-async-tests/) — patterns for testing coroutine code.
- [Benchmarking](/testing/benchmarking/) — perf gate, run discipline.
- [Contributing](/appendix/contributing/) — bench-gate protocol, when to add tests.

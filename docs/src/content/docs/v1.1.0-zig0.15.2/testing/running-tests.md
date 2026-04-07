---
title: Running Tests
description: How to run the Volt test suites, understand the test structure, and write new tests using the loom-style model checker.
---

Volt has four test suites that cover different aspects of correctness. All tests run through the Zig build system and require Zig 0.15.2 or later.

## Test commands

| Command | Suite | Count | What it tests |
|---------|-------|-------|---------------|
| `zig build test` | Unit tests | 588+ | Inline tests in source files |
| `zig build test-concurrency` | Concurrency | 83 | Loom-style interleaving exploration |
| `zig build test-robustness` | Robustness | 35+ | Edge cases, boundary conditions |
| `zig build test-stress` | Stress | varies | Real threads under sustained load |
| `zig build test-all` | All | 700+ | Every suite in one command |

Run all tests before submitting a pull request:

```bash
zig build test-all
```

To run a specific suite in isolation:

```bash
# Just the concurrency tests
zig build test-concurrency

# Unit tests with verbose output
zig build test -- --summary all
```

## Test directory structure

```
tests/
  common.zig              # Re-exports all test utilities
  common/
    concurrency.zig        # ConcurrentCounter, Latch, Barrier, runConcurrent
    model.zig              # Model checker: loomRun, loomQuick, loomThorough
    assertions.zig         # assertPending, assertReady, assertRecv, etc.
  test_config.zig          # StandardConfigs, autoConfig

  concurrency/             # Loom-style tests (83 tests)
    all.zig                # Entry point for zig build test-concurrency
    mutex_loom_test.zig
    semaphore_loom_test.zig
    rwlock_loom_test.zig
    barrier_loom_test.zig
    notify_loom_test.zig
    oncecell_loom_test.zig
    channel_loom_test.zig
    oneshot_loom_test.zig
    broadcast_loom_test.zig
    watch_loom_test.zig
    select_loom_test.zig

  robustness/              # Edge case tests (35+ tests)
    all.zig
    edge_cases_test.zig

  stress/                  # Load tests
    all.zig
```

Each suite has an `all.zig` entry point that the build system references. The `build.zig` wires each entry point to a build step:

```zig
// From build.zig -- concurrency test setup
const concurrency_mod = b.createModule(.{
    .root_source_file = b.path("tests/concurrency/all.zig"),
    .target = target,
    .optimize = optimize,
});
concurrency_mod.addImport("volt", volt_mod);
concurrency_mod.addImport("test_config", test_config_mod);
concurrency_mod.addImport("common", common_mod);
```

## The Model-based testing approach

Volt uses a loom-style model checker inspired by [Tokio's use of the loom crate](https://github.com/tokio-rs/loom). Since Zig does not have an equivalent to loom, Volt implements its own randomized schedule exploration.

### How the Model works

The `Model` struct in `tests/common/model.zig` provides systematic concurrency testing:

1. Each test runs multiple iterations with different random seeds.
2. At critical synchronization points, the model probabilistically inserts `Thread.yield()` calls.
3. Yield probability starts high (0.5) and decreases over iterations, exploring diverse interleavings early and then focusing on fast paths.

```zig
pub const Model = struct {
    iteration: usize,
    max_iterations: usize,
    seed: u64,
    rng: ThreadRng,
    yield_probability: f64,
    // ...

    /// Call at critical synchronization points
    pub fn yield(self: *Self) void {
        if (self.rng.chance(self.yield_probability)) {
            std.Thread.yield() catch {};
        }
    }

    /// Higher probability yield for critical sections
    pub fn yieldCritical(self: *Self) void { ... }
};
```

### Running tests with the Model

The `loomRun`, `loomQuick`, and `loomThorough` helpers wrap your test function and run it across multiple iterations:

```zig
const common = @import("common");

test "mutex fairness under contention" {
    try common.loomRun(struct {
        fn run(model: *common.Model) !void {
            var mutex = Mutex.init();
            var counter = std.atomic.Value(usize).init(0);

            const threads = try common.runConcurrent(4, struct {
                fn work(m: *common.Model, mtx: *Mutex, ctr: *std.atomic.Value(usize)) void {
                    for (0..100) |_| {
                        m.yield();  // Explore interleavings
                        mtx.tryLock() orelse continue;
                        defer mtx.unlock();
                        _ = ctr.fetchAdd(1, .monotonic);
                    }
                }
            }.work, .{ model, &mutex, &counter });

            for (threads) |t| t.join();
            try std.testing.expect(counter.load(.acquire) > 0);
        }
    }.run);
}
```

- `loomQuick` -- 10 iterations, fast CI feedback
- `loomRun` -- 50 iterations, good coverage
- `loomThorough` -- 200 iterations, deep exploration (use sparingly)

### Verbosity

Set `LOOM_VERBOSE=1` to see yield point statistics:

```bash
LOOM_VERBOSE=1 zig build test-concurrency
```

## Writing new tests

### Unit tests

Add inline tests at the bottom of source files. These run with `zig build test`:

```zig
// src/sync/Mutex.zig
test "Mutex - tryLock and unlock" {
    var mutex = Mutex.init();
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
    // Should be unlocked now
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}
```

### Concurrency tests

Create a file in `tests/concurrency/` and add it to `all.zig`:

```zig
// tests/concurrency/my_primitive_loom_test.zig
const std = @import("std");
const common = @import("common");
const volt = @import("volt");

test "my primitive - concurrent access" {
    try common.loomRun(struct {
        fn run(model: *common.Model) !void {
            // Your concurrency test here
            _ = model;
        }
    }.run);
}
```

Then add the import to `tests/concurrency/all.zig`:

```zig
comptime {
    _ = @import("my_primitive_loom_test.zig");
}
```

### Robustness tests

Robustness tests exercise edge cases: zero-size buffers, maximum values, closed channels, cancelled operations. They live in `tests/robustness/`.

### Stress tests

Stress tests run real threads under sustained load for longer durations. They live in `tests/stress/` and may take several seconds to complete.

## CI integration

The GitHub Actions CI runs all test suites on every push and pull request:

```yaml
# .github/workflows/ci.yml (simplified)
steps:
  - uses: actions/checkout@v4
  - name: Install Zig
    uses: goto-bus-stop/setup-zig@v2
    with:
      version: 0.15.2
  - name: Run all tests
    run: zig build test-all
```

The CI matrix covers:
- Linux x86_64 (primary)
- macOS aarch64 (Apple Silicon)
- Multiple optimization levels (Debug, ReleaseSafe, ReleaseFast)

Tests that are platform-specific use `builtin.os.tag` guards to skip gracefully on unsupported platforms.

## Troubleshooting

**Tests hang**: Concurrency bugs can cause deadlocks. Use `timeout` to bound test execution:

```bash
timeout 60 zig build test-concurrency
```

**Flaky failures**: If a test fails intermittently, it likely has a race condition. Increase the iteration count with `loomThorough` and add `model.yieldCritical()` at suspected race points.

**Platform-specific failures**: Check `builtin.os.tag` and `builtin.cpu.arch` in the test. Some tests require specific platform features (e.g., futex on Linux, kqueue on macOS).

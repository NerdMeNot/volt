//! Loom-Style Model Checker for Zig
//!
//! Provides systematic concurrency testing by exploring different
//! thread interleavings. This is a practical alternative to Rust's loom
//! crate, using randomized scheduling to discover race conditions.
//!
//! The model checker works by:
//! 1. Running tests multiple times with different seeds
//! 2. Injecting yield points at critical locations
//! 3. Tracking execution history for reproducibility
//!
//! Reference: tokio/tokio/src/loom/

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;
const ThreadRng = @import("concurrency.zig").ThreadRng;

/// Model checker state for systematic exploration
pub const Model = struct {
    /// Current iteration number
    iteration: usize,
    /// Maximum iterations to run
    max_iterations: usize,
    /// Seed for this iteration
    seed: u64,
    /// Thread-local RNG
    rng: ThreadRng,
    /// Yield probability (0.0 to 1.0)
    yield_probability: f64,
    /// Track yield points hit
    yield_count: Atomic(usize),
    /// Track potential interleavings explored
    interleaving_count: Atomic(usize),
    /// Enable verbose logging
    verbose: bool,

    const Self = @This();

    pub fn init(iteration: usize, max_iterations: usize) Self {
        const seed = computeSeed(iteration);
        return .{
            .iteration = iteration,
            .max_iterations = max_iterations,
            .seed = seed,
            .rng = ThreadRng.init(seed),
            .yield_probability = computeYieldProbability(iteration, max_iterations),
            .yield_count = Atomic(usize).init(0),
            .interleaving_count = Atomic(usize).init(0),
            .verbose = std.posix.getenv("LOOM_VERBOSE") != null,
        };
    }

    /// Compute seed that provides good coverage over iterations
    fn computeSeed(iteration: usize) u64 {
        // Mix iteration with constant to get varied seeds
        const base: u64 = @intCast(iteration);
        return base *% 0x9E3779B97F4A7C15 +% 0xC6A4A7935BD1E995;
    }

    /// Compute yield probability - higher for early iterations
    fn computeYieldProbability(iteration: usize, max_iterations: usize) f64 {
        // Start high (0.5), decrease to 0.1 over iterations
        const progress = @as(f64, @floatFromInt(iteration)) /
            @as(f64, @floatFromInt(max_iterations));
        return 0.5 - (progress * 0.4);
    }

    /// Critical yield point - call at important synchronization points
    pub fn yield(self: *Self) void {
        _ = self.yield_count.fetchAdd(1, .monotonic);

        if (self.rng.chance(self.yield_probability)) {
            _ = self.interleaving_count.fetchAdd(1, .monotonic);
            std.Thread.yield() catch {};
        }
    }

    /// Yield with higher probability (for critical sections)
    pub fn yieldCritical(self: *Self) void {
        _ = self.yield_count.fetchAdd(1, .monotonic);

        // Double the yield probability for critical points
        if (self.rng.chance(@min(self.yield_probability * 2.0, 0.8))) {
            _ = self.interleaving_count.fetchAdd(1, .monotonic);
            std.Thread.yield() catch {};
        }
    }

    /// Random spin to perturb timing
    pub fn spin(self: *Self) void {
        const iterations = self.rng.range(50) + 1;
        var x: u64 = self.seed;
        for (0..iterations) |_| {
            x = x *% 0x5851F42D4C957F2D +% 1;
        }
        std.mem.doNotOptimizeAway(&x);
    }

    /// Check if should continue testing
    pub fn shouldContinue(self: *const Self) bool {
        return self.iteration < self.max_iterations;
    }

    /// Get statistics for this iteration
    pub fn stats(self: *const Self) Stats {
        return .{
            .iteration = self.iteration,
            .seed = self.seed,
            .yields = self.yield_count.load(.acquire),
            .interleavings = self.interleaving_count.load(.acquire),
        };
    }

    pub const Stats = struct {
        iteration: usize,
        seed: u64,
        yields: usize,
        interleavings: usize,
    };

    /// Log if verbose mode enabled
    pub fn log(self: *const Self, comptime fmt: []const u8, args: anytype) void {
        if (self.verbose) {
            std.debug.print("[loom iter={d} seed=0x{x:0>16}] " ++ fmt ++ "\n", .{ self.iteration, self.seed } ++ args);
        }
    }
};

/// Run a loom-style test with systematic exploration
pub fn loomModel(
    max_iterations: usize,
    comptime testFn: fn (*Model) anyerror!void,
) !void {
    return loomModelInternal(max_iterations, testFn, true);
}

/// Silent version for tests that expect failures
fn loomModelSilent(
    max_iterations: usize,
    comptime testFn: fn (*Model) anyerror!void,
) !void {
    return loomModelInternal(max_iterations, testFn, false);
}

fn loomModelInternal(
    max_iterations: usize,
    comptime testFn: fn (*Model) anyerror!void,
    print_failures: bool,
) !void {
    var failures: usize = 0;
    var last_error: ?anyerror = null;
    var failing_seed: u64 = 0;

    for (0..max_iterations) |i| {
        var model = Model.init(i, max_iterations);

        testFn(&model) catch |err| {
            failures += 1;
            last_error = err;
            failing_seed = model.seed;

            if (model.verbose and print_failures) {
                std.debug.print(
                    "[loom] FAILURE at iteration {d}, seed=0x{x:0>16}: {}\n",
                    .{ i, model.seed, err },
                );
            }
        };
    }

    if (failures > 0) {
        if (print_failures) {
            std.debug.print(
                "[loom] {d}/{d} iterations failed. Last failure: seed=0x{x:0>16}\n",
                .{ failures, max_iterations, failing_seed },
            );
        }
        if (last_error) |err| {
            return err;
        }
    }
}

/// Simplified loom runner that uses default iteration count
/// Note: Since we spawn real threads (not simulated like Rust's loom),
/// keep iteration count reasonable to avoid excessive runtime.
pub fn loomRun(comptime testFn: fn (*Model) anyerror!void) !void {
    const iterations = blk: {
        const env = std.posix.getenv("LOOM_ITERATIONS");
        if (env) |val| {
            break :blk std.fmt.parseInt(usize, val, 10) catch 50;
        }
        break :blk 50; // Reduced from 500 - real thread spawning is expensive
    };
    try loomModel(iterations, testFn);
}

/// Quick loom test for CI (fewer iterations)
pub fn loomQuick(comptime testFn: fn (*Model) anyerror!void) !void {
    try loomModel(5, testFn);
}

/// Thorough loom test (many iterations) - use for focused debugging
pub fn loomThorough(comptime testFn: fn (*Model) anyerror!void) !void {
    try loomModel(500, testFn); // Reduced from 5000
}

/// Thread spawner that integrates with model
pub fn ModelThread(comptime Context: type) type {
    return struct {
        thread: std.Thread,
        id: usize,

        const Self = @This();

        pub fn spawn(
            model: *Model,
            id: usize,
            context: Context,
            comptime func: fn (Context, *Model, usize) void,
        ) !Self {
            const thread = try std.Thread.spawn(.{}, struct {
                fn run(ctx: Context, m: *Model, thread_id: usize) void {
                    func(ctx, m, thread_id);
                }
            }.run, .{ context, model, id });

            return .{ .thread = thread, .id = id };
        }

        pub fn join(self: Self) void {
            self.thread.join();
        }
    };
}

/// Helper to run parallel threads with model
pub fn runWithModel(
    model: *Model,
    comptime thread_count: usize,
    context: anytype,
    comptime func: fn (@TypeOf(context), *Model, usize) void,
) void {
    var threads: [thread_count]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..thread_count) |i| {
        threads[i] = std.Thread.spawn(.{}, struct {
            fn run(ctx: @TypeOf(context), m: *Model, tid: usize) void {
                func(ctx, m, tid);
            }
        }.run, .{ context, model, i }) catch continue;
        spawned += 1;
    }

    for (threads[0..spawned]) |thread| {
        thread.join();
    }
}

// ============================================================================
// Tests
// ============================================================================

test "Model yields vary by iteration" {
    const model1 = Model.init(0, 100);
    const model2 = Model.init(50, 100);
    const model3 = Model.init(99, 100);

    // Early iterations should have higher yield probability
    try testing.expect(model1.yield_probability > model3.yield_probability);
    try testing.expect(model2.yield_probability > model3.yield_probability);

    // All should be in valid range
    try testing.expect(model1.yield_probability <= 0.5);
    try testing.expect(model3.yield_probability >= 0.1);
}

test "Model seeds are unique" {
    var seeds: [100]u64 = undefined;
    for (0..100) |i| {
        const model = Model.init(i, 100);
        seeds[i] = model.seed;
    }

    // Check all seeds are unique
    for (0..100) |i| {
        for (i + 1..100) |j| {
            try testing.expect(seeds[i] != seeds[j]);
        }
    }
}

test "loom model runs multiple iterations" {
    // Just verify it doesn't crash - we can't easily count iterations
    // without using atomics or globals
    try loomModel(10, struct {
        fn test_fn(model: *Model) !void {
            _ = model;
            // Each iteration runs this function
        }
    }.test_fn);
}

test "loom detects failure" {
    // Use silent version since we expect this to fail
    const result = loomModelSilent(10, struct {
        fn test_fn(model: *Model) !void {
            if (model.iteration == 5) {
                return error.TestFailure;
            }
        }
    }.test_fn);

    try testing.expectError(error.TestFailure, result);
}

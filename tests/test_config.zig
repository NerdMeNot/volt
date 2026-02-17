//! Test Configuration
//!
//! Shared configuration for all test types in the volt test suite.
//! Controls iteration counts, thread counts, and timeouts based on test type.

const std = @import("std");
const builtin = @import("builtin");
const native_os = builtin.os.tag;

/// Cross-platform check for whether an environment variable is set.
fn hasEnv(comptime name: []const u8) bool {
    if (native_os == .windows) {
        const w_name = std.unicode.utf8ToUtf16LeStringLiteral(name);
        return std.process.getenvW(w_name) != null;
    } else {
        return std.posix.getenv(name) != null;
    }
}

/// Cross-platform getenv. Returns the value as a UTF-8 slice, or null.
/// On Windows, writes into the provided buffer to convert from UTF-16.
fn getenv(comptime name: []const u8, buf: []u8) ?[]const u8 {
    if (native_os == .windows) {
        const w_name = std.unicode.utf8ToUtf16LeStringLiteral(name);
        const w_value = std.process.getenvW(w_name) orelse return null;
        const len = std.unicode.utf16LeToUtf8(buf, w_value) catch return null;
        return buf[0..len];
    } else {
        return std.posix.getenv(name);
    }
}

/// Test intensity levels
pub const Intensity = enum {
    /// Quick smoke tests (CI-friendly)
    smoke,
    /// Standard development tests
    standard,
    /// Extended stress tests
    stress,
    /// Exhaustive tests (nightly)
    exhaustive,

    pub fn fromEnv() Intensity {
        var buf: [64]u8 = undefined;
        const env = getenv("VOLT_TEST_INTENSITY", &buf) orelse return .standard;
        return std.meta.stringToEnum(Intensity, env) orelse .standard;
    }
};

/// Get configuration based on intensity level
pub fn getConfig(intensity: Intensity) Config {
    return switch (intensity) {
        .smoke => Config{
            .thread_count = 4,
            .iterations = 100,
            .task_count = 10,
            .timeout_ms = 1_000,
            .loom_iterations = 50,
        },
        .standard => Config{
            .thread_count = 8,
            .iterations = 1_000,
            .task_count = 100,
            .timeout_ms = 5_000,
            .loom_iterations = 500,
        },
        .stress => Config{
            .thread_count = 16,
            .iterations = 50_000,
            .task_count = 5_000,
            .timeout_ms = 60_000,
            .loom_iterations = 1_000,
        },
        .exhaustive => Config{
            .thread_count = 32,
            .iterations = 200_000,
            .task_count = 20_000,
            .timeout_ms = 180_000,
            .loom_iterations = 5_000,
        },
    };
}

pub const Config = struct {
    /// Number of threads to spawn for concurrent tests
    thread_count: usize,
    /// Number of iterations for stress loops
    iterations: usize,
    /// Number of logical tasks/operations per test
    task_count: usize,
    /// Timeout in milliseconds for test completion
    timeout_ms: u64,
    /// Number of iterations for loom-style exploration
    loom_iterations: usize,

    /// Get thread count, capped at available CPUs
    pub fn effectiveThreadCount(self: Config) usize {
        const cpu_count = std.Thread.getCpuCount() catch 4;
        return @min(self.thread_count, cpu_count);
    }

    /// Convert timeout to nanoseconds
    pub fn timeoutNs(self: Config) u64 {
        return self.timeout_ms * std.time.ns_per_ms;
    }
};

/// Standard configurations for different test scenarios
pub const StandardConfigs = struct {
    /// Quick smoke test
    pub const smoke = getConfig(.smoke);
    /// Standard development
    pub const standard = getConfig(.standard);
    /// Stress testing
    pub const stress = getConfig(.stress);
    /// Exhaustive testing
    pub const exhaustive = getConfig(.exhaustive);

    /// Get config from environment or default to standard
    pub fn fromEnv() Config {
        return getConfig(Intensity.fromEnv());
    }
};

/// Debug mode detection
pub const is_debug = builtin.mode == .Debug;

/// Check if running in CI
pub fn isCI() bool {
    return hasEnv("CI") or hasEnv("GITHUB_ACTIONS");
}

/// Get appropriate config for current environment.
/// Used by concurrency and robustness tests.
pub fn autoConfig() Config {
    if (isCI()) {
        return StandardConfigs.smoke;
    }
    return StandardConfigs.fromEnv();
}

/// Get config for stress tests.
/// Defaults to `stress` intensity (50k iterations) unless overridden.
/// Stress tests are meant to hammer primitives under sustained load,
/// so the `standard` level (1k iterations) is not meaningful.
pub fn stressAutoConfig() Config {
    if (isCI()) {
        return StandardConfigs.standard; // CI: 1k iterations (fast)
    }
    const intensity = Intensity.fromEnv();
    // If user didn't explicitly set intensity, default to stress
    if (!hasEnv("VOLT_TEST_INTENSITY")) {
        return StandardConfigs.stress;
    }
    return getConfig(intensity);
}

/// Get intensity name string for display
pub fn intensityName(cfg: Config) []const u8 {
    if (cfg.iterations >= 200_000) return "exhaustive";
    if (cfg.iterations >= 50_000) return "stress";
    if (cfg.iterations >= 1_000) return "standard";
    return "smoke";
}

test "config sanity" {
    const config = autoConfig();
    try std.testing.expect(config.thread_count > 0);
    try std.testing.expect(config.iterations > 0);
    try std.testing.expect(config.timeout_ms > 0);
}

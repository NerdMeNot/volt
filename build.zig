const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ========================================================================
    // volt Module (for downstream users)
    // ========================================================================
    const volt_mod = b.addModule("volt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ========================================================================
    // Unit Tests (inline tests in source files)
    // ========================================================================
    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = unit_test_mod,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // ========================================================================
    // Test Configuration (shared between test suites)
    // ========================================================================
    const test_config_mod = b.addModule("test_config", .{
        .root_source_file = b.path("tests/test_config.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Common test utilities module
    const common_mod = b.addModule("common", .{
        .root_source_file = b.path("tests/common.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ========================================================================
    // Stress Tests
    // ========================================================================
    const stress_step = b.step("test-stress", "Run stress tests");

    const stress_mod = b.createModule(.{
        .root_source_file = b.path("tests/stress/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    stress_mod.addImport("volt", volt_mod);
    stress_mod.addImport("test_config", test_config_mod);
    stress_mod.addImport("common", common_mod);

    const stress_tests = b.addTest(.{ .root_module = stress_mod });
    stress_step.dependOn(&b.addRunArtifact(stress_tests).step);

    // ========================================================================
    // Robustness Tests
    // ========================================================================
    const robustness_step = b.step("test-robustness", "Run robustness tests");

    const robustness_mod = b.createModule(.{
        .root_source_file = b.path("tests/robustness/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    robustness_mod.addImport("volt", volt_mod);
    robustness_mod.addImport("test_config", test_config_mod);
    robustness_mod.addImport("common", common_mod);

    const robustness_tests = b.addTest(.{ .root_module = robustness_mod });
    robustness_step.dependOn(&b.addRunArtifact(robustness_tests).step);

    // ========================================================================
    // Concurrency Tests (Loom-style)
    // ========================================================================
    const concurrency_step = b.step("test-concurrency", "Run concurrency tests");

    const concurrency_mod = b.createModule(.{
        .root_source_file = b.path("tests/concurrency/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    concurrency_mod.addImport("volt", volt_mod);
    concurrency_mod.addImport("test_config", test_config_mod);
    concurrency_mod.addImport("common", common_mod);

    const concurrency_tests = b.addTest(.{ .root_module = concurrency_mod });
    concurrency_step.dependOn(&b.addRunArtifact(concurrency_tests).step);

    // ========================================================================
    // All Tests
    // ========================================================================
    const test_all_step = b.step("test-all", "Run all tests");
    test_all_step.dependOn(test_step);
    test_all_step.dependOn(stress_step);
    test_all_step.dependOn(robustness_step);
    test_all_step.dependOn(concurrency_step);

    // ========================================================================
    // Benchmarks
    // ========================================================================

    // Separate ReleaseFast volt module for benchmarks — ensures library code
    // (Channel.zig trySend/tryRecv, Scheduler, etc.) is fully optimized even
    // when the user doesn't pass -Doptimize=ReleaseFast.
    const volt_bench_mod = b.addModule("volt_bench", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/volt_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("volt", volt_bench_mod);

    const bench_exe = b.addExecutable(.{
        .name = "volt_bench",
        .root_module = bench_mod,
    });

    const install_bench = b.addInstallArtifact(bench_exe, .{
        .dest_dir = .{ .override = .{ .custom = "bench" } },
    });

    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    run_bench.step.dependOn(&install_bench.step);

    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    const bench_compile_step = b.step("bench-compile", "Compile benchmarks (no run)");
    bench_compile_step.dependOn(&install_bench.step);

    // I/O benchmark (TCP + timers)
    const io_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/io_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    io_bench_mod.addImport("volt", volt_bench_mod);

    const io_bench_exe = b.addExecutable(.{
        .name = "io_bench",
        .root_module = io_bench_mod,
    });

    const install_io_bench = b.addInstallArtifact(io_bench_exe, .{
        .dest_dir = .{ .override = .{ .custom = "bench" } },
    });

    const run_io_bench = b.addRunArtifact(io_bench_exe);
    if (b.args) |args| {
        run_io_bench.addArgs(args);
    }
    run_io_bench.step.dependOn(&install_io_bench.step);

    const io_bench_step = b.step("bench-io", "Run TCP and timer benchmarks");
    io_bench_step.dependOn(&run_io_bench.step);

    // MPMC test (temporary - for debugging scheduler wakeup bug)
    const mpmc_mod = b.createModule(.{
        .root_source_file = b.path("bench/mpmc_test.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    mpmc_mod.addImport("volt", volt_bench_mod);
    const mpmc_exe = b.addExecutable(.{
        .name = "mpmc_test",
        .root_module = mpmc_mod,
    });
    const run_mpmc = b.addRunArtifact(mpmc_exe);
    const mpmc_step = b.step("mpmc-test", "Run MPMC wakeup test");
    mpmc_step.dependOn(&run_mpmc.step);

    // Comparison benchmark (Volt vs Tokio)
    const compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });

    const compare_exe = b.addExecutable(.{
        .name = "compare",
        .root_module = compare_mod,
    });

    const install_compare = b.addInstallArtifact(compare_exe, .{
        .dest_dir = .{ .override = .{ .custom = "bench" } },
    });

    const run_compare = b.addRunArtifact(compare_exe);
    run_compare.step.dependOn(&install_compare.step);
    run_compare.step.dependOn(&install_bench.step); // Ensure blitz_bench is built first

    const compare_step = b.step("compare", "Run Volt vs Tokio comparison benchmark");
    compare_step.dependOn(&run_compare.step);

    // ========================================================================
    // Documentation
    // ========================================================================
    const docs_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const docs_obj = b.addObject(.{
        .name = "volt-docs",
        .root_module = docs_mod,
    });

    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });

    const docs_step = b.step("docs", "Generate documentation");
    docs_step.dependOn(&install_docs.step);
}

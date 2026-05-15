//! Volt build.
//!
//! Exposes the `volt` module, plus `zig build test`, `zig build docs`,
//! per-bench targets, and POC-spike targets. The pre-stackful tree
//! has been retired; v2 (POC-validated, multi-worker stackful) is now
//! `src/`.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public module.
    const volt_mod = b.addModule("volt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    volt_mod.link_libc = true;

    // Unit tests.
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.link_libc = true;
    const unit_tests = b.addTest(.{ .root_module = test_mod });
    b.step("test", "Run unit tests").dependOn(&b.addRunArtifact(unit_tests).step);

    // Documentation.
    const docs_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const docs_obj = b.addObject(.{ .name = "volt-docs", .root_module = docs_mod });
    b.step("docs", "Generate API documentation").dependOn(&b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .{ .custom = "docs/api-reference" },
        .install_subdir = "",
    }).step);

    // Benches.
    const benches = [_]struct { name: []const u8, src: []const u8, desc: []const u8 }{
        .{ .name = "spawn-join", .src = "bench/bench_spawn_join.zig", .desc = "Runtime spawn+join across worker counts" },
        .{ .name = "spawn-hot", .src = "bench/bench_spawn_hot.zig", .desc = "Long-running spawn+join hot loop for sampling profilers" },
        .{ .name = "spawn-hot-waitall", .src = "bench/bench_spawn_hot_waitall.zig", .desc = "Spawn-hot with single wait_all per batch (matches Go wg.Wait shape)" },
        .{ .name = "scaling", .src = "bench/bench_scaling.zig", .desc = "Multi-worker scaling curve — receipt bench for scheduler changes" },
        .{ .name = "rss", .src = "bench/bench_rss.zig", .desc = "RSS per idle coroutine — receipt bench for stack-cost work" },
        .{ .name = "fanout-scaling", .src = "bench/bench_fanout_scaling.zig", .desc = "Multi-driver fan-out scaling — true parallelism receipt" },
        .{ .name = "yield", .src = "bench/bench_yield.zig", .desc = "yield one-way ctx switch (target ≤ 12 ns/op)" },
        .{ .name = "spsc", .src = "bench/bench_spsc.zig", .desc = "Spsc channel send+recv (target ≤ 35 ns/op)" },
        .{ .name = "tcp-echo", .src = "bench/bench_tcp_echo.zig", .desc = "TCP echo 64 clients × 16 RTT × 1 KB" },
        .{ .name = "mutex", .src = "bench/bench_mutex.zig", .desc = "Mutex contended (8 coros × 50k acquires)" },
        .{ .name = "parallel-compute", .src = "bench/bench_parallel_compute.zig", .desc = "Parallel CPU-bound work across N workers" },
    };
    for (benches) |bench| {
        const mod = b.createModule(.{
            .root_source_file = b.path(bench.src),
            .target = target,
            .optimize = .ReleaseFast,
        });
        mod.addImport("volt", volt_mod);
        const exe = b.addExecutable(.{
            .name = b.fmt("volt-bench-{s}", .{bench.name}),
            .root_module = mod,
        });
        const install = b.addInstallArtifact(exe, .{});
        const run = b.addRunArtifact(exe);
        run.step.dependOn(&install.step);
        b.step(b.fmt("bench-{s}", .{bench.name}), bench.desc).dependOn(&run.step);
    }

    // Stress test — runs every primitive under sustained multi-worker
    // load for 60s with a watchdog. Pre-merge gate against multi-worker
    // correctness regressions.
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("bench/bench_stress.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        mod.addImport("volt", volt_mod);
        const exe = b.addExecutable(.{
            .name = "volt-stress",
            .root_module = mod,
        });
        const install = b.addInstallArtifact(exe, .{});
        const run = b.addRunArtifact(exe);
        run.step.dependOn(&install.step);
        b.step("stress", "Run 60s multi-primitive stress test under multi-worker").dependOn(&run.step);
    }

    // POC spikes — isolated proof-of-concept benches under `spike/`.
    // Each is standalone; POCs do not import the volt module.
    const spikes = [_]struct { name: []const u8, src: []const u8, desc: []const u8 }{
        .{ .name = "A", .src = "spike/A_ctx/bench_swap_narrow.zig", .desc = "POC-A — narrow-save context switch" },
        .{ .name = "D", .src = "spike/D_parker/bench_park.zig", .desc = "POC-D — Parker variants: pthread_cond vs Darwin __ulock" },
        .{ .name = "B", .src = "spike/B_dispatch/bench_dispatch.zig", .desc = "POC-B — vtable vs enum dispatch" },
        .{ .name = "G", .src = "spike/G_sched/bench_sched.zig", .desc = "POC-G — scheduler architecture bake-off" },
        .{ .name = "C", .src = "spike/C_spawn_floor/bench_spawn.zig", .desc = "POC-C — bare-floor stackful spawn+join" },
        .{ .name = "F", .src = "spike/F_spsc/bench_spsc.zig", .desc = "POC-F — SPSC channel fast path" },
        .{ .name = "H", .src = "spike/H_reactor/bench_pipe_rtt.zig", .desc = "POC-H — tight reactor pipe RTT" },
    };
    for (spikes) |sp| {
        const mod = b.createModule(.{
            .root_source_file = b.path(sp.src),
            .target = target,
            .optimize = .ReleaseFast,
        });
        const exe = b.addExecutable(.{
            .name = b.fmt("spike-{s}", .{sp.name}),
            .root_module = mod,
        });
        const install = b.addInstallArtifact(exe, .{});
        const run = b.addRunArtifact(exe);
        run.step.dependOn(&install.step);
        b.step(b.fmt("spike-{s}", .{sp.name}), sp.desc).dependOn(&run.step);
    }
}

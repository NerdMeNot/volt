//! Volt build — minimal scaffold while the stackful core is rebuilt.
//!
//! Exposes the `volt` module + a single `test` step. Stress / robustness /
//! concurrency / integration / benchmark targets are gone with the stackless
//! tree; new ones come back as the new core stabilizes.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // x86_64 context-switch asm lives in a separate .S so the linker
    // sees the symbols on every host. Module-level comptime asm in
    // Zig 0.16 doesn't always emit on x86_64-linux ELF.
    const target_is_x86_64 = target.result.cpu.arch == .x86_64;

    // Public module
    const volt_mod = b.addModule("volt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    volt_mod.link_libc = true;
    if (target_is_x86_64) {
        volt_mod.addAssemblyFile(b.path("src/coroutine/context_x86_64.S"));
    }

    // Unit tests — runs every inline test in src/.
    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    // libc on every platform — Volt uses sigsetjmp/siglongjmp,
    // mprotect (signal-handler path), raise, std.c.* in places that
    // either don't have a raw-syscall replacement or where we'd
    // duplicate libc's correctness-sensitive logic. Pragmatic choice:
    // link libc and keep the runtime itself raw-syscall-first inside.
    unit_test_mod.link_libc = true;
    if (target_is_x86_64) {
        unit_test_mod.addAssemblyFile(b.path("src/coroutine/context_x86_64.S"));
    }
    const unit_tests = b.addTest(.{ .root_module = unit_test_mod });

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    // Documentation
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
        .install_dir = .{ .custom = "docs/api-reference" },
        .install_subdir = "",
    });
    b.step("docs", "Generate documentation").dependOn(&install_docs.step);

    // Benchmarks — `zig build bench` runs `bench/bench_core.zig`,
    // `zig build bench-io-baseline` runs the v1.1 I/O baseline used as
    // the gate for P1's BufReader-via-trait acceptance criterion (≤10%
    // overhead vs. raw `io.lowlevel.read`).
    const benches = [_]struct { name: []const u8, src: []const u8, step: []const u8, desc: []const u8 }{
        .{ .name = "core", .src = "bench/bench_core.zig", .step = "bench", .desc = "Run core benchmarks (ReleaseFast)" },
        .{ .name = "io-baseline", .src = "bench/bench_io_baseline.zig", .step = "bench-io-baseline", .desc = "Run pipe-throughput baseline used as the P1 trait-overhead gate" },
        .{ .name = "io-traits", .src = "bench/bench_io_traits.zig", .step = "bench-io-traits", .desc = "Run BufReader-via-trait pipe throughput (compare to bench-io-baseline; ≤10% slower)" },
        .{ .name = "sleep-reset", .src = "bench/bench_sleep_reset.zig", .step = "bench-sleep-reset", .desc = "Cost of cancel-and-new-sleep (resettable timeout pattern)" },
    };
    for (benches) |b_| {
        const mod = b.createModule(.{
            .root_source_file = b.path(b_.src),
            .target = target,
            .optimize = .ReleaseFast,
        });
        mod.addImport("volt", b.modules.get("volt").?);
        const exe = b.addExecutable(.{
            .name = b.fmt("volt-bench-{s}", .{b_.name}),
            .root_module = mod,
        });
        const run = b.addRunArtifact(exe);
        b.step(b_.step, b_.desc).dependOn(&run.step);
    }

    // Cookbook examples — `zig build run-echo`, `zig build run-fan-out`,
    // `zig build run-work-offload`, `zig build run-timeout-retry`.
    const examples = [_]struct { name: []const u8, src: []const u8 }{
        .{ .name = "echo", .src = "examples/echo_server.zig" },
        .{ .name = "fan-out", .src = "examples/fan_out.zig" },
        .{ .name = "work-offload", .src = "examples/work_offload.zig" },
        .{ .name = "timeout-retry", .src = "examples/timeout_retry.zig" },
    };
    for (examples) |ex| {
        const ex_mod = b.createModule(.{
            .root_source_file = b.path(ex.src),
            .target = target,
            .optimize = optimize,
        });
        ex_mod.addImport("volt", b.modules.get("volt").?);
        ex_mod.link_libc = true;
        const ex_exe = b.addExecutable(.{
            .name = b.fmt("volt-example-{s}", .{ex.name}),
            .root_module = ex_mod,
        });
        const ex_run = b.addRunArtifact(ex_exe);
        b.step(b.fmt("run-{s}", .{ex.name}), b.fmt("Run example: {s}", .{ex.name})).dependOn(&ex_run.step);
    }
}

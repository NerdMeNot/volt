//! Volt build — minimal scaffold while the stackful core is rebuilt.
//!
//! Exposes the `volt` module + a single `test` step. Stress / robustness /
//! concurrency / integration / benchmark targets are gone with the stackless
//! tree; new ones come back as the new core stabilizes.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Reactor backend selection. Default: kqueue on Darwin/BSD, io_uring
    // on Linux (≥ 5.1). `-Dreactor=epoll` opts into the legacy epoll
    // backend on Linux for older kernels or comparison.
    // Cross-platform aware: passing -Dreactor=iouring on a non-Linux
    // target is rejected at compile time inside src/io/reactor.zig.
    const ReactorChoice = enum { default, epoll, iouring };
    const reactor_choice = b.option(
        ReactorChoice,
        "reactor",
        "Reactor backend (default: kqueue on Darwin, io_uring on Linux ≥ 5.1). " ++
            "Set to 'epoll' for the legacy Linux backend.",
    ) orelse .default;

    // Reactor-trace: compile-time-gated event ring buffer in the
    // reactor (register/unregister/poll-consume events). OFF by default
    // (zero overhead in production); turn ON with `-Dreactor-trace` for
    // diagnostic builds when a leak/race is being investigated. The
    // trace dumps to stderr if `Runtime.deinit` detects a non-zero
    // pendingCount.
    const reactor_trace = b.option(
        bool,
        "reactor-trace",
        "Enable reactor event trace ring buffer for diagnostic builds (default: off).",
    ) orelse false;

    // Surface the reactor choice as a comptime-readable module so
    // src/io/reactor.zig can switch on it. Threaded through to both
    // the public `volt` module and the test module.
    const build_options = b.addOptions();
    build_options.addOption(ReactorChoice, "reactor_choice", reactor_choice);
    build_options.addOption(bool, "reactor_trace", reactor_trace);

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
    volt_mod.addOptions("build_options", build_options);
    if (target_is_x86_64) {
        volt_mod.addAssemblyFile(b.path("src/coroutine/context_x86_64.S"));
    }

    // Unit tests — runs every inline test in src/.
    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    unit_test_mod.addOptions("build_options", build_options);
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
        .{ .name = "spawn-profile", .src = "bench/bench_spawn_profile.zig", .step = "bench-spawn-profile", .desc = "Decompose spawn+join into launch / join / dispatch phases" },
        .{ .name = "compare", .src = "benchmarks/volt_compare.zig", .step = "volt-compare-bin", .desc = "(internal) Build Volt side of the vs-Go comparison; emits JSON" },
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
        const install = b.addInstallArtifact(exe, .{});
        const run = b.addRunArtifact(exe);
        run.step.dependOn(&install.step);
        b.step(b_.step, b_.desc).dependOn(&run.step);
    }

    // Volt vs Go comparison orchestrator: `zig build compare`.
    // Builds + runs both the Volt and Go benchmark binaries, parses
    // their JSON output, prints a side-by-side comparison.
    {
        const compare_mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/compare.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        const compare_exe = b.addExecutable(.{
            .name = "volt-compare",
            .root_module = compare_mod,
        });
        const compare_run = b.addRunArtifact(compare_exe);
        // The orchestrator invokes `./zig-out/bin/volt-bench-compare`
        // so the volt-compare bench must be installed first.
        const volt_compare_install = b.addInstallArtifact(
            b.addExecutable(.{
                .name = "volt-bench-compare",
                .root_module = blk: {
                    const mod = b.createModule(.{
                        .root_source_file = b.path("benchmarks/volt_compare.zig"),
                        .target = target,
                        .optimize = .ReleaseFast,
                    });
                    mod.addImport("volt", b.modules.get("volt").?);
                    break :blk mod;
                },
            }),
            .{},
        );
        compare_run.step.dependOn(&volt_compare_install.step);
        b.step("compare", "Run Volt vs Go comparative benchmarks (median of 11 iters)").dependOn(&compare_run.step);
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

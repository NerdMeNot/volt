//! Volt build.
//!
//! Exposes the `volt` module, plus `zig build test`, `zig build docs`,
//! per-bench targets, and POC-spike targets.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The x86_64 context switch ships as a separate `.S` file —
    // Zig 0.16 comptime `asm()` blocks don't reliably emit global
    // symbols on x86_64-linux ELF (works on aarch64), so we link
    // an assembly source via `addAssemblyFile`. ARM64 uses module-
    // level inline asm in `src/context_arm64.zig` and needs no
    // build-system wiring.
    //
    // Two x86_64 ABIs, picked by target OS:
    //   * System V x86-64 — Linux + macOS-Intel + BSDs
    //     (`context_x86_64_sysv.S`)
    //   * Microsoft x64   — Windows (`context_x86_64_win.S`).
    //     Bigger save set: 8 callee-saved GPRs + XMM6-XMM15.
    const link_x86_64_ctx = struct {
        fn apply(mod: *std.Build.Module, builder: *std.Build, t: std.Build.ResolvedTarget) void {
            if (t.result.cpu.arch != .x86_64) return;
            const path = if (t.result.os.tag == .windows)
                "src/context_x86_64_win.S"
            else
                "src/context_x86_64_sysv.S";
            mod.addAssemblyFile(builder.path(path));
        }
    }.apply;

    // Public module.
    const volt_mod = b.addModule("volt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    volt_mod.link_libc = true;
    link_x86_64_ctx(volt_mod, b, target);

    // Unit tests. `-Dtest-filter=substr` narrows the run to tests
    // whose name contains the substring — useful for working on a
    // single subsystem (e.g. `-Dtest-filter=fs.`) or for dodging
    // a known-hanging test while developing.
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.link_libc = true;
    link_x86_64_ctx(test_mod, b, target);
    const test_filter = b.option([]const u8, "test-filter", "Only run tests matching this substring");
    var test_opts: std.Build.TestOptions = .{ .root_module = test_mod };
    if (test_filter) |f| {
        const filters = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
        filters[0] = f;
        test_opts.filters = filters;
    }
    const unit_tests = b.addTest(test_opts);
    b.step("test", "Run unit tests").dependOn(&b.addRunArtifact(unit_tests).step);

    // Documentation.
    const docs_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    link_x86_64_ctx(docs_mod, b, target);
    const docs_obj = b.addObject(.{ .name = "volt-docs", .root_module = docs_mod });
    b.step("docs", "Generate API documentation").dependOn(&b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .{ .custom = "docs/api-reference" },
        .install_subdir = "",
    }).step);

    // Benches.
    const benches = [_]struct { name: []const u8, src: []const u8, desc: []const u8 }{
        .{ .name = "spawn-hot", .src = "bench/bench_spawn_hot.zig", .desc = "Canonical spawn-hot: single Notify barrier per 1000-batch (matches Go wg.Wait)" },
        .{ .name = "scaling", .src = "bench/bench_scaling.zig", .desc = "Multi-worker scaling curve — receipt bench for scheduler changes" },
        .{ .name = "rss", .src = "bench/bench_rss.zig", .desc = "RSS per idle coroutine — receipt bench for stack-cost work" },
        .{ .name = "fanout-scaling", .src = "bench/bench_fanout_scaling.zig", .desc = "Multi-driver fan-out scaling — true parallelism receipt" },
        .{ .name = "yield", .src = "bench/bench_yield.zig", .desc = "yield one-way ctx switch (target ≤ 12 ns/op)" },
        .{ .name = "spsc", .src = "bench/bench_spsc.zig", .desc = "Spsc channel send+recv (target ≤ 35 ns/op)" },
        .{ .name = "mpmc", .src = "bench/bench_mpmc.zig", .desc = "Mpmc channel send+recv at 1×1 / 2×2 / 4×4 shapes" },
        .{ .name = "tcp-echo", .src = "bench/bench_tcp_echo.zig", .desc = "TCP echo 64 clients × 16 RTT × 1 KB" },
        .{ .name = "mutex", .src = "bench/bench_mutex.zig", .desc = "Mutex contended (8 coros × 50k acquires)" },
        .{ .name = "parallel-compute", .src = "bench/bench_parallel_compute.zig", .desc = "Parallel CPU-bound work across N workers" },
        .{ .name = "reactor-throughput", .src = "bench/bench_reactor_throughput.zig", .desc = "Reactor wakes/s — single connection, 1-byte payload, 1 worker (cross-platform receipt)" },
        .{ .name = "reactor-fanout", .src = "bench/bench_reactor_fanout.zig", .desc = "High-fd-pressure TCP echo (512 clients × 64 RTT, NumCPU workers) — Lane 4 measurement bench" },
        .{ .name = "fs-read", .src = "bench/bench_fs_read.zig", .desc = "fs.File random 4KB pread (16 coros × 256 ops, 64 MiB file) — io_uring vs spawnBlocking perf gate" },
        .{ .name = "blocking-deadlock-repro", .src = "bench/repro_blocking_deadlock.zig", .desc = "Regression: spawnBlocking closure-lifetime deadlock under bursty concurrency (watchdog-guarded)" },
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

    // Examples — runnable apps that mirror the cookbook. Each one
    // compiles + runs end-to-end. The `examples` step builds all
    // of them, providing a quality gate against API drift: any
    // breaking change to volt's public surface fails this step
    // before any user notices via a stale doc.
    const examples = [_]struct { name: []const u8, src: []const u8, desc: []const u8 }{
        .{ .name = "echo-server", .src = "examples/echo_server.zig", .desc = "TCP echo server (Ctrl-C to stop)" },
        .{ .name = "fan-out", .src = "examples/fan_out.zig", .desc = "Parallel map + heterogeneous joinAll" },
        .{ .name = "rate-limiter", .src = "examples/rate_limiter.zig", .desc = "Bounded concurrency via Semaphore" },
        .{ .name = "udp-echo", .src = "examples/udp_echo.zig", .desc = "UDP echo round-trip (4 messages)" },
        .{ .name = "ticker-heartbeat", .src = "examples/ticker_heartbeat.zig", .desc = "Ticker firing every 100ms × 10" },
        .{ .name = "dns-lookup", .src = "examples/dns_lookup.zig", .desc = "DNS lookup via spawnBlocking" },
        .{ .name = "file-copy", .src = "examples/file_copy.zig", .desc = "Copy a file via volt.fs.copyFile" },
        .{ .name = "dir-walk", .src = "examples/dir_walk.zig", .desc = "Walk a directory tree printing each entry" },
        .{ .name = "mmap-count", .src = "examples/mmap_count.zig", .desc = "Count newlines via mmap (zero-copy)" },
        .{ .name = "file-watcher", .src = "examples/file_watcher.zig", .desc = "Watch a dir + print events (Ctrl-C to stop)" },
    };
    const examples_step = b.step("examples", "Build every example program");
    for (examples) |ex| {
        const mod = b.createModule(.{
            .root_source_file = b.path(ex.src),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("volt", volt_mod);
        const exe = b.addExecutable(.{
            .name = b.fmt("volt-example-{s}", .{ex.name}),
            .root_module = mod,
        });
        const install = b.addInstallArtifact(exe, .{});
        examples_step.dependOn(&install.step);
        const run = b.addRunArtifact(exe);
        run.step.dependOn(&install.step);
        b.step(b.fmt("run-{s}", .{ex.name}), ex.desc).dependOn(&run.step);
    }
}

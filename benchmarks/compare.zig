//! Volt vs Go comparative benchmark runner.
//!
//! Builds both implementations in release mode, runs each, parses
//! their JSON output, and prints a side-by-side comparison table.
//!
//! Usage:
//!   zig build compare
//!
//! Both sides run the SAME workloads with the SAME iteration counts,
//! median over 11 samples. We don't run them concurrently — sequential
//! execution avoids cache/thermal interference between them.

const std = @import("std");

const Color = struct {
    const red = "\x1b[0;31m";
    const green = "\x1b[0;32m";
    const yellow = "\x1b[1;33m";
    const cyan = "\x1b[0;36m";
    const bold = "\x1b[1m";
    const dim = "\x1b[2m";
    const reset = "\x1b[0m";
};

const Results = struct {
    yield_one_way_ns: u64 = 0,
    spawn_join_ns: u64 = 0,
    spawn_waitgroup_ns: u64 = 0,
    channel_spsc_16_ns: u64 = 0,
    mutex_contended_8_ns: u64 = 0,

    pub fn parseFrom(json: []const u8, allocator: std.mem.Allocator) !Results {
        const parsed = try std.json.parseFromSlice(Results, allocator, json, .{});
        defer parsed.deinit();
        return parsed.value;
    }
};

const Bench = struct {
    name: []const u8,
    field: []const u8,
    unit: []const u8 = "ns/op",
};

const BENCHES = [_]Bench{
    .{ .name = "yield (one-way ctx switch)", .field = "yield_one_way_ns" },
    .{ .name = "spawn + waitgroup-wait", .field = "spawn_waitgroup_ns" },
    .{ .name = "spawn + per-coro join", .field = "spawn_join_ns" },
    .{ .name = "channel SPSC cap=16", .field = "channel_spsc_16_ns" },
    .{ .name = "mutex contended (8 coros)", .field = "mutex_contended_8_ns" },
};

fn runCommand(
    gpa: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
    cwd: ?[]const u8,
) !struct { stdout: []u8, stderr: []u8, success: bool } {
    const result = try std.process.run(gpa, io, .{
        .argv = argv,
        .cwd = if (cwd) |dir| .{ .path = dir } else .inherit,
    });
    const success = switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
    return .{ .stdout = result.stdout, .stderr = result.stderr, .success = success };
}

fn fieldValue(r: Results, field: []const u8) u64 {
    inline for (@typeInfo(Results).@"struct".fields) |f| {
        if (std.mem.eql(u8, f.name, field)) {
            return @field(r, f.name);
        }
    }
    unreachable;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    std.debug.print("{s}{s}Building Go benchmark...{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    {
        const r = try runCommand(
            allocator,
            io,
            &.{ "go", "build", "-o", "compare-bin", "compare.go" },
            "benchmarks/go",
        );
        defer allocator.free(r.stdout);
        defer allocator.free(r.stderr);
        if (!r.success) {
            std.debug.print("{s}Go build failed{s}\n{s}\n", .{ Color.red, Color.reset, r.stderr });
            return error.GoBuildFailed;
        }
    }

    std.debug.print("{s}{s}Running Volt benchmark (this takes ~30-60s)...{s}\n", .{ Color.bold, Color.yellow, Color.reset });
    const volt_r = try runCommand(
        allocator,
        io,
        &.{"./zig-out/bin/volt-bench-compare"},
        null,
    );
    defer allocator.free(volt_r.stdout);
    defer allocator.free(volt_r.stderr);
    if (!volt_r.success) {
        std.debug.print("{s}Volt bench failed{s}\n", .{ Color.red, Color.reset });
        return error.VoltBenchFailed;
    }
    // Volt emits JSON via std.debug.print (→ stderr).
    const volt_res = Results.parseFrom(volt_r.stderr, allocator) catch |err| {
        std.debug.print("{s}Failed to parse Volt JSON{s}: {any}\nstderr was:\n{s}\n", .{ Color.red, Color.reset, err, volt_r.stderr });
        return err;
    };

    std.debug.print("{s}{s}Running Go benchmark (this takes ~30-60s)...{s}\n", .{ Color.bold, Color.yellow, Color.reset });
    const go_r = try runCommand(
        allocator,
        io,
        &.{"./compare-bin"},
        "benchmarks/go",
    );
    defer allocator.free(go_r.stdout);
    defer allocator.free(go_r.stderr);
    if (!go_r.success) {
        std.debug.print("{s}Go bench failed{s}\n", .{ Color.red, Color.reset });
        return error.GoBenchFailed;
    }
    const go_res = Results.parseFrom(go_r.stdout, allocator) catch |err| {
        std.debug.print("{s}Failed to parse Go JSON{s}: {any}\nstdout was:\n{s}\n", .{ Color.red, Color.reset, err, go_r.stdout });
        return err;
    };

    std.debug.print("\n", .{});
    std.debug.print("{s}╔════════════════════════════════════════════════════════════════════╗{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}║          Volt vs Go — comparative benchmarks                       ║{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}╚════════════════════════════════════════════════════════════════════╝{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("\n", .{});

    std.debug.print("  {s}{s:<34} {s:>14} {s:>14} {s:>10}{s}\n", .{
        Color.bold,                            "Workload",
        "Volt",                                "Go",
        "Volt / Go",
        Color.reset,
    });
    std.debug.print("  {s}{s}{s}\n", .{ Color.dim, "─" ** 78, Color.reset });

    for (BENCHES) |b| {
        const volt_ns = fieldValue(volt_res, b.field);
        const go_ns = fieldValue(go_res, b.field);
        const ratio: f64 = @as(f64, @floatFromInt(volt_ns)) / @as(f64, @floatFromInt(go_ns));

        const ratio_color = if (ratio < 0.95) Color.green else if (ratio < 1.05) Color.yellow else Color.red;
        const ratio_glyph: []const u8 = if (ratio < 0.5)
            " ★★"
        else if (ratio < 0.95)
            " ★ "
        else if (ratio < 1.05)
            " ≈ "
        else
            "   ";

        std.debug.print("  {s:<34} {d:>10} {s:<3} {d:>10} {s:<3} {s}{d:>6.2}×{s}{s}\n", .{
            b.name,
            volt_ns,
            b.unit,
            go_ns,
            b.unit,
            ratio_color,
            ratio,
            Color.reset,
            ratio_glyph,
        });
    }

    std.debug.print("\n", .{});
    std.debug.print("  {s}★★ = ≥2× faster   ★ = 5-50% faster   ≈ = within 5%   (blank) = slower{s}\n", .{ Color.dim, Color.reset });
    std.debug.print("\n", .{});
}

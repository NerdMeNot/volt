//! Bench — long-running spawn+join hot loop for sampling profilers.
//!
//! Runs `spawn+join` in batches for ~10 s wall-clock so a sampling
//! profiler (samply / Instruments) gets enough data points to
//! produce a useful flamegraph. The existing `bench_spawn_join.zig`
//! runs in milliseconds — too short for sampling.
//!
//! Edit `WORKERS` / `DURATION_S` constants below to change worker
//! count and runtime.
//!
//! A watchdog OS thread fires if no batch completes for 2 s — it
//! dumps Runtime scheduler state and exits, so hangs are loud.
//!
//! Usage:
//!   samply record ./zig-out/bin/volt-bench-spawn-hot

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn nopFn() void {}

const Ctx = struct {
    batch: u32,
    target_ns: i128,
    total_ops: u64 = 0,
    elapsed_ns: i128 = 0,
    progress: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Set after the timed loop ends so the watchdog can exit
    /// cleanly instead of firing on a quiescent shutdown.
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

extern "c" fn nanosleep(req: *const std.posix.timespec, rem: ?*std.posix.timespec) c_int;

/// Watchdog — wakes every second, checks `progress` counter. If
/// the counter hasn't moved for two ticks, dumps scheduler state
/// and exits.
fn watchdog(ctx: *Ctx, rt: *volt.Runtime) void {
    var last_progress: u64 = 0;
    var stale_secs: u32 = 0;
    while (true) {
        const ts = std.posix.timespec{ .sec = 1, .nsec = 0 };
        _ = nanosleep(&ts, null);
        if (ctx.done.load(.acquire)) return;
        const cur = ctx.progress.load(.acquire);
        if (cur == last_progress) {
            stale_secs += 1;
            if (stale_secs >= 2) {
                std.debug.print("\n!!! WATCHDOG: no progress for {d}s (last batch count = {d}) !!!\n", .{ stale_secs, cur });
                rt.dumpState();
                std.process.exit(2);
            }
        } else {
            stale_secs = 0;
            last_progress = cur;
        }
    }
}

fn benchRoot(ctx: *Ctx) !void {
    const rt: *volt.Runtime = @ptrCast(@alignCast(volt.current.require().runtime));
    const tasks = try rt.allocator.alloc(*volt.Task(void), ctx.batch);
    defer rt.allocator.free(tasks);

    // Warmup — a few batches before timing starts.
    var w: u32 = 0;
    while (w < 3) : (w += 1) {
        for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
        for (tasks) |t| t.join();
    }

    var total_ops: u64 = 0;
    const start = nanosNow();
    while (true) {
        for (tasks) |*t| t.* = try rt.spawn(nopFn, .{});
        for (tasks) |t| t.join();
        total_ops += ctx.batch;
        _ = ctx.progress.fetchAdd(1, .release);
        if (nanosNow() - start >= ctx.target_ns) break;
    }
    ctx.elapsed_ns = nanosNow() - start;
    ctx.total_ops = total_ops;
    ctx.done.store(true, .release);
}

const DURATION_S: u32 = 10;
const BATCH: u32 = 1000;

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

fn parseWorkersEnv() ?usize {
    const raw = getenv("VOLT_BENCH_WORKERS") orelse return null;
    const slice: []const u8 = std.mem.span(raw);
    return std.fmt.parseInt(usize, slice, 10) catch null;
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const workers: ?usize = parseWorkersEnv(); // null = std.Thread.getCpuCount()

    var ctx = Ctx{
        .batch = BATCH,
        .target_ns = @as(i128, DURATION_S) * std.time.ns_per_s,
    };

    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();

    std.debug.print("=== spawn+join hot loop ===\n", .{});
    std.debug.print("workers={?d} (null = NumCPU), batch={d}, duration={d}s\n", .{ workers, BATCH, DURATION_S });

    const wd = try std.Thread.spawn(.{}, watchdog, .{ &ctx, rt });
    wd.detach();

    try (try rt.run(benchRoot, .{&ctx}));

    const ns_per_op = @divTrunc(ctx.elapsed_ns, @as(i128, ctx.total_ops));
    const ops_per_sec = @divTrunc(@as(i128, ctx.total_ops) * std.time.ns_per_s, ctx.elapsed_ns);
    std.debug.print("\nTotal ops: {d}\n", .{ctx.total_ops});
    std.debug.print("Elapsed:   {d:.2} s\n", .{@as(f64, @floatFromInt(ctx.elapsed_ns)) / 1e9});
    std.debug.print("ns/op:     {d}\n", .{ns_per_op});
    std.debug.print("ops/sec:   {d}\n", .{ops_per_sec});
}

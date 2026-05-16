//! Bench — spawn-hot (canonical / Go-shaped: single wait per batch).
//!
//! The driver spawns BATCH=1000 coros, waits via an atomic-counter +
//! Notify barrier (one wait per batch — matches Go's `wg.Wait`), then
//! does fast-path joins to free Task structs. Loops for `DURATION_S`.
//!
//! Reports ns/op = wall_time / (batches × BATCH). This is the
//! apples-to-apples comparison vs `bench/go/spawn_hot.go`.
//!
//! For the Volt-specific "1000 individual joins" pattern (with its
//! per-task cleanup cost), see `bench-spawn-hot-individual`.

const std = @import("std");
const volt = @import("volt");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const Bar = struct {
    remaining: std.atomic.Value(u32),
    note: *volt.Notify,
};

fn workerCoro(bar: *Bar) void {
    if (bar.remaining.fetchSub(1, .acq_rel) == 1) {
        bar.note.notifyOne();
    }
}

const Ctx = struct {
    batch: u32,
    target_ns: i128,
    total_ops: u64 = 0,
    elapsed_ns: i128 = 0,
};

fn benchRoot(ctx: *Ctx) !void {
    const rt = volt.runtime();
    const tasks = try rt.allocator.alloc(*volt.Task(void), ctx.batch);
    defer rt.allocator.free(tasks);

    var note = volt.Notify.init();
    defer note.deinit();

    // Warmup — three batches.
    var w: u32 = 0;
    while (w < 3) : (w += 1) {
        var bar = Bar{ .remaining = std.atomic.Value(u32).init(ctx.batch), .note = &note };
        for (tasks) |*t| t.* = try rt.spawn(workerCoro, .{&bar});
        while (bar.remaining.load(.acquire) > 0) note.wait();
        for (tasks) |t| t.join();
    }

    var total_ops: u64 = 0;
    const start = nanosNow();
    while (true) {
        var bar = Bar{ .remaining = std.atomic.Value(u32).init(ctx.batch), .note = &note };
        for (tasks) |*t| t.* = try rt.spawn(workerCoro, .{&bar});
        // ONE wait — driver parks at most once per batch, woken by the
        // last worker's notifyOne. Matches Go's wg.Wait.
        while (bar.remaining.load(.acquire) > 0) note.wait();
        // Cleanup joins — fast-path because remaining == 0 implies all
        // done flags are set.
        for (tasks) |t| t.join();
        total_ops += ctx.batch;
        if (nanosNow() - start >= ctx.target_ns) break;
    }
    ctx.elapsed_ns = nanosNow() - start;
    ctx.total_ops = total_ops;
}

fn parseWorkersEnv() ?usize {
    const raw = getenv("VOLT_BENCH_WORKERS") orelse return null;
    const slice: []const u8 = std.mem.span(raw);
    return std.fmt.parseInt(usize, slice, 10) catch null;
}

const DURATION_S: u32 = 10;
const BATCH: u32 = 1000;

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const workers: ?usize = parseWorkersEnv();
    var ctx = Ctx{
        .batch = BATCH,
        .target_ns = @as(i128, DURATION_S) * std.time.ns_per_s,
    };

    var rt = try volt.Runtime.init(.{ .allocator = allocator, .workers = workers });
    defer rt.deinit();

    std.debug.print("=== spawn-hot (Notify barrier, Go wg.Wait shape) ===\n", .{});
    std.debug.print("workers={?d} (null=NumCPU), batch={d}, duration={d}s\n", .{ workers, BATCH, DURATION_S });

    try (try rt.run(benchRoot, .{&ctx}));

    const ns_per_op = @divTrunc(ctx.elapsed_ns, @as(i128, ctx.total_ops));
    const ops_per_sec = @divTrunc(@as(i128, ctx.total_ops) * std.time.ns_per_s, ctx.elapsed_ns);
    std.debug.print("\nTotal ops: {d}\n", .{ctx.total_ops});
    std.debug.print("Elapsed:   {d:.2} s\n", .{@as(f64, @floatFromInt(ctx.elapsed_ns)) / 1e9});
    std.debug.print("ns/op:     {d}\n", .{ns_per_op});
    std.debug.print("ops/sec:   {d}\n", .{ops_per_sec});
}

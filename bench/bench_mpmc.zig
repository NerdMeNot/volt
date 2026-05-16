//! Bench — Mpmc channel under contention.
//!
//! N_PRODUCERS producers each send TOTAL/N_PRODUCERS values; N_CONSUMERS
//! consumers each recv until close. Measures the cost per send+recv
//! pair (wall_ns / TOTAL).
//!
//! Reference numbers (informational):
//!   Spsc same shape:  12 ns/op (single producer + single consumer)
//!   Go chan cap=16 contended (4×4): not directly comparable; Go
//!                                   channels are MPMC by default.

const std = @import("std");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const Channel = volt.Mpmc(u64, 64);

const Ctx = struct {
    ch: *Channel,
    total: u64,
    received: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn producer(ctx: *Ctx, n_to_send: u64) void {
    var i: u64 = 0;
    while (i < n_to_send) : (i += 1) {
        ctx.ch.send(i) catch return;
    }
}

fn consumer(ctx: *Ctx) void {
    while (true) {
        _ = ctx.ch.recv() catch return;
        _ = ctx.received.fetchAdd(1, .acq_rel);
    }
}

const Roots = struct {
    ctx: *Ctx,
    n_producers: u32,
    n_consumers: u32,
    per_producer: u64,
    wall_ns: i128 = 0,
};

fn benchRoot(rr: *Roots) !void {
    const rt = volt.runtime();
    const producers = try rt.allocator.alloc(*volt.Task(void), rr.n_producers);
    defer rt.allocator.free(producers);
    const consumers = try rt.allocator.alloc(*volt.Task(void), rr.n_consumers);
    defer rt.allocator.free(consumers);

    const start = nanosNow();
    for (consumers) |*t| t.* = try rt.spawn(consumer, .{rr.ctx});
    for (producers) |*t| t.* = try rt.spawn(producer, .{ rr.ctx, rr.per_producer });

    for (producers) |t| t.join();
    rr.ctx.ch.close();
    for (consumers) |t| t.join();
    const end = nanosNow();
    rr.wall_ns = end - start;
}

fn benchOnce(allocator: std.mem.Allocator, n_producers: u32, n_consumers: u32, total: u64) !i128 {
    var rt = try volt.Runtime.init(.{ .allocator = allocator });
    defer rt.deinit();
    var ch = Channel{};
    var ctx = Ctx{ .ch = &ch, .total = total };
    const per_producer = total / n_producers;
    var rr = Roots{
        .ctx = &ctx,
        .n_producers = n_producers,
        .n_consumers = n_consumers,
        .per_producer = per_producer,
    };
    try (try rt.run(benchRoot, .{&rr}));
    std.debug.assert(ctx.received.load(.acquire) == per_producer * n_producers);
    return @divTrunc(rr.wall_ns, @as(i128, per_producer * n_producers));
}

const REPS = 11;
const WARMUPS = 3;
const TOTAL: u64 = 200_000;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    std.debug.print("\n=== Mpmc channel bench ===\n", .{});
    std.debug.print("Platform: ReleaseFast, total={d}, reps={d}, cap=64\n\n", .{ TOTAL, REPS });

    const shapes = [_]struct { p: u32, c: u32 }{
        .{ .p = 1, .c = 1 },
        .{ .p = 2, .c = 2 },
        .{ .p = 4, .c = 4 },
    };

    for (shapes) |sh| {
        var samples: [REPS]i128 = undefined;
        var w: u32 = 0;
        while (w < WARMUPS) : (w += 1) _ = try benchOnce(smp, sh.p, sh.c, TOTAL);
        for (&samples) |*s| s.* = try benchOnce(smp, sh.p, sh.c, TOTAL);
        std.mem.sort(i128, &samples, {}, std.sort.asc(i128));

        const median = samples[REPS / 2];
        std.debug.print(
            "  {d}P × {d}C: {d} ns/op (median of {d})\n",
            .{ sh.p, sh.c, median, REPS },
        );
    }

    std.debug.print("\nReference: Spsc same shape (1P × 1C): 12 ns/op.\n", .{});
    std.debug.print("Mpmc pays a Vyukov CAS-per-op vs Spsc's owner-only writes.\n", .{});
}

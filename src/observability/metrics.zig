//! Per-worker scheduler metrics.
//!
//! Snapshot the lock-free atomic counters on each worker. Useful for
//! observability dashboards (Prometheus / OTel scrape), debugging
//! steal-imbalance issues, and benchmarks.

const std = @import("std");
const runtime_mod = @import("../runtime.zig");

pub const WorkerMetrics = struct {
    worker_id: usize,
    /// Total coroutines dispatched on this worker (monotonic).
    dispatch_count: u64,
    /// Number of times this worker entered `idleStep → parker.park`.
    park_count: u64,
    /// Successful steal-from-sibling operations.
    steal_success_count: u64,
    /// Steal attempts (success + failed).
    steal_attempt_count: u64,
    /// Current local-deque depth (snapshot — may be stale by the time
    /// the caller reads).
    queue_depth: usize,
};

pub const RuntimeMetrics = struct {
    workers: []WorkerMetrics,
    /// Items currently in the global injection queue.
    injection_depth: usize,
    /// Reactor's pending wait count.
    reactor_pending: usize,

    pub fn deinit(self: *RuntimeMetrics, allocator: std.mem.Allocator) void {
        allocator.free(self.workers);
    }
};

pub fn collect(allocator: std.mem.Allocator, rt: *const runtime_mod.Runtime) !RuntimeMetrics {
    const wm = try allocator.alloc(WorkerMetrics, rt.workers.len);
    errdefer allocator.free(wm);

    for (rt.workers, 0..) |*w, i| {
        wm[i] = .{
            .worker_id = w.id,
            .dispatch_count = w.dispatch_count.load(.acquire),
            .park_count = w.park_count.load(.acquire),
            .steal_success_count = w.steal_success_count.load(.acquire),
            .steal_attempt_count = w.steal_attempt_count.load(.acquire),
            .queue_depth = 0, // Deque doesn't currently expose len; M8 hardening.
        };
    }

    return .{
        .workers = wm,
        .injection_depth = rt.injection.lenApprox(),
        .reactor_pending = rt.reactor.pendingCount(),
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const MetricsCtx = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn metricsChild(ctx: *MetricsCtx) !void {
    var k: u32 = 0;
    while (k < 4) : (k += 1) try volt.yield();
    _ = ctx.counter.fetchAdd(1, .monotonic);
}

fn metricsRoot() !u64 {
    var ctx = MetricsCtx{};
    var jobs: [16]*volt.Job = undefined;
    for (&jobs) |*j| j.* = try volt.launch(metricsChild, .{&ctx});
    defer for (jobs) |j| volt.destroyJob(j);
    for (jobs) |j| try j.join();

    const rt = @import("../runtime.zig").currentRuntime().?;
    var m = try collect(std.testing.allocator, rt);
    defer m.deinit(std.testing.allocator);

    // Total dispatches across workers should be at least: 1 (root) +
    // 16 (children, each dispatched at least once after spawn) = 17.
    // Each child also gets re-dispatched per yield, so >= 17 in
    // practice.
    var total_dispatch: u64 = 0;
    for (m.workers) |wm| total_dispatch += wm.dispatch_count;
    return total_dispatch;
}

test "metrics: dispatch_count tracks at least N+1 dispatches for N children" {
    const total = try volt.run(.{ .allocator = std.testing.allocator }, metricsRoot, .{});
    try std.testing.expect(total >= 17);
}

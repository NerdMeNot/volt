//! Variant 2 — spin-then-park. Worker spin-polls the queue for up to
//! SPIN_BUDGET_NS before parking. Models Go's runtime.findRunnable behavior
//! where M (OS thread) doesn't park unless there's truly no work anywhere.
//!
//! Spin budget tuning: Go uses ~20 µs spin before park, with backoff
//! signals to other Ms.

const std = @import("std");
const task_mod = @import("task.zig");
const parker_mod = @import("parker_pthread.zig");

const SPIN_BUDGET_NS: u64 = 30_000; // 30 µs — covers typical burst-spawn gaps

fn nanosNow() u64 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

pub const Scheduler = struct {
    queue: task_mod.TaskQueue = .{},
    workers: []Worker = &.{},
    shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    allocator: std.mem.Allocator,
    threads: []std.Thread = &.{},

    pub fn init(allocator: std.mem.Allocator, n_workers: usize) !Scheduler {
        const workers = try allocator.alloc(Worker, n_workers);
        for (workers, 0..) |*w, i| {
            w.* = .{ .id = i, .parker = .{} };
            w.parker.init();
        }
        return .{ .allocator = allocator, .workers = workers };
    }

    pub fn deinit(self: *Scheduler) void {
        for (self.workers) |*w| w.parker.deinit();
        self.allocator.free(self.workers);
        if (self.threads.len > 0) self.allocator.free(self.threads);
    }

    pub fn start(self: *Scheduler) !void {
        self.threads = try self.allocator.alloc(std.Thread, self.workers.len);
        for (self.workers, 0..) |*w, i| {
            self.threads[i] = try std.Thread.spawn(.{}, runWorker, .{ self, w });
        }
    }

    pub fn shutdownAndJoin(self: *Scheduler) void {
        self.shutdown.store(true, .release);
        for (self.workers) |*w| w.parker.unpark();
        for (self.threads) |t| t.join();
    }

    pub fn submit(self: *Scheduler, t: *task_mod.Task) void {
        self.queue.push(t);
        // Wake one parked worker if any. Workers that are spinning
        // pick up new work naturally.
        for (self.workers) |*w| {
            if (w.parker.state.load(.acquire) == parker_mod.Parker.WAITING) {
                w.parker.unpark();
                return;
            }
        }
        // No parked worker → some worker is either running or spinning.
        // No need to wake.
    }

    pub const Worker = struct {
        id: usize,
        parker: parker_mod.Parker,
    };

    fn runWorker(self: *Scheduler, w: *Worker) void {
        while (!self.shutdown.load(.acquire)) {
            // Hot loop: try to pop work.
            if (self.queue.pop()) |t| {
                t.run_fn(t);
                continue;
            }
            // Spin briefly before parking.
            const spin_deadline = nanosNow() + SPIN_BUDGET_NS;
            spin: while (nanosNow() < spin_deadline) {
                if (self.shutdown.load(.acquire)) return;
                if (self.queue.pop()) |t| {
                    t.run_fn(t);
                    break :spin;
                }
                // Empty hint to the CPU that we're in a spin loop.
                std.atomic.spinLoopHint();
            } else {
                // Spin budget exhausted without finding work — park.
                w.parker.park();
            }
        }
    }
};

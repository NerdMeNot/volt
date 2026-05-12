//! Variant 1 — always park on empty queue. Mirrors current Volt's
//! Worker.run path: findWork → if empty, park immediately.

const std = @import("std");
const task_mod = @import("task.zig");
const parker_mod = @import("parker_pthread.zig");

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
        // Wake one worker.
        for (self.workers) |*w| {
            if (w.parker.state.load(.acquire) == parker_mod.Parker.WAITING) {
                w.parker.unpark();
                return;
            }
        }
        // No parked worker found — best-effort wake of #0 (which the
        // scheduler thread keeps in a known state).
        self.workers[0].parker.unpark();
    }

    pub const Worker = struct {
        id: usize,
        parker: parker_mod.Parker,
    };

    fn runWorker(self: *Scheduler, w: *Worker) void {
        while (!self.shutdown.load(.acquire)) {
            if (self.queue.pop()) |t| {
                t.run_fn(t);
                continue;
            }
            // Park immediately on empty.
            w.parker.park();
        }
    }
};

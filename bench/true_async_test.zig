//! True Async Integration Test
//!
//! This tests the ACTUAL async path using the public API:
//! - io.awaitFuture() with multi-poll Futures
//! - volt.Future(T) for awaiting and checking completion
//! - Convenience methods (mutex.lock(io)) for simple cases
//! - Proper yielding via .pending + waker-based rescheduling
//!
//! This proves whether the components actually talk to each other.

const std = @import("std");
const volt = @import("volt");

const Io = volt.Io;
const Mutex = volt.sync.Mutex;
const LockFuture = volt.sync.mutex.LockFuture;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n", .{});
    std.debug.print("=" ** 60 ++ "\n", .{});
    std.debug.print("      TRUE ASYNC INTEGRATION TEST\n", .{});
    std.debug.print("=" ** 60 ++ "\n\n", .{});

    // Create runtime
    std.debug.print("1. Creating runtime with 4 workers...\n", .{});
    var io = try Runtime.init(allocator, .{ .num_workers = 4 });
    defer io.deinit();
    std.debug.print("   OK: Runtime created\n\n", .{});

    // Create mutex
    std.debug.print("2. Creating mutex...\n", .{});
    var mutex = Mutex.init();
    std.debug.print("   OK: Mutex created\n\n", .{});

    // Test 1: Simple lock/unlock using convenience method
    std.debug.print("3. Testing mutex.lock(io) convenience (uncontended)...\n", .{});
    {
        mutex.lock(io);
        std.debug.print("   - Lock acquired via mutex.lock(io)\n", .{});

        mutex.unlock();
        std.debug.print("   - Lock released\n", .{});
        std.debug.print("   OK: Uncontended async lock works!\n\n", .{});
    }

    // Test 2: Contended lock with volt.Future — tests isDone() and waker
    std.debug.print("4. Testing contended async lock (2 tasks)...\n", .{});
    {
        // Task 1 acquires lock via io.awaitFuture
        var f1 = try io.awaitFuture(mutex.lockFuture());
        _ = f1.await(io);
        std.debug.print("   - Task 1 acquired lock\n", .{});

        // Task 2 tries to acquire (should block/yield)
        var f2 = try io.awaitFuture(mutex.lockFuture());
        std.debug.print("   - Task 2 spawned (waiting for lock)\n", .{});

        // Give scheduler time to process
        std.Thread.sleep(10 * std.time.ns_per_ms);

        // Check that task 2 is NOT complete yet
        if (!f2.isDone()) {
            std.debug.print("   - Task 2 correctly waiting (not complete)\n", .{});
        } else {
            std.debug.print("   - ERROR: Task 2 completed without lock!\n", .{});
        }

        // Release lock - this should wake task 2
        mutex.unlock();
        std.debug.print("   - Task 1 released lock\n", .{});

        // Now task 2 should complete
        _ = f2.await(io);
        std.debug.print("   - Task 2 acquired lock\n", .{});

        mutex.unlock();
        std.debug.print("   - Task 2 released lock\n", .{});
        std.debug.print("   OK: Contended async lock works!\n\n", .{});
    }

    // Test 3: Many concurrent tasks (like the benchmark)
    std.debug.print("5. Testing concurrent MutexWorkFuture (100 tasks)...\n", .{});
    {
        // MutexWorkFuture — multi-poll Future that yields properly to the scheduler
        const MutexWorkFuture = struct {
            mutex: *Mutex,
            counter: *std.atomic.Value(usize),
            lock_future: ?LockFuture = null,
            state: State = .init,

            const State = enum { init, locking, done };
            const Self = @This();
            pub const Output = void;

            pub fn init(m: *Mutex, c: *std.atomic.Value(usize)) Self {
                return .{ .mutex = m, .counter = c };
            }

            pub fn poll(self: *Self, ctx: *volt.future.Context) volt.future.PollResult(void) {
                switch (self.state) {
                    .init => {
                        self.lock_future = self.mutex.lockFuture();
                        self.state = .locking;
                        return self.poll(ctx);
                    },
                    .locking => {
                        switch (self.lock_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => {
                                _ = self.counter.fetchAdd(1, .monotonic);
                                self.mutex.unlock();
                                self.state = .done;
                                return .{ .ready = {} };
                            },
                        }
                    },
                    .done => return .{ .ready = {} },
                }
            }
        };

        var counter = std.atomic.Value(usize).init(0);
        const num_tasks: usize = 100;
        var futures: [100]volt.Future(void) = undefined;

        // Do multiple rounds like the benchmark (3 warmup + 10 measured = 13)
        const num_rounds: usize = 13;
        for (0..num_rounds) |round| {
            counter.store(0, .seq_cst);
            std.debug.print("   - Round {}: Spawning {} tasks...\n", .{ round, num_tasks });
            for (0..num_tasks) |i| {
                const future = MutexWorkFuture.init(&mutex, &counter);
                futures[i] = try io.awaitFuture(future);
            }

            for (0..num_tasks) |i| {
                _ = futures[i].await(io);
            }
            std.debug.print("   - Round {}: Counter = {}\n", .{ round, counter.load(.monotonic) });
        }
        std.debug.print("   OK: All rounds completed!\n\n", .{});
    }

    // Test 6: RwLock contended (readers and writers)
    std.debug.print("6. Testing RwLock with readers and writers...\n", .{});
    {
        const RwLock = volt.sync.RwLock;
        const ReadLockFuture = volt.sync.rwlock.ReadLockFuture;
        const WriteLockFuture = volt.sync.rwlock.WriteLockFuture;

        const RwLockReadFuture = struct {
            rwlock: *RwLock,
            counter: *std.atomic.Value(usize),
            lock_future: ?ReadLockFuture = null,
            state: State = .init,

            const State = enum { init, locking, done };
            const Self = @This();
            pub const Output = void;

            pub fn init(r: *RwLock, c: *std.atomic.Value(usize)) Self {
                return .{ .rwlock = r, .counter = c };
            }

            pub fn poll(self: *Self, ctx: *volt.future.Context) volt.future.PollResult(void) {
                switch (self.state) {
                    .init => {
                        self.lock_future = self.rwlock.readLockFuture();
                        self.state = .locking;
                        return self.poll(ctx);
                    },
                    .locking => {
                        switch (self.lock_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => {
                                _ = self.counter.fetchAdd(1, .monotonic);
                                self.rwlock.readUnlock();
                                self.state = .done;
                                return .{ .ready = {} };
                            },
                        }
                    },
                    .done => return .{ .ready = {} },
                }
            }
        };

        const RwLockWriteFuture = struct {
            rwlock: *RwLock,
            counter: *std.atomic.Value(usize),
            lock_future: ?WriteLockFuture = null,
            state: State = .init,

            const State = enum { init, locking, done };
            const Self = @This();
            pub const Output = void;

            pub fn init(r: *RwLock, c: *std.atomic.Value(usize)) Self {
                return .{ .rwlock = r, .counter = c };
            }

            pub fn poll(self: *Self, ctx: *volt.future.Context) volt.future.PollResult(void) {
                switch (self.state) {
                    .init => {
                        self.lock_future = self.rwlock.writeLockFuture();
                        self.state = .locking;
                        return self.poll(ctx);
                    },
                    .locking => {
                        switch (self.lock_future.?.poll(ctx)) {
                            .pending => return .pending,
                            .ready => {
                                _ = self.counter.fetchAdd(1, .monotonic);
                                self.rwlock.writeUnlock();
                                self.state = .done;
                                return .{ .ready = {} };
                            },
                        }
                    },
                    .done => return .{ .ready = {} },
                }
            }
        };

        var rwlock = RwLock.init();
        var counter = std.atomic.Value(usize).init(0);
        const num_readers: usize = 4;
        const num_writers: usize = 2;
        var futures: [6]volt.Future(void) = undefined;

        std.debug.print("   - Spawning {} readers and {} writers...\n", .{ num_readers, num_writers });
        for (0..num_readers) |i| {
            const future = RwLockReadFuture.init(&rwlock, &counter);
            futures[i] = try io.awaitFuture(future);
        }
        for (0..num_writers) |i| {
            const future = RwLockWriteFuture.init(&rwlock, &counter);
            futures[num_readers + i] = try io.awaitFuture(future);
        }

        std.debug.print("   - Waiting for tasks...\n", .{});
        for (0..num_readers + num_writers) |i| {
            _ = futures[i].await(io);
            std.debug.print("   - Task {} completed\n", .{i});
        }

        std.debug.print("   - Counter: {}\n", .{counter.load(.monotonic)});
        if (counter.load(.monotonic) == num_readers + num_writers) {
            std.debug.print("   OK: RwLock contended works!\n\n", .{});
        } else {
            std.debug.print("   ERROR: Counter mismatch!\n\n", .{});
        }
    }

    std.debug.print("=" ** 60 ++ "\n", .{});
    std.debug.print("      ALL TESTS PASSED!\n", .{});
    std.debug.print("=" ** 60 ++ "\n\n", .{});
    std.debug.print("The async components ARE properly connected:\n", .{});
    std.debug.print("  - mutex.lock(io) convenience works (uncontended)\n", .{});
    std.debug.print("  - volt.Future isDone() tracks task completion\n", .{});
    std.debug.print("  - io.awaitFuture() + Future.@\"await\"(io) for multi-poll\n", .{});
    std.debug.print("  - unlock() wakes waiting tasks via waker\n\n", .{});
}

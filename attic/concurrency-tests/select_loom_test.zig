//! Select Loom Tests - Concurrent select() verification
//!
//! Tests for race conditions in select operations:
//! - Multiple channels racing to be first
//! - CancelToken claiming semantics
//! - SelectContext coordination
//! - Branch waker correctness

const std = @import("std");
const testing = std.testing;
const Atomic = std.atomic.Value;

const common = @import("common");
const Model = common.Model;

const sync = @import("volt").sync;
const CancelToken = sync.cancel_token.CancelToken;
const SelectContext = sync.select_context.SelectContext;

const channel = @import("volt").channel;
const Channel = channel.Channel;
const Selector = channel.Selector;

// ─────────────────────────────────────────────────────────────────────────────
// CancelToken Tests
// ─────────────────────────────────────────────────────────────────────────────

test "CancelToken - single winner under concurrent claims" {
    const Context = struct {
        token: CancelToken,
        winners: Atomic(usize),

        const Self = @This();

        fn init() Self {
            return .{
                .token = CancelToken.init(),
                .winners = Atomic(usize).init(0),
            };
        }

        fn worker(self: *Self, branch_id: usize) void {
            if (self.token.tryClaimWinner(branch_id)) {
                _ = self.winners.fetchAdd(1, .acq_rel);
            }
        }
    };

    const iterations = common.config.autoConfig().loom_iterations;
    var passed: usize = 0;

    for (0..iterations) |_| {
        var ctx = Context.init();
        const thread_count = 4;

        var threads: [thread_count]std.Thread = undefined;
        var spawned: usize = 0;

        for (0..thread_count) |i| {
            threads[spawned] = std.Thread.spawn(.{}, Context.worker, .{ &ctx, i }) catch continue;
            spawned += 1;
        }

        for (threads[0..spawned]) |t| {
            t.join();
        }

        // Invariant: exactly one winner
        if (ctx.winners.load(.acquire) == 1) {
            passed += 1;
        }
    }

    try testing.expectEqual(iterations, passed);
}

test "CancelToken - isCancelledFor consistency" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var token = CancelToken.init();

        // First claim should succeed
        try testing.expect(token.tryClaimWinner(2));

        // Verify cancellation state is consistent
        try testing.expect(!token.isCancelledFor(2)); // Winner not cancelled
        try testing.expect(token.isCancelledFor(0)); // Others are cancelled
        try testing.expect(token.isCancelledFor(1));
        try testing.expect(token.isCancelledFor(3));

        // Winner should be correct
        try testing.expectEqual(@as(?usize, 2), token.getWinner());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SelectContext Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SelectContext - single branch completion" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var woken = Atomic(bool).init(false);
        var ctx = SelectContext.init(3);

        const TestWaker = struct {
            fn wake(ptr: *anyopaque) void {
                const w: *Atomic(bool) = @ptrCast(@alignCast(ptr));
                w.store(true, .release);
            }
        };

        ctx.setWaker(@ptrCast(&woken), TestWaker.wake);

        // Branch 1 becomes ready
        try testing.expect(ctx.branchReady(1));
        try testing.expect(ctx.isComplete());
        try testing.expect(woken.load(.acquire));
        try testing.expectEqual(@as(?usize, 1), ctx.getWinner());
    }
}

test "SelectContext - concurrent branch completion race" {
    const TestWaker = struct {
        fn wake(ptr: *anyopaque) void {
            const w: *Atomic(bool) = @ptrCast(@alignCast(ptr));
            w.store(true, .release);
        }
    };

    const Context = struct {
        select_ctx: SelectContext,
        woken: Atomic(bool),
        winners: Atomic(usize),

        const Self = @This();

        fn worker(self: *Self, branch_id: usize) void {
            if (self.select_ctx.branchReady(branch_id)) {
                _ = self.winners.fetchAdd(1, .acq_rel);
            }
        }
    };

    const iterations = common.config.autoConfig().loom_iterations;
    var passed: usize = 0;

    for (0..iterations) |_| {
        // Initialize at final location, then set waker
        var ctx = Context{
            .select_ctx = SelectContext.init(4),
            .woken = Atomic(bool).init(false),
            .winners = Atomic(usize).init(0),
        };
        // Set waker AFTER ctx is at its final location
        ctx.select_ctx.setWaker(@ptrCast(&ctx.woken), TestWaker.wake);

        const thread_count = 4;
        var threads: [thread_count]std.Thread = undefined;
        var spawned: usize = 0;

        for (0..thread_count) |i| {
            threads[spawned] = std.Thread.spawn(.{}, Context.worker, .{ &ctx, i }) catch continue;
            spawned += 1;
        }

        for (threads[0..spawned]) |t| {
            t.join();
        }

        // Invariants:
        // 1. Exactly one winner
        // 2. Waker was called
        // 3. Context is complete
        const ok = ctx.winners.load(.acquire) == 1 and
            ctx.woken.load(.acquire) and
            ctx.select_ctx.isComplete() and
            ctx.select_ctx.getWinner() != null;

        if (ok) {
            passed += 1;
        }
    }

    try testing.expectEqual(iterations, passed);
}

test "SelectContext - BranchWaker functionality" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var woken = Atomic(bool).init(false);
        var ctx = SelectContext.init(2);

        const TestWaker = struct {
            fn wake(ptr: *anyopaque) void {
                const w: *Atomic(bool) = @ptrCast(@alignCast(ptr));
                w.store(true, .release);
            }
        };

        ctx.setWaker(@ptrCast(&woken), TestWaker.wake);

        // Create branch wakers
        var waker0 = ctx.branchWaker(0);
        var waker1 = ctx.branchWaker(1);

        // Neither is cancelled initially
        try testing.expect(!waker0.isCancelled());
        try testing.expect(!waker1.isCancelled());

        // Simulate channel calling branch 0's waker
        const waker_fn = waker0.wakerFn();
        waker_fn(waker0.wakerCtx());

        // Branch 0 should win
        try testing.expect(waker0.isWinner());
        try testing.expect(!waker1.isWinner());
        try testing.expect(!waker0.isCancelled());
        try testing.expect(waker1.isCancelled());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Selector Tests with Real Channels
// ─────────────────────────────────────────────────────────────────────────────

test "Selector - trySelect finds ready channel" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var ch1 = try Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();
        var ch2 = try Channel(u32).init(testing.allocator, 10);
        defer ch2.deinit();

        // Add data to ch2 only
        _ = ch2.trySend(42);

        var sel = Selector(2).init();
        _ = sel.addRecv(u32, &ch1);
        _ = sel.addRecv(u32, &ch2);

        const result = sel.trySelect();
        try testing.expect(result != null);
        try testing.expectEqual(@as(usize, 1), result.?.branch);
    }
}

test "Selector - selectBlocking with immediately ready channel" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var ch1 = try Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();
        var ch2 = try Channel(u32).init(testing.allocator, 10);
        defer ch2.deinit();

        // Add data to ch1
        _ = ch1.trySend(100);

        var sel = Selector(2).init();
        _ = sel.addRecv(u32, &ch1);
        _ = sel.addRecv(u32, &ch2);

        const result = sel.selectBlocking();
        try testing.expectEqual(@as(usize, 0), result.branch);
        try testing.expect(!result.closed);
    }
}

test "Selector - detects closed channel" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var ch1 = try Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();

        ch1.close();

        var sel = Selector(1).init();
        _ = sel.addRecv(u32, &ch1);

        const result = sel.trySelect();
        try testing.expect(result != null);
        try testing.expect(result.?.closed);
    }
}

test "Selector - multiple channels race to ready" {
    const Context = struct {
        ch1: *Channel(u32),
        ch2: *Channel(u32),
        results: Atomic(usize),

        const Self = @This();

        fn sender1(self: *Self) void {
            _ = self.ch1.trySend(1);
        }

        fn sender2(self: *Self) void {
            _ = self.ch2.trySend(2);
        }
    };

    const iterations = common.config.autoConfig().loom_iterations;
    var valid_results: usize = 0;

    for (0..iterations) |_| {
        var ch1 = try Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();
        var ch2 = try Channel(u32).init(testing.allocator, 10);
        defer ch2.deinit();

        var ctx = Context{
            .ch1 = &ch1,
            .ch2 = &ch2,
            .results = Atomic(usize).init(0),
        };

        // Spawn two threads to send to different channels
        const t1 = std.Thread.spawn(.{}, Context.sender1, .{&ctx}) catch continue;
        const t2 = std.Thread.spawn(.{}, Context.sender2, .{&ctx}) catch {
            t1.join();
            continue;
        };

        t1.join();
        t2.join();

        // Now select - should find one of them
        var sel = Selector(2).init();
        _ = sel.addRecv(u32, &ch1);
        _ = sel.addRecv(u32, &ch2);

        const result = sel.trySelect();
        if (result != null and (result.?.branch == 0 or result.?.branch == 1)) {
            valid_results += 1;
        }
    }

    try testing.expectEqual(iterations, valid_results);
}

test "Selector - reset and reuse" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var ch1 = try Channel(u32).init(testing.allocator, 10);
        defer ch1.deinit();

        var sel = Selector(2).init();

        // First use
        _ = sel.addRecv(u32, &ch1);
        try testing.expectEqual(@as(usize, 1), sel.num_branches);

        // Reset
        sel.reset();
        try testing.expectEqual(@as(usize, 0), sel.num_branches);

        // Reuse
        _ = sel.addRecv(u32, &ch1);
        try testing.expectEqual(@as(usize, 1), sel.num_branches);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge Cases
// ─────────────────────────────────────────────────────────────────────────────

test "SelectContext - reset after completion" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var ctx = SelectContext.init(2);

        // Complete
        _ = ctx.branchReady(0);
        try testing.expect(ctx.isComplete());

        // Reset
        ctx.reset();
        try testing.expect(!ctx.isComplete());
        try testing.expectEqual(@as(?usize, null), ctx.getWinner());

        // Can use again
        try testing.expect(ctx.branchReady(1));
        try testing.expectEqual(@as(?usize, 1), ctx.getWinner());
    }
}

test "CancelToken - branch 0 can win" {
    const iterations = common.config.autoConfig().loom_iterations;

    for (0..iterations) |_| {
        var token = CancelToken.init();

        // Branch 0 should be able to win (test encoding of branch_id + 1)
        try testing.expect(token.tryClaimWinner(0));
        try testing.expectEqual(@as(?usize, 0), token.getWinner());
        try testing.expect(!token.isCancelledFor(0));
        try testing.expect(token.isCancelledFor(1));
    }
}

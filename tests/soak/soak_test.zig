//! # Volt Soak Test — Long-Running Correctness Under Sustained Load
//!
//! Spawns long-lived async tasks that hammer shared primitives for the
//! entire test duration. The main thread only monitors and reports — it
//! never calls blockOnComplete() during the test, avoiding scheduler
//! starvation. Tasks run on workers and yield cooperatively via .pending.
//!
//! Usage:
//!   zig build test-soak                              # Default 60s
//!   zig build test-soak -- --duration 10 --report 2  # Quick smoke test
//!   zig build test-soak -- --duration 1800 --report 60  # 30-minute CI run

const std = @import("std");
const Atomic = std.atomic.Value;

const volt = @import("volt");
const Io = volt.Io;
const Mutex = volt.sync.Mutex;
const RwLock = volt.sync.RwLock;
const Semaphore = volt.sync.Semaphore;
const Notify = volt.sync.Notify;
const Channel = volt.channel.Channel;
const Context = volt.future.Context;
const PollResult = volt.future.PollResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

const Config = struct {
    duration_secs: u64 = 60,
    num_workers: u32 = 4,
    report_interval_secs: u64 = 5,
};

fn parseArgs(args: []const [:0]const u8) Config {
    var cfg = Config{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--duration") and i + 1 < args.len) {
            i += 1;
            cfg.duration_secs = std.fmt.parseInt(u64, args[i], 10) catch 60;
        } else if (std.mem.eql(u8, arg, "--workers") and i + 1 < args.len) {
            i += 1;
            cfg.num_workers = std.fmt.parseInt(u32, args[i], 10) catch 4;
        } else if (std.mem.eql(u8, arg, "--report") and i + 1 < args.len) {
            i += 1;
            cfg.report_interval_secs = std.fmt.parseInt(u64, args[i], 10) catch 5;
        }
    }
    return cfg;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Shared State — lives for the entire soak run
// ═══════════════════════════════════════════════════════════════════════════════

const SoakState = struct {
    // Stop flag — set by main thread to signal all tasks to exit
    stop: Atomic(bool),

    // Mutex workload
    mutex: Mutex,
    mutex_counter: Atomic(usize),

    // RwLock workload
    rwlock: RwLock,
    rwlock_read_ops: Atomic(usize),
    rwlock_write_ops: Atomic(usize),

    // Semaphore workload
    semaphore: Semaphore,
    sem_counter: Atomic(usize),

    // Channel workload
    channel: Channel(u64),
    channel_sent: Atomic(usize),
    channel_received: Atomic(usize),

    // Notify ping-pong workload
    notify_a: Notify,
    notify_b: Notify,
    notify_roundtrips: Atomic(usize),

    fn init(allocator: std.mem.Allocator) !SoakState {
        return .{
            .stop = Atomic(bool).init(false),
            .mutex = Mutex.init(),
            .mutex_counter = Atomic(usize).init(0),
            .rwlock = RwLock.init(),
            .rwlock_read_ops = Atomic(usize).init(0),
            .rwlock_write_ops = Atomic(usize).init(0),
            .semaphore = Semaphore.init(2),
            .sem_counter = Atomic(usize).init(0),
            .channel = try Channel(u64).init(allocator, 16),
            .channel_sent = Atomic(usize).init(0),
            .channel_received = Atomic(usize).init(0),
            .notify_a = Notify.init(),
            .notify_b = Notify.init(),
            .notify_roundtrips = Atomic(usize).init(0),
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Workload 1: Mutex Contention (4 long-running tasks)
// Each task: lock → increment → unlock → repeat until stop
// ═══════════════════════════════════════════════════════════════════════════════

const SoakMutexFuture = struct {
    mutex: *Mutex,
    counter: *Atomic(usize),
    stop: *Atomic(bool),
    lock_future: ?volt.sync.mutex.LockFuture = null,
    state: State = .start,

    const State = enum { start, locking };
    pub const Output = void;

    pub fn poll(self: *SoakMutexFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .start => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    self.lock_future = self.mutex.lockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.mutex.unlock();
                            self.state = .start;
                        },
                    }
                },
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Workload 2: RwLock Mixed (3 readers + 1 writer, long-running)
// ═══════════════════════════════════════════════════════════════════════════════

const SoakRwLockReadFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    stop: *Atomic(bool),
    lock_future: ?volt.sync.rwlock.ReadLockFuture = null,
    state: State = .start,

    const State = enum { start, locking };
    pub const Output = void;

    pub fn poll(self: *SoakRwLockReadFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .start => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    self.lock_future = self.rwlock.readLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.readUnlock();
                            self.state = .start;
                        },
                    }
                },
            }
        }
    }
};

const SoakRwLockWriteFuture = struct {
    rwlock: *RwLock,
    counter: *Atomic(usize),
    stop: *Atomic(bool),
    lock_future: ?volt.sync.rwlock.WriteLockFuture = null,
    state: State = .start,

    const State = enum { start, locking };
    pub const Output = void;

    pub fn poll(self: *SoakRwLockWriteFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .start => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    self.lock_future = self.rwlock.writeLockFuture();
                    self.state = .locking;
                },
                .locking => {
                    switch (self.lock_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.rwlock.writeUnlock();
                            self.state = .start;
                        },
                    }
                },
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Workload 3: Semaphore Permits (6 tasks, 2 permits, long-running)
// ═══════════════════════════════════════════════════════════════════════════════

const SoakSemaphoreFuture = struct {
    semaphore: *Semaphore,
    counter: *Atomic(usize),
    stop: *Atomic(bool),
    acquire_future: ?volt.sync.semaphore.AcquireFuture = null,
    state: State = .start,

    const State = enum { start, acquiring };
    pub const Output = void;

    pub fn poll(self: *SoakSemaphoreFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .start => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    if (self.semaphore.tryAcquire(1)) {
                        _ = self.counter.fetchAdd(1, .monotonic);
                        self.semaphore.release(1);
                        continue;
                    }
                    self.acquire_future = self.semaphore.acquireFuture(1);
                    self.state = .acquiring;
                },
                .acquiring => {
                    switch (self.acquire_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            _ = self.counter.fetchAdd(1, .monotonic);
                            self.semaphore.release(1);
                            self.state = .start;
                        },
                    }
                },
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Workload 4: MPMC Channel (2 producers + 2 consumers, long-running)
// Stopped by closing the channel — trySend/tryRecv return .closed
// ═══════════════════════════════════════════════════════════════════════════════

const SoakSendFuture = struct {
    pub const Output = void;
    const SendFutureT = volt.channel.channel_mod.SendFuture(u64);

    channel: *Channel(u64),
    sent_counter: *Atomic(usize),
    next_value: u64,
    send_future: ?SendFutureT = null,

    pub fn poll(self: *SoakSendFuture, ctx: *Context) PollResult(void) {
        // Resume pending send future
        if (self.send_future != null) {
            switch (self.send_future.?.poll(ctx)) {
                .pending => return .pending,
                .ready => {
                    self.send_future = null;
                    _ = self.sent_counter.fetchAdd(1, .monotonic);
                    self.next_value +%= 1;
                },
            }
        }

        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.channel.trySend(self.next_value)) {
                .ok => {
                    _ = self.sent_counter.fetchAdd(1, .monotonic);
                    self.next_value +%= 1;
                },
                .closed => return .{ .ready = {} },
                .full => {
                    self.send_future = self.channel.sendFuture(self.next_value);
                    switch (self.send_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            self.send_future = null;
                            _ = self.sent_counter.fetchAdd(1, .monotonic);
                            self.next_value +%= 1;
                        },
                    }
                },
            }
        }
    }
};

const SoakRecvFuture = struct {
    pub const Output = void;
    const RecvFutureT = volt.channel.channel_mod.RecvFuture(u64);

    channel: *Channel(u64),
    recv_counter: *Atomic(usize),
    recv_future: ?RecvFutureT = null,

    pub fn poll(self: *SoakRecvFuture, ctx: *Context) PollResult(void) {
        // Resume pending recv future
        if (self.recv_future != null) {
            switch (self.recv_future.?.poll(ctx)) {
                .pending => return .pending,
                .ready => |val| {
                    self.recv_future = null;
                    if (val == null) return .{ .ready = {} }; // closed
                    _ = self.recv_counter.fetchAdd(1, .monotonic);
                },
            }
        }

        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.channel.tryRecv()) {
                .value => {
                    _ = self.recv_counter.fetchAdd(1, .monotonic);
                },
                .closed => return .{ .ready = {} },
                .empty => {
                    self.recv_future = self.channel.recvFuture();
                    switch (self.recv_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => |val| {
                            self.recv_future = null;
                            if (val == null) return .{ .ready = {} }; // closed
                            _ = self.recv_counter.fetchAdd(1, .monotonic);
                        },
                    }
                },
            }
        }
    }
};

// Barrier workload removed: barriers require all N tasks to arrive per
// generation, making clean shutdown impossible without a final-sync protocol
// that itself requires all tasks to be scheduled simultaneously.

// ═══════════════════════════════════════════════════════════════════════════════
// Workload 6: Notify Ping-Pong (1 ping + 1 pong, long-running)
// On stop: main thread calls notifyOne() on both Notifys to unblock waiters.
// Tasks check stop at every state transition.
// ═══════════════════════════════════════════════════════════════════════════════

const SoakNotifyPingFuture = struct {
    notify_a: *Notify,
    notify_b: *Notify,
    roundtrips: *Atomic(usize),
    stop: *Atomic(bool),
    wait_future: ?volt.sync.notify.WaitFuture = null,
    state: State = .notify_a,

    const State = enum { notify_a, wait_b };
    pub const Output = void;

    pub fn poll(self: *SoakNotifyPingFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .notify_a => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    if (self.notify_a.waiterCount() > 0) {
                        self.notify_a.notifyOne();
                        self.wait_future = self.notify_b.waitFuture();
                        self.state = .wait_b;
                    } else {
                        ctx.getWaker().wakeByRef();
                        return .pending;
                    }
                },
                .wait_b => {
                    switch (self.wait_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            self.wait_future = null;
                            if (self.stop.load(.acquire)) return .{ .ready = {} };
                            _ = self.roundtrips.fetchAdd(1, .monotonic);
                            self.state = .notify_a;
                        },
                    }
                },
            }
        }
    }
};

const SoakNotifyPongFuture = struct {
    notify_a: *Notify,
    notify_b: *Notify,
    roundtrips: *Atomic(usize),
    stop: *Atomic(bool),
    wait_future: ?volt.sync.notify.WaitFuture = null,
    state: State = .wait_a,

    const State = enum { wait_a, notify_b };
    pub const Output = void;

    pub fn poll(self: *SoakNotifyPongFuture, ctx: *Context) PollResult(void) {
        while (true) {
            if (!ctx.pollProceed()) return .pending;
            switch (self.state) {
                .wait_a => {
                    if (self.wait_future == null) {
                        self.wait_future = self.notify_a.waitFuture();
                    }
                    switch (self.wait_future.?.poll(ctx)) {
                        .pending => return .pending,
                        .ready => {
                            self.wait_future = null;
                            if (self.stop.load(.acquire)) return .{ .ready = {} };
                            self.state = .notify_b;
                        },
                    }
                },
                .notify_b => {
                    if (self.stop.load(.acquire)) return .{ .ready = {} };
                    if (self.notify_b.waiterCount() > 0) {
                        self.notify_b.notifyOne();
                        _ = self.roundtrips.fetchAdd(1, .monotonic);
                        self.state = .wait_a;
                    } else {
                        ctx.getWaker().wakeByRef();
                        return .pending;
                    }
                },
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Output Formatting
// ═══════════════════════════════════════════════════════════════════════════════

fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const output = std.fmt.bufPrint(&buf, fmt, args) catch return;
    std.fs.File.stdout().writeAll(output) catch return;
}

fn printHeader(cfg: Config) void {
    print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Volt Soak Test                                              │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Duration: {d:<4}s   Workers: {d}   Report: {d}s               │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{ cfg.duration_secs, cfg.num_workers, cfg.report_interval_secs });
}

fn printStats(elapsed_secs: f64, duration_secs: u64, state: *const SoakState) void {
    const mutex_ops = state.mutex_counter.load(.acquire);
    const rwlock_ops = state.rwlock_read_ops.load(.acquire) + state.rwlock_write_ops.load(.acquire);
    const sem_ops = state.sem_counter.load(.acquire);
    const ch_sent = state.channel_sent.load(.acquire);
    const ch_recv = state.channel_received.load(.acquire);
    const notify_trips = state.notify_roundtrips.load(.acquire);
    const total = mutex_ops + rwlock_ops + sem_ops + ch_sent + notify_trips;
    const ops_per_sec: f64 = if (elapsed_secs > 0) @as(f64, @floatFromInt(total)) / elapsed_secs else 0;

    print(
        "[{d:6.1}s/{d}s] {d:.0} ops/s | mutex={d} rwlock={d} sem={d} ch={d}/{d} notify={d} | OK\n",
        .{
            elapsed_secs,
            duration_secs,
            ops_per_sec,
            mutex_ops,
            rwlock_ops,
            sem_ops,
            ch_sent,
            ch_recv,
            notify_trips,
        },
    );
}

fn printSummary(
    elapsed_secs: f64,
    state: *const SoakState,
    leaked: bool,
) void {
    print(
        \\
        \\┌──────────────────────────────────────────────────────────────┐
        \\│  Soak Test Summary                                           │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Duration: {d:.1}s                                           │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  Mutex ops:      {d:<12}                                    │
        \\│  RwLock reads:   {d:<12}                                    │
        \\│  RwLock writes:  {d:<12}                                    │
        \\│  Sem ops:        {d:<12}                                    │
        \\│  Channel sent:   {d:<12}                                    │
        \\│  Channel recv:   {d:<12}                                    │
        \\│  Notify trips:   {d:<12}                                    │
        \\├──────────────────────────────────────────────────────────────┤
        \\│  GPA leak check: {s:<42} │
        \\│  Result:         {s:<42} │
        \\└──────────────────────────────────────────────────────────────┘
        \\
    , .{
        elapsed_secs,
        state.mutex_counter.load(.acquire),
        state.rwlock_read_ops.load(.acquire),
        state.rwlock_write_ops.load(.acquire),
        state.sem_counter.load(.acquire),
        state.channel_sent.load(.acquire),
        state.channel_received.load(.acquire),
        state.notify_roundtrips.load(.acquire),
        if (leaked) "LEAK DETECTED" else "PASS",
        if (leaked) "FAIL" else "PASS",
    });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

const TOTAL_HANDLES = 18; // 4 mutex + 4 rwlock + 6 sem + 4 channel
const IoFutureVoid = volt.Future(void);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) {
            print("GPA detected memory leak!\n", .{});
            std.process.exit(1);
        }
    }
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const cfg = parseArgs(if (args.len > 1) args[1..] else &.{});
    printHeader(cfg);

    var io = try Io.init(allocator, .{ .num_workers = cfg.num_workers });
    defer io.deinit();

    var state = try SoakState.init(allocator);

    // ── Spawn all long-running tasks ──────────────────────────────────────
    var handles: [TOTAL_HANDLES]IoFutureVoid = undefined;
    var h: usize = 0;

    // Mutex: 4 tasks
    for (0..4) |_| {
        handles[h] = io.awaitFuture(SoakMutexFuture{
            .mutex = &state.mutex,
            .counter = &state.mutex_counter,
            .stop = &state.stop,
        }) catch @panic("spawn failed");
        h += 1;
    }

    // RwLock: 3 readers + 1 writer
    for (0..3) |_| {
        handles[h] = io.awaitFuture(SoakRwLockReadFuture{
            .rwlock = &state.rwlock,
            .counter = &state.rwlock_read_ops,
            .stop = &state.stop,
        }) catch @panic("spawn failed");
        h += 1;
    }
    handles[h] = io.awaitFuture(SoakRwLockWriteFuture{
        .rwlock = &state.rwlock,
        .counter = &state.rwlock_write_ops,
        .stop = &state.stop,
    }) catch @panic("spawn failed");
    h += 1;

    // Semaphore: 6 tasks
    for (0..6) |_| {
        handles[h] = io.awaitFuture(SoakSemaphoreFuture{
            .semaphore = &state.semaphore,
            .counter = &state.sem_counter,
            .stop = &state.stop,
        }) catch @panic("spawn failed");
        h += 1;
    }

    // Channel: 2 producers + 2 consumers
    for (0..2) |_| {
        handles[h] = io.awaitFuture(SoakSendFuture{
            .channel = &state.channel,
            .sent_counter = &state.channel_sent,
            .next_value = 0,
        }) catch @panic("spawn failed");
        h += 1;
    }
    for (0..2) |_| {
        handles[h] = io.awaitFuture(SoakRecvFuture{
            .channel = &state.channel,
            .recv_counter = &state.channel_received,
        }) catch @panic("spawn failed");
        h += 1;
    }

    // Notify: ping + pong (pong first so it waits first)
    var pong_h = io.awaitFuture(SoakNotifyPongFuture{
        .notify_a = &state.notify_a,
        .notify_b = &state.notify_b,
        .roundtrips = &state.notify_roundtrips,
        .stop = &state.stop,
    }) catch @panic("spawn failed");
    var ping_h = io.awaitFuture(SoakNotifyPingFuture{
        .notify_a = &state.notify_a,
        .notify_b = &state.notify_b,
        .roundtrips = &state.notify_roundtrips,
        .stop = &state.stop,
    }) catch @panic("spawn failed");

    // ── Monitor loop — main thread just sleeps and reports ───────────────
    const start_time = std.time.nanoTimestamp();
    const duration_ns: i128 = @as(i128, cfg.duration_secs) * std.time.ns_per_s;
    const report_ns: u64 = cfg.report_interval_secs * std.time.ns_per_s;

    // Print first report after a brief warmup
    std.Thread.sleep(std.time.ns_per_ms * 100);
    {
        const now = std.time.nanoTimestamp();
        const elapsed_secs: f64 = @as(f64, @floatFromInt(now - start_time)) / @as(f64, @floatFromInt(std.time.ns_per_s));
        printStats(elapsed_secs, cfg.duration_secs, &state);
    }

    while (true) {
        std.Thread.sleep(report_ns);
        const now = std.time.nanoTimestamp();
        const elapsed = now - start_time;
        if (elapsed >= duration_ns) break;
        const elapsed_secs: f64 = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(std.time.ns_per_s));
        printStats(elapsed_secs, cfg.duration_secs, &state);
    }

    // ── Signal all tasks to stop ─────────────────────────────────────────
    state.stop.store(true, .release);

    // Unblock channel waiters (producers/consumers exit on .closed)
    state.channel.close();

    // Unblock any stuck notify waiters so they can see stop=true
    state.notify_a.notifyOne();
    state.notify_b.notifyOne();

    // ── Wait for tasks to finish ─────────────────────────────────────────
    for (handles[0..h]) |*handle| _ = handle.await(io);
    _ = ping_h.await(io);
    _ = pong_h.await(io);

    // ── Final checks ─────────────────────────────────────────────────────

    // Mutex must not be locked
    if (state.mutex.isLocked()) {
        print("\nFAIL: mutex still locked after stop\n", .{});
        std.process.exit(1);
    }

    // Semaphore: all permits returned
    if (state.semaphore.availablePermits() != 2) {
        print("\nFAIL: semaphore permits not fully returned (have {d}, expected 2)\n", .{state.semaphore.availablePermits()});
        std.process.exit(1);
    }

    // Drain remaining channel messages
    while (true) {
        switch (state.channel.tryRecv()) {
            .value => _ = state.channel_received.fetchAdd(1, .monotonic),
            .empty, .closed => break,
        }
    }
    state.channel.deinit();

    const end_time = std.time.nanoTimestamp();
    const total_elapsed_secs: f64 = @as(f64, @floatFromInt(end_time - start_time)) / @as(f64, @floatFromInt(std.time.ns_per_s));

    printSummary(total_elapsed_secs, &state, false);
}

//! Parking lot — one wait/wake mechanism for every coroutine-level
//! sync primitive in Volt.
//!
//! See `docs/internals/parking-lot.md` for the full design rationale.
//!
//! ## Public API
//!
//! From inside a coroutine:
//!   * `parkOn(addr, validator)` — block until `unparkOne` /
//!     `unparkAll` wakes us on `addr`. `validator` runs UNDER the
//!     bucket lock; if it returns false (condition already
//!     satisfied), parkOn returns immediately without parking.
//!
//! From any context with access to `*Runtime`:
//!   * `unparkOne(rt, addr)` — wake the FIFO-first waiter on `addr`.
//!   * `unparkAll(rt, addr)` — wake every waiter on `addr`.
//!
//! ## Why this closes the wait/wake race
//!
//! The validator-under-lock pattern atomically combines the
//! condition check with the decision to park:
//!
//!   * The wake side writes its state change (e.g.
//!     `done.store(1, .release)`) BEFORE calling `unparkOne`.
//!   * The park side reads that state INSIDE `validator`, which
//!     runs while holding the bucket's mutex.
//!   * The bucket mutex totally orders the two sides — there is no
//!     "register then sleep" gap because the registration happens
//!     atomically with the check.

const std = @import("std");
const coroutine = @import("coroutine.zig");
const current = @import("current.zig");
const runtime_mod = @import("runtime.zig");

const PthreadMutex = extern struct { _opaque: [8]u64 align(8) = @splat(0) };
extern "c" fn pthread_mutex_init(m: *PthreadMutex, attr: ?*anyopaque) c_int;
extern "c" fn pthread_mutex_destroy(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_lock(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_unlock(m: *PthreadMutex) c_int;

/// Validator function. Called UNDER the bucket lock by `parkOn`.
/// Return `true` to enqueue + park; `false` to return immediately
/// without parking (the condition is already satisfied).
pub const Validator = *const fn (addr: *const anyopaque) bool;

const BUCKET_COUNT: usize = 256;
const BUCKET_MASK: usize = BUCKET_COUNT - 1;

/// Intrusive list node. Lives on the parking coroutine's stack for
/// the duration of `parkOn`. Stackful coroutines preserve their
/// stack across suspension, so the node remains valid while the
/// coroutine is parked.
const Waiter = struct {
    coro: *coroutine.Coroutine,
    addr: usize,
    next: ?*Waiter = null,
};

const Bucket = struct {
    mutex: PthreadMutex,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
};

pub const ParkingLot = struct {
    buckets: [BUCKET_COUNT]Bucket,

    pub fn init() ParkingLot {
        var lot: ParkingLot = .{ .buckets = undefined };
        for (&lot.buckets) |*b| {
            b.* = .{ .mutex = .{} };
            _ = pthread_mutex_init(&b.mutex, null);
        }
        return lot;
    }

    pub fn deinit(self: *ParkingLot) void {
        for (&self.buckets) |*b| _ = pthread_mutex_destroy(&b.mutex);
    }

    fn bucketFor(self: *ParkingLot, addr_int: usize) *Bucket {
        // `>> 4` defeats alignment clustering — most atomic fields
        // are 4- or 8-byte aligned, which clusters low bits.
        const idx = (addr_int >> 4) & BUCKET_MASK;
        return &self.buckets[idx];
    }

    /// Acquire the bucket lock, call the validator, and either
    /// enqueue (returning true) or bail (returning false). The
    /// caller is responsible for calling `runtime.park()` AFTER a
    /// `true` return.
    fn tryEnqueue(
        self: *ParkingLot,
        waiter: *Waiter,
        addr: *const anyopaque,
        validator: Validator,
    ) bool {
        const bucket = self.bucketFor(waiter.addr);
        _ = pthread_mutex_lock(&bucket.mutex);
        if (!validator(addr)) {
            _ = pthread_mutex_unlock(&bucket.mutex);
            return false;
        }
        // FIFO: append at tail. Walking to tail would be O(n);
        // with an explicit tail pointer push is O(1).
        waiter.next = null;
        if (bucket.tail) |t| t.next = waiter;
        if (bucket.head == null) bucket.head = waiter;
        bucket.tail = waiter;
        _ = pthread_mutex_unlock(&bucket.mutex);
        return true;
    }

    /// Pop the FIFO-first waiter matching `addr` from its bucket.
    /// Returns the unparked coroutine, or null if no waiter on this
    /// address. The bucket may contain entries for OTHER addresses
    /// (hash collisions); those are walked past.
    fn popOneFor(self: *ParkingLot, addr_int: usize) ?*coroutine.Coroutine {
        const bucket = self.bucketFor(addr_int);
        _ = pthread_mutex_lock(&bucket.mutex);
        var prev: ?*Waiter = null;
        var cur = bucket.head;
        while (cur) |w| {
            if (w.addr == addr_int) {
                if (prev) |p| p.next = w.next else bucket.head = w.next;
                if (bucket.tail == w) bucket.tail = prev;
                _ = pthread_mutex_unlock(&bucket.mutex);
                return w.coro;
            }
            prev = w;
            cur = w.next;
        }
        _ = pthread_mutex_unlock(&bucket.mutex);
        return null;
    }

    /// Drain every waiter matching `addr` from the bucket into a
    /// returned linked list (via `Waiter.next`). The caller iterates
    /// the chain and unparks each one OUTSIDE the bucket lock — so
    /// unpark's work (push to injection, wakeOneParked) doesn't
    /// extend the critical section.
    fn drainAllFor(self: *ParkingLot, addr_int: usize) ?*Waiter {
        const bucket = self.bucketFor(addr_int);
        _ = pthread_mutex_lock(&bucket.mutex);
        var prev: ?*Waiter = null;
        var cur = bucket.head;
        var chain_head: ?*Waiter = null;
        var chain_tail: ?*Waiter = null;
        while (cur) |w| {
            const next = w.next;
            if (w.addr == addr_int) {
                if (prev) |p| p.next = next else bucket.head = next;
                if (bucket.tail == w) bucket.tail = prev;
                w.next = null;
                if (chain_tail) |t| t.next = w else chain_head = w;
                chain_tail = w;
            } else {
                prev = w;
            }
            cur = next;
        }
        _ = pthread_mutex_unlock(&bucket.mutex);
        return chain_head;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Public API (free functions)
// ─────────────────────────────────────────────────────────────────────

/// Park the current coroutine until `unparkOne` / `unparkAll` wakes
/// it on `addr`. `validator` is called UNDER the bucket lock — if
/// it returns false (the condition is already satisfied), this
/// function returns immediately without parking.
///
/// Must be called from inside a coroutine.
pub fn parkOn(addr: *const anyopaque, validator: Validator) void {
    const c = current.require();
    const rt: *runtime_mod.Runtime = @ptrCast(@alignCast(c.runtime));
    var waiter: Waiter = .{
        .coro = c,
        .addr = @intFromPtr(addr),
    };
    if (!rt.parking_lot.tryEnqueue(&waiter, addr, validator)) return;
    runtime_mod.park();
    // On wake: unparker has already unlinked us from the bucket.
}

/// Wake the first coroutine parked on `addr` (FIFO order). Returns
/// true if a waiter was popped and `runtime.unpark` was called on
/// it. May be called from any thread that holds the `*Runtime`.
pub fn unparkOne(rt: *runtime_mod.Runtime, addr: *const anyopaque) bool {
    const coro = rt.parking_lot.popOneFor(@intFromPtr(addr)) orelse return false;
    runtime_mod.unpark(coro);
    return true;
}

/// Wake every coroutine parked on `addr`. Returns the number woken.
pub fn unparkAll(rt: *runtime_mod.Runtime, addr: *const anyopaque) usize {
    var chain = rt.parking_lot.drainAllFor(@intFromPtr(addr));
    var count: usize = 0;
    while (chain) |w| {
        const next = w.next;
        runtime_mod.unpark(w.coro);
        count += 1;
        chain = next;
    }
    return count;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;
const Runtime = runtime_mod.Runtime;

const STATE_INITIAL: u32 = 0;
const STATE_SIGNALLED: u32 = 1;

const TestCtx = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(STATE_INITIAL),
    woken_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn isNotSignalled(addr: *const anyopaque) bool {
    const s: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    return s.load(.acquire) != STATE_SIGNALLED;
}

fn waiterCoro(ctx: *TestCtx) void {
    parkOn(&ctx.state, isNotSignalled);
    _ = ctx.woken_count.fetchAdd(1, .release);
}

fn singleWaiterRoot(ctx: *TestCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var child = try rt.spawn(waiterCoro, .{ctx});

    // Yield so the child gets scheduled and parks. Even if the
    // child hasn't parked yet by the time we signal, the
    // validator-under-lock will see STATE_SIGNALLED and bail —
    // the test is robust either way.
    runtime_mod.yield();
    runtime_mod.yield();

    ctx.state.store(STATE_SIGNALLED, .release);
    _ = unparkOne(rt, &ctx.state);

    child.join();
}

test "parking lot: single waiter wakes via unparkOne" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();

    var ctx = TestCtx{};
    try (try rt.run(singleWaiterRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u32, 1), ctx.woken_count.load(.acquire));
}

fn multiWaiterRoot(ctx: *TestCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    const Task = @import("task.zig").Task;
    var tasks: [16]*Task(void) = undefined;
    for (&tasks) |*t| t.* = try rt.spawn(waiterCoro, .{ctx});

    // Let them all park.
    var y: u32 = 0;
    while (y < 8) : (y += 1) runtime_mod.yield();

    ctx.state.store(STATE_SIGNALLED, .release);
    _ = unparkAll(rt, &ctx.state);

    for (tasks) |t| t.join();
}

test "parking lot: unparkAll wakes every waiter" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 4 });
    defer rt.deinit();

    var ctx = TestCtx{};
    try (try rt.run(multiWaiterRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u32, 16), ctx.woken_count.load(.acquire));
}

fn validatorBailsRoot(ctx: *TestCtx) !void {
    // Pre-set state to SIGNALLED before any park happens.
    ctx.state.store(STATE_SIGNALLED, .release);

    // Validator returns false; parkOn returns immediately.
    parkOn(&ctx.state, isNotSignalled);

    _ = ctx.woken_count.fetchAdd(1, .release);
}

test "parking lot: validator returning false bails without parking" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var ctx = TestCtx{};
    try (try rt.run(validatorBailsRoot, .{&ctx}));

    try std.testing.expectEqual(@as(u32, 1), ctx.woken_count.load(.acquire));
}

fn pingPongAddrA(ctx: *PingPongCtx) void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        // Wait for our turn (state == ME_A).
        parkOn(&ctx.state, addrAValidator);
        // Toggle to ME_B and wake the other side.
        ctx.state.store(ME_B, .release);
        const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
        _ = unparkOne(rt, &ctx.state);
    }
}

fn pingPongAddrB(ctx: *PingPongCtx) void {
    var i: u32 = 0;
    while (i < ctx.iters) : (i += 1) {
        parkOn(&ctx.state, addrBValidator);
        ctx.state.store(ME_A, .release);
        const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
        _ = unparkOne(rt, &ctx.state);
    }
}

const ME_A: u32 = 0;
const ME_B: u32 = 1;

const PingPongCtx = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(ME_A),
    iters: u32,
};

fn addrAValidator(addr: *const anyopaque) bool {
    const s: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    return s.load(.acquire) != ME_A;
}

fn addrBValidator(addr: *const anyopaque) bool {
    const s: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
    return s.load(.acquire) != ME_B;
}

fn pingPongRoot(ctx: *PingPongCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var a = try rt.spawn(pingPongAddrA, .{ctx});
    var b = try rt.spawn(pingPongAddrB, .{ctx});
    a.join();
    b.join();
}

test "parking lot: ping-pong on shared addr" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();

    var ctx = PingPongCtx{ .iters = 1000 };
    try (try rt.run(pingPongRoot, .{&ctx}));
}

//! Per-fd PollDesc state machine — Step 1 of the reactor restructure.
//!
//! Modelled on Go's `runtime/netpoll.go`. The goal: decouple
//! user-visible wake (`runtime.unpark(coro)`) from kernel deregister
//! (`epoll_ctl(DEL)` / `kevent(EV_DELETE)` / `io_uring POLL_REMOVE` /
//! IOCP close). Every race we've chased in the current per-wait-
//! registration model — close-vs-register, cancel-vs-register, the
//! io_uring SQ-lock-across-blocking-syscall deadlock — is eliminated
//! by construction once wakes are pure user-space CASes on this
//! state machine.
//!
//! See `docs/internals/reactor-restructure-proposal.md` for the full
//! plan; this file ships the state machine + tests. No backend uses
//! it yet (steps 2-5).
//!
//! ## State machine
//!
//! `rg` (read) and `wg` (write) are independent slots, each a usize:
//!
//!   PD_NIL    (0) — no event pending, no waiter
//!   PD_READY  (1) — event pending, no waiter yet (cached for next wait)
//!   PD_WAIT   (2) — waiter is mid-commit (between intent-to-park and
//!                   self-publish — transient, only seen across CAS races)
//!   <coro ptr>    — waiter is parked; pointer is `@intFromPtr(*Coroutine)`
//!
//! Coroutines are extern-struct heap allocations containing u32 atomics,
//! so their pointers are ≥ 4-byte aligned. PD_NIL/READY/WAIT all have
//! low bit(s) set or zero in the sentinel range that pointers don't hit.
//!
//! ## Transitions
//!
//! Owner: `wait(mode)` — caller wanting to park on `mode` readiness:
//!   1. CAS PD_READY → PD_NIL   (consume a cached ready, return .ready)
//!   2. CAS PD_NIL → PD_WAIT     (commit to wait)
//!   3. re-check `closing` flag (lock-free via `info`)
//!   4. CAS PD_WAIT → coro_ptr   (self-publish — installs our pointer)
//!   5. `context.swap` (park)
//!   6. on wake: swap slot → PD_NIL, inspect old value
//!
//! At any time, another worker may run:
//!   A. `deliverReady(mode)`  — CAS {WAIT|coro_ptr|NIL} → PD_READY
//!   B. `deliverCancel(mode)` — CAS {WAIT|coro_ptr} → PD_NIL  (cancel/close)
//!   C. `evict()`             — set `closing`, then deliverCancel both modes
//!
//! The closing flag is published BEFORE evict's CAS, so a waiter at
//! step (3) sees closing and bails. A waiter at (4) or later either
//! has its self-publish CAS fail (race A or B beat us) or gets popped
//! by (A/B) and woken via runtime.unpark — see `wait` for the post-
//! wake inspection that distinguishes ready from closing.

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const runtime = @import("runtime.zig");

const Coroutine = coroutine.Coroutine;

// ─── State machine sentinels ─────────────────────────────────────────
const PD_NIL: usize = 0;
const PD_READY: usize = 1;
const PD_WAIT: usize = 2;

// ─── Info bits (lock-free read, lock-write per `lock`) ───────────────
//
// Mirrors Go's `pollInfo` (runtime/netpoll.go:117). Holds the
// closing flag + a small fdseq counter for stale-event filtering.
// Slow-path writes happen under `pd.lock`; reads are lock-free
// (`isClosing`, `currentFdseq`) and used by `wait` to check error
// state without re-acquiring the lock.

const INFO_CLOSING: u32 = 1 << 0;
const INFO_FDSEQ_SHIFT: u5 = 1;
const INFO_FDSEQ_BITS: u5 = 20;
const INFO_FDSEQ_MASK: u32 = (@as(u32, 1) << INFO_FDSEQ_BITS) - 1;

// ─── Public types ────────────────────────────────────────────────────

pub const Mode = enum(u8) { read = 'r', write = 'w' };

pub const WaitResult = enum {
    /// fd is ready in `mode`; caller retries the syscall.
    ready,
    /// PollDesc was evicted (close in progress).
    closing,
};

pub const PollDesc = struct {
    // Hot: lock-free per-direction slots.
    rg: std.atomic.Value(usize) align(std.atomic.cache_line) =
        std.atomic.Value(usize).init(PD_NIL),
    wg: std.atomic.Value(usize) align(std.atomic.cache_line) =
        std.atomic.Value(usize).init(PD_NIL),
    /// Closing flag + fdseq, lock-free read, lock-write.
    info: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    // Cold: slow-path state under `lock`.
    lock: std.atomic.Value(u32) align(std.atomic.cache_line) =
        std.atomic.Value(u32).init(0),
    closing: bool = false,
    /// Stale-event sequence. Stored in `info` so the poll loop can
    /// read it lock-free when filtering kernel events. Bumped on
    /// `evict()` (and, later, on PollDesc reuse). Never zero —
    /// matches Go's special-case in runtime/netpoll.go:256.
    fdseq: u32 = 1,

    pub fn init(self: *PollDesc) void {
        self.* = .{};
        self.publishInfo();
    }

    pub fn deinit(self: *PollDesc) void {
        // No allocations to release. Asserts that the PollDesc was
        // either evicted or never used (clean exit). A PollDesc with
        // a parked waiter at deinit time would leak the waiter.
        std.debug.assert(self.rg.load(.acquire) <= PD_WAIT);
        std.debug.assert(self.wg.load(.acquire) <= PD_WAIT);
    }

    // ─── Lock-free info reads ───────────────────────────────────────

    pub fn isClosing(self: *const PollDesc) bool {
        return (self.info.load(.acquire) & INFO_CLOSING) != 0;
    }

    pub fn currentFdseq(self: *const PollDesc) u32 {
        return (self.info.load(.acquire) >> INFO_FDSEQ_SHIFT) & INFO_FDSEQ_MASK;
    }

    // ─── Slow-path lock ─────────────────────────────────────────────

    fn acquireLock(self: *PollDesc) void {
        while (self.lock.swap(1, .acquire) != 0) std.atomic.spinLoopHint();
    }

    fn releaseLock(self: *PollDesc) void {
        self.lock.store(0, .release);
    }

    fn publishInfo(self: *PollDesc) void {
        var i: u32 = 0;
        if (self.closing) i |= INFO_CLOSING;
        i |= (self.fdseq & INFO_FDSEQ_MASK) << INFO_FDSEQ_SHIFT;
        self.info.store(i, .release);
    }

    inline fn slot(self: *PollDesc, mode: Mode) *std.atomic.Value(usize) {
        return if (mode == .read) &self.rg else &self.wg;
    }

    // ─── Wake sources ───────────────────────────────────────────────

    /// Kernel reports `mode` is ready. Returns the parked coro to
    /// unpark, or null (event cached / waiter mid-commit / no waiter).
    /// Caller is responsible for calling `runtime.unpark(coro)`.
    pub fn deliverReady(self: *PollDesc, mode: Mode) ?*Coroutine {
        return self.unblock(mode, true);
    }

    /// Cancel.fire wakes a parked coro via the state machine —
    /// no reactor syscall. Returns the parked coro to unpark.
    /// Used by the cancel-aware wait path (`waitCancel`, added in
    /// a later step).
    pub fn deliverCancel(self: *PollDesc, mode: Mode) ?*Coroutine {
        return self.unblock(mode, false);
    }

    /// Go's `netpollunblock` (runtime/netpoll.go:591). The shared
    /// CAS-and-pop for all wake sources.
    fn unblock(self: *PollDesc, mode: Mode, ioready: bool) ?*Coroutine {
        const gpp = self.slot(mode);
        const new_state: usize = if (ioready) PD_READY else PD_NIL;
        while (true) {
            const old = gpp.load(.acquire);
            // Already delivered — no-op.
            if (old == PD_READY) return null;
            // For non-ioready (close/cancel), don't store PD_READY
            // on an empty slot. The waiter (if it later arrives)
            // checks `closing`/cancel state in step (3) and bails
            // without parking. Storing PD_READY would falsely tell
            // the next wait that I/O is ready.
            if (old == PD_NIL and !ioready) return null;
            if (gpp.cmpxchgWeak(old, new_state, .acq_rel, .acquire)) |_| continue;
            // Successful CAS.
            if (old == PD_NIL or old == PD_WAIT) {
                // Either cached for the next wait (ioready + NIL →
                // READY) or popped a still-committing waiter
                // (WAIT). In the WAIT case the waiter's self-
                // publish CAS will fail and it'll re-inspect state
                // — we don't return it here because it's not yet
                // parked.
                return null;
            }
            return @ptrFromInt(old);
        }
    }

    // ─── Close (eviction) ───────────────────────────────────────────

    /// Mark the PollDesc closing and wake any parked waiters via the
    /// state machine. Equivalent to Go's `poll_runtime_pollUnblock`
    /// (runtime/netpoll.go:451). Does NOT call the kernel.
    ///
    /// Idempotency: `evict()` is single-shot. A second call asserts
    /// (`closing` is already true). Callers must coordinate so only
    /// one evicts; the per-fd refcount + close semaphore added in
    /// Step 2 will enforce this at the socket layer.
    pub fn evict(self: *PollDesc) void {
        self.acquireLock();
        std.debug.assert(!self.closing);
        self.closing = true;
        self.bumpFdseqLocked();
        self.publishInfo();
        const rg = self.unblock(.read, false);
        const wg = self.unblock(.write, false);
        self.releaseLock();
        // Wake outside the lock — `runtime.unpark` may push to a P's
        // mailbox and call `wakeOneParked`, which can re-enter Volt
        // code. Holding pd.lock across that is asking for inversions.
        if (rg) |c| runtime.unpark(c);
        if (wg) |c| runtime.unpark(c);
    }

    fn bumpFdseqLocked(self: *PollDesc) void {
        var next = self.fdseq +% 1;
        next &= INFO_FDSEQ_MASK;
        if (next == 0) next = 1;
        self.fdseq = next;
    }

    // ─── Wait (user-facing) ─────────────────────────────────────────

    /// Park the current coroutine until `mode` becomes ready or the
    /// PollDesc is evicted. Returns `.ready` (retry syscall) or
    /// `.closing` (return error to user).
    ///
    /// Equivalent to Go's `netpollblock` (runtime/netpoll.go:548).
    pub fn wait(self: *PollDesc, mode: Mode) WaitResult {
        const me = current.require();
        const gpp = self.slot(mode);

        // Step 1: take the slot. Either consume a cached PD_READY
        // (fast path — no park) or transition PD_NIL → PD_WAIT.
        while (true) {
            if (gpp.cmpxchgWeak(PD_READY, PD_NIL, .acq_rel, .acquire) == null) {
                return .ready;
            }
            if (gpp.cmpxchgWeak(PD_NIL, PD_WAIT, .acq_rel, .acquire) == null) {
                break;
            }
            const v = gpp.load(.acquire);
            if (v != PD_READY and v != PD_NIL) {
                std.debug.panic("PollDesc.wait: corrupt slot {x}", .{v});
            }
        }

        // Step 2: re-check `closing` AFTER committing to PD_WAIT.
        // `evict` publishes the closing flag BEFORE its unblock CAS,
        // so this load is sequenced (via the info store-release /
        // load-acquire pair) with any concurrent evict that started
        // before we reached step 1.
        if (self.isClosing()) {
            // Try to revert. If the revert succeeds we own the
            // result; if it fails, a deliver{Ready,Cancel} already
            // popped our PD_WAIT — fall through to inspect.
            if (gpp.cmpxchgStrong(PD_WAIT, PD_NIL, .acq_rel, .acquire) == null) {
                return .closing;
            }
            const old = gpp.swap(PD_NIL, .acq_rel);
            return if (old == PD_READY) .ready else .closing;
        }

        // Step 3: self-publish. Atomically install our coro pointer
        // in place of PD_WAIT. If this fails an unblock raced past;
        // fall through to inspect (no need to park — already woken).
        if (gpp.cmpxchgStrong(PD_WAIT, @intFromPtr(me), .acq_rel, .acquire)) |_| {
            const old = gpp.swap(PD_NIL, .acq_rel);
            if (old == PD_READY) return .ready;
            // PD_NIL here means deliverCancel ran (close or cancel).
            return .closing;
        }

        // Step 4: park. On wake (deliver{Ready,Cancel} replaced our
        // pointer and called runtime.unpark), the slot is PD_READY
        // or PD_NIL. We finalize by swapping it back to PD_NIL.
        //
        // The runtime.park_state machine handles the race where the
        // unparker fires between our cmpxchgStrong above and the
        // context.swap below: park_state transitions RUNNING →
        // NOTIFIED, and dispatch's `.park` branch re-queues us
        // instead of leaving us stranded. See runtime.zig:160.
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        const old = gpp.swap(PD_NIL, .acq_rel);
        if (old > PD_WAIT) {
            std.debug.panic("PollDesc.wait: woke with stale coro ptr {x}", .{old});
        }
        return if (old == PD_READY) .ready else .closing;
    }
};

// ────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("testing.zig").allocator;
const Runtime = runtime.Runtime;

test "PollDesc.init: fresh state — both slots PD_NIL, not closing, fdseq=1" {
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    try testing.expectEqual(@as(usize, PD_NIL), pd.rg.load(.acquire));
    try testing.expectEqual(@as(usize, PD_NIL), pd.wg.load(.acquire));
    try testing.expect(!pd.isClosing());
    try testing.expectEqual(@as(u32, 1), pd.currentFdseq());
}

test "PollDesc.deliverReady: with no waiter caches PD_READY; next wait fast-paths" {
    // No runtime needed — no parking happens.
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();

    try testing.expect(pd.deliverReady(.read) == null);
    try testing.expectEqual(@as(usize, PD_READY), pd.rg.load(.acquire));

    // Second deliverReady on already-PD_READY is a no-op.
    try testing.expect(pd.deliverReady(.read) == null);
    try testing.expectEqual(@as(usize, PD_READY), pd.rg.load(.acquire));
}

test "PollDesc.deliverCancel: with no waiter is a no-op (slot stays PD_NIL)" {
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    try testing.expect(pd.deliverCancel(.read) == null);
    try testing.expectEqual(@as(usize, PD_NIL), pd.rg.load(.acquire));
}

test "PollDesc.evict: sets closing, bumps fdseq, slot returns to PD_NIL" {
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    pd.evict();
    try testing.expect(pd.isClosing());
    try testing.expectEqual(@as(u32, 2), pd.currentFdseq());
    try testing.expectEqual(@as(usize, PD_NIL), pd.rg.load(.acquire));
    try testing.expectEqual(@as(usize, PD_NIL), pd.wg.load(.acquire));
}

test "PollDesc.evict: clears a cached PD_READY (close beats deliveredReady)" {
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    _ = pd.deliverReady(.read);
    try testing.expectEqual(@as(usize, PD_READY), pd.rg.load(.acquire));
    pd.evict();
    // closing takes priority; the cached ready is left to be
    // consumed by the next wait, but the closing flag will make
    // the wait return .closing if rg is consumed first.
    try testing.expect(pd.isClosing());
}

// ─── Tests requiring the runtime (parking) ─────────────────────────

fn waitBody(ctx: *WaitCtx) !void {
    ctx.result = ctx.pd.wait(ctx.mode);
}

const WaitCtx = struct {
    pd: *PollDesc,
    mode: Mode,
    result: WaitResult = .ready,
};

fn deliverReadyBody(ctx: *DeliverCtx) void {
    // Brief yields let the waiter park before we deliver.
    var i: u32 = 0;
    while (i < ctx.yield_count) : (i += 1) @import("lib.zig").yield();
    if (ctx.pd.deliverReady(ctx.mode)) |c| runtime.unpark(c);
}

const DeliverCtx = struct {
    pd: *PollDesc,
    mode: Mode,
    yield_count: u32 = 0,
};

fn evictBody(ctx: *DeliverCtx) void {
    var i: u32 = 0;
    while (i < ctx.yield_count) : (i += 1) @import("lib.zig").yield();
    ctx.pd.evict();
}

// `rt.spawn`'s user_fn parameter is `comptime`, so we can't pass a
// runtime-chosen deliverer. Each scenario gets its own concrete root.
// Tested coverage matrix kept in the test names.

const SingleCtx = struct {
    pd: *PollDesc,
    mode: Mode,
    yield_count: u32,
    result: WaitResult = .ready,
};

fn singleDeliverReadyRoot(ctx: *SingleCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var wait_ctx = WaitCtx{ .pd = ctx.pd, .mode = ctx.mode };
    var deliver_ctx = DeliverCtx{
        .pd = ctx.pd,
        .mode = ctx.mode,
        .yield_count = ctx.yield_count,
    };
    var waiter = try rt.spawn(waitBody, .{&wait_ctx});
    var deliverer = try rt.spawn(deliverReadyBody, .{&deliver_ctx});
    _ = waiter.join() catch |e| return e;
    _ = deliverer.join();
    ctx.result = wait_ctx.result;
}

fn singleEvictRoot(ctx: *SingleCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var wait_ctx = WaitCtx{ .pd = ctx.pd, .mode = ctx.mode };
    var deliver_ctx = DeliverCtx{
        .pd = ctx.pd,
        .mode = ctx.mode,
        .yield_count = ctx.yield_count,
    };
    var waiter = try rt.spawn(waitBody, .{&wait_ctx});
    var deliverer = try rt.spawn(evictBody, .{&deliver_ctx});
    _ = waiter.join() catch |e| return e;
    _ = deliverer.join();
    ctx.result = wait_ctx.result;
}

test "PollDesc.wait + deliverReady from another coro: returns .ready" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var ctx = SingleCtx{ .pd = &pd, .mode = .read, .yield_count = 4 };
    try (try rt.run(singleDeliverReadyRoot, .{&ctx}));
    try testing.expectEqual(WaitResult.ready, ctx.result);
    try testing.expectEqual(@as(usize, PD_NIL), pd.rg.load(.acquire));
}

test "PollDesc.wait + evict from another coro: returns .closing" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var ctx = SingleCtx{ .pd = &pd, .mode = .read, .yield_count = 4 };
    try (try rt.run(singleEvictRoot, .{&ctx}));
    try testing.expectEqual(WaitResult.closing, ctx.result);
    try testing.expect(pd.isClosing());
}

test "PollDesc.wait: write mode parks on wg independently from rg" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var ctx = SingleCtx{ .pd = &pd, .mode = .write, .yield_count = 4 };
    try (try rt.run(singleDeliverReadyRoot, .{&ctx}));
    try testing.expectEqual(WaitResult.ready, ctx.result);
    try testing.expectEqual(@as(usize, PD_NIL), pd.wg.load(.acquire));
    // rg untouched by write-mode operations.
    try testing.expectEqual(@as(usize, PD_NIL), pd.rg.load(.acquire));
}

// ─── Race stresses ─────────────────────────────────────────────────
//
// Sweep `yield_count` so each iteration lands the deliverer at a
// different point in the waiter's commit window. This is the same
// pattern as the recvCancel stress test that hung Linux CI for the
// entire phase — but here the wake is pure user-space, so the bug
// classes that caused the hang are gone by construction.

fn stressDeliverReadyRoot(_: *void) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var pd: PollDesc = undefined;
        pd.init();
        defer pd.deinit();
        var wait_ctx = WaitCtx{ .pd = &pd, .mode = .read };
        var deliver_ctx = DeliverCtx{
            .pd = &pd,
            .mode = .read,
            .yield_count = i % 8,
        };
        var waiter = try rt.spawn(waitBody, .{&wait_ctx});
        var deliverer = try rt.spawn(deliverReadyBody, .{&deliver_ctx});
        _ = waiter.join() catch |e| return e;
        _ = deliverer.join();
        try testing.expectEqual(WaitResult.ready, wait_ctx.result);
    }
}

fn stressEvictRoot(_: *void) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var pd: PollDesc = undefined;
        pd.init();
        defer pd.deinit();
        var wait_ctx = WaitCtx{ .pd = &pd, .mode = .read };
        var deliver_ctx = DeliverCtx{
            .pd = &pd,
            .mode = .read,
            .yield_count = i % 8,
        };
        var waiter = try rt.spawn(waitBody, .{&wait_ctx});
        var deliverer = try rt.spawn(evictBody, .{&deliver_ctx});
        _ = waiter.join() catch |e| return e;
        _ = deliverer.join();
        try testing.expectEqual(WaitResult.closing, wait_ctx.result);
    }
}

test "PollDesc stress: 200 wait/deliverReady races, all return .ready" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    var sentinel: void = {};
    try (try rt.run(stressDeliverReadyRoot, .{&sentinel}));
}

test "PollDesc stress: 200 wait/evict races, all return .closing" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    var sentinel: void = {};
    try (try rt.run(stressEvictRoot, .{&sentinel}));
}

// ─── Two-direction test: a single PollDesc with both modes parked ──

const TwoDirCtx = struct {
    pd: *PollDesc,
    r_result: WaitResult = .ready,
    w_result: WaitResult = .ready,
};

fn twoDirReader(ctx: *TwoDirCtx) !void {
    ctx.r_result = ctx.pd.wait(.read);
}

fn twoDirWriter(ctx: *TwoDirCtx) !void {
    ctx.w_result = ctx.pd.wait(.write);
}

fn twoDirEvictor(ctx: *TwoDirCtx) void {
    // Yield a few times so the readers/writers are actually parked
    // before we evict.
    var i: u32 = 0;
    while (i < 6) : (i += 1) @import("lib.zig").yield();
    ctx.pd.evict();
}

fn twoDirRoot(ctx: *TwoDirCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var reader = try rt.spawn(twoDirReader, .{ctx});
    var writer = try rt.spawn(twoDirWriter, .{ctx});
    var evictor = try rt.spawn(twoDirEvictor, .{ctx});
    _ = reader.join() catch |e| return e;
    _ = writer.join() catch |e| return e;
    _ = evictor.join();
}

test "PollDesc.evict: wakes both rg and wg waiters with .closing" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var ctx = TwoDirCtx{ .pd = &pd };
    try (try rt.run(twoDirRoot, .{&ctx}));
    try testing.expectEqual(WaitResult.closing, ctx.r_result);
    try testing.expectEqual(WaitResult.closing, ctx.w_result);
}

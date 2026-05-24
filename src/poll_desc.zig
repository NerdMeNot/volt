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
const cancel_mod = @import("cancel.zig");

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

    /// Reference count for users of this PollDesc (parked waiters +
    /// in-flight syscalls). Starts at 1 (the owner). Modelled on
    /// Go's `internal/poll.fdMutex`. `evict()` callers wait on
    /// `awaitClosed()` until this hits zero before freeing the
    /// PollDesc — that's how Go avoids use-after-free when a parked
    /// waiter is mid-post-wake-cleanup at close time.
    refcount: std.atomic.Value(u32) = std.atomic.Value(u32).init(1),

    /// Coroutine waiting in `awaitClosed`. Set under `lock` so the
    /// drop-to-zero path observes it consistently. At most one
    /// closer per PollDesc (single-owner invariant).
    close_waiter: ?*Coroutine = null,

    pub fn init(self: *PollDesc) void {
        self.* = .{};
        self.publishInfo();
    }

    pub fn deinit(self: *PollDesc) void {
        // No allocations to release. Asserts that the PollDesc was
        // either evicted+drained or never used (clean exit). A
        // PollDesc with a parked waiter or live refs at deinit time
        // would leak the waiter / UAF on the next wake.
        std.debug.assert(self.rg.load(.acquire) <= PD_WAIT);
        std.debug.assert(self.wg.load(.acquire) <= PD_WAIT);
        std.debug.assert(self.refcount.load(.acquire) <= 1);
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

    // ─── Refcount (close coordination) ──────────────────────────────
    //
    // Refcount tracks "users that may still dereference this
    // PollDesc": the owner (initial 1) plus any in-flight wait /
    // waitCancel that has incref'd. `closeAndWait` is the lifecycle
    // primitive — it drops the owner ref and parks until the count
    // hits zero, at which point the caller may safely free the
    // PollDesc.
    //
    // Modelled on Go's `internal/poll.fdMutex` close semaphore
    // (internal/poll/fd_mutex.go:120-176), without the poisoning
    // bit — Volt's wait()/waitCancel() see `closing` via the info
    // word and bail with `.closing`, so we don't need fdmu's
    // "fail new ops once closing" gate at the refcount layer.

    pub fn incref(self: *PollDesc) void {
        const prev = self.refcount.fetchAdd(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    /// Drop a ref. If this was the last ref and a `closeAndWait` is
    /// parked, wake it. Safe to call from any worker.
    pub fn decref(self: *PollDesc) void {
        const prev = self.refcount.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev != 1) return;
        // We took the count to zero. Pick up close_waiter under the
        // lock (close_waiter is written under the lock).
        self.acquireLock();
        const w = self.close_waiter;
        self.close_waiter = null;
        self.releaseLock();
        if (w) |c| runtime.unpark(c);
    }

    /// Mark the PollDesc closing, wake any parked waiters, drop the
    /// owner's ref, and block until any in-flight wait/waitCancel
    /// callers have dropped theirs. After return the refcount is
    /// zero and it is safe to free the PollDesc.
    ///
    /// Single-shot: at most one closer per PollDesc.
    pub fn closeAndWait(self: *PollDesc) void {
        if (!self.isClosing()) self.evict();
        const me = current.require();
        // Register ourselves as the close_waiter BEFORE dropping
        // the owner ref. The owner ref keeps refcount > 0, so no
        // concurrent decref can drive it to zero between "we
        // registered" and "we drop our ref" — only our own decref
        // below can be the last one.
        self.acquireLock();
        std.debug.assert(self.close_waiter == null);
        self.close_waiter = me;
        self.releaseLock();
        // Park-then-drop. Setting pending=.park first means that if
        // our own decref takes us to zero and synchronously calls
        // runtime.unpark(me), park_state goes RUNNING→NOTIFIED and
        // dispatch re-queues us instead of stranding the coro. The
        // dispatch.park branch handles this exactly (see
        // docs/src/content/docs/internals/memory-model.md).
        me.pending = .park;
        self.decref();
        context.swap(&me.ctx, me.main_ctx);
        std.debug.assert(self.refcount.load(.acquire) == 0);
        std.debug.assert(self.close_waiter == null);
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
        if (old == PD_READY) return .ready;
        if (old == PD_NIL) return .closing;
        // `waitCancel` wakes us via `runtime.unpark` directly,
        // without manipulating the slot — the slot still holds our
        // own coro_ptr when we resume. Treat as `.closing` so
        // `waitCancel` can translate to `Cancelled` on its post-
        // wake `isFired` check. Any OTHER coro pointer would be
        // memory corruption.
        if (old == @intFromPtr(me)) return .closing;
        std.debug.panic("PollDesc.wait: woke with stale coro ptr {x}", .{old});
    }

    /// Cancel-aware variant of `wait`. Returns `error.Cancelled` if
    /// `c` fires before the wait resolves; otherwise behaves exactly
    /// like `wait(mode)`.
    ///
    /// Caller MUST guarantee `self` outlives this call (typically by
    /// holding a refcount themselves — see `incref` / `closeAndWait`
    /// for the socket-level model). `waitCancel` takes one extra ref
    /// internally to keep `self` alive across any out-of-lock callback
    /// dispatch by `c.fire`.
    pub fn waitCancel(
        self: *PollDesc,
        mode: Mode,
        c: *cancel_mod.Cancel,
    ) cancel_mod.Error!WaitResult {
        if (c.isFired()) return error.Cancelled;

        // The callback (if dispatched) unparks `me` directly and drops
        // the ref we take below. We unpark the specific coro (rather
        // than going through `deliverCancel(mode)`) because the slot
        // may be in any state when fire races wait() — including
        // PD_NIL if fire wins the race to step 1. `deliverCancel` on
        // PD_NIL is a no-op and would lose the wake; `unpark` always
        // works thanks to `park_state`'s NOTIFIED-before-park path.
        // wait()'s post-wake epilogue treats `old == @intFromPtr(me)`
        // as `.closing`, completing the round-trip.
        self.incref();
        const me = current.require();
        var ctx = WaitCancelCtx{ .pd = self, .coro = me };
        var w: cancel_mod.Waiter = .{};
        const fired = c.registerCallback(&w, &ctx, waitCancelDeliver);
        if (fired) {
            // Register saw the fired flag after our isFired check —
            // no callback will dispatch; the ref is ours to drop.
            self.decref();
            return error.Cancelled;
        }

        const result = self.wait(mode);

        // Race resolution. Either we unlink before fire snapshots,
        // or fire snapshotted us and the callback is in-flight /
        // about-to-run. If the callback is in-flight we MUST wait
        // for it to complete before `ctx` (on our stack) goes out
        // of scope. The completed flag bounds that wait to one
        // unpark + decref + store; on the hot path (no cancel) the
        // busy-wait branch never executes.
        if (c.deregisterRemoved(&w)) {
            self.decref();
        } else {
            while (!ctx.completed.load(.acquire)) std.atomic.spinLoopHint();
        }

        // wait() may have returned .ready via a kernel-driven wake
        // (no cancel involvement); only translate to Cancelled if
        // the cancel actually fired.
        if (c.isFired()) return error.Cancelled;
        return result;
    }
};

// ─── Shim-era placeholder ────────────────────────────────────────────
//
// The Step 2b PollDesc-aware reactor methods (`waitFd`, `waitFdCancel`)
// ignore their `pd` argument — they delegate to the existing per-wait
// `waitReadable` / `waitWritable` path. Callers that don't yet hold a
// real per-fd PollDesc pass `&shim_dummy` to satisfy the signature.
//
// Once all backends migrate to real PollDesc-based dispatch (Steps
// 2d–2g) and net.zig allocates a PollDesc per socket, this dummy
// disappears along with the shim paths. Until then it's harmless
// shared state — the shims do not read or mutate it.
pub var shim_dummy: PollDesc align(std.atomic.cache_line) = .{};

// ─── waitCancel deliver callback ─────────────────────────────────────
//
// One static fn that cancel.zig dispatches as `*const fn (*anyopaque)
// void`. Interprets `ctx` as `*WaitCancelCtx` (lives on the calling
// coro's stack — kept alive by waitCancel's busy-wait on `completed`
// when the deregister race with fire goes our way).
//
// The callback does the minimum: unpark the parked coro and drop the
// extra ref `waitCancel` took at register time. Slot manipulation is
// deliberately left to wait()'s normal epilogue (which handles `old
// == my_coro_ptr` as `.closing`); attempting `deliverCancel(mode)`
// here would race with wait()'s early steps and lose the wake when
// the slot happens to be PD_NIL.

const WaitCancelCtx = struct {
    pd: *PollDesc,
    coro: *Coroutine,
    completed: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),
};

fn waitCancelDeliver(raw: *anyopaque) void {
    const ctx: *WaitCancelCtx = @ptrCast(@alignCast(raw));
    // Order: unpark first (touches the parked coro), then decref
    // (may wake a closeAndWait parker that frees pd), then publish
    // completion. After `completed` is observed, ctx may be freed.
    runtime.unpark(ctx.coro);
    ctx.pd.decref();
    ctx.completed.store(true, .release);
}

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

// ─── Refcount + closeAndWait ────────────────────────────────────────

const Cancel = cancel_mod.Cancel;

test "PollDesc.incref/decref: count tracks balanced calls" {
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
    pd.incref();
    pd.incref();
    try testing.expectEqual(@as(u32, 3), pd.refcount.load(.acquire));
    pd.decref();
    pd.decref();
    try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
}

fn closeOnlyRoot(pd: *PollDesc) !void {
    pd.closeAndWait();
}

test "PollDesc.closeAndWait: no in-flight refs returns immediately" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 1 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    try (try rt.run(closeOnlyRoot, .{&pd}));
    try testing.expect(pd.isClosing());
    try testing.expectEqual(@as(u32, 0), pd.refcount.load(.acquire));
}

const RefcountedWaitCtx = struct {
    pd: *PollDesc,
    /// Set true AFTER incref so the closer can wait for the
    /// in-flight ref to be visible before draining the owner ref.
    /// Without this, a slow scheduler can let the closer drain
    /// refcount to zero before the waiter even runs — then the
    /// waiter's incref hits the prev>0 assert and panics. (Hit
    /// once on macos-latest CI; reliably reproducible only under
    /// the CI runner's scheduling.) Real production callers
    /// (net.zig) will use an atomic refcount-or-closed transition;
    /// the test just needs deterministic ordering.
    ready: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),
    result: WaitResult = .ready,
};

fn refcountedWaitBody(ctx: *RefcountedWaitCtx) void {
    ctx.pd.incref();
    ctx.ready.store(true, .release);
    defer ctx.pd.decref();
    ctx.result = ctx.pd.wait(.read);
}

fn closeAndWaitRoot(pd: *PollDesc) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var wait_ctx = RefcountedWaitCtx{ .pd = pd };
    var waiter = try rt.spawn(refcountedWaitBody, .{&wait_ctx});
    while (!wait_ctx.ready.load(.acquire)) @import("lib.zig").yield();
    // After the ready signal, yield a few more times so the waiter
    // reaches wait()'s park before we evict — exercises the
    // "evict wakes a parked waiter, then closeAndWait drains" path
    // rather than the "evict beats the waiter to step 1" path.
    var i: u32 = 0;
    while (i < 6) : (i += 1) @import("lib.zig").yield();
    pd.closeAndWait();
    _ = waiter.join();
    try testing.expectEqual(WaitResult.closing, wait_ctx.result);
}

test "PollDesc.closeAndWait: blocks until in-flight wait drains" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    try (try rt.run(closeAndWaitRoot, .{&pd}));
    try testing.expectEqual(@as(u32, 0), pd.refcount.load(.acquire));
    try testing.expect(pd.isClosing());
}

// ─── waitCancel ─────────────────────────────────────────────────────

const TestWaitCancelCtx = struct {
    pd: *PollDesc,
    cancel: *Cancel,
    mode: Mode,
    yield_count: u32 = 0,
    err: ?anyerror = null,
    result: WaitResult = .ready,
};

fn waitCancelBody(ctx: *TestWaitCancelCtx) void {
    if (ctx.pd.waitCancel(ctx.mode, ctx.cancel)) |r| {
        ctx.result = r;
    } else |e| {
        ctx.err = e;
    }
}

fn fireBody(ctx: *TestWaitCancelCtx) void {
    var i: u32 = 0;
    while (i < ctx.yield_count) : (i += 1) @import("lib.zig").yield();
    ctx.cancel.fire();
}

fn deliverReadyAfterYields(ctx: *TestWaitCancelCtx) void {
    var i: u32 = 0;
    while (i < ctx.yield_count) : (i += 1) @import("lib.zig").yield();
    if (ctx.pd.deliverReady(ctx.mode)) |c| runtime.unpark(c);
}

fn waitCancelFireRoot(ctx: *TestWaitCancelCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var waiter = try rt.spawn(waitCancelBody, .{ctx});
    var firer = try rt.spawn(fireBody, .{ctx});
    _ = waiter.join();
    _ = firer.join();
}

test "PollDesc.waitCancel: cancel.fire wakes the waiter with Cancelled" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    var ctx = TestWaitCancelCtx{ .pd = &pd, .cancel = &c, .mode = .read, .yield_count = 4 };
    try (try rt.run(waitCancelFireRoot, .{&ctx}));
    try testing.expectEqual(@as(?anyerror, cancel_mod.Error.Cancelled), ctx.err);
    // After waitCancel returns, both refs (the implicit owner's and
    // any ref taken inside waitCancel) are accounted for.
    try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
}

fn waitCancelDeliverRoot(ctx: *TestWaitCancelCtx) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var waiter = try rt.spawn(waitCancelBody, .{ctx});
    var deliverer = try rt.spawn(deliverReadyAfterYields, .{ctx});
    _ = waiter.join();
    _ = deliverer.join();
}

test "PollDesc.waitCancel: deliverReady wins → returns .ready, no Cancelled" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    var ctx = TestWaitCancelCtx{ .pd = &pd, .cancel = &c, .mode = .read, .yield_count = 4 };
    try (try rt.run(waitCancelDeliverRoot, .{&ctx}));
    try testing.expectEqual(@as(?anyerror, null), ctx.err);
    try testing.expectEqual(WaitResult.ready, ctx.result);
    try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
}

const PreFireCtx = struct { pd: *PollDesc, c: *Cancel };

fn preFireBody(args: *PreFireCtx) !void {
    try testing.expectError(
        cancel_mod.Error.Cancelled,
        args.pd.waitCancel(.read, args.c),
    );
}

test "PollDesc.waitCancel: cancel pre-fired returns Cancelled without parking" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 1 });
    defer rt.deinit();
    var pd: PollDesc = undefined;
    pd.init();
    defer pd.deinit();
    var c = Cancel.init(rt);
    defer c.deinit();
    c.fire();
    var args = PreFireCtx{ .pd = &pd, .c = &c };
    try (try rt.run(preFireBody, .{&args}));
    try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
}

// ─── Stress: waitCancel vs fire races, vs deliverReady races ────────

fn waitCancelFireStressRoot(_: *void) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var pd: PollDesc = undefined;
        pd.init();
        defer pd.deinit();
        var c = Cancel.init(rt);
        defer c.deinit();
        var ctx = TestWaitCancelCtx{ .pd = &pd, .cancel = &c, .mode = .read, .yield_count = i % 8 };
        var waiter = try rt.spawn(waitCancelBody, .{&ctx});
        var firer = try rt.spawn(fireBody, .{&ctx});
        _ = waiter.join();
        _ = firer.join();
        try testing.expectEqual(@as(?anyerror, cancel_mod.Error.Cancelled), ctx.err);
        try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
    }
}

test "PollDesc stress: 200 waitCancel vs fire races, all Cancelled, no refcount leaks" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    var sentinel: void = {};
    try (try rt.run(waitCancelFireStressRoot, .{&sentinel}));
}

fn waitCancelDeliverStressRoot(_: *void) !void {
    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var pd: PollDesc = undefined;
        pd.init();
        defer pd.deinit();
        var c = Cancel.init(rt);
        defer c.deinit();
        var ctx = TestWaitCancelCtx{ .pd = &pd, .cancel = &c, .mode = .read, .yield_count = i % 8 };
        var waiter = try rt.spawn(waitCancelBody, .{&ctx});
        var deliverer = try rt.spawn(deliverReadyAfterYields, .{&ctx});
        _ = waiter.join();
        _ = deliverer.join();
        try testing.expectEqual(@as(?anyerror, null), ctx.err);
        try testing.expectEqual(WaitResult.ready, ctx.result);
        try testing.expectEqual(@as(u32, 1), pd.refcount.load(.acquire));
    }
}

test "PollDesc stress: 200 waitCancel vs deliverReady races, all .ready, no refcount leaks" {
    var rt = try Runtime.init(.{ .allocator = test_alloc, .workers = 4 });
    defer rt.deinit();
    var sentinel: void = {};
    try (try rt.run(waitCancelDeliverStressRoot, .{&sentinel}));
}

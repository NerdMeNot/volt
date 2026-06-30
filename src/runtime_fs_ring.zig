//! Per-P io_uring fs-ring scheduling glue (Linux). Extracted from
//! runtime.zig: ring init, eventfd wake wiring, the hot-path CQE
//! drainers called from the worker loop, and the fsRingX submit/
//! await helpers the reactor backends route file I/O through.
//!
//! Imports runtime.zig back for Runtime/unpark/park — the same
//! circular-import shape the reactor backends already use. Tests for
//! this code remain in runtime.zig.

const std = @import("std");
const builtin = @import("builtin");
const runtime = @import("runtime.zig");
const Runtime = runtime.Runtime;
const M = runtime.M;
const P = runtime.P;
const unpark = runtime.unpark;
const park = runtime.park;
const worker_mod = @import("worker.zig");
const current = @import("current.zig");
const fs_ring_mod = @import("fs/ring.zig");

// ─── Phase 2B: per-P io_uring (fs) ring initialisation ────────────

/// Try to populate `rt.fs_rings` with one `FsRing` per P. All-or-
/// nothing: any per-P init failure undoes the successful ones and
/// leaves `rt.fs_rings = null`. The Reactor's fs path treats null
/// as "use spawnBlocking proxy", so the fallback is automatic.
///
/// Caller must already have checked `builtin.os.tag == .linux`.
pub fn tryInitFsRings(rt: *Runtime, allocator: std.mem.Allocator) void {
    const rings = allocator.alloc(fs_ring_mod.FsRing, rt.ps.len) catch return;
    var inited: usize = 0;
    while (inited < rings.len) : (inited += 1) {
        rings[inited] = fs_ring_mod.FsRing.init() catch {
            // Roll back what we've built, then bail.
            var j: usize = 0;
            while (j < inited) : (j += 1) rings[j].deinit();
            allocator.free(rings);
            return;
        };
    }
    rt.fs_rings = rings;
}

// ─── Phase 2C.1: fs ring wake-up wiring ───────────────────────────

/// Module-private back-pointer used by `fsRingWakeHandler`. Set
/// once at `Runtime.init` time, never changes. The handler runs
/// on the reactor poller's thread and has no other way to reach
/// the Runtime without it. Single-runtime-per-process for now —
/// see `is_windows_rt` block below for the same assumption on the
/// signal infrastructure.
pub var fs_ring_wake_rt: ?*Runtime = null;

/// Phase 2C.1: register each ring's eventfd with the shared
/// reactor's epoll and install the wake handler. Called from
/// `Runtime.init` after `tryInitFsRings` succeeded.
///
/// On any registration failure, returns the error — the caller
/// (init) treats this as fatal-to-fs-rings (not fatal-to-init)
/// and tears down the rings, falling back to spawnBlocking.
pub fn wireFsRingWakeups(rt: *Runtime) !void {
    const rings = rt.fs_rings.?; // guaranteed non-null by caller
    // Bind the module back-pointer first so the handler is safe
    // to call the moment we install it.
    fs_ring_wake_rt = rt;
    for (rings, 0..) |*r, i| {
        try r.registerEventFd();
        try rt.reactor.addFsRingEventFd(r.eventfd, @intCast(i));
    }
    rt.reactor.setFsWakeHandler(&fsRingWakeHandler);
}

/// Reactor poller invokes this when a per-P fs ring's eventfd
/// fires. Implements the Signal-Only-If-Idle protocol from
/// `docs/internals/phase-2c-design.md §2`:
///   1. Drain the eventfd (8-byte read, mandatory for
///      level-triggered epoll).
///   2. Load `parked_workers.acquire`.
///   3. If owner's bit is set, unpark the owner M. Otherwise drop
///      — the owner is running or searching and will pick up the
///      CQE on its next find-work iteration.
fn fsRingWakeHandler(p_id: u32) void {
    const rt = fs_ring_wake_rt orelse return;
    const rings = rt.fs_rings orelse return;
    if (p_id >= rings.len) return;
    rings[p_id].drainEventFd();

    // Signal-Only-If-Idle: only wake the owner M if its bit is
    // set in parked_workers. Otherwise the owner is in find-work
    // (Running or Searching state) and will drain its own ring
    // via the Layer 1 peek on the next iteration.
    const bit: u64 = @as(u64, 1) << @intCast(p_id);
    const bm = rt.parked_workers.load(.acquire);
    if ((bm & bit) != 0) {
        rt.ms[p_id].parker.unpark();
    }
}

/// Phase 2C.1 Layer 1: drain ready CQEs on `p`'s fs ring, resolve
/// each via `user_data → *FsOp`, write the result, and unpark the
/// waiting coroutine. Returns count drained. Safe to call from
/// the owner worker only (owner-only-drain decision, design memo
/// §4).
///
/// Called from `tryFindAndDispatch` (between popMailbox-fairness
/// and popLocal) and from `parkWorker` (after `markParked` and
/// inside the spin loop). The unparked coroutines land in this
/// P's mailbox (via `unpark`'s currentM-aware target selection),
/// where the existing `mailbox.isEmpty()` defensive check picks
/// them up and aborts the park.
///
/// Uses `unpark` rather than direct `p.pushQueue` because the
/// coroutine may be in PARKED state already (CQE arrived after
/// the dispatcher's swap-back) or in RUNNING state (CQE arrived
/// before swap-back). `unpark`'s CAS handles both transitions
/// correctly; a bare push would race with the swap-back's
/// PARK_STATE transition and risk double-dispatch.
pub inline fn drainFsRingInto(rt: *Runtime, p: *P) usize {
    if (builtin.os.tag != .linux) return 0;
    // On Linux, the fast path (no fs ops in flight on this P) is
    // a single load + branch. Inlined so spawn-heavy workloads
    // (bench-spawn-hot, bench-fanout-scaling) don't pay a call
    // overhead on the find-work / parkWorker hot path.
    const rings = rt.fs_rings orelse return 0;
    if (p.id >= rings.len) return 0;
    var cqes: [16]@import("std").os.linux.io_uring_cqe = undefined;
    const n = rings[p.id].peekBatch(&cqes) catch return 0;
    for (cqes[0..n]) |cqe| {
        // Phase 3: cancel-ack CQE fast path. The cancel SQE's
        // user_data is the original FsOp ptr with bit 0 set;
        // the kernel echoes it back unchanged. No FsOp lookup,
        // no unpark — the coroutine is waiting on the ORIGINAL
        // op's CQE (handled below), not the cancel ack.
        if (fs_ring_mod.isCancelAckUserData(cqe.user_data)) {
            continue;
        }
        const op: *fs_ring_mod.FsOp = @ptrFromInt(cqe.user_data);
        const res: isize = cqe.res;
        op.result = .{
            .value = res,
            .err = if (res < 0) @intCast(-res) else 0,
        };
        // Phase 3 state machine: try Pending → Completed first
        // (the common path; no cancel involved). If that fails
        // the state must be Cancelling — transition to
        // CancelledAndDrained so the coroutine knows on wake to
        // return error.Cancelled. Both transitions write the
        // result and unpark; the coroutine's branch on wake
        // decides what to return.
        const cas = op.state.cmpxchgStrong(
            fs_ring_mod.STATE_PENDING,
            fs_ring_mod.STATE_COMPLETED,
            .acq_rel,
            .acquire,
        );
        if (cas) |observed| {
            // CAS failed — must be Cancelling. Drainer is the
            // single writer of this transition; no contention.
            std.debug.assert(observed == fs_ring_mod.STATE_CANCELLING);
            op.state.store(fs_ring_mod.STATE_CANCELLED_AND_DRAINED, .release);
        }
        unpark(op.coro);
    }
    if (n > 0) _ = rt.fs_in_flight.fetchSub(n, .acq_rel);
    return n;
}

// ─── Phase 2C.2: fs ring routing helpers ─────────────────────────
//
// Each helper attempts to route an fs op through the per-P
// io_uring ring. Returns non-null on success, null when the ring
// path is unavailable (no fs_rings on this Runtime, not on a
// worker M, SQ full) — caller falls back to the Phase 1
// spawnBlocking proxy. Callers must be in a coroutine (the
// spawnBlocking fallback also requires it).
//
// The FsOp is allocated on the caller's coroutine stack. The
// stack VA is stable for the coroutine's lifetime (CLAUDE.md
// invariant 5); the coroutine waits in `park()` for its CQE
// before returning, so the FsOp outlives the kernel's in-flight
// window. See `docs/internals/phase-2c-design.md §1`.
//
// **Error policy (Seastar `68cec26e`):** `flush()` failure is
// treated as panic-worthy state corruption — `io_uring_enter`
// only fails with EBADF/EINVAL/EOPNOTSUPP, all of which mean
// the ring is in a state we can't recover from. Returning null
// here instead would leave the orphan SQE (already written to
// the userspace ring with our user_data) referencing a stack-
// allocated `FsOp` that goes out of scope as soon as we fall
// back — a subsequent successful flush would then submit that
// orphan with a stale pointer, and the kernel would write into
// a dead frame. Panic forces the issue surface as a crash, not
// as memory corruption.
//
// `prepRead`/etc failures (SubmissionQueueFull) are different:
// no SQE was written, so a graceful return-null + spawnBlocking
// fallback is correct.

const fs_result = @import("reactor/fs.zig").FsResult;

inline fn currentPRing(rt: *Runtime) ?*fs_ring_mod.FsRing {
    const rings = rt.fs_rings orelse return null;
    const m_raw = worker_mod.currentM() orelse return null;
    const m: *M = @ptrCast(@alignCast(m_raw));
    if (m.p.id >= rings.len) return null;
    return &rings[m.p.id];
}

/// Submit the SQE prepared in `ring`. Panics on failure (see
/// error policy in the comment above this section). Bumps
/// `fs_in_flight` after a successful submit so the worker
/// poll/park machinery knows there's a kernel-side op pending.
inline fn flushRingOrPanic(rt: *Runtime, ring: *fs_ring_mod.FsRing) void {
    _ = ring.flush() catch |e| std.debug.panic(
        "fs_ring io_uring_enter failed: {s} — see Phase 2C.2 error policy in runtime.zig",
        .{@errorName(e)},
    );
    _ = rt.fs_in_flight.fetchAdd(1, .acq_rel);
    _ = rt.fs_ops_via_ring.fetchAdd(1, .monotonic);
}

/// Phase 6A: inline-completion fast path. After flush, peek the
/// CQ for CQEs that the kernel finished synchronously (cached
/// reads complete inline almost always). If our `my_op`'s CQE
/// is in the drained batch, write the result, decrement
/// `fs_in_flight`, and return true — caller can return without
/// parking, saving the ~1-2µs futex park+unpark round-trip.
///
/// Other CQEs in the same drain batch are dispatched normally
/// (CAS state + unpark) — same as the standard `drainFsRingInto`
/// path. This is important: `copy_cqes` ADVANCES the CQ head as
/// it copies, so we can't leave drained CQEs unprocessed without
/// losing them. We must handle the whole batch.
///
/// Skips `unpark(my_op.coro)` for the caller's own op (we're
/// returning to it inline, no wake needed). For my_op we also
/// skip the state CAS — the FsOp is going out of scope as soon
/// as the helper returns; no one else will ever look at state.
inline fn tryFsRingInlineComplete(
    rt: *Runtime,
    ring: *fs_ring_mod.FsRing,
    my_op: *fs_ring_mod.FsOp,
) bool {
    if (builtin.os.tag != .linux) return false;
    var cqes: [16]@import("std").os.linux.io_uring_cqe = undefined;
    const n = ring.peekBatch(&cqes) catch return false;
    if (n == 0) return false;
    var found_my_op = false;
    for (cqes[0..n]) |cqe| {
        if (fs_ring_mod.isCancelAckUserData(cqe.user_data)) continue;
        const op: *fs_ring_mod.FsOp = @ptrFromInt(cqe.user_data);
        const res: isize = cqe.res;
        op.result = .{
            .value = res,
            .err = if (res < 0) @intCast(-res) else 0,
        };
        if (op == my_op) {
            // Our op — caller will return directly with the
            // result. No state CAS, no unpark; FsOp's stack
            // frame ends as soon as fsRingX returns.
            found_my_op = true;
            continue;
        }
        // Other ops in the batch — full standard dispatch.
        const cas = op.state.cmpxchgStrong(
            fs_ring_mod.STATE_PENDING,
            fs_ring_mod.STATE_COMPLETED,
            .acq_rel,
            .acquire,
        );
        if (cas) |observed| {
            std.debug.assert(observed == fs_ring_mod.STATE_CANCELLING);
            op.state.store(fs_ring_mod.STATE_CANCELLED_AND_DRAINED, .release);
        }
        unpark(op.coro);
    }
    _ = rt.fs_in_flight.fetchSub(n, .acq_rel);
    return found_my_op;
}

// ─── Phase 3C: cancel-aware fs ring helpers ──────────────────────
//
// The four fsRingX helpers take an optional `cancel: ?*Cancel`
// final parameter. When null, behaviour is identical to Phase 2C
// (no register, no state branching — the drainer's CAS
// Pending→Completed succeeds on the first try, coro wakes, reads
// result). When non-null, the helper registers a cancel
// callback before submit and branches on `op.state` after wake
// per the state machine in `docs/internals/phase-3-design.md §1`.
//
// Cancelled result is encoded as `FsResult{.value=-1, .err=ECANCELED}`
// — the kernel's natural CQE shape for cancelled ops. The caller
// (fs/file.zig) maps the ECANCELED errno to `error.Cancelled` the
// same way it maps any other errno.

const cancel_mod = @import("cancel.zig");

/// Stack-allocated callback context for the cancel-deliver path.
/// Lives in the caller's `fsRingX` frame for the duration of the
/// in-flight op. The callback is invoked under `cancel.fire()`
/// from arbitrary thread; the busy-wait on `completed` (post-park)
/// keeps the ctx alive until the callback finishes touching it
/// — standard PollDesc.waitCancel pattern (poll_desc.zig:457-461).
const FsCancelCtx = struct {
    op: *fs_ring_mod.FsOp,
    completed: std.atomic.Value(bool) align(std.atomic.cache_line) =
        std.atomic.Value(bool).init(false),
};

fn fsCancelDeliver(raw: *anyopaque) void {
    const ctx: *FsCancelCtx = @ptrCast(@alignCast(raw));
    // CAS Pending → Cancelling. If it fails, state is already
    // Completed — the op finished before cancel could land,
    // race resolves in favour of completion (memo §2.3). No
    // unpark needed; the drainer already unparked the coro and
    // it'll observe state=Completed on wake.
    const observed = ctx.op.state.cmpxchgStrong(
        fs_ring_mod.STATE_PENDING,
        fs_ring_mod.STATE_CANCELLING,
        .acq_rel,
        .acquire,
    );
    if (observed == null) {
        // CAS won: state is now Cancelling. Wake the parked coro
        // — it'll see Cancelling and drive the ASYNC_CANCEL +
        // re-park flow. Standard `unpark` handles the case where
        // the coro hasn't actually called park() yet
        // (NOTIFIED-before-PARK race) via park_state.
        unpark(ctx.op.coro);
    }
    // Always publish completion so the caller's post-wake
    // busy-wait can drop ctx safely.
    ctx.completed.store(true, .release);
}

/// Build the Cancelled-encoded `FsResult` returned by the
/// helpers (and Reactor.fsX spawnBlocking-fallback path) when
/// the op was cancelled. Caller maps the ECANCELED errno to
/// `error.Cancelled`. Public so the Reactor backends can reuse
/// it on their fallback path (Phase 3D).
pub inline fn cancelledFsResult() fs_result {
    return .{
        .value = -1,
        .err = @intFromEnum(std.c.E.CANCELED),
    };
}

/// Shared post-submit machinery. The op's SQE is already in the
/// ring + flushed; `op` is the caller's stack-allocated FsOp.
/// Handles the optional cancel: register / park / branch on
/// state / submit cancel + re-park if needed / return.
fn fsRingAwait(
    rt: *Runtime,
    ring: *fs_ring_mod.FsRing,
    op: *fs_ring_mod.FsOp,
    cancel: ?*cancel_mod.Cancel,
) fs_result {
    // Cancel registration (no-op when cancel == null).
    var w: cancel_mod.Waiter = .{};
    var ctx: FsCancelCtx = .{ .op = op };
    var registered = false;
    if (cancel) |c| {
        const fired = c.registerCallback(&w, &ctx, fsCancelDeliver);
        if (fired) {
            // Cancel fired between caller's isFired pre-check and
            // here. The original SQE is already in flight; we
            // can't un-submit. Mark Cancelling so the drainer's
            // CAS observes it correctly when the CQE comes back,
            // then fall through to wait for the original CQE
            // (memory safety: we must drain the CQE before the
            // buffer can be freed).
            const observed = op.state.cmpxchgStrong(
                fs_ring_mod.STATE_PENDING,
                fs_ring_mod.STATE_CANCELLING,
                .acq_rel,
                .acquire,
            );
            _ = observed;
            // Submit ASYNC_CANCEL eagerly to nudge the kernel.
            const op_ud = @intFromPtr(op);
            ring.prepCancel(op_ud, fs_ring_mod.cancelUserDataFor(op_ud)) catch {};
            flushRingOrPanic(rt, ring);
            park();
            return cancelledFsResult();
        }
        registered = true;
    }

    park();

    // Post-wake: deregister cancel + (if callback in flight)
    // busy-wait for it to release ctx.
    if (registered) {
        if (!cancel.?.deregisterRemoved(&w)) {
            while (!ctx.completed.load(.acquire)) std.atomic.spinLoopHint();
        }
    }

    // Branch on state. With no cancel, state is always Completed.
    // With cancel, state is one of {Completed, Cancelling,
    // CancelledAndDrained}; see memo §2 for the four scenarios.
    const state = op.state.load(.acquire);
    return switch (state) {
        fs_ring_mod.STATE_COMPLETED => op.result,
        fs_ring_mod.STATE_CANCELLING => blk: {
            // Cancel fired first, original CQE hasn't drained.
            // Submit ASYNC_CANCEL + re-park; drainer transitions
            // Cancelling→CancelledAndDrained when the original
            // CQE arrives (with the cancel-ack CQE handled by
            // the bit-0 fast path). When we wake again the state
            // must be CancelledAndDrained.
            const op_ud = @intFromPtr(op);
            ring.prepCancel(op_ud, fs_ring_mod.cancelUserDataFor(op_ud)) catch {};
            flushRingOrPanic(rt, ring);
            park();
            std.debug.assert(op.state.load(.acquire) == fs_ring_mod.STATE_CANCELLED_AND_DRAINED);
            break :blk cancelledFsResult();
        },
        fs_ring_mod.STATE_CANCELLED_AND_DRAINED => cancelledFsResult(),
        else => unreachable, // drainer never leaves state at Pending after unpark
    };
}

/// Shared epilogue: post-flush, try the inline-completion fast
/// path; if our CQE wasn't ready, fall through to the
/// park-aware Await machinery. Cancel-aware callers can't take
/// the inline path — Await's register/deregister sequence has
/// to wrap the entire wait window for safe `*Cancel` lifetime.
inline fn fsRingTail(
    rt: *Runtime,
    ring: *fs_ring_mod.FsRing,
    op: *fs_ring_mod.FsOp,
    cancel: ?*cancel_mod.Cancel,
) fs_result {
    if (cancel == null) {
        if (tryFsRingInlineComplete(rt, ring, op)) return op.result;
    }
    return fsRingAwait(rt, ring, op, cancel);
}

pub fn fsRingRead(fd: i32, buf: []u8, offset: u64, cancel: ?*cancel_mod.Cancel) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    if (cancel) |c| if (c.isFired()) return cancelledFsResult();
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepRead(fd, buf, offset, @intFromPtr(&op)) catch return null;
    flushRingOrPanic(rt, ring);
    return fsRingTail(rt, ring, &op, cancel);
}

pub fn fsRingWrite(fd: i32, buf: []const u8, offset: u64, cancel: ?*cancel_mod.Cancel) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    if (cancel) |c| if (c.isFired()) return cancelledFsResult();
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepWrite(fd, buf, offset, @intFromPtr(&op)) catch return null;
    flushRingOrPanic(rt, ring);
    return fsRingTail(rt, ring, &op, cancel);
}

pub fn fsRingFsync(fd: i32, cancel: ?*cancel_mod.Cancel) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    if (cancel) |c| if (c.isFired()) return cancelledFsResult();
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepFsync(fd, @intFromPtr(&op), false) catch return null;
    flushRingOrPanic(rt, ring);
    return fsRingTail(rt, ring, &op, cancel);
}

pub fn fsRingFdatasync(fd: i32, cancel: ?*cancel_mod.Cancel) ?fs_result {
    const co = current.require();
    const rt: *Runtime = @ptrCast(@alignCast(co.runtime));
    const ring = currentPRing(rt) orelse return null;
    if (cancel) |c| if (c.isFired()) return cancelledFsResult();
    var op: fs_ring_mod.FsOp = .{ .coro = co, .result = undefined };
    ring.prepFsync(fd, @intFromPtr(&op), true) catch return null;
    flushRingOrPanic(rt, ring);
    return fsRingTail(rt, ring, &op, cancel);
}

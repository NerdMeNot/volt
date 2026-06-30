//! Per-socket PollDesc lifecycle helpers.
//!
//! Each socket type (TcpStream, TcpListener, UdpSocket, UnixStream,
//! UnixListener, UnixDatagram) carries a `pd: pd_handle.Atomic`
//! field and uses the helpers here to lazy-init / tear down the
//! PollDesc.
//!
//! ## Slot lock
//!
//! The slot has a small spinlock that serializes `ensure` against
//! `release`. Within the lock, `ensure` reads the current pd, bumps
//! its refcount, and returns it — under the lock guarantee that
//! `release` can't concurrently swap-and-free.
//!
//! Without the lock, this race breaks:
//!
//!   1. Accepter (T0): `ensure` returns `pd` (cached load).
//!   2. (T0 hasn't called `pd.incref` yet — it'll happen inside
//!      `Reactor.waitFd`. Or, with the v0 sentinel design, it never
//!      did `incref` before returning at all.)
//!   3. Closer (T1): `release`. swap pd→sentinel. evict.
//!      closeAndWait drives refcount 1→0 (owner ref dropped). Frees.
//!   4. T0: calls `pd.incref` → reads freed memory. fetchAdd on a
//!      zeroed-out refcount returns prev=0. Panic ("incref from zero").
//!
//! With the lock, step 1's incref happens atomically with the slot
//! lookup. Step 3's `release` waits for the lock, then sees the
//! incref'd refcount; closeAndWait waits for T0's eventual decref
//! before freeing.
//!
//! The lock is held for a few atomic ops on the fast path. The slow
//! path (lazy registerFd) holds it for one kevent syscall — still
//! well-bounded.
//!
//! ## Lifecycle contract
//!
//! `ensure` returns a `*PollDesc` with an **extra refcount the caller
//! owns**. Caller must call `pd.decref()` exactly once when done.
//! Typical pattern:
//!
//! ```zig
//! const pd = try pd_handle.ensure(&self.pd, rt, fd);
//! defer pd.decref();
//! // use pd via rt.reactor.waitFd / waitFdCancel
//! ```
//!
//! This caller-held ref keeps `pd` alive across the entire scope —
//! across any number of `waitFd` calls or syscalls in between — so
//! the closer's `closeAndWait` blocks until the scope exits.
//!
//! ## Why lazy-init
//!
//! Socket constructors like `TcpListener.bind` may be called BEFORE
//! `rt.run()`, with no coroutine context to access the runtime
//! allocator. Only operations that may park (accept, read,
//! recvFrom, …) need a PollDesc, and those are always called inside
//! a coroutine. We defer allocation until the first wait.

const std = @import("std");
const poll_desc = @import("poll_desc.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const reactor_mod = @import("reactor.zig");

pub const Atomic = struct {
    /// Spinlock protecting `pd`, `closed`, and `rt`. 0=unlocked, 1=locked.
    /// Held for a handful of atomic ops on the fast path; one kevent
    /// syscall on the lazy-init path. Spin contention should be rare.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// The PollDesc, if initialised. Set to non-null inside `ensure`'s
    /// init path under the lock; cleared back to null by `release`.
    pd: ?*poll_desc.PollDesc = null,
    /// True once `release` has run. Future `ensure` calls bail with
    /// `error.BadDescriptor` so a not-yet-scheduled peer coroutine
    /// can't re-register the closed fd.
    closed: bool = false,
    /// Set when the PD is initialised. `release` needs this to access
    /// the allocator + reactor; it may run outside coroutine context
    /// (test cleanup), where `current.require()` is unavailable.
    rt: ?*runtime.Runtime = null,
};

inline fn acquire(slot: *Atomic) void {
    while (slot.lock.swap(1, .acquire) != 0) std.atomic.spinLoopHint();
}

inline fn release_lock(slot: *Atomic) void {
    slot.lock.store(0, .release);
}

/// Lazy-init a PollDesc for `fd` and register it with the reactor.
/// Returns the PD with an extra refcount the caller owns; caller MUST
/// `pd.decref()` exactly once when done with the pd.
///
/// Returns `error.BadDescriptor` if `release` has already closed the
/// slot.
pub fn ensure(
    slot: *Atomic,
    rt: *runtime.Runtime,
    fd: i32,
) reactor_mod.ReactorWaitError!*poll_desc.PollDesc {
    acquire(slot);
    if (slot.closed) {
        release_lock(slot);
        return error.BadDescriptor;
    }
    if (slot.pd) |pd| {
        pd.incref();
        release_lock(slot);
        return pd;
    }
    // First-time init under the lock. registerFd may fail (EBADF on a
    // closed fd, EMFILE on resource exhaustion); on failure we unwind
    // and surface the error without publishing anything.
    const new_pd = rt.allocator.create(poll_desc.PollDesc) catch {
        release_lock(slot);
        return error.SystemResources;
    };
    new_pd.init();
    rt.reactor.registerFd(fd, new_pd) catch |e| {
        new_pd.deinit();
        rt.allocator.destroy(new_pd);
        release_lock(slot);
        return e;
    };
    slot.pd = new_pd;
    slot.rt = rt;
    new_pd.incref();
    release_lock(slot);
    return new_pd;
}

/// Tear down the PollDesc owned by this slot and mark the slot
/// closed. Future `ensure` calls return `error.BadDescriptor`.
///
/// In coroutine context: `evict` + `closeAndWait` wakes any parked
/// waiters and blocks until every in-flight `ensure`-incref has been
/// matched by a `pd.decref()`. Outside coroutine context (test
/// cleanup after `rt.run` returns) the slot can have at most the
/// owner ref alive (no live waiters), so we drop it directly.
///
/// Caller must follow with the actual fd close (libc `close` or
/// `closeFdDispatch`).
pub fn release(slot: *Atomic, fd: i32) void {
    acquire(slot);
    if (slot.closed) {
        // Double-release. Idempotent.
        release_lock(slot);
        return;
    }
    slot.closed = true;
    const pd = slot.pd orelse {
        // Slot was never lazy-initialised — nothing to tear down.
        release_lock(slot);
        return;
    };
    slot.pd = null;
    const rt = slot.rt.?;
    release_lock(slot);

    if (current.get() != null) {
        pd.evict();
        pd.closeAndWait();
    } else {
        // No coroutine context → no waiters possible. (Verified by the
        // assert: only the owner ref should be live.)
        pd.evict();
        const prev = pd.refcount.fetchSub(1, .acq_rel);
        std.debug.assert(prev == 1);
    }
    rt.reactor.unregisterFd(fd, pd);
    pd.deinit();
    rt.allocator.destroy(pd);
}

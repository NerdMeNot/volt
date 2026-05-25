//! Per-socket PollDesc lifecycle helpers.
//!
//! Each socket type (TcpStream, TcpListener, UdpSocket, UnixStream,
//! UnixListener) carries a `pd: std.atomic.Value(?*PollDesc)` field
//! and uses the helpers here to lazy-init / tear down the PollDesc.
//!
//! Why lazy-init: socket constructors like `TcpListener.bind` may be
//! called BEFORE `rt.run()`, with no coroutine context to access the
//! runtime allocator. Only operations that may park (accept, read,
//! recvFrom, …) need a PollDesc, and those are always called inside
//! a coroutine. We defer allocation until the first wait.
//!
//! Why atomic: `TcpStream.read` and `TcpStream.write` can run on
//! independent coroutines parking on different filter slots. Both
//! call `ensure` concurrently the first time. The CAS-and-free-on-
//! loss pattern below keeps the fast path (already initialized) to
//! a single relaxed load.
//!
//! Why this is per-fd and not a shared map: the kqueue migration
//! uses the PollDesc pointer as the kevent `udata`, so the reactor
//! dispatches events directly to it. Owning the PollDesc next to
//! the fd keeps the lifetime explicit.

const std = @import("std");
const poll_desc = @import("poll_desc.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const reactor_mod = @import("reactor.zig");

pub const Atomic = std.atomic.Value(?*poll_desc.PollDesc);

/// Lazy-init a PollDesc for `fd` and register it with the reactor.
/// Subsequent calls return the cached pointer with a single relaxed
/// load. Safe to call concurrently from multiple coroutines; only
/// one will win the CAS and complete the registerFd.
///
/// Returns `ReactorWaitError` rather than the broader allocator error
/// set so it composes cleanly into existing IoError-returning paths.
/// `OutOfMemory` from the allocator maps to `SystemResources` — the
/// shape downstream callers already retry on for kernel-resource
/// exhaustion.
pub fn ensure(
    slot: *Atomic,
    rt: *runtime.Runtime,
    fd: i32,
) reactor_mod.ReactorWaitError!*poll_desc.PollDesc {
    if (slot.load(.acquire)) |pd| return pd;
    const new_pd = rt.allocator.create(poll_desc.PollDesc) catch return error.SystemResources;
    new_pd.init();
    errdefer {
        new_pd.deinit();
        rt.allocator.destroy(new_pd);
    }
    // Register the kernel side BEFORE publishing the pointer. A
    // concurrent reader that wins the load after we publish must see
    // the registration in place, otherwise its waitFd would park on
    // a PollDesc the reactor isn't dispatching events to.
    try rt.reactor.registerFd(fd, new_pd);
    if (slot.cmpxchgStrong(null, new_pd, .acq_rel, .acquire)) |existing| {
        // Lost the race. Roll back our registration + free our PD;
        // return the winner's.
        rt.reactor.unregisterFd(fd, new_pd);
        new_pd.deinit();
        rt.allocator.destroy(new_pd);
        return existing.?;
    }
    return new_pd;
}

/// Tear down the PollDesc associated with this socket. Safe to call
/// from coroutine context (uses pd.closeAndWait for in-flight wait
/// draining) or non-coroutine context (skips the wait — no waiters
/// possible without a coroutine). Idempotent; second call is a no-op.
///
/// Caller must follow with the actual fd close (via `closeFdDispatch`
/// or equivalent). The `unregisterFd` here releases the reactor's
/// tracking; the libc close releases the kernel registration.
pub fn release(slot: *Atomic, fd: i32) void {
    const pd = slot.swap(null, .acq_rel) orelse return;
    // We need an allocator + reactor to free. Both live on the
    // Runtime. Outside coroutine context, no Runtime is reachable
    // via `current`, so the PD storage leaks — but this only happens
    // if the user closes the socket post-`rt.deinit`, which is a
    // misuse pattern we don't try to handle (the fd close itself
    // also can't dispatch waiters in that case).
    const c = current.get() orelse {
        // Coroutine context required for clean shutdown. Without it,
        // we can't free the PD. Leak it deliberately and move on.
        return;
    };
    const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
    // Mark closing + wake any parked state-machine waiters. evict is
    // user-space CAS; safe even if no one is parked.
    pd.evict();
    pd.closeAndWait();
    rt.reactor.unregisterFd(fd, pd);
    pd.deinit();
    rt.allocator.destroy(pd);
}

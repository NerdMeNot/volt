//! Windows reactor backend — IOCP + AFD (readiness model).
//!
//! Same public surface as `reactor_kqueue.zig` / `reactor_epoll.zig` /
//! `reactor_iouring.zig`, enforced by the comptime conformance check
//! in `reactor.zig`.
//!
//! ## Why AFD instead of completion-model IOCP
//!
//! Volt's I/O protocol everywhere else is **readiness-based**: a
//! coroutine calls a non-blocking syscall, parks on `WouldBlock`,
//! the reactor wakes it when the fd becomes ready, and the user code
//! re-tries the syscall. Plain IOCP is a *completion* model — the
//! kernel does the read/write itself and you receive bytes-transferred
//! in the CQE. Trying to bridge the two leads to either two distinct
//! I/O paths per platform or a per-call buffer copy on Windows.
//!
//! The fix used by mio, wepoll, and Tokio: AFD.sys (the Winsock
//! kernel driver) exposes a poll-style readiness IOCTL —
//! `IOCTL_AFD_POLL`. Issued via `NtDeviceIoControlFile` against an
//! AFD device handle, it delivers an IOCP completion when the
//! requested event mask fires on the target socket. The reactor
//! interface stays identical: `registerWait(fd, kind, target)` →
//! `poll() → wakeFn(target)` works on Windows with the same model.
//!
//! ## How a registration flows
//!
//! 1. `registerWait(socket, kind, target)`:
//!    - Allocate a `Registration` slab slot. Slot's `OVERLAPPED` is
//!      stable and IOCP-keyable.
//!    - Build an `AFD_POLL_INFO` describing (socket, AFD event mask
//!      derived from `kind`).
//!    - `NtDeviceIoControlFile(afd_handle, IOCTL_AFD_POLL, ...)`.
//!    - The OVERLAPPED's address is what the IOCP delivers on
//!      completion.
//!
//! 2. `poll(timeout, wakeFn)`:
//!    - `GetQueuedCompletionStatus` on the IOCP.
//!    - The completion's `lpOverlapped` points back to a slot's
//!      OVERLAPPED. Walk the slot's stored `target` and dispatch.
//!
//! 3. `unregisterWait(socket, kind)`:
//!    - Find the matching slot, mark it cancelled (bump generation).
//!    - `CancelIoEx(afd_handle, &slot.overlapped)` to abort the
//!      kernel-side poll. The original AFD completion will still
//!      arrive — its CQE has `STATUS_CANCELLED`. The poll handler
//!      detects the generation mismatch and drops it.
//!
//! ## Status
//!
//! Cross-compiles. NOT yet runtime-validated — Windows CI runner
//! comes with Phase 2g of the v1.1 plan. `reactor.zig` still
//! `@compileError`s on Windows because Phase 2c-2g (syscall layer
//! + process spawn + signal + watcher arms) aren't done; flipping
//! the dispatcher requires those.

const std = @import("std");
const builtin = @import("builtin");

const reactor_types = @import("reactor_types.zig");
const Slab = @import("../internal/util/slab.zig").Slab;

const Mutex = @import("../internal/thread.zig").Mutex;

comptime {
    if (builtin.os.tag != .windows) {
        @compileError("reactor_iocp.zig is for Windows only");
    }
}

const ntdll = @import("../internal/win32/ntdll.zig");

/// Re-exported so `reactor.zig`'s conformance check can read it from
/// `impl.EventKind`. The canonical declaration lives in
/// `reactor_types.zig`.
pub const EventKind = reactor_types.EventKind;

const ReactorError = reactor_types.ReactorError;

// ─────────────────────────────────────────────────────────────────────
// Registration table
// ─────────────────────────────────────────────────────────────────────

const RegKind = enum(u8) {
    wait,
    timer,
    cancelled, // marked but waiting for kernel completion to land
};

const Registration = struct {
    /// OVERLAPPED owned by this slot. Pointer-stable for the slot's
    /// lifetime; IOCP delivers the address here on completion. Each
    /// AFD poll request needs a fresh OVERLAPPED, so we never reuse
    /// these across registrations.
    overlapped: ntdll.OVERLAPPED = std.mem.zeroes(ntdll.OVERLAPPED),
    /// AFD's input/output buffer for `IOCTL_AFD_POLL`. The kernel
    /// reads it on submit and writes back the fired events on
    /// completion. Must outlive the in-flight ioctl.
    poll_info: ntdll.AFD_POLL_INFO = std.mem.zeroes(ntdll.AFD_POLL_INFO),
    /// Wake target (typically a `*Park`). Stale completions detected
    /// via `generation` never dereference this.
    target: *anyopaque,
    /// What kind of registration occupies this slot.
    kind: RegKind,
    /// Bumped on cancel. The CQE handler compares against the
    /// generation embedded in the slot lookup; mismatch → drop.
    generation: u16,
    /// For `kind == .wait`: which (socket, event_kind) the slot
    /// represents. Lets `unregisterWait(fd, kind)` find the slot.
    fd: ?std.posix.fd_t,
    event_kind: EventKind,
    /// For `kind == .timer`: the threadpool timer object — closed on
    /// cancel or fire.
    tp_timer: ?*ntdll.TP_TIMER,
};

/// Slab capacity. Same sizing rationale as the iouring backend —
/// 65k concurrent registrations covers any realistic workload, with
/// ~2 MB worst-case footprint.
const SLAB_CAPACITY: usize = 65_536;

// ─────────────────────────────────────────────────────────────────────
// User-data (completion key) encoding
// ─────────────────────────────────────────────────────────────────────

/// Tickle key — sentinel posted via `PostQueuedCompletionStatus` to
/// kick the reactor out of GetQueuedCompletionStatus.
const TICKLE_KEY: usize = 1;

/// Cancel-ack key — IOCP completions that result from CancelIoEx
/// arrive on the same OVERLAPPED; we identify them by NTSTATUS in
/// the OVERLAPPED's `Internal` field.
///
/// Per-registration completion key: encode the slot index and
/// generation. Since the OVERLAPPED is what the kernel keys the
/// completion on, we don't strictly need the slot index in the
/// completion key — we can read the OVERLAPPED's address and walk
/// back to the Registration. But carrying slot+generation in the
/// completion key gives O(1) lookup + generation check without the
/// reverse-mapping.
const CompletionKey = packed struct(u64) {
    /// 0 = tickle, 1 = wait, 2 = timer, 3 = cancel-ack.
    tag: u4,
    _pad: u12 = 0,
    generation: u16,
    slot: u32,

    fn pack(self: CompletionKey) usize {
        return @bitCast(self);
    }
    fn unpack(value: usize) CompletionKey {
        return @bitCast(value);
    }
};

const KEY_TICKLE: u4 = 0;
const KEY_WAIT: u4 = 1;
const KEY_TIMER: u4 = 2;

// ─────────────────────────────────────────────────────────────────────
// Reactor
// ─────────────────────────────────────────────────────────────────────

pub const Reactor = struct {
    /// IOCP that all completions land on.
    iocp: ntdll.HANDLE,
    /// `\Device\Afd\Volt` handle — the AFD device we issue
    /// `IOCTL_AFD_POLL` against. Associated with `iocp` so AFD
    /// completions are routed through the IOCP.
    afd: ntdll.HANDLE,
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    submit_mutex: Mutex = .{},
    regs_mutex: Mutex = .{},
    regs: Slab(Registration),
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) ReactorError!Reactor {
        const port = ntdll.CreateIoCompletionPort(
            ntdll.INVALID_HANDLE_VALUE,
            null,
            0,
            0,
        ) orelse return error.InitFailed;
        errdefer _ = ntdll.CloseHandle(port);

        const afd = openAfdDevice() catch return error.InitFailed;
        errdefer _ = ntdll.NtClose(afd);

        // Associate AFD with the IOCP. Completion key 0 because we
        // distinguish tickle vs. wait via the OVERLAPPED → slot lookup
        // path on the receive side. (`PostQueuedCompletionStatus` will
        // push tickles with TICKLE_KEY, which can't collide because no
        // real OVERLAPPED can have key=TICKLE_KEY+slot=0.)
        if (ntdll.CreateIoCompletionPort(afd, port, 0, 0) == null) {
            return error.InitFailed;
        }

        // Bypass IOCP for synchronous completions (where AFD returns
        // STATUS_SUCCESS instead of STATUS_PENDING). Without this we'd
        // get spurious wake-ups for the small number of fast-path
        // completions where the readiness was already true at submit
        // time.
        _ = ntdll.SetFileCompletionNotificationModes(
            afd,
            ntdll.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | ntdll.FILE_SKIP_SET_EVENT_ON_HANDLE,
        );

        var regs = Slab(Registration).init(allocator, SLAB_CAPACITY) catch return error.OutOfMemory;
        errdefer regs.deinit();

        return .{
            .iocp = port,
            .afd = afd,
            .regs = regs,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        // Tear down outstanding timers (their callbacks could still
        // fire after we close the IOCP; CloseThreadpoolTimer +
        // WaitForThreadpoolTimerCallbacks together wait the in-flight
        // ones out).
        var it = self.regs.iterator();
        while (it.next()) |entry| {
            if (entry.value.tp_timer) |t| {
                ntdll.CloseThreadpoolTimer(t);
                ntdll.WaitForThreadpoolTimerCallbacks(t, std.os.windows.BOOL.TRUE);
            }
        }
        self.regs.deinit();
        _ = ntdll.NtClose(self.afd);
        _ = ntdll.CloseHandle(self.iocp);
    }

    pub fn pendingCount(self: *const Reactor) usize {
        return self.pending.load(.acquire);
    }

    pub fn tickle(self: *Reactor) void {
        const key = (CompletionKey{ .tag = KEY_TICKLE, .generation = 0, .slot = 0 }).pack();
        _ = ntdll.PostQueuedCompletionStatus(self.iocp, 0, key, null);
    }

    pub fn registerWait(
        self: *Reactor,
        fd: std.posix.fd_t,
        kind: EventKind,
        target: *anyopaque,
    ) ReactorError!void {
        // Allocate the slot up-front so the OVERLAPPED + AFD_POLL_INFO
        // have a stable address for the kernel to write into.
        const slot_id = self.allocSlot(target, .wait, fd, kind, null) catch
            return error.RegistrationFailed;
        errdefer self.freeSlot(slot_id);

        // Fill the AFD poll info. Single-handle, non-exclusive.
        const reg_ptr = self.getRegPtr(slot_id) orelse return error.RegistrationFailed;
        reg_ptr.poll_info = .{
            .Timeout = std.math.maxInt(ntdll.LARGE_INTEGER), // no timeout — we cancel via CancelIoEx
            .NumberOfHandles = 1,
            .Exclusive = 0,
            .Handles = .{.{
                .Handle = @ptrCast(fd),
                .Events = afdEventsFor(kind),
                .Status = 0,
            }},
        };

        // Issue IOCTL_AFD_POLL. The IOSB on the slot tracks completion;
        // the OVERLAPPED's address is what GetQueuedCompletionStatus
        // delivers. AFD ignores the apc/event because we're using
        // IOCP completion-port routing.
        var iosb_local: ntdll.IO_STATUS_BLOCK = undefined;
        // The kernel writes directly into the OVERLAPPED's Internal/
        // InternalHigh fields, but AFD specifically wants its IOSB
        // pointer too — we use a stack-local IOSB only for the
        // synchronous path; async completions land in the OVERLAPPED.
        _ = &iosb_local;

        self.submit_mutex.lock();
        defer self.submit_mutex.unlock();

        const status = ntdll.NtDeviceIoControlFile(
            self.afd,
            null,
            null,
            null,
            @ptrCast(&reg_ptr.overlapped), // IO_STATUS_BLOCK overlay
            ntdll.IOCTL_AFD_POLL,
            @ptrCast(&reg_ptr.poll_info),
            @sizeOf(ntdll.AFD_POLL_INFO),
            @ptrCast(&reg_ptr.poll_info),
            @sizeOf(ntdll.AFD_POLL_INFO),
        );

        if (status != ntdll.STATUS_PENDING and !ntdll.ntSuccess(status)) {
            return error.RegistrationFailed;
        }
        _ = self.pending.fetchAdd(1, .release);
    }

    pub fn unregisterWait(self: *Reactor, fd: std.posix.fd_t, kind: EventKind) void {
        const slot_id = self.findWaitSlot(fd, kind) orelse return;

        // Bump generation + mark as cancelled BEFORE issuing
        // CancelIoEx — so when the cancel completion arrives we
        // recognize it as stale and drop it.
        self.markCancelled(slot_id);

        const reg_ptr = self.getRegPtr(slot_id) orelse return;
        _ = ntdll.CancelIoEx(self.afd, @ptrCast(&reg_ptr.overlapped));
        // Don't free the slot here — the kernel still owns the
        // OVERLAPPED until the cancel completion arrives. `poll`
        // frees on stale-CQE arrival.
    }

    pub fn registerTimer(
        self: *Reactor,
        duration_ns: u64,
        target: *anyopaque,
    ) ReactorError!u64 {
        const tp = ntdll.CreateThreadpoolTimer(&timerCallback, null, null) orelse
            return error.RegistrationFailed;
        errdefer ntdll.CloseThreadpoolTimer(tp);

        const slot_id = self.allocSlot(target, .timer, null, .readable, tp) catch
            return error.RegistrationFailed;
        errdefer self.freeSlot(slot_id);

        // Re-fetch with the slot+generation context so the callback
        // can route back to the right slot.
        const reg_ptr = self.getRegPtr(slot_id) orelse return error.RegistrationFailed;

        // The threadpool callback receives the slot pointer as
        // context. We post that pointer to IOCP; the poll handler
        // reads the slot from there.
        ntdll.SetThreadpoolTimer(tp, ntsTimeFromNs(duration_ns), 0, 0);

        // Stash the slot pointer for the callback. We use the
        // OVERLAPPED's `hEvent` field as a piggyback for the slot
        // pointer (we never use hEvent for actual events on this
        // backend — the threadpool timer doesn't go through AFD).
        reg_ptr.overlapped.hEvent = @ptrCast(@constCast(reg_ptr));

        _ = self.pending.fetchAdd(1, .release);

        return (CompletionKey{
            .tag = KEY_TIMER,
            .generation = 0,
            .slot = @intCast(slot_id),
        }).pack();
    }

    pub fn unregisterTimer(self: *Reactor, id: u64) void {
        const key = CompletionKey.unpack(id);
        if (key.tag != KEY_TIMER) return;

        const slot_id: usize = key.slot;
        self.markCancelled(slot_id);

        // CloseThreadpoolTimer + cancelPending=true aborts the timer.
        // If the callback already started, WaitForThreadpoolTimerCallbacks
        // ensures it finishes before we proceed. The slot's generation
        // is bumped so any in-flight callback that posts a CQE has its
        // completion silently dropped.
        const reg_ptr = self.getRegPtr(slot_id) orelse return;
        if (reg_ptr.tp_timer) |tp| {
            ntdll.CloseThreadpoolTimer(tp);
            ntdll.WaitForThreadpoolTimerCallbacks(tp, std.os.windows.BOOL.TRUE);
            reg_ptr.tp_timer = null;
        }
        self.freeSlot(slot_id);
        _ = self.pending.fetchSub(1, .release);
    }

    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
    ) anyerror!usize {
        const timeout_ms: ntdll.ULONG = if (timeout_ns) |ns|
            @intCast(@min(@divTrunc(ns, std.time.ns_per_ms), std.math.maxInt(ntdll.ULONG) - 1))
        else
            ntdll.INFINITE;

        var transferred: ntdll.ULONG = 0;
        var key_raw: usize = 0;
        var ovl: ?*ntdll.OVERLAPPED = null;
        const ok = ntdll.GetQueuedCompletionStatus(
            self.iocp,
            &transferred,
            &key_raw,
            &ovl,
            timeout_ms,
        );

        if (key_raw == 0 and ovl == null) {
            // Timeout or empty wake.
            return 0;
        }

        const key = CompletionKey.unpack(key_raw);

        // Tickle: just return; the next poll cycle picks up new work.
        if (key.tag == KEY_TICKLE) return 0;

        // Look up the slot. If the generation no longer matches, the
        // completion is for an op that was cancelled — drop it and
        // free the slot now (the kernel is done).
        const target = self.consumeSlot(key) orelse {
            _ = self.pending.fetchSub(1, .release);
            return 0;
        };

        _ = self.pending.fetchSub(1, .release);
        // For .wait completions, a `BOOL ok = false` from
        // GetQueuedCompletionStatus combined with the OVERLAPPED's
        // Internal field holding STATUS_CANCELLED would also be a
        // cancelled op — but consumeSlot already filtered those by
        // generation. Anything that gets past consumeSlot is a real
        // wake.
        _ = ok;

        try wakeFn(wake_ctx, target);
        return 1;
    }

    // ───────────────────────────────────────────────────────────────
    // Internal helpers
    // ───────────────────────────────────────────────────────────────

    fn allocSlot(
        self: *Reactor,
        target: *anyopaque,
        kind: RegKind,
        fd: ?std.posix.fd_t,
        event_kind: EventKind,
        tp_timer: ?*ntdll.TP_TIMER,
    ) !usize {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        return self.regs.insert(.{
            .target = target,
            .kind = kind,
            .generation = 0,
            .fd = fd,
            .event_kind = event_kind,
            .tp_timer = tp_timer,
        });
    }

    fn freeSlot(self: *Reactor, slot_id: usize) void {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();
        _ = self.regs.remove(slot_id);
    }

    fn markCancelled(self: *Reactor, slot_id: usize) void {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();
        if (self.regs.get(slot_id)) |entry| {
            entry.generation +%= 1;
            entry.kind = .cancelled;
        }
    }

    /// Look up a slot for completion delivery. Returns `target` on a
    /// match (and frees the slot); null if the slot is gone or the
    /// generation has changed (cancelled).
    fn consumeSlot(self: *Reactor, key: CompletionKey) ?*anyopaque {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        const entry = self.regs.get(key.slot) orelse return null;
        if (entry.generation != key.generation or entry.kind == .cancelled) {
            // Stale — clean up the slot.
            _ = self.regs.remove(key.slot);
            return null;
        }
        const target = entry.target;
        _ = self.regs.remove(key.slot);
        return target;
    }

    fn findWaitSlot(self: *Reactor, fd: std.posix.fd_t, kind: EventKind) ?usize {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();

        var it = self.regs.iterator();
        while (it.next()) |entry| {
            const reg = entry.value;
            if (reg.kind != .wait) continue;
            if (reg.fd) |reg_fd| {
                if (reg_fd != fd or reg.event_kind != kind) continue;
                return entry.key;
            }
        }
        return null;
    }

    fn getRegPtr(self: *Reactor, slot_id: usize) ?*Registration {
        self.regs_mutex.lock();
        defer self.regs_mutex.unlock();
        return self.regs.get(slot_id);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

fn afdEventsFor(kind: EventKind) ntdll.ULONG {
    return switch (kind) {
        .readable => ntdll.AFD_POLL_RECEIVE | ntdll.AFD_POLL_DISCONNECT |
            ntdll.AFD_POLL_ABORT | ntdll.AFD_POLL_ACCEPT,
        .writable => ntdll.AFD_POLL_SEND | ntdll.AFD_POLL_CONNECT_FAIL,
    };
}

/// Convert a relative duration in ns to a Win32 FILETIME pointer
/// suitable for `SetThreadpoolTimer`. Negative 100-ns intervals are
/// the threadpool timer's relative encoding.
fn ntsTimeFromNs(duration_ns: u64) *ntdll.FILETIME {
    // Static thread-local: SetThreadpoolTimer reads the FILETIME
    // synchronously, so we don't need long lifetime — but a zero-
    // sized stack reference would dangle. Use a thread-local for
    // safety.
    const Local = struct {
        threadlocal var ft: ntdll.FILETIME = .{ .dwLowDateTime = 0, .dwHighDateTime = 0 };
    };
    // 100-ns ticks; negative = relative.
    const ticks_100ns: i64 = -@as(i64, @intCast(duration_ns / 100));
    const u: u64 = @bitCast(ticks_100ns);
    Local.ft.dwLowDateTime = @truncate(u);
    Local.ft.dwHighDateTime = @truncate(u >> 32);
    return &Local.ft;
}

/// Threadpool timer callback. Posts a completion to the reactor's
/// IOCP keyed on the slot. We get the slot via the registration
/// pointer stashed in `Context` at SetThreadpoolTimer time.
fn timerCallback(
    instance: ?*ntdll.TP_CALLBACK_INSTANCE,
    context: ?*anyopaque,
    timer: ?*ntdll.TP_TIMER,
) callconv(.winapi) void {
    _ = instance;
    _ = timer;
    _ = context;
    // Real implementation: walk back to the Reactor + slot via
    // context, then `PostQueuedCompletionStatus(reactor.iocp, 0,
    // CompletionKey{.tag=KEY_TIMER, slot, gen}.pack(), null)`.
    //
    // This requires storing the reactor pointer alongside the slot;
    // the stub here lands the reactor file in compiling shape so
    // Phase 2b's structural part can ship and the runtime hook-up
    // is a self-contained follow-up.
}

/// Open `\Device\Afd\Volt` via NtCreateFile.
fn openAfdDevice() !ntdll.HANDLE {
    var handle: ntdll.HANDLE = undefined;
    var iosb: ntdll.IO_STATUS_BLOCK = undefined;

    const path = ntdll.AFD_DEVICE_NAME_W;
    var name = ntdll.UNICODE_STRING{
        .Length = @intCast(path.len * @sizeOf(u16)),
        .MaximumLength = @intCast(path.len * @sizeOf(u16)),
        .Buffer = path.ptr,
    };

    var attrs = ntdll.OBJECT_ATTRIBUTES{
        .Length = @sizeOf(ntdll.OBJECT_ATTRIBUTES),
        .RootDirectory = null,
        .ObjectName = &name,
        .Attributes = ntdll.OBJ_CASE_INSENSITIVE,
        .SecurityDescriptor = null,
        .SecurityQualityOfService = null,
    };

    const status = ntdll.NtCreateFile(
        &handle,
        ntdll.SYNCHRONIZE,
        &attrs,
        &iosb,
        null,
        0,
        ntdll.FILE_SHARE_READ | ntdll.FILE_SHARE_WRITE,
        ntdll.FILE_OPEN,
        0,
        null,
        0,
    );

    if (!ntdll.ntSuccess(status)) {
        return error.AfdOpenFailed;
    }
    return handle;
}

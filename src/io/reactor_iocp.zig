//! Windows IOCP reactor backend.
//!
//! Same public interface as the kqueue/epoll/io_uring backends
//! (Reactor + EventKind + WaitKey + init/deinit/pendingCount/tickle/
//! registerWait/registerTimer/poll). Built directly on Win32
//! `CreateIoCompletionPort` / `GetQueuedCompletionStatus`.
//!
//! ## Status
//!
//! Cross-compiles for `x86_64-windows-gnu` and `aarch64-windows-gnu`,
//! NOT yet runtime-default. The reactor dispatcher in `reactor.zig`
//! `@compileError`s on Windows because:
//!
//! 1. **Untested at runtime.** Cross-compile catches API/type errors
//!    but not OS-interaction bugs. The CI matrix needs a Windows
//!    runner to gate the default flip.
//! 2. **Stack overflow handling.** The Darwin/Linux paths use SIGSEGV
//!    + sigsetjmp; Windows uses Structured Exception Handling (SEH).
//!    `stack_overflow.zig` needs an SEH variant.
//! 3. **VirtualAlloc-based stacks.** `coroutine/stack.zig` already
//!    has the Windows arm (VirtualAlloc reserve+commit, VirtualFree
//!    release). Tested via cross-compile; needs runtime validation.
//!
//! Windows runtime-default lands when the SEH overflow handler ships
//! and a Windows CI runner is green.
//!
//! ## Implementation
//!
//! - `init`: `CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0)`
//!   with `NumberOfConcurrentThreads = 0` (= NumberOfProcessors).
//! - `tickle()`: `PostQueuedCompletionStatus` with a sentinel key.
//! - `registerWait`: `CreateIoCompletionPort(fd, port, key=target, 0)`
//!   to associate the fd. Then issue an overlapped WSARecv/WSASend to
//!   actually wait — today we use a poll-style interface that mirrors
//!   the kqueue/epoll one (caller has already issued the syscall and
//!   gotten WSAEWOULDBLOCK; we wait for the IO to be schedulable).
//!   A future rewrite can make the I/O calls fully completion-based.
//! - `registerTimer`: `CreateThreadpoolTimer` + `SetThreadpoolTimer`.
//!   Callback issues `PostQueuedCompletionStatus` with the target.
//! - `poll`: `GetQueuedCompletionStatus(port, &transferred, &key,
//!   &overlapped, timeout_ms)` in a loop, dispatching by key.

const std = @import("std");
const builtin = @import("builtin");
const Mutex = @import("../internal/thread.zig").Mutex;

comptime {
    if (builtin.os.tag != .windows) {
        @compileError("reactor_iocp.zig is for Windows only");
    }
}

pub const EventKind = enum(u8) { readable, writable };

const HANDLE = std.os.windows.HANDLE;
const DWORD = std.os.windows.DWORD;
const ULONG_PTR = usize;
const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));
const INFINITE: DWORD = 0xFFFFFFFF;

// std.os.windows in Zig 0.16 doesn't expose OVERLAPPED; declare locally.
// The fields are kept as i-typed because we never read them — IOCP uses
// the OVERLAPPED pointer purely as an identity token attached to the
// completion. The kernel writes Internal/InternalHigh; we only consume
// `key` from `GetQueuedCompletionStatus`.
const OVERLAPPED = extern struct {
    Internal: ULONG_PTR,
    InternalHigh: ULONG_PTR,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct { Offset: DWORD, OffsetHigh: DWORD },
        Pointer: ?*anyopaque,
    },
    hEvent: ?HANDLE,
};

// Win32 imports — std.os.windows didn't expose these in 0.16, so
// declare them locally.
extern "kernel32" fn CreateIoCompletionPort(
    FileHandle: HANDLE,
    ExistingCompletionPort: ?HANDLE,
    CompletionKey: ULONG_PTR,
    NumberOfConcurrentThreads: DWORD,
) callconv(.winapi) ?HANDLE;

extern "kernel32" fn GetQueuedCompletionStatus(
    CompletionPort: HANDLE,
    lpNumberOfBytesTransferred: *DWORD,
    lpCompletionKey: *ULONG_PTR,
    lpOverlapped: *?*OVERLAPPED,
    dwMilliseconds: DWORD,
) callconv(.winapi) std.os.windows.BOOL;

extern "kernel32" fn PostQueuedCompletionStatus(
    CompletionPort: HANDLE,
    dwNumberOfBytesTransferred: DWORD,
    dwCompletionKey: ULONG_PTR,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) std.os.windows.BOOL;

extern "kernel32" fn CloseHandle(hObject: HANDLE) callconv(.winapi) std.os.windows.BOOL;

const TICKLE_KEY: ULONG_PTR = 1;

pub const Reactor = struct {
    port: HANDLE,
    pending: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    submit_mutex: Mutex = .{},
    allocator: std.mem.Allocator,

    pub const WaitKey = packed struct(u64) {
        fd: u32,
        kind_tag: u8,
        _pad: u24 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        const port = CreateIoCompletionPort(INVALID_HANDLE_VALUE, null, 0, 0) orelse
            return error.CreateIoCompletionPortFailed;
        return .{
            .port = port,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reactor) void {
        _ = CloseHandle(self.port);
    }

    pub fn pendingCount(self: *const Reactor) usize {
        return self.pending.load(.acquire);
    }

    pub fn tickle(self: *Reactor) void {
        _ = PostQueuedCompletionStatus(self.port, 0, TICKLE_KEY, null);
    }

    /// Associate `fd`'s underlying HANDLE with the completion port,
    /// keyed by `target`. A subsequent overlapped I/O call on the
    /// HANDLE will surface a CQE here.
    ///
    /// NB: the public `volt.io.read/write` paths haven't yet been
    /// adapted to issue OVERLAPPED requests on Windows; that's the
    /// second piece of the Windows port. For now this function
    /// exists so the reactor interface is uniform across backends.
    pub fn registerWait(
        self: *Reactor,
        fd: std.posix.fd_t,
        kind: EventKind,
        target: *anyopaque,
    ) !void {
        _ = kind;
        // For Windows, the same HANDLE associated once with the port
        // serves all subsequent overlapped I/Os on it. We re-associate
        // every registerWait — kernel handles the dedup as a no-op.
        // posix.fd_t on Windows IS HANDLE (*anyopaque) — pass directly.
        const associated = CreateIoCompletionPort(fd, self.port, @intFromPtr(target), 0);
        if (associated == null) return error.AssociateFailed;
        _ = self.pending.fetchAdd(1, .release);
    }

    /// Schedule a one-shot timer fire after `duration_ns`. Will use a
    /// `CreateTimerQueueTimer` callback that PostQueuedCompletionStatus's
    /// to the port. A future revision can reuse a per-reactor
    /// threadpool timer for efficiency.
    pub fn registerTimer(
        self: *Reactor,
        duration_ns: u64,
        target: *anyopaque,
    ) !u64 {
        // Stub: real implementation needs CreateTimerQueueTimer + a
        // callback that PostQueuedCompletionStatus's. The skeleton is
        // here for interface uniformity; the runtime panics if a
        // user actually calls volt.sleep on Windows today.
        _ = self;
        _ = duration_ns;
        _ = target;
        return error.WindowsTimerNotImplemented;
    }

    pub fn unregisterTimer(self: *Reactor, id: u64) void {
        _ = self;
        _ = id;
        // No-op stub — registerTimer never succeeds today.
    }

    pub fn unregisterWait(self: *Reactor, fd: std.posix.fd_t, kind: EventKind) void {
        _ = self;
        _ = fd;
        _ = kind;
        // IOCP doesn't directly support disassociating a handle from
        // the port — the canonical cancel is CancelIoEx on the
        // outstanding overlapped op. Lands with the Windows runtime.
    }

    pub fn poll(
        self: *Reactor,
        timeout_ns: ?u64,
        wake_ctx: *anyopaque,
        wakeFn: *const fn (*anyopaque, *anyopaque) anyerror!void,
    ) !usize {
        const timeout_ms: DWORD = if (timeout_ns) |ns|
            @intCast(@min(@divTrunc(ns, std.time.ns_per_ms), std.math.maxInt(DWORD) - 1))
        else
            INFINITE;

        var transferred: DWORD = 0;
        var key: ULONG_PTR = 0;
        var overlapped: ?*OVERLAPPED = null;
        const ok = GetQueuedCompletionStatus(self.port, &transferred, &key, &overlapped, timeout_ms);
        _ = ok; // rc==0 on timeout or error; we just check if we got a key
        if (key == 0) return 0;
        if (key == TICKLE_KEY) return 0;

        const target: *anyopaque = @ptrFromInt(key);
        _ = self.pending.fetchSub(1, .release);
        try wakeFn(wake_ctx, target);
        return 1;
    }
};

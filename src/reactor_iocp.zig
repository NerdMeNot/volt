//! Windows IOCP reactor — **PollDesc-integrated, IOCP-as-readiness**.
//!
//! Step 2e migration: structural parity with `reactor_kqueue.zig`'s
//! Step 2d.2 persistent-registration model. The user-visible wait
//! path runs the `PollDesc` state machine in user space; the
//! IOCP-side machinery is reduced to "submit a zero-byte probe and
//! call `pd.deliverReady(mode)` on completion".
//!
//! ## Why readiness emulation (not native completion)
//!
//! IOCP is fundamentally completion-based — the standard pattern is
//! a real `WSARecv` with the user's buffer, kernel-initiated DMA
//! into it, completion delivers data + length. Volt's `net.zig`
//! is readiness-based across platforms (kqueue, epoll, io_uring's
//! poll mode). To keep one code path, we submit **zero-byte**
//! `WSARecv`/`WSASend` as readiness probes — completion fires when
//! the socket is readable/writable, the parked coroutine then
//! retries the real syscall through the existing non-blocking
//! path. Go's `runtime/netpoll_windows.go` ships this design.
//!
//! Trade-off: extra `recv`/`send` syscall per real I/O. Native
//! completion mode could be layered on later without disrupting
//! the readiness path.
//!
//! ## Mapping to Volt's PollDesc state machine
//!
//! Each registered fd has a heap-allocated `WinPdBackend` (stored
//! in `pd.backend_data`) holding two `PollOp`s — one each for
//! `.read` / `.write`. A `PollOp` is an `extern struct` with
//! `OVERLAPPED` at offset 0 (the standard Win32 "extend OVERLAPPED"
//! pattern, exactly Go's `pollOperation` shape — see
//! `runtime/netpoll_windows.go:58-66`):
//!
//! ```
//! PollOp = { overlapped: OVERLAPPED, pd: *PollDesc, mode: Mode }
//! ```
//!
//! The OVERLAPPED at offset 0 means the kernel's `lpOverlapped`
//! pointer on completion casts directly to `*PollOp`. We then read
//! `op.pd` and `op.mode` to drive the state machine.
//!
//! ### Why per-PD (not per-wait) OVERLAPPED
//!
//! Volt's `pd.wait` can fast-path (consume a cached `PD_READY`)
//! without parking. If the OVERLAPPED were stack-allocated on the
//! waiter's coroutine stack and the wait fast-pathed while a
//! previous zero-byte probe was still in flight, the kernel would
//! write to memory that the next stack frame may overwrite — UAF.
//! Tying lifetime to the PD (refcount-managed via `pd.incref` /
//! `closeAndWait`) avoids this entirely. Same memory-safety
//! discipline as the `in_poll` barrier on `unregisterFd`.
//!
//! ## Wake/dispatch flow
//!
//! 1. `waitFd(fd, pd, mode)` → `ensureArmed(pd, mode)` CASes the
//!    direction's `armed` flag and submits a zero-byte probe if it
//!    wins. Then `pd.wait(mode)` runs the state machine — may
//!    fast-path on a cached READY or park.
//! 2. Kernel posts completion to the IOCP when ready.
//! 3. `poll()` drains via `GetQueuedCompletionStatusEx`, casts
//!    `lpOverlapped` to `*PollOp`, clears `armed`, calls
//!    `pd.deliverReady(mode)` → pops parked coroutine (if any) →
//!    `runtime.unpark`.
//! 4. Timers + interrupt continue to ride the legacy
//!    `lpOverlapped == null` channel (coroutine ptr in
//!    `CompletionKey` for timers, `INTERRUPT_KEY` sentinel for
//!    interrupts). The poll loop's first dispatch check
//!    distinguishes timer/interrupt from I/O completions before
//!    casting to `*PollOp`.
//!
//! `AcceptEx` / `ConnectEx` completions keep their existing
//! pattern (coroutine ptr in `OVERLAPPED.DUMMYUNIONNAME.Pointer`)
//! because they're inherently completion-based, not readiness.
//! The poll loop distinguishes them from `PollOp` by checking
//! `OVERLAPPED.Pointer != null`: PollOp leaves it null (pd is in
//! the separate field), Accept/Connect set it to the coroutine.
//!
//! ## Lifecycle safety (`in_poll` barrier)
//!
//! Mirrors `reactor_kqueue.zig:96` exactly: `unregisterFd` issues
//! `CancelIoEx(handle, NULL)` to abort all in-flight ops on the
//! fd, then `interrupt()` + spin until `in_poll == false` so any
//! poller mid-dispatch with a stale `OVERLAPPED` entry has
//! finished reading the now-defunct memory before we free the
//! `WinPdBackend`.
//!
//! ## Runtime verification status
//!
//! Volt's primary dev platform is Darwin; this implementation is
//! validated by the cross-compile gate
//! (`zig build-lib -target x86_64-windows-gnu -lc -fno-emit-bin`).
//! Production-grade Windows support requires runtime validation
//! on a Windows host (CI runner or VM) with the full bench gate +
//! stress test. Anyone with a Windows dev environment is welcome
//! to contribute the runtime validation pass.

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const cancel_mod = @import("cancel.zig");
const poll_desc = @import("poll_desc.zig");

const win = std.os.windows;
const ws2_32 = win.ws2_32;

const COMPLETION_BATCH: usize = 32;

// ─── Win32 / Winsock type bindings ───────────────────────────────
//
// Zig 0.16's `std.os.windows` exposes the basic integer / handle
// aliases (HANDLE, DWORD, ULONG, BOOL, FILETIME) but not the
// I/O-shaped types we need: OVERLAPPED, OVERLAPPED_ENTRY, WSABUF,
// SOCKET. Declare them locally with the Win32 layout. Names and
// field shapes match the Win32 SDK one-to-one.

const SOCKET = usize; // UINT_PTR in winsock2.h
const INFINITE: win.DWORD = 0xFFFFFFFF;

const OVERLAPPED = extern struct {
    Internal: win.ULONG_PTR,
    InternalHigh: win.ULONG_PTR,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct {
            Offset: win.DWORD,
            OffsetHigh: win.DWORD,
        },
        Pointer: ?*anyopaque,
    },
    hEvent: ?win.HANDLE,
};

const WSABUF = extern struct {
    len: win.ULONG,
    buf: [*]u8,
};

// ─── Win32 / Winsock extern bindings ─────────────────────────────
//
// `std.os.windows.kernel32` doesn't expose IOCP or threadpool timer
// functions; `ws2_32.zig` exposes the constants but not the
// overlapped I/O functions. Declare them directly.

// IOCP
extern "kernel32" fn CreateIoCompletionPort(
    FileHandle: win.HANDLE,
    ExistingCompletionPort: ?win.HANDLE,
    CompletionKey: usize,
    NumberOfConcurrentThreads: win.DWORD,
) callconv(.winapi) ?win.HANDLE;

const OVERLAPPED_ENTRY = extern struct {
    lpCompletionKey: usize,
    lpOverlapped: ?*OVERLAPPED,
    Internal: usize,
    dwNumberOfBytesTransferred: win.DWORD,
};

extern "kernel32" fn GetQueuedCompletionStatusEx(
    CompletionPort: win.HANDLE,
    lpCompletionPortEntries: [*]OVERLAPPED_ENTRY,
    ulCount: win.ULONG,
    ulNumEntriesRemoved: *win.ULONG,
    dwMilliseconds: win.DWORD,
    fAlertable: win.BOOL,
) callconv(.winapi) win.BOOL;

extern "kernel32" fn PostQueuedCompletionStatus(
    CompletionPort: win.HANDLE,
    dwNumberOfBytesTransferred: win.DWORD,
    dwCompletionKey: usize,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) win.BOOL;

extern "kernel32" fn CloseHandle(hObject: win.HANDLE) callconv(.winapi) win.BOOL;
extern "ws2_32" fn closesocket(s: SOCKET) callconv(.winapi) c_int;
extern "kernel32" fn GetLastError() callconv(.winapi) win.DWORD;
extern "kernel32" fn CancelIoEx(
    hFile: win.HANDLE,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) win.BOOL;

// Threadpool timer
const TP_TIMER = opaque {};
const TP_CALLBACK_INSTANCE = opaque {};
const TP_CALLBACK_ENVIRON = opaque {};
const PTP_TIMER_CALLBACK = *const fn (
    Instance: ?*TP_CALLBACK_INSTANCE,
    Context: ?*anyopaque,
    Timer: *TP_TIMER,
) callconv(.winapi) void;

extern "kernel32" fn CreateThreadpoolTimer(
    pfnti: PTP_TIMER_CALLBACK,
    pv: ?*anyopaque,
    pcbe: ?*TP_CALLBACK_ENVIRON,
) callconv(.winapi) ?*TP_TIMER;

extern "kernel32" fn SetThreadpoolTimer(
    pti: *TP_TIMER,
    pftDueTime: ?*const win.FILETIME,
    msPeriod: win.DWORD,
    msWindowLength: win.DWORD,
) callconv(.winapi) void;

extern "kernel32" fn WaitForThreadpoolTimerCallbacks(
    pti: *TP_TIMER,
    fCancelPendingCallbacks: win.BOOL,
) callconv(.winapi) void;

extern "kernel32" fn CloseThreadpoolTimer(pti: *TP_TIMER) callconv(.winapi) void;

// Winsock
extern "ws2_32" fn WSAStartup(wVersionRequested: u16, lpWSAData: *anyopaque) callconv(.winapi) c_int;
extern "ws2_32" fn WSACleanup() callconv(.winapi) c_int;
extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
extern "ws2_32" fn WSARecv(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: win.DWORD,
    lpNumberOfBytesRecvd: ?*win.DWORD,
    lpFlags: *win.DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*const anyopaque,
) callconv(.winapi) c_int;
extern "ws2_32" fn WSASend(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: win.DWORD,
    lpNumberOfBytesSent: ?*win.DWORD,
    dwFlags: win.DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*const anyopaque,
) callconv(.winapi) c_int;

// `WSAIoctl` is used to fetch the runtime function pointers for
// `AcceptEx` / `ConnectEx` (Winsock extension funcs aren't real DLL
// exports — they're loaded via `SIO_GET_EXTENSION_FUNCTION_POINTER`
// off any open socket).
extern "ws2_32" fn WSAIoctl(
    s: SOCKET,
    dwIoControlCode: win.DWORD,
    lpvInBuffer: ?*const anyopaque,
    cbInBuffer: win.DWORD,
    lpvOutBuffer: ?*anyopaque,
    cbOutBuffer: win.DWORD,
    lpcbBytesReturned: *win.DWORD,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*const anyopaque,
) callconv(.winapi) c_int;

// _WSAIORW(IOC_WS2, 6) = 0x80000000 | 0x40000000 | 0x08000000 | 6.
const SIO_GET_EXTENSION_FUNCTION_POINTER: win.DWORD = 0xC8000006;

const GUID = extern struct {
    Data1: u32,
    Data2: u16,
    Data3: u16,
    Data4: [8]u8,
};

// Canonical Winsock-extension GUIDs (see mswsock.h).
const WSAID_ACCEPTEX = GUID{
    .Data1 = 0xB5367DF1,
    .Data2 = 0xCBAC,
    .Data3 = 0x11CF,
    .Data4 = .{ 0x95, 0xCA, 0x00, 0x80, 0x5F, 0x48, 0xA1, 0x92 },
};
const WSAID_CONNECTEX = GUID{
    .Data1 = 0x25A207B9,
    .Data2 = 0xDDF3,
    .Data3 = 0x4660,
    .Data4 = .{ 0x8E, 0xE9, 0x76, 0xE5, 0x8C, 0x74, 0x06, 0x3E },
};

const AcceptExFn = *const fn (
    sListenSocket: SOCKET,
    sAcceptSocket: SOCKET,
    lpOutputBuffer: *anyopaque,
    dwReceiveDataLength: win.DWORD,
    dwLocalAddressLength: win.DWORD,
    dwRemoteAddressLength: win.DWORD,
    lpdwBytesReceived: *win.DWORD,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) win.BOOL;

const ConnectExFn = *const fn (
    s: SOCKET,
    name: *const anyopaque,
    namelen: c_int,
    lpSendBuffer: ?*const anyopaque,
    dwSendDataLength: win.DWORD,
    lpdwBytesSent: *win.DWORD,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) win.BOOL;

// WSADATA layout: 400 bytes is the documented max; we don't care
// about contents.
const WSA_DATA_BYTES: usize = 400;

const WSA_IO_PENDING: c_int = 997;

// Sentinel `CompletionKey` for the cross-thread interrupt
// completion. Coroutine pointers are heap-allocated and never null,
// so `0` is a safe sentinel; timer completions and I/O completions
// both carry a real coroutine ptr.
const INTERRUPT_KEY: usize = 0;

// ─── Per-PollDesc backend state ──────────────────────────────────
//
// `PollOp` is the Win32 "extend OVERLAPPED" pattern — OVERLAPPED at
// offset 0 lets the kernel's completion pointer cast directly to
// `*PollOp`. Exactly Go's `pollOperation` shape
// (runtime/netpoll_windows.go:58-66).

const PollOp = extern struct {
    overlapped: OVERLAPPED,
    pd: *poll_desc.PollDesc,
    mode_u32: u32, // 'r' or 'w' — keep as u32 for extern-struct safety
};

/// Per-fd backend state held in `pd.backend_data`. Allocated by
/// `registerFd`, freed by `unregisterFd` after the in_poll barrier
/// drains any in-flight dispatch.
const WinPdBackend = struct {
    read_op: PollOp,
    write_op: PollOp,
    /// "Is there an outstanding zero-byte probe in this direction?"
    /// CAS-flipped 0→1 by the wait path before submitting; reset to
    /// 0 by the poll loop AFTER reading the PollOp but BEFORE
    /// `pd.deliverReady`. The ordering ensures a new wait racing in
    /// after the clear can submit a fresh probe, while a wait racing
    /// in before sees armed=1 and just runs `pd.wait` (the existing
    /// probe will eventually fire).
    read_armed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    write_armed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// The socket handle. Stored so the cancel-aware path can issue
    /// `CancelIoEx(handle, &op.overlapped)` without re-threading the
    /// fd through every call.
    sock: SOCKET,
};

inline fn opFor(b: *WinPdBackend, mode: poll_desc.Mode) *PollOp {
    return if (mode == .read) &b.read_op else &b.write_op;
}

inline fn armedFor(b: *WinPdBackend, mode: poll_desc.Mode) *std.atomic.Value(u32) {
    return if (mode == .read) &b.read_armed else &b.write_armed;
}

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    /// Sentinel `INVALID_HANDLE_VALUE` mirrors kqueue's `kq = -1`
    /// so `Reactor = .{}` works as a struct-level default in
    /// `runtime.zig`. Real value is installed by `init`.
    iocp: win.HANDLE = win.INVALID_HANDLE_VALUE,

    /// In-flight I/O ops. Same role as kqueue's `pending`.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// True while the poller is iterating the events[] buffer
    /// returned by `GetQueuedCompletionStatusEx`. Closers that have
    /// just issued `CancelIoEx` on an fd MUST wait for this to clear
    /// before freeing the backend memory: the kernel may have already
    /// transferred a completion entry into the poller's buffer (a
    /// `*PollOp` pointer pointing into our backend struct). Same
    /// barrier as `reactor_kqueue.zig:95`.
    in_poll: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// `AcceptEx` / `ConnectEx` are dynamically loaded via WSAIoctl
    /// (Winsock extension funcs aren't standard DLL exports). 0 means
    /// "not loaded yet"; first caller loads and publishes via .release.
    /// Idempotent under races — multiple loaders compute the same
    /// pointer and the last store wins.
    acceptex_fn_raw: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    connectex_fn_raw: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    allocator: std.mem.Allocator,

    /// `WSAStartup` is per-process; ensure it's called once across
    /// any number of Runtime instances. Idempotent in spirit;
    /// reference-counted via `WSACleanup` symmetry would be
    /// proper-but-overkill for the runtime's lifetime model.
    var wsa_started: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

    pub fn init(allocator: std.mem.Allocator) !Reactor {
        if (!wsa_started.swap(true, .acq_rel)) {
            var wsa_data: [WSA_DATA_BYTES]u8 align(8) = undefined;
            // MAKEWORD(2, 2) — Winsock 2.2.
            const version: u16 = 0x0202;
            const rc = WSAStartup(version, @ptrCast(&wsa_data));
            if (rc != 0) return error.WSAStartupFailed;
        }

        // CreateIoCompletionPort with FileHandle = INVALID_HANDLE_VALUE
        // creates a new IOCP not yet associated with any handle.
        // 0 NumberOfConcurrentThreads = let the system pick (= NumCPU).
        const iocp = CreateIoCompletionPort(
            win.INVALID_HANDLE_VALUE,
            null,
            0,
            0,
        ) orelse return error.IocpCreateFailed;
        return .{ .iocp = iocp, .allocator = allocator };
    }

    /// Close a socket. In the PollDesc-integrated model PD lifecycle
    /// (evict + closeAndWait + unregisterFd + free) is owned by the
    /// socket type via `pd_handle.release`, called BEFORE this fn.
    /// All this needs to do is the libc close — same as kqueue.
    pub fn closeFd(self: *Reactor, fd: i32) void {
        _ = self;
        _ = closesocket(@intCast(fd));
    }

    pub fn deinit(self: *Reactor) void {
        // In-flight IOCP ops at deinit would post completions to
        // a closed port (kernel: silently dropped or worse). The
        // caller's shutdown sequence must drain all parked
        // coroutines first — the assertion fails loudly in debug
        // if it didn't.
        std.debug.assert(self.pending.load(.acquire) == 0);
        std.debug.assert(!self.in_poll.load(.acquire));
        _ = CloseHandle(self.iocp);
        // WSACleanup is intentionally not called; matches the
        // single-Runtime-per-process common case. A future
        // multi-Runtime reference-counted teardown can revisit.
    }

    /// Associate a socket with this reactor's IOCP. Called by
    /// `net.zig` after each socket creation (or on first use, for
    /// listener sockets that may be constructed pre-`rt.run`). The
    /// CompletionKey is the socket handle itself; the per-op
    /// coroutine pointer goes in `OVERLAPPED.DUMMYUNIONNAME.Pointer`
    /// (see the submit paths — `hEvent` can't be reused because
    /// mswsock validates it as a real Win32 event handle).
    pub fn associate(self: *Reactor, sock_handle: usize) !void {
        const handle: win.HANDLE = @ptrFromInt(sock_handle);
        const ret = CreateIoCompletionPort(handle, self.iocp, sock_handle, 0);
        if (ret == null) return error.IocpAssociateFailed;
    }

    // ─── PollDesc-aware interface ──────────────────────────────────

    /// Register a per-fd PollDesc. Allocates the backend state and
    /// installs it in `pd.backend_data`. The fd must already be
    /// IOCP-associated (callers do this via `associate` at accept /
    /// connect time — see `net.zig`'s `acceptWindows` and
    /// `connectWindows`).
    ///
    /// Unlike kqueue, no kernel registration happens here — IOCP
    /// arming is per-submission (a zero-byte WSARecv/WSASend), not
    /// per-fd. `pending` is bumped per probe, not per register.
    pub fn registerFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) ReactorWaitError!void {
        const backend = self.allocator.create(WinPdBackend) catch
            return error.SystemResources;
        backend.* = .{
            .sock = @intCast(fd),
            .read_op = .{
                .overlapped = std.mem.zeroes(OVERLAPPED),
                .pd = pd,
                .mode_u32 = @intFromEnum(poll_desc.Mode.read),
            },
            .write_op = .{
                .overlapped = std.mem.zeroes(OVERLAPPED),
                .pd = pd,
                .mode_u32 = @intFromEnum(poll_desc.Mode.write),
            },
        };
        pd.backend_data = backend;
    }

    /// Reverse of `registerFd`. Cancels any in-flight probes, waits
    /// for the poll loop to drain entries that may reference our
    /// backend, then frees it.
    ///
    /// `CancelIoEx(handle, NULL)` aborts all overlapped ops on the
    /// handle — the kernel posts `STATUS_CANCELLED` completions to
    /// our IOCP for each. The poll loop dispatches them via the
    /// PollOp pointer, which drops back to `pd.deliverReady` (the
    /// caller's `pd_handle.release` has already evicted, so any
    /// waiter sees `.closing` and exits).
    ///
    /// The `interrupt()` + spin-until-`in_poll == false` is the same
    /// barrier as `reactor_kqueue.zig:96` — events the kernel has
    /// already transferred into a poller's user-space buffer must
    /// be drained before we free the backend they point into.
    pub fn unregisterFd(self: *Reactor, fd: i32, pd: *poll_desc.PollDesc) void {
        const backend_ptr = pd.backend_data orelse return;
        const backend: *WinPdBackend = @ptrCast(@alignCast(backend_ptr));

        // Cancel any in-flight zero-byte probes. The kernel posts a
        // STATUS_CANCELLED completion for each to the IOCP. ERROR_NOT_FOUND
        // (no in-flight ops) is the common case — silently ignored.
        const handle: win.HANDLE = @ptrFromInt(@as(usize, @intCast(fd)));
        _ = CancelIoEx(handle, null);

        // Drain the cancellation completions before freeing the
        // backend. `pd_handle.release` has already run `evict` +
        // `closeAndWait`, so the PD's refcount is zero and no
        // waiter can call `ensureArmed` to submit a new probe.
        // What's left: the cancellation completions queued by
        // the kernel, which the poll loop will process by reading
        // `*PollOp` from `lpOverlapped` (pointing into our backend).
        //
        // Each loop iteration:
        //   1. `poll(false)` — pull any visible completions into
        //      our thread and dispatch (clears armed → no-op
        //      deliverReady since slot is PD_NIL post-evict).
        //   2. `interrupt()` + spin until `in_poll == false` —
        //      catches a concurrent worker that pulled a completion
        //      for our backend into its `entries[]` buffer before
        //      we did. The barrier guarantees that worker has
        //      finished dispatching before we free.
        //   3. If both armed flags are still 0 after the in_poll
        //      barrier clears, we're done — no more completions
        //      can reference our backend.
        //
        // Bounded by 10,000 iterations as a safety net; in practice
        // STATUS_CANCELLED arrives within a handful of polls.
        var spins: u32 = 0;
        while (spins < 10_000) : (spins += 1) {
            _ = self.poll(false);
            self.interrupt();
            while (self.in_poll.load(.acquire)) std.atomic.spinLoopHint();
            if (armedFor(backend, .read).load(.acquire) == 0 and
                armedFor(backend, .write).load(.acquire) == 0)
            {
                break;
            }
            std.atomic.spinLoopHint();
        }

        pd.backend_data = null;
        self.allocator.destroy(backend);
    }

    /// Park the current coroutine until `fd` is ready in `mode`.
    /// Runs the `pd.wait` state machine after ensuring a zero-byte
    /// readiness probe is in flight.
    pub fn waitFd(
        self: *Reactor,
        fd: i32,
        pd: *poll_desc.PollDesc,
        mode: poll_desc.Mode,
    ) ReactorWaitError!void {
        _ = fd;
        pd.incref();
        defer pd.decref();
        try self.ensureArmed(pd, mode);
        return switch (pd.wait(mode)) {
            .ready => {},
            .closing => error.BadDescriptor,
        };
    }

    /// Cancel-aware variant of `waitFd`.
    pub fn waitFdCancel(
        self: *Reactor,
        fd: i32,
        pd: *poll_desc.PollDesc,
        mode: poll_desc.Mode,
        c: *cancel_mod.Cancel,
    ) (ReactorWaitError || cancel_mod.Error)!void {
        _ = fd;
        try self.ensureArmed(pd, mode);
        const result = try pd.waitCancel(mode, c);
        return switch (result) {
            .ready => {},
            .closing => error.BadDescriptor,
        };
    }

    /// Submit a zero-byte WSARecv/WSASend probe if none is in flight
    /// for `mode`. CAS-flips `armed[mode]` 0→1; the poll loop CAS-
    /// flips it back to 0 on completion (BEFORE calling deliverReady,
    /// so a waiter racing in after sees armed=0 and submits fresh).
    ///
    /// ## UDP safety: `MSG_PEEK` on read probes
    ///
    /// Without `MSG_PEEK`, a zero-byte WSARecv on a UDP socket with
    /// a pending datagram CONSUMES the datagram (truncated to 0
    /// bytes), and the parked waiter resumes only to find the
    /// real `recvFrom` blocking on an empty queue — hang. Setting
    /// `MSG_PEEK` makes the kernel report readiness without
    /// removing the datagram. On TCP, `MSG_PEEK` + 0-byte buffer
    /// is harmless (nothing to consume either way). Mirrors Go's
    /// `internal/poll/fd_windows.go:1276-1283` `RawRead`.
    ///
    /// `WSAEMSGSIZE` from the peek means "data is there, didn't
    /// fit in 0 bytes" — exactly the readiness signal we want.
    /// Treat it as immediate-success: deliver the ready signal
    /// inline (no completion is posted for sync errors), undo
    /// the `pending` bump.
    fn ensureArmed(self: *Reactor, pd: *poll_desc.PollDesc, mode: poll_desc.Mode) ReactorWaitError!void {
        const backend: *WinPdBackend = @ptrCast(@alignCast(pd.backend_data.?));
        const armed = armedFor(backend, mode);
        if (armed.cmpxchgStrong(0, 1, .acq_rel, .acquire)) |_| {
            // Already armed — another wait submitted; we'll catch
            // the completion via `pd.wait`'s state machine.
            return;
        }

        const op = opFor(backend, mode);
        op.overlapped = std.mem.zeroes(OVERLAPPED);

        var dummy_buf: [1]u8 = .{0};
        var wsabuf = WSABUF{ .len = 0, .buf = &dummy_buf };
        var bytes_transferred: win.DWORD = 0;
        // For read probes, set `MSG_PEEK` so UDP datagrams aren't
        // consumed by the zero-byte recv. See the function-level
        // doc for the full rationale.
        var flags: win.DWORD = if (mode == .read) MSG_PEEK else 0;

        // Bump `pending` BEFORE submit. A peer worker observing the
        // reactor between our WSARecv and a post-submit fetchAdd
        // would see pendingCount == 0, decide the reactor is idle,
        // and park. The IOCP poller is the only path that can
        // dequeue the completion we're about to cause; if no
        // worker is in `poll(true)`, the completion sits unread
        // and the parked waiter hangs.
        _ = self.pending.fetchAdd(1, .acq_rel);

        const rc = switch (mode) {
            .read => WSARecv(backend.sock, @ptrCast(&wsabuf), 1, &bytes_transferred, &flags, &op.overlapped, null),
            .write => WSASend(backend.sock, @ptrCast(&wsabuf), 1, &bytes_transferred, 0, &op.overlapped, null),
        };
        if (rc != 0) {
            const e = WSAGetLastError();
            if (e == WSAEMSGSIZE) {
                // The peek found a datagram. Data is buffered for
                // the next real recvFrom. No async completion will
                // fire (this is a synchronous error, not an
                // overlapped initiation), so we deliver the ready
                // signal inline. `pd.deliverReady` is idempotent —
                // if a waiter is already parked it gets popped;
                // otherwise PD_READY is cached for the next wait.
                _ = self.pending.fetchSub(1, .acq_rel);
                armed.store(0, .release);
                if (pd.deliverReady(mode)) |c| runtime.unpark(c);
                return;
            }
            if (e != WSA_IO_PENDING) {
                // Submit failed synchronously — no completion will
                // fire. Undo the pending bump and clear armed so
                // the next wait can retry.
                _ = self.pending.fetchSub(1, .acq_rel);
                armed.store(0, .release);
                return wsaToSetupError(e);
            }
        }
    }

    fn loadExtensionFn(self: *Reactor, sock: SOCKET, guid: *const GUID, cache: *std.atomic.Value(usize)) !usize {
        const cached = cache.load(.acquire);
        if (cached != 0) return cached;
        var fn_ptr: usize = 0;
        var bytes_returned: win.DWORD = 0;
        const rc = WSAIoctl(
            sock,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            @ptrCast(guid),
            @sizeOf(GUID),
            @ptrCast(&fn_ptr),
            @sizeOf(usize),
            &bytes_returned,
            null,
            null,
        );
        if (rc != 0 or fn_ptr == 0) return error.LoadWinsockExtFailed;
        cache.store(fn_ptr, .release);
        _ = self;
        return fn_ptr;
    }

    /// Submit `AcceptEx` on `listen_sock`, parking the current
    /// coroutine until the connection completes.
    ///
    /// `accept_sock` must be a pre-created socket (AcceptEx fills
    /// it). `out_buf` is the addresses output buffer — minimum size
    /// is `2 * (sizeof(sockaddr_in) + 16)`; caller stack-allocates.
    /// After resume, the caller must `setsockopt(SO_UPDATE_ACCEPT_CONTEXT)`
    /// to finalise the inheritance from `listen_sock`.
    pub fn submitAcceptEx(self: *Reactor, listen_sock: SOCKET, accept_sock: SOCKET, out_buf: []u8, addr_len: win.DWORD) !void {
        const raw = try self.loadExtensionFn(listen_sock, &WSAID_ACCEPTEX, &self.acceptex_fn_raw);
        const accept_ex: AcceptExFn = @ptrFromInt(raw);

        const me = current.require();
        // Stack-allocate OVERLAPPED. The kernel writes to this
        // address up until the completion fires; safe because Volt
        // stacks are mmap'd and stable across the suspend (see
        // `src/stack.zig`). A future scheme that ever copies or
        // moves coroutine stacks across suspends MUST switch IOCP
        // submit paths to a coroutine-pool-backed allocation.
        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };
        var bytes_received: win.DWORD = 0;
        const rc = accept_ex(
            listen_sock,
            accept_sock,
            out_buf.ptr,
            0, // dwReceiveDataLength = 0 — don't want any initial-recv coupling.
            addr_len,
            addr_len,
            &bytes_received,
            &ovl,
        );
        // BOOL TRUE = synchronous completion (still posts to IOCP by
        // default — we park, the IOCP poll wakes us). BOOL FALSE +
        // GLE=WSA_IO_PENDING is the normal async case. Any other
        // FALSE is a real submission failure.
        if (rc == win.BOOL.FALSE) {
            const e = WSAGetLastError();
            if (e != WSA_IO_PENDING) return error.AcceptExFailed;
        }
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    /// Cancel-aware variant of `submitAcceptEx`. Same `CancelIoEx`
    /// mechanism as `submitAcceptExCancel`: the kernel posts a
    /// `STATUS_CANCELLED` completion for the cancelled OVERLAPPED,
    /// the poll loop unparks via the normal path, and the caller
    /// observes `error.Cancelled` because `Cancel.isFired()` is true.
    pub fn submitAcceptExCancel(
        self: *Reactor,
        listen_sock: SOCKET,
        accept_sock: SOCKET,
        out_buf: []u8,
        addr_len: win.DWORD,
        c: *cancel_mod.Cancel,
    ) (ReactorWaitError || cancel_mod.Error)!void {
        const raw = try self.loadExtensionFn(listen_sock, &WSAID_ACCEPTEX, &self.acceptex_fn_raw);
        const accept_ex: AcceptExFn = @ptrFromInt(raw);

        const me = current.require();
        try c.checkpoint();

        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };
        var bytes_received: win.DWORD = 0;
        const rc = accept_ex(
            listen_sock,
            accept_sock,
            out_buf.ptr,
            0,
            addr_len,
            addr_len,
            &bytes_received,
            &ovl,
        );
        if (rc == win.BOOL.FALSE) {
            const e = WSAGetLastError();
            if (e != WSA_IO_PENDING) return wsaToSetupError(e);
        }

        // CancelIoEx targets the OVERLAPPED on the listener handle —
        // AcceptEx is bound to listen_sock, not the pre-created
        // accept_sock.
        var op = WaitOp{ .io = .{ .sock = listen_sock, .overlapped = &ovl } };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        var w = cancel_mod.Waiter{};
        const already_fired = c.registerReactor(&w, me);
        defer c.deregister(&w);
        if (already_fired) {
            _ = CancelIoEx(@ptrFromInt(listen_sock), &ovl);
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Submit `ConnectEx` on `sock`, parking the current coroutine
    /// until the connection completes.
    ///
    /// `sock` must already be bound (ConnectEx requires this — bind
    /// to `0.0.0.0:0` works). After resume, the caller must
    /// `setsockopt(SO_UPDATE_CONNECT_CONTEXT)` so the socket is
    /// fully usable (getpeername etc).
    pub fn submitConnectEx(self: *Reactor, sock: SOCKET, sa: *const anyopaque, sa_len: c_int) !void {
        const raw = try self.loadExtensionFn(sock, &WSAID_CONNECTEX, &self.connectex_fn_raw);
        const connect_ex: ConnectExFn = @ptrFromInt(raw);

        const me = current.require();
        // Stack-allocate OVERLAPPED. The kernel writes to this
        // address up until the completion fires; safe because Volt
        // stacks are mmap'd and stable across the suspend (see
        // `src/stack.zig`). A future scheme that ever copies or
        // moves coroutine stacks across suspends MUST switch IOCP
        // submit paths to a coroutine-pool-backed allocation.
        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };
        var bytes_sent: win.DWORD = 0;
        const rc = connect_ex(
            sock,
            sa,
            sa_len,
            null, // lpSendBuffer — no initial data
            0,
            &bytes_sent,
            &ovl,
        );
        if (rc == win.BOOL.FALSE) {
            const e = WSAGetLastError();
            if (e != WSA_IO_PENDING) return error.ConnectExFailed;
        }
        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
    }

    /// Cancel-aware variant of `submitConnectEx`. Same mechanism as
    /// `submitAcceptExCancel`: `CancelIoEx` against the connecting
    /// socket's OVERLAPPED, completion path unparks via the normal
    /// route.
    pub fn submitConnectExCancel(
        self: *Reactor,
        sock: SOCKET,
        sa: *const anyopaque,
        sa_len: c_int,
        c: *cancel_mod.Cancel,
    ) (ReactorWaitError || cancel_mod.Error)!void {
        const raw = try self.loadExtensionFn(sock, &WSAID_CONNECTEX, &self.connectex_fn_raw);
        const connect_ex: ConnectExFn = @ptrFromInt(raw);

        const me = current.require();
        try c.checkpoint();

        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };
        var bytes_sent: win.DWORD = 0;
        const rc = connect_ex(
            sock,
            sa,
            sa_len,
            null,
            0,
            &bytes_sent,
            &ovl,
        );
        if (rc == win.BOOL.FALSE) {
            const e = WSAGetLastError();
            if (e != WSA_IO_PENDING) return wsaToSetupError(e);
        }

        var op = WaitOp{ .io = .{ .sock = sock, .overlapped = &ovl } };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        var w = cancel_mod.Waiter{};
        const already_fired = c.registerReactor(&w, me);
        defer c.deregister(&w);
        if (already_fired) {
            _ = CancelIoEx(@ptrFromInt(sock), &ovl);
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    /// Timer callback context — the threadpool fires this on a pool
    /// thread; we post a completion to the IOCP keyed on the
    /// coroutine pointer.
    const TimerCtx = struct {
        reactor: *Reactor,
        coro: *coroutine.Coroutine,
        timer: ?*TP_TIMER = null,
    };

    fn timerCallback(
        _: ?*TP_CALLBACK_INSTANCE,
        ctx_ptr: ?*anyopaque,
        _: *TP_TIMER,
    ) callconv(.winapi) void {
        const ctx: *TimerCtx = @ptrCast(@alignCast(ctx_ptr.?));
        // Post a completion. The IOCP poll loop will see this with
        // CompletionKey = coroutine pointer (cast to usize) and
        // unpark.
        _ = PostQueuedCompletionStatus(
            ctx.reactor.iocp,
            0, // bytes transferred — unused for timer wakes
            @intFromPtr(ctx.coro),
            null, // no OVERLAPPED — distinguishes timer from I/O completion
        );
    }

    pub fn waitTimer(self: *Reactor, ns: u64) ReactorWaitError!void {
        // ns=0 mirrors POSIX backends — the contract is the same on
        // every reactor. SetThreadpoolTimer with an expired due time
        // is ambiguous: the timer can fire before the park completes,
        // or be coalesced by the threadpool. Short-circuit at the
        // entry to match Darwin/Linux semantics.
        if (ns == 0) return;
        // ticks_100ns is `i64`; even `ns / 100` could overflow if ns is
        // above i64_max. Match POSIX backends' bound for consistency.
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        const me = current.require();

        // Per-sleep allocation. A future per-P pool of pre-created
        // threadpool timers would amortise the
        // CreateThreadpoolTimer cost.
        var ctx = TimerCtx{ .reactor = self, .coro = me };
        const timer = CreateThreadpoolTimer(&timerCallback, &ctx, null) orelse
            return error.SystemResources;
        ctx.timer = timer;

        // Windows FILETIME is in 100ns units, negative for relative
        // due time. ns / 100 = 100ns ticks.
        const ticks_100ns: i64 = -@as(i64, @intCast(ns / 100));
        const due_time = win.FILETIME{
            .dwLowDateTime = @intCast(ticks_100ns & 0xFFFFFFFF),
            .dwHighDateTime = @intCast((ticks_100ns >> 32) & 0xFFFFFFFF),
        };
        SetThreadpoolTimer(timer, &due_time, 0, 0);

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        // Cleanup. The timer has fired by the time we resume; wait
        // for any in-flight callback to complete, then release.
        WaitForThreadpoolTimerCallbacks(timer, win.BOOL.FALSE);
        CloseThreadpoolTimer(timer);
    }

    /// Cancel-aware variant of `waitTimer`. Stashes the timer
    /// handle in the per-coro `WaitOp.timer` slot so `cancelCoro`
    /// can disarm + post a manual completion if the cancel fires
    /// before the timer.
    pub fn waitTimerCancel(self: *Reactor, ns: u64, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        const me = current.require();
        try c.checkpoint();
        // ns=0 mirrors waitTimer — early return AFTER the checkpoint
        // so a pre-fired cancel still surfaces error.Cancelled.
        if (ns == 0) return;
        if (ns > std.math.maxInt(i64)) return error.TimeoutOutOfRange;

        var ctx = TimerCtx{ .reactor = self, .coro = me };
        const timer = CreateThreadpoolTimer(&timerCallback, &ctx, null) orelse
            return error.SystemResources;
        ctx.timer = timer;

        const ticks_100ns: i64 = -@as(i64, @intCast(ns / 100));
        const due_time = win.FILETIME{
            .dwLowDateTime = @intCast(ticks_100ns & 0xFFFFFFFF),
            .dwHighDateTime = @intCast((ticks_100ns >> 32) & 0xFFFFFFFF),
        };
        SetThreadpoolTimer(timer, &due_time, 0, 0);

        var op = WaitOp{ .timer = .{ .timer = timer } };
        me.reactor_wait_op = &op;
        defer me.reactor_wait_op = null;

        var w = cancel_mod.Waiter{};
        const already_fired = c.registerReactor(&w, me);
        defer c.deregister(&w);
        if (already_fired) {
            // Cancel fired between checkpoint and register — tear
            // down the timer before parking.
            SetThreadpoolTimer(timer, null, 0, 0);
            WaitForThreadpoolTimerCallbacks(timer, win.BOOL.TRUE);
            CloseThreadpoolTimer(timer);
            return error.Cancelled;
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        WaitForThreadpoolTimerCallbacks(timer, win.BOOL.FALSE);
        CloseThreadpoolTimer(timer);

        if (c.isFired()) return error.Cancelled;
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
    }

    /// Per-call stash for cancel-aware waits. A tagged union of
    /// the two cancel mechanisms we use — `CancelIoEx` for socket
    /// I/O and `SetThreadpoolTimer(NULL)` + manual completion-post
    /// for timer waits.
    pub const WaitOp = union(enum) {
        io: struct {
            sock: usize,
            overlapped: *OVERLAPPED,
        },
        timer: struct {
            timer: *TP_TIMER,
        },
    };

    /// Cancel this coro's in-flight IOCP op.
    ///
    /// For I/O: `CancelIoEx` queues a `STATUS_CANCELLED` completion
    /// that the poll path picks up and unparks via the normal route
    /// — no double-unpark because exactly one completion fires per
    /// submitted op.
    ///
    /// For timers: `SetThreadpoolTimer(NULL)` disarms the timer
    /// (preventing the natural fire callback), and we explicitly
    /// `PostQueuedCompletionStatus` with the coro ptr as the
    /// CompletionKey so the poll path sees the same shape it would
    /// from a natural timer fire.
    pub fn cancelCoro(self: *Reactor, c: *coroutine.Coroutine) void {
        const op_ptr = c.reactor_wait_op orelse return;
        const op: *WaitOp = @ptrCast(@alignCast(op_ptr));
        switch (op.*) {
            .io => |io| {
                _ = CancelIoEx(@ptrFromInt(io.sock), io.overlapped);
                // Don't unpark here — the cancellation completion
                // will fire via the IOCP poll loop's normal path.
            },
            .timer => |t| {
                SetThreadpoolTimer(t.timer, null, 0, 0);
                _ = PostQueuedCompletionStatus(
                    self.iocp,
                    0,
                    @intFromPtr(c),
                    null,
                );
            },
        }
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;

        var entries: [COMPLETION_BATCH]OVERLAPPED_ENTRY = undefined;
        var removed: win.ULONG = 0;
        const timeout_ms: win.DWORD = if (blocking) INFINITE else 0;

        // Set `in_poll` BEFORE `GetQueuedCompletionStatusEx`. Same
        // barrier as kqueue: a concurrent `unregisterFd` about to
        // free a backend whose PollOp pointer the kernel will
        // transfer into our `entries[]` MUST observe `in_poll == true`
        // for the entire window the buffer is live. See
        // `reactor_kqueue.zig:396` for the full rationale.
        self.in_poll.store(true, .release);
        defer self.in_poll.store(false, .release);

        const ok = GetQueuedCompletionStatusEx(
            self.iocp,
            &entries,
            COMPLETION_BATCH,
            &removed,
            timeout_ms,
            win.BOOL.FALSE, // not alertable
        );
        if (ok == win.BOOL.FALSE) return 0;

        const count: usize = @intCast(removed);
        var real_count: usize = 0;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const e = &entries[i];
            // Four completion shapes:
            //   1. Interrupt — lpOverlapped == null, CompletionKey
            //      == INTERRUPT_KEY. Skip without unparking.
            //   2. Timer — lpOverlapped == null, CompletionKey ==
            //      *Coroutine. Unpark directly.
            //   3. AcceptEx/ConnectEx — lpOverlapped != null, AND
            //      OVERLAPPED.Pointer != null (== *Coroutine). The
            //      completion-based path inherits the legacy shape.
            //   4. PollOp readiness probe — lpOverlapped != null AND
            //      OVERLAPPED.Pointer == null (PollOp.overlapped is
            //      zero-initialised by `ensureArmed`). Cast
            //      lpOverlapped to *PollOp, clear armed, deliver
            //      readiness through `pd.deliverReady`.
            if (e.lpOverlapped == null) {
                if (e.lpCompletionKey == INTERRUPT_KEY) continue;
                // Timer completion.
                const coro: *coroutine.Coroutine = @ptrFromInt(e.lpCompletionKey);
                runtime.unpark(coro);
                real_count += 1;
                continue;
            }
            const ovl = e.lpOverlapped.?;
            if (ovl.DUMMYUNIONNAME.Pointer) |coro_raw| {
                // AcceptEx / ConnectEx completion — coroutine ptr in
                // OVERLAPPED.Pointer.
                const coro: *coroutine.Coroutine = @ptrCast(@alignCast(coro_raw));
                runtime.unpark(coro);
                real_count += 1;
                continue;
            }
            // PollOp readiness completion. OVERLAPPED at offset 0 of
            // PollOp means the kernel's `lpOverlapped` pointer is the
            // address of `op.overlapped` — `@fieldParentPtr` recovers
            // the enclosing PollOp.
            const op: *PollOp = @fieldParentPtr("overlapped", ovl);
            const pd = op.pd;
            const mode: poll_desc.Mode = @enumFromInt(op.mode_u32);
            const backend: *WinPdBackend = @ptrCast(@alignCast(pd.backend_data.?));
            // Clear armed BEFORE deliverReady. A wait racing in
            // before this would see armed=1, skip submit, and wait
            // for the in-flight probe (us) — fine. A wait racing in
            // after sees armed=0, submits fresh — also fine. If we
            // cleared armed AFTER deliverReady and a wait sneaked
            // in between deliverReady and the clear, that wait
            // would see armed=1 and skip submit while pd had
            // already cached PD_READY; the wait would fast-path,
            // armed stays at 1, the next wait would deadlock.
            armedFor(backend, mode).store(0, .release);
            if (pd.deliverReady(mode)) |coro| runtime.unpark(coro);
            real_count += 1;
        }
        if (real_count > 0) _ = self.pending.fetchSub(@intCast(real_count), .acq_rel);
        return real_count;
    }

    /// Wake a worker currently blocked in `poll(true)`. Safe to call
    /// from any thread. The posted completion carries the
    /// `INTERRUPT_KEY` sentinel; the poll loop recognises and
    /// discards it without unparking any coroutine.
    pub fn interrupt(self: *Reactor) void {
        _ = PostQueuedCompletionStatus(
            self.iocp,
            0,
            INTERRUPT_KEY,
            null,
        );
    }
};

// ─────────────────────────────────────────────────────────────────────
// Non-blocking IO helpers
// ─────────────────────────────────────────────────────────────────────

// Winsock equivalents of fcntl(O_NONBLOCK) and POSIX read/write.

// _IOW('f', 126, u_long) — 0x8004667e — set non-blocking via FIONBIO.
const FIONBIO: win.LONG = @bitCast(@as(u32, 0x8004667e));

extern "ws2_32" fn ioctlsocket(s: SOCKET, cmd: win.LONG, argp: *win.ULONG) callconv(.winapi) c_int;
// `recv` and `send` are the actual ws2_32.dll symbol names; the
// `send_zig`/`recv_zig` Zig identifiers are aliases via `@extern`
// to avoid colliding with std identifiers. `extern "X" fn name`
// uses the Zig identifier AS the import name, which doesn't match
// the real DLL symbol — link fails at exe time.
const recv_zig = @extern(
    *const fn (SOCKET, [*]u8, c_int, c_int) callconv(.winapi) c_int,
    .{ .name = "recv", .library_name = "ws2_32" },
);
const send_zig = @extern(
    *const fn (SOCKET, [*]const u8, c_int, c_int) callconv(.winapi) c_int,
    .{ .name = "send", .library_name = "ws2_32" },
);

// WSA error codes. Subset we map; the rest fall through to
// `error.Unexpected`.
const WSAEWOULDBLOCK: c_int = 10035;
const WSAECONNRESET: c_int = 10054;
const WSAECONNABORTED: c_int = 10053;
const WSAECONNREFUSED: c_int = 10061;
const WSAENOTCONN: c_int = 10057;
const WSAESHUTDOWN: c_int = 10058;
const WSAETIMEDOUT: c_int = 10060;
const WSAENOTSOCK: c_int = 10038;
const WSAEBADF: c_int = 10009;
const WSAEMFILE: c_int = 10024;
const WSAENOBUFS: c_int = 10055;
const WSAEINTR: c_int = 10004;
/// "Message too large to fit in the supplied buffer; truncated." Expected
/// when our 0-byte readiness probe finds a pending UDP datagram with
/// `MSG_PEEK` set. Treated as success — the data is buffered, ready
/// for the caller's real recvFrom.
const WSAEMSGSIZE: c_int = 10040;

/// `MSG_PEEK` — peek at incoming data without removing it from the
/// socket's input queue. Required on UDP for zero-byte WSARecv
/// readiness probes; without it, a 0-byte recv on a datagram
/// socket CONSUMES the queued datagram (truncated to 0 bytes), so
/// the parked waiter resumes but the real recvFrom finds nothing
/// and blocks forever. See Go's `internal/poll/fd_windows.go:1272-1287`
/// (`RawRead`) for the canonical reference.
const MSG_PEEK: win.DWORD = 0x0002;

const reactor_mod = @import("reactor.zig");
pub const IoError = reactor_mod.IoError;
pub const ReactorSetupError = reactor_mod.ReactorSetupError;
pub const ReactorWaitError = reactor_mod.ReactorWaitError;

/// Translate a WSA error code into the categorical `IoError` set.
/// Unknown values collapse to `error.Unexpected` — callers can
/// still printf the raw code for diagnostics, but the typed error
/// is what downstream libraries match on.
pub fn wsaToIoError(e: c_int) IoError {
    return switch (e) {
        WSAECONNRESET => error.ConnectionReset,
        WSAESHUTDOWN => error.BrokenPipe,
        WSAECONNABORTED => error.ConnectionAborted,
        WSAENOTCONN => error.NotConnected,
        WSAECONNREFUSED => error.ConnectionRefused,
        WSAETIMEDOUT => error.Timeout,
        WSAENOTSOCK, WSAEBADF => error.BadDescriptor,
        WSAEMFILE => error.OutOfDescriptors,
        WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

/// Same mapping projected onto `ReactorSetupError`. Used by
/// register-type calls (`setNonblock`, `associate`, AcceptEx /
/// ConnectEx submit) which can't surface connection-state errors.
pub fn wsaToSetupError(e: c_int) ReactorSetupError {
    return switch (e) {
        WSAEMFILE => error.OutOfDescriptors,
        WSAENOBUFS => error.SystemResources,
        else => error.Unexpected,
    };
}

pub fn setNonblock(fd: i32) ReactorSetupError!void {
    var mode: win.ULONG = 1;
    if (ioctlsocket(@intCast(fd), FIONBIO, &mode) != 0) return wsaToSetupError(WSAGetLastError());
}

pub fn readAsync(rx: *Reactor, fd: i32, pd: *poll_desc.PollDesc, buf: []u8) IoError!usize {
    while (true) {
        const r = recv_zig(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (r >= 0) return @intCast(r);
        const e = WSAGetLastError();
        if (e == WSAEWOULDBLOCK) {
            try rx.waitFd(fd, pd, .read);
            continue;
        }
        if (e == WSAEINTR) continue;
        return wsaToIoError(e);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, pd: *poll_desc.PollDesc, buf: []const u8) IoError!usize {
    while (true) {
        const w = send_zig(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (w >= 0) return @intCast(w);
        const e = WSAGetLastError();
        if (e == WSAEWOULDBLOCK) {
            try rx.waitFd(fd, pd, .write);
            continue;
        }
        if (e == WSAEINTR) continue;
        return wsaToIoError(e);
    }
}

pub fn readFull(rx: *Reactor, fd: i32, pd: *poll_desc.PollDesc, buf: []u8) IoError!usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, pd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

pub fn writeAll(rx: *Reactor, fd: i32, pd: *poll_desc.PollDesc, buf: []const u8) IoError!void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, pd, buf[total..]);
        total += w;
    }
}

// Compile-time check: Windows-only.
comptime {
    if (builtin.os.tag != .windows) {
        @compileError("reactor_iocp.zig is Windows-only; src/reactor.zig dispatches by os.tag");
    }
}

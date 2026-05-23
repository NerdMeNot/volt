//! Windows IOCP reactor — **IOCP polyfilled as readiness**.
//!
//! IOCP is fundamentally completion-based: you submit an I/O
//! operation with a buffer, the kernel completes it asynchronously,
//! and your code reads the result. Volt's `net.zig` is
//! readiness-based — coroutines do their own `read`/`write` after
//! a wake. To bridge, we use Go's `netpoll_windows.go` pattern:
//!
//!   * **Readiness via zero-byte `WSARecv` / `WSASend`.** Submit
//!     a zero-byte I/O op with `WSARecv` (for read-ready) or
//!     `WSASend` (for write-ready). The completion fires when the
//!     socket is readable/writable; the parked coroutine then does
//!     its real read/write via the normal `net.zig` path.
//!   * **Coroutine pointer = `OVERLAPPED.Pointer`.** Each parked
//!     I/O stack-allocates an `OVERLAPPED`. Because Volt's stackful
//!     coroutines have stable mmap'd stacks, the `OVERLAPPED`
//!     pointer is valid until the coroutine resumes — same shape
//!     as kqueue's `udata` or epoll's `data.ptr`.
//!   * **Timers via `CreateThreadpoolTimer`.** The Windows thread
//!     pool fires a callback on a pool thread; the callback
//!     `PostQueuedCompletionStatus`'s a completion to the IOCP
//!     with the coroutine pointer as the completion key. Same
//!     wake path as fd readiness.
//!   * **Batch harvest via `GetQueuedCompletionStatusEx`.** The
//!     plural form (Vista+) drains up to N completions per call;
//!     same shape as kqueue's `kevent`-with-batch or epoll's
//!     `epoll_wait`.
//!
//! ## Trade-offs vs native IOCP
//!
//! The zero-byte polyfill loses IOCP's main perf win — kernel-
//! initiated DMA into application buffers. We pay an extra `recv`
//! / `send` syscall per real I/O op. Justification:
//!
//!   - `net.zig` stays uniform across all platforms (one code path).
//!   - Go shipped this design for 15+ years; it works.
//!   - If perf data ever justifies it, a buffer-ownership Windows
//!     path can be added without disrupting POSIX.
//!
//! ## Runtime verification status
//!
//! Volt's primary dev platform is Darwin; the implementation here
//! is best-effort based on Win32 / Winsock documentation and Go's
//! `netpoll_windows.go` reference. **The cross-compile gate
//! (`zig build-lib -target x86_64-windows-gnu -lc -fno-emit-bin`)
//! passes; runtime behaviour on Windows is unverified at the time
//! of this commit.** Production-grade Windows support requires:
//!
//!   1. A Windows VM or CI runner.
//!   2. Running the full bench gate + stress test.
//!   3. Any bug fixes that surface (especially around `OVERLAPPED`
//!      lifetime, WSAStartup ordering, and the threadpool timer
//!      cleanup path).
//!
//! Anyone with a Windows dev environment is welcome to contribute
//! the runtime validation pass.

const std = @import("std");
const builtin = @import("builtin");
const coroutine = @import("coroutine.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");
const context = @import("context.zig");
const cancel_mod = @import("cancel.zig");

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

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    /// Sentinel `INVALID_HANDLE_VALUE` mirrors kqueue's `kq = -1`
    /// so `Reactor = .{}` works as a struct-level default in
    /// `runtime.zig`. Real value is installed by `init`.
    iocp: win.HANDLE = win.INVALID_HANDLE_VALUE,

    /// In-flight I/O ops. Same role as kqueue's `pending`.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// `AcceptEx` / `ConnectEx` are dynamically loaded via WSAIoctl
    /// (Winsock extension funcs aren't standard DLL exports). 0 means
    /// "not loaded yet"; first caller loads and publishes via .release.
    /// Idempotent under races — multiple loaders compute the same
    /// pointer and the last store wins.
    acceptex_fn_raw: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    connectex_fn_raw: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    /// `WSAStartup` is per-process; ensure it's called once across
    /// any number of Runtime instances. Idempotent in spirit;
    /// reference-counted via `WSACleanup` symmetry would be
    /// proper-but-overkill for the runtime's lifetime model.
    var wsa_started: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

    pub fn init() !Reactor {
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
        return .{ .iocp = iocp };
    }

    pub fn deinit(self: *Reactor) void {
        // In-flight IOCP ops at deinit would post completions to
        // a closed port (kernel: silently dropped or worse). The
        // caller's shutdown sequence must drain all parked
        // coroutines first — the assertion fails loudly in debug
        // if it didn't.
        std.debug.assert(self.pending.load(.acquire) == 0);
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

    pub fn waitReadable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.submitReadiness(@intCast(fd), .read);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) ReactorWaitError!void {
        return self.submitReadiness(@intCast(fd), .write);
    }

    const Direction = enum { read, write };

    fn submitReadiness(self: *Reactor, sock: usize, dir: Direction) ReactorWaitError!void {
        const me = current.require();

        // Stack-allocate OVERLAPPED. Volt's stacks are mmap'd
        // and stable; the kernel can write to this address up
        // until we resume.
        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        // Stash the coroutine pointer in OVERLAPPED.Pointer. The
        // union field is unused for socket I/O (it's only the file
        // seek offset for file I/O). We can't reuse hEvent for
        // this: mswsock's AcceptEx / ConnectEx validate hEvent as
        // a real Win32 event-handle and fail with
        // ERROR_INVALID_HANDLE when it isn't. WSARecv / WSASend
        // happen to be more lenient, but using one consistent slot
        // is cleaner than relying on per-API leniency.
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };

        var dummy_buf: [1]u8 = .{0};
        var wsabuf = WSABUF{
            .len = 0,
            .buf = &dummy_buf,
        };
        var bytes_transferred: win.DWORD = 0;
        var flags: win.DWORD = 0;

        const rc = switch (dir) {
            .read => WSARecv(@intCast(sock), @ptrCast(&wsabuf), 1, &bytes_transferred, &flags, &ovl, null),
            .write => WSASend(@intCast(sock), @ptrCast(&wsabuf), 1, &bytes_transferred, 0, &ovl, null),
        };

        // WSARecv/Send returns 0 on immediate completion (rare for
        // zero-byte readiness probe — only happens if the socket
        // is already in the desired state). Returns SOCKET_ERROR
        // (-1) for both errors and async-in-progress; WSAGetLastError
        // == WSA_IO_PENDING is the normal path.
        if (rc != 0) {
            const e = WSAGetLastError();
            if (e != WSA_IO_PENDING) return wsaToSetupError(e);
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
        // On resume: poll() has unparked us. The OVERLAPPED on our
        // stack is no longer in use by the kernel. Return — caller
        // retries the real read/write.
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
    /// mechanism as `submitReadinessCancel`: the kernel posts a
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

    pub fn waitReadableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.submitReadinessCancel(@intCast(fd), .read, c);
    }

    pub fn waitWritableCancel(self: *Reactor, fd: i32, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        return self.submitReadinessCancel(@intCast(fd), .write, c);
    }

    fn submitReadinessCancel(self: *Reactor, sock: usize, dir: Direction, c: *cancel_mod.Cancel) (ReactorWaitError || cancel_mod.Error)!void {
        const me = current.require();
        try c.checkpoint();

        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        ovl.DUMMYUNIONNAME = .{ .Pointer = @ptrFromInt(@intFromPtr(me)) };

        var dummy_buf: [1]u8 = .{0};
        var wsabuf = WSABUF{ .len = 0, .buf = &dummy_buf };
        var bytes_transferred: win.DWORD = 0;
        var flags: win.DWORD = 0;

        const rc = switch (dir) {
            .read => WSARecv(@intCast(sock), @ptrCast(&wsabuf), 1, &bytes_transferred, &flags, &ovl, null),
            .write => WSASend(@intCast(sock), @ptrCast(&wsabuf), 1, &bytes_transferred, 0, &ovl, null),
        };
        if (rc != 0) {
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
            // Cancel fired between checkpoint and register —
            // issue CancelIoEx now so the kernel's completion path
            // wakes us with `STATUS_CANCELLED`.
            _ = CancelIoEx(@ptrFromInt(sock), &ovl);
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);

        if (c.isFired()) return error.Cancelled;
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        if (self.pending.load(.acquire) == 0) return 0;

        var entries: [COMPLETION_BATCH]OVERLAPPED_ENTRY = undefined;
        var removed: win.ULONG = 0;
        const timeout_ms: win.DWORD = if (blocking) INFINITE else 0;

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
            // Three completion shapes:
            //   1. I/O completion (WSARecv/WSASend/AcceptEx/ConnectEx)
            //      — OVERLAPPED is non-null; coroutine ptr is in
            //      OVERLAPPED.Pointer (the union field, unused for
            //      socket I/O — see note in submit paths).
            //   2. Timer completion (PostQueuedCompletionStatus from
            //      the threadpool callback) — OVERLAPPED is null;
            //      coroutine ptr is in CompletionKey.
            //   3. Interrupt completion (PostQueuedCompletionStatus
            //      from `interrupt()`) — OVERLAPPED is null;
            //      CompletionKey == INTERRUPT_KEY (the sentinel).
            //      Doesn't count toward `pending` — skip the unpark.
            if (e.lpOverlapped == null and e.lpCompletionKey == INTERRUPT_KEY) continue;

            real_count += 1;
            const coro_ptr: usize = if (e.lpOverlapped) |ovl|
                @intFromPtr(ovl.DUMMYUNIONNAME.Pointer)
            else
                e.lpCompletionKey;

            const coro: *coroutine.Coroutine = @ptrFromInt(coro_ptr);
            runtime.unpark(coro);
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

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) IoError!usize {
    while (true) {
        const r = recv_zig(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (r >= 0) return @intCast(r);
        const e = WSAGetLastError();
        if (e == WSAEWOULDBLOCK) {
            try rx.waitReadable(fd);
            continue;
        }
        if (e == WSAEINTR) continue;
        return wsaToIoError(e);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) IoError!usize {
    while (true) {
        const w = send_zig(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (w >= 0) return @intCast(w);
        const e = WSAGetLastError();
        if (e == WSAEWOULDBLOCK) {
            try rx.waitWritable(fd);
            continue;
        }
        if (e == WSAEINTR) continue;
        return wsaToIoError(e);
    }
}

pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) IoError!usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) IoError!void {
    var total: usize = 0;
    while (total < buf.len) {
        const w = try writeAsync(rx, fd, buf[total..]);
        total += w;
    }
}

// Compile-time check: Windows-only.
comptime {
    if (builtin.os.tag != .windows) {
        @compileError("reactor_iocp.zig is Windows-only; src/reactor.zig dispatches by os.tag");
    }
}

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

// WSADATA layout: 400 bytes is the documented max; we don't care
// about contents.
const WSA_DATA_BYTES: usize = 400;

const WSA_IO_PENDING: c_int = 997;

// ─── Reactor ─────────────────────────────────────────────────────

pub const Reactor = struct {
    /// Sentinel `INVALID_HANDLE_VALUE` mirrors kqueue's `kq = -1`
    /// so `Reactor = .{}` works as a struct-level default in
    /// `runtime.zig`. Real value is installed by `init`.
    iocp: win.HANDLE = win.INVALID_HANDLE_VALUE,

    /// In-flight I/O ops. Same role as kqueue's `pending`.
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

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
        _ = CloseHandle(self.iocp);
        // WSACleanup is intentionally not called; matches the
        // single-Runtime-per-process common case. A future
        // multi-Runtime reference-counted teardown can revisit.
    }

    /// Associate a socket with this reactor's IOCP. Called by
    /// `net.zig` after each socket creation. The CompletionKey is
    /// the socket handle itself; the per-op coroutine pointer
    /// goes in `OVERLAPPED.hEvent` (see `submitReadiness`).
    pub fn associate(self: *Reactor, sock_handle: usize) !void {
        const handle: win.HANDLE = @ptrFromInt(sock_handle);
        const ret = CreateIoCompletionPort(handle, self.iocp, sock_handle, 0);
        if (ret == null) return error.IocpAssociateFailed;
    }

    pub fn waitReadable(self: *Reactor, fd: i32) void {
        self.submitReadiness(@intCast(fd), .read);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) void {
        self.submitReadiness(@intCast(fd), .write);
    }

    const Direction = enum { read, write };

    fn submitReadiness(self: *Reactor, sock: usize, dir: Direction) void {
        const me = current.require();

        // Stack-allocate OVERLAPPED. Volt's stacks are mmap'd
        // and stable; the kernel can write to this address up
        // until we resume.
        var ovl: OVERLAPPED = std.mem.zeroes(OVERLAPPED);
        // Stash the coroutine pointer in hEvent. The IOCP
        // doesn't care what's there; we'll cast it back on
        // wake.
        ovl.hEvent = @ptrFromInt(@intFromPtr(me));

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
        if (rc != 0 and WSAGetLastError() != WSA_IO_PENDING) {
            @panic("WSARecv/WSASend zero-byte readiness submission failed");
        }

        _ = self.pending.fetchAdd(1, .acq_rel);
        me.pending = .park;
        context.swap(&me.ctx, me.main_ctx);
        // On resume: poll() has unparked us. The OVERLAPPED on our
        // stack is no longer in use by the kernel. Return — caller
        // retries the real read/write.
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

    pub fn waitTimer(self: *Reactor, ns: u64) void {
        const me = current.require();

        // Per-sleep allocation. A future per-P pool of pre-created
        // threadpool timers would amortise the
        // CreateThreadpoolTimer cost.
        var ctx = TimerCtx{ .reactor = self, .coro = me };
        const timer = CreateThreadpoolTimer(&timerCallback, &ctx, null) orelse
            @panic("CreateThreadpoolTimer failed");
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

    pub fn pendingCount(self: *const Reactor) u32 {
        return self.pending.load(.acquire);
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
        _ = self.pending.fetchSub(@intCast(count), .acq_rel);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const e = &entries[i];
            // Two completion shapes:
            //   1. I/O completion (WSARecv/WSASend) — OVERLAPPED
            //      is non-null; coroutine ptr is in OVERLAPPED.hEvent.
            //   2. Timer completion (PostQueuedCompletionStatus) —
            //      OVERLAPPED is null; coroutine ptr is in
            //      CompletionKey.
            const coro_ptr: usize = if (e.lpOverlapped) |ovl|
                @intFromPtr(ovl.hEvent)
            else
                e.lpCompletionKey;

            const coro: *coroutine.Coroutine = @ptrFromInt(coro_ptr);
            runtime.unpark(coro);
        }
        return count;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Non-blocking IO helpers
// ─────────────────────────────────────────────────────────────────────

// Winsock equivalents of fcntl(O_NONBLOCK) and POSIX read/write.

// _IOW('f', 126, u_long) — 0x8004667e — set non-blocking via FIONBIO.
const FIONBIO: win.LONG = @bitCast(@as(u32, 0x8004667e));

extern "ws2_32" fn ioctlsocket(s: SOCKET, cmd: win.LONG, argp: *win.ULONG) callconv(.winapi) c_int;
extern "ws2_32" fn recv(s: SOCKET, buf: [*]u8, len: c_int, flags: c_int) callconv(.winapi) c_int;
extern "ws2_32" fn send_fn(s: SOCKET, buf: [*]const u8, len: c_int, flags: c_int) callconv(.winapi) c_int;

const WSAEWOULDBLOCK: c_int = 10035;

pub fn setNonblock(fd: i32) !void {
    var mode: win.ULONG = 1;
    if (ioctlsocket(@intCast(fd), FIONBIO, &mode) != 0) return error.FcntlSetFailed;
}

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) !usize {
    while (true) {
        const r = recv(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (r >= 0) return @intCast(r);
        if (WSAGetLastError() != WSAEWOULDBLOCK) return error.ReadFailed;
        rx.waitReadable(fd);
    }
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) !usize {
    while (true) {
        const w = send_fn(@intCast(fd), buf.ptr, @intCast(buf.len), 0);
        if (w >= 0) return @intCast(w);
        if (WSAGetLastError() != WSAEWOULDBLOCK) return error.WriteFailed;
        rx.waitWritable(fd);
    }
}

pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const got = try readAsync(rx, fd, buf[total..]);
        if (got == 0) return total;
        total += got;
    }
    return total;
}

pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) !void {
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

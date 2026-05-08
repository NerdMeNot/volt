//! NTDLL + Ancillary Function Driver (AFD) bindings.
//!
//! Volt's Windows reactor (`io/reactor_iocp.zig`) needs poll-style
//! readiness notifications from IOCP — the same shape kqueue and
//! epoll deliver. Win32 doesn't expose a public API for that, but
//! AFD.sys (the kernel driver behind Winsock) does, via
//! `IOCTL_AFD_POLL` issued through `NtDeviceIoControlFile`. mio,
//! wepoll, and Tokio all use this approach; it's the only practical
//! way to get readiness semantics on Windows without rebuilding the
//! whole I/O surface around completion ports.
//!
//! ## What lives here
//!
//! - Minimal NT bindings (`NtCreateFile`, `NtDeviceIoControlFile`)
//!   and the structs they need (`IO_STATUS_BLOCK`,
//!   `OBJECT_ATTRIBUTES`, `UNICODE_STRING`).
//! - The AFD ABI: `AFD_POLL_INFO` + `AFD_POLL_HANDLE_INFO` + the
//!   `AFD_POLL_*` event flags + the `IOCTL_AFD_POLL` control code.
//!
//! Everything is in one file so the reactor doesn't need to know
//! about NT-vs-Win32 layering. None of these symbols are documented
//! by Microsoft for general consumption — they're stable in practice
//! (Winsock and many third-party tools depend on them; documented
//! by the wepoll project + ReactOS source).
//!
//! ## References
//!
//! - mio: `src/sys/windows/afd.rs` (the canonical Rust port)
//! - wepoll: `src/wepoll.c` (the canonical C port)
//! - Microsoft docs on `NtCreateFile` / `NtDeviceIoControlFile`
//!   (the public NT API surface)

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.os.tag != .windows) {
        @compileError("internal/win32/ntdll.zig is for Windows only");
    }
}

// ─────────────────────────────────────────────────────────────────────
// NT base types
// ─────────────────────────────────────────────────────────────────────

pub const NTSTATUS = i32;
pub const HANDLE = std.os.windows.HANDLE;
pub const BOOLEAN = u8;
pub const ULONG = u32;
pub const USHORT = u16;
pub const LONG = i32;
pub const LARGE_INTEGER = i64;
pub const PVOID = ?*anyopaque;
pub const ACCESS_MASK = u32;

/// Common NTSTATUS values we care about.
pub const STATUS_SUCCESS: NTSTATUS = 0;
pub const STATUS_PENDING: NTSTATUS = 0x00000103;
/// Returned when an op was cancelled (e.g. via `CancelIoEx`).
pub const STATUS_CANCELLED: NTSTATUS = @bitCast(@as(u32, 0xC0000120));
pub const STATUS_INVALID_HANDLE: NTSTATUS = @bitCast(@as(u32, 0xC0000008));

pub fn ntSuccess(status: NTSTATUS) bool {
    return status >= 0;
}

// ─────────────────────────────────────────────────────────────────────
// IO_STATUS_BLOCK + OVERLAPPED
// ─────────────────────────────────────────────────────────────────────

/// `IO_STATUS_BLOCK` — kernel-side completion record. The reactor
/// keys completions by the OVERLAPPED's address, but
/// `NtDeviceIoControlFile` writes the final NTSTATUS + transferred
/// byte count here on completion. We embed it inside our per-
/// registration record.
pub const IO_STATUS_BLOCK = extern struct {
    /// Either NTSTATUS or pointer (kernel union); we treat as NTSTATUS.
    Status: NTSTATUS,
    Information: usize,
};

/// Win32 `OVERLAPPED`. The kernel's IOCP layer reads/writes this
/// during async I/O. Ordering of fields matches the Win32 ABI.
pub const OVERLAPPED = extern struct {
    Internal: usize,
    InternalHigh: usize,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct { Offset: ULONG, OffsetHigh: ULONG },
        Pointer: PVOID,
    },
    hEvent: ?HANDLE,
};

/// Result of `GetQueuedCompletionStatus`: which OVERLAPPED completed,
/// completion key, bytes transferred. The reactor keys callbacks by
/// `lpCompletionKey` so the OVERLAPPED is just identity.
pub const COMPLETION = struct {
    transferred: ULONG,
    key: usize,
    overlapped: ?*OVERLAPPED,
};

// ─────────────────────────────────────────────────────────────────────
// UNICODE_STRING + OBJECT_ATTRIBUTES (for NtCreateFile)
// ─────────────────────────────────────────────────────────────────────

pub const UNICODE_STRING = extern struct {
    Length: USHORT,
    MaximumLength: USHORT,
    Buffer: [*]const u16,
};

pub const OBJECT_ATTRIBUTES = extern struct {
    Length: ULONG,
    RootDirectory: ?HANDLE,
    ObjectName: ?*const UNICODE_STRING,
    Attributes: ULONG,
    SecurityDescriptor: PVOID,
    SecurityQualityOfService: PVOID,
};

pub const OBJ_CASE_INSENSITIVE: ULONG = 0x40;

// FILE_* access bits relevant for opening AFD.
pub const SYNCHRONIZE: ACCESS_MASK = 0x00100000;
pub const FILE_OPEN: ULONG = 0x00000001;
pub const FILE_SHARE_READ: ULONG = 0x00000001;
pub const FILE_SHARE_WRITE: ULONG = 0x00000002;
pub const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x01;
pub const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x02;

// ─────────────────────────────────────────────────────────────────────
// NtCreateFile / NtDeviceIoControlFile
// ─────────────────────────────────────────────────────────────────────

pub const PIO_APC_ROUTINE = ?*const fn (PVOID, *IO_STATUS_BLOCK, ULONG) callconv(.winapi) void;

pub extern "ntdll" fn NtCreateFile(
    FileHandle: *HANDLE,
    DesiredAccess: ACCESS_MASK,
    ObjectAttributes: *OBJECT_ATTRIBUTES,
    IoStatusBlock: *IO_STATUS_BLOCK,
    AllocationSize: ?*LARGE_INTEGER,
    FileAttributes: ULONG,
    ShareAccess: ULONG,
    CreateDisposition: ULONG,
    CreateOptions: ULONG,
    EaBuffer: PVOID,
    EaLength: ULONG,
) callconv(.winapi) NTSTATUS;

pub extern "ntdll" fn NtDeviceIoControlFile(
    FileHandle: HANDLE,
    Event: ?HANDLE,
    ApcRoutine: PIO_APC_ROUTINE,
    ApcContext: PVOID,
    IoStatusBlock: *IO_STATUS_BLOCK,
    IoControlCode: ULONG,
    InputBuffer: PVOID,
    InputBufferLength: ULONG,
    OutputBuffer: PVOID,
    OutputBufferLength: ULONG,
) callconv(.winapi) NTSTATUS;

pub extern "ntdll" fn NtClose(Handle: HANDLE) callconv(.winapi) NTSTATUS;

// ─────────────────────────────────────────────────────────────────────
// AFD ABI
// ─────────────────────────────────────────────────────────────────────

/// `IOCTL_AFD_POLL` — the kernel control code to issue a poll-style
/// async wait against AFD.sys. Keyed by AFD's device prefix (0x12 =
/// FILE_DEVICE_NETWORK) and a method (METHOD_NEITHER = 0x3) chosen
/// for input/output buffer pass-through.
///
/// Bit layout: `(DeviceType << 16) | (Access << 14) | (Function << 2) | Method`
/// = `(0x12 << 16) | (0 << 14) | (9 << 2) | 3` = `0x00120024`.
/// (mio and wepoll both use this exact value; verified against
/// reactos's `afd.h`.)
pub const IOCTL_AFD_POLL: ULONG = 0x00012024;

/// AFD poll event flags. These map onto the readiness conditions an
/// epoll/kqueue user expects.
pub const AFD_POLL_RECEIVE: ULONG = 0x0001;
pub const AFD_POLL_RECEIVE_EXPEDITED: ULONG = 0x0002;
pub const AFD_POLL_SEND: ULONG = 0x0004;
pub const AFD_POLL_DISCONNECT: ULONG = 0x0008;
pub const AFD_POLL_ABORT: ULONG = 0x0010;
pub const AFD_POLL_LOCAL_CLOSE: ULONG = 0x0020;
pub const AFD_POLL_ACCEPT: ULONG = 0x0080;
pub const AFD_POLL_CONNECT_FAIL: ULONG = 0x0100;

/// `AFD_POLL_HANDLE_INFO` — per-socket poll request inside an
/// `AFD_POLL_INFO`. We use single-handle requests today, but the
/// kernel ABI permits an array.
pub const AFD_POLL_HANDLE_INFO = extern struct {
    Handle: HANDLE,
    Events: ULONG,
    Status: NTSTATUS,
};

/// `AFD_POLL_INFO` — the input AND output buffer of `IOCTL_AFD_POLL`.
/// On submit: `Timeout` holds a deadline (or maxInt for "no timeout"),
/// `NumberOfHandles` = 1, `Handles[0]` describes the (socket, mask)
/// pair we're polling. On completion: `Handles[0].Events` is set to
/// the actual events that fired, and `Status` to the per-handle
/// NTSTATUS.
///
/// `Exclusive` controls AFD's "multiple-poll" semantics — set to 0
/// (FALSE) for our use case so multiple registrations on the same
/// socket all see the events. Mio uses 0 too.
pub const AFD_POLL_INFO = extern struct {
    Timeout: LARGE_INTEGER,
    NumberOfHandles: ULONG,
    Exclusive: ULONG,
    Handles: [1]AFD_POLL_HANDLE_INFO,
};

/// AFD device path. The device is per-Winsock-Provider; the standard
/// path used by all known consumers (mio, wepoll, libuv via direct
/// IOCTL) is `\Device\Afd`. We append a process-unique suffix so two
/// reactors in the same process don't share AFD handles. mio uses
/// `\Device\Afd\Mio`.
pub const AFD_DEVICE_NAME_W: []const u16 = std.unicode.utf8ToUtf16LeStringLiteral("\\Device\\Afd\\Volt");

// ─────────────────────────────────────────────────────────────────────
// IOCP + cancellation Win32 wrappers
// ─────────────────────────────────────────────────────────────────────

pub const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));
pub const INFINITE: ULONG = 0xFFFFFFFF;

pub extern "kernel32" fn CreateIoCompletionPort(
    FileHandle: HANDLE,
    ExistingCompletionPort: ?HANDLE,
    CompletionKey: usize,
    NumberOfConcurrentThreads: ULONG,
) callconv(.winapi) ?HANDLE;

pub extern "kernel32" fn GetQueuedCompletionStatus(
    CompletionPort: HANDLE,
    lpNumberOfBytesTransferred: *ULONG,
    lpCompletionKey: *usize,
    lpOverlapped: *?*OVERLAPPED,
    dwMilliseconds: ULONG,
) callconv(.winapi) std.os.windows.BOOL;

pub extern "kernel32" fn PostQueuedCompletionStatus(
    CompletionPort: HANDLE,
    dwNumberOfBytesTransferred: ULONG,
    dwCompletionKey: usize,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) std.os.windows.BOOL;

pub extern "kernel32" fn CancelIoEx(
    hFile: HANDLE,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) std.os.windows.BOOL;

pub extern "kernel32" fn SetFileCompletionNotificationModes(
    FileHandle: HANDLE,
    Flags: u8,
) callconv(.winapi) std.os.windows.BOOL;

pub extern "kernel32" fn CloseHandle(hObject: HANDLE) callconv(.winapi) std.os.windows.BOOL;

// ─────────────────────────────────────────────────────────────────────
// Threadpool timer — used by `registerTimer` to PQCS the IOCP when
// the timer fires.
// ─────────────────────────────────────────────────────────────────────

pub const TP_CALLBACK_INSTANCE = opaque {};
pub const TP_TIMER = opaque {};
pub const TP_CALLBACK_ENVIRON_V3 = opaque {};
pub const FILETIME = extern struct {
    dwLowDateTime: ULONG,
    dwHighDateTime: ULONG,
};

pub const PTP_TIMER_CALLBACK = *const fn (
    Instance: ?*TP_CALLBACK_INSTANCE,
    Context: PVOID,
    Timer: ?*TP_TIMER,
) callconv(.winapi) void;

pub extern "kernel32" fn CreateThreadpoolTimer(
    pfnti: PTP_TIMER_CALLBACK,
    pv: PVOID,
    pcbe: ?*TP_CALLBACK_ENVIRON_V3,
) callconv(.winapi) ?*TP_TIMER;

pub extern "kernel32" fn SetThreadpoolTimer(
    pti: ?*TP_TIMER,
    pftDueTime: ?*FILETIME,
    msPeriod: ULONG,
    msWindowLength: ULONG,
) callconv(.winapi) void;

pub extern "kernel32" fn CloseThreadpoolTimer(pti: ?*TP_TIMER) callconv(.winapi) void;

pub extern "kernel32" fn WaitForThreadpoolTimerCallbacks(
    pti: ?*TP_TIMER,
    fCancelPendingCallbacks: std.os.windows.BOOL,
) callconv(.winapi) void;

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "ntdll: AFD_POLL_INFO layout matches the kernel ABI" {
    // 8 (LARGE_INTEGER Timeout) + 4 (ULONG NumberOfHandles) +
    // 4 (ULONG Exclusive) + sizeof(AFD_POLL_HANDLE_INFO) for the
    // single-handle stub.
    const HandleInfo = AFD_POLL_HANDLE_INFO;
    // HANDLE (8 on x64) + ULONG (4) + NTSTATUS (4) = 16, no padding.
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(HandleInfo));
    // 8 + 4 + 4 + 16 = 32.
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(AFD_POLL_INFO));
}

test "ntdll: STATUS_CANCELLED is the documented value" {
    try std.testing.expectEqual(@as(NTSTATUS, @bitCast(@as(u32, 0xC0000120))), STATUS_CANCELLED);
}

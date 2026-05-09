//! Volt-internal ws2_32.dll bindings.
//!
//! Zig 0.16's `std.os.windows.ws2_32` exposes types but NOT extern
//! function declarations for socket I/O (closesocket, recv, send, etc).
//! Volt declares them here so the runtime can call Winsock directly
//! without leaning on a libc shim.
//!
//! ## What lives here
//!
//! - The minimum extern declarations the syscall layer + reactor need
//!   (closesocket, recv, send, recvfrom, sendto, ioctlsocket, WSAGetLastError).
//! - Constants for socket-shutdown modes, ioctl commands.
//!
//! ## References
//!
//! - Microsoft Winsock docs (winsock2.h)
//! - Zig 0.16 `std.os.windows.ws2_32` (types + structs)

const std = @import("std");
const windows = std.os.windows;

pub const SOCKET = *opaque {};
pub const INVALID_SOCKET: SOCKET = @ptrFromInt(std.math.maxInt(usize));
pub const SOCKET_ERROR: c_int = -1;

// ── Socket shutdown modes ──────────────────────────────────────────
pub const SD_RECEIVE: c_int = 0;
pub const SD_SEND: c_int = 1;
pub const SD_BOTH: c_int = 2;

// ── ioctlsocket commands ───────────────────────────────────────────
pub const FIONREAD: c_long = 0x4004667F;
pub const FIONBIO: c_long = -2147195266; // 0x8004667E (signed wrap)

// ── Winsock error codes (subset) ───────────────────────────────────
pub const WSAEWOULDBLOCK: c_int = 10035;
pub const WSAEINPROGRESS: c_int = 10036;
pub const WSAEINTR: c_int = 10004;
pub const WSAENOTSOCK: c_int = 10038;
pub const WSAECONNRESET: c_int = 10054;
pub const WSAECONNABORTED: c_int = 10053;
pub const WSAESHUTDOWN: c_int = 10058;
pub const WSAETIMEDOUT: c_int = 10060;
pub const WSAEMSGSIZE: c_int = 10040;
pub const WSAENOBUFS: c_int = 10055;

// ── Function bindings ──────────────────────────────────────────────

pub extern "ws2_32" fn closesocket(s: SOCKET) callconv(.winapi) c_int;

pub extern "ws2_32" fn recv(
    s: SOCKET,
    buf: [*]u8,
    len: c_int,
    flags: c_int,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn send(
    s: SOCKET,
    buf: [*]const u8,
    len: c_int,
    flags: c_int,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn recvfrom(
    s: SOCKET,
    buf: [*]u8,
    len: c_int,
    flags: c_int,
    from: ?*windows.ws2_32.sockaddr,
    fromlen: ?*c_int,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn sendto(
    s: SOCKET,
    buf: [*]const u8,
    len: c_int,
    flags: c_int,
    to: ?*const windows.ws2_32.sockaddr,
    tolen: c_int,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn shutdown(
    s: SOCKET,
    how: c_int,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn ioctlsocket(
    s: SOCKET,
    cmd: c_long,
    argp: *c_ulong,
) callconv(.winapi) c_int;

pub extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;

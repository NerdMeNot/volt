//! `volt.net.sockopt` — typed socket-option setters.
//!
//! Free functions over a raw fd. Same option works for TCP, UDP, and
//! Unix sockets (modulo what the protocol supports), so the typed
//! setters live here rather than per-socket-type. `TcpStream`,
//! `UdpSocket`, etc. expose `setOption` / `setNoDelay` / etc. as
//! convenience methods that forward here.
//!
//! ## Naming
//!
//! Each setter takes the typed value the user actually has (`bool`,
//! `Duration`, `usize`) — not raw `c_int` or socklen-sized bytes.
//! Conversions to the kernel ABI happen here. Bool options pass an
//! `c_int` of 0 or 1; durations split into `timeval` for socket-
//! buffer timeouts and `c_int` seconds for keepalive parameters.

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const io_errors = @import("../io/errors.zig");
const time_mod = @import("../time.zig");
const Duration = time_mod.Duration;

pub const SetOptionError = io_errors.SetSockOptError;

// ─────────────────────────────────────────────────────────────────────
// Common helpers — bool / int / linger.
// ─────────────────────────────────────────────────────────────────────

fn setBool(fd: posix.socket_t, level: i32, optname: u32, value: bool) SetOptionError!void {
    var v: c_int = if (value) 1 else 0;
    try syscall.setsockopt(fd, level, optname, std.mem.asBytes(&v));
}

fn setInt(fd: posix.socket_t, level: i32, optname: u32, value: c_int) SetOptionError!void {
    var v = value;
    try syscall.setsockopt(fd, level, optname, std.mem.asBytes(&v));
}

// ─────────────────────────────────────────────────────────────────────
// Public setters
// ─────────────────────────────────────────────────────────────────────

/// `TCP_NODELAY` — disable Nagle's algorithm. Latency-sensitive
/// protocols (interactive RPC, keystroke streaming) want this on.
pub fn setNoDelay(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, value);
}

/// `SO_REUSEADDR` — allow binding to an address in TIME_WAIT. Most
/// servers want this on so a restart doesn't fail with EADDRINUSE.
pub fn setReuseAddr(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, value);
}

/// `SO_REUSEPORT` — allow multiple sockets to bind the same port for
/// kernel-level load balancing. Linux 3.9+, all BSDs.
pub fn setReusePort(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, value);
}

/// `SO_BROADCAST` — permit sending to broadcast addresses. UDP only.
pub fn setBroadcast(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.SOL.SOCKET, posix.SO.BROADCAST, value);
}

/// `SO_KEEPALIVE` — enable TCP keepalive probes. Use the per-socket
/// `setKeepAliveParams` to tune intervals.
pub fn setKeepAlive(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, value);
}

/// Tune TCP keepalive parameters. Idle is the time before the first
/// probe; interval is the gap between probes; count is how many
/// missed probes terminate the connection. Caller should also call
/// `setKeepAlive(fd, true)` to enable the mechanism.
pub fn setKeepAliveParams(
    fd: posix.socket_t,
    idle: Duration,
    interval: Duration,
    count: u32,
) SetOptionError!void {
    const idle_secs: c_int = @intCast(@max(@as(u64, 1), idle.nanos / std.time.ns_per_s));
    const interval_secs: c_int = @intCast(@max(@as(u64, 1), interval.nanos / std.time.ns_per_s));
    const count_int: c_int = @intCast(count);

    // Darwin spells the idle-time option TCP_KEEPALIVE; Linux spells
    // it TCP_KEEPIDLE. KEEPINTVL / KEEPCNT match on both.
    const idle_opt: u32 = comptime switch (@import("builtin").os.tag) {
        .macos, .ios, .tvos, .watchos, .visionos, .driverkit => posix.TCP.KEEPALIVE,
        else => posix.TCP.KEEPIDLE,
    };
    try setInt(fd, posix.IPPROTO.TCP, idle_opt, idle_secs);
    try setInt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPINTVL, interval_secs);
    try setInt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPCNT, count_int);
}

/// `SO_LINGER` — control close() behaviour. `null` disables linger
/// (the default — close returns immediately, kernel drains in
/// background). `Duration` enables it: close blocks for up to the
/// given duration waiting for unsent data to drain.
pub fn setLinger(fd: posix.socket_t, value: ?Duration) SetOptionError!void {
    const Linger = extern struct { onoff: c_int, linger: c_int };
    var l: Linger = if (value) |d|
        .{ .onoff = 1, .linger = @intCast(d.nanos / std.time.ns_per_s) }
    else
        .{ .onoff = 0, .linger = 0 };
    try syscall.setsockopt(fd, posix.SOL.SOCKET, posix.SO.LINGER, std.mem.asBytes(&l));
}

/// `SO_RCVBUF` — request a receive buffer size hint to the kernel.
/// The kernel may double-and-cap; query via `getsockopt` if the
/// actual value matters.
pub fn setRecvBufSize(fd: posix.socket_t, bytes: usize) SetOptionError!void {
    return setInt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, @intCast(bytes));
}

/// `SO_SNDBUF` — same as `setRecvBufSize` but for the send buffer.
pub fn setSendBufSize(fd: posix.socket_t, bytes: usize) SetOptionError!void {
    return setInt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, @intCast(bytes));
}

/// `IPV6_V6ONLY` — when off (default on most platforms), an AF_INET6
/// socket also accepts IPv4 connections via IPv4-mapped addresses.
/// Set to true if you want a strictly-IPv6 socket.
pub fn setIpv6Only(fd: posix.socket_t, value: bool) SetOptionError!void {
    return setBool(fd, posix.IPPROTO.IPV6, posix.IPV6.V6ONLY, value);
}

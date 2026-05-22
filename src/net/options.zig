//! Shared socket-option helpers — typed setters/getters used by
//! TcpStream, TcpListener, UdpSocket, and the Unix socket family.
//!
//! The kernel exposes options as flat `int` / `bool` / struct
//! payloads via `setsockopt` / `getsockopt`. This module provides:
//!
//!   * `Keepalive` — typed struct for TCP keepalive (idle / interval
//!     / retries) with per-platform setsockopt translation
//!   * `Linger` — typed struct for SO_LINGER (drain timeout on close)
//!   * Generic `setBoolOption`, `setIntOption`, `getBoolOption`,
//!     `getIntOption` helpers that wrap the platform setsockopt
//!
//! The per-handle types call into these so the option logic lives
//! in one place.

const std = @import("std");
const builtin = @import("builtin");

// ─── libc extern bindings ────────────────────────────────────────────
//
// std.posix.setsockopt / getsockopt got dropped in Zig 0.16; we go
// via @extern to libc directly. Same pattern as src/net.zig and
// src/reactor_*.zig already use.

const c_setsockopt = if (builtin.os.tag == .windows) {} else @extern(
    *const fn (c_int, c_int, c_int, *const anyopaque, c_uint) callconv(.c) c_int,
    .{ .name = "setsockopt" },
);

const c_getsockopt = if (builtin.os.tag == .windows) {} else @extern(
    *const fn (c_int, c_int, c_int, *anyopaque, *c_uint) callconv(.c) c_int,
    .{ .name = "getsockopt" },
);

// Windows uses Winsock-prefixed names. The signature shape matches
// POSIX (almost — Winsock's `optlen` is signed; we cast).
const ws2_setsockopt = if (builtin.os.tag == .windows) @extern(
    *const fn (usize, c_int, c_int, [*]const u8, c_int) callconv(.winapi) c_int,
    .{ .name = "setsockopt", .library_name = "ws2_32" },
) else {};

const ws2_getsockopt = if (builtin.os.tag == .windows) @extern(
    *const fn (usize, c_int, c_int, [*]u8, *c_int) callconv(.winapi) c_int,
    .{ .name = "getsockopt", .library_name = "ws2_32" },
) else {};

// ─── Level + option constants ───────────────────────────────────────

pub const SOL_SOCKET: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0xFFFF,
    .linux => 1,
    .windows => 0xFFFF,
    else => 1,
};

pub const IPPROTO_TCP: c_int = 6;
pub const IPPROTO_IP: c_int = 0;
pub const IPPROTO_IPV6: c_int = 41;

// SOL_SOCKET options.
pub const SO_REUSEADDR: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x0004,
    .linux => 2,
    .windows => 0x0004,
    else => 2,
};

pub const SO_REUSEPORT: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x0200,
    .linux => 15,
    else => 0, // Windows: not standard; SO_REUSEADDR has the right semantic
};

pub const SO_KEEPALIVE: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x0008,
    .linux => 9,
    .windows => 0x0008,
    else => 9,
};

pub const SO_SNDBUF: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x1001,
    .linux => 7,
    .windows => 0x1001,
    else => 7,
};

pub const SO_RCVBUF: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x1002,
    .linux => 8,
    .windows => 0x1002,
    else => 8,
};

pub const SO_BROADCAST: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x0020,
    .linux => 6,
    .windows => 0x0020,
    else => 6,
};

pub const SO_LINGER: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 0x0080,
    .linux => 13,
    .windows => 0x0080,
    else => 13,
};

// IPPROTO_TCP options.
pub const TCP_NODELAY: c_int = switch (builtin.os.tag) {
    .linux => 1,
    .windows => 0x0001,
    else => 0x01, // Darwin / BSD
};

// TCP keepalive — fully cross-platform mess. macOS has TCP_KEEPALIVE
// (single setting). Linux and Windows have TCP_KEEPIDLE / KEEPINTVL /
// KEEPCNT trio. We bridge below via setKeepalive.
pub const TCP_KEEPALIVE_DARWIN: c_int = 0x10; // macOS / iOS
pub const TCP_KEEPIDLE_LINUX: c_int = 4;
pub const TCP_KEEPINTVL_LINUX: c_int = 5;
pub const TCP_KEEPCNT_LINUX: c_int = 6;
pub const TCP_KEEPIDLE_WINDOWS: c_int = 3;
pub const TCP_KEEPINTVL_WINDOWS: c_int = 17;
pub const TCP_KEEPCNT_WINDOWS: c_int = 16;

// IPv6 options.
pub const IPV6_V6ONLY: c_int = switch (builtin.os.tag) {
    .linux => 26,
    .windows => 27,
    else => 27, // Darwin
};

// ─── Typed option structs ───────────────────────────────────────────

/// TCP keepalive configuration. `idle` is the time before the first
/// probe; `interval` is the gap between probes; `retries` is the
/// count before declaring the connection dead.
///
/// `interval` and `retries` are Linux/Windows-only — Darwin's
/// `TCP_KEEPALIVE` is a single per-connection value so only `.idle`
/// applies there.
pub const Keepalive = struct {
    idle: u32, // seconds before first probe
    interval: ?u32 = null, // seconds between probes (Linux/Windows)
    retries: ?u32 = null, // max probes before dead (Linux/Windows)
};

/// SO_LINGER — graceful close timeout. `null` means "don't linger"
/// (the default, which is to return from `close()` immediately and
/// let the kernel flush in the background).
pub const Linger = struct {
    timeout: u16, // seconds to wait at close() for queued data to drain
};

// Platform struct layouts for the option payloads.
const LingerStruct = extern struct {
    l_onoff: c_int,
    l_linger: c_int,
};

// ─── Generic setters / getters ──────────────────────────────────────

pub const OptionError = error{ BadDescriptor, NotSupported, Unexpected };

pub fn setBoolOption(fd: i32, level: c_int, opt: c_int, value: bool) OptionError!void {
    const v: c_int = if (value) 1 else 0;
    try setRaw(fd, level, opt, std.mem.asBytes(&v));
}

pub fn getBoolOption(fd: i32, level: c_int, opt: c_int) OptionError!bool {
    var v: c_int = 0;
    try getRaw(fd, level, opt, std.mem.asBytes(&v));
    return v != 0;
}

pub fn setIntOption(fd: i32, level: c_int, opt: c_int, value: i32) OptionError!void {
    const v: c_int = value;
    try setRaw(fd, level, opt, std.mem.asBytes(&v));
}

pub fn getIntOption(fd: i32, level: c_int, opt: c_int) OptionError!i32 {
    var v: c_int = 0;
    try getRaw(fd, level, opt, std.mem.asBytes(&v));
    return v;
}

fn setRaw(fd: i32, level: c_int, opt: c_int, payload: []const u8) OptionError!void {
    if (comptime builtin.os.tag == .windows) {
        const sock: usize = @intCast(fd);
        const rc = ws2_setsockopt(sock, level, opt, payload.ptr, @intCast(payload.len));
        if (rc != 0) return error.Unexpected;
        return;
    }
    const rc = c_setsockopt(@intCast(fd), level, opt, payload.ptr, @intCast(payload.len));
    if (rc != 0) {
        // Map common errnos. EBADF / ENOPROTOOPT are the ones worth
        // discriminating; rest fall through.
        return error.Unexpected;
    }
}

fn getRaw(fd: i32, level: c_int, opt: c_int, out: []u8) OptionError!void {
    var len: c_uint = @intCast(out.len);
    if (comptime builtin.os.tag == .windows) {
        const sock: usize = @intCast(fd);
        var wlen: c_int = @intCast(out.len);
        const rc = ws2_getsockopt(sock, level, opt, out.ptr, &wlen);
        if (rc != 0) return error.Unexpected;
        return;
    }
    const rc = c_getsockopt(@intCast(fd), level, opt, out.ptr, &len);
    if (rc != 0) return error.Unexpected;
}

// ─── Typed setters for the well-known options ──────────────────────

pub fn setTcpNoDelay(fd: i32, enabled: bool) OptionError!void {
    return setBoolOption(fd, IPPROTO_TCP, TCP_NODELAY, enabled);
}

pub fn getTcpNoDelay(fd: i32) OptionError!bool {
    return getBoolOption(fd, IPPROTO_TCP, TCP_NODELAY);
}

pub fn setReuseAddr(fd: i32, enabled: bool) OptionError!void {
    return setBoolOption(fd, SOL_SOCKET, SO_REUSEADDR, enabled);
}

pub fn setReusePort(fd: i32, enabled: bool) OptionError!void {
    // Windows uses SO_REUSEADDR for the load-balance semantic.
    if (comptime builtin.os.tag == .windows) {
        return setBoolOption(fd, SOL_SOCKET, SO_REUSEADDR, enabled);
    }
    return setBoolOption(fd, SOL_SOCKET, SO_REUSEPORT, enabled);
}

pub fn setSendBufferSize(fd: i32, size: u32) OptionError!void {
    return setIntOption(fd, SOL_SOCKET, SO_SNDBUF, @intCast(size));
}

pub fn getSendBufferSize(fd: i32) OptionError!u32 {
    const v = try getIntOption(fd, SOL_SOCKET, SO_SNDBUF);
    return @intCast(@max(0, v));
}

pub fn setRecvBufferSize(fd: i32, size: u32) OptionError!void {
    return setIntOption(fd, SOL_SOCKET, SO_RCVBUF, @intCast(size));
}

pub fn getRecvBufferSize(fd: i32) OptionError!u32 {
    const v = try getIntOption(fd, SOL_SOCKET, SO_RCVBUF);
    return @intCast(@max(0, v));
}

pub fn setBroadcast(fd: i32, enabled: bool) OptionError!void {
    return setBoolOption(fd, SOL_SOCKET, SO_BROADCAST, enabled);
}

/// Set TCP keepalive. `idle`, `interval`, `retries` semantics differ
/// per-platform; `interval` and `retries` are ignored on Darwin.
pub fn setKeepalive(fd: i32, ka: ?Keepalive) OptionError!void {
    const enabled = ka != null;
    try setBoolOption(fd, SOL_SOCKET, SO_KEEPALIVE, enabled);
    if (ka) |k| {
        switch (builtin.os.tag) {
            .macos, .ios, .tvos, .watchos => {
                try setIntOption(fd, IPPROTO_TCP, TCP_KEEPALIVE_DARWIN, @intCast(k.idle));
            },
            .linux => {
                try setIntOption(fd, IPPROTO_TCP, TCP_KEEPIDLE_LINUX, @intCast(k.idle));
                if (k.interval) |iv| try setIntOption(fd, IPPROTO_TCP, TCP_KEEPINTVL_LINUX, @intCast(iv));
                if (k.retries) |rt| try setIntOption(fd, IPPROTO_TCP, TCP_KEEPCNT_LINUX, @intCast(rt));
            },
            .windows => {
                try setIntOption(fd, IPPROTO_TCP, TCP_KEEPIDLE_WINDOWS, @intCast(k.idle));
                if (k.interval) |iv| try setIntOption(fd, IPPROTO_TCP, TCP_KEEPINTVL_WINDOWS, @intCast(iv));
                if (k.retries) |rt| try setIntOption(fd, IPPROTO_TCP, TCP_KEEPCNT_WINDOWS, @intCast(rt));
            },
            else => {},
        }
    }
}

pub fn setLinger(fd: i32, linger: ?Linger) OptionError!void {
    const l = LingerStruct{
        .l_onoff = if (linger == null) 0 else 1,
        .l_linger = if (linger) |x| @intCast(x.timeout) else 0,
    };
    try setRaw(fd, SOL_SOCKET, SO_LINGER, std.mem.asBytes(&l));
}

/// Set IPV6_V6ONLY. When false (and the socket is bound to `::`),
/// the listener accepts both IPv6 *and* IPv4 (via IPv4-mapped IPv6).
/// When true, IPv4 traffic is rejected on the IPv6 socket — you need
/// a separate IPv4 listener.
pub fn setIpv6Only(fd: i32, enabled: bool) OptionError!void {
    return setBoolOption(fd, IPPROTO_IPV6, IPV6_V6ONLY, enabled);
}

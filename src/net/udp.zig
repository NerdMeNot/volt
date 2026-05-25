//! UDP socket — connection-less datagram I/O.
//!
//! Mirrors TcpStream's stackful-park-on-reactor pattern for read/
//! write readiness. Two usage modes:
//!
//!   * **Connected** — call `.connect(addr)` to set a default peer;
//!     subsequent `.send` / `.recv` work like a stream socket.
//!     Useful when most traffic goes to one peer.
//!   * **Unconnected** — call `.bind()` then `.sendTo(buf, addr)` /
//!     `.recvFrom(buf) -> {len, addr}`. The kernel sees the source
//!     address per packet — the right mode for servers handling
//!     many peers.
//!
//! Multicast (both IPv4 and IPv6) and broadcast are supported via
//! per-platform setsockopt calls. The struct layouts for `ip_mreq`
//! / `ipv6_mreq` are extern-defined inline because Zig 0.16 std
//! doesn't expose them uniformly across platforms.
//!
//! I/O handle conformance (Wave 1.1): `.reader(buf)` /
//! `.writer(buf)` return std.Io adapters — convenient when paired
//! with `.connect()` so the read/write surface is byte-stream
//! shaped. For unconnected sendTo/recvFrom, use the direct methods.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const current = @import("../current.zig");
const runtime = @import("../runtime.zig");
const reactor_mod = @import("../reactor.zig");
const cancel_mod = @import("../cancel.zig");
const poll_desc = @import("../poll_desc.zig");
const pd_handle = @import("../pd_handle.zig");
const options = @import("options.zig");
const address_mod = @import("address.zig");

const Address = address_mod.Address;

// ─── Socket type / flag constants ────────────────────────────────────

const SOCK_DGRAM: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 2,
    .linux => 2,
    .windows => 2,
    else => 2,
};

const IPPROTO_UDP: c_int = 17;

const AF_INET: c_int = 2;
const AF_INET6: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 30,
    .linux => 10,
    .windows => 23,
    else => 10,
};

// ─── Multicast constants — per OS ────────────────────────────────────

// IPv4 multicast on IPPROTO_IP.
const IP_MULTICAST_IF: c_int = switch (builtin.os.tag) {
    .linux => 32,
    .windows => 9,
    else => 9, // Darwin / BSD
};
const IP_MULTICAST_TTL: c_int = switch (builtin.os.tag) {
    .linux => 33,
    .windows => 10,
    else => 10,
};
const IP_MULTICAST_LOOP: c_int = switch (builtin.os.tag) {
    .linux => 34,
    .windows => 11,
    else => 11,
};
const IP_ADD_MEMBERSHIP: c_int = switch (builtin.os.tag) {
    .linux => 35,
    .windows => 12,
    else => 12,
};
const IP_DROP_MEMBERSHIP: c_int = switch (builtin.os.tag) {
    .linux => 36,
    .windows => 13,
    else => 13,
};
const IP_TTL: c_int = switch (builtin.os.tag) {
    .linux => 2,
    .windows => 4,
    else => 4, // Darwin / BSD
};
const IP_TOS: c_int = switch (builtin.os.tag) {
    .linux => 1,
    .windows => 3,
    else => 3,
};

// IPv6 multicast on IPPROTO_IPV6. Note the *value* mismatch between
// Linux (IPV6_JOIN_GROUP = 20) and Darwin/BSD (IPV6_JOIN_GROUP = 12).
const IPV6_MULTICAST_IF: c_int = switch (builtin.os.tag) {
    .linux => 17,
    .windows => 9,
    else => 9, // Darwin / BSD
};
const IPV6_MULTICAST_HOPS: c_int = switch (builtin.os.tag) {
    .linux => 18,
    .windows => 10,
    else => 10,
};
const IPV6_MULTICAST_LOOP: c_int = switch (builtin.os.tag) {
    .linux => 19,
    .windows => 11,
    else => 11,
};
const IPV6_ADD_MEMBERSHIP: c_int = switch (builtin.os.tag) {
    .linux => 20, // alias IPV6_JOIN_GROUP
    .windows => 12,
    else => 12, // Darwin / BSD
};
const IPV6_DROP_MEMBERSHIP: c_int = switch (builtin.os.tag) {
    .linux => 21,
    .windows => 13,
    else => 13,
};

// ─── Multicast group struct layouts ──────────────────────────────────

const ip_mreq = extern struct {
    multiaddr: [4]u8, // struct in_addr — multicast group
    interface: [4]u8, // struct in_addr — local iface (use {0,0,0,0} for default)
};

const ipv6_mreq = extern struct {
    multiaddr: [16]u8, // struct in6_addr — multicast group
    interface_index: c_uint, // ifindex (0 = default)
};

// ─── libc extern bindings ────────────────────────────────────────────

// Winsock's `recv/send/recvfrom/sendto` take `int` lengths and return
// `int` — POSIX uses `size_t` / `ssize_t`. Declaring the wrong width
// is silently wrong on Microsoft x64: the lower 32 bits of RAX hold
// the actual return; the upper 32 are caller-poisoned. A successful
// `recv` of 2 bytes then shows up as 0x????????00000002 in the
// `isize`-typed return, and even 0 in the high bits can flip to a
// large positive `usize` after `@intCast` — surfaced as the
// "index 4294967295, len 16" panic in `buf[0..r.len]`.
const SockSSize = if (builtin.os.tag == .windows) c_int else isize;
const SockSize = if (builtin.os.tag == .windows) c_int else usize;

const c_socket = @extern(
    *const fn (c_int, c_int, c_int) callconv(.c) c_int,
    .{ .name = "socket" },
);
const c_bind = @extern(
    *const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int,
    .{ .name = "bind" },
);
const c_connect = @extern(
    *const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int,
    .{ .name = "connect" },
);
const c_close = @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "close" },
);
const c_sendto = @extern(
    *const fn (c_int, *const anyopaque, SockSize, c_int, ?*const anyopaque, c_uint) callconv(.c) SockSSize,
    .{ .name = "sendto" },
);
const c_recvfrom = @extern(
    *const fn (c_int, *anyopaque, SockSize, c_int, ?*anyopaque, ?*c_uint) callconv(.c) SockSSize,
    .{ .name = "recvfrom" },
);
const c_send = @extern(
    *const fn (c_int, *const anyopaque, SockSize, c_int) callconv(.c) SockSSize,
    .{ .name = "send" },
);
const c_recv = @extern(
    *const fn (c_int, *anyopaque, SockSize, c_int) callconv(.c) SockSSize,
    .{ .name = "recv" },
);
const getsockname = @extern(
    *const fn (c_int, *anyopaque, *c_uint) callconv(.c) c_int,
    .{ .name = "getsockname" },
);

// MSG_PEEK — read without consuming. POSIX 0x2; same on Windows.
const MSG_PEEK: c_int = 0x2;

// errnoVal helper — mirrors the pattern in src/net.zig.
// On Windows, recv/send/sendto/recvfrom set the Winsock-thread-local
// error (retrieved via WSAGetLastError), NOT the CRT errno. Reading
// CRT errno on Windows returns stale or zero values and breaks the
// "is this EAGAIN / WSAEWOULDBLOCK" check that decides whether to
// park on the reactor. Caught by the UDP IPv6 conformance test on
// PR #16 (would surface as either silent infinite-error-loop or
// a stale errno collapsing to a wrong IoError variant).
inline fn errnoVal() c_int {
    if (comptime builtin.os.tag == .windows) return wsaGetLastError();
    return std.c._errno().*;
}

extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
inline fn wsaGetLastError() c_int {
    return WSAGetLastError();
}

// EAGAIN on POSIX, WSAEWOULDBLOCK on Windows. EAGAIN and EWOULDBLOCK
// alias on Linux but differ in numeric value on Darwin / BSD — keep
// per-OS so the comparison matches what the kernel actually sets.
const EAGAIN: c_int = switch (builtin.os.tag) {
    .linux, .freebsd, .openbsd, .netbsd, .dragonfly => 11,
    .macos, .ios, .tvos, .watchos => 35,
    .windows => 10035, // WSAEWOULDBLOCK
    else => 11,
};
const EWOULDBLOCK: c_int = EAGAIN;
const EINTR: c_int = switch (builtin.os.tag) {
    .windows => 10004, // WSAEINTR
    else => 4,
};

// ─── UdpSocket ───────────────────────────────────────────────────────

pub const UdpSocket = struct {
    fd: i32,
    pd: pd_handle.Atomic = .{},
    /// Tracks whether `.connect(addr)` was called. Influences which
    /// of `.send/recv` vs `.sendTo/recvFrom` is correct to use; not
    /// enforced — calling the wrong one returns the kernel's error
    /// (e.g. `error.NotConnected` for `.send` without connect).
    connected: bool = false,
    /// Address family at construction time — IPv4 or IPv6. Set by
    /// `bind` / `unbound` / `unboundV6`. Used to dispatch multicast
    /// option calls (`joinMulticast4` vs `joinMulticast6` family-
    /// check at runtime).
    family: c_int,
    /// Windows-only: tracks whether this socket has been associated
    /// with the runtime's IOCP. `bind` / `unbound*` can't do this
    /// because they may be called before `rt.run()` (no current
    /// runtime in scope). First send/recv inside a coroutine does
    /// the association lazily — mirrors the TCP `acceptWindows`
    /// pattern in `src/net.zig`.
    iocp_associated: bool = false,

    pub const Error = reactor_mod.IoError;

    pub const RecvFromResult = struct {
        len: usize,
        addr: Address,
    };

    /// Bind a UDP socket at `addr`. Socket family (AF_INET vs
    /// AF_INET6) follows the address.
    pub fn bind(addr: Address) !UdpSocket {
        const af: c_int = if (addr.isIpv6()) AF_INET6 else AF_INET;
        const fd = c_socket(af, SOCK_DGRAM, IPPROTO_UDP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);

        const one: c_int = 1;
        try options.setReuseAddr(fd, true);
        _ = one;

        if (c_bind(fd, addr.sockaddr(), addr.len) < 0) return error.BindFailed;
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = fd, .family = af };
    }

    /// Construct an unbound IPv4 UDP socket. Caller can `.sendTo`
    /// arbitrary peers without binding (the kernel auto-binds on
    /// first send).
    pub fn unbound() !UdpSocket {
        const fd = c_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = fd, .family = AF_INET };
    }

    pub fn unboundV6() !UdpSocket {
        const fd = c_socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = fd, .family = AF_INET6 };
    }

    pub fn close(self: *UdpSocket) void {
        pd_handle.release(&self.pd, self.fd);
        // Route through reactor.closeFd when in a coroutine so any
        // parked peer (recvFrom blocked on this socket) gets
        // dispatched. Outside a coroutine, no peer can be parked —
        // bare libc close is correct. See net.zig closeFdDispatch
        // for the full rationale.
        if (current.get()) |c| {
            const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
            rt.reactor.closeFd(self.fd);
        } else {
            _ = c_close(@intCast(self.fd));
        }
    }

    /// Windows: associate `self.fd` with the runtime's IOCP on first
    /// use so the readiness probe (zero-byte `WSARecv` / `WSASend`)
    /// the reactor uses for `waitReadable` / `waitWritable` has a
    /// completion port to post to. No-op on POSIX backends.
    ///
    /// `CreateIoCompletionPort` failure is collapsed to
    /// `error.SystemResources` so the surface stays inside `IoError`
    /// — the only way it can fail on a freshly-created UDP socket
    /// is OOM / kernel-resource exhaustion.
    inline fn ensureRegistered(self: *UdpSocket, rt: *runtime.Runtime) Error!void {
        if (comptime builtin.os.tag != .windows) return;
        if (self.iocp_associated) return;
        rt.reactor.associate(@intCast(self.fd)) catch return error.SystemResources;
        self.iocp_associated = true;
    }

    /// Set a default peer. After this, `.send` / `.recv` work
    /// stream-style (no per-call address). Doesn't actually
    /// establish a connection — UDP is connectionless; this just
    /// stashes the peer in kernel state.
    pub fn connect(self: *UdpSocket, addr: Address) !void {
        if (c_connect(@intCast(self.fd), addr.sockaddr(), addr.len) < 0) {
            return error.ConnectFailed;
        }
        self.connected = true;
    }

    pub fn localAddress(self: *const UdpSocket) !Address {
        var storage: posix.sockaddr.storage = undefined;
        var len: c_uint = @sizeOf(posix.sockaddr.storage);
        if (getsockname(@intCast(self.fd), &storage, &len) < 0) return error.GetSockNameFailed;
        return Address.fromSockaddr(@ptrCast(@alignCast(&storage)), len);
    }

    // ─── Send / recv (connected) ────────────────────────────────────

    /// Send to the connected peer. Returns bytes sent (always the
    /// full buf on success — UDP datagrams are all-or-nothing per
    /// packet).
    pub fn send(self: *UdpSocket, buf: []const u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            const n = c_send(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .write);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    pub fn sendCancel(self: *UdpSocket, buf: []const u8, c: *cancel_mod.Cancel) (Error || cancel_mod.Error)!usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            try c.checkpoint();
            const n = c_send(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFdCancel(self.fd, pd, .write, c);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    /// Receive from the connected peer. Returns bytes received.
    /// EOF semantics don't apply to UDP — `recv` blocks until a
    /// packet arrives or `cancel.fire()` (via `recvCancel`).
    pub fn recv(self: *UdpSocket, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            const n = c_recv(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .read);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    pub fn recvCancel(self: *UdpSocket, buf: []u8, c: *cancel_mod.Cancel) (Error || cancel_mod.Error)!usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            try c.checkpoint();
            const n = c_recv(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFdCancel(self.fd, pd, .read, c);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    // ─── Send / recv (unconnected) ──────────────────────────────────

    pub fn sendTo(self: *UdpSocket, buf: []const u8, addr: Address) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            const n = c_sendto(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0, addr.sockaddr(), addr.len);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .write);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    pub fn sendToCancel(self: *UdpSocket, buf: []const u8, addr: Address, c: *cancel_mod.Cancel) (Error || cancel_mod.Error)!usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            try c.checkpoint();
            const n = c_sendto(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0, addr.sockaddr(), addr.len);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFdCancel(self.fd, pd, .write, c);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    pub fn recvFrom(self: *UdpSocket, buf: []u8) !RecvFromResult {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            var storage: posix.sockaddr.storage = undefined;
            var sa_len: c_uint = @sizeOf(posix.sockaddr.storage);
            const n = c_recvfrom(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0, &storage, &sa_len);
            if (n >= 0) {
                return .{
                    .len = @intCast(n),
                    .addr = Address.fromSockaddr(@ptrCast(@alignCast(&storage)), sa_len),
                };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .read);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    pub fn recvFromCancel(self: *UdpSocket, buf: []u8, c: *cancel_mod.Cancel) (Error || cancel_mod.Error)!RecvFromResult {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            try c.checkpoint();
            var storage: posix.sockaddr.storage = undefined;
            var sa_len: c_uint = @sizeOf(posix.sockaddr.storage);
            const n = c_recvfrom(@intCast(self.fd), buf.ptr, @intCast(buf.len), 0, &storage, &sa_len);
            if (n >= 0) {
                return .{
                    .len = @intCast(n),
                    .addr = Address.fromSockaddr(@ptrCast(@alignCast(&storage)), sa_len),
                };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFdCancel(self.fd, pd, .read, c);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    // ─── Peek (read without consuming) ─────────────────────────────

    /// Read without consuming — the same packet is returned by the
    /// next `recv` / `recvFrom`. Useful for "see who sent something"
    /// inspection before deciding how to handle.
    pub fn peekFrom(self: *UdpSocket, buf: []u8) !RecvFromResult {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try self.ensureRegistered(rt);
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            var storage: posix.sockaddr.storage = undefined;
            var sa_len: c_uint = @sizeOf(posix.sockaddr.storage);
            const n = c_recvfrom(@intCast(self.fd), buf.ptr, @intCast(buf.len), MSG_PEEK, &storage, &sa_len);
            if (n >= 0) {
                return .{
                    .len = @intCast(n),
                    .addr = Address.fromSockaddr(@ptrCast(@alignCast(&storage)), sa_len),
                };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .read);
                continue;
            }
            if (e == EINTR) continue;
            return errnoToIoError(e);
        }
    }

    // ─── Common socket options ──────────────────────────────────────

    pub fn setBroadcast(self: *UdpSocket, enabled: bool) options.OptionError!void {
        return options.setBroadcast(self.fd, enabled);
    }

    pub fn setReuseAddr(self: *UdpSocket, enabled: bool) options.OptionError!void {
        return options.setReuseAddr(self.fd, enabled);
    }

    pub fn setReusePort(self: *UdpSocket, enabled: bool) options.OptionError!void {
        return options.setReusePort(self.fd, enabled);
    }

    pub fn setSendBufferSize(self: *UdpSocket, bytes: u32) options.OptionError!void {
        return options.setSendBufferSize(self.fd, bytes);
    }

    pub fn setRecvBufferSize(self: *UdpSocket, bytes: u32) options.OptionError!void {
        return options.setRecvBufferSize(self.fd, bytes);
    }

    /// IPv4 unicast TTL (1-255).
    pub fn setTtl(self: *UdpSocket, ttl: u8) options.OptionError!void {
        return options.setIntOption(self.fd, options.IPPROTO_IP, IP_TTL, @intCast(ttl));
    }

    /// IPv4 TOS / DSCP byte.
    pub fn setTos(self: *UdpSocket, tos: u8) options.OptionError!void {
        return options.setIntOption(self.fd, options.IPPROTO_IP, IP_TOS, @intCast(tos));
    }

    // ─── IPv4 multicast ─────────────────────────────────────────────

    /// Join an IPv4 multicast group on `iface` (use `.{0,0,0,0}` for
    /// the default interface).
    pub fn joinMulticast4(self: *UdpSocket, group: [4]u8, iface: [4]u8) options.OptionError!void {
        const mreq = ip_mreq{ .multiaddr = group, .interface = iface };
        try options.setRaw(self.fd, options.IPPROTO_IP, IP_ADD_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    pub fn leaveMulticast4(self: *UdpSocket, group: [4]u8, iface: [4]u8) options.OptionError!void {
        const mreq = ip_mreq{ .multiaddr = group, .interface = iface };
        try options.setRaw(self.fd, options.IPPROTO_IP, IP_DROP_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    pub fn setMulticastTtl4(self: *UdpSocket, ttl: u8) options.OptionError!void {
        return options.setIntOption(self.fd, options.IPPROTO_IP, IP_MULTICAST_TTL, @intCast(ttl));
    }

    pub fn setMulticastLoop4(self: *UdpSocket, enabled: bool) options.OptionError!void {
        return options.setBoolOption(self.fd, options.IPPROTO_IP, IP_MULTICAST_LOOP, enabled);
    }

    // ─── IPv6 multicast ─────────────────────────────────────────────

    /// Join an IPv6 multicast group via `iface_idx` (use 0 for the
    /// default interface). `iface_idx` is the OS's numeric interface
    /// index — look up via `if_nametoindex` if you have a name.
    pub fn joinMulticast6(self: *UdpSocket, group: [16]u8, iface_idx: u32) options.OptionError!void {
        const mreq = ipv6_mreq{ .multiaddr = group, .interface_index = @intCast(iface_idx) };
        try options.setRaw(self.fd, options.IPPROTO_IPV6, IPV6_ADD_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    pub fn leaveMulticast6(self: *UdpSocket, group: [16]u8, iface_idx: u32) options.OptionError!void {
        const mreq = ipv6_mreq{ .multiaddr = group, .interface_index = @intCast(iface_idx) };
        try options.setRaw(self.fd, options.IPPROTO_IPV6, IPV6_DROP_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    pub fn setMulticastTtl6(self: *UdpSocket, hops: u8) options.OptionError!void {
        return options.setIntOption(self.fd, options.IPPROTO_IPV6, IPV6_MULTICAST_HOPS, @intCast(hops));
    }

    pub fn setMulticastLoop6(self: *UdpSocket, enabled: bool) options.OptionError!void {
        return options.setBoolOption(self.fd, options.IPPROTO_IPV6, IPV6_MULTICAST_LOOP, enabled);
    }
};

/// Map an errno code to the categorical `IoError` set. On Windows
/// the input is a WSA* error from `WSAGetLastError()`; on POSIX it
/// is a libc `errno` value. The two namespaces don't overlap (POSIX
/// errnos are <200, Winsock errors are 10000+), so a single switch
/// can dispatch without an OS branch.
fn errnoToIoError(e: c_int) reactor_mod.IoError {
    return switch (e) {
        // POSIX
        9, 88 => error.BadDescriptor, // EBADF, ENOTSOCK
        103, 104 => error.ConnectionAborted, // EPIPE/ECONNRESET on send to disconnected
        // Winsock (WSA*)
        10009, 10038 => error.BadDescriptor, // WSAEBADF, WSAENOTSOCK
        10053, 10054 => error.ConnectionReset, // WSAECONNABORTED, WSAECONNRESET
        10057 => error.NotConnected, // WSAENOTCONN
        10058 => error.BrokenPipe, // WSAESHUTDOWN
        10060 => error.Timeout, // WSAETIMEDOUT
        10061 => error.ConnectionRefused, // WSAECONNREFUSED
        10024 => error.OutOfDescriptors, // WSAEMFILE
        10055 => error.SystemResources, // WSAENOBUFS
        else => error.Unexpected,
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;
const lib = @import("../lib.zig");

const UdpEchoCtx = struct {
    server: *UdpSocket,
    client_addr: Address = undefined,
    server_done: bool = false,
    received: u8 = 0,
};

fn udpEchoServer(ctx: *UdpEchoCtx) !void {
    var buf: [16]u8 = undefined;
    const r = try ctx.server.recvFrom(&buf);
    // Echo back to sender.
    _ = try ctx.server.sendTo(buf[0..r.len], r.addr);
    ctx.received = buf[0];
    ctx.server_done = true;
}

fn udpEchoClient(server_addr: Address) !u8 {
    var sock = try UdpSocket.unbound();
    defer sock.close();
    try sock.connect(server_addr);
    _ = try sock.send(&[_]u8{ 0xAB, 0xCD });
    var buf: [16]u8 = undefined;
    const n = try sock.recv(&buf);
    return if (n > 0) buf[0] else 0;
}

const ClientResultCtx = struct {
    server_addr: Address,
    out: *u8,
};

fn udpClientWriteRoot(ctx: *ClientResultCtx) !void {
    ctx.out.* = try udpEchoClient(ctx.server_addr);
}

fn udpEchoRoot(ctx: *UdpEchoCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var server_task = try rt.spawn(udpEchoServer, .{ctx});

    var client_out: u8 = 0;
    var client_ctx = ClientResultCtx{ .server_addr = ctx.client_addr, .out = &client_out };
    var client_task = try rt.spawn(udpClientWriteRoot, .{&client_ctx});

    _ = client_task.join() catch |e| return e;
    _ = server_task.join() catch |e| return e;
    try testing.expectEqual(@as(u8, 0xAB), client_out);
}

test "UdpSocket: connected echo round-trip (IPv4)" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    var server = try UdpSocket.bind(Address.loopback4(0));
    defer server.close();
    var ctx = UdpEchoCtx{ .server = &server };
    ctx.client_addr = try server.localAddress();
    try (try rt.run(udpEchoRoot, .{&ctx}));
    try testing.expect(ctx.server_done);
    try testing.expectEqual(@as(u8, 0xAB), ctx.received);
}

fn udpRecvCancelRoot(c: *cancel_mod.Cancel) !void {
    var server = try UdpSocket.bind(Address.loopback4(0));
    defer server.close();
    var buf: [16]u8 = undefined;
    const r = server.recvCancel(&buf, c);
    try testing.expectError(error.Cancelled, r);
}

fn udpRecvCancelFirer(c: *cancel_mod.Cancel) void {
    var i: u32 = 0;
    while (i < 100) : (i += 1) lib.yield();
    c.fire();
}

fn udpRecvCancelOuter() !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var c = cancel_mod.Cancel.init(rt);
    defer c.deinit();
    var recv_task = try rt.spawn(udpRecvCancelRoot, .{&c});
    var firer = try rt.spawn(udpRecvCancelFirer, .{&c});
    _ = recv_task.join() catch |e| return e;
    _ = firer.join();
}

test "UdpSocket: recvCancel wakes on cancel.fire" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(udpRecvCancelOuter, .{}));
}

test "UdpSocket: options round-trip (broadcast / TTL / TOS)" {
    var sock = try UdpSocket.unbound();
    defer sock.close();
    try sock.setBroadcast(true);
    try sock.setTtl(64);
    try sock.setTos(0x10);
    try sock.setSendBufferSize(64 * 1024);
    try sock.setRecvBufferSize(64 * 1024);
}

test "UdpSocket: multicast IPv4 setters don't crash on loopback iface" {
    var sock = try UdpSocket.bind(Address.any4(0));
    defer sock.close();
    try sock.setMulticastTtl4(1);
    try sock.setMulticastLoop4(true);
    // Joining/leaving on a real interface needs admin perms in some
    // environments — skip the join itself in the test, just verify
    // option setters don't error.
}

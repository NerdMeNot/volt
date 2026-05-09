//! `volt.net.UdpSocket` — async UDP datagram socket.
//!
//! Same shape as `TcpStream` (non-blocking fd, EAGAIN-park pattern)
//! but message-oriented: `sendTo` / `recvFrom` carry an `Address`
//! per call, no stream framing.
//!
//! Supports the connect-then-send pattern too: after `connect`,
//! `send` / `recv` work without per-call addresses (the kernel
//! filters incoming datagrams to the connected peer and lets you
//! call send() instead of sendto()).
//!
//! Multicast: `joinMulticast` / `leaveMulticast` for group
//! membership; `setMulticastTtl`, `setMulticastLoopback` for the
//! associated tunables. IPv4 uses `ip_mreq`; IPv6 uses `ipv6_mreq`
//! (with interface index).

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io_errors = @import("../io/errors.zig");
const sockopt = @import("sockopt.zig");
const Address = @import("Address.zig").Address;

// std.posix.IP is `void` on Darwin in Zig 0.16 — the multicast
// constants we need aren't exposed cross-platform. Define our own
// platform-keyed namespace; values cross-checked against
// /usr/include/netinet/in.h and Linux kernel headers.
const ip = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit => struct {
        pub const MULTICAST_TTL: u32 = 10;
        pub const MULTICAST_LOOP: u32 = 11;
        pub const ADD_MEMBERSHIP: u32 = 12;
        pub const DROP_MEMBERSHIP: u32 = 13;
    },
    .linux => struct {
        pub const MULTICAST_TTL: u32 = 33;
        pub const MULTICAST_LOOP: u32 = 34;
        pub const ADD_MEMBERSHIP: u32 = 35;
        pub const DROP_MEMBERSHIP: u32 = 36;
    },
    else => struct {
        pub const MULTICAST_TTL: u32 = 0;
        pub const MULTICAST_LOOP: u32 = 0;
        pub const ADD_MEMBERSHIP: u32 = 0;
        pub const DROP_MEMBERSHIP: u32 = 0;
    },
};

const ipv6 = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit => struct {
        pub const JOIN_GROUP: u32 = 12;
        pub const LEAVE_GROUP: u32 = 13;
    },
    .linux => struct {
        pub const JOIN_GROUP: u32 = 20;
        pub const LEAVE_GROUP: u32 = 21;
    },
    else => struct {
        pub const JOIN_GROUP: u32 = 0;
        pub const LEAVE_GROUP: u32 = 0;
    },
};

pub const BindError =
    io_errors.SocketError ||
    io_errors.BindError ||
    io_errors.FcntlError;

pub const ConnectError =
    io_errors.ConnectError ||
    io_errors.GetSockOptError ||
    error{ConnectFailed};

pub const SendError = io_errors.SendError;
pub const RecvError = io_errors.RecvError;

/// Result of `recvFrom` — payload length plus source address.
pub const Datagram = struct {
    len: usize,
    addr: Address,
};

/// IPv4 multicast group descriptor — kernel ABI.
const IpMreq = extern struct {
    /// Multicast group, network byte order.
    multiaddr: u32,
    /// Local interface (any = `INADDR_ANY` = 0).
    interface: u32,
};

/// IPv6 multicast group descriptor — kernel ABI.
const Ipv6Mreq = extern struct {
    multiaddr: [16]u8,
    /// Interface index (`if_nametoindex(name)`); 0 = default.
    interface: u32,
};

pub const UdpSocket = struct {
    fd: posix.socket_t,

    /// Bind a UDP socket to `address`. Non-blocking.
    pub fn bind(address: Address) BindError!UdpSocket {
        const sock_type = posix.SOCK.DGRAM | syscall.SOCK_NONBLOCK | syscall.SOCK_CLOEXEC;
        const fd = try syscall.socket(address.family(), sock_type, 0);
        errdefer syscall.close(fd);
        try syscall.bind(fd, &address.any, address.osSockLen());
        return .{ .fd = fd };
    }

    pub fn close(self: *UdpSocket) void {
        syscall.close(self.fd);
    }

    /// "Connect" the socket to a peer — for UDP this just sets the
    /// default destination + filter. Subsequent `send` / `recv`
    /// can be used in place of sendTo/recvFrom.
    pub fn connect(self: *UdpSocket, address: Address) ConnectError!void {
        syscall.connect(self.fd, &address.any, address.osSockLen()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => {
                // UDP connect is synchronous in the kernel; either it
                // succeeds immediately or fails. WouldBlock here is
                // unexpected — treat as ConnectFailed.
                return error.ConnectFailed;
            },
            else => return err,
        };
    }

    /// Send a datagram to `address`.
    pub fn sendTo(self: *UdpSocket, buf: []const u8, address: Address) SendError!usize {
        while (true) {
            const r = syscall.sendto(self.fd, buf, 0, &address.any, address.osSockLen()) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitWritable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    /// Receive a datagram. Returns the byte count and the source
    /// address. Buffer should be sized to the largest expected
    /// datagram; the kernel truncates excess.
    pub fn recvFrom(self: *UdpSocket, buf: []u8) RecvError!Datagram {
        while (true) {
            var addr: Address = undefined;
            var addrlen: posix.socklen_t = @sizeOf(Address);
            const r = syscall.recvfrom(self.fd, buf, 0, &addr.any, &addrlen) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitReadable(self.fd);
                    continue;
                },
                else => return err,
            };
            return .{ .len = r, .addr = addr };
        }
    }

    /// Send to the connected peer (after `connect`).
    pub fn send(self: *UdpSocket, buf: []const u8) SendError!usize {
        while (true) {
            const r = syscall.send(self.fd, buf, 0) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitWritable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    /// Receive from the connected peer.
    pub fn recv(self: *UdpSocket, buf: []u8) RecvError!usize {
        while (true) {
            const r = syscall.recv(self.fd, buf, 0) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitReadable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    /// Read the locally bound address.
    pub fn localAddress(self: *const UdpSocket) !Address {
        var addr: Address = undefined;
        var len: posix.socklen_t = @sizeOf(Address);
        try syscall.getsockname(self.fd, &addr.any, &len);
        return addr;
    }

    // ── Multicast ──────────────────────────────────────────────────────

    /// Join `group` on the given interface. `interface_index` is
    /// from `if_nametoindex` (Linux/IPv6) or 0 for "kernel choose".
    /// IPv4 multicast on Darwin doesn't take an ifindex — pass 0;
    /// the kernel uses `INADDR_ANY` for the local interface.
    pub fn joinMulticast(self: *UdpSocket, group: Address, interface_index: u32) sockopt.SetOptionError!void {
        return membershipChange(self.fd, group, interface_index, true);
    }

    pub fn leaveMulticast(self: *UdpSocket, group: Address, interface_index: u32) sockopt.SetOptionError!void {
        return membershipChange(self.fd, group, interface_index, false);
    }

    pub fn setMulticastTtl(self: *UdpSocket, ttl: u8) sockopt.SetOptionError!void {
        var v: c_int = ttl;
        try syscall.setsockopt(self.fd, posix.IPPROTO.IP, ip.MULTICAST_TTL, std.mem.asBytes(&v));
    }

    pub fn setMulticastLoopback(self: *UdpSocket, value: bool) sockopt.SetOptionError!void {
        var v: c_int = if (value) 1 else 0;
        try syscall.setsockopt(self.fd, posix.IPPROTO.IP, ip.MULTICAST_LOOP, std.mem.asBytes(&v));
    }

    pub fn setBroadcast(self: *UdpSocket, value: bool) sockopt.SetOptionError!void {
        return sockopt.setBroadcast(self.fd, value);
    }
};

fn membershipChange(
    fd: posix.socket_t,
    group: Address,
    interface_index: u32,
    join: bool,
) sockopt.SetOptionError!void {
    // IPv4: ip_mreq doesn't carry an ifindex on Darwin — interface_index is
    // ignored on the v4 branch and used only on the v6 branch below.
    switch (group.family()) {
        posix.AF.INET => {
            const opt: u32 = if (join) ip.ADD_MEMBERSHIP else ip.DROP_MEMBERSHIP;
            const mreq = IpMreq{
                .multiaddr = group.in.addr,
                .interface = 0, // INADDR_ANY
            };
            try syscall.setsockopt(fd, posix.IPPROTO.IP, opt, std.mem.asBytes(&mreq));
        },
        posix.AF.INET6 => {
            const opt: u32 = if (join) ipv6.JOIN_GROUP else ipv6.LEAVE_GROUP;
            const mreq = Ipv6Mreq{
                .multiaddr = group.in6.addr,
                .interface = interface_index,
            };
            try syscall.setsockopt(fd, posix.IPPROTO.IPV6, opt, std.mem.asBytes(&mreq));
        },
        else => return error.InvalidProtocolOption,
    }
}

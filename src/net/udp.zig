//! UDP Socket - Production-Quality Implementation
//!
//! Provides production-grade UDP networking with:
//! - One-to-many: bind() + sendTo()/recvFrom() for multiple remotes
//! - One-to-one: connect() + send()/recv() for single remote
//! - Full socket options (broadcast, multicast, TTL, TOS)
//! - Non-blocking try* methods and async futures
//!
//! ## Echo Server Example (one-to-many)
//!
//! ```zig
//! var socket = try UdpSocket.bind(Address.fromPort(8080));
//! defer socket.close();
//!
//! var buf: [1024]u8 = undefined;
//! while (true) {
//!     if (try socket.tryRecvFrom(&buf)) |result| {
//!         _ = try socket.trySendTo(buf[0..result.len], result.addr);
//!     }
//! }
//! ```
//!
//! ## Client Example (one-to-one)
//!
//! ```zig
//! var socket = try UdpSocket.bind(Address.fromPort(0)); // Any port
//! try socket.connect(server_addr);
//!
//! _ = try socket.trySend("hello");
//! const n = try socket.tryRecv(&buf) orelse 0;
//! ```

const std = @import("std");
const posix = std.posix;
const mem = std.mem;
const builtin = @import("builtin");

const Address = @import("address.zig").Address;
const Duration = @import("../time.zig").Duration;
const io = @import("../stream.zig");
const ScheduledIo = @import("../internal/backend/scheduled_io.zig").ScheduledIo;
const Ready = @import("../internal/backend/scheduled_io.zig").Ready;
const Interest = @import("../internal/backend/scheduled_io.zig").Interest;
const Waker = @import("../internal/backend/scheduled_io.zig").Waker;
const FutureWaker = @import("../future/Waker.zig").Waker;
const FutureContext = @import("../future/Waker.zig").Context;
const FuturePollResult = @import("../future/Poll.zig").PollResult;
const runtime_mod = @import("../runtime.zig");
const IoDriver = @import("../internal/io_driver.zig").IoDriver;
const xplat = @import("tcp/common.zig");

// IP socket options (cross-platform definitions)
// Linux values from linux/in.h, macOS/BSD values from netinet/in.h
const IP = struct {
    const TOS: u32 = if (builtin.os.tag == .linux) 1 else 3;
    const TTL: u32 = if (builtin.os.tag == .linux) 2 else 4;
    const MULTICAST_TTL: u32 = if (builtin.os.tag == .linux) 33 else 10;
    const MULTICAST_LOOP: u32 = if (builtin.os.tag == .linux) 34 else 11;
    const ADD_MEMBERSHIP: u32 = if (builtin.os.tag == .linux) 35 else 12;
    const DROP_MEMBERSHIP: u32 = if (builtin.os.tag == .linux) 36 else 13;
};

const IPV6 = struct {
    const MULTICAST_LOOP: u32 = if (builtin.os.tag == .linux) 19 else 11;
    const JOIN_GROUP: u32 = if (builtin.os.tag == .linux) 20 else 12;
    const LEAVE_GROUP: u32 = if (builtin.os.tag == .linux) 21 else 13;
};

// ═══════════════════════════════════════════════════════════════════════════════
// UdpSocket
// ═══════════════════════════════════════════════════════════════════════════════

/// A UDP socket for sending and receiving datagrams.
///
/// UDP is connectionless - a single socket can communicate with many remotes.
/// Two usage patterns:
///
/// 1. **One-to-many**: Use `sendTo()`/`recvFrom()` to communicate with any address
/// 2. **One-to-one**: Call `connect()` then use `send()`/`recv()` for a single peer
///
/// Unlike TCP, UDP does not need splitting - all methods take `&self` (not `&mut self`),
/// so the socket can be shared via `*UdpSocket` across multiple contexts.
pub const UdpSocket = struct {
    fd: posix.socket_t,
    local_addr: Address,
    peer_addr: ?Address,
    scheduled_io: ?*ScheduledIo,

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Bind to an address. Use port 0 to let the OS assign a port.
    pub fn bind(addr: Address) !UdpSocket {
        const family = addr.family();
        const fd = try posix.socket(
            family,
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );
        errdefer posix.close(fd);

        // Bind to address
        try posix.bind(fd, addr.sockaddr(), addr.len);

        // Get actual bound address (important when port was 0)
        var local_addr: Address = undefined;
        local_addr.len = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(fd, local_addr.sockaddrMut(), &local_addr.len);

        return .{
            .fd = fd,
            .local_addr = local_addr,
            .peer_addr = null,
            .scheduled_io = null,
        };
    }

    /// Create an unbound socket (IPv4). Must bind before receiving.
    pub fn unbound() !UdpSocket {
        return unboundWithFamily(posix.AF.INET);
    }

    /// Create an unbound socket with specific address family.
    pub fn unboundWithFamily(family: u32) !UdpSocket {
        const fd = try posix.socket(
            family,
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );
        return .{
            .fd = fd,
            .local_addr = undefined,
            .peer_addr = null,
            .scheduled_io = null,
        };
    }

    /// Create from a raw file descriptor (takes ownership).
    pub fn fromRaw(fd: posix.socket_t) UdpSocket {
        return .{
            .fd = fd,
            .local_addr = undefined,
            .peer_addr = null,
            .scheduled_io = null,
        };
    }

    /// Create from a standard library UDP socket.
    /// Takes ownership of the underlying file descriptor.
    pub fn fromStd(std_socket: std.net.DatagramSocket) UdpSocket {
        var local_addr: Address = undefined;
        local_addr.len = @sizeOf(posix.sockaddr.storage);
        posix.getsockname(std_socket.handle, local_addr.sockaddrMut(), &local_addr.len) catch {};

        return .{
            .fd = std_socket.handle,
            .local_addr = local_addr,
            .peer_addr = null,
            .scheduled_io = null,
        };
    }

    /// Convert to a standard library UDP socket.
    /// The UdpSocket should not be used after this call.
    pub fn toStd(self: *UdpSocket) std.net.DatagramSocket {
        return .{ .handle = self.fd };
    }

    /// Close the socket.
    pub fn close(self: *UdpSocket) void {
        posix.close(self.fd);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Connection (for one-to-one pattern)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Connect to a remote address.
    ///
    /// After connecting:
    /// - `send()` and `recv()` work without specifying address
    /// - `recv()` only receives from the connected peer
    /// - The socket is NOT actually connected (UDP is connectionless)
    ///   but the kernel filters incoming packets and remembers the destination
    pub fn connect(self: *UdpSocket, addr: Address) !void {
        try posix.connect(self.fd, addr.sockaddr(), addr.len);
        self.peer_addr = addr;
    }

    /// Disconnect (clear the connected peer).
    /// After this, must use sendTo()/recvFrom() again.
    pub fn disconnect(self: *UdpSocket) !void {
        // Connect to AF_UNSPEC to disconnect
        var unspec: posix.sockaddr = undefined;
        unspec.family = posix.AF.UNSPEC;
        posix.connect(self.fd, &unspec, @sizeOf(posix.sockaddr)) catch |err| {
            // Some systems return EAFNOSUPPORT, which is fine
            if (err != error.AddressFamilyNotSupported) return err;
        };
        self.peer_addr = null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Sending (one-to-one, requires connect())
    // ═══════════════════════════════════════════════════════════════════════════

    /// Send data to the connected peer. Returns bytes sent.
    /// Returns null if would block.
    pub fn trySend(self: *UdpSocket, data: []const u8) !?usize {
        const n = xplat.send(self.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Sending (one-to-many)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Send data to a specific address. Returns bytes sent.
    /// Returns null if would block.
    pub fn trySendTo(self: *UdpSocket, data: []const u8, addr: Address) !?usize {
        const n = xplat.sendto(self.fd, data, 0, addr.sockaddr(), addr.len) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Receiving (one-to-one, requires connect())
    // ═══════════════════════════════════════════════════════════════════════════

    /// Receive data from the connected peer.
    /// Returns bytes received, or null if would block.
    /// Returns 0 on orderly shutdown (though UDP doesn't really have this).
    pub fn tryRecv(self: *UdpSocket, buf: []u8) !?usize {
        const n = xplat.recv(self.fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Receiving (one-to-many)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Receive result with sender address.
    pub const RecvFromResult = struct {
        len: usize,
        addr: Address,
    };

    /// Receive data from any sender.
    /// Returns bytes received and sender address, or null if would block.
    pub fn tryRecvFrom(self: *UdpSocket, buf: []u8) !?RecvFromResult {
        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);

        const n = xplat.recvfrom(self.fd, buf, 0, addr.sockaddrMut(), &addr.len) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        return .{ .len = n, .addr = addr };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Peeking (read without consuming)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Peek at data without removing it from the queue.
    /// Returns null if would block.
    pub fn tryPeek(self: *UdpSocket, buf: []u8) !?usize {
        const n = xplat.recv(self.fd, buf, posix.MSG.PEEK) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Peek at data and get sender address.
    pub fn tryPeekFrom(self: *UdpSocket, buf: []u8) !?RecvFromResult {
        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);

        const n = xplat.recvfrom(self.fd, buf, posix.MSG.PEEK, addr.sockaddrMut(), &addr.len) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        return .{ .len = n, .addr = addr };
    }

    /// Peek just the sender address (without reading data).
    pub fn tryPeekSender(self: *UdpSocket) !?Address {
        var buf: [1]u8 = undefined;
        const result = try self.tryPeekFrom(&buf);
        if (result) |r| {
            return r.addr;
        }
        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Address Information
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the local address this socket is bound to.
    pub fn localAddr(self: UdpSocket) Address {
        return self.local_addr;
    }

    /// Get the connected peer address (if connected).
    pub fn peerAddr(self: UdpSocket) ?Address {
        return self.peer_addr;
    }

    /// Get the underlying file descriptor.
    pub fn fileno(self: UdpSocket) posix.socket_t {
        return self.fd;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - Broadcast
    // ═══════════════════════════════════════════════════════════════════════════

    /// Check if broadcast is enabled.
    pub fn getBroadcast(self: UdpSocket) !bool {
        return getBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.BROADCAST);
    }

    /// Enable or disable broadcast.
    /// Required for sending to broadcast addresses (e.g., 255.255.255.255).
    pub fn setBroadcast(self: *UdpSocket, enabled: bool) !void {
        try setBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.BROADCAST, enabled);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - TTL
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the IP TTL (Time To Live).
    pub fn getTtl(self: UdpSocket) !u32 {
        return getU32Option(self.fd, posix.IPPROTO.IP, IP.TTL);
    }

    /// Set the IP TTL. Limits how many hops the packet can traverse.
    pub fn setTtl(self: *UdpSocket, ttl: u32) !void {
        try setU32Option(self.fd, posix.IPPROTO.IP, IP.TTL, ttl);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - Multicast
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get multicast TTL for IPv4.
    pub fn getMulticastTtlV4(self: UdpSocket) !u32 {
        return getU32Option(self.fd, posix.IPPROTO.IP, IP.MULTICAST_TTL);
    }

    /// Set multicast TTL for IPv4.
    pub fn setMulticastTtlV4(self: *UdpSocket, ttl: u32) !void {
        try setU32Option(self.fd, posix.IPPROTO.IP, IP.MULTICAST_TTL, ttl);
    }

    /// Check if multicast loopback is enabled for IPv4.
    pub fn getMulticastLoopV4(self: UdpSocket) !bool {
        return getBoolOption(self.fd, posix.IPPROTO.IP, IP.MULTICAST_LOOP);
    }

    /// Enable or disable multicast loopback for IPv4.
    /// When enabled, sent multicast packets are looped back to local sockets.
    pub fn setMulticastLoopV4(self: *UdpSocket, enabled: bool) !void {
        try setBoolOption(self.fd, posix.IPPROTO.IP, IP.MULTICAST_LOOP, enabled);
    }

    /// Check if multicast loopback is enabled for IPv6.
    pub fn getMulticastLoopV6(self: UdpSocket) !bool {
        return getBoolOption(self.fd, posix.IPPROTO.IPV6, IPV6.MULTICAST_LOOP);
    }

    /// Enable or disable multicast loopback for IPv6.
    pub fn setMulticastLoopV6(self: *UdpSocket, enabled: bool) !void {
        try setBoolOption(self.fd, posix.IPPROTO.IPV6, IPV6.MULTICAST_LOOP, enabled);
    }

    /// Join an IPv4 multicast group.
    pub fn joinMulticastV4(self: *UdpSocket, multicast_addr: [4]u8, interface_addr: [4]u8) !void {
        const mreq = posix.ip_mreq{
            .multiaddr = .{ .s_addr = @bitCast(multicast_addr) },
            .interface = .{ .s_addr = @bitCast(interface_addr) },
        };
        try xplat.setsockopt(self.fd, posix.IPPROTO.IP, IP.ADD_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    /// Leave an IPv4 multicast group.
    pub fn leaveMulticastV4(self: *UdpSocket, multicast_addr: [4]u8, interface_addr: [4]u8) !void {
        const mreq = posix.ip_mreq{
            .multiaddr = .{ .s_addr = @bitCast(multicast_addr) },
            .interface = .{ .s_addr = @bitCast(interface_addr) },
        };
        try xplat.setsockopt(self.fd, posix.IPPROTO.IP, IP.DROP_MEMBERSHIP, std.mem.asBytes(&mreq));
    }

    /// Join an IPv6 multicast group.
    pub fn joinMulticastV6(self: *UdpSocket, multicast_addr: [16]u8, interface_index: u32) !void {
        const mreq = posix.ipv6_mreq{
            .multiaddr = multicast_addr,
            .interface = interface_index,
        };
        try xplat.setsockopt(self.fd, posix.IPPROTO.IPV6, IPV6.JOIN_GROUP, std.mem.asBytes(&mreq));
    }

    /// Leave an IPv6 multicast group.
    pub fn leaveMulticastV6(self: *UdpSocket, multicast_addr: [16]u8, interface_index: u32) !void {
        const mreq = posix.ipv6_mreq{
            .multiaddr = multicast_addr,
            .interface = interface_index,
        };
        try xplat.setsockopt(self.fd, posix.IPPROTO.IPV6, IPV6.LEAVE_GROUP, std.mem.asBytes(&mreq));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - Buffer Sizes
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the send buffer size.
    pub fn getSendBufferSize(self: UdpSocket) !u32 {
        return getU32Option(self.fd, posix.SOL.SOCKET, posix.SO.SNDBUF);
    }

    /// Set the send buffer size.
    pub fn setSendBufferSize(self: *UdpSocket, size: u32) !void {
        try setU32Option(self.fd, posix.SOL.SOCKET, posix.SO.SNDBUF, size);
    }

    /// Get the receive buffer size.
    pub fn getRecvBufferSize(self: UdpSocket) !u32 {
        return getU32Option(self.fd, posix.SOL.SOCKET, posix.SO.RCVBUF);
    }

    /// Set the receive buffer size.
    pub fn setRecvBufferSize(self: *UdpSocket, size: u32) !void {
        try setU32Option(self.fd, posix.SOL.SOCKET, posix.SO.RCVBUF, size);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - TOS (Type of Service) / DSCP
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get IP TOS value (Type of Service / DSCP).
    pub fn getTos(self: UdpSocket) !u32 {
        return getU32Option(self.fd, posix.IPPROTO.IP, IP.TOS);
    }

    /// Set IP TOS value (Type of Service / DSCP).
    /// Common values: 0 (normal), 0x10 (low delay), 0x08 (high throughput).
    pub fn setTos(self: *UdpSocket, tos: u32) !void {
        try setU32Option(self.fd, posix.IPPROTO.IP, IP.TOS, tos);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options - Reuse
    // ═══════════════════════════════════════════════════════════════════════════

    /// Check if SO_REUSEADDR is enabled.
    pub fn getReuseAddr(self: UdpSocket) !bool {
        return getBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.REUSEADDR);
    }

    /// Enable or disable SO_REUSEADDR.
    /// Allows multiple sockets to bind to the same address (useful for multicast).
    pub fn setReuseAddr(self: *UdpSocket, enabled: bool) !void {
        try setBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, enabled);
    }

    /// Check if SO_REUSEPORT is enabled (Linux/BSD only).
    pub fn getReusePort(self: UdpSocket) !bool {
        if (comptime builtin.os.tag == .linux or builtin.os.tag == .macos or
            builtin.os.tag == .freebsd or builtin.os.tag == .openbsd)
        {
            return getBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.REUSEPORT);
        }
        return error.NotSupported;
    }

    /// Enable or disable SO_REUSEPORT (Linux/BSD only).
    /// Allows multiple sockets to bind to the same port for load balancing.
    pub fn setReusePort(self: *UdpSocket, enabled: bool) !void {
        if (comptime builtin.os.tag == .linux or builtin.os.tag == .macos or
            builtin.os.tag == .freebsd or builtin.os.tag == .openbsd)
        {
            try setBoolOption(self.fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, enabled);
        } else {
            return error.NotSupported;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Error Handling
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get and clear the pending socket error.
    pub fn takeError(self: *UdpSocket) !?anyerror {
        const err_val = try getU32Option(self.fd, posix.SOL.SOCKET, posix.SO.ERROR);
        if (err_val == 0) return null;
        return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(err_val))));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Async Readiness Methods
    // ═══════════════════════════════════════════════════════════════════════════

    /// Returns a future that resolves when the socket becomes readable.
    ///
    /// "Readable" means `tryRecv()` or `tryRecvFrom()` would return data
    /// (not necessarily that it would return all the requested data).
    pub fn readable(self: *UdpSocket) ReadableFuture {
        return .{ .socket = self };
    }

    /// Returns a future that resolves when the socket becomes writable.
    ///
    /// "Writable" means `trySend()` or `trySendTo()` would accept data.
    pub fn writable(self: *UdpSocket) WritableFuture {
        return .{ .socket = self };
    }

    /// Returns a future that resolves when the socket is ready for the specified interest.
    ///
    /// This allows waiting for both read and write readiness simultaneously.
    pub fn ready(self: *UdpSocket, interest: Interest) ReadyFuture {
        return .{ .socket = self, .interest = interest };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Async Send/Recv (connected mode)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Returns a future that sends data to the connected peer.
    /// The socket must be connected via `connect()` first.
    pub fn send(self: *UdpSocket, data: []const u8) SendFuture {
        return .{ .socket = self, .data = data };
    }

    /// Returns a future that receives data from the connected peer.
    /// The socket must be connected via `connect()` first.
    pub fn recv(self: *UdpSocket, buf: []u8) RecvFuture {
        return .{ .socket = self, .buf = buf };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Async Send/Recv (unconnected mode)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Returns a future that sends data to a specific address.
    pub fn sendTo(self: *UdpSocket, data: []const u8, addr: Address) SendToFuture {
        return .{ .socket = self, .data = data, .addr = addr };
    }

    /// Returns a future that receives data from any sender.
    pub fn recvFrom(self: *UdpSocket, buf: []u8) RecvFromFuture {
        return .{ .socket = self, .buf = buf };
    }

    /// Returns a future that peeks at data without consuming it.
    pub fn peek(self: *UdpSocket, buf: []u8) PeekFuture {
        return .{ .socket = self, .buf = buf };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Custom I/O Operations
    // ═══════════════════════════════════════════════════════════════════════════

    /// Execute a custom I/O operation when the socket is ready.
    ///
    /// This is useful for operations not directly exposed by UdpSocket,
    /// such as vectored I/O or MSG_TRUNC inspection.
    ///
    /// The function should return null if it would block, allowing the
    /// readiness tracking to work correctly.
    pub fn tryIo(
        self: *UdpSocket,
        interest: Interest,
        comptime io_fn: fn (*UdpSocket) anyerror!?usize,
    ) !?usize {
        // First, clear readiness if we have scheduled_io
        if (self.scheduled_io) |sio| {
            _ = sio.clearReadiness(if (interest.readable) Ready.readable() else Ready.writable());
        }

        // Try the operation
        return io_fn(self);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reader/Writer Interfaces (for DTLS composition)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get a polymorphic Reader interface for this socket.
    ///
    /// **Note:** Requires the socket to be connected via `connect()`.
    /// For unconnected sockets, use `tryRecvFrom()` instead.
    ///
    /// Enables layered stream composition (DTLS, etc.):
    /// ```zig
    /// var udp = try UdpSocket.bind(Address.fromPort(0));
    /// try udp.connect(server_addr);
    /// var dtls = try DtlsStream.init(udp.reader(), udp.writer());
    /// ```
    pub fn reader(self: *UdpSocket) io.Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = udpReadFn,
        };
    }

    /// Get a polymorphic Writer interface for this socket.
    ///
    /// **Note:** Requires the socket to be connected via `connect()`.
    /// For unconnected sockets, use `trySendTo()` instead.
    ///
    /// Enables layered stream composition (DTLS, etc.):
    /// ```zig
    /// var udp = try UdpSocket.bind(Address.fromPort(0));
    /// try udp.connect(server_addr);
    /// var dtls = try DtlsStream.init(udp.reader(), udp.writer());
    /// ```
    pub fn writer(self: *UdpSocket) io.Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = udpWriteFn,
            .flushFn = null, // UDP doesn't buffer
        };
    }

    fn udpReadFn(ctx: *anyopaque, buffer: []u8) io.Error!usize {
        const self: *UdpSocket = @ptrCast(@alignCast(ctx));
        const n = xplat.recv(self.fd, buffer, 0) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.ConnectionRefused => return error.ConnectionRefused,
            error.ConnectionResetByPeer => return error.ConnectionReset,
            error.ConnectionTimedOut => return error.TimedOut,
            else => return error.Unexpected,
        };
        return n;
    }

    fn udpWriteFn(ctx: *anyopaque, data: []const u8) io.Error!usize {
        const self: *UdpSocket = @ptrCast(@alignCast(ctx));
        const n = xplat.send(self.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.ConnectionRefused => return error.ConnectionRefused,
            error.ConnectionResetByPeer => return error.ConnectionReset,
            error.NetworkUnreachable => return error.NetworkUnreachable,
            error.BrokenPipe => return error.BrokenPipe,
            else => return error.Unexpected,
        };
        return n;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Async Futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Future for waiting until socket becomes readable.
pub const ReadableFuture = struct {
    pub const Output = anyerror!void;

    socket: *UdpSocket,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *ReadableFuture, ctx: *FutureContext) FuturePollResult(Output) {
        // Try a zero-length peek to check readiness
        _ = xplat.recv(self.socket.fd, &[_]u8{}, posix.MSG.PEEK | posix.MSG.DONTWAIT) catch |err| switch (err) {
            error.WouldBlock => {
                // Register waker for read readiness
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = {} };
    }

    pub fn deinit(self: *ReadableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for waiting until socket becomes writable.
pub const WritableFuture = struct {
    pub const Output = anyerror!void;

    socket: *UdpSocket,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *WritableFuture, ctx: *FutureContext) FuturePollResult(Output) {
        // Check write readiness via getsockopt
        const err_val = getU32Option(self.socket.fd, posix.SOL.SOCKET, posix.SO.ERROR) catch |err| {
            return .{ .ready = err };
        };
        if (err_val != 0) {
            return .{ .ready = posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(err_val)))) };
        }

        // Register waker for write readiness
        if (self.socket.scheduled_io) |sio| {
            // If already writable, return ready
            const event = sio.readiness();
            if (event.ready.isWritable()) {
                return .{ .ready = {} };
            }
            updateStoredWaker(&self.stored_waker, ctx);
            sio.setWriterWaker(bridgeWaker(&self.stored_waker));
        }

        return .pending;
    }

    pub fn deinit(self: *WritableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for waiting until socket is ready for specified interest.
pub const ReadyFuture = struct {
    pub const Output = Ready;

    socket: *UdpSocket,
    interest: Interest,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *ReadyFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.socket.scheduled_io) |sio| {
            const event = sio.readiness();
            // Match readiness against interest
            var matched = Ready{};
            if (self.interest.readable and event.ready.isReadable()) {
                matched.readable = true;
            }
            if (self.interest.writable and event.ready.isWritable()) {
                matched.writable = true;
            }
            if (matched.readable or matched.writable) {
                return .{ .ready = matched };
            }

            updateStoredWaker(&self.stored_waker, ctx);
            if (self.interest.readable) {
                sio.setReaderWaker(bridgeWaker(&self.stored_waker));
            }
            if (self.interest.writable) {
                sio.setWriterWaker(bridgeWaker(&self.stored_waker));
            }
        }
        return .pending;
    }

    pub fn deinit(self: *ReadyFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for sending data (connected mode).
pub const SendFuture = struct {
    pub const Output = anyerror!usize;

    socket: *UdpSocket,
    data: []const u8,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *SendFuture, ctx: *FutureContext) FuturePollResult(Output) {
        const n = xplat.send(self.socket.fd, self.data, 0) catch |err| switch (err) {
            error.WouldBlock => {
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setWriterWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = n };
    }

    pub fn deinit(self: *SendFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for receiving data (connected mode).
pub const RecvFuture = struct {
    pub const Output = anyerror!usize;

    socket: *UdpSocket,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *RecvFuture, ctx: *FutureContext) FuturePollResult(Output) {
        const n = xplat.recv(self.socket.fd, self.buf, 0) catch |err| switch (err) {
            error.WouldBlock => {
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = n };
    }

    pub fn deinit(self: *RecvFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for sending data to a specific address.
pub const SendToFuture = struct {
    pub const Output = anyerror!usize;

    socket: *UdpSocket,
    data: []const u8,
    addr: Address,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *SendToFuture, ctx: *FutureContext) FuturePollResult(Output) {
        const n = xplat.sendto(self.socket.fd, self.data, 0, self.addr.sockaddr(), self.addr.len) catch |err| switch (err) {
            error.WouldBlock => {
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setWriterWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = n };
    }

    pub fn deinit(self: *SendToFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for receiving data from any sender.
pub const RecvFromFuture = struct {
    pub const Output = anyerror!UdpSocket.RecvFromResult;

    socket: *UdpSocket,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *RecvFromFuture, ctx: *FutureContext) FuturePollResult(Output) {
        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);

        const n = xplat.recvfrom(self.socket.fd, self.buf, 0, addr.sockaddrMut(), &addr.len) catch |err| switch (err) {
            error.WouldBlock => {
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = .{ .len = n, .addr = addr } };
    }

    pub fn deinit(self: *RecvFromFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for peeking at data.
pub const PeekFuture = struct {
    pub const Output = anyerror!usize;

    socket: *UdpSocket,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    /// Poll the future.
    pub fn poll(self: *PeekFuture, ctx: *FutureContext) FuturePollResult(Output) {
        const n = xplat.recv(self.socket.fd, self.buf, posix.MSG.PEEK) catch |err| switch (err) {
            error.WouldBlock => {
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.socket.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = n };
    }

    pub fn deinit(self: *PeekFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Future Waker Bridge Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Update a stored Future Waker from poll context.
fn updateStoredWaker(slot: *?FutureWaker, ctx: *FutureContext) void {
    const new_waker = ctx.getWaker();
    if (slot.*) |*old| {
        if (!old.willWakeSame(new_waker)) {
            old.deinit();
            slot.* = new_waker.clone();
        }
    } else {
        slot.* = new_waker.clone();
    }
}

/// Clean up a stored Future Waker.
fn cleanupStoredWaker(slot: *?FutureWaker) void {
    if (slot.*) |*w| {
        w.deinit();
        slot.* = null;
    }
}

/// Create a backend I/O Waker that bridges to a stored Future Waker.
fn bridgeWaker(slot: *?FutureWaker) Waker {
    return .{
        .context = @ptrCast(slot),
        .wake_fn = bridgeWakeFn,
    };
}

fn bridgeWakeFn(ctx: *anyopaque) void {
    const slot: *?FutureWaker = @ptrCast(@alignCast(ctx));
    if (slot.*) |*w| {
        w.wakeByRef();
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

fn setBoolOption(fd: posix.socket_t, level: i32, opt: u32, value: bool) !void {
    const val: u32 = if (value) 1 else 0;
    try xplat.setsockopt(fd, @intCast(level), opt, std.mem.asBytes(&val));
}

fn getBoolOption(fd: posix.socket_t, level: i32, opt: u32) !bool {
    var val: u32 = 0;
    const bytes = std.mem.asBytes(&val);
    _ = try xplat.getsockopt(fd, @intCast(level), opt, bytes);
    return val != 0;
}

fn setU32Option(fd: posix.socket_t, level: i32, opt: u32, value: u32) !void {
    try xplat.setsockopt(fd, @intCast(level), opt, std.mem.asBytes(&value));
}

fn getU32Option(fd: posix.socket_t, level: i32, opt: u32) !u32 {
    var val: u32 = 0;
    const bytes = std.mem.asBytes(&val);
    _ = try xplat.getsockopt(fd, @intCast(level), opt, bytes);
    return val;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "UdpSocket - bind and close" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Should have been assigned a port
    try std.testing.expect(socket.localAddr().port() != 0);
}

test "UdpSocket - send and receive" {
    // Create two sockets
    var server = try UdpSocket.bind(Address.fromPort(0));
    defer server.close();

    var client = try UdpSocket.bind(Address.fromPort(0));
    defer client.close();

    // Use 127.0.0.1 as destination (0.0.0.0 is not a valid sendto target on Windows)
    const server_addr = Address.loopbackV4(server.localAddr().port());

    // Send from client to server
    const msg = "hello udp";
    _ = client.trySendTo(msg, server_addr) catch |err| {
        // CI VMs may not have full IPv4 networking (e.g., GitHub Actions macOS)
        if (err == error.NetworkUnreachable) return error.SkipZigTest;
        return err;
    };

    // Receive on server
    var buf: [64]u8 = undefined;
    // May need a small delay for the packet to arrive
    std.Thread.sleep(1_000_000); // 1ms

    if (try server.tryRecvFrom(&buf)) |result| {
        try std.testing.expectEqualStrings(msg, buf[0..result.len]);
        try std.testing.expectEqual(client.localAddr().port(), result.addr.port());
    }
}

test "UdpSocket - connect and send/recv" {
    var server = try UdpSocket.bind(Address.fromPort(0));
    defer server.close();

    var client = try UdpSocket.bind(Address.fromPort(0));
    defer client.close();

    // Connect client to server (use 127.0.0.1, not 0.0.0.0)
    try client.connect(Address.loopbackV4(server.localAddr().port()));
    try std.testing.expect(client.peerAddr() != null);

    // Send using connected send
    const msg = "connected udp";
    _ = try client.trySend(msg);

    std.Thread.sleep(1_000_000); // 1ms

    var buf: [64]u8 = undefined;
    if (try server.tryRecvFrom(&buf)) |_| {
        // Received the message
    }
}

test "UdpSocket - broadcast option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Should be disabled by default
    try std.testing.expect(!(try socket.getBroadcast()));

    // Enable broadcast
    try socket.setBroadcast(true);
    try std.testing.expect(try socket.getBroadcast());

    // Disable broadcast
    try socket.setBroadcast(false);
    try std.testing.expect(!(try socket.getBroadcast()));
}

test "UdpSocket - TTL option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Set TTL
    try socket.setTtl(64);
    try std.testing.expectEqual(@as(u32, 64), try socket.getTtl());
}

test "UdpSocket - buffer sizes" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Get initial sizes (just verify no error)
    _ = try socket.getSendBufferSize();
    _ = try socket.getRecvBufferSize();

    // Set sizes (kernel may adjust)
    try socket.setSendBufferSize(65536);
    try socket.setRecvBufferSize(65536);
}

test "UdpSocket - TOS option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Set TOS
    try socket.setTos(0x10); // Low delay
    try std.testing.expectEqual(@as(u32, 0x10), try socket.getTos());
}

test "UdpSocket - reuseAddr option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Enable reuse
    try socket.setReuseAddr(true);
    try std.testing.expect(try socket.getReuseAddr());

    // Disable reuse
    try socket.setReuseAddr(false);
    try std.testing.expect(!(try socket.getReuseAddr()));
}

test "UdpSocket - multicast loop option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Test IPv4 multicast loop
    try socket.setMulticastLoopV4(true);
    try std.testing.expect(try socket.getMulticastLoopV4());

    try socket.setMulticastLoopV4(false);
    try std.testing.expect(!(try socket.getMulticastLoopV4()));
}

test "UdpSocket - multicast TTL option" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Set multicast TTL
    try socket.setMulticastTtlV4(5);
    try std.testing.expectEqual(@as(u32, 5), try socket.getMulticastTtlV4());
}

test "UdpSocket - disconnect" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    var server = try UdpSocket.bind(Address.fromPort(0));
    defer server.close();

    // Connect to server (use 127.0.0.1, not 0.0.0.0)
    try socket.connect(Address.loopbackV4(server.localAddr().port()));
    try std.testing.expect(socket.peerAddr() != null);

    // Disconnect
    try socket.disconnect();
    try std.testing.expect(socket.peerAddr() == null);
}

test "UdpSocket - takeError" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Should have no error initially
    const err = try socket.takeError();
    try std.testing.expect(err == null);
}

test "UdpSocket - peek" {
    var server = try UdpSocket.bind(Address.fromPort(0));
    defer server.close();

    var client = try UdpSocket.bind(Address.fromPort(0));
    defer client.close();

    // Send message (use 127.0.0.1, not 0.0.0.0)
    const msg = "peek test";
    _ = client.trySendTo(msg, Address.loopbackV4(server.localAddr().port())) catch |err| {
        // CI VMs may not have full IPv4 networking
        if (err == error.NetworkUnreachable) return error.SkipZigTest;
        return err;
    };

    std.Thread.sleep(1_000_000); // 1ms

    // Peek should return data without consuming it
    var buf1: [64]u8 = undefined;
    if (try server.tryPeek(&buf1)) |n1| {
        try std.testing.expectEqualStrings(msg, buf1[0..n1]);

        // Read should return the same data
        var buf2: [64]u8 = undefined;
        if (try server.tryRecvFrom(&buf2)) |result| {
            try std.testing.expectEqualStrings(msg, buf2[0..result.len]);
        }
    }
}

test "UdpSocket - async futures creation" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    // Test that we can create async futures (they won't actually execute without runtime)
    var buf: [64]u8 = undefined;
    const server_addr = socket.localAddr();

    // These just verify the types are correct
    _ = socket.readable();
    _ = socket.writable();
    _ = socket.ready(.{ .readable = true, .writable = false });
    _ = socket.send("test");
    _ = socket.recv(&buf);
    _ = socket.sendTo("test", server_addr);
    _ = socket.recvFrom(&buf);
    _ = socket.peek(&buf);
}

test "UdpSocket - reader/writer interfaces" {
    var socket = try UdpSocket.bind(Address.fromPort(0));
    defer socket.close();

    var server = try UdpSocket.bind(Address.fromPort(0));
    defer server.close();

    // Connect for reader/writer usage (use 127.0.0.1, not 0.0.0.0)
    try socket.connect(Address.loopbackV4(server.localAddr().port()));

    // Get reader and writer - just verify we can get them
    _ = socket.reader();
    _ = socket.writer();
}

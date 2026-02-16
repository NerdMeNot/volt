//! # Networking Module
//!
//! High-performance async networking for TCP, UDP, and Unix sockets.
//!
//! ## Socket Types
//!
//! | Type | Description |
//! |------|-------------|
//! | `TcpListener` | Accept incoming TCP connections |
//! | `TcpStream` | Bidirectional TCP connection |
//! | `TcpSocket` | Socket builder for custom configuration |
//! | `UdpSocket` | Connectionless datagram socket |
//! | `UnixStream` | Unix domain stream socket |
//! | `UnixListener` | Unix domain socket listener |
//! | `UnixDatagram` | Unix domain datagram socket |
//! | `Address` | IPv4/IPv6 socket address |
//!
//! ## Convenience Functions
//!
//! | Function | Description |
//! |----------|-------------|
//! | `listen` | Create TCP listener from address string |
//! | `listenPort` | Create TCP listener on all interfaces |
//! | `connect` | Connect to TCP server from address string |
//! | `resolve` | DNS hostname resolution |
//! | `resolveFirst` | Resolve and return first address |
//! | `connectHost` | Connect with DNS resolution |
//!
//! ## Usage
//!
//! ```zig
//! // TCP Server
//! var listener = try io.net.listen("0.0.0.0:8080");
//! defer listener.close();
//! while (listener.tryAccept() catch null) |result| {
//!     handleClient(result.stream);
//! }
//!
//! // TCP Client
//! var stream = try io.net.connect("127.0.0.1:8080");
//! defer stream.close();
//! try stream.writeAll("hello");
//! ```
//!
//! ## Advanced
//!
//! Split halves, future types, and result types are accessible via sub-modules:
//! `net.tcp.ReadHalf`, `net.tcp.AcceptFuture`, `net.udp.SendFuture`, etc.

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════
// Sub-modules (for advanced access to internal types)
// ═══════════════════════════════════════════════════════════════════════════

pub const address = @import("net/address.zig");
pub const tcp = @import("net/tcp.zig");
pub const udp = @import("net/udp.zig");
pub const unix = @import("net/unix.zig");

// ═══════════════════════════════════════════════════════════════════════════
// Primary API — Socket Types
// ═══════════════════════════════════════════════════════════════════════════

/// IPv4/IPv6 socket address.
pub const Address = address.Address;

/// TCP socket builder. Configure options before connect/listen.
pub const TcpSocket = tcp.TcpSocket;

/// TCP server listener. Use `bind()` to create, then `tryAccept()` for connections.
pub const TcpListener = tcp.TcpListener;

/// TCP stream (connected socket). Use `tryRead`/`tryWrite`/`writeAll` for I/O.
pub const TcpStream = tcp.TcpStream;

/// UDP socket for connectionless datagram communication.
pub const UdpSocket = udp.UdpSocket;

/// Unix domain stream socket (connection-oriented).
pub const UnixStream = unix.UnixStream;

/// Unix domain socket listener (for stream connections).
pub const UnixListener = unix.UnixListener;

/// Unix domain datagram socket (connectionless).
pub const UnixDatagram = unix.UnixDatagram;

// ═══════════════════════════════════════════════════════════════════════════
// Convenience Functions
// ═══════════════════════════════════════════════════════════════════════════

/// Create a TCP listener bound to the given address.
pub fn listen(addr_str: []const u8) !TcpListener {
    const addr = try Address.parse(addr_str);
    return TcpListener.bind(addr);
}

/// Create a TCP listener bound to a port on all interfaces (0.0.0.0).
pub fn listenPort(port: u16) !TcpListener {
    return TcpListener.bind(Address.fromPort(port));
}

/// Connect to a TCP server at the given address.
pub fn connect(addr_str: []const u8) !TcpStream {
    const addr = try Address.parse(addr_str);
    return TcpStream.connect(addr);
}

// ═══════════════════════════════════════════════════════════════════════════
// DNS Resolution
// ═══════════════════════════════════════════════════════════════════════════

/// DNS lookup result.
pub const LookupResult = struct {
    addresses: []Address,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *LookupResult) void {
        self.allocator.free(self.addresses);
    }
};

/// Resolve a hostname to IP addresses (blocking — use blocking pool for async).
pub fn resolve(allocator: std.mem.Allocator, host: []const u8, port: u16) !LookupResult {
    const list = try std.net.getAddressList(allocator, host, port);
    defer list.deinit();

    var addresses = try allocator.alloc(Address, list.addrs.len);
    errdefer allocator.free(addresses);

    for (list.addrs, 0..) |addr, i| {
        addresses[i] = Address.fromStd(addr);
    }

    return .{
        .addresses = addresses,
        .allocator = allocator,
    };
}

/// Resolve a hostname and return the first address.
pub fn resolveFirst(allocator: std.mem.Allocator, host: []const u8, port: u16) !Address {
    var result = try resolve(allocator, host, port);
    defer result.deinit();

    if (result.addresses.len == 0) {
        return error.HostNotFound;
    }

    return result.addresses[0];
}

/// Connect to a hostname (with DNS resolution, blocking).
pub fn connectHost(allocator: std.mem.Allocator, host: []const u8, port: u16) !TcpStream {
    const addr = try resolveFirst(allocator, host, port);
    return TcpStream.connect(addr);
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test {
    _ = address;
    _ = tcp;
    _ = udp;
    _ = unix;
}

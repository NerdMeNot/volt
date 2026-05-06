//! `volt.net` — networking namespace.
//!
//! Re-exports the concrete types built on top of the `volt.io` trait
//! surface. Adding a type? Put it in its own `*.zig` file under
//! `src/net/` and re-export here.

pub const Address = @import("Address.zig").Address;
pub const TcpListener = @import("TcpListener.zig").TcpListener;
pub const TcpStream = @import("TcpStream.zig").TcpStream;
pub const ShutdownHow = @import("TcpStream.zig").ShutdownHow;
pub const UdpSocket = @import("UdpSocket.zig").UdpSocket;
pub const Datagram = @import("UdpSocket.zig").Datagram;

pub const ListenError = @import("TcpListener.zig").ListenError;
pub const AcceptError = @import("TcpListener.zig").AcceptError;
pub const ConnectError = @import("TcpStream.zig").ConnectError;
pub const UdpBindError = @import("UdpSocket.zig").BindError;
pub const UdpConnectError = @import("UdpSocket.zig").ConnectError;
pub const SendError = @import("UdpSocket.zig").SendError;
pub const RecvError = @import("UdpSocket.zig").RecvError;

/// Typed socket-option setters. Reusable across TcpStream / UdpSocket /
/// Unix sockets — each socket type also exposes per-option methods
/// that forward here.
pub const sockopt = @import("sockopt.zig");

test {
    _ = @import("Address.zig");
    _ = @import("TcpListener.zig");
    _ = @import("TcpStream.zig");
    _ = @import("UdpSocket.zig");
    _ = @import("sockopt.zig");
}

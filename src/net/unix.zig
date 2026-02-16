//! Unix Domain Sockets
//!
//! Provides async Unix domain sockets for local inter-process communication.
//!
//! ## Socket Types
//!
//! - `UnixStream` - Connected stream socket (like TCP)
//! - `UnixListener` - Server socket that accepts stream connections
//! - `UnixDatagram` - Datagram socket (like UDP)
//!
//! ## Quick Start
//!
//! ### Stream Sockets (Server)
//!
//! ```zig
//! const unix = @import("volt").net.unix;
//!
//! var listener = try unix.UnixListener.bind("/tmp/my.sock");
//! defer listener.close();
//!
//! while (true) {
//!     const result = try listener.accept();
//!     defer result.stream.close();
//!     // Handle connection
//! }
//! ```
//!
//! ### Stream Sockets (Client)
//!
//! ```zig
//! var stream = try unix.UnixStream.connect("/tmp/my.sock");
//! defer stream.close();
//!
//! try stream.writeAll("Hello!");
//! var buf: [1024]u8 = undefined;
//! const n = try stream.read(&buf);
//! ```
//!
//! ### Datagram Sockets
//!
//! ```zig
//! var socket = try unix.UnixDatagram.bind("/tmp/my.sock");
//! defer socket.close();
//!
//! // Send to specific address
//! try socket.sendTo("Hello", try unix.UnixAddr.fromPath("/tmp/other.sock"));
//!
//! // Receive from any address
//! var buf: [1024]u8 = undefined;
//! const result = try socket.recvFrom(&buf);
//! ```
//!
//! ## Socket Pairs
//!
//! ```zig
//! // Create connected pair for IPC
//! var sockets = try unix.UnixDatagram.pair();
//! defer sockets[0].close();
//! defer sockets[1].close();
//!
//! try sockets[0].send("Hello from parent");
//! const n = try sockets[1].recv(&buf);
//! ```

const std = @import("std");
const builtin = @import("builtin");

// Unix domain sockets are not available on Windows
comptime {
    if (builtin.os.tag == .windows) {
        @compileError("Unix domain sockets are not supported on Windows. Use TCP sockets or named pipes instead.");
    }
}

// Re-export all types
pub const stream = @import("unix/stream.zig");
pub const listener = @import("unix/listener.zig");
pub const datagram = @import("unix/datagram.zig");

// Main types
pub const UnixStream = stream.UnixStream;
pub const UnixAddr = stream.UnixAddr;
pub const ReadHalf = stream.ReadHalf;
pub const WriteHalf = stream.WriteHalf;

pub const UnixListener = listener.UnixListener;
pub const AcceptResult = listener.AcceptResult;
pub const AcceptWaiter = listener.AcceptWaiter;

pub const UnixDatagram = datagram.UnixDatagram;
pub const RecvFromResult = datagram.RecvFromResult;
pub const DatagramWaiter = datagram.DatagramWaiter;

// Waiters
pub const StreamWaiter = stream.StreamWaiter;

// Tests
test {
    _ = @import("unix/stream.zig");
    _ = @import("unix/listener.zig");
    _ = @import("unix/datagram.zig");
}

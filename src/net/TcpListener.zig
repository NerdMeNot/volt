//! `volt.net.TcpListener` — async TCP listener.
//!
//! Wraps a non-blocking listening socket. `accept` parks the calling
//! coroutine on the reactor when no connection is ready, and the
//! reactor wakes us when the kernel queues one.

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io_errors = @import("../io/errors.zig");
const Address = @import("Address.zig").Address;
const TcpStream = @import("TcpStream.zig").TcpStream;

pub const ListenError =
    io_errors.SocketError ||
    io_errors.BindError ||
    io_errors.ListenError ||
    io_errors.FcntlError;

pub const AcceptError = io_errors.AcceptError || io_errors.FcntlError;

pub const TcpListener = struct {
    fd: posix.socket_t,

    /// Bind a TCP listener to `address`. The socket is non-blocking.
    pub fn bind(address: Address) ListenError!TcpListener {
        const sock_type = posix.SOCK.STREAM | syscall.SOCK_NONBLOCK | syscall.SOCK_CLOEXEC;
        const fd = try syscall.socket(address.family(), sock_type, 0);
        errdefer syscall.close(fd);

        try syscall.bind(fd, &address.any, address.osSockLen());
        try syscall.listen(fd, 128);

        return .{ .fd = fd };
    }

    pub fn close(self: *TcpListener) void {
        syscall.close(self.fd);
    }

    /// Async accept. Returns a new `TcpStream` for the accepted connection.
    pub fn accept(self: *TcpListener) AcceptError!TcpStream {
        const flags: u32 = syscall.SOCK_NONBLOCK | syscall.SOCK_CLOEXEC;
        while (true) {
            const accepted = syscall.accept(self.fd, null, null, flags) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitReadable(self.fd);
                    continue;
                },
                else => return err,
            };
            return .{ .fd = accepted };
        }
    }

    /// Read the kernel-assigned address (useful when bound to port 0).
    pub fn localAddress(self: *const TcpListener) !Address {
        var addr: Address = undefined;
        var len: posix.socklen_t = @sizeOf(Address);
        try syscall.getsockname(self.fd, &addr.any, &len);
        return addr;
    }
};

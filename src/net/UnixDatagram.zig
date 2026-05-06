//! `volt.net.UnixDatagram` — async Unix domain SOCK_DGRAM socket.
//!
//! Companion to `UdpSocket` but over `AF_UNIX`: message-oriented IPC
//! between processes on the same host without going through the IP
//! stack. Useful for syslog-style transports and small-message RPC
//! where the overhead of TCP/UDS-stream framing isn't justified.

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io_errors = @import("../io/errors.zig");
const UnixAddress = @import("UnixAddress.zig").UnixAddress;

pub const BindError =
    io_errors.SocketError ||
    io_errors.BindError ||
    io_errors.FcntlError;

pub const SendError = io_errors.SendError;
pub const RecvError = io_errors.RecvError;

pub const Datagram = struct {
    len: usize,
    addr: UnixAddress,
};

pub const UnixDatagram = struct {
    fd: posix.socket_t,

    pub fn bind(address: UnixAddress) BindError!UnixDatagram {
        const sock_type = posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
        const fd = try syscall.socket(posix.AF.UNIX, sock_type, 0);
        errdefer syscall.close(fd);
        try syscall.bind(fd, address.sockaddrPtr(), address.osSockLen());
        return .{ .fd = fd };
    }

    pub fn close(self: *UnixDatagram) void {
        syscall.close(self.fd);
    }

    pub fn sendTo(self: *UnixDatagram, buf: []const u8, address: UnixAddress) SendError!usize {
        while (true) {
            const r = syscall.sendto(self.fd, buf, 0, address.sockaddrPtr(), address.osSockLen()) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitWritable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    pub fn recvFrom(self: *UnixDatagram, buf: []u8) RecvError!Datagram {
        while (true) {
            var addr: UnixAddress = .{ .inner = .{}, .path_len = 0 };
            var addrlen: posix.socklen_t = @sizeOf(@TypeOf(addr.inner));
            const r = syscall.recvfrom(self.fd, buf, 0, addr.sockaddrPtrMut(), &addrlen) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitReadable(self.fd);
                    continue;
                },
                else => return err,
            };
            // Recover the path length from the kernel-reported sockaddr len.
            // 2 bytes precede the path on every platform we support.
            if (addrlen >= 2) addr.path_len = addrlen - 2;
            return .{ .len = r, .addr = addr };
        }
    }
};

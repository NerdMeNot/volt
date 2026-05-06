//! `volt.net.UnixListener` — async listening Unix domain socket.
//!
//! `bind` removes any existing file at `path` so the socket can
//! claim it. `close` does NOT unlink — caller is responsible for
//! cleanup (mirrors Tokio / std::os::unix behaviour). Pair with a
//! `defer std.posix.unlink(path)` if you want auto-cleanup.

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io_errors = @import("../io/errors.zig");
const UnixAddress = @import("UnixAddress.zig").UnixAddress;
const UnixStream = @import("UnixStream.zig").UnixStream;

pub const ListenError =
    io_errors.SocketError ||
    io_errors.BindError ||
    io_errors.ListenError ||
    io_errors.FcntlError;

pub const AcceptError = io_errors.AcceptError || io_errors.FcntlError;

pub const UnixListener = struct {
    fd: posix.socket_t,

    pub fn bind(address: UnixAddress) ListenError!UnixListener {
        const sock_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
        const fd = try syscall.socket(posix.AF.UNIX, sock_type, 0);
        errdefer syscall.close(fd);

        try syscall.bind(fd, address.sockaddrPtr(), address.osSockLen());
        try syscall.listen(fd, 128);
        return .{ .fd = fd };
    }

    pub fn close(self: *UnixListener) void {
        syscall.close(self.fd);
    }

    pub fn accept(self: *UnixListener) AcceptError!UnixStream {
        const flags: u32 = posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
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
};

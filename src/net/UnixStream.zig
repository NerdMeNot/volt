//! `volt.net.UnixStream` — async connected Unix domain socket.
//!
//! Same shape as `TcpStream` but over `AF_UNIX`. Useful for IPC,
//! sidecars, container-host bridges. Implements the `volt.io`
//! trait surface (Reader/Writer/Closer with `as_fd`).
//!
//! ## SCM_RIGHTS / fd-passing — deferred
//!
//! The motivating reason many users reach for Unix sockets is to
//! pass file descriptors between processes via `SCM_RIGHTS` control
//! messages on `sendmsg`/`recvmsg`. Volt v1.1 ships the byte-stream
//! surface only; fd-passing wrappers are tracked as a follow-up
//! (need `sendmsg` / `recvmsg` syscall wrappers and platform-
//! specific `cmsghdr` layout dispatch).

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io = @import("../io/io.zig");
const io_errors = @import("../io/errors.zig");
const traits = @import("../io/traits/traits.zig");
const UnixAddress = @import("UnixAddress.zig").UnixAddress;

pub const ConnectError =
    io_errors.SocketError ||
    io_errors.ConnectError ||
    io_errors.FcntlError ||
    io_errors.GetSockOptError ||
    error{ConnectFailed};

pub const ShutdownHow = enum { read, write, both };

pub const UnixStream = struct {
    fd: posix.socket_t,

    pub fn connect(address: UnixAddress) ConnectError!UnixStream {
        const sock_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
        const fd = try syscall.socket(posix.AF.UNIX, sock_type, 0);
        errdefer syscall.close(fd);

        syscall.connect(fd, address.sockaddrPtr(), address.osSockLen()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => {
                try wait.waitWritable(fd);
                var errno_val: u32 = 0;
                try syscall.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, std.mem.asBytes(&errno_val));
                if (errno_val != 0) return error.ConnectFailed;
            },
            else => return err,
        };
        return .{ .fd = fd };
    }

    pub fn close(self: *UnixStream) void {
        syscall.close(self.fd);
    }

    pub fn read(self: *UnixStream, buf: []u8) io.ReadError!usize {
        return io.read(self.fd, buf);
    }

    pub fn write(self: *UnixStream, buf: []const u8) io.WriteError!usize {
        return io.write(self.fd, buf);
    }

    pub fn writeAll(self: *UnixStream, buf: []const u8) io.WriteError!void {
        return io.writeAll(self.fd, buf);
    }

    pub fn readv(self: *UnixStream, iovs: []posix.iovec) io.ReadError!usize {
        while (true) {
            const r = syscall.readv(self.fd, iovs) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitReadable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    pub fn writev(self: *UnixStream, iovs: []const posix.iovec_const) io.WriteError!usize {
        while (true) {
            const r = syscall.writev(self.fd, iovs) catch |err| switch (err) {
                error.WouldBlock => {
                    try wait.waitWritable(self.fd);
                    continue;
                },
                else => return err,
            };
            return r;
        }
    }

    pub fn shutdown(self: *UnixStream, how: ShutdownHow) io_errors.ShutdownError!void {
        const how_syscall: syscall.ShutdownHow = switch (how) {
            .read => .recv,
            .write => .send,
            .both => .both,
        };
        return syscall.shutdown(self.fd, how_syscall);
    }

    // ── Trait surface ──────────────────────────────────────────────────

    pub fn reader(self: *UnixStream) traits.Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &reader_vtable };
    }

    pub fn writer(self: *UnixStream) traits.Writer {
        return .{ .ctx = @ptrCast(self), .vtable = &writer_vtable };
    }

    pub fn closer(self: *UnixStream) traits.Closer {
        return .{ .ctx = @ptrCast(self), .vtable = &closer_vtable };
    }

    const reader_vtable: traits.Reader.VTable = .{ .read = &traitRead, .as_fd = &traitAsFd };
    const writer_vtable: traits.Writer.VTable = .{ .write = &traitWrite, .as_fd = &traitAsFd };
    const closer_vtable: traits.Closer.VTable = .{ .close = &traitClose };

    fn traitRead(ctx: *anyopaque, buf: []u8) io_errors.ReadError!usize {
        const self: *UnixStream = @ptrCast(@alignCast(ctx));
        return io.read(self.fd, buf);
    }

    fn traitWrite(ctx: *anyopaque, buf: []const u8) io_errors.WriteError!usize {
        const self: *UnixStream = @ptrCast(@alignCast(ctx));
        return io.write(self.fd, buf);
    }

    fn traitClose(ctx: *anyopaque) void {
        const self: *UnixStream = @ptrCast(@alignCast(ctx));
        syscall.close(self.fd);
    }

    fn traitAsFd(ctx: *anyopaque) ?posix.fd_t {
        const self: *UnixStream = @ptrCast(@alignCast(ctx));
        return self.fd;
    }
};

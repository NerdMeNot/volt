//! `volt.net.TcpStream` — async TCP connection.
//!
//! Wraps a non-blocking stream socket. `read` / `write` park on EAGAIN
//! and resume when the reactor reports readiness. Implements the
//! `volt.io.Reader` / `Writer` / `Closer` traits with `as_fd`
//! populated — `volt.io.copy` will dispatch to sendfile/splice when
//! both ends are real fds (P2.E fills those arms).

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const io = @import("../io/io.zig");
const io_errors = @import("../io/errors.zig");
const traits = @import("../io/traits/traits.zig");
const Address = @import("Address.zig").Address;

pub const ConnectError =
    io_errors.SocketError ||
    io_errors.ConnectError ||
    io_errors.FcntlError ||
    io_errors.GetSockOptError ||
    error{ConnectFailed};

pub const TcpStream = struct {
    fd: posix.socket_t,

    /// Async connect. On WouldBlock/InProgress, parks until writable
    /// then checks `SO_ERROR` to determine final status.
    pub fn connect(address: Address) ConnectError!TcpStream {
        const sock_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
        const fd = try syscall.socket(address.family(), sock_type, 0);
        errdefer syscall.close(fd);

        syscall.connect(fd, &address.any, address.osSockLen()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => {
                try wait.waitWritable(fd);
                var errno_val: u32 = 0;
                const opt_buf = std.mem.asBytes(&errno_val);
                try syscall.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, opt_buf);
                if (errno_val != 0) return error.ConnectFailed;
            },
            else => return err,
        };

        return .{ .fd = fd };
    }

    pub fn close(self: *TcpStream) void {
        syscall.close(self.fd);
    }

    pub fn read(self: *TcpStream, buf: []u8) io.ReadError!usize {
        return io.read(self.fd, buf);
    }

    pub fn write(self: *TcpStream, buf: []const u8) io.WriteError!usize {
        return io.write(self.fd, buf);
    }

    pub fn writeAll(self: *TcpStream, buf: []const u8) io.WriteError!void {
        return io.writeAll(self.fd, buf);
    }

    // ── Trait surface ──────────────────────────────────────────────────
    //
    // `as_fd` is populated — TcpStream is a real fd that participates
    // in kernel-level zero-copy via `volt.io.copy`. Transforming
    // wrappers (volt-tls future) MUST set `as_fd = null` even when
    // they hold a TcpStream underneath, or `copy` will pipe raw bytes
    // straight through and skip the encrypt/decrypt step.

    pub fn reader(self: *TcpStream) traits.Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &reader_vtable };
    }

    pub fn writer(self: *TcpStream) traits.Writer {
        return .{ .ctx = @ptrCast(self), .vtable = &writer_vtable };
    }

    pub fn closer(self: *TcpStream) traits.Closer {
        return .{ .ctx = @ptrCast(self), .vtable = &closer_vtable };
    }

    const reader_vtable: traits.Reader.VTable = .{
        .read = &traitRead,
        .as_fd = &traitAsFd,
    };

    const writer_vtable: traits.Writer.VTable = .{
        .write = &traitWrite,
        .as_fd = &traitAsFd,
    };

    const closer_vtable: traits.Closer.VTable = .{ .close = &traitClose };

    fn traitRead(ctx: *anyopaque, buf: []u8) io_errors.ReadError!usize {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        return io.read(self.fd, buf);
    }

    fn traitWrite(ctx: *anyopaque, buf: []const u8) io_errors.WriteError!usize {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        return io.write(self.fd, buf);
    }

    fn traitClose(ctx: *anyopaque) void {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        syscall.close(self.fd);
    }

    fn traitAsFd(ctx: *anyopaque) ?posix.fd_t {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        return self.fd;
    }
};

test "TcpStream: trait surface compiles" {
    var s: TcpStream = .{ .fd = -1 };
    const r = s.reader();
    const w = s.writer();
    const c = s.closer();
    try std.testing.expect(r.vtable.as_fd != null);
    try std.testing.expect(w.vtable.as_fd != null);
    _ = c;
}

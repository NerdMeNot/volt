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
const sockopt = @import("sockopt.zig");
const Address = @import("Address.zig").Address;
const Duration = @import("../time.zig").Duration;

pub const ConnectError =
    io_errors.SocketError ||
    io_errors.ConnectError ||
    io_errors.FcntlError ||
    io_errors.GetSockOptError ||
    error{ConnectFailed};

/// Half-close direction for `TcpStream.shutdown`.
pub const ShutdownHow = enum { read, write, both };

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

    /// Vectored read into a list of iovecs. Same EAGAIN-park pattern
    /// as `read`. Returns total bytes copied across the iovecs.
    pub fn readv(self: *TcpStream, iovs: []posix.iovec) io.ReadError!usize {
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

    /// Vectored write. Returns bytes consumed across the iovecs (may
    /// be less than the total — partial vectored writes split a vector).
    pub fn writev(self: *TcpStream, iovs: []const posix.iovec_const) io.WriteError!usize {
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

    /// Half-close one or both directions. After `shutdown(.write)`,
    /// peer reads see EOF; after `shutdown(.read)`, further reads
    /// return EOF locally. `shutdown(.both)` is the moral equivalent
    /// of close() but leaves the fd valid for getsockname/getpeername
    /// inspection.
    pub fn shutdown(self: *TcpStream, how: ShutdownHow) io_errors.ShutdownError!void {
        const how_syscall: syscall.ShutdownHow = switch (how) {
            .read => .recv,
            .write => .send,
            .both => .both,
        };
        return syscall.shutdown(self.fd, how_syscall);
    }

    // ── Socket options ─────────────────────────────────────────────────
    //
    // Forwarders to volt.net.sockopt — convenience methods so callers
    // don't have to reach for the raw fd. UDP / Unix sockets get the
    // same shape.

    pub fn setNoDelay(self: *TcpStream, value: bool) sockopt.SetOptionError!void {
        return sockopt.setNoDelay(self.fd, value);
    }

    pub fn setKeepAlive(self: *TcpStream, value: bool) sockopt.SetOptionError!void {
        return sockopt.setKeepAlive(self.fd, value);
    }

    pub fn setKeepAliveParams(
        self: *TcpStream,
        idle: Duration,
        interval: Duration,
        count: u32,
    ) sockopt.SetOptionError!void {
        return sockopt.setKeepAliveParams(self.fd, idle, interval, count);
    }

    pub fn setLinger(self: *TcpStream, value: ?Duration) sockopt.SetOptionError!void {
        return sockopt.setLinger(self.fd, value);
    }

    pub fn setRecvBufSize(self: *TcpStream, bytes: usize) sockopt.SetOptionError!void {
        return sockopt.setRecvBufSize(self.fd, bytes);
    }

    pub fn setSendBufSize(self: *TcpStream, bytes: usize) sockopt.SetOptionError!void {
        return sockopt.setSendBufSize(self.fd, bytes);
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

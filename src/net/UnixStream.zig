//! `volt.net.UnixStream` — async connected Unix domain socket.
//!
//! Same shape as `TcpStream` but over `AF_UNIX`. Useful for IPC,
//! sidecars, container-host bridges. Implements the `volt.io`
//! trait surface (Reader/Writer/Closer with `as_fd`).
//!
//! ## SCM_RIGHTS / fd-passing
//!
//! Use `sendFds(fds, payload)` to attach 1+ file descriptors to a
//! sent payload, and `recvFd(buf)` to receive one back. The peer
//! sees a duplicated fd in their own table. Multi-fd recv via a
//! single call is a v1.2 follow-up — use multiple `recvFd` calls
//! today, or open a richer `recvFds(buf, max_fds)` issue if needed.

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

    // ── SCM_RIGHTS fd-passing ──────────────────────────────────────────
    //
    // A peer that calls recvFd / recvFds on a payload sent here will
    // see DUPLICATED fds in their own fd table — same kernel resource,
    // independent fd numbers. Closing one doesn't close the other.
    // Useful for: privileged-helper architectures, server fd-handoff,
    // sandbox bridges, sidecar IPC.

    pub const FdPassError = error{
        WouldBlock,
        BrokenPipe,
        ConnectionResetByPeer,
        AccessDenied,
        SystemResources,
        TooManyFds,
        InvalidArgument,
        Cancelled,
        WaitRegistrationFailed,
        Unexpected,
    };

    pub const RecvFdResult = struct {
        /// Received fd, or null if the peer didn't attach one.
        fd: ?std.posix.fd_t,
        /// Bytes copied into the caller's buffer.
        len: usize,
    };

    /// Cap on the number of fds in a single sendFds call. The kernel
    /// imposes its own limit (~SCM_MAX_FD = 253 on Linux); we set a
    /// stricter bound to keep the on-stack control buffer small.
    pub const MAX_FDS_PER_SEND = 16;

    /// Send up to MAX_FDS_PER_SEND descriptors along with `payload`.
    /// The payload MUST be at least one byte — SCM_RIGHTS is
    /// ancillary data on a regular sendmsg, so a zero-byte payload
    /// would be a no-op send.
    pub fn sendFds(
        self: *UnixStream,
        fds: []const std.posix.fd_t,
        payload: []const u8,
    ) FdPassError!usize {
        if (fds.len == 0) return error.InvalidArgument;
        if (payload.len == 0) return error.InvalidArgument;
        if (fds.len > MAX_FDS_PER_SEND) return error.TooManyFds;

        // Control buffer: enough for a cmsghdr + MAX_FDS_PER_SEND ints.
        // 16 (cmsghdr on 64-bit) + 16*4 (max fds) = 80; round up to
        // 128 for safety against alignment / padding nuances.
        var control_buf: [128]u8 align(@alignOf(std.c.cmsghdr)) = undefined;

        const cmsg_data_bytes = fds.len * @sizeOf(c_int);
        const cmsg_len: u32 = @intCast(@sizeOf(std.c.cmsghdr) + cmsg_data_bytes);

        // Write the cmsghdr at the start of the control buffer.
        const cmsg: *std.c.cmsghdr = @ptrCast(@alignCast(&control_buf));
        cmsg.* = .{
            .len = @intCast(cmsg_len),
            .level = std.posix.SOL.SOCKET,
            .type = scm_rights_value,
        };

        // Write each fd as c_int immediately after the cmsghdr.
        const data_offset = @sizeOf(std.c.cmsghdr);
        for (fds, 0..) |fd, i| {
            const fd_int: c_int = @intCast(fd);
            const ptr: *align(1) c_int = @ptrCast(&control_buf[data_offset + i * @sizeOf(c_int)]);
            ptr.* = fd_int;
        }

        var iov = [_]std.posix.iovec_const{.{
            .base = payload.ptr,
            .len = payload.len,
        }};

        var msg: std.c.msghdr_const = std.mem.zeroes(std.c.msghdr_const);
        msg.iov = &iov;
        msg.iovlen = 1;
        msg.control = &control_buf;
        msg.controllen = @intCast(cmsg_len);

        while (true) {
            const rc = std.c.sendmsg(self.fd, &msg, 0);
            if (rc >= 0) return @intCast(rc);
            switch (std.posix.errno(rc)) {
                .INTR => continue,
                .AGAIN => {
                    wait.waitWritable(self.fd) catch |err| return mapWaitError(err);
                    continue;
                },
                .PIPE => return error.BrokenPipe,
                .CONNRESET => return error.ConnectionResetByPeer,
                .ACCES, .PERM => return error.AccessDenied,
                .NOMEM, .NOBUFS => return error.SystemResources,
                .INVAL => return error.InvalidArgument,
                else => return error.Unexpected,
            }
        }
    }

    /// Receive at most one fd along with up to `buf.len` bytes of
    /// payload. Returns the byte count and (optionally) a single
    /// received fd. If the peer attached more fds in one message,
    /// only the first is returned and the rest are silently
    /// closed by the kernel.
    pub fn recvFd(self: *UnixStream, buf: []u8) FdPassError!RecvFdResult {
        var control_buf: [128]u8 align(@alignOf(std.c.cmsghdr)) = undefined;

        var iov = [_]std.posix.iovec{.{
            .base = buf.ptr,
            .len = buf.len,
        }};

        var msg: std.c.msghdr = std.mem.zeroes(std.c.msghdr);
        msg.iov = &iov;
        msg.iovlen = 1;
        msg.control = &control_buf;
        msg.controllen = control_buf.len;

        while (true) {
            const rc = std.c.recvmsg(self.fd, &msg, 0);
            if (rc < 0) {
                switch (std.posix.errno(rc)) {
                    .INTR => continue,
                    .AGAIN => {
                        wait.waitReadable(self.fd) catch |err| return mapWaitError(err);
                        continue;
                    },
                    .CONNRESET => return error.ConnectionResetByPeer,
                    .NOMEM, .NOBUFS => return error.SystemResources,
                    else => return error.Unexpected,
                }
            }
            const got: usize = @intCast(rc);

            // Walk the control buffer for an SCM_RIGHTS cmsg.
            var fd_out: ?std.posix.fd_t = null;
            if (msg.controllen >= @sizeOf(std.c.cmsghdr)) {
                const cmsg: *const std.c.cmsghdr = @ptrCast(@alignCast(&control_buf));
                if (cmsg.level == std.posix.SOL.SOCKET and cmsg.type == scm_rights_value) {
                    const data_offset = @sizeOf(std.c.cmsghdr);
                    if (cmsg.len >= data_offset + @sizeOf(c_int)) {
                        const fd_ptr: *align(1) const c_int = @ptrCast(&control_buf[data_offset]);
                        fd_out = @intCast(fd_ptr.*);
                    }
                }
            }
            return .{ .fd = fd_out, .len = got };
        }
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

fn mapWaitError(err: anytype) UnixStream.FdPassError {
    return switch (err) {
        error.Cancelled => error.Cancelled,
        error.OutOfMemory => error.SystemResources,
        error.AccessDenied => error.AccessDenied,
        error.SystemResources => error.SystemResources,
        error.WaitRegistrationFailed => error.WaitRegistrationFailed,
        else => error.Unexpected,
    };
}

// std.c.SCM dispatches per platform; SCM.RIGHTS is the standard
// ancillary type for fd-passing. Both Darwin and Linux use the
// same value (0x01) but we go through the dispatch so future
// platforms get the right constant.
const scm_rights_value: c_int = @intCast(std.c.SCM.RIGHTS);

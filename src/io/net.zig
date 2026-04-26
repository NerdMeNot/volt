//! TCP types — `volt.io.TcpListener` and `volt.io.TcpStream`.
//!
//! Both wrap a non-blocking socket fd and use `volt.io.wait*` to suspend the
//! calling coroutine on EAGAIN. The reactor wakes us when the kernel has work.
//!
//! v0.2 surface: IPv4 + IPv6 only (no Unix sockets, no UDP, no TLS). We
//! roll a minimal `Address` type rather than wire to Zig 0.16's `std.Io.net`
//! to keep the runtime self-contained — `std.Io` requires a runtime, which
//! is what we *are*.

const std = @import("std");
const posix = std.posix;
const system = posix.system;
const builtin = @import("builtin");

const syscall = @import("../internal/syscall.zig");
const wait = @import("wait.zig");
const io = @import("io.zig");

// ─────────────────────────────────────────────────────────────────────────────
// Address — small wrapper around posix sockaddr_in / sockaddr_in6.
// ─────────────────────────────────────────────────────────────────────────────

pub const Address = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,

    pub fn ip4(octets: [4]u8, port: u16) Address {
        // sockaddr_in.addr is in network byte order (big-endian) — but stored
        // as a u32. We pack the octets directly: a.b.c.d -> a is highest byte
        // when laid out big-endian.
        const addr_be: u32 = (@as(u32, octets[0]) << 24) |
            (@as(u32, octets[1]) << 16) |
            (@as(u32, octets[2]) << 8) |
            @as(u32, octets[3]);
        return .{ .in = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.nativeToBig(u32, addr_be),
            .zero = .{0} ** 8,
        } };
    }

    pub fn loopback4(port: u16) Address {
        return ip4(.{ 127, 0, 0, 1 }, port);
    }

    pub fn family(self: *const Address) u16 {
        return self.any.family;
    }

    pub fn osSockLen(self: *const Address) posix.socklen_t {
        return switch (self.any.family) {
            posix.AF.INET => @sizeOf(posix.sockaddr.in),
            posix.AF.INET6 => @sizeOf(posix.sockaddr.in6),
            else => @sizeOf(posix.sockaddr),
        };
    }

    /// Port in native byte order.
    pub fn getPort(self: *const Address) u16 {
        return switch (self.any.family) {
            posix.AF.INET => std.mem.bigToNative(u16, self.in.port),
            posix.AF.INET6 => std.mem.bigToNative(u16, self.in6.port),
            else => 0,
        };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

pub const ListenError = syscall.SocketError || syscall.BindError ||
    syscall.ListenError || syscall.FcntlError;

pub const AcceptError = syscall.AcceptError || wait.WaitError ||
    syscall.FcntlError;

pub const ConnectError = syscall.SocketError || syscall.ConnectError ||
    syscall.FcntlError || syscall.GetSockOptError ||
    wait.WaitError || error{ConnectFailed};

// ─────────────────────────────────────────────────────────────────────────────
// TcpListener
// ─────────────────────────────────────────────────────────────────────────────

pub const TcpListener = struct {
    fd: posix.socket_t,

    /// Bind a TCP listener to `address`. The socket is non-blocking.
    pub fn bind(address: Address) ListenError!TcpListener {
        const sock_type = posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
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

    /// Read the kernel-assigned address (useful when bound to port 0).
    pub fn localAddress(self: *const TcpListener) !Address {
        var addr: Address = undefined;
        var len: posix.socklen_t = @sizeOf(Address);
        try syscall.getsockname(self.fd, &addr.any, &len);
        return addr;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// TcpStream
// ─────────────────────────────────────────────────────────────────────────────

pub const TcpStream = struct {
    fd: posix.socket_t,

    /// Async connect. On WouldBlock/InProgress, parks until writable then
    /// checks `SO_ERROR` to determine final status.
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
};

test "address: ip4 round-trip" {
    const addr = Address.ip4(.{ 192, 168, 1, 50 }, 8080);
    try std.testing.expectEqual(@as(u16, posix.AF.INET), addr.family());
    try std.testing.expectEqual(@as(u16, 8080), addr.getPort());
}

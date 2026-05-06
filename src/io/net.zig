//! TCP types — `volt.io.TcpListener` and `volt.io.TcpStream`.
//!
//! Both wrap a non-blocking socket fd and use `volt.io.wait*` to suspend the
//! calling coroutine on EAGAIN. The reactor wakes us when the kernel has work.
//!
//! Current surface: IPv4 + IPv6 only (no Unix sockets, no UDP, no TLS). We
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
const io_errors = @import("errors.zig");
const traits = @import("traits/traits.zig");

// ─────────────────────────────────────────────────────────────────────────────
// Address — small wrapper around posix sockaddr_in / sockaddr_in6.
// ─────────────────────────────────────────────────────────────────────────────

/// IPv4 / IPv6 socket address. Naming and conventions mirror
/// `std.net.Address` (`initIp4` for explicit-octets, `parseIp` for
/// string parsing, `loopback*` / `any*` for common defaults) so users
/// who already know the std pattern can reach for the right ctor.
pub const Address = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,

    /// Construct an IPv4 address from a 4-byte octet array + port.
    pub fn initIp4(octets: [4]u8, port: u16) Address {
        // sockaddr_in.addr is a 4-byte field in network byte order. Reinterpret
        // the octets as a u32 with the same byte layout — bytesToValue preserves
        // the in-memory byte order, so a.b.c.d on the wire stays a.b.c.d.
        return .{ .in = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.bytesToValue(u32, &octets),
            .zero = .{0} ** 8,
        } };
    }

    /// Construct an IPv6 address from a 16-byte array + port.
    pub fn initIp6(bytes: [16]u8, port: u16) Address {
        return .{ .in6 = .{
            .family = posix.AF.INET6,
            .port = std.mem.nativeToBig(u16, port),
            .flowinfo = 0,
            .addr = bytes,
            .scope_id = 0,
        } };
    }

    /// Loopback IPv4 (127.0.0.1) on the given port.
    pub fn loopback4(port: u16) Address {
        return initIp4(.{ 127, 0, 0, 1 }, port);
    }

    /// Loopback IPv6 (::1) on the given port.
    pub fn loopback6(port: u16) Address {
        var b: [16]u8 = .{0} ** 16;
        b[15] = 1;
        return initIp6(b, port);
    }

    /// Wildcard IPv4 (0.0.0.0) — for `bind` to accept on every
    /// interface.
    pub fn any4(port: u16) Address {
        return initIp4(.{ 0, 0, 0, 0 }, port);
    }

    /// Wildcard IPv6 (::) — for `bind` to accept on every interface.
    pub fn any6(port: u16) Address {
        return initIp6(.{0} ** 16, port);
    }

    /// Parse a dotted-quad IPv4 string (`"127.0.0.1"`) plus port.
    pub fn parseIp4(str: []const u8, port: u16) error{InvalidIPv4}!Address {
        var octets: [4]u8 = undefined;
        var i: usize = 0;
        var it = std.mem.splitScalar(u8, str, '.');
        while (it.next()) |part| : (i += 1) {
            if (i >= 4) return error.InvalidIPv4;
            octets[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidIPv4;
        }
        if (i != 4) return error.InvalidIPv4;
        return initIp4(octets, port);
    }

    /// Parse a "host:port" string. Detects IPv4 vs IPv6 by syntax —
    /// IPv6 must be bracketed (`[::1]:8080`). Returns `error.InvalidAddress`
    /// for unparseable input.
    pub fn parse(str: []const u8) error{InvalidAddress}!Address {
        // IPv6: `[host]:port`
        if (str.len > 0 and str[0] == '[') {
            const close = std.mem.indexOfScalar(u8, str, ']') orelse return error.InvalidAddress;
            if (close + 1 >= str.len or str[close + 1] != ':') return error.InvalidAddress;
            const host = str[1..close];
            const port_str = str[close + 2 ..];
            const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;
            // We don't yet ship a full IPv6 text parser; accept "::1" only.
            if (std.mem.eql(u8, host, "::1")) return loopback6(port);
            if (std.mem.eql(u8, host, "::")) return any6(port);
            return error.InvalidAddress;
        }
        // IPv4: `a.b.c.d:port`
        const colon = std.mem.lastIndexOfScalar(u8, str, ':') orelse return error.InvalidAddress;
        const host = str[0..colon];
        const port_str = str[colon + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;
        return parseIp4(host, port) catch error.InvalidAddress;
    }

    /// Address family (`AF_INET` / `AF_INET6`).
    pub fn family(self: *const Address) u16 {
        return self.any.family;
    }

    /// Length to pass to bind/connect/accept.
    pub fn osSockLen(self: *const Address) posix.socklen_t {
        return switch (self.any.family) {
            posix.AF.INET => @sizeOf(posix.sockaddr.in),
            posix.AF.INET6 => @sizeOf(posix.sockaddr.in6),
            else => @panic("Address.osSockLen: unsupported family — only AF_INET / AF_INET6"),
        };
    }

    /// Port in native byte order.
    pub fn getPort(self: *const Address) u16 {
        return switch (self.any.family) {
            posix.AF.INET => std.mem.bigToNative(u16, self.in.port),
            posix.AF.INET6 => std.mem.bigToNative(u16, self.in6.port),
            else => @panic("Address.getPort: unsupported family"),
        };
    }
};

test "Address.parse: ipv4 + port" {
    const a = try Address.parse("127.0.0.1:8080");
    try std.testing.expectEqual(@as(u16, 8080), a.getPort());
    try std.testing.expectEqual(@as(u16, posix.AF.INET), a.family());
}

test "Address.parse: ipv6 loopback bracketed" {
    const a = try Address.parse("[::1]:9090");
    try std.testing.expectEqual(@as(u16, 9090), a.getPort());
    try std.testing.expectEqual(@as(u16, posix.AF.INET6), a.family());
}

test "Address.parse: rejects garbage" {
    try std.testing.expectError(error.InvalidAddress, Address.parse("not-an-address"));
    try std.testing.expectError(error.InvalidAddress, Address.parse("256.0.0.1:1"));
}

test "Address.any4/loopback4 round-trip port" {
    try std.testing.expectEqual(@as(u16, 80), Address.any4(80).getPort());
    try std.testing.expectEqual(@as(u16, 81), Address.loopback4(81).getPort());
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

pub const ListenError = io_errors.SocketError || io_errors.BindError ||
    io_errors.ListenError || io_errors.FcntlError;

pub const AcceptError = io_errors.AcceptError || io_errors.FcntlError;

pub const ConnectError = io_errors.SocketError || io_errors.ConnectError ||
    io_errors.FcntlError || io_errors.GetSockOptError ||
    error{ConnectFailed};

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

    // ── Trait surface ──────────────────────────────────────────────────
    //
    // `as_fd` is populated — a TcpStream is a real fd that participates
    // in kernel-level zero-copy via `volt.io.copy`. Transforming
    // wrappers (volt-tls future) MUST set `as_fd = null` even when
    // they hold a TcpStream underneath, or `copy` will pipe raw bytes
    // straight through and skip the TLS encrypt/decrypt step.

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

test "address: ip4 round-trip" {
    const addr = Address.initIp4(.{ 192, 168, 1, 50 }, 8080);
    try std.testing.expectEqual(@as(u16, posix.AF.INET), addr.family());
    try std.testing.expectEqual(@as(u16, 8080), addr.getPort());
}

test "TcpStream: trait surface compiles" {
    // Structural check — verifies reader()/writer()/closer() exist and
    // the vtable types align. Runtime behaviour exercised in
    // src/test/io_traits_test.zig (P1.E) where a runtime is available.
    var s: TcpStream = .{ .fd = -1 };
    const r = s.reader();
    const w = s.writer();
    const c = s.closer();
    // as_fd populated on both Reader and Writer.
    try std.testing.expect(r.vtable.as_fd != null);
    try std.testing.expect(w.vtable.as_fd != null);
    _ = c; // Closer has no marker; just check the binding exists.
}

//! TCP networking — TcpListener + TcpStream over the kqueue reactor.
//! IPv4 / loopback today; IPv6 + DNS later.
//!
//! All blocking ops yield to the reactor on EAGAIN. The API looks
//! synchronous; suspend points are transparent.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const reactor_mod = @import("reactor.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");

const AF_INET: c_int = 2;
const SOCK_STREAM: c_int = 1;
const IPPROTO_TCP: c_int = 6;

// Platform-specific socket constants. Darwin and Linux disagree on
// SOL_SOCKET / EINPROGRESS / EAGAIN values; the switches resolve at
// comptime so each target sees one canonical value.
const SOL_SOCKET: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 0xFFFF,
    .linux, .freebsd, .openbsd, .netbsd, .dragonfly => 1,
    .windows => 0xFFFF, // WinSock SOL_SOCKET
    else => @compileError("SOL_SOCKET not defined for this OS"),
};
const SO_REUSEADDR: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 0x0004,
    .linux => 2,
    .freebsd, .openbsd, .netbsd, .dragonfly => 0x0004,
    .windows => 0x0004,
    else => @compileError("SO_REUSEADDR not defined for this OS"),
};
const EINPROGRESS: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 36,
    .linux => 115,
    .freebsd, .openbsd, .netbsd, .dragonfly => 36,
    .windows => 10036, // WSAEINPROGRESS
    else => @compileError("EINPROGRESS not defined for this OS"),
};
const EAGAIN: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 35,
    .linux => 11,
    .freebsd, .openbsd, .netbsd, .dragonfly => 35,
    .windows => 10035, // WSAEWOULDBLOCK
    else => @compileError("EAGAIN not defined for this OS"),
};

const sockaddr_in = extern struct {
    len: u8 = @sizeOf(sockaddr_in),
    family: u8 = AF_INET,
    port: u16, // network order
    addr: u32, // network order
    zero: [8]u8 = @splat(0),
};

// Use c_-prefixed names to avoid shadowing user method names like
// `TcpListener.close` / `.bind`. Each extern is name-mapped to its
// real C symbol via the @extern intrinsic.
const socket = @extern(*const fn (c_int, c_int, c_int) callconv(.c) c_int, .{ .name = "socket" });
const c_bind = @extern(*const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "bind" });
const c_listen = @extern(*const fn (c_int, c_int) callconv(.c) c_int, .{ .name = "listen" });
const c_accept = @extern(*const fn (c_int, ?*anyopaque, ?*c_uint) callconv(.c) c_int, .{ .name = "accept" });
const c_connect = @extern(*const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "connect" });
const setsockopt = @extern(*const fn (c_int, c_int, c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "setsockopt" });
const getsockname = @extern(*const fn (c_int, *anyopaque, *c_uint) callconv(.c) c_int, .{ .name = "getsockname" });
const c_close = @extern(*const fn (c_int) callconv(.c) c_int, .{ .name = "close" });
const c_error = @extern(*const fn () callconv(.c) *c_int, .{ .name = "__error" });
inline fn errnoVal() c_int {
    return c_error().*;
}

// Note: `setNonblock` lives in `reactor.zig` (re-exported from each
// platform's backend). The duplicate that used to live here was
// consolidated as part of the L1 reactor refactor.

pub const Address = struct {
    /// IPv4 host bytes in host order (192.168.1.1 → {192,168,1,1}).
    host: [4]u8,
    /// Port in host order.
    port: u16,

    /// IPv4 loopback (127.0.0.1) at the given port. Use port 0 to
    /// let the kernel pick (see `TcpListener.localAddress`).
    pub fn loopback4(port: u16) Address {
        return .{ .host = .{ 127, 0, 0, 1 }, .port = port };
    }

    /// IPv4 "any" (0.0.0.0) — listen on every interface.
    pub fn any4(port: u16) Address {
        return .{ .host = .{ 0, 0, 0, 0 }, .port = port };
    }

    /// Parse a dotted-quad IPv4 string (`"192.168.1.1"`). Returns
    /// `error.InvalidAddress` on malformed input. No DNS — that's
    /// library territory.
    pub fn parse4(host: []const u8, port: u16) error{InvalidAddress}!Address {
        var bytes: [4]u8 = undefined;
        var i: usize = 0;
        var start: usize = 0;
        for (host, 0..) |ch, idx| {
            if (ch == '.') {
                if (i >= 4 or idx == start) return error.InvalidAddress;
                bytes[i] = std.fmt.parseInt(u8, host[start..idx], 10) catch return error.InvalidAddress;
                i += 1;
                start = idx + 1;
            }
        }
        if (i != 3 or start >= host.len) return error.InvalidAddress;
        bytes[3] = std.fmt.parseInt(u8, host[start..], 10) catch return error.InvalidAddress;
        return .{ .host = bytes, .port = port };
    }

    fn toSockaddr(self: Address) sockaddr_in {
        const addr_he: u32 = (@as(u32, self.host[0]) << 24) | (@as(u32, self.host[1]) << 16) |
            (@as(u32, self.host[2]) << 8) | @as(u32, self.host[3]);
        return .{
            .port = std.mem.nativeToBig(u16, self.port),
            .addr = std.mem.nativeToBig(u32, addr_he),
        };
    }

    fn fromSockaddr(sa: sockaddr_in) Address {
        const addr_he = std.mem.bigToNative(u32, sa.addr);
        return .{
            .host = .{
                @intCast((addr_he >> 24) & 0xFF),
                @intCast((addr_he >> 16) & 0xFF),
                @intCast((addr_he >> 8) & 0xFF),
                @intCast(addr_he & 0xFF),
            },
            .port = std.mem.bigToNative(u16, sa.port),
        };
    }
};

pub const TcpListener = struct {
    fd: i32,

    /// Bind a TCP listener at `addr`. Sets SO_REUSEADDR so repeated
    /// runs don't EADDRINUSE; non-blocking from the start.
    pub fn bind(addr: Address) !TcpListener {
        const fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);

        // SO_REUSEADDR so repeated test runs don't get EADDRINUSE.
        const one: c_int = 1;
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, @sizeOf(c_int));

        const sa = addr.toSockaddr();
        if (c_bind(fd, &sa, @sizeOf(sockaddr_in)) < 0) return error.BindFailed;
        if (c_listen(fd, 128) < 0) return error.ListenFailed;
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpListener) void {
        _ = c_close(@intCast(self.fd));
    }

    /// Get the bound local address (useful when bind port = 0).
    pub fn localAddress(self: *const TcpListener) !Address {
        var sa: sockaddr_in = undefined;
        var len: c_uint = @sizeOf(sockaddr_in);
        if (getsockname(@intCast(self.fd), &sa, &len) < 0) return error.GetSockNameFailed;
        return Address.fromSockaddr(sa);
    }

    /// Accept the next incoming connection. Yields to the reactor on EAGAIN.
    pub fn accept(self: *TcpListener) !TcpStream {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        while (true) {
            const new_fd = c_accept(@intCast(self.fd), null, null);
            if (new_fd >= 0) {
                try reactor_mod.setNonblock(@intCast(new_fd));
                return .{ .fd = @intCast(new_fd) };
            }
            if (errnoVal() != EAGAIN) return error.AcceptFailed;
            rt.reactor.waitReadable(self.fd);
        }
    }
};

pub const TcpStream = struct {
    fd: i32,

    pub fn connect(addr: Address) !TcpStream {
        const fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));

        const sa = addr.toSockaddr();
        if (c_connect(fd, &sa, @sizeOf(sockaddr_in)) < 0) {
            const e = errnoVal();
            if (e != EINPROGRESS) return error.ConnectFailed;
            // Wait for writable = connect completed.
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            rt.reactor.waitWritable(fd);
        }
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpStream) void {
        _ = c_close(@intCast(self.fd));
    }

    pub fn read(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.readAsync(&rt.reactor, self.fd, buf);
    }

    pub fn write(self: *TcpStream, buf: []const u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.writeAsync(&rt.reactor, self.fd, buf);
    }

    pub fn writeAll(self: *TcpStream, buf: []const u8) !void {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.writeAll(&rt.reactor, self.fd, buf);
    }

    pub fn readFull(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.readFull(&rt.reactor, self.fd, buf);
    }
};

// Note: the `EAGAIN` constant is defined at the top of this file
// via a `builtin.os.tag` switch. `isAgain` is no longer needed —
// callers compare against the platform-specific `EAGAIN` directly.

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// See note in src/runtime.zig: std.testing.allocator's stack-trace
// capture corrupts under multi-worker spawn. smp_allocator is the
// thread-safe choice for runtime-touching tests.
const test_allocator = std.heap.smp_allocator;

fn testListenerLifecycle() !void {
    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();
    if (addr.port == 0) return error.BadPort;
}

test "TcpListener: bind + localAddress" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(testListenerLifecycle, .{}));
}

const EchoTestCtx = struct {
    listener: *TcpListener,
    server_done: bool = false,
    client_done: bool = false,
    addr: Address = undefined,
    result_byte: u8 = 0,
};

fn testEchoServer(ctx: *EchoTestCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var buf: [4]u8 = undefined;
    _ = try stream.readFull(&buf);
    try stream.writeAll(&buf);
    ctx.server_done = true;
}

fn testEchoClient(ctx: *EchoTestCtx) !void {
    var stream = try TcpStream.connect(ctx.addr);
    defer stream.close();
    const send_buf = [_]u8{ 0xAB, 0xCD, 0xEF, 0x42 };
    try stream.writeAll(&send_buf);
    var recv_buf: [4]u8 = undefined;
    _ = try stream.readFull(&recv_buf);
    ctx.result_byte = recv_buf[3];
    ctx.client_done = true;
}

fn echoTestRoot(ctx: *EchoTestCtx) !void {
    const cur = @import("current.zig");
    const rt: *runtime.Runtime = @ptrCast(@alignCast(cur.require().runtime));
    var server_task = try rt.spawn(testEchoServer, .{ctx});
    var client_task = try rt.spawn(testEchoClient, .{ctx});
    _ = server_task.join() catch |err| return err;
    _ = client_task.join() catch |err| return err;
}

test "TCP echo single client round-trip" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = EchoTestCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(echoTestRoot, .{&ctx}));

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 0x42), ctx.result_byte);
}
